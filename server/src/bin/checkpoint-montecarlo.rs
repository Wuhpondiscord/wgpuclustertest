//! Seeded Monte Carlo comparison of checkpoint policies under repeated failures.

#[path = "../checkpoint.rs"]
mod checkpoint;
use checkpoint::{adaptive_interval, build_manifest, choose_storage_strategy, payload_size, ArtifactManifest, CheckpointInput, StorageStrategy, WorkerLink};

const MIB: u64 = 1024 * 1024;
const TASK_WORK: f64 = 100.0;
const WORKER_RATE: f64 = 1.0;
const FULL_BYTES: u64 = 256 * MIB;
const DELTA_BYTES: u64 = 51 * MIB;
const TRIALS: usize = 2_000;
const ENCODE_MS_PER_MB_PARITY: f64 = 0.18;
const DECODE_MS_PER_MB: f64 = 0.30;
const MAX_FAILURES_PER_TRIAL: usize = 1_000;

#[derive(Clone, Copy)]
struct Scenario { name: &'static str, bandwidth_mbps: u32, rtt_ms: u32, mttf_seconds: f64 }
const SCENARIOS: [Scenario; 2] = [
    Scenario { name: "stable_fast", bandwidth_mbps: 250, rtt_ms: 10, mttf_seconds: 3_600.0 },
    Scenario { name: "unreliable_slow", bandwidth_mbps: 18, rtt_ms: 80, mttf_seconds: 12.0 },
];

#[derive(Clone, Copy)]
struct Policy { name: &'static str, interval: f64, delta: bool, strategy: StorageStrategy }

#[derive(Default, Clone, Copy)]
struct Metrics { elapsed_ms: f64, traffic_mb: f64, wasted_work: f64, codec_ms: f64, peak_storage_mb: f64, failures: usize, data_loss: bool, finished: bool, capped: bool }

#[derive(Clone, Copy)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        self.0 = x;
        let bits = x.wrapping_mul(2_685_821_657_736_338_717) >> 11;
        (bits as f64 / 9_007_199_254_740_992.0).clamp(1e-12, 1.0 - 1e-12)
    }
    fn failure_after(&mut self, mean_seconds: f64) -> f64 { -mean_seconds.max(0.1) * (1.0 - self.next()).ln() }
}

fn main() {
    println!("scenario,policy,trials,checkpoint_interval_s,p50_total_s,p95_total_s,mean_total_s,mean_traffic_mb,mean_wasted_work,mean_failures,data_loss_trials_pct,mean_codec_s,peak_storage_mb,peer_domains,capped_trials_pct,finished_pct");
    for (scenario_index, scenario) in SCENARIOS.iter().enumerate() {
        let peers = worker_links(*scenario);
        for policy in policies(*scenario) {
            let mut samples = Vec::with_capacity(TRIALS);
            let mut total = Metrics::default();
            let mut data_loss_trials = 0;
            let mut capped_trials = 0;
            let mut finished = 0;
            for i in 0..TRIALS {
                // Use the same base stream per scenario/trial across policies.
                let seed = 0x6d65_7368_7265_636f_u64 ^ ((scenario_index as u64) << 48) ^ i as u64;
                let metrics = simulate(*scenario, &peers, policy, Rng(seed));
                samples.push(metrics.elapsed_ms / 1_000.0);
                total.elapsed_ms += metrics.elapsed_ms;
                total.traffic_mb += metrics.traffic_mb;
                total.wasted_work += metrics.wasted_work;
                total.codec_ms += metrics.codec_ms;
                total.peak_storage_mb = total.peak_storage_mb.max(metrics.peak_storage_mb);
                total.failures += metrics.failures;
                if metrics.data_loss { data_loss_trials += 1; }
                if metrics.capped { capped_trials += 1; }
                if metrics.finished { finished += 1; }
            }
            samples.sort_by(f64::total_cmp);
            let domains = peers.iter().map(|w| w.failure_domain.as_str()).collect::<std::collections::HashSet<_>>().len();
            println!("{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.3},{:.2},{:.2},{},{:.2},{}",
                scenario.name, policy.name, TRIALS, policy.interval,
                percentile(&samples, 0.50), percentile(&samples, 0.95), total.elapsed_ms / TRIALS as f64 / 1_000.0,
                total.traffic_mb / TRIALS as f64, total.wasted_work / TRIALS as f64,
                total.failures as f64 / TRIALS as f64,
                data_loss_trials as f64 * 100.0 / TRIALS as f64,
                total.codec_ms / TRIALS as f64 / 1_000.0,
                total.peak_storage_mb, domains, capped_trials as f64 * 100.0 / TRIALS as f64, finished * 100 / TRIALS);
        }
    }
}

fn policies(s: Scenario) -> Vec<Policy> {
    let base_input = CheckpointInput { full_bytes: FULL_BYTES, changed_bytes: DELTA_BYTES, has_base_checkpoint: true,
        observed_transfer_ms: transfer_ms(s, DELTA_BYTES), estimated_mttf_seconds: s.mttf_seconds,
        work_per_second: WORKER_RATE, min_interval_seconds: 2.0, max_interval_seconds: 60.0 };
    let adaptive = adaptive_interval(&base_input);
    let (delta_bytes, delta_enabled) = payload_size(&base_input);
    let adaptive_storage = choose_storage_strategy(delta_bytes, adaptive, s.bandwidth_mbps, 16 * MIB, 8.0, 0);
    let two_loss = choose_storage_strategy(delta_bytes, adaptive, s.bandwidth_mbps, 16 * MIB, 8.0, 2);
    let full_input = CheckpointInput { observed_transfer_ms: transfer_ms(s, FULL_BYTES), ..base_input.clone() };
    vec![
        Policy { name: "fixed_full_replica", interval: 10.0, delta: false, strategy: StorageStrategy::Replication2x },
        Policy { name: "fixed_delta_replica", interval: 10.0, delta: delta_enabled, strategy: StorageStrategy::Replication2x },
        Policy { name: "adaptive_full_replica", interval: adaptive_interval(&full_input), delta: false, strategy: StorageStrategy::Replication2x },
        Policy { name: "adaptive_delta", interval: adaptive, delta: delta_enabled, strategy: adaptive_storage },
        Policy { name: "adaptive_delta_two_loss", interval: adaptive, delta: delta_enabled, strategy: two_loss },
    ]
}

fn simulate(s: Scenario, peers: &[WorkerLink], policy: Policy, mut rng: Rng) -> Metrics {
    let mut out = Metrics::default();
    let manifest = build_manifest("sim-checkpoint".into(), 0, None, FULL_BYTES, FULL_BYTES, policy.strategy, peers);
    let Some(manifest) = manifest else { return out; };
    let (base_ms, base_codec) = write_cost(s, peers, &manifest, FULL_BYTES, policy.strategy);
    out.elapsed_ms += base_ms;
    out.traffic_mb += coded_mb(FULL_BYTES, policy.strategy);
    out.codec_ms += base_codec;
    out.peak_storage_mb = coded_mb(FULL_BYTES, policy.strategy);

    let mut durable_work = 0.0;
    let mut progress = 0.0;
    let mut delta_count = 0_u64;
    let mut latest_logical_bytes = FULL_BYTES;
    while progress < TASK_WORK && out.failures < MAX_FAILURES_PER_TRIAL {
        let next_boundary = ((durable_work / policy.interval).floor() + 1.0) * policy.interval;
        let target = next_boundary.min(TASK_WORK);
        let work_segment = (target - progress).max(0.0);
        let compute_seconds = work_segment / WORKER_RATE;
        let failure_seconds = rng.failure_after(s.mttf_seconds);
        if failure_seconds < compute_seconds {
            let partial_work = failure_seconds * WORKER_RATE;
            out.elapsed_ms += failure_seconds * 1_000.0;
            out.wasted_work += partial_work;
            progress += partial_work;
            recover(s, peers, &manifest, policy, &mut durable_work, &mut progress, &mut latest_logical_bytes, &mut delta_count, &mut out, &mut rng);
            continue;
        }

        out.elapsed_ms += compute_seconds * 1_000.0;
        progress = target;
        if progress >= TASK_WORK { break; }

        // A full compaction follows each four committed deltas, bounding restore-chain cost.
        let compaction = policy.delta && delta_count >= 4;
        let write_bytes = if policy.delta && !compaction { DELTA_BYTES } else { FULL_BYTES };
        let (write_ms, codec_ms) = write_cost(s, peers, &manifest, write_bytes, policy.strategy);
        let transfer_failure = rng.failure_after(s.mttf_seconds);
        if transfer_failure < write_ms / 1_000.0 {
            out.elapsed_ms += transfer_failure * 1_000.0;
            out.traffic_mb += coded_mb(write_bytes, policy.strategy) * (transfer_failure * 1_000.0 / write_ms.max(1.0)).clamp(0.0, 1.0);
            out.codec_ms += codec_ms * (transfer_failure * 1_000.0 / write_ms.max(1.0)).clamp(0.0, 1.0);
            out.wasted_work += (progress - durable_work).max(0.0);
            recover(s, peers, &manifest, policy, &mut durable_work, &mut progress, &mut latest_logical_bytes, &mut delta_count, &mut out, &mut rng);
            continue;
        }

        out.elapsed_ms += write_ms;
        out.traffic_mb += coded_mb(write_bytes, policy.strategy);
        out.codec_ms += codec_ms;
        durable_work = progress;
        if policy.delta && !compaction {
            delta_count += 1;
            latest_logical_bytes = latest_logical_bytes.saturating_add(DELTA_BYTES);
        } else {
            delta_count = 0;
            latest_logical_bytes = FULL_BYTES;
        }
        out.peak_storage_mb = out.peak_storage_mb.max(coded_mb(latest_logical_bytes, policy.strategy));
    }
    out.finished = progress >= TASK_WORK;
    out.capped = !out.finished && out.failures >= MAX_FAILURES_PER_TRIAL;
    out
}

fn recover(s: Scenario, peers: &[WorkerLink], manifest: &ArtifactManifest, policy: Policy,
    durable_work: &mut f64, progress: &mut f64, logical_bytes: &mut u64, delta_count: &mut u64,
    out: &mut Metrics, rng: &mut Rng) {
    out.failures += 1;
    let survivors = surviving_shards(peers, manifest, rng);
    let needed = policy.strategy.data_shards();
    if survivors.len() < needed {
        out.wasted_work += *durable_work;
        *durable_work = 0.0;
        *progress = 0.0;
        *logical_bytes = FULL_BYTES;
        *delta_count = 0;
        out.data_loss = true;
    } else {
        let restore_ms = restore_cost(s, &survivors[..needed], *logical_bytes, policy.strategy);
        out.elapsed_ms += restore_ms;
        out.codec_ms += if needed > 1 { *logical_bytes as f64 / MIB as f64 * DECODE_MS_PER_MB } else { 0.0 };
        *progress = *durable_work;
    }
}

fn surviving_shards<'a>(workers: &'a [WorkerLink], manifest: &ArtifactManifest, rng: &mut Rng) -> Vec<&'a WorkerLink> {
    let mut domain_down = std::collections::HashSet::new();
    let domains = manifest.shards.iter().map(|s| s.failure_domain.as_str()).collect::<std::collections::HashSet<_>>();
    for domain in domains {
        if rng.next() < 0.002 { domain_down.insert(domain); }
    }
    let mut survivors = manifest.shards.iter().filter_map(|shard| {
        let worker = workers.iter().find(|w| w.node_id == shard.node_id)?;
        let down = domain_down.contains(shard.failure_domain.as_str()) || rng.next() < 0.01;
        if down { None } else { Some(worker) }
    }).collect::<Vec<_>>();
    survivors.sort_by(|a, b| b.bandwidth_mbps.cmp(&a.bandwidth_mbps).then_with(|| a.rtt_ms.cmp(&b.rtt_ms)));
    survivors
}

fn write_cost(s: Scenario, peers: &[WorkerLink], manifest: &ArtifactManifest, logical_bytes: u64, strategy: StorageStrategy) -> (f64, f64) {
    let shard_bytes = logical_bytes.div_ceil(strategy.data_shards() as u64);
    let network = manifest.shards.iter().filter_map(|shard| peers.iter().find(|w| w.node_id == shard.node_id))
        .map(|w| transfer_ms(Scenario { bandwidth_mbps: w.bandwidth_mbps, rtt_ms: w.rtt_ms, ..s }, shard_bytes))
        .fold(0.0_f64, f64::max);
    let codec = if strategy.data_shards() == 1 { 0.0 } else {
        logical_bytes as f64 / MIB as f64 * ENCODE_MS_PER_MB_PARITY * (strategy.total_shards() - strategy.data_shards()) as f64
    };
    (network + codec, codec)
}

fn restore_cost(s: Scenario, peers: &[&WorkerLink], logical_bytes: u64, strategy: StorageStrategy) -> f64 {
    let shard_bytes = logical_bytes.div_ceil(strategy.data_shards() as u64);
    peers.iter().map(|w| transfer_ms(Scenario { bandwidth_mbps: w.bandwidth_mbps, rtt_ms: w.rtt_ms, ..s }, shard_bytes))
        .fold(0.0_f64, f64::max)
}

fn coded_mb(bytes: u64, strategy: StorageStrategy) -> f64 {
    bytes as f64 / MIB as f64 * strategy.total_shards() as f64 / strategy.data_shards() as f64
}

fn transfer_ms(s: Scenario, bytes: u64) -> f64 {
    s.rtt_ms as f64 + bytes as f64 * 8.0 / (s.bandwidth_mbps as f64 * 1_000.0)
}

fn worker_links(s: Scenario) -> Vec<WorkerLink> {
    let (base, rtt) = (s.bandwidth_mbps, s.rtt_ms);
    vec![
        WorkerLink { node_id: format!("{}-a", s.name), failure_domain: "home-a".into(), bandwidth_mbps: base, rtt_ms: rtt },
        WorkerLink { node_id: format!("{}-b", s.name), failure_domain: "home-a".into(), bandwidth_mbps: base.saturating_sub(4).max(1), rtt_ms: rtt + 5 },
        WorkerLink { node_id: format!("{}-c", s.name), failure_domain: "home-c".into(), bandwidth_mbps: base.saturating_sub(6).max(1), rtt_ms: rtt + 10 },
        WorkerLink { node_id: format!("{}-d", s.name), failure_domain: "home-d".into(), bandwidth_mbps: base.saturating_sub(8).max(1), rtt_ms: rtt + 15 },
    ]
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seeded_stream_is_reproducible_and_bounded() {
        let mut a = Rng(42); let mut b = Rng(42);
        for _ in 0..10 { assert_eq!(a.next(), b.next()); }
    }
    #[test]
    fn placements_meet_each_strategy_shard_requirement() {
        let peers = worker_links(SCENARIOS[0]);
        for strategy in [StorageStrategy::Replication2x, StorageStrategy::Erasure2Plus1, StorageStrategy::Erasure2Plus2] {
            let manifest = build_manifest("test".into(), 1, None, FULL_BYTES, FULL_BYTES, strategy, &peers).unwrap();
            assert_eq!(manifest.shards.len(), strategy.total_shards());
        }
    }
}

