//! Compare fixed full snapshots with adaptive, incremental checkpoint plans.
#[path = "../checkpoint.rs"]
mod checkpoint;
use checkpoint::{adaptive_interval, build_manifest, choose_storage_strategy, payload_size, CheckpointInput, StorageStrategy, WorkerLink};

const MIB: u64 = 1024 * 1024;
const TASK_WORK: f64 = 100.0;
const FAILURE_PROGRESS: f64 = 63.0;
const ENCODE_MS_PER_MB_PARITY: f64 = 0.18;
const DECODE_MS_PER_MB: f64 = 0.30;

#[derive(Clone, Copy)]
struct Scenario { name: &'static str, bandwidth_mbps: u32, rtt_ms: u32, mttf_seconds: f64 }
const SCENARIOS: [Scenario; 2] = [
    Scenario { name: "stable_fast", bandwidth_mbps: 250, rtt_ms: 10, mttf_seconds: 3_600.0 },
    Scenario { name: "unreliable_slow", bandwidth_mbps: 18, rtt_ms: 80, mttf_seconds: 30.0 },
];

fn main() {
    println!("scenario,plan,interval_seconds,payload_mb,strategy,peer_nodes,failure_domains,peer_losses,recovery_ms,total_job_ms,work_lost,traffic_mb,retained_mb,codec_ms,recovered");
    for s in SCENARIOS {
        let workers = worker_links(s);
        let full = 256 * MIB;
        let changed = 51 * MIB;
        let baseline = simulate(s, &workers, 10.0, full, full, false, StorageStrategy::Replication2x, 1);
        print_result(s, "fixed_full_replica", full, &workers, baseline);

        let input = CheckpointInput { full_bytes: full, changed_bytes: changed, has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms(s, changed), estimated_mttf_seconds: s.mttf_seconds,
            work_per_second: 1.0, min_interval_seconds: 2.0, max_interval_seconds: 60.0 };
        let interval = adaptive_interval(&input);
        let (payload, is_delta) = payload_size(&input);
        let strategy = choose_storage_strategy(payload, interval, s.bandwidth_mbps, 16 * MIB, 8.0, 0);
        let adaptive = simulate(s, &workers, interval, full, payload, is_delta, strategy, strategy.tolerable_losses() as u8);
        print_result(s, "adaptive_delta", payload, &workers, adaptive);

        let safer = choose_storage_strategy(payload, interval, s.bandwidth_mbps, 16 * MIB, 8.0, 2);
        let two_loss = simulate(s, &workers, interval, full, payload, is_delta, safer, 2);
        print_result(s, "two_loss_requirement", payload, &workers, two_loss);
    }
    println!("# storage_choice_examples");
    println!("small_frequent={:?}", choose_storage_strategy(8 * MIB, 3.0, 20, 16 * MIB, 8.0, 0));
    println!("large_slow={:?}", choose_storage_strategy(256 * MIB, 60.0, 18, 16 * MIB, 8.0, 0));
    println!("explicit_two_loss={:?}", choose_storage_strategy(8 * MIB, 3.0, 250, 16 * MIB, 8.0, 2));
}

#[derive(Clone, Copy)]
struct Row { interval: f64, recovery_ms: f64, total_ms: f64, lost_work: f64, traffic_mb: f64, retained_mb: f64, codec_ms: f64, recovered: bool }

fn simulate(s: Scenario, workers: &[WorkerLink], interval: f64, full: u64, payload: u64, is_delta: bool, strategy: StorageStrategy, peer_losses: u8) -> Row {
    let saved = (FAILURE_PROGRESS / interval).floor() * interval;
    let n = (saved / interval).floor() as u64;
    let logical_uploaded = if n == 0 { 0 } else if is_delta { full + n.saturating_sub(1) * payload } else { n * payload };
    let overhead = strategy.total_shards() as f64 / strategy.data_shards() as f64;
    let traffic_mb = logical_uploaded as f64 / MIB as f64 * overhead;
    let retained_mb = if n == 0 { 0.0 } else { full as f64 / MIB as f64 * overhead };
    let manifest = if n > 0 {
        build_manifest("checkpoint-latest".into(), n, if is_delta { Some("checkpoint-base".into()) } else { None }, full, payload, strategy, workers)
    } else { None };
    let placement_ok = manifest.as_ref().is_some_and(|m| m.shards.len() == strategy.total_shards());
    let recovered = n == 0 || (placement_ok && peer_losses as usize <= strategy.tolerable_losses());

    let write_cost = |bytes: u64| -> (f64, f64) {
        let shard_bytes = bytes / strategy.data_shards() as u64;
        let network = manifest.as_ref().map(|m| m.shards.iter().filter_map(|shard| workers.iter().find(|w| w.node_id == shard.node_id))
            .map(|w| transfer_ms(Scenario { bandwidth_mbps: w.bandwidth_mbps, rtt_ms: w.rtt_ms, ..s }, shard_bytes)).fold(0.0_f64, f64::max)).unwrap_or(0.0);
        let codec = if strategy.data_shards() == 1 { 0.0 } else { bytes as f64 / MIB as f64 * ENCODE_MS_PER_MB_PARITY * (strategy.total_shards() - strategy.data_shards()) as f64 };
        (network, codec)
    };
    let (first_net, first_codec) = if n > 0 { write_cost(if is_delta { full } else { payload }) } else { (0.0, 0.0) };
    let (delta_net, delta_codec) = if n > 1 { write_cost(payload) } else { (0.0, 0.0) };
    let write_ms = first_net + first_codec + n.saturating_sub(1) as f64 * (delta_net + delta_codec);
    let codec_write = first_codec + n.saturating_sub(1) as f64 * delta_codec;
    let restore_net = if n > 0 { transfer_ms(s, full / strategy.data_shards() as u64) } else { 0.0 };
    let codec_read = if n > 0 && strategy.data_shards() > 1 { full as f64 / MIB as f64 * DECODE_MS_PER_MB } else { 0.0 };
    let remaining = if recovered { TASK_WORK - saved } else { TASK_WORK };
    let recovery = restore_net + codec_read + remaining / 0.8 * 1_000.0;
    Row { interval, recovery_ms: recovery, total_ms: FAILURE_PROGRESS * 1_000.0 + write_ms + recovery,
        lost_work: if recovered { FAILURE_PROGRESS - saved } else { FAILURE_PROGRESS }, traffic_mb,
        retained_mb, codec_ms: codec_write + codec_read, recovered }
}

fn print_result(s: Scenario, name: &str, payload: u64, workers: &[WorkerLink], row: Row) {
    let strategy = match name { "fixed_full_replica" => StorageStrategy::Replication2x, "two_loss_requirement" => StorageStrategy::Erasure2Plus2,
        _ => choose_storage_strategy(payload, row.interval, s.bandwidth_mbps, 16 * MIB, 8.0, 0) };
    let manifest = build_manifest("output".into(), 1, None, payload, payload, strategy, workers);
    let nodes = manifest.as_ref().map(|m| m.shards.iter().map(|x| x.node_id.as_str()).collect::<Vec<_>>().join("+")).unwrap_or_else(|| "insufficient".into());
    let domains = manifest.as_ref().map(|m| m.shards.iter().map(|x| x.failure_domain.as_str()).collect::<std::collections::HashSet<_>>().len()).unwrap_or(0);
    println!("{},{},{:.2},{:.2},{:?},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{}", s.name, name, row.interval,
        payload as f64 / MIB as f64, strategy, nodes, domains, strategy.tolerable_losses(), row.recovery_ms, row.total_ms,
        row.lost_work, row.traffic_mb, row.retained_mb, row.codec_ms, row.recovered);
}
fn transfer_ms(s: Scenario, bytes: u64) -> f64 { s.rtt_ms as f64 + bytes as f64 * 8.0 / (s.bandwidth_mbps as f64 * 1_000.0) }
fn worker_links(s: Scenario) -> Vec<WorkerLink> {
    let (base, rtt) = (s.bandwidth_mbps, s.rtt_ms);
    vec![
        WorkerLink { node_id: format!("{}-a", s.name), failure_domain: "home-a".into(), bandwidth_mbps: base, rtt_ms: rtt },
        WorkerLink { node_id: format!("{}-b", s.name), failure_domain: "home-a".into(), bandwidth_mbps: base.saturating_sub(4).max(1), rtt_ms: rtt + 5 },
        WorkerLink { node_id: format!("{}-c", s.name), failure_domain: "home-c".into(), bandwidth_mbps: base.saturating_sub(6).max(1), rtt_ms: rtt + 10 },
        WorkerLink { node_id: format!("{}-d", s.name), failure_domain: "home-d".into(), bandwidth_mbps: base.saturating_sub(8).max(1), rtt_ms: rtt + 15 },
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delta_upload_reduces_traffic_and_unreliable_worker_shortens_interval() {
        let s = SCENARIOS[1]; let links = worker_links(s);
        let input = CheckpointInput { full_bytes: 256 * MIB, changed_bytes: 51 * MIB, has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms(s, 51 * MIB), estimated_mttf_seconds: s.mttf_seconds,
            work_per_second: 1.0, min_interval_seconds: 2.0, max_interval_seconds: 60.0 };
        let interval = adaptive_interval(&input); assert!(interval < 60.0);
        let (payload, delta) = payload_size(&input);
        let baseline = simulate(s, &links, 10.0, 256 * MIB, 256 * MIB, false, StorageStrategy::Replication2x, 0);
        let optimized = simulate(s, &links, interval, 256 * MIB, payload, delta, StorageStrategy::Erasure2Plus1, 0);
        assert!(optimized.traffic_mb < baseline.traffic_mb);
    }
}
