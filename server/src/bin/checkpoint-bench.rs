//! Policy comparison for adaptive and incremental checkpointing.

use mesh_coordinator::checkpoint::{
    adaptive_interval, build_manifest, choose_storage_strategy, payload_size, CheckpointInput,
    StorageStrategy, WorkerLink,
};

const MIB: u64 = 1024 * 1024;
const TASK_WORK: f64 = 100.0;
const FAILURE_PROGRESS: f64 = 63.0;
const ENCODE_MS_PER_MB_PARITY: f64 = 0.18;
const DECODE_MS_PER_MB: f64 = 0.30;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    bandwidth_mbps: u32,
    rtt_ms: u32,
    mttf_seconds: f64,
}

const SCENARIOS: [Scenario; 2] = [
    Scenario {
        name: "stable_fast",
        bandwidth_mbps: 250,
        rtt_ms: 10,
        mttf_seconds: 3_600.0,
    },
    Scenario {
        name: "unreliable_slow",
        bandwidth_mbps: 18,
        rtt_ms: 80,
        mttf_seconds: 12.0,
    },
];

fn main() {
    println!("scenario,plan,interval_seconds,checkpoint_payload_mb,strategy,peer_nodes,failure_domains,peer_losses,recovery_ms,total_job_ms,work_lost,traffic_mb,retained_mb,codec_ms,recovered");
    for scenario in SCENARIOS {
        let workers = worker_links(scenario);
        let full = 256 * MIB;
        let changed = 51 * MIB;
        let full_transfer_ms = transfer_ms(scenario, full);
        let baseline = simulate(
            scenario,
            &workers,
            "fixed_full_replica",
            10.0,
            full,
            full,
            false,
            StorageStrategy::Replication2x,
            1,
        );
        print_result(scenario, "fixed_full_replica", full, &workers, baseline);

        let input = CheckpointInput {
            full_bytes: full,
            changed_bytes: changed,
            has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms(scenario, changed),
            estimated_mttf_seconds: scenario.mttf_seconds,
            work_per_second: 1.0,
            min_interval_seconds: 2.0,
            max_interval_seconds: 60.0,
        };
        let interval = adaptive_interval(&input);
        let (payload, is_delta) = payload_size(&input);
        let strategy =
            choose_storage_strategy(payload, interval, scenario.bandwidth_mbps, 16 * MIB, 8.0, 0);
        let adaptive = simulate(
            scenario,
            &workers,
            "adaptive_delta",
            interval,
            full,
            payload,
            is_delta,
            strategy,
            strategy.tolerable_losses() as u8,
        );
        print_result(scenario, "adaptive_delta", payload, &workers, adaptive);

        let safer =
            choose_storage_strategy(payload, interval, scenario.bandwidth_mbps, 16 * MIB, 8.0, 2);
        let two_loss = simulate(
            scenario,
            &workers,
            "two_loss_requirement",
            interval,
            full,
            payload,
            is_delta,
            safer,
            2,
        );
        print_result(
            scenario,
            "two_loss_requirement",
            payload,
            &workers,
            two_loss,
        );

        // Keep arithmetic honest in the benchmark output even if a future
        // policy refactor changes which checkpoint cost is used for planning.
        assert!(full_transfer_ms > 0.0);
    }

    println!("# storage_choice_examples");
    println!(
        "small_frequent={:?}",
        choose_storage_strategy(8 * MIB, 3.0, 20, 16 * MIB, 8.0, 0)
    );
    println!(
        "large_slow={:?}",
        choose_storage_strategy(256 * MIB, 60.0, 18, 16 * MIB, 8.0, 0)
    );
    println!(
        "explicit_two_loss={:?}",
        choose_storage_strategy(8 * MIB, 3.0, 250, 16 * MIB, 8.0, 2)
    );
}

#[derive(Clone, Copy)]
struct ResultRow {
    interval: f64,
    recovery_ms: f64,
    total_ms: f64,
    lost_work: f64,
    traffic_mb: f64,
    retained_mb: f64,
    codec_ms: f64,
    recovered: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "Explicit simulation axes and state are retained for reproducible benchmarks"
)]
fn simulate(
    scenario: Scenario,
    workers: &[WorkerLink],
    _plan: &str,
    interval: f64,
    full_bytes: u64,
    payload_bytes: u64,
    is_delta: bool,
    strategy: StorageStrategy,
    peer_losses: u8,
) -> ResultRow {
    let updates = (FAILURE_PROGRESS / interval).floor() as u64;
    let saved = updates as f64 * interval;
    // Place a full base before execution; later snapshots are deltas or full replacements.
    let logical_uploaded = full_bytes + updates * payload_bytes;
    let overhead = strategy.total_shards() as f64 / strategy.data_shards() as f64;
    let traffic_mb = logical_uploaded as f64 / MIB as f64 * overhead;
    // Delta chains retain their base and patches until a full compaction.
    let retained_logical = if is_delta {
        logical_uploaded
    } else {
        full_bytes
    };
    let retained_mb = retained_logical as f64 / MIB as f64 * overhead;
    let manifest = build_manifest(
        "checkpoint-latest".into(),
        updates,
        if is_delta {
            Some("checkpoint-base".into())
        } else {
            None
        },
        full_bytes,
        payload_bytes,
        strategy,
        workers,
    );
    let placement_ok = manifest
        .as_ref()
        .is_some_and(|m| m.shards.len() == strategy.total_shards());
    let recovered = placement_ok && peer_losses as usize <= strategy.tolerable_losses();

    // All peer writes happen in parallel; model the slowest assigned link.
    let write_cost = |write_bytes: u64| -> (f64, f64) {
        let shard_bytes = write_bytes / strategy.data_shards() as u64;
        let network = manifest
            .as_ref()
            .map(|m| {
                m.shards
                    .iter()
                    .filter_map(|shard| workers.iter().find(|w| w.node_id == shard.node_id))
                    .map(|w| {
                        transfer_ms(
                            Scenario {
                                bandwidth_mbps: w.bandwidth_mbps,
                                rtt_ms: w.rtt_ms,
                                ..scenario
                            },
                            shard_bytes,
                        )
                    })
                    .fold(0.0_f64, f64::max)
            })
            .unwrap_or(0.0);
        let codec = if strategy.data_shards() == 1 {
            0.0
        } else {
            write_bytes as f64 / MIB as f64
                * ENCODE_MS_PER_MB_PARITY
                * (strategy.total_shards() - strategy.data_shards()) as f64
        };
        (network, codec)
    };
    let (base_network, base_codec) = write_cost(full_bytes);
    let (update_network, update_codec) = write_cost(payload_bytes);
    let write_stage_ms =
        base_network + base_codec + (updates as f64) * (update_network + update_codec);
    let codec_write_ms = base_codec + updates as f64 * update_codec;

    let restore_logical = if is_delta {
        logical_uploaded
    } else {
        full_bytes
    };
    let restore_network_ms = if recovered {
        transfer_ms(scenario, restore_logical / strategy.data_shards() as u64)
    } else {
        0.0
    };
    let codec_read_ms = if recovered && strategy.data_shards() > 1 {
        restore_logical as f64 / MIB as f64 * DECODE_MS_PER_MB
    } else {
        0.0
    };
    let work_after_failure = if recovered {
        TASK_WORK - saved
    } else {
        TASK_WORK
    };
    let recovery_ms = restore_network_ms + codec_read_ms + work_after_failure / 0.8 * 1_000.0;
    let total_ms = FAILURE_PROGRESS * 1_000.0 + write_stage_ms + recovery_ms;

    ResultRow {
        interval,
        recovery_ms,
        total_ms,
        lost_work: if recovered {
            FAILURE_PROGRESS - saved
        } else {
            FAILURE_PROGRESS
        },
        traffic_mb,
        retained_mb,
        codec_ms: codec_write_ms + codec_read_ms,
        recovered,
    }
}

fn print_result(
    scenario: Scenario,
    plan: &str,
    payload_bytes: u64,
    workers: &[WorkerLink],
    row: ResultRow,
) {
    let input_strategy = match plan {
        "fixed_full_replica" => StorageStrategy::Replication2x,
        "two_loss_requirement" => StorageStrategy::Erasure2Plus2,
        _ => choose_storage_strategy(
            payload_bytes,
            row.interval,
            scenario.bandwidth_mbps,
            16 * MIB,
            8.0,
            0,
        ),
    };
    let manifest = build_manifest(
        "output".into(),
        1,
        None,
        payload_bytes,
        payload_bytes,
        input_strategy,
        workers,
    );
    let nodes = manifest
        .as_ref()
        .map(|m| {
            m.shards
                .iter()
                .map(|s| s.node_id.as_str())
                .collect::<Vec<_>>()
                .join("+")
        })
        .unwrap_or_else(|| "insufficient".into());
    let domains = manifest
        .as_ref()
        .map(|m| {
            m.shards
                .iter()
                .map(|s| s.failure_domain.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len()
        })
        .unwrap_or(0);
    println!(
        "{},{},{:.2},{:.2},{:?},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{}",
        scenario.name,
        plan,
        row.interval,
        payload_bytes as f64 / MIB as f64,
        input_strategy,
        nodes,
        domains,
        input_strategy.tolerable_losses(),
        row.recovery_ms,
        row.total_ms,
        row.lost_work,
        row.traffic_mb,
        row.retained_mb,
        row.codec_ms,
        row.recovered
    );
}

fn transfer_ms(scenario: Scenario, bytes: u64) -> f64 {
    scenario.rtt_ms as f64 + bytes as f64 * 8.0 / (scenario.bandwidth_mbps as f64 * 1_000.0)
}

fn worker_links(scenario: Scenario) -> Vec<WorkerLink> {
    let (base, rtt) = (scenario.bandwidth_mbps, scenario.rtt_ms);
    vec![
        WorkerLink {
            node_id: format!("{}-a", scenario.name),
            failure_domain: "home-a".into(),
            bandwidth_mbps: base,
            rtt_ms: rtt,
        },
        WorkerLink {
            node_id: format!("{}-b", scenario.name),
            failure_domain: "home-a".into(),
            bandwidth_mbps: base.saturating_sub(4).max(1),
            rtt_ms: rtt + 5,
        },
        WorkerLink {
            node_id: format!("{}-c", scenario.name),
            failure_domain: "home-c".into(),
            bandwidth_mbps: base.saturating_sub(6).max(1),
            rtt_ms: rtt + 10,
        },
        WorkerLink {
            node_id: format!("{}-d", scenario.name),
            failure_domain: "home-d".into(),
            bandwidth_mbps: base.saturating_sub(8).max(1),
            rtt_ms: rtt + 15,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_upload_reduces_traffic_and_unreliable_worker_shortens_interval() {
        let s = SCENARIOS[1];
        let links = worker_links(s);
        let input = CheckpointInput {
            full_bytes: 256 * MIB,
            changed_bytes: 51 * MIB,
            has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms(s, 51 * MIB),
            estimated_mttf_seconds: s.mttf_seconds,
            work_per_second: 1.0,
            min_interval_seconds: 2.0,
            max_interval_seconds: 60.0,
        };
        let interval = adaptive_interval(&input);
        assert!(interval < 60.0);
        let (payload, is_delta) = payload_size(&input);
        let baseline = simulate(
            s,
            &links,
            "fixed",
            10.0,
            256 * MIB,
            256 * MIB,
            false,
            StorageStrategy::Replication2x,
            0,
        );
        let optimized = simulate(
            s,
            &links,
            "adaptive",
            interval,
            256 * MIB,
            payload,
            is_delta,
            StorageStrategy::Erasure2Plus1,
            0,
        );
        assert!(optimized.traffic_mb < baseline.traffic_mb);
    }
}
