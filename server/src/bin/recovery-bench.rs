//! Deterministic discrete-event recovery simulator.
//!
//! The simulator models heterogeneous workers, checkpoint traffic, peer loss,
//! and reconstruction. It reports modeled wall-clock values; hardware runs are
//! still needed to calibrate the worker and codec rates.

use mesh_coordinator::recovery::{RecoveryLedger, ResultCommit};

const TASK_WORK: f64 = 100.0;
const CHECKPOINT_MB: f64 = 64.0;
const PRIMARY_WORK_PER_SEC: f64 = 1.0;
const REPLACEMENT_WORK_PER_SEC: f64 = 0.8;
const CHECKPOINT_INTERVALS: [f64; 3] = [10.0, 25.0, 50.0];
const FAILURE_POINTS: [f64; 3] = [25.0, 63.0, 99.0];
const CODEC_ENCODE_MS_PER_MB_PER_PARITY: f64 = 0.18;
const CODEC_DECODE_MS_PER_MB: f64 = 0.30;

#[derive(Clone, Copy)]
struct Network {
    name: &'static str,
    bandwidth_mbps: f64,
    latency_ms: f64,
}

const NETWORKS: [Network; 3] = [
    Network {
        name: "lan",
        bandwidth_mbps: 800.0,
        latency_ms: 2.0,
    },
    Network {
        name: "broadband",
        bandwidth_mbps: 80.0,
        latency_ms: 25.0,
    },
    Network {
        name: "constrained",
        bandwidth_mbps: 12.0,
        latency_ms: 90.0,
    },
];

#[derive(Clone, Copy)]
struct Outcome {
    recovery_ms: f64,
    total_ms: f64,
    lost_work: f64,
    transfer_mb: f64,
    storage_mb: f64,
    codec_ms: f64,
    recoverable_peer_losses: u8,
    success: bool,
}

fn transfer_ms(network: Network, megabytes: f64) -> f64 {
    if megabytes <= 0.0 {
        0.0
    } else {
        network.latency_ms + megabytes * 8_000.0 / network.bandwidth_mbps
    }
}

fn fmt(value: f64) -> String {
    format!("{value:.2}")
}

fn main() {
    // Keep the real lease/fencing primitive in the benchmark's smoke path.
    let mut ledger = RecoveryLedger::default();
    ledger.add_task("simulator-task").expect("add task");
    let lease = ledger
        .claim("simulator-task", "worker-a", 0, 10)
        .expect("claim task");
    assert_eq!(
        ledger.commit_result(&lease, 1, "sha256:accepted"),
        Ok(ResultCommit::Accepted)
    );
    assert_eq!(
        ledger.commit_result(&lease, 2, "sha256:accepted"),
        Ok(ResultCommit::Duplicate)
    );

    println!("network,policy,failure_progress_pct,checkpoint_interval_work,peer_losses,max_tolerated_peer_losses,recovery_ms,total_job_ms,lost_work_units,transfer_mb,stored_mb,codec_ms,recovers");
    for network in NETWORKS {
        for failure_pct in FAILURE_POINTS {
            for interval in CHECKPOINT_INTERVALS {
                for (policy, parity) in [
                    ("retry", 0_u8),
                    ("replica_2x", 0),
                    ("ec_2_1", 1),
                    ("ec_2_2", 2),
                ] {
                    let max_peer_losses = if policy == "replica_2x" { 1 } else { parity };
                    for peer_losses in 0..=max_peer_losses {
                        let outcome =
                            simulate(network, policy, failure_pct, interval, peer_losses, parity);
                        println!(
                            "{},{},{},{:.0},{},{},{},{},{},{},{},{},{}",
                            network.name,
                            policy,
                            failure_pct,
                            interval,
                            peer_losses,
                            outcome.recoverable_peer_losses,
                            fmt(outcome.recovery_ms),
                            fmt(outcome.total_ms),
                            fmt(outcome.lost_work),
                            fmt(outcome.transfer_mb),
                            fmt(outcome.storage_mb),
                            fmt(outcome.codec_ms),
                            outcome.success
                        );
                    }
                }
            }
        }
    }
}

fn simulate(
    network: Network,
    policy: &str,
    failure_pct: f64,
    interval: f64,
    peer_losses: u8,
    parity: u8,
) -> Outcome {
    let progress_at_failure = TASK_WORK * failure_pct / 100.0;
    let saved_progress = if policy == "retry" {
        0.0
    } else {
        (progress_at_failure / interval).floor() * interval
    };
    let checkpoints = (saved_progress / interval).floor();
    let has_checkpoint = checkpoints > 0.0;
    let lost_work = progress_at_failure - saved_progress;
    // Checkpoint writes run in parallel to peers; accumulated write latency is
    // part of pre-failure wall time. Retry has no checkpoint traffic.
    let checkpoint_peers = match policy {
        "replica_2x" => 2_u8,
        "ec_2_1" => 3,
        "ec_2_2" => 4,
        _ => 0,
    };
    let shard_mb = if parity == 0 {
        CHECKPOINT_MB
    } else {
        CHECKPOINT_MB / 2.0
    };
    let write_per_checkpoint = if checkpoint_peers == 0 || !has_checkpoint {
        0.0
    } else {
        transfer_ms(network, shard_mb)
            + if parity > 0 {
                CHECKPOINT_MB * CODEC_ENCODE_MS_PER_MB_PER_PARITY * f64::from(parity)
            } else {
                0.0
            }
    };
    let write_elapsed = checkpoints * write_per_checkpoint;
    let storage_mb = if has_checkpoint {
        match policy {
            "replica_2x" => CHECKPOINT_MB * 2.0,
            "ec_2_1" => CHECKPOINT_MB * 1.5,
            "ec_2_2" => CHECKPOINT_MB * 2.0,
            _ => 0.0,
        }
    } else {
        0.0
    };
    let transfer_mb = if has_checkpoint {
        checkpoints * storage_mb
    } else {
        0.0
    };
    let codec_write_ms = if parity > 0 && has_checkpoint {
        checkpoints * CHECKPOINT_MB * CODEC_ENCODE_MS_PER_MB_PER_PARITY * f64::from(parity)
    } else {
        0.0
    };
    let mut success = true;
    let mut restore_ms = 0.0;
    let mut codec_read_ms = 0.0;
    let mut restore_transfer_mb = 0.0;
    if has_checkpoint && policy != "retry" {
        success = peer_losses <= if policy == "replica_2x" { 1 } else { parity };
        if success {
            restore_transfer_mb = CHECKPOINT_MB;
            restore_ms = transfer_ms(
                network,
                if parity == 0 {
                    CHECKPOINT_MB
                } else {
                    shard_mb * 2.0
                },
            );
            if parity > 0 {
                codec_read_ms = CHECKPOINT_MB * CODEC_DECODE_MS_PER_MB;
            }
        }
    }
    let post_failure_work = if success {
        TASK_WORK - saved_progress
    } else {
        TASK_WORK
    };
    let recovery_ms = if success {
        restore_ms + codec_read_ms + post_failure_work / REPLACEMENT_WORK_PER_SEC * 1_000.0
    } else {
        TASK_WORK / REPLACEMENT_WORK_PER_SEC * 1_000.0
    };
    let pre_failure_ms = progress_at_failure / PRIMARY_WORK_PER_SEC * 1_000.0 + write_elapsed;
    Outcome {
        recovery_ms,
        total_ms: pre_failure_ms + recovery_ms,
        lost_work: if success {
            lost_work
        } else {
            progress_at_failure
        },
        transfer_mb: transfer_mb + restore_transfer_mb,
        storage_mb,
        codec_ms: codec_write_ms + codec_read_ms,
        recoverable_peer_losses: if has_checkpoint {
            if policy == "replica_2x" {
                1
            } else {
                parity
            }
        } else {
            0
        },
        success,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn erasure_coding_recovers_with_parity_but_fails_beyond_it() {
        let network = NETWORKS[1];
        let one = simulate(network, "ec_2_1", 63.0, 10.0, 1, 1);
        let two = simulate(network, "ec_2_2", 63.0, 10.0, 2, 2);
        let too_many = simulate(network, "ec_2_1", 63.0, 10.0, 2, 1);
        assert!(one.success && two.success);
        assert!(!too_many.success);
    }

    #[test]
    fn checkpoint_interval_changes_rework_and_transfer_cost() {
        let network = NETWORKS[1];
        let frequent = simulate(network, "replica_2x", 63.0, 10.0, 0, 0);
        let sparse = simulate(network, "replica_2x", 63.0, 50.0, 0, 0);
        assert!(frequent.lost_work < sparse.lost_work);
        assert!(frequent.total_ms > sparse.total_ms); // more frequent durable writes
    }

    #[test]
    fn transfer_and_recovery_time_respond_to_network_profile() {
        let fast = simulate(NETWORKS[0], "ec_2_1", 63.0, 10.0, 0, 1);
        let slow = simulate(NETWORKS[2], "ec_2_1", 63.0, 10.0, 0, 1);
        assert!(slow.recovery_ms > fast.recovery_ms);
        assert_eq!(fast.storage_mb, slow.storage_mb);
    }
}
