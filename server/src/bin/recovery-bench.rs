//! Deterministic model benchmark for Mesh recovery policies.
//!
//! This compares logical recovery/transfer costs under identical failures. It is
//! not a hardware throughput benchmark; use real workers before choosing policy.

#[path = "../recovery.rs"]
mod recovery;

use recovery::{RecoveryLedger, ResultCommit};

const TASK_WORK: u64 = 100;
const CHECKPOINT_SIZE_UNITS: u64 = 20;
const FAILURE_POINTS: [u64; 3] = [25, 63, 99];
const CHECKPOINT_INTERVALS: [u64; 3] = [10, 25, 50];
const EVENT_BYTES: u64 = 256;

fn print_header() {
    println!("failure_at,method,checkpoint_interval,work_lost,post_failure_work,extra_compute,artifact_storage_units,network_units,metadata_bytes,max_artifact_node_losses");
}

fn print_row(
    failure_at: u64,
    method: &str,
    checkpoint_interval: u64,
    work_lost: u64,
    post_failure_work: u64,
    extra_compute: u64,
    artifact_storage_units: u64,
    network_units: u64,
    metadata_bytes: u64,
    max_artifact_node_losses: u64,
) {
    println!("{failure_at},{method},{checkpoint_interval},{work_lost},{post_failure_work},{extra_compute},{artifact_storage_units},{network_units},{metadata_bytes},{max_artifact_node_losses}");
}

fn main() {
    // Smoke the exact lease/fencing implementation before generating comparison rows.
    let mut ledger = RecoveryLedger::default();
    ledger.add_task("benchmark-task").expect("add benchmark task");
    let lease = ledger.claim("benchmark-task", "worker-a", 0, 10).expect("claim benchmark task");
    assert_eq!(ledger.commit_result(&lease, 1, "sha256:accepted"), Ok(ResultCommit::Accepted));
    assert_eq!(ledger.commit_result(&lease, 2, "sha256:accepted"), Ok(ResultCommit::Duplicate));

    print_header();
    for failure_at in FAILURE_POINTS {
        for checkpoint_interval in CHECKPOINT_INTERVALS {
            // Lease + retry: the failed worker's completed prefix must be rerun.
            print_row(failure_at, "lease_retry", checkpoint_interval, failure_at, TASK_WORK, 0, 0, 0, 0, 0);

            // Duplicate: assume an equally fast independent copy was already
            // running. It finishes after the primary drops; compute doubles.
            print_row(failure_at, "duplicate_first_result", checkpoint_interval, 0, TASK_WORK - failure_at, TASK_WORK, 0, 0, 0, 0);

            // A peer holds the latest complete checkpoint (two full copies total).
            let saved_at = failure_at / checkpoint_interval * checkpoint_interval;
            let missed_since_checkpoint = failure_at - saved_at;
            let checkpoints_written = saved_at / checkpoint_interval;
            let replicate_network = if saved_at > 0 {
                checkpoints_written * CHECKPOINT_SIZE_UNITS + CHECKPOINT_SIZE_UNITS
            } else {
                0
            };
            print_row(
                failure_at,
                "checkpoint_replication_2x",
                checkpoint_interval,
                missed_since_checkpoint,
                TASK_WORK - saved_at,
                0,
                if saved_at > 0 { CHECKPOINT_SIZE_UNITS * 2 } else { 0 },
                replicate_network,
                0,
                if saved_at > 0 { 1 } else { 0 },
            );

            // Reed-Solomon-like artifact model: k=2 with m=1 or m=2. Checkpoint
            // work recovered is the same; this row models stored bytes and shard
            // fetch volume, not codec CPU time or peer transfer throughput.
            for (parity_shards, label) in [(1_u64, "artifact_ec_2_1"), (2, "artifact_ec_2_2")] {
                let total_shards = 2 + parity_shards;
                let storage = if saved_at > 0 { CHECKPOINT_SIZE_UNITS * total_shards / 2 } else { 0 };
                let write_network = storage;
                let read_network = if saved_at > 0 { CHECKPOINT_SIZE_UNITS } else { 0 };
                print_row(
                    failure_at,
                    label,
                    checkpoint_interval,
                    missed_since_checkpoint,
                    TASK_WORK - saved_at,
                    0,
                    storage,
                    write_network + read_network,
                    0,
                    if saved_at > 0 { parity_shards } else { 0 },
                );
            }

            // Lattice-inspired log keeps replicated metadata, not task state or
            // checkpoints. It helps reconstruct history but cannot save compute.
            let events = 3_u64; // assigned, checkpoint/result offered, coordinator acceptance
            let metadata = events * EVENT_BYTES * 3; // coordinator + two peers
            print_row(failure_at, "signed_event_log_3x", checkpoint_interval, failure_at, TASK_WORK, 0, 0, 0, metadata, 0);
        }
    }
}

