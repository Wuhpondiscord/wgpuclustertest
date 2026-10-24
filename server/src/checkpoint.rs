//! Checkpoint policy and peer placement primitives.
//!
//! This module plans metadata only. Checkpoint bytes stay with workers and move
//! directly between peers; the coordinator should retain leases and manifests.

#[derive(Clone, Debug)]
pub struct CheckpointInput {
    pub full_bytes: u64,
    pub changed_bytes: u64,
    pub has_base_checkpoint: bool,
    pub observed_transfer_ms: f64,
    pub estimated_mttf_seconds: f64,
    pub work_per_second: f64,
    pub min_interval_seconds: f64,
    pub max_interval_seconds: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageStrategy { Replication2x, Erasure2Plus1, Erasure2Plus2 }

impl StorageStrategy {
    pub fn data_shards(self) -> usize {
        match self { Self::Replication2x => 1, Self::Erasure2Plus1 | Self::Erasure2Plus2 => 2 }
    }
    pub fn total_shards(self) -> usize {
        match self { Self::Replication2x => 2, Self::Erasure2Plus1 => 3, Self::Erasure2Plus2 => 4 }
    }
    pub fn tolerable_losses(self) -> usize {
        match self { Self::Replication2x | Self::Erasure2Plus1 => 1, Self::Erasure2Plus2 => 2 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLink {
    pub node_id: String,
    pub failure_domain: String,
    pub bandwidth_mbps: u32,
    pub rtt_ms: u32,
}

#[derive(Clone, Debug)]
pub struct CheckpointPlan {
    pub interval_seconds: f64,
    pub interval_work_units: f64,
    pub payload_bytes: u64,
    pub is_delta: bool,
    pub strategy: StorageStrategy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactShard {
    pub index: usize,
    pub node_id: String,
    pub failure_domain: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub checkpoint_id: String,
    pub generation: u64,
    pub base_checkpoint_id: Option<String>,
    pub logical_bytes: u64,
    pub payload_bytes: u64,
    pub strategy: StorageStrategy,
    pub shards: Vec<ArtifactShard>,
}

/// Young/Daly-style interval estimate using observed checkpoint wall time and
/// estimated mean time to failure, bounded for product-level safety limits.
pub fn adaptive_interval(input: &CheckpointInput) -> f64 {
    let min = input.min_interval_seconds.max(0.1);
    let max = input.max_interval_seconds.max(min);
    let checkpoint_cost = (input.observed_transfer_ms / 1_000.0).max(0.001);
    let mttf = input.estimated_mttf_seconds.max(checkpoint_cost);
    (2.0 * checkpoint_cost * mttf).sqrt().clamp(min, max)
}

/// Delta transfer is allowed only when a compatible base exists. Pass false
/// when a periodic full compaction is required.
pub fn payload_size(input: &CheckpointInput) -> (u64, bool) {
    if input.has_base_checkpoint && input.changed_bytes < input.full_bytes {
        (input.changed_bytes.max(1), true)
    } else { (input.full_bytes, false) }
}

/// Small/frequent artifacts stay simple; large ones use 2+1 on slower links.
/// 2+2 is selected only when the workload asks for two-loss tolerance.
pub fn choose_storage_strategy(
    payload_bytes: u64, interval_seconds: f64, bandwidth_mbps: u32,
    small_artifact_limit_bytes: u64, frequent_interval_seconds: f64,
    required_peer_loss_tolerance: usize,
) -> StorageStrategy {
    if required_peer_loss_tolerance >= 2 { return StorageStrategy::Erasure2Plus2; }
    if payload_bytes <= small_artifact_limit_bytes || interval_seconds <= frequent_interval_seconds {
        return StorageStrategy::Replication2x;
    }
    if bandwidth_mbps < 100 { StorageStrategy::Erasure2Plus1 } else { StorageStrategy::Replication2x }
}

/// Rank fast, low-latency peers, choosing different failure domains first.
pub fn place_shards(workers: &[WorkerLink], count: usize) -> Vec<WorkerLink> {
    let mut ranked = workers.to_vec();
    ranked.sort_by(|a, b| {
        let ascore = f64::from(a.bandwidth_mbps) / (1.0 + f64::from(a.rtt_ms));
        let bscore = f64::from(b.bandwidth_mbps) / (1.0 + f64::from(b.rtt_ms));
        bscore.total_cmp(&ascore).then_with(|| a.node_id.cmp(&b.node_id))
    });
    let mut selected = Vec::with_capacity(count.min(ranked.len()));
    for worker in &ranked {
        if selected.len() == count { return selected; }
        if !selected.iter().any(|p: &WorkerLink| p.failure_domain == worker.failure_domain) {
            selected.push(worker.clone());
        }
    }
    for worker in ranked {
        if selected.len() == count { break; }
        if !selected.iter().any(|p| p.node_id == worker.node_id) { selected.push(worker); }
    }
    selected
}

/// Coordinator metadata only: no artifact bytes or GitHub checkpoint payloads.
pub fn build_manifest(
    checkpoint_id: String, generation: u64, base_checkpoint_id: Option<String>,
    logical_bytes: u64, payload_bytes: u64, strategy: StorageStrategy, workers: &[WorkerLink],
) -> Option<ArtifactManifest> {
    let selected = place_shards(workers, strategy.total_shards());
    if selected.len() != strategy.total_shards() { return None; }
    let shard_bytes = payload_bytes.div_ceil(strategy.data_shards() as u64);
    let shards = selected.into_iter().enumerate().map(|(index, worker)| ArtifactShard {
        index, node_id: worker.node_id, failure_domain: worker.failure_domain, bytes: shard_bytes,
    }).collect();
    Some(ArtifactManifest { checkpoint_id, generation, base_checkpoint_id, logical_bytes, payload_bytes, strategy, shards })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(mttf: f64, transfer_ms: f64) -> CheckpointInput {
        CheckpointInput { full_bytes: 100_000, changed_bytes: 10_000, has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms, estimated_mttf_seconds: mttf, work_per_second: 1.0,
            min_interval_seconds: 1.0, max_interval_seconds: 120.0 }
    }
    #[test]
    fn unreliable_workers_get_shorter_intervals() {
        assert!(adaptive_interval(&input(30.0, 1_000.0)) < adaptive_interval(&input(3_600.0, 1_000.0)));
    }
    #[test]
    fn deltas_need_a_base_and_compaction_restores_full_snapshot() {
        assert_eq!(payload_size(&input(60.0, 100.0)), (10_000, true));
        let mut no_base = input(60.0, 100.0);
        no_base.has_base_checkpoint = false;
        assert_eq!(payload_size(&no_base), (100_000, false));
    }
    #[test]
    fn storage_policy_matches_size_frequency_and_tolerance() {
        assert_eq!(choose_storage_strategy(8_000, 60.0, 20, 16_000, 5.0, 0), StorageStrategy::Replication2x);
        assert_eq!(choose_storage_strategy(100_000, 60.0, 20, 16_000, 5.0, 0), StorageStrategy::Erasure2Plus1);
        assert_eq!(choose_storage_strategy(8_000, 1.0, 20, 16_000, 5.0, 2), StorageStrategy::Erasure2Plus2);
    }
    #[test]
    fn placement_prefers_speed_but_spreads_failure_domains() {
        let ws = vec![
            WorkerLink { node_id: "fast-a".into(), failure_domain: "home-a".into(), bandwidth_mbps: 900, rtt_ms: 5 },
            WorkerLink { node_id: "fast-b".into(), failure_domain: "home-a".into(), bandwidth_mbps: 800, rtt_ms: 6 },
            WorkerLink { node_id: "medium-c".into(), failure_domain: "home-c".into(), bandwidth_mbps: 300, rtt_ms: 20 },
            WorkerLink { node_id: "slow-d".into(), failure_domain: "home-d".into(), bandwidth_mbps: 100, rtt_ms: 40 },
        ];
        let picked = place_shards(&ws, 3);
        assert_eq!(picked[0].node_id, "fast-a");
        assert_eq!(picked.iter().map(|w| &w.failure_domain).collect::<std::collections::HashSet<_>>().len(), 3);
    }
    #[test]
    fn manifest_requires_enough_hosts() {
        let ws = vec![WorkerLink { node_id: "a".into(), failure_domain: "one".into(), bandwidth_mbps: 100, rtt_ms: 10 }];
        assert!(build_manifest("cp-1".into(), 1, None, 100, 100, StorageStrategy::Erasure2Plus1, &ws).is_none());
    }
}
