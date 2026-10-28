//! Checkpoint policy and peer placement primitives.
//!
//! This module plans metadata only. Checkpoint bytes stay with workers and move
//! directly between peers; the coordinator should retain leases and manifests.

use std::collections::{HashMap, HashSet};

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
pub enum StorageStrategy {
    Replication2x,
    Erasure2Plus1,
    Erasure2Plus2,
}

impl StorageStrategy {
    pub fn data_shards(self) -> usize {
        match self {
            Self::Replication2x => 1,
            Self::Erasure2Plus1 | Self::Erasure2Plus2 => 2,
        }
    }

    pub fn total_shards(self) -> usize {
        match self {
            Self::Replication2x => 2,
            Self::Erasure2Plus1 => 3,
            Self::Erasure2Plus2 => 4,
        }
    }

    pub fn tolerable_losses(self) -> usize {
        match self {
            Self::Replication2x | Self::Erasure2Plus1 => 1,
            Self::Erasure2Plus2 => 2,
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardReceipt {
    pub checkpoint_id: String,
    pub shard_index: usize,
    pub node_id: String,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointError {
    InvalidManifest,
    StaleGeneration,
    MissingBase,
    UnknownTransaction,
    UnknownShard,
    WrongNode,
    HashMismatch,
    NotDurable,
    DuplicateCheckpoint,
    BrokenChain,
}

#[derive(Clone, Debug)]
struct PendingCheckpoint {
    manifest: ArtifactManifest,
    expected_hashes: Vec<String>,
    receipts: HashSet<usize>,
}

/// Metadata-only two-phase checkpoint commit coordinator.
///
/// Workers stage and hash their own shards, then submit receipts. The catalog
/// publishes a new head only once enough distinct shards are verified to meet
/// the strategy's reconstruction threshold. This type never handles artifact
/// bytes and is intended to sit beside a direct peer transport.
#[derive(Clone, Debug, Default)]
pub struct CheckpointCatalog {
    current_id: Option<String>,
    committed: HashMap<String, ArtifactManifest>,
    pending: HashMap<String, PendingCheckpoint>,
}

impl CheckpointCatalog {
    pub fn current(&self) -> Option<&ArtifactManifest> {
        self.current_id
            .as_ref()
            .and_then(|id| self.committed.get(id))
    }

    /// Start staging a manifest. `expected_hashes` has one SHA-256 digest per
    /// manifest shard, in shard-index order. A delta must extend the current
    /// head; a full checkpoint (no base ID) can compact the chain.
    pub fn begin(
        &mut self,
        manifest: ArtifactManifest,
        expected_hashes: Vec<String>,
    ) -> Result<(), CheckpointError> {
        if self.pending.contains_key(&manifest.checkpoint_id)
            || self.committed.contains_key(&manifest.checkpoint_id)
        {
            return Err(CheckpointError::DuplicateCheckpoint);
        }
        if manifest.shards.len() != manifest.strategy.total_shards()
            || expected_hashes.len() != manifest.shards.len()
            || expected_hashes.iter().any(|hash| !is_sha256(hash))
            || manifest
                .shards
                .iter()
                .enumerate()
                .any(|(index, shard)| shard.index != index)
            || manifest
                .shards
                .iter()
                .any(|shard| shard.node_id.is_empty() || shard.bytes == 0)
            || manifest
                .shards
                .iter()
                .map(|shard| shard.node_id.as_str())
                .collect::<HashSet<_>>()
                .len()
                != manifest.shards.len()
        {
            return Err(CheckpointError::InvalidManifest);
        }

        let expected_generation = self
            .current()
            .map_or(1, |current| current.generation.saturating_add(1));
        if manifest.generation != expected_generation {
            return Err(CheckpointError::StaleGeneration);
        }
        match (&self.current_id, &manifest.base_checkpoint_id) {
            (None, None) => {}
            (Some(current), Some(base)) if current == base => {}
            (Some(_), None) => {} // full compaction
            _ => return Err(CheckpointError::MissingBase),
        }

        self.pending.insert(
            manifest.checkpoint_id.clone(),
            PendingCheckpoint {
                manifest,
                expected_hashes,
                receipts: HashSet::new(),
            },
        );
        Ok(())
    }

    /// Accept a receipt only from the peer assigned to that shard, and only
    /// when its digest matches the expected digest from the job manifest.
    pub fn acknowledge(&mut self, receipt: ShardReceipt) -> Result<(), CheckpointError> {
        let pending = self
            .pending
            .get_mut(&receipt.checkpoint_id)
            .ok_or(CheckpointError::UnknownTransaction)?;
        let shard = pending
            .manifest
            .shards
            .get(receipt.shard_index)
            .ok_or(CheckpointError::UnknownShard)?;
        if shard.node_id != receipt.node_id {
            return Err(CheckpointError::WrongNode);
        }
        if !is_sha256(&receipt.sha256)
            || pending.expected_hashes[receipt.shard_index] != receipt.sha256
        {
            return Err(CheckpointError::HashMismatch);
        }
        pending.receipts.insert(receipt.shard_index);
        Ok(())
    }

    /// Publish the staged manifest when enough receipts exist to reconstruct
    /// it. Until then `current()` continues returning the previous good head.
    pub fn commit(&mut self, checkpoint_id: &str) -> Result<&ArtifactManifest, CheckpointError> {
        let pending = self
            .pending
            .get(checkpoint_id)
            .ok_or(CheckpointError::UnknownTransaction)?;
        // Require the complete desired replica/parity set before publishing.
        // A later repair process may support degraded commits explicitly.
        if pending.receipts.len() < pending.manifest.strategy.total_shards() {
            return Err(CheckpointError::NotDurable);
        }
        let pending = self
            .pending
            .remove(checkpoint_id)
            .expect("pending checkpoint was checked");
        self.current_id = Some(checkpoint_id.to_owned());
        self.committed
            .insert(checkpoint_id.to_owned(), pending.manifest);
        Ok(self
            .committed
            .get(checkpoint_id)
            .expect("checkpoint was inserted"))
    }

    /// Drop an incomplete transaction without changing the published head.
    pub fn abort(&mut self, checkpoint_id: &str) -> bool {
        self.pending.remove(checkpoint_id).is_some()
    }

    /// Return the committed full base followed by each delta through the head.
    /// A missing parent means the artifact chain is incomplete and must not be
    /// presented as recoverable.
    pub fn restore_chain(&self) -> Result<Vec<&ArtifactManifest>, CheckpointError> {
        let mut chain = Vec::new();
        let mut cursor = self.current().ok_or(CheckpointError::BrokenChain)?;
        loop {
            chain.push(cursor);
            match cursor.base_checkpoint_id.as_deref() {
                Some(parent_id) => {
                    cursor = self
                        .committed
                        .get(parent_id)
                        .ok_or(CheckpointError::BrokenChain)?
                }
                None => break,
            }
        }
        chain.reverse();
        Ok(chain)
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Young/Daly-style interval estimate using observed checkpoint wall time and
/// estimated mean time to failure, bounded for product-level safety limits.
pub fn adaptive_interval(input: &CheckpointInput) -> f64 {
    let min = input.min_interval_seconds.max(0.1);
    let max = input.max_interval_seconds.max(min);
    let checkpoint_cost = (input.observed_transfer_ms / 1_000.0).max(0.001);
    let mttf = input.estimated_mttf_seconds.max(0.1);
    let estimate = (2.0 * checkpoint_cost * mttf).sqrt();
    estimate.clamp(min, max)
}

/// Delta transfer is allowed only when a compatible base exists. Periodic full
/// compaction can be expressed by passing `has_base_checkpoint = false`.
pub fn payload_size(input: &CheckpointInput) -> (u64, bool) {
    if input.has_base_checkpoint && input.changed_bytes < input.full_bytes {
        (input.changed_bytes.max(1), true)
    } else {
        (input.full_bytes, false)
    }
}

/// Keep small/frequent artifacts simple; use 2+1 for large artifacts on slower
/// links; use 2+2 only when the workload explicitly requires two-loss tolerance.
pub fn choose_storage_strategy(
    payload_bytes: u64,
    interval_seconds: f64,
    bandwidth_mbps: u32,
    small_artifact_limit_bytes: u64,
    frequent_interval_seconds: f64,
    required_peer_loss_tolerance: usize,
) -> StorageStrategy {
    if required_peer_loss_tolerance >= 2 {
        return StorageStrategy::Erasure2Plus2;
    }
    if payload_bytes <= small_artifact_limit_bytes || interval_seconds <= frequent_interval_seconds
    {
        return StorageStrategy::Replication2x;
    }
    if bandwidth_mbps < 100 {
        StorageStrategy::Erasure2Plus1
    } else {
        StorageStrategy::Replication2x
    }
}

/// Prefer high-throughput, low-latency peers while placing one shard per
/// failure domain before reusing a domain. This helps avoid correlated loss.
pub fn place_shards(workers: &[WorkerLink], count: usize) -> Vec<WorkerLink> {
    let mut ranked = workers.to_vec();
    ranked.sort_by(|a, b| {
        let a_score = f64::from(a.bandwidth_mbps) / (1.0 + f64::from(a.rtt_ms));
        let b_score = f64::from(b.bandwidth_mbps) / (1.0 + f64::from(b.rtt_ms));
        b_score
            .total_cmp(&a_score)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });

    let mut selected = Vec::with_capacity(count.min(ranked.len()));
    for worker in &ranked {
        if selected.len() == count {
            return selected;
        }
        if !selected
            .iter()
            .any(|picked: &WorkerLink| picked.failure_domain == worker.failure_domain)
        {
            selected.push(worker.clone());
        }
    }
    for worker in ranked {
        if selected.len() == count {
            break;
        }
        if !selected
            .iter()
            .any(|picked| picked.node_id == worker.node_id)
        {
            selected.push(worker);
        }
    }
    selected
}

pub fn build_manifest(
    checkpoint_id: String,
    generation: u64,
    base_checkpoint_id: Option<String>,
    logical_bytes: u64,
    payload_bytes: u64,
    strategy: StorageStrategy,
    workers: &[WorkerLink],
) -> Option<ArtifactManifest> {
    let selected = place_shards(workers, strategy.total_shards());
    if selected.len() != strategy.total_shards() {
        return None;
    }
    let shard_bytes = payload_bytes.div_ceil(strategy.data_shards() as u64);
    let shards = selected
        .into_iter()
        .enumerate()
        .map(|(index, worker)| ArtifactShard {
            index,
            node_id: worker.node_id,
            failure_domain: worker.failure_domain,
            bytes: shard_bytes,
        })
        .collect();
    Some(ArtifactManifest {
        checkpoint_id,
        generation,
        base_checkpoint_id,
        logical_bytes,
        payload_bytes,
        strategy,
        shards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(mttf: f64, transfer_ms: f64) -> CheckpointInput {
        CheckpointInput {
            full_bytes: 100_000,
            changed_bytes: 10_000,
            has_base_checkpoint: true,
            observed_transfer_ms: transfer_ms,
            estimated_mttf_seconds: mttf,
            work_per_second: 1.0,
            min_interval_seconds: 1.0,
            max_interval_seconds: 120.0,
        }
    }

    #[test]
    fn unreliable_workers_get_shorter_intervals() {
        assert!(
            adaptive_interval(&input(30.0, 1_000.0)) < adaptive_interval(&input(3_600.0, 1_000.0))
        );
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
        assert_eq!(
            choose_storage_strategy(8_000, 60.0, 20, 16_000, 5.0, 0),
            StorageStrategy::Replication2x
        );
        assert_eq!(
            choose_storage_strategy(100_000, 60.0, 20, 16_000, 5.0, 0),
            StorageStrategy::Erasure2Plus1
        );
        assert_eq!(
            choose_storage_strategy(8_000, 1.0, 20, 16_000, 5.0, 2),
            StorageStrategy::Erasure2Plus2
        );
    }

    #[test]
    fn placement_prefers_speed_but_spreads_failure_domains() {
        let workers = vec![
            WorkerLink {
                node_id: "fast-a".into(),
                failure_domain: "home-a".into(),
                bandwidth_mbps: 900,
                rtt_ms: 5,
            },
            WorkerLink {
                node_id: "fast-b".into(),
                failure_domain: "home-a".into(),
                bandwidth_mbps: 800,
                rtt_ms: 6,
            },
            WorkerLink {
                node_id: "medium-c".into(),
                failure_domain: "home-c".into(),
                bandwidth_mbps: 300,
                rtt_ms: 20,
            },
            WorkerLink {
                node_id: "slow-d".into(),
                failure_domain: "home-d".into(),
                bandwidth_mbps: 100,
                rtt_ms: 40,
            },
        ];
        let picked = place_shards(&workers, 3);
        assert_eq!(picked[0].node_id, "fast-a");
        assert_eq!(
            picked
                .iter()
                .map(|w| &w.failure_domain)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn manifest_contains_metadata_only_and_requires_enough_hosts() {
        let workers = vec![WorkerLink {
            node_id: "a".into(),
            failure_domain: "one".into(),
            bandwidth_mbps: 100,
            rtt_ms: 10,
        }];
        assert!(build_manifest(
            "cp-1".into(),
            1,
            None,
            100,
            100,
            StorageStrategy::Erasure2Plus1,
            &workers
        )
        .is_none());
    }

    fn peers() -> Vec<WorkerLink> {
        vec![
            WorkerLink {
                node_id: "node-a".into(),
                failure_domain: "home-a".into(),
                bandwidth_mbps: 500,
                rtt_ms: 10,
            },
            WorkerLink {
                node_id: "node-b".into(),
                failure_domain: "home-b".into(),
                bandwidth_mbps: 400,
                rtt_ms: 20,
            },
            WorkerLink {
                node_id: "node-c".into(),
                failure_domain: "home-c".into(),
                bandwidth_mbps: 300,
                rtt_ms: 30,
            },
            WorkerLink {
                node_id: "node-d".into(),
                failure_domain: "home-d".into(),
                bandwidth_mbps: 200,
                rtt_ms: 40,
            },
        ]
    }

    fn manifest(id: &str, generation: u64, base: Option<&str>) -> ArtifactManifest {
        build_manifest(
            id.into(),
            generation,
            base.map(str::to_owned),
            100,
            10,
            StorageStrategy::Erasure2Plus1,
            &peers(),
        )
        .unwrap()
    }

    fn acknowledge(catalog: &mut CheckpointCatalog, id: &str, index: usize, hash: &str) {
        let pending = catalog.pending.get(id).unwrap();
        catalog
            .acknowledge(ShardReceipt {
                checkpoint_id: id.into(),
                shard_index: index,
                node_id: pending.manifest.shards[index].node_id.clone(),
                sha256: hash.into(),
            })
            .unwrap();
    }

    #[test]
    fn checkpoint_head_moves_only_after_verified_durability_threshold() {
        let mut catalog = CheckpointCatalog::default();
        let hashes = vec!["a".repeat(64), "b".repeat(64), "c".repeat(64)];
        catalog
            .begin(manifest("base", 1, None), hashes.clone())
            .unwrap();
        acknowledge(&mut catalog, "base", 0, &hashes[0]);
        acknowledge(&mut catalog, "base", 1, &hashes[1]);
        assert_eq!(
            catalog.commit("base").unwrap_err(),
            CheckpointError::NotDurable
        );
        acknowledge(&mut catalog, "base", 2, &hashes[2]);
        catalog.commit("base").unwrap();

        catalog
            .begin(manifest("delta-1", 2, Some("base")), hashes.clone())
            .unwrap();
        acknowledge(&mut catalog, "delta-1", 0, &hashes[0]);
        acknowledge(&mut catalog, "delta-1", 1, &hashes[1]);
        assert_eq!(
            catalog.commit("delta-1").unwrap_err(),
            CheckpointError::NotDurable
        );
        assert_eq!(catalog.current().unwrap().checkpoint_id, "base");

        acknowledge(&mut catalog, "delta-1", 2, &hashes[2]);
        catalog.commit("delta-1").unwrap();
        let chain = catalog.restore_chain().unwrap();
        assert_eq!(
            chain
                .iter()
                .map(|entry| entry.checkpoint_id.as_str())
                .collect::<Vec<_>>(),
            vec!["base", "delta-1"]
        );
    }

    #[test]
    fn rejects_unassigned_or_corrupt_shard_receipts() {
        let mut catalog = CheckpointCatalog::default();
        let hashes = vec!["a".repeat(64), "b".repeat(64), "c".repeat(64)];
        catalog
            .begin(manifest("base", 1, None), hashes.clone())
            .unwrap();
        assert_eq!(
            catalog.acknowledge(ShardReceipt {
                checkpoint_id: "base".into(),
                shard_index: 0,
                node_id: "attacker".into(),
                sha256: hashes[0].clone(),
            }),
            Err(CheckpointError::WrongNode)
        );
        let assigned_node = catalog.pending["base"].manifest.shards[0].node_id.clone();
        assert_eq!(
            catalog.acknowledge(ShardReceipt {
                checkpoint_id: "base".into(),
                shard_index: 0,
                node_id: assigned_node,
                sha256: "d".repeat(64),
            }),
            Err(CheckpointError::HashMismatch)
        );
        assert!(catalog.current().is_none());
    }

    #[test]
    fn abort_and_full_compaction_preserve_a_recoverable_chain() {
        let mut catalog = CheckpointCatalog::default();
        let hashes = vec!["a".repeat(64), "b".repeat(64), "c".repeat(64)];
        catalog
            .begin(manifest("base", 1, None), hashes.clone())
            .unwrap();
        acknowledge(&mut catalog, "base", 0, &hashes[0]);
        acknowledge(&mut catalog, "base", 1, &hashes[1]);
        acknowledge(&mut catalog, "base", 2, &hashes[2]);
        catalog.commit("base").unwrap();

        catalog
            .begin(manifest("failed-delta", 2, Some("base")), hashes.clone())
            .unwrap();
        assert!(catalog.abort("failed-delta"));
        assert_eq!(catalog.current().unwrap().checkpoint_id, "base");

        catalog
            .begin(manifest("compacted", 2, None), hashes.clone())
            .unwrap();
        acknowledge(&mut catalog, "compacted", 0, &hashes[0]);
        acknowledge(&mut catalog, "compacted", 1, &hashes[1]);
        acknowledge(&mut catalog, "compacted", 2, &hashes[2]);
        catalog.commit("compacted").unwrap();
        let chain = catalog.restore_chain().unwrap();
        assert_eq!(
            chain
                .iter()
                .map(|entry| entry.checkpoint_id.as_str())
                .collect::<Vec<_>>(),
            vec!["compacted"]
        );
    }
}
