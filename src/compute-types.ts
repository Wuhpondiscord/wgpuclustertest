export type ClusterNode = { id: string; cluster_id: string; name: string; paused: boolean; online: boolean; last_seen: number | null; holder_ready: boolean }
export type ShardReceipt = { shard_index: number; node_id: string; artifact_name: string; sha256: string; bytes: number }
export type BatchTimings = { compute_ms: number; validation_ms: number; encoding_ms: number; transfer_ms: number }
export type ExecutionReport = BatchTimings & {
  execution_mode: 'webgpu' | 'cpu-fallback' | 'cpu'; fallback_reason: string | null
  gpu_readback_ms?: number; gpu_buffers_reused?: boolean
}
export type AttemptObservation = { worker_id: string; previous_worker_id: string | null; generation: number; attempt_id: number; coordinator_epoch: number; claimed_at_ms: number; failure_detected_at_ms: number | null; reassigned_at_ms: number | null; previous_attempt_reason?: 'disconnect' | 'expiry' | 'release' | 'revoked' | null; previous_attempt_ended_at_ms?: number | null }
export type ComputeCheckpoint = {
  batch_index: number; sha256: string; bytes: number; shards: ShardReceipt[]; available_shards: number; verified_holders: number;
  durability: string; manifest_revision: number; worker_node_id?: string; committed_at_ms?: number;
  execution?: ExecutionReport | null; attempt?: AttemptObservation | null;
}
export type ComputeJob = { id: string; cluster_id: string; created_at: number; workload: 'cpu-hash' | 'vector-add'; total_units: number; batch_units: number; total_batches: number; completed_batches: number; checkpoints: ComputeCheckpoint[] }
export type TaskLease = { task_id: string; worker_id: string; coordinator_epoch: number; generation: number; attempt_id: number; expires_at_ms: number }
export type JobLease = { job_id: string; batch_index: number; start: number; end: number; lease: TaskLease }
