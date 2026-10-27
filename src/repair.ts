import type { PeerMeshNode } from './peer'
import { encode2Plus1, reconstruct2Plus1, sha256Hex, type EncodedShard } from './sharding'

export type RepairShard = { shard_index: number; node_id: string; artifact_name: string; sha256: string; bytes: number }
export type RepairTask = {
  id: string; job_id: string; batch_index: number; manifest_revision: number;
  sha256: string; bytes: number; shards: RepairShard[]; target: RepairShard;
  worker_node_id: string; expires_at: number;
}

export async function executeRepair(task: RepairTask, mesh: PeerMeshNode) {
  if (!task.id || !Number.isSafeInteger(task.bytes) || task.bytes <= 0 || task.bytes > 8 * 1024 * 1024
    || ![2, 3].includes(task.shards.length) || ![0, 1, 2].includes(task.target.shard_index)) throw new Error('Invalid checkpoint repair assignment')
  const threshold = task.shards.length === 3 ? 2 : 1
  const candidates = task.shards.filter(shard => shard.shard_index !== task.target.shard_index)
  const available = await Promise.allSettled(candidates.map(async shard => {
    const bytes = await mesh.getArtifact(shard.node_id, shard.artifact_name)
    if (bytes.length !== shard.bytes || await sha256Hex(bytes) !== shard.sha256) throw new Error('A repair source failed its integrity check')
    return { index: shard.shard_index as 0 | 1 | 2, bytes }
  }))
  const good = available.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
  if (good.length < threshold) throw new Error('Too few verified holders remain to repair this checkpoint')
  const full = task.shards.length === 3 ? reconstruct2Plus1(good.slice(0, 2) as EncodedShard[], task.bytes) : good[0].bytes
  if (full.length !== task.bytes || await sha256Hex(full) !== task.sha256) throw new Error('The reconstructed checkpoint failed its full-artifact integrity check')
  const replacement = task.shards.length === 3 ? encode2Plus1(full)[task.target.shard_index].bytes : full
  if (replacement.length !== task.target.bytes || await sha256Hex(replacement) !== task.target.sha256) throw new Error('The replacement shard does not match its committed digest')
  await mesh.sendArtifact(task.target.node_id, task.target.artifact_name, replacement, false)
}

