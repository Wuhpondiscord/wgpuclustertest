import { api, ApiError } from './api'
import type { ClusterNode, ComputeJob, ExecutionReport, JobLease, ShardReceipt } from './compute-types'
import type { ContributionSettings } from './contribution-settings'
import { maintainLease } from './lease'
import { PeerMeshNode, PeerTransferError } from './peer'
import { delay, errorStatus, isCustodyConflict, isTransient, retryDelay, WorkWake } from './runner-policy'
import { encode2Plus1, sha256Hex } from './sharding'
import { createVectorAddExecutor, digestVectorAddResult, encodeVectorAddOutput, GpuValidationError, runVectorAddCpu } from './webgpu'

export type RunnerState = {
  phase: 'waiting' | 'running' | 'backoff' | 'paused' | 'stopped' | 'error'; message: string;
  jobId?: string; batchIndex?: number; generation?: number; executionMode?: string; fallbackReason?: string | null;
  timings?: ExecutionReport; completed: number; retries: number;
  lastError?: string;
}
type RunnerOptions = {
  nodeId: string; clusterId: string; jobId?: string;
  getMesh: () => PeerMeshNode | null;
  settings: () => ContributionSettings;
  state: (state: RunnerState) => void;
  notice: (message: string) => void;
  jobs: (jobs: ComputeJob[]) => void;
}

function computeHash(start: number, end: number) {
  let accumulator = 0x811c9dc5
  for (let value = start; value < end; value++) {
    let mixed = (value ^ 0x9e3779b9) >>> 0
    mixed = Math.imul(mixed ^ (mixed >>> 16), 0x85ebca6b) >>> 0
    mixed = Math.imul(mixed ^ (mixed >>> 13), 0xc2b2ae35) >>> 0
    mixed = (mixed ^ (mixed >>> 16)) >>> 0
    accumulator = Math.imul(accumulator ^ mixed, 0x01000193) >>> 0
  }
  return accumulator.toString(16).padStart(8, '0')
}

function gpuFailure(error: unknown, fallback: string) {
  return (error instanceof Error ? error.message : fallback).replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, 256)
}

export class ComputeRunner {
  private controller = new AbortController()
  private attempt: AbortController | null = null
  private wakeEvent = new WorkWake()
  private gpu: Awaited<ReturnType<typeof createVectorAddExecutor>> = null
  private gpuRetryAt = 0
  private gpuReason: string | null = null
  private excludedUntil = new Map<string, number>()
  private placement = 0
  private failures = 0
  private jobCursor = 0
  private blockedJobs = new Set<string>()
  private state: RunnerState = { phase: 'waiting', message: 'Waiting for work', completed: 0, retries: 0 }
  constructor(private readonly options: RunnerOptions) {}
  wake() { this.wakeEvent.wake() }
  transportChanged() { this.attempt?.abort(new Error('Node transport changed; resynchronizing')); this.wake() }
  stop() { this.controller.abort(new DOMException('Contribution stopped', 'AbortError')); this.transportChanged() }
  private report(patch: Partial<RunnerState>) { this.state = { ...this.state, ...patch }; this.options.state(this.state) }
  private async request<T>(path: string, signal = this.controller.signal, init: RequestInit = {}) {
    return api<T>(path, { ...init, signal: AbortSignal.any([signal, AbortSignal.timeout(12_000)]) })
  }
  private async readJobs() { return this.request<ComputeJob[]>(`/api/clusters/${this.options.clusterId}/jobs`) }

  async run() {
    const signal = this.controller.signal
    const onVisibility = () => this.wake()
    document.addEventListener('visibilitychange', onVisibility)
    try {
      while (!signal.aborted) {
        const settings = this.options.settings()
        if (settings.pauseWhenHidden && document.hidden) {
          this.report({ phase: 'paused', message: 'New work paused while this tab is hidden' })
          await this.wakeEvent.wait(5_000, signal); continue
        }
        const mesh = this.options.getMesh()
        if (!mesh) {
          this.report({ phase: 'waiting', message: 'Waiting for this node to reconnect and verify storage' })
          await this.wakeEvent.wait(2_000, signal); continue
        }
        let claim: JobLease | null = null
        let attemptCommitted = false
        let job: ComputeJob | undefined
        try {
          const jobs = await this.readJobs()
          this.options.jobs(jobs)
          const candidates = jobs.filter(job => job.completed_batches < job.total_batches && !this.blockedJobs.has(job.id) && (!this.options.jobId || job.id === this.options.jobId))
            .sort((a, b) => a.created_at - b.created_at || (a.id < b.id ? -1 : 1))
          if (!candidates.length && this.options.jobId) {
            this.options.notice(jobs.some(job => job.id === this.options.jobId) ? 'All job batches are complete' : 'This job is no longer available'); return
          }
          for (let index = 0; index < candidates.length; index++) {
            const candidate = candidates[(this.jobCursor + index) % candidates.length]
            const claimedAt = performance.now()
            claim = await this.request<JobLease | null>(`/api/jobs/${candidate.id}/claim`, signal, { method: 'POST', body: JSON.stringify({ node_id: this.options.nodeId }) })
            if (!claim) continue
            job = candidate
            this.jobCursor = (this.jobCursor + index + 1) % candidates.length
            const began = performance.now()
            await this.execute(job, claim, mesh, claimedAt)
            attemptCommitted = true
            this.failures = 0
            this.report({ completed: this.state.completed + 1, phase: 'waiting', message: 'Waiting for the next batch', lastError: undefined })
            // Observation failure after acceptance is not a failed compute attempt.
            // The normal loop will read again without releasing committed work.
            const updated = await this.readJobs().catch(error => {
              if ([401, 403].includes(errorStatus(error) ?? 0)) throw error
              this.report({ message: 'Batch committed; job refresh will retry', lastError: error instanceof Error ? error.message : 'Job refresh failed' })
              return null
            })
            if (updated) this.options.jobs(updated)
            if (this.options.jobId && updated?.some(item => item.id === job!.id && item.completed_batches === item.total_batches)) {
              this.options.notice(`All job batches completed and replicated (${this.state.executionMode})`); return
            }
            const cooldown = (performance.now() - began) * (100 / this.options.settings().dutyCycle - 1)
            if (cooldown > 0) { this.report({ phase: 'paused', message: 'Duty-cycle cooldown' }); await delay(cooldown, signal) }
            break
          }
          if (!claim) {
            this.report({ phase: 'waiting', message: this.blockedJobs.size ? 'Some jobs require attention; waiting for other available work' : 'Waiting for available work or another worker’s lease' })
            await this.wakeEvent.wait(2_000 + Math.random() * 3_000, signal)
          }
        } catch (error) {
          if (signal.aborted) break
          const status = errorStatus(error)
          if (error instanceof ApiError && error.code === 'account_session_required') {
            this.report({ phase: 'waiting', message: 'Sign in again to resume this contributor' })
            await this.wakeEvent.wait(5_000, signal); continue
          }
          if (status === 401 || status === 403) {
            this.report({ phase: 'error', message: error instanceof Error ? error.message : 'Node authorization was revoked' }); return
          }
          // A failed attempt never takes the contributor out of service.
          // Release only its exact fenced lease; an already committed attempt
          // or a reassigned lease is harmlessly rejected by the coordinator.
          if (claim && job && !attemptCommitted) await api(`/api/jobs/${job.id}/release`, {
            method: 'POST', body: JSON.stringify({ lease: claim.lease }), signal: AbortSignal.timeout(5_000),
          }).catch(() => undefined)
          if (job && (status === 400 || status === 422)) {
            this.blockedJobs.add(job.id)
            if (this.options.jobId) { this.report({ phase: 'error', message: error instanceof Error ? error.message : 'Invalid job' }); return }
          }
          const reason = error instanceof Error ? error.message : 'temporary worker failure'
          this.report({ phase: 'backoff', retries: this.state.retries + 1, message: `Retrying: ${reason}`, lastError: reason })
          // Our own release emits a work hint. It must not bypass backoff and
          // create a tight claim/fail/release loop during an outage.
          await delay(retryDelay(this.failures++), signal)
        }
      }
    } catch (error) {
      if (!signal.aborted) this.report({ phase: 'error', message: error instanceof Error ? error.message : 'Worker stopped unexpectedly' })
    } finally {
      document.removeEventListener('visibilitychange', onVisibility)
      this.gpu?.close(); this.gpu = null
      if (this.state.phase !== 'error') this.report({ phase: 'stopped', message: 'Contribution stopped' })
    }
  }

  private async execute(job: ComputeJob, claim: JobLease, mesh: PeerMeshNode, claimedAt: number) {
    const localAttempt = new AbortController()
    this.attempt = localAttempt
    const lease = maintainLease(job.id, claim.lease, claimedAt, AbortSignal.any([this.controller.signal, localAttempt.signal]))
    const signal = lease.signal
    const report: ExecutionReport = { execution_mode: 'cpu', fallback_reason: null, compute_ms: 0, validation_ms: 0, encoding_ms: 0, transfer_ms: 0 }
    this.report({ phase: 'running', jobId: job.id, batchIndex: claim.batch_index, generation: claim.lease.generation, message: `Processing batch ${claim.batch_index + 1}` })
    try {
      let vector: Float32Array | null = null
      let started = performance.now()
      if (job.workload === 'vector-add') {
        if (!this.gpu && performance.now() >= this.gpuRetryAt) {
          try { this.gpu = await createVectorAddExecutor(); this.gpuReason = this.gpu ? null : 'gpu' in navigator ? 'No WebGPU adapter is available' : 'This browser does not expose WebGPU' }
          catch (error) { this.gpuReason = gpuFailure(error, 'WebGPU device initialization failed') }
          if (!this.gpu) this.gpuRetryAt = performance.now() + 30_000
        }
        lease.current()
        started = performance.now()
        if (this.gpu) {
          try {
            vector = await this.gpu.run(claim.start, claim.end)
            report.validation_ms = this.gpu.validationMs
          } catch (error) {
            this.gpu.close(); this.gpu = null; this.gpuRetryAt = performance.now() + 30_000
            this.gpuReason = gpuFailure(error, 'WebGPU batch failed')
            // An invalid numerical result is never silently accepted by switching
            // execution mode. Device loss/unavailability can use an allowed fallback.
            if (error instanceof GpuValidationError) throw new ApiError(422, this.gpuReason)
          }
        }
        lease.current()
        if (!vector) {
          if (!this.options.settings().allowCpuFallback) throw new Error(`CPU fallback is disabled: ${this.gpuReason}`)
          vector = runVectorAddCpu(claim.start, claim.end)
          report.execution_mode = 'cpu-fallback'; report.fallback_reason = this.gpuReason
        } else report.execution_mode = 'webgpu'
      }
      const hash = vector ? null : computeHash(claim.start, claim.end)
      report.compute_ms = Math.max(0, performance.now() - started - report.validation_ms)
      lease.current()
      started = performance.now()
      const digest = vector ? await digestVectorAddResult(vector) : hash
      const artifact = new TextEncoder().encode(JSON.stringify({
        type: 'mesh-compute-checkpoint-v3', workload: job.workload, execution_mode: report.execution_mode,
        job_id: job.id, batch_index: claim.batch_index, start: claim.start, end: claim.end, result_digest: digest,
        output: vector ? encodeVectorAddOutput(vector) : undefined, worker_node_id: this.options.nodeId, generation: claim.lease.generation,
      }))
      const sha256 = await sha256Hex(artifact)
      report.encoding_ms = performance.now() - started
      const name = `job-${job.id}-batch-${claim.batch_index}-g${claim.lease.generation}`
      const place = async (fullCopies: boolean) => {
        lease.current()
        for (const [id, until] of this.excludedUntil) if (until <= performance.now()) this.excludedUntil.delete(id)
        const nodes = await this.request<ClusterNode[]>(`/api/clusters/${job.cluster_id}/nodes`, signal)
        if (!nodes.some(node => node.id === this.options.nodeId && node.online && !node.paused && node.holder_ready)) throw new Error('This worker is not an active verified holder')
        let peers = nodes.filter(node => node.id !== this.options.nodeId && node.online && !node.paused && node.holder_ready
          && mesh.canTransferTo(node.id) && (this.excludedUntil.get(node.id) ?? 0) <= performance.now())
        // Rotate the stable order using this worker's identity as well as the
        // attempt, so simultaneous survivors do not all select the first peer.
        peers = peers.sort((a, b) => a.id < b.id ? -1 : 1)
        const offset = peers.length ? (parseInt(this.options.nodeId.replace(/-/g, '').slice(0, 8), 16) + this.placement++) % peers.length : 0
        peers = peers.slice(offset).concat(peers.slice(0, offset))
        const failed = new Set<string>()
        const send = async (target: ClusterNode, artifactName: string, bytes: Uint8Array) => {
          try { return await mesh.sendArtifact(target.id, artifactName, bytes, false, signal) }
          catch (error) {
            signal.throwIfAborted()
            if (!(error instanceof PeerTransferError && error.code === 'receiver_busy')) {
              failed.add(target.id); this.excludedUntil.set(target.id, performance.now() + 15_000)
            }
            throw error
          }
        }
        if (!fullCopies && artifact.length >= 64 * 1024 && peers.length >= 2) {
          try {
            const coded = encode2Plus1(artifact)
            const local = await mesh.storeArtifact(`${name}-s0`, coded[0].bytes, signal)
            const receipts: ShardReceipt[] = [{ shard_index: 0, node_id: this.options.nodeId, artifact_name: `${name}-s0`, ...local }]
            for (const shard of coded.slice(1)) {
              lease.current()
              const target = peers[shard.index - 1]
              const transferred = await send(target, `${name}-s${shard.index}`, shard.bytes)
              receipts.push({ shard_index: shard.index, node_id: target.id, artifact_name: `${name}-s${shard.index}`, sha256: transferred.sha256, bytes: transferred.bytes })
            }
            return receipts
          } catch { signal.throwIfAborted(); this.options.notice(`A shard peer left during batch ${claim.batch_index + 1}; retrying with full replicas`) }
        }
        const local = await mesh.storeArtifact(`${name}-r0`, artifact, signal)
        let last: unknown = new Error('Waiting for an available checkpoint holder')
        for (const target of peers.filter(peer => !failed.has(peer.id))) {
          try {
            const transferred = await send(target, `${name}-r1`, artifact)
            return [
              { shard_index: 0, node_id: this.options.nodeId, artifact_name: `${name}-r0`, ...local },
              { shard_index: 1, node_id: target.id, artifact_name: `${name}-r1`, sha256: transferred.sha256, bytes: transferred.bytes },
            ] satisfies ShardReceipt[]
          } catch (error) { last = error; signal.throwIfAborted() }
        }
        throw last
      }
      started = performance.now()
      let shards = await place(false)
      report.transfer_ms = performance.now() - started
      this.report({ executionMode: report.execution_mode, fallbackReason: report.fallback_reason, timings: report })
      let replanned = false
      for (let attempt = 0; ; attempt++) {
        try {
          await api(`/api/jobs/${job.id}/commit`, {
            method: 'POST', body: JSON.stringify({ lease: lease.current(), sha256, bytes: artifact.length, shards, execution: report }),
            signal: AbortSignal.any([signal, AbortSignal.timeout(90_000)]),
          })
          break
        } catch (error) {
          if (!replanned && isCustodyConflict(error)) {
            replanned = true
            const at = performance.now(); shards = await place(true); report.transfer_ms += performance.now() - at
            continue
          }
          if (!isTransient(error)) throw error
          // A response can be lost after persistence. Reconcile first; never
          // manufacture a new artifact or generation to retry an ambiguous commit.
          const snapshot = await this.request<ComputeJob[]>(`/api/clusters/${job.cluster_id}/jobs`, this.controller.signal).catch(() => [])
          const committed = snapshot.find(item => item.id === job.id)?.checkpoints.find(item => item.batch_index === claim.batch_index)
          if (committed?.sha256 === sha256) break
          if (committed) throw new ApiError(409, 'Another attempt committed this batch')
          signal.throwIfAborted()
          if (attempt >= 3) throw error
          this.report({ retries: this.state.retries + 1, message: 'Retrying the same checkpoint commit after a temporary failure' })
          await delay(retryDelay(attempt), signal)
        }
      }
      this.options.notice(`Checkpointed ${job.workload === 'vector-add' ? 'vector-add' : 'CPU'} batch ${claim.batch_index + 1} of ${job.total_batches} using ${shards.length === 3 ? '2+1 shards' : 'two replicas'}`)
    } finally {
      lease.stop()
      if (this.attempt === localAttempt) this.attempt = null
      // Explicit pause releases work promptly. Network loss still relies on
      // coordinator lease fencing if this request cannot be delivered.
      if (this.controller.signal.aborted) void api(`/api/jobs/${job.id}/release`, {
        method: 'POST', body: JSON.stringify({ lease: claim.lease }), signal: AbortSignal.timeout(5_000),
      }).catch(() => undefined)
    }
  }
}
