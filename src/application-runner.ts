import { api, ApiError } from './api'
import type { ApplicationJob, ApplicationWork } from './application-types'
import { BrowserJobExecutor } from './application-runtime'
import { maintainLease } from './lease'
import { delay, isTransient } from './runner-policy'

export class ApplicationRunner {
  private executor = new BrowserJobExecutor()
  close() { this.executor.close() }
  async execute(work: ApplicationWork, claimedAt: number, allowCpu: boolean, parent: AbortSignal, progress: (message: string) => void) {
    const lease = maintainLease(work.job_id, work.lease, claimedAt, parent, `/api/application-work/${work.job_id}/renew`)
    let accepted = false
    let failure: unknown
    const matches = (job: ApplicationJob) => job.workers.some(receipt => receipt.node_id === work.lease.worker_id && receipt.attempt_id === work.lease.attempt_id && receipt.coordinator_epoch === work.lease.coordinator_epoch && receipt.generation === work.lease.generation)
    try {
      const result = await this.executor.run(work.input, allowCpu, lease.signal, work.timeout_ms, progress)
      for (let attempt = 0; ; attempt++) {
        try {
          const job = await api<ApplicationJob>(`/api/application-work/${work.job_id}/commit`, {
            method: 'POST', body: JSON.stringify({ lease: lease.current(), ...result }),
            signal: AbortSignal.any([lease.signal, AbortSignal.timeout(12_000)]),
          })
          accepted = matches(job)
          if (!accepted) throw new Error('Coordinator did not acknowledge this worker result')
          return { ...result.runtime, status: job.status }
        } catch (error) {
          // A timed-out commit can already be durable. Reconcile provenance before
          // considering it a failure, even if its lease has since expired.
          const observed = await api<ApplicationJob>(`/api/applications/${work.application_id}/jobs/${work.job_id}`, { signal: AbortSignal.any([parent, AbortSignal.timeout(5_000)]) }).catch(() => null)
          if (observed && matches(observed)) { accepted = true; return { ...result.runtime, status: observed.status } }
          if (attempt >= 2 || !isTransient(error)) throw error
          await delay(500 * 2 ** attempt, lease.signal)
        }
      }
    } catch (error) { failure = error; throw error }
    finally {
      lease.stop()
      if (!accepted) {
        const reportError = !parent.aborted && !(failure instanceof ApiError && [401, 403, 404, 409].includes(failure.status))
        await api(`/api/application-work/${work.job_id}/release`, {
          method: 'POST', body: JSON.stringify({ lease: work.lease, ...(reportError ? { error: failure instanceof Error ? failure.message.slice(0, 256) : 'Browser execution failed' } : {}) }),
          signal: AbortSignal.timeout(5_000),
        }).catch(() => undefined)
      }
    }
  }
}
