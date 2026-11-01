import { api, ApiError } from './api'
import type { TaskLease } from './compute-types'

export function maintainLease(jobId: string, initial: TaskLease, claimedAt: number, parent: AbortSignal) {
  parent.throwIfAborted()
  let current = initial
  let stopped = false
  let renewTimer = 0
  let expiryTimer = 0
  let deadline = claimedAt + 44_000
  const attempt = new AbortController()
  const signal = AbortSignal.any([parent, attempt.signal])
  const armExpiry = (at: number) => {
    deadline = at + 44_000
    window.clearTimeout(expiryTimer)
    expiryTimer = window.setTimeout(() => attempt.abort(new ApiError(409, 'Worker lease expired')), Math.max(0, at + 44_000 - performance.now()))
  }
  const renew = async () => {
    const startedAt = performance.now()
    let delay = 10_000
    try {
      current = await api<TaskLease>(`/api/jobs/${jobId}/renew`, {
        method: 'POST', body: JSON.stringify({ lease: current }), signal: AbortSignal.any([signal, AbortSignal.timeout(8_000)]),
      })
      if (!stopped && !signal.aborted) armExpiry(startedAt)
    } catch (error) {
      if (error instanceof ApiError && [401, 403, 404, 409].includes(error.status)) attempt.abort(error)
      else delay = 2_000
    }
    if (!stopped && !signal.aborted) renewTimer = window.setTimeout(() => void renew(), delay)
  }
  const stop = () => { stopped = true; window.clearTimeout(renewTimer); window.clearTimeout(expiryTimer) }
  signal.addEventListener('abort', stop, { once: true })
  armExpiry(claimedAt)
  renewTimer = window.setTimeout(() => void renew(), 10_000)
  return {
    signal,
    current: () => {
      if (performance.now() >= deadline) attempt.abort(new ApiError(409, 'Worker lease expired'))
      signal.throwIfAborted(); return current
    },
    stop: () => { stop(); signal.removeEventListener('abort', stop) },
  }
}
