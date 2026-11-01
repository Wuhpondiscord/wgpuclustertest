export function retryDelay(failures: number, random = Math.random) {
  return Math.round(Math.min(30_000, 500 * 2 ** Math.min(failures, 6)) * (0.8 + random() * 0.4))
}
export function errorStatus(error: unknown): number | undefined { return (error as { status?: number } | null)?.status }
export function isTransient(error: unknown) {
  const status = errorStatus(error)
  return status === undefined || status === 408 || status === 429 || status >= 500
}
export function isCustodyConflict(error: unknown) {
  return errorStatus(error) === 409 && /active cluster nodes|independently confirmed storage/.test(error instanceof Error ? error.message : '')
}

export async function delay(ms: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted()
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(signal.reason) }
    const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, ms)
    signal.addEventListener('abort', abort, { once: true })
  })
}

/** Push notifications shorten waits; bounded polling remains authoritative. */
export class WorkWake {
  private notified = false
  private resume: (() => void) | null = null
  wake() { this.notified = true; this.resume?.() }
  async wait(ms: number, signal: AbortSignal) {
    signal.throwIfAborted()
    if (this.notified) { this.notified = false; return }
    await new Promise<void>((resolve, reject) => {
      const finish = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); this.resume = null; this.notified = false; resolve() }
      const abort = () => { clearTimeout(timer); this.resume = null; reject(signal.reason) }
      const timer = setTimeout(finish, ms)
      this.resume = finish
      signal.addEventListener('abort', abort, { once: true })
    })
  }
}
