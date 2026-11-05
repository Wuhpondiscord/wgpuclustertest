import type { ApplicationInput, ApplicationOutput, ApplicationRuntimeReport } from './application-types'
export type ApplicationResult = { output: ApplicationOutput; runtime: ApplicationRuntimeReport }
/** One serial, disposable worker per contributor. Stop/lease loss discards its state. */
export class BrowserJobExecutor {
  private worker: Worker | null = null
  private busy = false
  close() { this.worker?.terminate(); this.worker = null }
  async run(input: ApplicationInput, allowCpu: boolean, parent: AbortSignal, timeoutMs: number, progress: (message: string) => void): Promise<ApplicationResult> {
    if (this.busy) throw new Error('Browser worker is already executing a job')
    parent.throwIfAborted()
    this.busy = true
    const signal = AbortSignal.any([parent, AbortSignal.timeout(Math.min(timeoutMs, 300_000))])
    try {
      this.worker ??= new Worker(new URL('./application-worker.ts', import.meta.url), { type: 'module' })
      const worker = this.worker
      return await new Promise<ApplicationResult>((resolve, reject) => {
        const cleanup = () => { worker.onmessage = null; worker.onerror = null; signal.removeEventListener('abort', abort) }
        const fail = (error: unknown) => { cleanup(); this.close(); reject(error) }
        const abort = () => fail(signal.reason)
        signal.addEventListener('abort', abort, { once: true })
        worker.onerror = () => fail(new Error('Browser runtime worker failed'))
        worker.onmessage = ({ data }) => {
          if (typeof data.progress === 'string') { progress(data.progress); return }
          if (data.error) { fail(new Error(String(data.error))); return }
          cleanup(); resolve(data as ApplicationResult)
        }
        if (signal.aborted) { abort(); return }
        worker.postMessage({ input, allowCpu })
      })
    } finally { this.busy = false }
  }
}
