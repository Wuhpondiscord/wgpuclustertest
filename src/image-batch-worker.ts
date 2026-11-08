/// <reference types="@webgpu/types" />
import { fetchImageModel, imageSize, modelRepository, modelRevision } from './image-model'

type Start = { app: string; worker: string; identity?: string; invite?: string; browserKey: string }
type Work = { run_id: string; index: number; lease: string; image_rgb96: string }
type Backend = 'webgpu' | 'wasm'
const scope = globalThis as unknown as {
  onmessage: ((event: MessageEvent<Start | 'stop'>) => void) | null
  postMessage: (data: { message: string; stopped?: boolean }) => void
  navigator: Navigator
  location: Location
}
const report = (message: string, stopped = false) => scope.postMessage({ message, stopped })
let controller: AbortController | undefined

class RequestError extends Error {
  constructor(readonly status: number, message: string) { super(message) }
}
function wait(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    signal.throwIfAborted()
    const cleanup = () => { clearTimeout(timer); signal.removeEventListener('abort', abort) }
    const abort = () => { cleanup(); reject(signal.reason) }
    const timer = setTimeout(() => { cleanup(); resolve() }, ms)
    signal.addEventListener('abort', abort, { once: true })
  })
}
function untilAborted<T>(promise: Promise<T>, signal: AbortSignal) {
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason)
    signal.addEventListener('abort', abort, { once: true })
    if (signal.aborted) abort()
    void promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort))
  })
}
const reason = (error: unknown) => error instanceof Error ? error.message : String(error)

async function load(backend: Backend) {
  const { env, AutoImageProcessor, AutoModelForImageClassification, RawImage } = await import('@huggingface/transformers')
  env.allowLocalModels = false
  env.remotePathTemplate = `{model}/resolve/${modelRevision}/`
  env.fetch = fetchImageModel
  env.useBrowserCache = false
  env.useFSCache = false
  env.useCustomCache = false
  env.useWasmCache = false
  const wasm = env.backends.onnx.wasm
  if (!wasm) throw new Error('ONNX WASM backend is unavailable')
  wasm.numThreads = 1
  wasm.proxy = false
  wasm.wasmPaths = new URL('/ort/', scope.location.origin).href
  const [processor, model] = await Promise.all([
    AutoImageProcessor.from_pretrained(modelRepository, { revision: modelRevision }),
    AutoModelForImageClassification.from_pretrained(modelRepository, { revision: modelRevision, dtype: 'fp32', device: backend }),
  ])
  return { processor, model, RawImage, backend }
}

async function classify(runtime: Awaited<ReturnType<typeof load>>, input: string) {
  if (input.length !== imageSize * imageSize * 4) throw new Error('Invalid bounded image payload')
  const rgb = Uint8Array.from(atob(input), char => char.charCodeAt(0))
  const image = new runtime.RawImage(rgb, imageSize, imageSize, 3)
  const inputs = await runtime.processor(image)
  const output = await runtime.model(inputs)
  try {
    const logits = Array.from(output.logits.data, Number)
    if (logits.length !== 1000 || logits.some(value => !Number.isFinite(value))) throw new Error('Invalid model output')
    const max = Math.max(...logits)
    const probabilities = logits.map(value => Math.exp(value - max))
    const sum = probabilities.reduce((total, value) => total + value, 0)
    return probabilities.map((score, class_id) => ({ class_id, score: score / sum }))
      .sort((left, right) => right.score - left.score).slice(0, 3)
  } finally {
    output.logits.dispose()
    inputs.pixel_values.dispose()
  }
}

async function run(config: Start, signal: AbortSignal) {
  const base = `/api/applications/${encodeURIComponent(config.app)}/image-work`
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (config.invite) {
    headers['X-Mesh-ML-Invite'] = config.invite
    headers['X-Mesh-ML-Browser'] = config.browserKey
  } else if (config.identity) headers['X-Mesh-Identity'] = config.identity
  async function post<T>(path: string, body: unknown, requestSignal: AbortSignal) {
    const response = await fetch(base + path, {
      method: 'POST', headers, body: JSON.stringify(body), signal: requestSignal, redirect: 'error',
    })
    if (!response.ok) {
      const error = await response.json().catch(() => ({})) as { error?: string }
      throw new RequestError(response.status, error.error ?? `Coordinator returned ${response.status}`)
    }
    return (response.status === 204 ? undefined : await response.json()) as T
  }
  let runtime: Awaited<ReturnType<typeof load>> | undefined
  let completed = 0
  let claimErrors = 0
  while (!signal.aborted) {
    report(`Waiting for images · ${completed} completed`)
    let work: Work | null
    try {
      work = await post<Work | null>('/claim', { worker_id: config.worker, warm: Boolean(runtime) }, AbortSignal.any([signal, AbortSignal.timeout(20_000)]))
      claimErrors = 0
    } catch (error) {
      signal.throwIfAborted()
      if (error instanceof RequestError && [401, 403, 404].includes(error.status)) throw error
      await wait(Math.min(15_000, 500 * 2 ** Math.min(claimErrors++, 5)) + Math.random() * 500, signal)
      continue
    }
    if (!work) { await wait(Math.random() * 800, signal); continue }
    const path = `/${work.run_id}`
    const attempt = { worker_id: config.worker, index: work.index, lease: work.lease }
    const task = new AbortController()
    const taskSignal = AbortSignal.any([task.signal, signal, AbortSignal.timeout(180_000)])
    const started = performance.now()
    let accepted = false
    const renew = (async () => {
      let renewedAt = performance.now()
      while (!taskSignal.aborted) {
        await wait(12_000, taskSignal)
        try {
          await post(path + '/renew', attempt, AbortSignal.any([taskSignal, AbortSignal.timeout(6_000)]))
          renewedAt = performance.now()
        } catch (error) {
          if ((error instanceof RequestError && [401, 403, 404, 409].includes(error.status)) || performance.now() - renewedAt > 32_000) {
            task.abort(error)
            return
          }
        }
      }
    })().catch(() => undefined)
    try {
      let load_ms = 0
      const loadRuntime = async (device: Backend) => {
        report(`Loading MobileNetV4 on ${device.toUpperCase()}`)
        const at = performance.now()
        const loaded = await untilAborted(load(device), taskSignal)
        load_ms += performance.now() - at
        return loaded
      }
      if (!runtime) {
        const adapter = await untilAborted(scope.navigator.gpu?.requestAdapter().catch(() => null) ?? Promise.resolve(null), taskSignal)
        if (adapter) {
          try { runtime = await loadRuntime('webgpu') }
          catch { taskSignal.throwIfAborted() }
        }
        runtime ??= await loadRuntime('wasm')
      }
      report(`Classifying image ${work.index + 1} · ${runtime.backend.toUpperCase()}`)
      const computeStarted = performance.now()
      const initialLoad = load_ms
      let predictions
      try { predictions = await untilAborted(classify(runtime, work.image_rgb96), taskSignal) }
      catch (error) {
        taskSignal.throwIfAborted()
        if (runtime.backend !== 'webgpu') throw error
        await untilAborted(runtime.model.dispose(), taskSignal)
        runtime = await loadRuntime('wasm')
        predictions = await untilAborted(classify(runtime, work.image_rgb96), taskSignal)
      }
      const result = { ...attempt, predictions, backend: runtime.backend, load_ms,
        compute_ms: Math.max(0, performance.now() - computeStarted - (load_ms - initialLoad)) }
      for (let retry = 0; ; retry++) {
        try {
          await post(path + '/commit', result, AbortSignal.any([taskSignal, AbortSignal.timeout(8_000)]))
          accepted = true
          completed++
          break
        } catch (error) {
          if (retry === 2 || (error instanceof RequestError && error.status < 500)) throw error
          await wait(500 * (retry + 1), taskSignal)
        }
      }
    } catch (error) {
      signal.throwIfAborted()
      if (taskSignal.aborted || !(error instanceof RequestError && [404, 409].includes(error.status))) throw error
    } finally {
      task.abort()
      await renew
      if (!accepted) await post(path + '/release', attempt, AbortSignal.timeout(4_000)).catch(() => undefined)
    }
    // Leave time for the user's own browser activity between assignments.
    await wait(Math.min(2000, performance.now() - started), signal)
  }
}

scope.onmessage = ({ data }) => {
  if (data === 'stop') {
    controller?.abort()
    return
  }
  if (controller) return
  const current = new AbortController()
  controller = current
  void run(data, current.signal).then(
    () => report('ML contribution stopped', true),
    error => report(current.signal.aborted ? 'ML contribution stopped' : `ML worker stopped · ${reason(error)}`, true),
  )
}
