/// <reference types="@webgpu/types" />
import type { ApplicationInput, ApplicationOutput, ApplicationRuntimeReport } from './application-types'

const scope = globalThis as unknown as { onmessage: ((event: MessageEvent) => void) | null; postMessage: (value: unknown) => void; location: Location; navigator: Navigator }
const REPOSITORY = 'Xenova/all-MiniLM-L6-v2'
const REVISION = '751bff37182d3f1213fa05d7196b954e230abad9'
const MODEL_HASH = '759c3cd2b7fe7e93933ad23c4c9181b7396442a2ed746ec7c1d46192c469c46e'
let device: GPUDevice | null = null
const pipelines = new Map<string, GPUComputePipeline>()
let extractor: Awaited<ReturnType<typeof loadExtractor>> | null = null
let extractorBackend: 'webgpu' | 'wasm' = 'wasm'
let modelFallback: string | null = null
const message = (progress: string) => scope.postMessage({ progress })
const reason = (error: unknown) => (error instanceof Error ? error.message : String(error)).replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, 256)

function validate(input: ApplicationInput) {
  if (input.operation === 'embedding') {
    if (input.model !== 'minilm-l6-v2' || !Array.isArray(input.texts) || !input.texts.length || input.texts.length > 4 || input.texts.some(text => typeof text !== 'string' || !text.trim() || new TextEncoder().encode(text).length > 2048)) throw new Error('Invalid bounded embedding input')
    return
  }
  if (!['vector_add', 'matmul'].includes(input.operation) || !Array.isArray(input.a) || !Array.isArray(input.b) || [...input.a, ...input.b].some(v => !Number.isFinite(v) || Math.abs(v) > 1e6)) throw new Error('Invalid numeric input')
  if (input.operation === 'vector_add') {
    if (!input.a.length || input.a.length > 4096 || input.a.length !== input.b.length) throw new Error('Vector size exceeds runtime limits')
  } else if (![input.rows, input.inner, input.cols].every(v => Number.isInteger(v) && v > 0 && v <= 128) || input.a.length !== input.rows * input.inner || input.b.length !== input.inner * input.cols || input.a.length + input.b.length > 8192 || input.rows * input.cols > 4096 || input.rows * input.inner * input.cols > 262144) throw new Error('Matrix size exceeds runtime limits')
}
function cpu(input: Exclude<ApplicationInput, { operation: 'embedding' }>): ApplicationOutput {
  if (input.operation === 'vector_add') return { values: input.a.map((a, i) => Math.fround(Math.fround(a) + Math.fround(input.b[i]))), shape: [input.a.length] }
  const values = new Float32Array(input.rows * input.cols)
  for (let row = 0; row < input.rows; row++) for (let col = 0; col < input.cols; col++) {
    let sum = 0
    for (let k = 0; k < input.inner; k++) sum += Math.fround(input.a[row * input.inner + k]) * Math.fround(input.b[k * input.cols + col])
    values[row * input.cols + col] = sum
  }
  return { values: Array.from(values), shape: [input.rows, input.cols] }
}
async function gpu(input: Exclude<ApplicationInput, { operation: 'embedding' }>): Promise<ApplicationOutput> {
  if (!device) {
    const adapter = await scope.navigator.gpu?.requestAdapter()
    if (!adapter) throw new Error('No WebGPU adapter is available')
    const created = await adapter.requestDevice(); device = created
    void created.lost.then(() => { if (device === created) { device = null; pipelines.clear() } })
  }
  const current = device
  const outputSize = input.operation === 'vector_add' ? input.a.length : input.rows * input.cols
  const key = input.operation === 'vector_add' ? 'vector_add' : `matmul:${input.rows}:${input.inner}:${input.cols}`
  let pipeline = pipelines.get(key)
  current.pushErrorScope('validation')
  const buffers: GPUBuffer[] = []
  try {
    if (!pipeline) {
      const expression = input.operation === 'vector_add' ? 'out[i] = a[i] + b[i];' : `var sum = 0.0; let row = i / ${input.cols}u; let col = i % ${input.cols}u; for (var k = 0u; k < ${input.inner}u; k++) { sum += a[row * ${input.inner}u + k] * b[k * ${input.cols}u + col]; } out[i] = sum;`
      const code = `@group(0) @binding(0) var<storage,read> a: array<f32>; @group(0) @binding(1) var<storage,read> b: array<f32>; @group(0) @binding(2) var<storage,read_write> out: array<f32>; @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) id: vec3<u32>) { let i=id.x; if (i < arrayLength(&out)) { ${expression} } }`
      pipeline = await current.createComputePipelineAsync({ layout: 'auto', compute: { module: current.createShaderModule({ code }), entryPoint: 'main' } })
      if (pipelines.size >= 16) pipelines.clear()
      pipelines.set(key, pipeline)
    }
    const create = (size: number, usage: GPUBufferUsageFlags) => { const buffer = current.createBuffer({ size, usage }); buffers.push(buffer); return buffer }
    const a = create(input.a.length * 4, GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST)
    const b = create(input.b.length * 4, GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST)
    const output = create(outputSize * 4, GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC)
    const read = create(outputSize * 4, GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ)
    current.queue.writeBuffer(a, 0, new Float32Array(input.a)); current.queue.writeBuffer(b, 0, new Float32Array(input.b))
    const bind = current.createBindGroup({ layout: pipeline.getBindGroupLayout(0), entries: [a, b, output].map((buffer, binding) => ({ binding, resource: { buffer } })) })
    const commands = current.createCommandEncoder(); const pass = commands.beginComputePass()
    pass.setPipeline(pipeline); pass.setBindGroup(0, bind); pass.dispatchWorkgroups(Math.ceil(outputSize / 64)); pass.end()
    commands.copyBufferToBuffer(output, 0, read, 0, outputSize * 4); current.queue.submit([commands.finish()])
    await read.mapAsync(GPUMapMode.READ)
    const values = Array.from(new Float32Array(read.getMappedRange().slice(0))); read.unmap()
    return { values, shape: input.operation === 'vector_add' ? [outputSize] : [input.rows, input.cols] }
  } finally {
    for (const buffer of buffers) buffer.destroy()
    const error = await current.popErrorScope()
    if (error) throw new Error(`WebGPU validation failed: ${error.message}`)
  }
}

async function loadExtractor(backend: 'webgpu' | 'wasm') {
  const { env, pipeline } = await import('@huggingface/transformers')
  env.allowLocalModels = false
  // Transformers also probes metadata with its default revision. Pin the URL
  // template itself so those probes cannot read a moving main branch.
  env.remotePathTemplate = `{model}/resolve/${REVISION}/`
  env.useBrowserCache = false; env.useFSCache = false; env.useWasmCache = false
  const wasm = env.backends.onnx.wasm
  if (!wasm) throw new Error('ONNX WASM backend is unavailable')
  wasm.numThreads = 1
  wasm.proxy = false
  wasm.wasmPaths = new URL('/ort/', scope.location.origin).href
  const allowed = (url: string) => {
    const parsed = new URL(url)
    return parsed.origin === 'https://huggingface.co' && parsed.pathname.startsWith(`/${REPOSITORY}/resolve/${REVISION}/`) && !parsed.search
  }
  const verified = async (url: string, response: Response) => {
    if (!allowed(url)) throw new Error('Runtime attempted to load an unapproved model asset')
    if (url.endsWith('/onnx/model.onnx') && response.ok) {
      message('Verifying pinned model integrity')
      const bytes = await response.clone().arrayBuffer()
      const hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(v => v.toString(16).padStart(2, '0')).join('')
      if (hash !== MODEL_HASH) throw new Error('Model integrity verification failed')
    }
    return response
  }
  env.fetch = async (input: string | URL, init?: RequestInit) => {
    const url = String(input)
    if (!allowed(url)) throw new Error(`Unapproved model asset: ${new URL(url).origin}${new URL(url).pathname}`)
    const response = await fetch(url, { ...init, credentials: 'omit' })
    // Metadata range probes never supply model bytes to an inference session.
    if (new Headers(init?.headers).has('Range')) return response
    return verified(url, response)
  }
  // Recheck cached model bytes too. No application input/output is ever cached.
  const cache = typeof caches !== 'undefined' ? await caches.open('mesh-minilm-pinned-v1') : null
  env.useCustomCache = !!cache
  env.customCache = cache ? {
    match: async (key: string) => { const response = await cache.match(key); if (!response) return undefined; try { return await verified(key, response) } catch (error) { await cache.delete(key); throw error } },
    put: async (key: string, response: Response) => { if (!allowed(key)) throw new Error('Unapproved model cache key'); await cache.put(key, response) },
  } : null
  message(`Loading pinned embedding model (${backend})`)
  const loaded = await pipeline('feature-extraction', REPOSITORY, { revision: REVISION, dtype: 'fp32', device: backend })
  Object.defineProperty(loaded.tokenizer, 'model_max_length', { value: 128 })
  return loaded
}

scope.onmessage = ({ data }: MessageEvent<{ input: ApplicationInput; allowCpu: boolean }>) => {
  void (async () => {
    const { input, allowCpu } = data; validate(input)
    const runtime: ApplicationRuntimeReport = { backend: 'cpu', compute_ms: 0, load_ms: 0, fallback_reason: null }
    let output: ApplicationOutput
    if (input.operation === 'embedding') {
      const loadStarted = performance.now()
      if (!extractor || (!allowCpu && extractorBackend !== 'webgpu')) {
        const adapter = await scope.navigator.gpu?.requestAdapter().catch(() => null)
        extractorBackend = adapter ? 'webgpu' : 'wasm'; modelFallback = adapter ? null : 'No WebGPU adapter is available'
        if (extractorBackend === 'wasm' && !allowCpu) throw new Error('WebGPU unavailable and CPU fallback is disabled')
        try { extractor = await loadExtractor(extractorBackend) }
        catch (error) {
          if (!allowCpu || extractorBackend === 'wasm') throw error
          modelFallback = reason(error); extractorBackend = 'wasm'; extractor = await loadExtractor('wasm')
        }
      }
      runtime.load_ms = performance.now() - loadStarted; runtime.backend = extractorBackend; runtime.fallback_reason = modelFallback
      message(`Running embeddings (${extractorBackend})`)
      const started = performance.now()
      let tensor
      let fallbackLoadMs = 0
      try { tensor = await extractor(input.texts, { pooling: 'mean', normalize: true }) }
      catch (error) {
        if (!allowCpu || extractorBackend !== 'webgpu') throw error
        modelFallback = reason(error)
        await extractor.dispose().catch(() => undefined)
        const reloading = performance.now()
        extractor = await loadExtractor('wasm'); extractorBackend = 'wasm'
        fallbackLoadMs = performance.now() - reloading
        runtime.load_ms += fallbackLoadMs
        runtime.backend = 'wasm'; runtime.fallback_reason = modelFallback
        tensor = await extractor(input.texts, { pooling: 'mean', normalize: true })
      }
      output = { values: Array.from(tensor.data, Number), shape: [...tensor.dims] }
      runtime.compute_ms = Math.max(0, performance.now() - started - fallbackLoadMs)
    } else {
      const started = performance.now()
      try { output = await gpu(input); runtime.backend = 'webgpu' }
      catch (error) {
        if (!allowCpu) throw error
        runtime.fallback_reason = reason(error); output = cpu(input)
      }
      runtime.compute_ms = performance.now() - started
    }
    if (output.values.some(value => !Number.isFinite(value))) throw new Error('Runtime produced a non-finite output')
    scope.postMessage({ output, runtime })
  })().catch(error => scope.postMessage({ error: reason(error) }))
}
