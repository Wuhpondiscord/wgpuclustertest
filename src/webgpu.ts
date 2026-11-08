/// <reference types="@webgpu/types" />
export class GpuValidationError extends Error {
  constructor(message = 'WebGPU batch validation failed; result was not checkpointed.') { super(message); this.name = 'GpuValidationError' }
}

const MAP_READ = 0x0001
const STORAGE = 0x0080
const COPY_SRC = 0x0004
const COPY_DST = 0x0008
const LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1

const VECTOR_ADD_SHADER = `
@group(0) @binding(0) var<storage, read> left: array<f32>;
@group(0) @binding(1) var<storage, read> right: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  let index = id.x;
  if (index < arrayLength(&output)) {
    output[index] = left[index] + right[index];
  }
}
`

function vectorInputs(start: number, end: number) {
  const count = end - start
  const left = new Float32Array(count)
  const right = new Float32Array(count)
  for (let i = 0; i < count; i++) {
    const n = start + i
    left[i] = ((n * 17) % 100_003) / 100_003
    right[i] = ((n * 29 + 7) % 100_019) / 100_019
  }
  return { left, right }
}

export function runVectorAddCpu(start: number, end: number) {
  const { left, right } = vectorInputs(start, end)
  const output = new Float32Array(left.length)
  for (let i = 0; i < output.length; i++) output[i] = left[i] + right[i]
  return output
}

/** Portable f32 checkpoint representation, bounded to 2.67 MB at 500k values. */
export function encodeVectorAddOutput(values: Float32Array) {
  let bytes = new Uint8Array(values.buffer, values.byteOffset, values.byteLength)
  if (!LITTLE_ENDIAN) {
    bytes = new Uint8Array(values.byteLength)
    const view = new DataView(bytes.buffer)
    for (let i = 0; i < values.length; i++) view.setFloat32(i * 4, values[i], true)
  }
  const encoder = bytes as Uint8Array & { toBase64?: () => string }
  if (typeof encoder.toBase64 === 'function') {
    return { encoding: 'f32-le-base64', elements: values.length, data: encoder.toBase64() }
  }
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 0x8000) binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000))
  return { encoding: 'f32-le-base64', elements: values.length, data: btoa(binary) }
}

export async function digestVectorAddResult(values: Float32Array) {
  const result = await crypto.subtle.digest('SHA-256', values)
  return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, '0')).join('')
}

/** Creates a bounded WebGPU vector-add executor, or null when WebGPU is unavailable. */
export async function createVectorAddExecutor() {
  const gpu = navigator.gpu
  if (!gpu) return null
  const adapter = await gpu.requestAdapter()
  if (!adapter) return null
  const device = await adapter.requestDevice()
  let disposed = false
  let lost = false
  void device.lost.then(() => { lost = true })
  let pipeline: GPUComputePipeline
  try {
    const module = device.createShaderModule({ code: VECTOR_ADD_SHADER })
    pipeline = device.createComputePipeline({ layout: 'auto', compute: { module, entryPoint: 'main' } })
  } catch (error) { device.destroy(); throw error }

  type Buffers = { capacity: number; left: GPUBuffer; right: GPUBuffer; output: GPUBuffer; readback: GPUBuffer }
  let buffers: Buffers | undefined
  let binding: { bytes: number; group: GPUBindGroup } | undefined
  let running = false
  let validationMs = 0
  let readbackMs = 0
  let reusedBuffers = false

  function releaseBuffers() {
    if (!buffers) return
    const { left, right, output, readback } = buffers
    buffers = undefined
    binding = undefined
    for (const buffer of [left, right, output, readback]) buffer.destroy()
  }

  function allocate(bytes: number): Buffers {
    const allocated: GPUBuffer[] = []
    const create = (usage: GPUBufferUsageFlags) => {
      const buffer = device.createBuffer({ size: bytes, usage })
      allocated.push(buffer)
      return buffer
    }
    try {
      return {
        capacity: bytes,
        left: create(STORAGE | COPY_DST), right: create(STORAGE | COPY_DST),
        output: create(STORAGE | COPY_SRC), readback: create(MAP_READ | COPY_DST),
      }
    } catch (error) {
      for (const buffer of allocated) buffer.destroy()
      throw error
    }
  }

  return {
    get validationMs() { return validationMs },
    get readbackMs() { return readbackMs },
    get reusedBuffers() { return reusedBuffers },
    async run(start: number, end: number, signal?: AbortSignal) {
      if (disposed || lost) throw new Error('WebGPU device was lost. Reconnect this node and retry the job.')
      if (running) throw new Error('A WebGPU vector batch is already in progress.')
      signal?.throwIfAborted()
      const count = end - start
      if (!Number.isSafeInteger(count) || count <= 0 || count > 500_000) throw new Error('WebGPU batch exceeds the safe 500,000-element limit.')
      const byteLength = count * Float32Array.BYTES_PER_ELEMENT
      if (byteLength > device.limits.maxStorageBufferBindingSize || byteLength > device.limits.maxBufferSize) {
        throw new Error('This GPU cannot fit one batch within its storage-buffer limits.')
      }

      running = true
      try {
        reusedBuffers = buffers !== undefined && buffers.capacity >= byteLength
        if (!buffers || buffers.capacity < byteLength) {
          releaseBuffers()
          buffers = allocate(byteLength)
        }
        const { left: leftBuffer, right: rightBuffer, output, readback } = buffers
        const { left, right } = vectorInputs(start, end)
        device.queue.writeBuffer(leftBuffer, 0, left)
        device.queue.writeBuffer(rightBuffer, 0, right)
        // Bind only the logical range when a short final batch reuses larger buffers.
        if (binding?.bytes !== byteLength) {
          binding = { bytes: byteLength, group: device.createBindGroup({
            layout: pipeline.getBindGroupLayout(0),
            entries: [leftBuffer, rightBuffer, output].map((buffer, index) => ({
              binding: index, resource: { buffer, size: byteLength },
            })),
          }) }
        }
        const encoder = device.createCommandEncoder()
        const pass = encoder.beginComputePass()
        pass.setPipeline(pipeline)
        pass.setBindGroup(0, binding.group)
        pass.dispatchWorkgroups(Math.ceil(count / 64))
        pass.end()
        encoder.copyBufferToBuffer(output, 0, readback, 0, byteLength)
        device.queue.submit([encoder.finish()])

        const readbackStarted = performance.now()
        const timeout = AbortSignal.timeout(15_000)
        const readSignal = signal ? AbortSignal.any([signal, timeout]) : timeout
        let cancel = () => {}
        const cancelled = new Promise<never>((_, reject) => {
          cancel = () => reject(readSignal.reason)
          if (readSignal.aborted) cancel()
          else readSignal.addEventListener('abort', cancel, { once: true })
        })
        let result: Float32Array
        try {
          await Promise.race([readback.mapAsync(MAP_READ, 0, byteLength), cancelled])
          readSignal.throwIfAborted()
          // The copy owns its bytes after unmap invalidates the mapped range.
          result = new Float32Array(readback.getMappedRange(0, byteLength).slice(0))
          readback.unmap()
          readbackMs = performance.now() - readbackStarted
        } finally {
          readSignal.removeEventListener('abort', cancel)
        }

        const validationStarted = performance.now()
        for (let i = 0; i < count; i++) {
          const expected = Math.fround(left[i] + right[i])
          if (!Number.isFinite(result[i]) || result[i] !== expected) throw new GpuValidationError()
        }
        validationMs = performance.now() - validationStarted
        return result
      } catch (error) {
        // Destroy a failed or pending map before these buffers can be used again.
        releaseBuffers()
        throw error
      } finally {
        running = false
      }
    },
    close() {
      if (!disposed) {
        disposed = true
        releaseBuffers()
        device.destroy()
      }
    },
  }
}
