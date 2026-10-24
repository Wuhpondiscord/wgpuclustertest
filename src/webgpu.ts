type GpuBuffer = {
  destroy(): void
  mapAsync(mode: number): Promise<void>
  getMappedRange(): ArrayBuffer
  unmap(): void
}

type GpuComputePass = {
  setPipeline(pipeline: unknown): void
  setBindGroup(index: number, group: unknown): void
  dispatchWorkgroups(count: number): void
  end(): void
}

type GpuDevice = {
  limits: { maxStorageBufferBindingSize: number; maxBufferSize: number }
  queue: { writeBuffer(buffer: GpuBuffer, offset: number, data: BufferSource): void; submit(commands: unknown[]): void }
  createBuffer(descriptor: { size: number; usage: number; mappedAtCreation?: boolean }): GpuBuffer
  createShaderModule(descriptor: { code: string }): unknown
  createComputePipeline(descriptor: { layout: string; compute: { module: unknown; entryPoint: string } }): unknown
  createBindGroup(descriptor: { layout: unknown; entries: Array<{ binding: number; resource: { buffer: GpuBuffer } }> }): unknown
  createCommandEncoder(): {
    beginComputePass(): GpuComputePass
    copyBufferToBuffer(source: GpuBuffer, sourceOffset: number, destination: GpuBuffer, destinationOffset: number, size: number): void
    finish(): unknown
  }
  destroy(): void
  lost: Promise<{ message: string }>
}

type GpuAdapter = { requestDevice(): Promise<GpuDevice> }
type GpuNavigator = { requestAdapter(): Promise<GpuAdapter | null> }

const MAP_READ = 0x0001
const STORAGE = 0x0080
const COPY_SRC = 0x0004
const COPY_DST = 0x0008

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

export function runVectorAddCpu(start: number, end: number) {
  const count = end - start
  const left = new Float32Array(count)
  const right = new Float32Array(count)
  const output = new Float32Array(count)
  for (let i = 0; i < count; i++) {
    const n = start + i
    left[i] = ((n * 17) % 100_003) / 100_003
    right[i] = ((n * 29 + 7) % 100_019) / 100_019
    output[i] = left[i] + right[i]
  }
  return output
}

export async function digestVectorAddResult(values: Float32Array) {
  const bytes = values.slice().buffer
  const result = await crypto.subtle.digest('SHA-256', bytes)
  return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, '0')).join('')
}

/** Creates a bounded WebGPU vector-add worker, or null when WebGPU is unavailable. */
export async function createVectorAddWorker() {
  const gpu = (navigator as Navigator & { gpu?: GpuNavigator }).gpu
  if (!gpu) return null
  const adapter = await gpu.requestAdapter()
  if (!adapter) return null
  const device = await adapter.requestDevice()
  let disposed = false
  let lost = false
  void device.lost.then(() => { lost = true })

  return {
    async run(start: number, end: number) {
      if (disposed || lost) throw new Error('WebGPU device was lost. Reconnect this node and retry the job.')
      const count = end - start
      if (!Number.isSafeInteger(count) || count <= 0 || count > 500_000) throw new Error('WebGPU batch exceeds the safe 500,000-element limit.')
      const byteLength = count * Float32Array.BYTES_PER_ELEMENT
      if (byteLength > device.limits.maxStorageBufferBindingSize || byteLength > device.limits.maxBufferSize) {
        throw new Error('This GPU cannot fit one batch within its storage-buffer limits.')
      }

      const left = new Float32Array(count)
      const right = new Float32Array(count)
      for (let i = 0; i < count; i++) {
        const n = start + i
        left[i] = ((n * 17) % 100_003) / 100_003
        right[i] = ((n * 29 + 7) % 100_019) / 100_019
      }

      const usage = STORAGE | COPY_DST
      const leftBuffer = device.createBuffer({ size: byteLength, usage })
      const rightBuffer = device.createBuffer({ size: byteLength, usage })
      const outputBuffer = device.createBuffer({ size: byteLength, usage: STORAGE | COPY_SRC })
      const readbackBuffer = device.createBuffer({ size: byteLength, usage: MAP_READ | COPY_DST })
      try {
        device.queue.writeBuffer(leftBuffer, 0, left)
        device.queue.writeBuffer(rightBuffer, 0, right)
        const module = device.createShaderModule({ code: VECTOR_ADD_SHADER })
        const pipeline = device.createComputePipeline({ layout: 'auto', compute: { module, entryPoint: 'main' } })
        const bindGroup = device.createBindGroup({
          layout: (pipeline as { getBindGroupLayout(index: number): unknown }).getBindGroupLayout(0),
          entries: [
            { binding: 0, resource: { buffer: leftBuffer } },
            { binding: 1, resource: { buffer: rightBuffer } },
            { binding: 2, resource: { buffer: outputBuffer } },
          ],
        })
        const encoder = device.createCommandEncoder()
        const pass = encoder.beginComputePass()
        pass.setPipeline(pipeline)
        pass.setBindGroup(0, bindGroup)
        pass.dispatchWorkgroups(Math.ceil(count / 64))
        pass.end()
        encoder.copyBufferToBuffer(outputBuffer, 0, readbackBuffer, 0, byteLength)
        device.queue.submit([encoder.finish()])
        await readbackBuffer.mapAsync(MAP_READ)
        const result = new Float32Array(readbackBuffer.getMappedRange().slice(0))
        readbackBuffer.unmap()

        // Check a small deterministic sample on the CPU so shader or transfer errors fail the batch.
        for (const i of [0, Math.floor(count / 2), count - 1]) {
          const expected = left[i] + right[i]
          if (Math.abs(result[i] - expected) > 1e-6) throw new Error('WebGPU batch validation failed; result was not checkpointed.')
        }
        return result
      } finally {
        leftBuffer.destroy()
        rightBuffer.destroy()
        outputBuffer.destroy()
        readbackBuffer.destroy()
      }
    },
    close() {
      if (!disposed) { disposed = true; device.destroy() }
    },
  }
}

