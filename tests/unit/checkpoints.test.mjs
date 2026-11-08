import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import ts from 'typescript'

async function loadSource(path) {
  const source = await readFile(new URL(path, import.meta.url), 'utf8')
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
  return import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
}
const { encode2Plus1, reconstruct2Plus1 } = await loadSource('../../src/sharding.ts')
const { runVectorAddCpu, encodeVectorAddOutput, digestVectorAddResult, createVectorAddExecutor } = await loadSource('../../src/webgpu.ts')
const vectors = JSON.parse(await readFile(new URL('../fixtures/sharding-vectors.json', import.meta.url)))

test('shared Rust/TypeScript coding vectors survive every single shard loss', () => {
  for (const vector of vectors) {
    const encoded = encode2Plus1(Uint8Array.from(vector.input))
    assert.deepEqual(encoded.map(shard => Array.from(shard.bytes)), vector.shards)
    for (let missing = 0; missing < 3; missing++) {
      assert.deepEqual(Array.from(reconstruct2Plus1(encoded.filter(shard => shard.index !== missing), vector.input.length)), vector.input)
    }
  }
  assert.throws(() => encode2Plus1(new Uint8Array()))
  assert.throws(() => reconstruct2Plus1([{ index: 0, bytes: new Uint8Array(2) }, { index: 0, bytes: new Uint8Array(2) }], 4))
})

test('the maximum 500k vector checkpoint preserves all f32 bytes within the transfer limit', () => {
  const values = runVectorAddCpu(1234, 501234)
  const output = encodeVectorAddOutput(values)
  const artifact = Buffer.from(JSON.stringify({ type: 'mesh-compute-checkpoint-v3', output }))
  assert.equal(output.encoding, 'f32-le-base64')
  assert.equal(output.elements, 500000)
  assert.ok(artifact.length < 3 * 1024 * 1024)
  const raw = Buffer.from(output.data, 'base64')
  assert.equal(raw.length, 2000000)
  for (let i = 0; i < values.length; i++) assert.equal(raw.readFloatLE(i * 4), values[i])
  const encoded = encode2Plus1(artifact)
  assert.deepEqual(Buffer.from(reconstruct2Plus1([encoded[0], encoded[2]], artifact.length)), artifact)
})

test('vector digests hash only the bytes in a typed-array view', async () => {
  const values = new Float32Array([99, 1.25, 2.5, 99])
  assert.equal(await digestVectorAddResult(values.subarray(1, 3)), await digestVectorAddResult(new Float32Array([1.25, 2.5])))
})

test('native and legacy checkpoint encoders serialize the same little-endian subview', t => {
  const property = Object.getOwnPropertyDescriptor(Uint8Array.prototype, 'toBase64')
  t.after(() => {
    if (property) Object.defineProperty(Uint8Array.prototype, 'toBase64', property)
    else delete Uint8Array.prototype.toBase64
  })
  const values = new Float32Array([99, 1.25, -2.5, 99]).subarray(1, 3)
  const expected = Buffer.alloc(8)
  expected.writeFloatLE(1.25)
  expected.writeFloatLE(-2.5, 4)
  Object.defineProperty(Uint8Array.prototype, 'toBase64', { configurable: true, value: undefined })
  const fallback = encodeVectorAddOutput(values)
  assert.equal(fallback.data, expected.toString('base64'))
  let called = false
  Object.defineProperty(Uint8Array.prototype, 'toBase64', {
    configurable: true,
    value() { called = true; return Buffer.from(this).toString('base64') },
  })
  assert.deepEqual(encodeVectorAddOutput(values), fallback)
  assert.equal(called, true)
})

test('vector batches reuse bounded GPU buffers, return independent outputs, and discard a cancelled map', async t => {
  const gpu = Object.getOwnPropertyDescriptor(navigator, 'gpu')
  t.after(() => { if (gpu) Object.defineProperty(navigator, 'gpu', gpu); else delete navigator.gpu })
  const counts = { allocated: 0, destroyed: 0, bound: 0, submitted: 0, mapped: 0, devicesClosed: 0 }
  let stallMapping = false
  const device = {
    limits: { maxStorageBufferBindingSize: 2_000_000, maxBufferSize: 2_000_000 },
    lost: new Promise(() => {}),
    destroy() { counts.devicesClosed++ },
    createShaderModule() {},
    createComputePipeline() { return { getBindGroupLayout() {} } },
    createBuffer({ size }) {
      const bytes = new ArrayBuffer(size)
      let cancelMap
      counts.allocated++
      return {
        bytes,
        destroy() { counts.destroyed++; cancelMap?.(new Error('Buffer destroyed')) },
        async mapAsync(_mode, offset, length) {
          assert.equal(offset, 0)
          assert.ok(length <= size)
          counts.mapped++
          if (stallMapping) await new Promise((_, reject) => { cancelMap = reject })
        },
        getMappedRange(offset, length) { return bytes.slice(offset, offset + length) },
        unmap() {},
      }
    },
    createBindGroup({ entries }) {
      counts.bound++
      return entries.map(entry => entry.resource)
    },
    createCommandEncoder() {
      let resources, copy
      return {
        beginComputePass() {
          return {
            setPipeline() {}, setBindGroup(_index, group) { resources = group },
            dispatchWorkgroups(groups) { assert.equal(groups, Math.ceil(resources[2].size / 256)) }, end() {},
          }
        },
        copyBufferToBuffer(source, from, destination, to, length) { copy = { source, from, destination, to, length } },
        finish() {
          return () => {
            const [left, right, output] = resources.map(({ buffer, size }) => new Float32Array(buffer.bytes, 0, size / 4))
            for (let i = 0; i < output.length; i++) output[i] = left[i] + right[i]
            new Uint8Array(copy.destination.bytes).set(new Uint8Array(copy.source.bytes, copy.from, copy.length), copy.to)
          }
        },
      }
    },
    queue: {
      writeBuffer(buffer, offset, values) { new Float32Array(buffer.bytes, offset).set(values) },
      submit(commands) { counts.submitted++; for (const command of commands) command() },
    },
  }
  Object.defineProperty(navigator, 'gpu', { configurable: true, value: { requestAdapter: async () => ({ requestDevice: async () => device }) } })
  const executor = await createVectorAddExecutor()
  const first = await executor.run(100, 200)
  assert.equal(executor.reusedBuffers, false)
  assert.deepEqual(first, runVectorAddCpu(100, 200))
  assert.deepEqual(await executor.run(200, 300), runVectorAddCpu(200, 300))
  assert.equal(executor.reusedBuffers, true)
  assert.equal(counts.allocated, 4)
  assert.equal(counts.bound, 1)
  assert.deepEqual(await executor.run(300, 351), runVectorAddCpu(300, 351))
  assert.equal(counts.allocated, 4)
  assert.equal(counts.bound, 2)
  assert.deepEqual(first, runVectorAddCpu(100, 200))
  assert.deepEqual(await executor.run(500, 700), runVectorAddCpu(500, 700))
  assert.equal(counts.allocated, 8)
  assert.equal(counts.destroyed, 4)

  stallMapping = true
  const controller = new AbortController()
  const pending = executor.run(0, 50, controller.signal)
  await assert.rejects(executor.run(0, 50), /already in progress/)
  controller.abort()
  await assert.rejects(pending, { name: 'AbortError' })
  assert.equal(counts.destroyed, 8)
  stallMapping = false
  assert.deepEqual(await executor.run(701, 731), runVectorAddCpu(701, 731))
  assert.equal(counts.allocated, 12)
  assert.equal(counts.submitted, 6)
  assert.equal(counts.mapped, 6)
  executor.close()
  executor.close()
  assert.equal(counts.destroyed, 12)
  assert.equal(counts.devicesClosed, 1)
})
