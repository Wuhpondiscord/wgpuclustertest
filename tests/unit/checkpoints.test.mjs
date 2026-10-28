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
const { runVectorAddCpu, encodeVectorAddOutput } = await loadSource('../../src/webgpu.ts')
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
