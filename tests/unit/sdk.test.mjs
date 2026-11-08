import test from 'node:test'
import assert from 'node:assert/strict'
import { MeshClient } from '../../public/sdk/mesh-client.mjs'
const applicationId = '12345678-1234-1234-1234-123456789abc'
const jobId = '12345678-1234-1234-1234-123456789def'
const token = `mak_${'a'.repeat(64)}`
test('SDK scopes credentials, serializes typed arrays and preserves explicit idempotency', async () => {
  const calls = []
  const mesh = new MeshClient({ endpoint: 'https://mesh.example', applicationId, token, fetch: async (url, init) => {
    calls.push({ url, init }); return Response.json({ id: jobId, status: 'queued' }, { status: 202 })
  } })
  await mesh.vectorAdd(new Float32Array([1, 2]), [3, 4], { idempotencyKey: 'stable-submit' })
  const { url, init } = calls[0]
  assert.equal(url, `https://mesh.example/api/applications/${applicationId}/jobs`)
  assert.ok(!url.includes(token)); assert.ok(!JSON.stringify(mesh).includes(token))
  assert.equal(init.credentials, 'omit'); assert.equal(init.headers.Authorization, `Bearer ${token}`)
  assert.equal(init.headers['Idempotency-Key'], 'stable-submit')
  assert.deepEqual(JSON.parse(init.body), { operation: 'vector_add', a: [1, 2], b: [3, 4] })
  await mesh.embeddings(['hello'])
  assert.equal(calls[1].url.endsWith('/inference'), true)
  assert.deepEqual(JSON.parse(calls[1].init.body), { model: 'minilm-l6-v2', input: ['hello'], verification: 'replicated' })
  await mesh.getDeployment()
  assert.equal(calls[2].url.endsWith('/deployment'), true)
  assert.equal(calls[2].init.method, 'GET')
  await mesh.runDeployment(jobId, { idempotencyKey: 'stable-workflow' })
  assert.equal(calls[3].url.endsWith('/deployment/run'), true)
  assert.deepEqual(JSON.parse(calls[3].init.body), { revision_id: jobId })
  assert.equal(calls[3].init.headers['Idempotency-Key'], 'stable-workflow')
})
test('SDK polls asynchronous jobs, propagates failures and cancellation', async () => {
  let reads = 0
  const mesh = new MeshClient({ endpoint: 'http://127.0.0.1', applicationId, token, fetch: async () => Response.json({ id: jobId, status: ++reads < 2 ? 'queued' : 'completed', output: { values: [3], shape: [1] } }) })
  assert.equal((await mesh.wait(jobId, { pollMs: 100 })).status, 'completed')
  mesh.fetch = async () => Response.json({ id: jobId, status: 'failed', error: 'Results disagree' })
  await assert.rejects(mesh.wait(jobId), /Results disagree/)
  mesh.fetch = async () => Response.json({ source: 'mesh-coordinator', error: 'Revoked' }, { status: 401 })
  await assert.rejects(mesh.getJob(jobId), error => error.status === 401 && error.message === 'Revoked')
  const controller = new AbortController(); controller.abort(new Error('Stopped'))
  await assert.rejects(mesh.wait(jobId, { signal: controller.signal }), /Stopped/)
  assert.throws(() => new MeshClient({ endpoint: 'http://remote.example', applicationId, token }), /HTTPS/)
  assert.throws(() => new MeshClient({ endpoint: 'https://mesh.example', applicationId, token: 'a'.repeat(64) }), /scoped/)
})
