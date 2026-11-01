import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import ts from 'typescript'

const source = await readFile(new URL('../../src/runner-policy.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
const { WorkWake, delay, retryDelay, isTransient, isCustodyConflict } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)

test('transient retries are bounded and distinguish fenced/auth failures from custody replanning', () => {
  assert.equal(retryDelay(0, () => 0.5), 500)
  assert.equal(retryDelay(100, () => 0.5), 30000)
  assert.ok(retryDelay(100, () => 1) <= 36000)
  for (const status of [408, 429, 500, 503]) assert.ok(isTransient({ status }))
  for (const status of [400, 401, 403, 409, 422]) assert.ok(!isTransient({ status }))
  assert.ok(isTransient(new TypeError('connection lost')))
  assert.ok(isCustodyConflict(Object.assign(new Error('Two active cluster nodes must acknowledge the replicated checkpoint'), { status: 409 })))
  assert.ok(!isCustodyConflict(Object.assign(new Error('This worker lease is stale'), { status: 409 })))
})

test('work hints wake bounded polling and cancellation removes the idle wait', async () => {
  const event = new WorkWake()
  const signal = new AbortController()
  event.wake()
  await event.wait(10000, signal.signal)
  const pending = event.wait(10000, signal.signal)
  event.wake()
  await pending
  const stopped = event.wait(10000, signal.signal)
  signal.abort(new Error('stopped'))
  await assert.rejects(stopped, /stopped/)
  await assert.rejects(delay(10000, signal.signal), /stopped/)
})
