import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import ts from 'typescript'

const source = await readFile(new URL('../../src/api.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
const { api, isRevokedAccess } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)

test('only coordinator JSON access denials can forget a saved device', async () => {
  const originalFetch = globalThis.fetch
  const originalStorage = globalThis.localStorage
  globalThis.localStorage = { getItem: () => 'a'.repeat(64) }
  try {
    for (const [status, body, type, revoked] of [
      [404, 'proxy restart', 'text/plain', false],
      [403, '<h1>Space sleeping</h1>', 'text/html', false],
      [401, '{broken', 'application/json', false],
      [401, JSON.stringify({ source: 'mesh-coordinator', code: 'account_session_required', error: 'Sign in again' }), 'application/json', false],
      [404, JSON.stringify({ error: 'not found' }), 'application/json', false],
      [404, JSON.stringify({ source: 'mesh-coordinator', error: 'Cluster not found' }), 'application/json', true],
      [503, JSON.stringify({ source: 'mesh-coordinator', error: 'Unavailable' }), 'application/json', false],
    ]) {
      globalThis.fetch = async () => new Response(body, { status, headers: { 'Content-Type': type } })
      await assert.rejects(api('/api/clusters/id/nodes'), error => {
        assert.equal(isRevokedAccess(error), revoked)
        if (revoked) assert.equal(error.message, 'Cluster not found')
        return true
      })
    }
  } finally { globalThis.fetch = originalFetch; globalThis.localStorage = originalStorage }
})
