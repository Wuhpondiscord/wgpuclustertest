import { randomBytes } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test, expect, type Browser } from '@playwright/test'
import type { ApplicationJob } from '../../src/application-types'
const origin = 'http://127.0.0.1:5173'
const manifest = JSON.parse(readFileSync(new URL('../../mesh.json', import.meta.url), 'utf8'))
let session: string
async function api<T>(path: string, method = 'GET', body?: unknown): Promise<T> {
  const response = await fetch(`${origin}${path}`, { method, headers: { 'X-Mesh-Identity': session, ...(body ? { 'Content-Type': 'application/json' } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) })
  if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`)
  return response.status === 204 ? undefined as T : response.json()
}
test.beforeAll(async () => {
  session = randomBytes(32).toString('hex')
  const account = await api<{ session: string }>('/api/accounts', 'POST', { username: `runtime-${randomBytes(4).toString('hex')}` })
  session = account.session
  await api('/api/account/activate', 'POST')
})
async function fixture(browser: Browser) {
  const cluster = await api<{ id: string }>('/api/clusters', 'POST', { name: 'application runtime e2e' })
  const app = await api<{ id: string }>(`/api/clusters/${cluster.id}/applications`, 'POST', { name: 'Browser runtime', slug: `runtime-${randomBytes(4).toString('hex')}` })
  const node = await api<{ node: { id: string }; token: string }>(`/api/clusters/${cluster.id}/nodes`, 'POST', { name: 'runtime browser' })
  const context = await browser.newContext()
  await context.addInitScript(({ session, node, clusterId }) => {
    sessionStorage.setItem('mesh.account-session', session)
    sessionStorage.setItem('mesh.node-tokens', JSON.stringify({ [node.node.id]: node.token }))
    sessionStorage.setItem('mesh.contribution-settings', JSON.stringify({ [node.node.id]: { clusterId, enabled: true, dutyCycle: 100, allowCpuFallback: true, pauseWhenHidden: false, allowInference: false } }))
  }, { session, node, clusterId: cluster.id })
  const page = await context.newPage()
  page.on('pageerror', error => console.error(error.message))
  page.on('response', response => { if (response.status() >= 400 && response.url().includes('/api/application-work/')) void response.text().then(text => console.error(`Application API ${response.status()}: ${text.slice(0, 200)}`)) })
  await page.goto('/')
  await expect(page.getByRole('button', { name: 'Stop contributing', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Browser runtime · owner', exact: true }).click()
  return { cluster, app, page, context, close: async () => { await context.close(); await api(`/api/clusters/${cluster.id}`, 'DELETE') } }
}

async function extraContributor(browser: Browser, clusterId: string, inference: boolean) {
  const node = await api<{ node: { id: string }; token: string }>(`/api/clusters/${clusterId}/nodes`, 'POST', { name: 'takeover browser' })
  const context = await browser.newContext()
  await context.addInitScript(({ session, node, clusterId, inference }) => {
    sessionStorage.setItem('mesh.account-session', session)
    sessionStorage.setItem('mesh.node-tokens', JSON.stringify({ [node.node.id]: node.token }))
    sessionStorage.setItem('mesh.contribution-settings', JSON.stringify({ [node.node.id]: { clusterId, enabled: true, dutyCycle: 100, allowCpuFallback: true, pauseWhenHidden: false, allowInference: inference } }))
  }, { session, node, clusterId, inference })
  const page = await context.newPage(); await page.goto('/')
  await expect(page.getByRole('button', { name: 'Stop contributing', exact: true })).toBeVisible()
  return { context, page, nodeId: node.node.id }
}

test('the built-in sample runs on a contributor and can be replaced by an upload or pinned GitHub import', async ({ browser }) => {
  const f = await fixture(browser)
  try {
    const { page } = f
    const panel = page.getByRole('region', { name: 'Application deployment' })
    await panel.getByRole('button', { name: 'Load sample workflow', exact: true }).click()
    await expect(panel.getByText('Workflow preview', { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Save deployment', exact: true }).click()
    await expect(panel.getByText(manifest.name, { exact: true })).toBeVisible()
    const saved = await api<{ id: string }>(`/api/applications/${f.app.id}/deployment`)
    await page.reload()
    await page.getByRole('button', { name: 'Browser runtime · owner', exact: true }).click()
    await expect(panel.getByText(manifest.name, { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Run deployment', exact: true }).click()
    const output = page.getByTestId('application-job')
    await expect(output.getByTestId('application-job-status')).toHaveText(['completed', 'completed'], { timeout: 60_000 })
    const jobs = await api<ApplicationJob[]>(`/api/applications/${f.app.id}/jobs`)
    expect(jobs).toHaveLength(2)
    expect(jobs.find(job => job.deployment?.name === 'add')?.output?.values).toEqual([4, 6])
    expect(jobs.find(job => job.deployment?.name === 'multiply')?.output?.values).toEqual([19, 22, 43, 50])
    expect(jobs.every(job => job.deployment?.revision_id === saved.id)).toBe(true)

    const uploaded = { ...manifest, name: 'Uploaded workflow' }
    await panel.getByText('Replace deployment', { exact: true }).click()
    await panel.getByLabel('Upload mesh.json').setInputFiles({
      name: 'mesh.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify(uploaded)),
    })
    await expect(panel.getByText('Workflow preview', { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Replace deployment', exact: true }).click()
    await expect(panel.getByText(uploaded.name, { exact: true })).toBeVisible()

    const sha = 'a'.repeat(40)
    const repository = 'mesh-test/workflow'
    const imported = { ...manifest, name: 'GitHub workflow' }
    await page.route(`https://api.github.com/repos/${repository}/commits?*`, async route => {
      expect(new URL(route.request().url()).searchParams.get('sha')).toBe('main')
      expect(route.request().headers().authorization).toBeUndefined()
      await route.fulfill({ json: [{ sha }], headers: { 'access-control-allow-origin': '*' } })
    })
    await page.route(`https://raw.githubusercontent.com/${repository}/${sha}/mesh.json`, async route => {
      expect(route.request().headers()['x-mesh-identity']).toBeUndefined()
      await route.fulfill({ json: imported, headers: { 'access-control-allow-origin': '*' } })
    })
    await panel.getByText('Replace deployment', { exact: true }).click()
    await panel.getByLabel('Public GitHub repository').fill(`https://github.com/${repository}`)
    await panel.getByLabel('Branch or commit').fill('main')
    await panel.getByRole('button', { name: 'Preview GitHub workflow', exact: true }).click()
    await expect(panel.getByText('Workflow preview', { exact: true })).toBeVisible()
    await expect(panel.getByText(sha, { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Replace deployment', exact: true }).click()
    await expect(panel.getByText(imported.name, { exact: true })).toBeVisible()
    const replacement = await api<{ id: string; source: unknown }>(`/api/applications/${f.app.id}/deployment`)
    expect(replacement.id).not.toBe(saved.id)
    expect(replacement.source).toEqual({ kind: 'github', repository, commit: sha })
    await expect(output.getByTestId('application-job-status')).toHaveText(['completed', 'completed'])
  } finally { await f.close() }
})

test('queued browser matrix jobs commit once after a lost response and scoped SDK reads results', async ({ browser }) => {
  const f = await fixture(browser)
  try {
    let dropped = false
    await f.page.route('**/api/application-work/*/commit', async route => {
      if (!dropped) { dropped = true; await route.fetch(); await route.abort(); return }
      await route.continue()
    })
    await f.page.getByLabel('Operation template').selectOption('matmul')
    await f.page.getByRole('button', { name: 'Queue application job', exact: true }).click()
    await expect(f.page.getByTestId('application-job-status')).toHaveText('completed', { timeout: 60_000 })
    await expect(f.page.getByTestId('application-job-output')).toContainText('50')
    const jobs = await api<ApplicationJob[]>(`/api/applications/${f.app.id}/jobs`)
    expect(jobs[0].output?.values).toEqual([19, 22, 43, 50]); expect(jobs[0].workers).toHaveLength(1); expect(dropped).toBe(true)
    if (process.env.MESH_TEST_GPU === '1') expect(jobs[0].workers[0].backend).toBe('webgpu')
    const key = await api<{ key: string }>(`/api/applications/${f.app.id}/keys`, 'POST', { label: 'e2e', read_only: true })
    const result = await f.page.evaluate(async ({ app, key, jobId }) => {
      const path = '/sdk/mesh-client.mjs'
      const { MeshClient } = await import(path)
      return new MeshClient({ endpoint: location.origin, applicationId: app, token: key }).wait(jobId)
    }, { app: f.app.id, key: key.key, jobId: jobs[0].id })
    expect(result.output.values).toEqual([19, 22, 43, 50])
    await f.page.getByRole('button', { name: 'Stop contributing', exact: true }).click()
    const pending = await api<ApplicationJob>(`/api/applications/${f.app.id}/jobs`, 'POST', { operation: 'vector_add', a: [10], b: [32] })
    await new Promise(resolve => setTimeout(resolve, 3000))
    expect((await api<ApplicationJob>(pending.status_url)).status).toBe('queued')
    await f.page.getByRole('button', { name: 'Contribute compute', exact: true }).click()
    await expect.poll(async () => (await api<ApplicationJob>(pending.status_url)).status, { timeout: 30_000 }).toBe('completed')
  } finally { await f.close() }
})

test('real pinned MiniLM embeddings execute only after model-download opt-in', async ({ browser }) => {
  test.setTimeout(300_000)
  const f = await fixture(browser)
  try {
    const job = await api<ApplicationJob>(`/api/applications/${f.app.id}/inference`, 'POST', {
      model: 'minilm-l6-v2', verification: 'single', input: ['A cat sits on a mat.', 'A kitten rests on a rug.', 'The stock exchange closed lower today.'],
    })
    await new Promise(resolve => setTimeout(resolve, 2500))
    expect((await api<ApplicationJob>(job.status_url)).status).toBe('queued')
    await f.page.getByLabel('ML inference (downloads a ~90 MB model plus runtime)', { exact: true }).check()
    await expect.poll(async () => {
      const current = await api<ApplicationJob>(job.status_url)
      if (current.status === 'failed') throw new Error(current.error ?? 'Inference failed')
      return current.status
    }, { timeout: 250_000, intervals: [1000, 2000] }).toBe('completed')
    const result = await api<ApplicationJob>(job.status_url)
    expect(result.verification).toBe('single_worker_unverified')
    expect(result.output?.shape).toEqual([3, 384])
    const v = result.output!.values
    const cosine = (a: number, b: number) => v.slice(a * 384, (a + 1) * 384).reduce((sum, value, i) => sum + value * v[b * 384 + i], 0)
    expect(cosine(0, 1)).toBeGreaterThan(cosine(0, 2) + 0.15)
    for (let row = 0; row < 3; row++) expect(cosine(row, row)).toBeCloseTo(1, 3)
    console.log(`Real MiniLM: ${result.workers[0].backend}, load ${Math.round(result.workers[0].load_ms)}ms, inference ${Math.round(result.workers[0].compute_ms)}ms, receipt ${result.result_received_elapsed_ms}ms`)
    const warm = await api<ApplicationJob>(`/api/applications/${f.app.id}/inference`, 'POST', { model: 'minilm-l6-v2', verification: 'single', input: ['A cat sits on a mat.'] })
    await expect.poll(async () => (await api<ApplicationJob>(warm.status_url)).status, { timeout: 30_000 }).toBe('completed')
    const warmed = await api<ApplicationJob>(warm.status_url)
    expect(warmed.output?.shape).toEqual([1, 384])
    expect(warmed.output!.values.reduce((sum, value, i) => sum + value * v[i], 0)).toBeGreaterThan(0.9995)
    expect(warmed.workers[0].load_ms).toBeLessThan(result.workers[0].load_ms)
    console.log(`Warm MiniLM: ${warmed.workers[0].backend}, load ${Math.round(warmed.workers[0].load_ms)}ms, inference ${Math.round(warmed.workers[0].compute_ms)}ms, receipt ${warmed.result_received_elapsed_ms}ms`)

    const second = await extraContributor(browser, f.cluster.id, true)
    try {
      const replicated = await api<ApplicationJob>(`/api/applications/${f.app.id}/inference`, 'POST', { model: 'minilm-l6-v2', input: ['A cat sits on a mat.'] })
      await expect.poll(async () => {
        const current = await api<ApplicationJob>(replicated.status_url)
        if (current.status === 'failed') throw new Error(current.error ?? 'Replica mismatch')
        return current.status
      }, { timeout: 120_000 }).toBe('completed')
      const verified = await api<ApplicationJob>(replicated.status_url)
      expect(verified.verification).toBe('two_node_agreement')
      expect(new Set(verified.workers.map(worker => worker.node_id)).size).toBe(2)
      expect(verified.output?.shape).toEqual([1, 384])
    } finally { await second.context.close() }

  } finally { await f.close() }
})

test('private application work is reclaimed after its compute browser disconnects', async ({ browser }) => {
  const f = await fixture(browser)
  let second: Awaited<ReturnType<typeof extraContributor>> | null = null
  try {
    const held = f.page.waitForRequest('**/api/application-work/*/commit')
    await f.page.route('**/api/application-work/*/commit', route => route.abort())
    const job = await api<ApplicationJob>(`/api/applications/${f.app.id}/jobs`, 'POST', { operation: 'vector_add', a: [11, 12], b: [31, 30] })
    await held
    await f.context.close()
    second = await extraContributor(browser, f.cluster.id, false)
    await expect.poll(async () => (await api<ApplicationJob>(job.status_url)).status, { timeout: 45_000 }).toBe('completed')
    const result = await api<ApplicationJob>(job.status_url)
    expect(result.output?.values).toEqual([42, 42])
    expect(result.workers).toHaveLength(1)
    expect(result.workers[0].node_id).toBe(second.nodeId)
    expect(result.workers[0].generation).toBeGreaterThan(1)
  } finally { await second?.context.close(); await f.close() }
})

test('WebGPU matrix kernels execute without CPU fallback when an adapter is available', async ({ browser }) => {
  const f = await fixture(browser)
  try {
    const available = await f.page.evaluate(async () => {
      const gpu = (navigator as unknown as { gpu?: { requestAdapter(): Promise<unknown> } }).gpu
      return Boolean(await gpu?.requestAdapter())
    })
    test.skip(!available, 'This test host has no usable WebGPU adapter; CPU/WASM tests run separately')
    await f.page.getByLabel('Allow CPU fallback', { exact: true }).uncheck()
    const job = await api<ApplicationJob>(`/api/applications/${f.app.id}/jobs`, 'POST', { operation: 'matmul', a: [1, 2, 3, 4], b: [5, 6, 7, 8], rows: 2, inner: 2, cols: 2 })
    await expect.poll(async () => (await api<ApplicationJob>(job.status_url)).status, { timeout: 30_000 }).toBe('completed')
    const result = await api<ApplicationJob>(job.status_url)
    expect(result.workers[0].backend).toBe('webgpu')
    expect(result.output?.values).toEqual([19, 22, 43, 50])
  } finally { await f.close() }
})
