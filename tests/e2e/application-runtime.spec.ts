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
  await page.getByRole('link', { name: 'Applications', exact: true }).click()
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

test('an image batch uses invited browsers and recovers a disconnected MobileNet worker', async ({ browser }) => {
  test.setTimeout(180_000)
  const f = await fixture(browser)
  const guests = [await browser.newContext(), await browser.newContext()]
  const pages = await Promise.all(guests.map(context => context.newPage()))
  try {
    await f.page.getByRole('button', { name: 'Runs', exact: true }).click()
    await f.page.getByText('Image classification (ML)', { exact: true }).click()
    await f.page.getByRole('button', { name: 'Share ML contributor link' }).click()
    const link = f.page.getByLabel('ML contributor link', { exact: true })
    await expect(link).toBeVisible()
    const url = await link.inputValue()
    for (const page of pages) {
      await page.goto(url)
      await expect(page.getByRole('heading', { name: 'Contribute to Browser runtime' })).toBeVisible()
      await page.getByRole('button', { name: 'Start ML worker' }).click()
      await expect(page.getByText('Waiting for images', { exact: false })).toBeVisible()
      expect(await page.evaluate(() => [localStorage.getItem('mesh.identity'), sessionStorage.getItem('mesh.node-tokens')])).toEqual([null, null])
    }
    await f.page.getByRole('button', { name: 'Try 32-image batch' }).click()
    await expect(f.page.getByTestId('image-batch-progress')).toContainText('32/32 classified', { timeout: 140_000 })
    await expect(f.page.getByTestId('image-batch-progress')).toContainText('2 contributing browsers')
    const [run] = await api<Array<{ id: string }>>(`/api/applications/${f.app.id}/image-batches`)
    const data = await api<{ results: Array<{ index: number; predictions: Array<{ class_id: number }> }> }>(`/api/applications/${f.app.id}/image-batches/${run.id}?results=true`)
    expect(data.results.map(row => row.index)).toEqual(Array.from({ length: 32 }, (_, index) => index))
    expect(data.results.every(row => row.predictions[0].class_id === 292)).toBe(true)

    for (const page of pages) {
      await page.getByRole('button', { name: 'Stop ML worker' }).click()
      await expect(page.getByRole('button', { name: 'Start ML worker' })).toBeVisible()
    }
    const [leaving, remaining] = pages
    await f.page.getByRole('button', { name: 'Try sample image' }).click()
    await expect(f.page.getByText('Queued 1 image', { exact: true })).toBeVisible()
    // Hold a real lease from the browser and close the page without a release.
    // Claim directly so inference timing and request interception cannot race the close.
    const abandoned = await leaving.evaluate(async ({ app, url }) => {
      const token = new URLSearchParams(new URL(url).hash.slice(1)).get('ml-invite')!
      const response = await fetch(`/api/applications/${app}/image-work/claim`, {
        method: 'POST', headers: {
          'Content-Type': 'application/json',
          'X-Mesh-ML-Invite': token,
          'X-Mesh-ML-Browser': crypto.randomUUID(),
        }, body: JSON.stringify({ worker_id: crypto.randomUUID() }),
      })
      if (!response.ok) throw new Error(`Could not claim the test image (${response.status})`)
      const work = await response.json() as { run_id: string; index: number } | null
      return work && { id: work.run_id, index: work.index }
    }, { app: f.app.id, url })
    expect(abandoned?.index).toBe(0)
    await guests[0].close()
    await remaining.getByRole('button', { name: 'Start ML worker' }).click()
    await expect(f.page.getByTestId('image-batch-progress')).toContainText('1/1 classified', { timeout: 65_000 })
    const retry = await api<{ results: Array<{ attempts: number; predictions: Array<{ class_id: number }> }> }>(`/api/applications/${f.app.id}/image-batches/${abandoned!.id}?results=true`)
    expect(retry.results[0].attempts).toBe(2)
    expect(retry.results[0].predictions[0].class_id).toBe(292)
  } finally {
    await Promise.all(guests.map(guest => guest.close()))
    await f.close()
  }
})

test('a guest joins from one link and passes the quick test without an account', async ({ browser }) => {
  const f = await fixture(browser)
  const guest = await browser.newContext()
  try {
    await f.page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await f.page.getByRole('button', { name: 'Stop contributing', exact: true }).click()
    await f.page.getByRole('link', { name: 'Applications', exact: true }).click()
    await f.page.getByRole('button', { name: 'Share contributor link' }).click()
    const link = f.page.getByLabel('Invite link', { exact: true })
    await expect(link).toBeVisible()
    const page = await guest.newPage()
    await page.goto(await link.inputValue())
    await expect(page.getByText('You’re invited to contribute to', { exact: false })).toBeVisible()
    await page.getByRole('button', { name: 'Join and contribute' }).click()
    await expect(page.getByRole('button', { name: 'Stop contributing', exact: true })).toBeVisible()
    expect(await page.evaluate(() => sessionStorage.getItem('mesh.account-session'))).toBeNull()
    await f.page.getByRole('button', { name: 'Runs', exact: true }).click()
    await f.page.getByRole('button', { name: 'Run quick test' }).click()
    await expect(f.page.getByText('Test passed. The verified result is [5,7,9].', { exact: true })).toBeVisible()
  } finally {
    await guest.close()
    await f.close()
  }
})

test('the built-in sample runs on a contributor and can be replaced by an upload or pinned GitHub import', async ({ browser }) => {
  const f = await fixture(browser)
  try {
    const { page } = f
    const panel = page.getByRole('region', { name: 'Application deployment' })
    await panel.getByRole('button', { name: 'Load sample workflow', exact: true }).click()
    await expect(panel.getByText('Workflow preview', { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
    await expect(panel.getByText(manifest.name, { exact: true })).toBeVisible()
    const saved = await api<{ id: string }>(`/api/applications/${f.app.id}/deployment`)
    await page.reload()
    await page.getByRole('link', { name: 'Applications', exact: true }).click()
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
    await page.getByRole('button', { name: 'Deployments', exact: true }).click()
    await panel.getByText('Replace deployment', { exact: true }).click()
    await panel.getByLabel('Upload mesh.json').setInputFiles({
      name: 'mesh.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify(uploaded)),
    })
    await expect(panel.getByText('Workflow preview', { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
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
    await panel.getByText('Workflow preview', { exact: true }).click()
    await expect(panel.getByText(sha, { exact: true })).toBeVisible()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
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
    await f.page.getByRole('button', { name: 'Runs', exact: true }).click()
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
    await f.page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await f.page.getByRole('button', { name: 'Stop contributing', exact: true }).click()
    await f.page.getByRole('link', { name: 'Applications', exact: true }).click()
    const pending = await api<ApplicationJob>(`/api/applications/${f.app.id}/jobs`, 'POST', { operation: 'vector_add', a: [10], b: [32] })
    await new Promise(resolve => setTimeout(resolve, 3000))
    expect((await api<ApplicationJob>(pending.status_url)).status).toBe('queued')
    await f.page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await f.page.getByRole('button', { name: 'Contribute compute', exact: true }).click()
    await f.page.getByRole('link', { name: 'Applications', exact: true }).click()
    await expect.poll(async () => (await api<ApplicationJob>(pending.status_url)).status, { timeout: 30_000 }).toBe('completed')
  } finally { await f.close() }
})

test('real pinned MiniLM embeddings execute only after model-download opt-in', async ({ browser }) => {
  test.setTimeout(300_000)
  const f = await fixture(browser)
  try {
    await f.page.getByRole('button', { name: 'Runs', exact: true }).click()
    await f.page.getByLabel('Operation template').selectOption('embedding')
    await f.page.getByLabel('Text to embed').fill('A cat sits on a mat.\nA kitten rests on a rug.\nThe stock exchange closed lower today.')
    await f.page.getByLabel('Result verification').selectOption('single')
    await f.page.getByRole('button', { name: 'Queue application job' }).click()
    await expect(f.page.getByTestId('application-job-status')).toHaveText('queued')
    const [job] = await api<ApplicationJob[]>(`/api/applications/${f.app.id}/jobs`)
    await new Promise(resolve => setTimeout(resolve, 2500))
    expect((await api<ApplicationJob>(job.status_url)).status).toBe('queued')
    await f.page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await f.page.getByLabel('ML inference (downloads a ~90 MB model plus runtime)', { exact: true }).check()
    await f.page.getByRole('link', { name: 'Applications', exact: true }).click()
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
    await f.page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await f.page.getByLabel('Allow CPU fallback', { exact: true }).uncheck()
    await f.page.getByRole('link', { name: 'Applications', exact: true }).click()
    const job = await api<ApplicationJob>(`/api/applications/${f.app.id}/jobs`, 'POST', { operation: 'matmul', a: [1, 2, 3, 4], b: [5, 6, 7, 8], rows: 2, inner: 2, cols: 2 })
    await expect.poll(async () => (await api<ApplicationJob>(job.status_url)).status, { timeout: 30_000 }).toBe('completed')
    const result = await api<ApplicationJob>(job.status_url)
    expect(result.workers[0].backend).toBe('webgpu')
    expect(result.output?.values).toEqual([19, 22, 43, 50])
  } finally { await f.close() }
})


test('application studio separates cluster controls and edits deployed workflow revisions', async ({ browser }) => {
  const f = await fixture(browser)
  try {
    const page = f.page
    await expect(page.getByRole('button', { name: 'New cluster', exact: true })).toBeHidden()
    const panel = page.getByRole('region', { name: 'Application deployment' })
    await panel.getByRole('button', { name: 'Load sample workflow' }).click()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
    await expect(panel.getByRole('button', { name: 'Edit deployed workflow' })).toBeVisible()
    const summary = page.getByRole('definition').filter({ hasText: 'Numerical checks' })
    await expect(summary).toBeVisible()
    await expect(page.getByRole('complementary', { name: 'Application context' }).getByRole('link', { name: 'application runtime e2e' })).toHaveAttribute('href', new RegExp(`cluster=${f.cluster.id}`))
    if (process.env.MESH_TEST_PREVIEW) {
      await page.setViewportSize({ width: 1440, height: 1000 })
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.screenshot({ path: 'test-results-ui/applications-desktop.png' })
    }
    const before = await api<{ id: string }>(`/api/applications/${f.app.id}/deployment`)
    await panel.getByRole('button', { name: 'Edit deployed workflow' }).click()
    const updated = { ...manifest, name: 'Edited numerical workflow' }
    await panel.getByLabel('Workflow JSON').fill(JSON.stringify(updated))
    await panel.getByRole('button', { name: 'Preview changes' }).click()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
    await expect(panel.getByText(updated.name, { exact: true })).toBeVisible()
    const after = await api<{ id: string }>(`/api/applications/${f.app.id}/deployment`)
    expect(after.id).not.toBe(before.id)
    await panel.getByRole('button', { name: 'Edit deployed workflow' }).click()
    await api(`/api/applications/${f.app.id}/deployment`, 'POST', { expected_revision_id: after.id, source: { kind: 'upload' }, manifest: { ...updated, name: 'Changed in another tab' } })
    await panel.getByRole('button', { name: 'Preview changes' }).click()
    await panel.getByRole('button', { name: 'Publish revision', exact: true }).click()
    await expect(panel.getByRole('alert')).toContainText('changed')
    await page.getByRole('button', { name: 'Access & settings' }).click()
    await page.getByLabel('New application name').fill('Updated project')
    await page.getByRole('button', { name: 'Save application name' }).click()
    await expect(page.getByRole('heading', { name: 'Updated project', exact: true })).toBeVisible()
    await page.getByRole('link', { name: 'Clusters', exact: true }).click()
    await expect(page.getByRole('button', { name: 'New cluster', exact: true })).toBeVisible()
    await expect(panel).toBeHidden()
  } finally { await f.close() }
})

test('vision studio trains a boxed dataset on a real browser worker and reloads its deployed model', async ({ browser }) => {
  test.setTimeout(300_000)
  const f = await fixture(browser)
  try {
    const page = f.page
    const overview = page.getByRole('region', { name: 'Vision Studio overview' })
    if (process.env.MESH_TEST_PREVIEW) {
      const deploymentPanel = page.getByRole('region', { name: 'Application deployment' })
      await deploymentPanel.getByRole('button', { name: 'Load sample workflow' }).click()
      await deploymentPanel.getByRole('button', { name: 'Publish revision' }).click()
    }
    await overview.getByRole('button', { name: 'Open labeling' }).click()
    const studio = page.getByRole('region', { name: 'Object detection studio' })
    await studio.getByRole('button', { name: 'Load box detection sample' }).click()
    await studio.getByRole('button', { name: 'Continue to training' }).click()
    await expect(studio.getByText('6 training images · 2 validation images', { exact: true })).toBeVisible()
    await studio.getByRole('button', { name: 'Start ML worker' }).click()
    await studio.getByRole('button', { name: 'Train detector on cluster' }).click()
    await expect(studio.getByTestId('detection-validation')).toContainText('Held-out detection', { timeout: 240_000 })
    console.log('Vision sample:', await studio.getByTestId('detection-validation').innerText())
    await expect(studio.getByText('Detector draft · 1 contributing browsers', { exact: false })).toBeVisible()
    await studio.getByRole('button', { name: 'Deploy detector', exact: true }).click()
    await expect(studio.getByRole('button', { name: 'Download deployed detector' })).toBeVisible()
    const deployment = await api<{ id: string; model: { labels: string[]; prototypes: string[]; counts: number[] } }>(`/api/applications/${f.app.id}/detector`)
    expect(deployment.model.labels).toEqual(['__background__', 'blue-block', 'red-block'])
    expect(deployment.model.counts).toEqual([6, 3, 3])
    expect(deployment.model.prototypes.every(value => value.length === 5336)).toBe(true)
    await studio.getByRole('button', { name: '1 · Label images' }).click()
    const downloadPromise = page.waitForEvent('download')
    await studio.getByRole('button', { name: 'Export labeled dataset' }).click()
    const dataset = JSON.parse(readFileSync((await (await downloadPromise).path())!, 'utf8'))
    await studio.getByRole('button', { name: '3 · Deploy & test' }).click()
    const image = Buffer.from(dataset.images[6].data.split(',')[1], 'base64')
    await studio.getByLabel('Test image', { exact: true }).setInputFiles({ name: 'new-test.jpg', mimeType: 'image/jpeg', buffer: image })
    await expect(studio.getByRole('button', { name: 'Detect objects on cluster' })).toBeEnabled()
    await studio.getByRole('button', { name: 'Detect objects on cluster' }).click()
    await expect(studio.getByText('Detection complete using the deployed revision.', { exact: true })).toBeVisible({ timeout: 120_000 })
    await studio.getByRole('button', { name: 'Stop ML worker' }).click()
    await page.getByRole('button', { name: 'Deployments', exact: true }).click()
    await expect(overview.getByRole('article', { name: 'Label images summary' })).toContainText('6 training · 2 held out')
    await expect(overview.getByRole('article', { name: 'Train and evaluate summary' })).toContainText('Precision')
    await expect(overview.getByRole('img', { name: 'Last test image' })).toBeVisible()
    if (process.env.MESH_TEST_PREVIEW) {
      await page.setViewportSize({ width: 1440, height: 1000 })
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.screenshot({ path: 'test-results-ui/applications-light-desktop.png' })
    }
    await overview.getByRole('button', { name: 'Test detector' }).click()
    await expect(studio.getByRole('button', { name: 'Detect objects on cluster' })).toBeVisible()
    if (process.env.MESH_TEST_PREVIEW) {
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.screenshot({ path: 'test-results-ui/vision-studio-light-desktop.png' })
    }
    await page.setViewportSize({ width: 390, height: 844 })
    const overflow = await page.evaluate(() => [...document.querySelectorAll('body *')].filter(element => element.getBoundingClientRect().right > innerWidth + 1).map(element => ({ tag: element.tagName, className: element.className, width: element.getBoundingClientRect().width, text: element.textContent?.slice(0, 60) })))
    expect(overflow).toEqual([])
    await page.screenshot({ path: 'test-results-ui/vision-studio-mobile.png', fullPage: true })
    await page.reload()
    await page.getByRole('button', { name: 'Browser runtime · owner', exact: true }).click()
    await page.getByRole('button', { name: 'Vision studio', exact: true }).click()
    await expect(page.getByRole('button', { name: 'Download deployed detector' })).toBeVisible()
    expect((await api<{ id: string }>(`/api/applications/${f.app.id}/detector`)).id).toBe(deployment.id)
  } finally { await f.close() }
})
