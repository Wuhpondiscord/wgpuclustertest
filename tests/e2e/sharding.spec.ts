import { randomBytes } from 'node:crypto'
import { expect, test, type Browser, type BrowserContext } from '@playwright/test'

const baseURL = 'http://127.0.0.1:5173'
const ownerIdentity = () => randomBytes(32).toString('hex')

async function api<T>(identity: string, path: string, method = 'GET', body?: unknown): Promise<T> {
  const response = await fetch(new URL(path, baseURL), {
    method,
    headers: { 'X-Mesh-Identity': identity, ...(body ? { 'Content-Type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  })
  if (!response.ok) throw new Error(`${method} ${path} failed (${response.status}): ${await response.text()}`)
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

async function waitFor<T>(read: () => Promise<T>, ready: (value: T) => boolean, label: string, timeoutMs = 45_000): Promise<T> {
  const stopAt = Date.now() + timeoutMs
  let last: T | undefined
  while (Date.now() < stopAt) {
    last = await read()
    if (ready(last)) return last
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  throw new Error(`Timed out waiting for ${label}; last value: ${JSON.stringify(last)}`)
}

async function openPeer(browser: Browser, identity: string, nodeId: string, token: string): Promise<{ context: BrowserContext; page: Awaited<ReturnType<BrowserContext['newPage']>> }> {
  const context = await browser.newContext()
  await context.addInitScript(({ meshIdentity, nodeId, token }) => {
    localStorage.setItem('mesh.identity', meshIdentity)
    sessionStorage.setItem('mesh.node-tokens', JSON.stringify({ [nodeId]: token }))
  }, { meshIdentity: identity, nodeId, token })
  const page = await context.newPage()
  await page.goto(`${baseURL}/?mesh-e2e=local`)
  await expect(page.getByText('Coordinator online')).toBeVisible()
  return { context, page }
}

test('cluster creation uses an in-app form instead of browser prompts', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('Coordinator online')).toBeVisible()
  await page.getByRole('button', { name: 'New cluster' }).click()
  const dialog = page.getByRole('dialog')
  await expect(dialog.getByRole('heading', { name: 'Create a cluster' })).toBeVisible()
  await dialog.getByLabel('Cluster name').fill('modal-smoke-test')
  await dialog.getByRole('button', { name: 'Create cluster' }).click()
  await expect(page.getByRole('heading', { name: 'modal-smoke-test' })).toBeVisible()
  await expect(page.getByText('Created “modal-smoke-test”')).toBeVisible()

  const identity = await page.evaluate(() => localStorage.getItem('mesh.identity') ?? '')
  const clusters = await api<Array<{ id: string; name: string }>>(identity, '/api/clusters')
  const created = clusters.find(cluster => cluster.name === 'modal-smoke-test')
  if (created) await api(identity, `/api/clusters/${created.id}`, 'DELETE')
})

test('reconstructs a 2+1 checkpoint after one peer goes offline', async ({ browser }) => {
  const identities = [ownerIdentity(), ownerIdentity(), ownerIdentity()]
  let clusterId = ''
  const contexts: BrowserContext[] = []
  try {
    const cluster = await api<{ id: string }>(identities[0], '/api/clusters', 'POST', { name: 'webrtc-sharding-e2e' })
    clusterId = cluster.id
    const invite = await api<{ token: string }>(identities[0], `/api/clusters/${clusterId}/invites`, 'POST')
    await Promise.all(identities.slice(1).map(identity => api(identity, '/api/invites/join', 'POST', { token: invite.token })))

    const createdNodes = await Promise.all(identities.map((identity, index) => api<{ node: { id: string }; token: string }>(
      identity, `/api/clusters/${clusterId}/nodes`, 'POST', { name: `e2e-peer-${index + 1}` },
    )))
    const peers = await Promise.all(createdNodes.map((node, index) => openPeer(browser, identities[index], node.node.id, node.token)))
    contexts.push(...peers.map(peer => peer.context))

    const nodesPath = `/api/clusters/${clusterId}/nodes`
    await waitFor(async () => api<Array<{ id: string; online: boolean; paused: boolean }>>(identities[0], nodesPath),
      nodes => nodes.length === 3 && nodes.every(node => node.online && !node.paused), 'three live WebSocket nodes')
    await expect(peers[0].page.locator('.node-row').filter({ hasText: 'Connected' })).toHaveCount(3)

    const job = await api<{ id: string }>(identities[0], `/api/clusters/${clusterId}/jobs`, 'POST', {
      workload: 'vector-add', total_units: 100_000, batch_units: 100_000,
    })
    const runButton = peers[0].page.getByRole('button', { name: 'Run on this node' })
    await expect(runButton).toBeEnabled()
    await runButton.click()

    const jobsPath = `/api/clusters/${clusterId}/jobs`
    const withCheckpoint = await waitFor(async () => api<Array<{
      id: string
      checkpoints: Array<{ batch_index: number; sha256: string; shards: Array<{ shard_index: number; node_id: string; sha256: string }> }>
    }>>(identities[0], jobsPath), jobs => {
      const current = jobs.find(item => item.id === job.id)
      return Boolean(current?.checkpoints[0]?.shards.length === 3)
    }, 'a committed 2+1 checkpoint', 90_000)
    const checkpoint = withCheckpoint.find(item => item.id === job.id)!.checkpoints[0]
    expect(checkpoint.shards.map(shard => shard.shard_index).sort()).toEqual([0, 1, 2])
    expect(new Set(checkpoint.shards.map(shard => shard.node_id)).size).toBe(3)
    expect(checkpoint.shards.every(shard => /^[0-9a-f]{64}$/i.test(shard.sha256))).toBe(true)

    const unavailable = checkpoint.shards.find(shard => shard.node_id !== createdNodes[0].node.id)!
    await api(identities[0], `/api/nodes/${unavailable.node_id}/pause`, 'POST')
    await waitFor(async () => api<Array<{ id: string; paused: boolean }>>(identities[0], nodesPath),
      nodes => nodes.some(node => node.id === unavailable.node_id && node.paused), 'a shard peer to pause')
    await expect(peers[0].page.locator('.node-row').filter({ hasText: 'Paused' })).toBeVisible()
    await peers[0].page.getByRole('button', { name: 'Test restore' }).click()
    await expect(peers[0].page.getByText(/Recovered batch 1 from 2 verified shard\(s\); full artifact SHA-256 matches\./)).toBeVisible({ timeout: 45_000 })
  } finally {
    await Promise.all(contexts.map(context => context.close()))
    if (clusterId) await api(identities[0], `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
  }
})

test('keeps a large checkpoint recoverable when a peer disconnects mid-transfer', async ({ browser }) => {
  const identities = [ownerIdentity(), ownerIdentity(), ownerIdentity()]
  let clusterId = ''
  const contexts: BrowserContext[] = []
  try {
    const cluster = await api<{ id: string }>(identities[0], '/api/clusters', 'POST', { name: 'webrtc-interrupted-transfer-e2e' })
    clusterId = cluster.id
    const invite = await api<{ token: string }>(identities[0], `/api/clusters/${clusterId}/invites`, 'POST')
    await Promise.all(identities.slice(1).map(identity => api(identity, '/api/invites/join', 'POST', { token: invite.token })))

    const createdNodes = await Promise.all(identities.map((identity, index) => api<{ node: { id: string }; token: string }>(
      identity, `/api/clusters/${clusterId}/nodes`, 'POST', { name: `e2e-fault-peer-${index + 1}` },
    )))
    const peers = await Promise.all(createdNodes.map((node, index) => openPeer(browser, identities[index], node.node.id, node.token)))
    contexts.push(...peers.map(peer => peer.context))
    const nodesPath = `/api/clusters/${clusterId}/nodes`
    await waitFor(async () => api<Array<{ id: string; online: boolean; paused: boolean }>>(identities[0], nodesPath),
      nodes => nodes.length === 3 && nodes.every(node => node.online && !node.paused), 'three live WebSocket nodes')

    const job = await api<{ id: string }>(identities[0], `/api/clusters/${clusterId}/jobs`, 'POST', {
      workload: 'vector-add', total_units: 100_000, batch_units: 100_000,
    })
    await peers[0].page.getByRole('button', { name: 'Run on this node' }).click()
    const transferNotice = peers[0].page.locator('.feedback.success').filter({ hasText: 'Sending checkpoint artifact' })
    await expect(transferNotice).toBeVisible({ timeout: 45_000 })
    const noticeText = await transferNotice.textContent()
    const failedPeerId = noticeText?.match(/to peer ([0-9a-f-]{36})/)?.[1]
    expect(failedPeerId).toBeTruthy()
    await api(identities[0], `/api/nodes/${failedPeerId}/pause`, 'POST')
    await waitFor(async () => api<Array<{ id: string; paused: boolean }>>(identities[0], nodesPath),
      nodes => nodes.some(node => node.id === failedPeerId && node.paused), 'the transferring peer to disconnect')

    const committed = await waitFor(async () => api<Array<{
      id: string
      completed_batches: number
      checkpoints: Array<{ batch_index: number; sha256: string; bytes: number; shards: Array<{ shard_index: number; node_id: string; sha256: string; bytes: number }> }>
    }>>(identities[0], `/api/clusters/${clusterId}/jobs`), jobs => {
      const current = jobs.find(item => item.id === job.id)
      return Boolean(current?.checkpoints[0]?.shards.length === 2)
    }, 'the fallback full-copy checkpoint to commit', 90_000)
    const committedJob = committed.find(item => item.id === job.id)!
    const checkpoint = committedJob.checkpoints[0]
    expect(committedJob.completed_batches).toBe(1)
    expect(new Set(checkpoint.shards.map(shard => shard.node_id)).size).toBe(2)
    expect(checkpoint.shards.map(shard => shard.bytes)).toEqual([checkpoint.bytes, checkpoint.bytes])
    expect(checkpoint.shards.every(shard => shard.sha256 === checkpoint.sha256)).toBe(true)
    await expect(peers[0].page.locator('.feedback.success')).toContainText('All job batches completed and replicated')
  } finally {
    await Promise.all(contexts.map(context => context.close()))
    if (clusterId) await api(identities[0], `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
  }
})

