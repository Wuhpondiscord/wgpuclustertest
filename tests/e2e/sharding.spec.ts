import { randomBytes } from 'node:crypto'
import { expect, test, type Browser, type BrowserContext } from '@playwright/test'

const baseURL = 'http://127.0.0.1:5173'
const ownerIdentity = () => randomBytes(32).toString('hex')

test('serializes peer descriptions and buffers ICE candidates that arrive early', async ({ page }) => {
  await page.goto('/?mesh-e2e=local')
  const result = await page.evaluate(async () => {
    const path = '/src/peer.ts'
    const { PeerMeshNode } = await import(path)
    const native = window.RTCPeerConnection
    const events: string[] = []
    const notices: string[] = []
    class FakePeer {
      remoteDescription: RTCSessionDescriptionInit | null = null
      localDescription: RTCSessionDescriptionInit | null = null
      connectionState = 'new'
      signalingState = 'stable'
      async setRemoteDescription(description: RTCSessionDescriptionInit) {
        events.push('remote-start')
        await new Promise(resolve => setTimeout(resolve, 30))
        this.remoteDescription = description
        events.push('remote-ready')
      }
      async addIceCandidate(candidate: RTCIceCandidateInit) {
        if (!this.remoteDescription) throw new Error('Remote description is missing')
        events.push(`candidate:${candidate.candidate}`)
      }
      async createAnswer() { return { type: 'answer', sdp: 'answer' } }
      async setLocalDescription(description: RTCSessionDescriptionInit) { this.localDescription = description }
      close() { this.connectionState = 'closed' }
    }
    Object.defineProperty(window, 'RTCPeerConnection', { configurable: true, value: FakePeer })
    const node = new PeerMeshNode('worker', () => undefined, (notice: string) => notices.push(notice))
    try {
      await Promise.all([
        node.handleSignal('peer', { candidate: { candidate: 'early' } }),
        node.handleSignal('peer', { description: { type: 'offer', sdp: 'offer' } }),
        node.handleSignal('peer', { candidate: { candidate: 'concurrent' } }),
      ])
      return { events, notices }
    } finally {
      node.close()
      Object.defineProperty(window, 'RTCPeerConnection', { configurable: true, value: native })
    }
  })
  expect(result.notices).toEqual([])
  expect(result.events).toEqual(['remote-start', 'remote-ready', 'candidate:early', 'candidate:concurrent'])
})

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

async function openPeer(browser: Browser, identity: string, nodeId: string, token: string, beforeLoad?: (context: BrowserContext) => Promise<void>): Promise<{ context: BrowserContext; page: Awaited<ReturnType<BrowserContext['newPage']>> }> {
  const context = await browser.newContext()
  await context.addInitScript(({ meshIdentity, nodeId, token }) => {
    localStorage.setItem('mesh.identity', meshIdentity)
    sessionStorage.setItem('mesh.node-tokens', JSON.stringify({ [nodeId]: token }))
  }, { meshIdentity: identity, nodeId, token })
  await beforeLoad?.(context)
  const page = await context.newPage()
  page.on('pageerror', error => console.error(`Peer page error: ${error.message}`))
  page.on('websocket', socket => socket.on('framereceived', event => {
    try {
      const message = JSON.parse(typeof event.payload === 'string' ? event.payload : event.payload.toString())
      if (message.type === 'node_reply' && message.ok === false) console.error(`Node operation rejected: ${String(message.error).slice(0, 300)}`)
    } catch { /* Ignore non-JSON frames in diagnostic logging. */ }
  }))
  page.on('response', response => {
    if (response.status() >= 400 && response.url().includes('/api/jobs/')) {
      void response.text().then(text => console.error(`Job request ${response.status()}: ${text.slice(0, 300)}`)).catch(() => undefined)
    }
  })
  await page.goto(`${baseURL}/?mesh-e2e=local`)
  await expect(page.getByText('Coordinator online')).toBeVisible()
  return { context, page }
}

async function threePeerFixture(browser: Browser) {
  const identities = [ownerIdentity(), ownerIdentity(), ownerIdentity()]
  const clusterId = (await api<{ id: string }>(identities[0], '/api/clusters', 'POST', { name: 'three-device-preflight' })).id
  const peers: Awaited<ReturnType<typeof openPeer>>[] = []
  try {
    const invite = await api<{ token: string }>(identities[0], `/api/clusters/${clusterId}/invites`, 'POST')
    await Promise.all(identities.slice(1).map(identity => api(identity, '/api/invites/join', 'POST', { token: invite.token })))
    const nodes = await Promise.all(identities.map((identity, index) => api<{ node: { id: string }; token: string }>(identity,
      `/api/clusters/${clusterId}/nodes`, 'POST', { name: `preflight-peer-${index}` })))
    for (let index = 0; index < nodes.length; index++) peers.push(await openPeer(browser, identities[index], nodes[index].node.id, nodes[index].token))
    await waitFor(() => api<Array<{ online: boolean; holder_ready: boolean }>>(identities[0], `/api/clusters/${clusterId}/nodes`),
      nodes => nodes.length === 3 && nodes.every(node => node.online && node.holder_ready), 'three verified holders')
    return { identities, clusterId, nodes, peers, close: async () => {
      await Promise.all(peers.map(peer => peer.context.close()))
      await api(identities[0], `/api/clusters/${clusterId}`, 'DELETE')
    } }
  } catch (error) {
    await Promise.all(peers.map(peer => peer.context.close()))
    await api(identities[0], `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
    throw error
  }
}

test('maximum vector batches replan after holder ACK then finish with two devices', async ({ browser }) => {
  const fixture = await threePeerFixture(browser)
  const { identities, clusterId, nodes, peers } = fixture
  try {
    const job = await api<{ id: string }>(identities[0], `/api/clusters/${clusterId}/jobs`, 'POST', {
      workload: 'vector-add', total_units: 1_500_000, batch_units: 500_000,
    })
    let interrupted = false
    await peers[0].page.route(`**/api/jobs/${job.id}/commit`, async route => {
      if (!interrupted) {
        interrupted = true
        const receipt = route.request().postDataJSON().shards[1] as { node_id: string }
        const target = nodes.findIndex(node => node.node.id === receipt.node_id)
        await peers[target].context.close()
        await waitFor(() => api<Array<{ id: string; online: boolean }>>(identities[0], `/api/clusters/${clusterId}/nodes`),
          nodes => nodes.some(node => node.id === receipt.node_id && !node.online), 'a real holder socket disconnect')
      }
      await route.continue()
    })
    await peers[0].page.getByRole('button', { name: 'Run on this node' }).click()
    const current = await waitFor(() => api<Array<{ id: string; completed_batches: number; checkpoints: Array<{ bytes: number; shards: Array<{ bytes: number }> }> }>>(
      identities[0], `/api/clusters/${clusterId}/jobs`), jobs => jobs.some(item => item.id === job.id && item.completed_batches === 3), 'all maximum-sized vector batches after holder loss', 120_000)
    const completed = current.find(item => item.id === job.id)!
    expect(interrupted).toBe(true)
    for (const checkpoint of completed.checkpoints) {
      expect(checkpoint.bytes).toBeLessThan(3 * 1024 * 1024)
      expect(checkpoint.shards.map(shard => shard.bytes)).toEqual([checkpoint.bytes, checkpoint.bytes])
    }
    await expect(peers[0].page.locator('.feedback.error')).toHaveCount(0)
    await peers[0].page.getByRole('button', { name: 'Test restore' }).click()
    await expect(peers[0].page.getByText('Recovered batch 3 from 2 verified shard(s); full artifact SHA-256 matches.', { exact: true })).toBeVisible({ timeout: 45_000 })
  } finally { await fixture.close() }
})

test('a surviving device resumes unfinished GPU job batches after the compute browser closes', async ({ browser }) => {
  const fixture = await threePeerFixture(browser)
  const { identities, clusterId, nodes, peers } = fixture
  try {
    const job = await api<{ id: string }>(identities[0], `/api/clusters/${clusterId}/jobs`, 'POST', {
      workload: 'vector-add', total_units: 300_000, batch_units: 100_000,
    })
    let signalInFlight!: () => void
    const inFlight = new Promise<void>(resolve => { signalInFlight = resolve })
    await peers[0].page.route(`**/api/jobs/${job.id}/commit`, async route => {
      if (route.request().postDataJSON().lease.task_id.endsWith(':1')) { signalInFlight(); return }
      await route.continue()
    })
    await peers[0].page.getByRole('button', { name: 'Run on this node' }).click()
    await inFlight
    const read = () => api<Array<{ id: string; completed_batches: number; checkpoints: Array<{ batch_index: number; sha256: string }> }>>(identities[0], `/api/clusters/${clusterId}/jobs`)
    const original = (await read()).find(item => item.id === job.id)!.checkpoints[0]
    await peers[0].context.close()
    await waitFor(() => api<Array<{ id: string; online: boolean }>>(identities[1], `/api/clusters/${clusterId}/nodes`),
      nodesNow => nodesNow.some(node => node.id === nodes[0].node.id && !node.online), 'the compute worker to disconnect')
    await peers[1].page.getByRole('button', { name: 'Refresh', exact: true }).click()
    await expect(peers[1].page.getByRole('button', { name: 'Run on this node' })).toBeEnabled()
    await peers[1].page.getByRole('button', { name: 'Run on this node' }).click()
    const completed = (await waitFor(read, jobs => jobs.some(item => item.id === job.id && item.completed_batches === 3), 'a surviving worker to reclaim and finish every batch', 90_000)).find(item => item.id === job.id)!
    expect(completed.checkpoints.find(checkpoint => checkpoint.batch_index === 0)!.sha256).toBe(original.sha256)
    await expect(peers[1].page.locator('.feedback.error')).toHaveCount(0)
  } finally { await fixture.close() }
})

test('every device checks every peer and the UI CPU demo completes and restores', async ({ browser }) => {
  const fixture = await threePeerFixture(browser)
  try {
    for (const { page } of fixture.peers) {
      await page.getByRole('button', { name: 'Test P2P', exact: true }).click()
      await expect(page.getByText('Peer transfers verified to all 2 other connected node(s). Run this test on every device to check all network pairs.', { exact: true })).toBeVisible({ timeout: 45_000 })
      await expect(page.locator('.feedback.error')).toHaveCount(0)
    }
    const page = fixture.peers[0].page
    await page.getByRole('button', { name: 'Create CPU demo', exact: true }).click()
    await page.getByRole('button', { name: 'Run on this node', exact: true }).click()
    await expect(page.getByText('All job batches completed and replicated (cpu)', { exact: true })).toBeVisible({ timeout: 90_000 })
    await page.getByRole('button', { name: 'Test restore', exact: true }).click()
    await expect(page.getByText('Recovered batch 12 from 2 verified shard(s); full artifact SHA-256 matches.', { exact: true })).toBeVisible({ timeout: 45_000 })
  } finally { await fixture.close() }
})

test('transfer ACK deadline starts after the final chunk, including slow sends', async ({ page }) => {
  await page.goto('/')
  const outcome = await page.evaluate(async () => {
    const path = '/src/peer.ts'
    const { PeerMeshNode } = await import(path)
    const mesh = new PeerMeshNode('sender', () => undefined, () => undefined)
    const nativeTimeout = window.setTimeout
    let chunks = 0
    let chunksWhenArmed = -1
    let start: { transfer_id: string; sha256: string }
    const channel = { readyState: 'open' }
    mesh.channels.set('receiver', channel)
    window.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) => {
      if (timeout === 30000) chunksWhenArmed = chunks
      return nativeTimeout(handler, timeout, ...args)
    }) as typeof window.setTimeout
    mesh.sendFrame = async (_channel: unknown, message: { type: string; transfer_id: string; sha256: string; index: number }) => {
      if (message.type === 'artifact_start') start = message
      else if (message.type === 'artifact_chunk') {
        chunks++
        // A slow network can spend over 30 seconds in these awaits; no ACK
        // deadline may run before the last chunk has been queued.
        if (chunksWhenArmed !== -1) throw new Error('ACK timer armed during the transfer')
        if (chunks === 3) nativeTimeout(() => void mesh.handleDataMessage('receiver', channel, JSON.stringify({ type: 'artifact_ack', transfer_id: start.transfer_id, sha256: start.sha256 })), 0)
      }
    }
    try {
      await mesh.sendArtifact('receiver', 'slow-link-fixture', new Uint8Array(40000), false)
      return { chunks, chunksWhenArmed }
    } finally { window.setTimeout = nativeTimeout; mesh.close() }
  })
  expect(outcome).toEqual({ chunks: 3, chunksWhenArmed: 3 })
})

test('a different peer cannot replace transfer metadata or inject chunks', async ({ page }) => {
  await page.goto('/')
  const outcome = await page.evaluate(async () => {
    const path = '/src/peer.ts'
    const { PeerMeshNode } = await import(path)
    const mesh = new PeerMeshNode('receiver', () => undefined, () => undefined)
    const messages: Array<{ type: string }> = []
    const channel = { readyState: 'open', send: (value: string) => messages.push(JSON.parse(value)) }
    const sha256 = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array([1, 2, 3]))), b => b.toString(16).padStart(2, '0')).join('')
    const start = { type: 'artifact_start', transfer_id: 'ownership-fixture', name: 'ownership-fixture', total_bytes: 3, chunks: 1, sha256 }
    try {
      await mesh.handleDataMessage('good', channel, JSON.stringify(start))
      await mesh.handleDataMessage('bad', channel, JSON.stringify({ ...start, name: 'attacker-overwrite' }))
      await mesh.handleDataMessage('bad', channel, JSON.stringify({ type: 'artifact_chunk', transfer_id: start.transfer_id, index: 0, data: '!!!' }))
      await mesh.handleDataMessage('good', channel, JSON.stringify({ type: 'artifact_chunk', transfer_id: start.transfer_id, index: 0, data: 'AQID' }))
      return { messages, bytes: Array.from(await mesh.getArtifact('receiver', start.name)) }
    } finally { mesh.close() }
  })
  expect(outcome.messages).toHaveLength(1)
  expect(outcome.messages[0].type).toBe('artifact_ack')
  expect(outcome.bytes).toEqual([1, 2, 3])
})

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

test('reconnecting an older cluster node preserves the selected cluster', async ({ browser }) => {
  const identity = ownerIdentity()
  const first = (await api<{ id: string }>(identity, '/api/clusters', 'POST', { name: 'older-node-cluster' })).id
  const second = (await api<{ id: string }>(identity, '/api/clusters', 'POST', { name: 'selected-cluster' })).id
  let context: BrowserContext | undefined
  try {
    const node = await api<{ node: { id: string }; token: string }>(identity, `/api/clusters/${first}/nodes`, 'POST', { name: 'reconnecting-node' })
    const peer = await openPeer(browser, identity, node.node.id, node.token, async context => {
      await context.addInitScript(() => {
        const NativeSocket = window.WebSocket
        const sockets: WebSocket[] = []
        window.WebSocket = class extends NativeSocket {
          constructor(url: string | URL, protocols?: string | string[]) { super(url, protocols); sockets.push(this) }
        }
        Object.assign(window, { dropTestSockets: () => sockets.forEach(socket => socket.close()) })
      })
    })
    context = peer.context
    await peer.page.getByRole('button', { name: /older-node-cluster owner/ }).click()
    await expect(peer.page.getByText('Connected · storage verified')).toBeVisible()
    await peer.page.getByRole('button', { name: /selected-cluster owner/ }).click()
    await expect(peer.page.getByRole('heading', { name: 'selected-cluster', exact: true })).toBeVisible()
    await peer.page.evaluate(() => (window as unknown as { dropTestSockets: () => void }).dropTestSockets())
    await waitFor(() => api<Array<{ online: boolean; holder_ready: boolean }>>(identity, `/api/clusters/${first}/nodes`),
      nodes => nodes.every(node => node.online && node.holder_ready), 'the original node to reconnect')
    await expect(peer.page.getByRole('heading', { name: 'selected-cluster', exact: true })).toBeVisible()
  } finally {
    await context?.close()
    await api(identity, `/api/clusters/${first}`, 'DELETE').catch(() => undefined)
    await api(identity, `/api/clusters/${second}`, 'DELETE').catch(() => undefined)
  }
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

test('renews a lease through a slow commit and a temporary renewal failure', async ({ browser }) => {
  const identity = ownerIdentity()
  let clusterId = ''
  const contexts: BrowserContext[] = []
  try {
    const cluster = await api<{ id: string }>(identity, '/api/clusters', 'POST', { name: 'slow-checkpoint-lease-e2e' })
    clusterId = cluster.id
    const created = await Promise.all([0, 1].map(index => api<{ node: { id: string }; token: string }>(
      identity, `/api/clusters/${clusterId}/nodes`, 'POST', { name: `lease-peer-${index}` },
    )))
    const peers = await Promise.all(created.map(node => openPeer(browser, identity, node.node.id, node.token)))
    contexts.push(...peers.map(peer => peer.context))
    await waitFor(() => api<Array<{ online: boolean }>>(identity, `/api/clusters/${clusterId}/nodes`),
      nodes => nodes.length === 2 && nodes.every(node => node.online), 'two live worker nodes')
    const job = await api<{ id: string }>(identity, `/api/clusters/${clusterId}/jobs`, 'POST', {
      workload: 'cpu-hash', total_units: 1000, batch_units: 1000,
    })
    let renewAttempts = 0
    let renewed = 0
    peers[0].page.on('response', response => {
      if (response.url().endsWith(`/api/jobs/${job.id}/renew`) && response.ok()) renewed++
    })
    await peers[0].page.route(`**/api/jobs/${job.id}/renew`, async route => {
      if (++renewAttempts === 1) await route.fulfill({ status: 503, body: 'Temporary renewal outage' })
      else await route.continue()
    })
    await peers[0].page.route(`**/api/jobs/${job.id}/commit`, async route => {
      // The original lease lasts 45 seconds. Deliver the commit after that
      // deadline to prove successful renewals keep the same attempt valid.
      await new Promise(resolve => setTimeout(resolve, 52_000))
      await route.continue()
    })
    await peers[0].page.getByRole('button', { name: 'Run on this node' }).click()
    const jobs = await waitFor(() => api<Array<{ id: string; completed_batches: number }>>(identity, `/api/clusters/${clusterId}/jobs`),
      jobs => jobs.some(item => item.id === job.id && item.completed_batches === 1), 'a checkpoint committed beyond its original lease deadline', 90_000)
    expect(jobs.find(item => item.id === job.id)!.completed_batches).toBe(1)
    expect(renewAttempts).toBeGreaterThanOrEqual(4)
    expect(renewed).toBeGreaterThanOrEqual(3)
    await expect(peers[0].page.locator('.feedback.error')).toHaveCount(0)
  } finally {
    await Promise.all(contexts.map(context => context.close()))
    if (clusterId) await api(identity, `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
  }
})

test('stored artifacts cannot be overwritten with different bytes', async ({ page }) => {
  await page.goto('/?mesh-e2e=local')
  const result = await page.evaluate(async () => {
    const path = '/src/peer.ts'
    const { PeerMeshNode } = await import(path)
    const node = new PeerMeshNode('immutable-test', () => undefined, () => undefined)
    try {
      await node.storeArtifact('immutable-test-artifact', new Uint8Array([1, 2, 3]))
      let rejected = false
      try { await node.storeArtifact('immutable-test-artifact', new Uint8Array([4, 5, 6])) } catch { rejected = true }
      return { rejected, stored: Array.from(await node.getArtifact('immutable-test', 'immutable-test-artifact')) }
    } finally { node.close() }
  })
  expect(result).toEqual({ rejected: true, stored: [1, 2, 3] })
})

test('an online worker cannot fabricate another holders storage confirmation', async ({ browser }) => {
  const identity = ownerIdentity()
  let clusterId = ''
  const contexts: BrowserContext[] = []
  try {
    clusterId = (await api<{ id: string }>(identity, '/api/clusters', 'POST', { name: 'forged-custody-e2e' })).id
    const created = await Promise.all([0, 1].map(index => api<{ node: { id: string }; token: string }>(identity,
      `/api/clusters/${clusterId}/nodes`, 'POST', { name: `custody-peer-${index}` })))
    const peers = await Promise.all(created.map(node => openPeer(browser, identity, node.node.id, node.token)))
    contexts.push(...peers.map(peer => peer.context))
    await waitFor(() => api<Array<{ online: boolean }>>(identity, `/api/clusters/${clusterId}/nodes`), nodes => nodes.length === 2 && nodes.every(node => node.online), 'two live holders')
    const job = await api<{ id: string }>(identity, `/api/clusters/${clusterId}/jobs`, 'POST', { workload: 'cpu-hash', total_units: 1000, batch_units: 1000 })
    const claim = await api<{ lease: unknown }>(identity, `/api/jobs/${job.id}/claim`, 'POST', { node_id: created[0].node.id })
    const response = await fetch(`${baseURL}/api/jobs/${job.id}/commit`, {
      method: 'POST', headers: { 'X-Mesh-Identity': identity, 'Content-Type': 'application/json' },
      body: JSON.stringify({ lease: claim.lease, sha256: 'a'.repeat(64), bytes: 128, shards: created.map((node, index) => ({
        shard_index: index, node_id: node.node.id, artifact_name: `fabricated-${index}`, sha256: 'a'.repeat(64), bytes: 128,
      })) }),
    })
    expect(response.status).toBe(409)
    expect(await response.text()).toContain('independently confirmed storage')
    const jobs = await api<Array<{ id: string; completed_batches: number }>>(identity, `/api/clusters/${clusterId}/jobs`)
    expect(jobs.find(item => item.id === job.id)!.completed_batches).toBe(0)
  } finally {
    await Promise.all(contexts.map(context => context.close()))
    if (clusterId) await api(identity, `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
  }
})

for (const workload of ['vector-add', 'cpu-hash'] as const) {
test(`repairs ${workload} checkpoints onto a spare and survives a second original holder loss`, async ({ browser }) => {
  const desiredHolders = workload === 'vector-add' ? 3 : 2
  const identities = Array.from({ length: desiredHolders + 1 }, ownerIdentity)
  let clusterId = ''
  const contexts: BrowserContext[] = []
  type Checkpoint = { sha256: string; manifest_revision: number; available_shards: number; verified_holders: number; durability: string;
    shards: Array<{ shard_index: number; node_id: string; artifact_name: string; sha256: string }> }
  try {
    clusterId = (await api<{ id: string }>(identities[0], '/api/clusters', 'POST', { name: 'automatic-shard-repair-e2e' })).id
    const invite = await api<{ token: string }>(identities[0], `/api/clusters/${clusterId}/invites`, 'POST')
    await Promise.all(identities.slice(1).map(identity => api(identity, '/api/invites/join', 'POST', { token: invite.token })))
    const created = await Promise.all(identities.map((identity, index) => api<{ node: { id: string }; token: string }>(identity,
      `/api/clusters/${clusterId}/nodes`, 'POST', { name: `repair-peer-${index}` })))
    const peers = await Promise.all(created.map((node, index) => openPeer(browser, identities[index], node.node.id, node.token)))
    contexts.push(...peers.map(peer => peer.context))
    await waitFor(() => api<Array<{ online: boolean }>>(identities[0], `/api/clusters/${clusterId}/nodes`),
      nodes => nodes.length === identities.length && nodes.every(node => node.online), 'the live worker and spare nodes')
    const units = workload === 'vector-add' ? 100_000 : 1000
    const job = await api<{ id: string }>(identities[0], `/api/clusters/${clusterId}/jobs`, 'POST', { workload, total_units: units, batch_units: units })
    await peers[0].page.getByRole('button', { name: 'Run on this node' }).click()
    const read = () => api<Array<{ id: string; checkpoints: Checkpoint[] }>>(identities[0], `/api/clusters/${clusterId}/jobs`)
    const initial = (await waitFor(read, jobs => jobs.some(item => item.id === job.id && item.checkpoints[0]?.verified_holders === desiredHolders),
      'a checkpoint independently acknowledged by every assigned holder', 90_000)).find(item => item.id === job.id)!.checkpoints[0]
    const failed = initial.shards.find(shard => shard.node_id !== created[0].node.id)!
    const spare = created.find(node => !initial.shards.some(shard => shard.node_id === node.node.id))!
    await api(identities[0], `/api/nodes/${failed.node_id}/pause`, 'POST')
    const repaired = (await waitFor(read, jobs => jobs.some(item => item.id === job.id && item.checkpoints[0]?.manifest_revision === 1),
      'the missing shard to be repaired onto the spare holder', 90_000)).find(item => item.id === job.id)!.checkpoints[0]
    expect(repaired.sha256).toBe(initial.sha256)
    expect(repaired.durability).toBe('healthy')
    expect(repaired.available_shards).toBe(desiredHolders)
    expect(repaired.verified_holders).toBe(desiredHolders)
    expect(repaired.shards.some(shard => shard.node_id === failed.node_id)).toBe(false)
    expect(repaired.shards.find(shard => shard.shard_index === failed.shard_index)!.node_id).toBe(spare.node.id)
    const second = initial.shards.find(shard => desiredHolders === 3
      ? shard.node_id !== failed.node_id && shard.node_id !== created[0].node.id
      : shard.node_id === created[0].node.id)!
    await api(identities[0], `/api/nodes/${second.node_id}/pause`, 'POST')
    const restorePage = desiredHolders === 3 ? peers[0].page : peers[created.findIndex(node => node.node.id === spare.node.id)].page
    await restorePage.getByRole('button', { name: 'Refresh', exact: true }).click()
    await restorePage.getByRole('button', { name: 'Test restore' }).click()
    await expect(restorePage.getByText(`Recovered batch 1 from ${desiredHolders - 1} verified shard(s); full artifact SHA-256 matches.`, { exact: true })).toBeVisible({ timeout: 45_000 })
  } finally {
    await Promise.all(contexts.map(context => context.close()))
    if (clusterId) await api(identities[0], `/api/clusters/${clusterId}`, 'DELETE').catch(() => undefined)
  }
})
}
