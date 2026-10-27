import { useEffect, useRef, useState, type FormEvent } from 'react'
import { Activity, ArrowRight, Boxes, Check, Copy, Cpu, Link2, LogOut, Pause, Play, Plus, RefreshCw, Settings, Shield, Trash2, Users, X } from 'lucide-react'
import { PeerMeshNode } from './peer'
import { createVectorAddWorker, digestVectorAddResult, runVectorAddCpu } from './webgpu'
import { encode2Plus1, reconstruct2Plus1, sha256Hex, type EncodedShard } from './sharding'

type Cluster = { id: string; name: string; role: 'owner' | 'member'; members: number; nodes: number; online_nodes: number }
type Node = { id: string; cluster_id: string; name: string; paused: boolean; online: boolean; last_seen: number | null }
type Member = { id: string; role: 'owner' | 'member' }
type ActiveInvite = { token: string; expires_at: number }
type InvitePreview = { cluster_id: string; cluster_name: string; expires_at: number }
type NodeCreated = { node: Node; token: string }
type ShardReceipt = { shard_index: number; node_id: string; artifact_name: string; sha256: string; bytes: number }
type ComputeCheckpoint = { batch_index: number; sha256: string; bytes: number; shards: ShardReceipt[] }
type ComputeJob = { id: string; cluster_id: string; created_at: number; workload: 'cpu-hash' | 'vector-add'; total_units: number; batch_units: number; total_batches: number; completed_batches: number; checkpoints: ComputeCheckpoint[] }
type JobLease = { job_id: string; batch_index: number; start: number; end: number; lease: { task_id: string; worker_id: string; coordinator_epoch: number; generation: number; attempt_id: number; expires_at_ms: number } }
type BrowserNodeConnection = { url: string; socket: WebSocket | null; retryTimer: number | null; heartbeatTimer: number | null; stableTimer: number | null; attempts: number; stopped: boolean; peerMesh: PeerMeshNode | null }
type UiDialog =
  | { kind: 'input'; title: string; description: string; label: string; value: string; placeholder?: string; submitLabel: string; onSubmit: (value: string) => void }
  | { kind: 'confirm'; title: string; description: string; confirmLabel: string; requiredValue?: string; onConfirm: () => void }

function getSessionNodeTokens(): Record<string, string> {
  try { return JSON.parse(sessionStorage.getItem('mesh.node-tokens') ?? '{}') as Record<string, string> }
  catch { return {} }
}

function getIdentity() {
  let token = localStorage.getItem('mesh.identity')
  if (!token) { token = `${crypto.randomUUID().replace(/-/g, '')}${crypto.randomUUID().replace(/-/g, '')}`; localStorage.setItem('mesh.identity', token) }
  return token
}

function computeBatch(start: number, end: number) {
  let accumulator = 0x811c9dc5
  for (let value = start; value < end; value++) {
    let mixed = (value ^ 0x9e3779b9) >>> 0
    mixed = Math.imul(mixed ^ (mixed >>> 16), 0x85ebca6b) >>> 0
    mixed = Math.imul(mixed ^ (mixed >>> 13), 0xc2b2ae35) >>> 0
    mixed = (mixed ^ (mixed >>> 16)) >>> 0
    accumulator = Math.imul(accumulator ^ mixed, 0x01000193) >>> 0
  }
  return accumulator.toString(16).padStart(8, '0')
}

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('X-Mesh-Identity', getIdentity())
  if (init.body) headers.set('Content-Type', 'application/json')
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) throw Object.assign(new Error((await response.text()) || `Request failed (${response.status})`), { status: response.status })
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

function maintainLease(jobId: string, initial: JobLease['lease'], claimedAt: number) {
  let current = initial
  // Use monotonic elapsed time so a device with an incorrect wall clock does
  // not reject a valid server lease. Reserve one second for server rounding.
  let deadline = claimedAt + 44_000
  let stopped = false
  let lost: Error | null = null
  let timer = 0
  const schedule = (delay: number) => { if (!stopped) timer = window.setTimeout(() => void renew(), delay) }
  const renew = async () => {
    let delay = 10_000
    const startedAt = performance.now()
    try {
      current = await api<JobLease['lease']>(`/api/jobs/${jobId}/renew`, { method: 'POST', body: JSON.stringify({ lease: current }), signal: AbortSignal.timeout(8_000) })
      deadline = startedAt + 44_000
    } catch (error) {
      const status = (error as { status?: number }).status
      if ((status && [401, 403, 404, 409].includes(status)) || performance.now() >= deadline) {
        lost = error instanceof Error ? error : new Error('The worker lease was lost')
      } else { delay = 2_000 }
    }
    if (!lost) schedule(delay)
  }
  schedule(Math.min(10_000, Math.max(0, deadline - performance.now() - 15_000)))
  return {
    current: () => {
      if (lost) throw lost
      if (performance.now() >= deadline) throw new Error('The worker lease expired before checkpoint commit; run the job again')
      return current
    },
    stop: () => { stopped = true; window.clearTimeout(timer) },
  }
}

function App() {
  const [clusters, setClusters] = useState<Cluster[]>([])
  const [activeId, setActiveId] = useState('')
  const [nodes, setNodes] = useState<Node[]>([])
  const [members, setMembers] = useState<Member[]>([])
  const [invites, setInvites] = useState<ActiveInvite[]>([])
  const [jobs, setJobs] = useState<ComputeJob[]>([])
  const [invite, setInvite] = useState<InvitePreview | null>(null)
  const [joinToken, setJoinToken] = useState(new URLSearchParams(window.location.search).get('invite') ?? '')
  const [nodeCredentials, setNodeCredentials] = useState<{ name: string; url: string } | null>(null)
  const [dialog, setDialog] = useState<UiDialog | null>(null)
  const [dialogValue, setDialogValue] = useState('')
  const sockets = useRef(new Map<string, BrowserNodeConnection>())
  const [busy, setBusy] = useState(true)
  const [coordinatorStatus, setCoordinatorStatus] = useState<'checking' | 'online' | 'offline'>('checking')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const active = clusters.find(c => c.id === activeId) ?? clusters[0]

  async function refreshClusters(preferId?: string) {
    const data = await api<Cluster[]>('/api/clusters')
    setClusters(data)
    const nextId = preferId && data.some(c => c.id === preferId) ? preferId : data.some(c => c.id === activeId) ? activeId : data[0]?.id ?? ''
    setActiveId(nextId)
    if (nextId) {
      const [nextNodes, nextMembers] = await Promise.all([
        api<Node[]>(`/api/clusters/${nextId}/nodes`),
        api<Member[]>(`/api/clusters/${nextId}/members`),
      ])
      const nextJobs = await api<ComputeJob[]>(`/api/clusters/${nextId}/jobs`)
      setNodes(nextNodes); setMembers(nextMembers)
      setJobs(nextJobs)
      const tokens = getSessionNodeTokens()
      for (const node of nextNodes) {
        const token = tokens[node.id]
        if (token && !sockets.current.has(node.id)) {
          const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:'
          connectBrowserNode(node.id, `${wsProto}//${location.host}/api/nodes/connect?token=${token}`)
        }
      }
      const selected = data.find(c => c.id === nextId)
      setInvites(selected?.role === 'owner' ? await api<ActiveInvite[]>(`/api/clusters/${nextId}/invites`) : [])
    } else { setNodes([]); setMembers([]); setInvites([]); setJobs([]) }
  }

  useEffect(() => {
    getIdentity()
    void (async () => {
      try {
        const token = new URLSearchParams(window.location.search).get('invite')
        if (token) { setJoinToken(token); setInvite(await api<InvitePreview>(`/api/invites/${encodeURIComponent(token)}`)) }
        await refreshClusters()
      } catch (e) { setError(e instanceof Error ? e.message : 'Could not connect to the Mesh coordinator') }
      finally { setBusy(false) }
    })()
  }, [])

  useEffect(() => {
    let stopped = false
    let timer = 0
    const checkCoordinator = async () => {
      try {
        const response = await fetch('/health', { cache: 'no-store' })
        if (!stopped) setCoordinatorStatus(response.ok ? 'online' : 'offline')
      } catch {
        if (!stopped) setCoordinatorStatus('offline')
      } finally {
        if (!stopped) timer = window.setTimeout(() => void checkCoordinator(), 10000)
      }
    }
    void checkCoordinator()
    return () => { stopped = true; window.clearTimeout(timer) }
  }, [])

  useEffect(() => {
    if (!activeId) return
    const timer = window.setInterval(() => {
      void api<ComputeJob[]>(`/api/clusters/${activeId}/jobs`).then(setJobs).catch(() => undefined)
    }, 5000)
    return () => window.clearInterval(timer)
  }, [activeId])

  useEffect(() => {
    if (!activeId) return
    const timer = window.setInterval(() => { void refreshClusters(activeId).catch(e => setError(e instanceof Error ? e.message : 'Refresh failed')) }, 12000)
    return () => window.clearInterval(timer)
  }, [activeId])

  useEffect(() => () => { for (const id of sockets.current.keys()) stopBrowserNode(id) }, [])

  useEffect(() => {
    const reconnect = () => {
      for (const [id, connection] of sockets.current) {
        if (connection.retryTimer !== null) window.clearTimeout(connection.retryTimer)
        connection.retryTimer = null
        if (!connection.socket || connection.socket.readyState === WebSocket.CLOSED) openBrowserNode(id, connection)
      }
    }
    const offline = () => setNotice('This device is offline. Its node will reconnect when the network returns.')
    window.addEventListener('online', reconnect)
    window.addEventListener('offline', offline)
    return () => { window.removeEventListener('online', reconnect); window.removeEventListener('offline', offline) }
  }, [])

  async function run(action: () => Promise<void>) {
    setError(''); setNotice('')
    try { await action() }
    catch (e) { setError(e instanceof Error ? e.message : 'Request failed') }
  }
  function openInputDialog(options: Omit<Extract<UiDialog, { kind: 'input' }>, 'kind'>) {
    setDialogValue(options.value)
    setDialog({ kind: 'input', ...options })
  }
  function openConfirmDialog(options: Omit<Extract<UiDialog, { kind: 'confirm' }>, 'kind'>) {
    setDialogValue('')
    setDialog({ kind: 'confirm', ...options })
  }
  function submitDialog(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!dialog) return
    if (dialog.kind === 'input') {
      const value = dialogValue.trim()
      if (!value) return
      const submit = dialog.onSubmit
      setDialog(null)
      submit(value)
      return
    }
    if (dialog.requiredValue && dialogValue !== dialog.requiredValue) return
    const confirm = dialog.onConfirm
    setDialog(null)
    confirm()
  }
  async function createCluster() {
    openInputDialog({ title: 'Create a cluster', description: 'Choose a name for this compute pool.', label: 'Cluster name', placeholder: 'Research pool', value: '', submitLabel: 'Create cluster', onSubmit: name => { void run(async () => { const c = await api<Cluster>('/api/clusters', { method: 'POST', body: JSON.stringify({ name }) }); await refreshClusters(c.id); setNotice(`Created “${c.name}”`) }) } })
  }
  async function makeInvite() {
    if (!active) return
    await run(async () => {
      const data = await api<{ token: string }>(`/api/clusters/${active.id}/invites`, { method: 'POST' })
      const url = `${location.origin}/?invite=${data.token}`
      await navigator.clipboard.writeText(url)
      await refreshClusters(active.id)
      setNotice('Seven day invite link copied')
    })
  }
  async function join(token = joinToken) {
    await run(async () => {
      const joined = await api<Cluster>('/api/invites/join', { method: 'POST', body: JSON.stringify({ token }) })
      setInvite(null); setJoinToken(''); history.replaceState({}, '', location.pathname)
      await refreshClusters(joined.id); setNotice(`Joined “${joined.name}”`)
    })
  }
  function openInviteDialog() {
    openInputDialog({ title: 'Join by invite', description: 'Paste a cluster invite URL or its token to preview and join.', label: 'Invite URL or token', placeholder: 'https://…?invite=…', value: joinToken, submitLabel: 'Preview invite', onSubmit: value => {
      const token = value.includes('invite=') ? new URL(value, location.origin).searchParams.get('invite') ?? '' : value.trim()
      if (!token) { setError('Enter a valid invite URL or token.'); return }
      setJoinToken(token)
      void run(async () => { setInvite(await api<InvitePreview>(`/api/invites/${encodeURIComponent(token)}`)) })
    } })
  }
  async function createNode() {
    if (!active) return
    openInputDialog({ title: 'Add a node', description: 'Register this browser or machine with the selected cluster.', label: 'Node name', placeholder: 'Office GPU', value: '', submitLabel: 'Add node', onSubmit: name => { void run(async () => {
      const created = await api<NodeCreated>(`/api/clusters/${active.id}/nodes`, { method: 'POST', body: JSON.stringify({ name }) })
      const tokens = getSessionNodeTokens()
      tokens[created.node.id] = created.token
      sessionStorage.setItem('mesh.node-tokens', JSON.stringify(tokens))
      const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:'
      const url = `${wsProto}//${location.host}/api/nodes/connect?token=${created.token}`
      setNodeCredentials({ name, url })
      connectBrowserNode(created.node.id, url)
      await refreshClusters(active.id)
    }) } })
  }
  function connectBrowserNode(id: string, url: string) {
    stopBrowserNode(id)
    const connection: BrowserNodeConnection = { url, socket: null, retryTimer: null, heartbeatTimer: null, stableTimer: null, attempts: 0, stopped: false, peerMesh: null }
    sockets.current.set(id, connection)
    openBrowserNode(id, connection)
  }
  function createPeerMesh(id: string, socket: WebSocket) {
    return new PeerMeshNode(id, (peerNodeId, payload) => {
      if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'signal', peer_node_id: peerNodeId, payload }))
    }, setNotice)
  }
  function openBrowserNode(id: string, connection: BrowserNodeConnection) {
    if (connection.stopped || sockets.current.get(id) !== connection || !navigator.onLine) return
    let socket: WebSocket
    try { socket = new WebSocket(connection.url) }
    catch { scheduleBrowserReconnect(id, connection); return }
    connection.socket = socket
    socket.addEventListener('open', () => {
      if (connection.stopped || sockets.current.get(id) !== connection) { socket.close(); return }
      setNotice(`${nameForNode(id)} connected`)
      if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
      connection.stableTimer = window.setTimeout(() => { connection.attempts = 0; connection.stableTimer = null }, 60000)
      connection.heartbeatTimer = window.setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.send('{"type":"heartbeat"}') }, 15000)
      connection.peerMesh?.close()
      connection.peerMesh = createPeerMesh(id, socket)
      void refreshClusters(activeId).catch(() => undefined)
    })
    socket.addEventListener('message', event => {
      try {
        const message = JSON.parse(String(event.data)) as { type?: string; paused?: boolean; peer_node_id?: string; peer_node_ids?: string[]; payload?: { description?: RTCSessionDescriptionInit; candidate?: RTCIceCandidateInit } }
        if (message.type === 'control') {
          if (message.paused) { connection.peerMesh?.close(); connection.peerMesh = null }
          else if (!connection.peerMesh) connection.peerMesh = createPeerMesh(id, socket)
          setNotice(message.paused ? `${nameForNode(id)} paused` : `${nameForNode(id)} resumed`)
        }
        else if (message.type === 'peer_list') connection.peerMesh?.addPeers(message.peer_node_ids ?? [])
        else if (message.type === 'peer_joined' && message.peer_node_id) connection.peerMesh?.peerJoined(message.peer_node_id)
        else if (message.type === 'peer_left' && message.peer_node_id) connection.peerMesh?.peerLeft(message.peer_node_id)
        else if (message.type === 'signal' && message.peer_node_id && message.payload) void connection.peerMesh?.handleSignal(message.peer_node_id, message.payload)
      } catch { /* Ignore unknown worker messages. */ }
    })
    socket.addEventListener('close', () => {
      if (connection.heartbeatTimer !== null) window.clearInterval(connection.heartbeatTimer)
      if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
      connection.heartbeatTimer = null
      connection.stableTimer = null
      connection.peerMesh?.close(); connection.peerMesh = null
      if (connection.socket === socket) connection.socket = null
      if (connection.stopped || sockets.current.get(id) !== connection) return
      setNotice(`${nameForNode(id)} disconnected; retrying automatically`)
      void refreshClusters(activeId).catch(() => undefined)
      scheduleBrowserReconnect(id, connection)
    })
    socket.addEventListener('error', () => socket.close())
  }
  function scheduleBrowserReconnect(id: string, connection: BrowserNodeConnection) {
    if (connection.stopped || sockets.current.get(id) !== connection || !navigator.onLine || connection.retryTimer !== null) return
    const baseDelay = Math.min(30000, 1000 * 2 ** Math.min(connection.attempts, 5))
    const delay = Math.round(baseDelay * (0.8 + Math.random() * 0.4))
    connection.attempts += 1
    connection.retryTimer = window.setTimeout(() => {
      connection.retryTimer = null
      openBrowserNode(id, connection)
    }, delay)
  }
  function stopBrowserNode(id: string) {
    const connection = sockets.current.get(id)
    if (!connection) return
    connection.stopped = true
    if (connection.retryTimer !== null) window.clearTimeout(connection.retryTimer)
    if (connection.heartbeatTimer !== null) window.clearInterval(connection.heartbeatTimer)
    if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
    connection.peerMesh?.close()
    sockets.current.delete(id)
    connection.socket?.close()
  }
  function nameForNode(id: string) { return nodes.find(node => node.id === id)?.name ?? 'Node' }
  async function toggleNode(node: Node) {
    await run(async () => { await api(`/api/nodes/${node.id}/${node.paused ? 'resume' : 'pause'}`, { method: 'POST' }); await refreshClusters(active.id) })
  }
  async function removeNode(node: Node) {
    openConfirmDialog({ title: 'Remove node?', description: `Remove “${node.name}” from this cluster? Its connection token will stop working.`, confirmLabel: 'Remove node', onConfirm: () => { void run(async () => { await api(`/api/nodes/${node.id}`, { method: 'DELETE' }); stopBrowserNode(node.id); const tokens = getSessionNodeTokens(); delete tokens[node.id]; sessionStorage.setItem('mesh.node-tokens', JSON.stringify(tokens)); await refreshClusters(active.id) }) } })
  }
  async function testPeerTransfer() {
    if (!active) return
    const source = [...sockets.current.entries()].find(([id, connection]) => nodes.some(node => node.id === id && node.cluster_id === active.id && !node.paused) && connection.socket?.readyState === WebSocket.OPEN && connection.peerMesh)
    const target = nodes.find(node => node.cluster_id === active.id && node.online && !node.paused && node.id !== source?.[0])
    if (!source || !target || !source[1].peerMesh) { setError('Keep this tab connected to a node and connect a second node in the same cluster first.'); return }
    setError(''); setNotice('Establishing a direct peer channel…')
    const bytes = new TextEncoder().encode(JSON.stringify({ kind: 'checkpoint-transfer-probe', cluster_id: active.id, generated_at: Date.now(), payload: 'mesh-peer-transfer'.repeat(2048) }))
    let lastError: unknown
    const candidates = nodes.filter(node => node.cluster_id === active.id && node.online && !node.paused && node.id !== source[0])
    for (const peer of candidates) {
      try {
        const result = await source[1].peerMesh.sendArtifact(peer.id, 'mesh-p2p-checkpoint-probe', bytes)
        setNotice(`Peer transfer verified: ${result.bytes.toLocaleString()} bytes, SHA-256 ${result.sha256.slice(0, 16)}…`)
        return
      } catch (e) { lastError = e }
    }
    setError(lastError instanceof Error ? lastError.message : 'Peer transfer failed; no active peer acknowledged the test artifact')
  }
  async function createComputeJob(workload: ComputeJob['workload']) {
    if (!active) return
    await run(async () => {
      await api<ComputeJob>(`/api/clusters/${active.id}/jobs`, { method: 'POST', body: JSON.stringify({ workload, total_units: 1_200_000, batch_units: 100_000 }) })
      setJobs(await api<ComputeJob[]>(`/api/clusters/${active.id}/jobs`))
      setNotice(workload === 'vector-add' ? 'Created a 12-batch WebGPU vector-add job (workers without WebGPU use the bounded CPU fallback)' : 'Created a 12-batch recoverable CPU demo')
    })
  }
  async function runComputeJob(job: ComputeJob) {
    if (!active) return
    const tokens = getSessionNodeTokens()
    const workers = [...sockets.current.entries()].filter(([nodeId, connection]) =>
      Object.prototype.hasOwnProperty.call(tokens, nodeId) && nodes.some(node => node.id === nodeId && node.cluster_id === active.id && node.online && !node.paused)
      && connection.socket?.readyState === WebSocket.OPEN && connection.peerMesh)
    const source = workers[0]
    const replicas = nodes.filter(node => node.cluster_id === active.id && node.online && !node.paused && node.id !== source?.[0])
    if (!source?.[1].peerMesh || !replicas.length) { setError('Connect one node owned by this browser and a second online node in the cluster.'); return }
    if (job.workload === 'vector-add' && replicas.length < 2) { setError('This vector-add job produces a large checkpoint and needs three online nodes: this worker plus two peers.'); return }
    const peerMesh = source[1].peerMesh
    if (!peerMesh) { setError('The worker peer transport is not ready.'); return }
    const availableCheckpointPeers = async (excluded: Set<string> = new Set()) => {
      const currentNodes = await api<Node[]>(`/api/clusters/${job.cluster_id}/nodes`)
      setNodes(currentNodes)
      return currentNodes.filter(node => node.cluster_id === job.cluster_id && node.online && !node.paused && node.id !== source[0]
        && !excluded.has(node.id) && peerMesh.canTransferTo(node.id))
    }
    let gpuWorker: Awaited<ReturnType<typeof createVectorAddWorker>> = null
    let executionMode: 'webgpu' | 'cpu-fallback' | 'cpu' = 'cpu'
    if (job.workload === 'vector-add') {
      try { gpuWorker = await createVectorAddWorker() } catch { gpuWorker = null }
      executionMode = gpuWorker ? 'webgpu' : 'cpu-fallback'
    }
    setError(''); setNotice(gpuWorker ? 'WebGPU worker is claiming and processing job batches…' : job.workload === 'vector-add' ? 'WebGPU is unavailable; using the bounded CPU fallback…' : 'CPU worker is claiming and processing job batches…')
    await run(async () => {
      try {
       let idlePolls = 0
       while (idlePolls < 60) {
        const claimedAt = performance.now()
        const claim = await api<JobLease | null>(`/api/jobs/${job.id}/claim`, { method: 'POST', body: JSON.stringify({ node_id: source[0] }) })
        if (!claim) {
          const current = await api<ComputeJob[]>(`/api/clusters/${job.cluster_id}/jobs`)
          setJobs(current)
          const updated = current.find(item => item.id === job.id)
          if (!updated || updated.completed_batches >= updated.total_batches) { setNotice('All job batches are complete'); return }
          idlePolls++
          await new Promise(resolve => window.setTimeout(resolve, 1000))
          continue
        }
        idlePolls = 0
        let checkpointPolicy = ''
        const heartbeat = maintainLease(job.id, claim.lease, claimedAt)
        try {
        const vectorResult = job.workload === 'vector-add'
          ? (gpuWorker ? await gpuWorker.run(claim.start, claim.end) : runVectorAddCpu(claim.start, claim.end))
          : null
        const result = vectorResult ? await digestVectorAddResult(vectorResult) : computeBatch(claim.start, claim.end)
        const artifact = new TextEncoder().encode(JSON.stringify({
          type: 'mesh-compute-checkpoint-v2', workload: job.workload, execution_mode: executionMode,
          job_id: job.id, batch_index: claim.batch_index, start: claim.start, end: claim.end,
          result_digest: result, output: vectorResult ? Array.from(vectorResult) : undefined,
          worker_node_id: source[0], generation: claim.lease.generation,
        }))
        const shardReceipts: ShardReceipt[] = []
        const checkpointName = `job-${job.id}-batch-${claim.batch_index}-g${claim.lease.generation}`
        const checkpointDigest = await sha256Hex(artifact)
        const tryReplicatedCheckpoint = async (excluded: Set<string>) => {
          const candidates = await availableCheckpointPeers(excluded)
          let lastTransferError: unknown
          for (const target of candidates) {
            try {
              const local = await peerMesh.storeArtifact(`${checkpointName}-s0`, artifact)
              const transfer = await peerMesh.sendArtifact(target.id, `${checkpointName}-s1`, artifact, false)
              return [
                { shard_index: 0, node_id: source[0], artifact_name: `${checkpointName}-s0`, sha256: local.sha256, bytes: local.bytes },
                { shard_index: 1, node_id: target.id, artifact_name: `${checkpointName}-s1`, sha256: transfer.sha256, bytes: transfer.bytes },
              ] satisfies ShardReceipt[]
            } catch (error) { lastTransferError = error }
          }
          throw lastTransferError instanceof Error ? lastTransferError : new Error('No active peer acknowledged the replicated checkpoint')
        }
        if (artifact.byteLength >= 64 * 1024) {
          const peers = await availableCheckpointPeers()
          let shardAttemptFailed = false
          if (peers.length >= 2) {
            const orderedPeers = peers.map((_, offset) => peers[(claim.batch_index + offset) % peers.length])
            try {
              const encoded = encode2Plus1(artifact)
              const local = await peerMesh.storeArtifact(`${checkpointName}-s0`, encoded[0].bytes)
              shardReceipts.push({ shard_index: 0, node_id: source[0], artifact_name: `${checkpointName}-s0`, sha256: local.sha256, bytes: local.bytes })
              for (const shard of encoded.slice(1)) {
                const target = orderedPeers[shard.index - 1]
                const transfer = await peerMesh.sendArtifact(target.id, `${checkpointName}-s${shard.index}`, shard.bytes, false)
                shardReceipts.push({ shard_index: shard.index, node_id: target.id, artifact_name: `${checkpointName}-s${shard.index}`, sha256: transfer.sha256, bytes: transfer.bytes })
              }
            } catch (error) {
              shardAttemptFailed = true
              shardReceipts.length = 0
              setNotice(`A shard peer left during batch ${claim.batch_index + 1}; retrying this checkpoint with full replicas`)
            }
          }
          if (peers.length < 2 || shardAttemptFailed) shardReceipts.push(...await tryReplicatedCheckpoint(new Set()))
        } else {
          shardReceipts.push(...await tryReplicatedCheckpoint(new Set()))
        }
        await api(`/api/jobs/${job.id}/commit`, { method: 'POST', body: JSON.stringify({
          lease: heartbeat.current(), sha256: checkpointDigest, bytes: artifact.byteLength, shards: shardReceipts,
        }) })
        checkpointPolicy = shardReceipts.length === 3 ? '2+1 shards' : 'two replicas'
        } finally { heartbeat.stop() }
        const current = await api<ComputeJob[]>(`/api/clusters/${job.cluster_id}/jobs`)
        setJobs(current)
        const updated = current.find(item => item.id === job.id)
        setNotice(`Checkpointed ${job.workload === 'vector-add' ? 'vector-add' : 'CPU'} batch ${claim.batch_index + 1} of ${job.total_batches} using ${checkpointPolicy}`)
        if (updated && updated.completed_batches >= updated.total_batches) { setNotice('All job batches completed and replicated'); return }
      }
      throw new Error('No batches became available before the worker wait limit. You can run the job again after active leases expire.')
      } finally { gpuWorker?.close() }
    })
  }
  async function restoreLatestCheckpoint(job: ComputeJob) {
    const checkpoint = job.checkpoints[job.checkpoints.length - 1]
    if (!checkpoint || !active) return
    const worker = [...sockets.current.entries()].find(([id, connection]) =>
      Object.prototype.hasOwnProperty.call(getSessionNodeTokens(), id)
      && nodes.some(node => node.id === id && node.cluster_id === active.id && node.online && !node.paused)
      && connection.socket?.readyState === WebSocket.OPEN && connection.peerMesh)
    if (!worker?.[1].peerMesh) { setError('Connect one of your cluster nodes in this browser to restore a checkpoint.'); return }
    setError(''); setNotice('Requesting checkpoint shards from their assigned peers…')
    await run(async () => {
      const onlineIds = new Set(nodes.filter(node => node.cluster_id === active.id && node.online && !node.paused).map(node => node.id))
      const available = await Promise.allSettled(checkpoint.shards.filter(shard => onlineIds.has(shard.node_id)).map(async shard => {
        const bytes = await worker[1].peerMesh!.getArtifact(shard.node_id, shard.artifact_name)
        if (await sha256Hex(bytes) !== shard.sha256) throw new Error(`Shard ${shard.shard_index} failed its SHA-256 check`)
        return { shard, bytes }
      }))
      const good = available.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
      let restored: Uint8Array | null = null
      if (checkpoint.shards.length === 2) {
        restored = good.find(entry => entry.shard.sha256 === checkpoint.sha256)?.bytes ?? null
      } else if (checkpoint.shards.length === 3 && good.length >= 2) {
        for (let i = 0; i < good.length && !restored; i++) for (let j = i + 1; j < good.length && !restored; j++) {
          const pair: EncodedShard[] = [
            { index: good[i].shard.shard_index as 0 | 1 | 2, bytes: good[i].bytes },
            { index: good[j].shard.shard_index as 0 | 1 | 2, bytes: good[j].bytes },
          ]
          try {
            const candidate = reconstruct2Plus1(pair, checkpoint.bytes)
            if (await sha256Hex(candidate) === checkpoint.sha256) restored = candidate
          } catch { /* Try another pair of surviving shards. */ }
        }
      }
      if (!restored) throw new Error('Could not reconstruct this checkpoint from the currently available shards.')
      const artifact = JSON.parse(new TextDecoder().decode(restored)) as { job_id?: string; batch_index?: number }
      if (artifact.job_id !== job.id || artifact.batch_index !== checkpoint.batch_index) throw new Error('Restored artifact metadata does not match this job checkpoint.')
      setNotice(`Recovered batch ${checkpoint.batch_index + 1} from ${good.length} verified shard(s); full artifact SHA-256 matches.`)
    })
  }
  async function removeMember(member: Member) {
    if (!active || member.role === 'owner') return
    openConfirmDialog({ title: 'Remove member?', description: `Remove ${member.id} from “${active.name}”? Their nodes will be disconnected.`, confirmLabel: 'Remove member', onConfirm: () => { void run(async () => { await api(`/api/clusters/${active.id}/members/${member.id}`, { method: 'DELETE' }); await refreshClusters(active.id); setNotice('Member removed') }) } })
  }
  async function revokeInvite(inv: ActiveInvite) {
    if (!active) return
    openConfirmDialog({ title: 'Revoke invite?', description: 'Anyone who has this link will no longer be able to join the cluster.', confirmLabel: 'Revoke invite', onConfirm: () => { void run(async () => { await api(`/api/clusters/${active.id}/invites/${inv.token}`, { method: 'DELETE' }); await refreshClusters(active.id); setNotice('Invite revoked') }) } })
  }
  async function renameCluster() {
    if (!active || active.role !== 'owner') return
    openInputDialog({ title: 'Rename cluster', description: 'Choose a new name for this cluster.', label: 'Cluster name', value: active.name, submitLabel: 'Save name', onSubmit: name => { if (name === active.name) return; void run(async () => { const c = await api<Cluster>(`/api/clusters/${active.id}`, { method: 'PATCH', body: JSON.stringify({ name }) }); await refreshClusters(c.id); setNotice('Cluster renamed') }) } })
  }
  async function deleteCluster() {
    if (!active || active.role !== 'owner') return
    const cluster = active
    const removedNodeIds = nodes.map(node => node.id)
    openConfirmDialog({ title: `Delete “${cluster.name}”?`, description: 'This permanently removes the cluster, its nodes, and active invites. Type DELETE to confirm.', confirmLabel: 'Delete cluster', requiredValue: 'DELETE', onConfirm: () => { void run(async () => {
      await api(`/api/clusters/${cluster.id}`, { method: 'DELETE' })
      removedNodeIds.forEach(stopBrowserNode)
      await refreshClusters()
      setNotice(`Deleted “${cluster.name}”`)
    }) } })
  }
  async function leaveCluster() {
    if (!active || active.role !== 'member') return
    openConfirmDialog({ title: 'Leave cluster?', description: `Leave “${active.name}”? You will need a new invite to rejoin.`, confirmLabel: 'Leave cluster', onConfirm: () => { void run(async () => { await api(`/api/clusters/${active.id}/leave`, { method: 'POST' }); await refreshClusters(); setNotice('You left the cluster') }) } })
  }
  async function copy(text: string) {
    await navigator.clipboard.writeText(text); setNotice('Copied to clipboard')
  }

  return <div className="manage-app">
    <header className="manage-top"><a className="manage-brand" href="/"><span className="brand-glyph"><Boxes size={18}/></span>mesh<span className="brand-period">.</span></a><div className={`control-badge ${coordinatorStatus}`}><i/>{coordinatorStatus === 'online' ? 'Coordinator online' : coordinatorStatus === 'offline' ? 'Coordinator unavailable' : 'Checking coordinator'}</div><button className="refresh-button" title="Refresh" onClick={() => void run(() => refreshClusters())}><RefreshCw size={15}/></button></header>
    <main className="manage-main">
      {invite && <div className="invite-banner"><div><b>Invitation to {invite.cluster_name}</b><span>Join this cluster with your current Mesh identity.</span></div><button className="manage-button primary" onClick={() => void join(joinToken)}>Join cluster <ArrowRight size={15}/></button><button className="dismiss-button" onClick={() => { setInvite(null); setJoinToken(''); history.replaceState({}, '', location.pathname) }} aria-label="Dismiss"><X size={17}/></button></div>}
      <div className="manage-heading"><div><div className="overline">COMPUTE NETWORK</div><h1>Clusters</h1><p>Manage cluster membership, invitations, and connected nodes.</p></div><div className="heading-buttons"><button className="manage-button" onClick={openInviteDialog}><Link2 size={15}/>Join by invite</button><button className="manage-button primary" onClick={() => void createCluster()}><Plus size={16}/>New cluster</button></div></div>
      {error && <div className="feedback error">{error}</div>}{notice && <div className="feedback success"><Check size={14}/>{notice}</div>}
      {busy ? <div className="empty-state">Connecting to coordinator…</div> : <div className="manage-layout">
        <aside className="cluster-rail"><div className="rail-title">YOUR CLUSTERS <span>{clusters.length}</span></div>{clusters.map(c=><button key={c.id} className={`rail-cluster ${active?.id===c.id?'active':''}`} onClick={()=>void run(()=>refreshClusters(c.id))}><span className="rail-icon">{c.name.slice(0,2).toUpperCase()}</span><span className="rail-label"><b>{c.name}</b><small>{c.role} · {c.members} members</small></span><span className={`rail-state ${c.online_nodes?'online':''}`}/></button>)}{!clusters.length&&<p className="rail-empty">No clusters yet.</p>}<button className="rail-create" onClick={() => void createCluster()}><Plus size={14}/>Create cluster</button><div className="identity-note"><Shield size={14}/><span>Your browser holds your private Mesh identity key. Clearing site data loses access to this anonymous account.</span></div></aside>
        <section className="cluster-content">{!active ? <div className="empty-state"><div className="empty-symbol"><Boxes size={22}/></div><h2>Start your first cluster</h2><p>Create a pool, then invite members and register their nodes.</p><button className="manage-button primary" onClick={() => void createCluster()}><Plus size={16}/>Create cluster</button></div> : <>
          <div className="cluster-summary"><div className="summary-ident"><span className="summary-icon">{active.name.slice(0,2).toUpperCase()}</span><div><div className="overline">{active.role === 'owner' ? 'OWNER' : 'MEMBER'} CLUSTER</div><h2>{active.name}</h2><span className="summary-sub">{active.members} members · {active.nodes} registered nodes</span></div></div><div className="summary-actions">{active.role==='owner'?<><button className="manage-button" onClick={() => void makeInvite()}><Link2 size={15}/>Invite members</button><button className="manage-button" onClick={() => void renameCluster()}><Settings size={15}/>Rename</button><button className="manage-button danger" onClick={() => void deleteCluster()}><Trash2 size={15}/>Delete cluster</button></>:<button className="manage-button" onClick={() => void leaveCluster()}><LogOut size={15}/>Leave cluster</button>}</div></div>
          <div className="resource-grid"><article className="resource-card"><div className="card-heading"><div><h3><Cpu size={16}/>Nodes</h3><p>Machines registered to this cluster</p></div><div className="heading-buttons"><button className="manage-button small" disabled={nodes.filter(node=>node.online&&!node.paused).length<2} onClick={() => void testPeerTransfer()}>Test P2P</button><button className="manage-button small primary" onClick={() => void createNode()}><Plus size={14}/>Add node</button></div></div>{nodes.length===0?<div className="table-empty">No nodes registered. Add a node to get its secure connection URL.</div>:<div className="node-list">{nodes.map(n=><div className="node-row" key={n.id}><span className={`node-dot ${n.online&&!n.paused?'online':''}`}/><div className="node-name"><b>{n.name}</b><small>{n.paused?'Paused':n.online?'Connected':'Offline'}{n.last_seen?` · seen ${new Date(n.last_seen*1000).toLocaleTimeString()}`:''}</small></div><button className="node-action" title={n.paused?'Resume':'Pause'} onClick={() => void toggleNode(n)}>{n.paused?<Play size={14}/>:<Pause size={14}/>}</button><button className="node-action danger" title="Remove node" onClick={() => void removeNode(n)}><Trash2 size={14}/></button></div>)}</div>}</article>
            <article className="resource-card"><div className="card-heading"><div><h3><Users size={16}/>Members</h3><p>People with access to this cluster</p></div><span className="count-chip">{members.length}</span></div><div className="member-list">{members.map(m=><div className="member-row" key={m.id}><span className="member-avatar">{m.role==='owner'?'O':'M'}</span><div className="node-name"><b>{m.role==='owner'?'Cluster owner':m.id}</b><small>{m.role==='owner'?'Full cluster control':'Contributor access'}</small></div><span className={`role-chip ${m.role}`}>{m.role}</span>{active.role==='owner'&&m.role!=='owner'&&<button className="node-action danger" title="Remove member" onClick={()=>void removeMember(m)}><Trash2 size={14}/></button>}</div>)}</div>{active.role==='owner'&&<div className="invite-management"><div className="invite-management-title">ACTIVE INVITES <span>{invites.length}</span></div>{invites.length?invites.map(inv=><div className="invite-row" key={inv.token}><span className="invite-key"><Link2 size={12}/>{inv.token.slice(0,10)}…</span><span className="invite-expiry">expires {new Date(inv.expires_at*1000).toLocaleDateString()}</span><button className="node-action danger" title="Revoke invite" onClick={()=>void revokeInvite(inv)}><X size={13}/></button></div>):<div className="no-invites">No active links. Create one with Invite members.</div>}</div>}<div className="member-footnote">Removing a member also disconnects their nodes.</div></article></div>
          <article className="resource-card job-card"><div className="card-heading"><div><h3><Activity size={16}/>Compute jobs</h3><p>Small checkpoints use two copies; larger results use 2+1 shards across three nodes.</p></div><div className="heading-buttons"><button className="manage-button small" onClick={() => void createComputeJob('cpu-hash')}>Create CPU demo</button><button className="manage-button small primary" onClick={() => void createComputeJob('vector-add')}>Create WebGPU job</button></div></div>{jobs.length===0?<div className="table-empty">Create a bounded CPU or WebGPU vector-add workload to exercise leases and recoverable checkpoints.</div>:<div className="job-list">{jobs.map(job=>{const done=job.completed_batches>=job.total_batches;const percent=Math.floor((job.completed_batches/job.total_batches)*100);const online=nodes.filter(node=>node.online&&!node.paused).length;return <div className="job-row" key={job.id}><div className="job-title"><b>{job.workload==='vector-add'?'Vector addition (WebGPU with CPU fallback)':'Deterministic CPU hash'}</b><small>{job.total_units.toLocaleString()} elements · {job.completed_batches}/{job.total_batches} batches · {done?'Complete':'Available'}{job.checkpoints.length?` · ${job.checkpoints.length} recoverable checkpoint(s)`:''}</small><div className="job-progress"><span style={{width:`${percent}%`}}/></div></div><div className="heading-buttons">{job.checkpoints.some(checkpoint=>checkpoint.shards.length>0)&&<button className="manage-button small" onClick={()=>void restoreLatestCheckpoint(job)}>Test restore</button>}<button className="manage-button small" disabled={done||online<(job.workload==='vector-add'?3:2)} onClick={() => void runComputeJob(job)}>{done?'Complete':'Run on this node'}</button></div></div>})}</div>}</article>
          <div className="coordinator-note"><Activity size={16}/><div><b>Coordinator manages leases and manifests; artifact bytes move peer to peer.</b><span>Large batches are split into two data shards plus XOR parity and written to three distinct nodes. Any two verified shards can reconstruct a checkpoint; use “Test restore” to request shards, reconstruct the latest checkpoint, and verify its full-artifact digest. This prototype does not yet attest to trusted computation or issue rewards.</span></div></div>
        </>}</section>
      </div>}
      <footer className="manage-footer">Mesh early access <span>·</span> Cluster state is saved by the coordinator</footer>
    </main>
    {nodeCredentials && <div className="dialog-shade" onMouseDown={e=>{if(e.target===e.currentTarget)setNodeCredentials(null)}}><section className="credentials-dialog"><button className="dialog-close" onClick={()=>setNodeCredentials(null)} aria-label="Close"><X size={17}/></button><div className="dialog-icon"><Cpu size={19}/></div><div className="overline">NODE REGISTERED</div><h2>{nodeCredentials.name}</h2><p>This tab connects the browser node to the coordinator and its cluster peers. Keep it open while the node is online.</p><div className="credential-value">{nodeCredentials.url}</div><button className="manage-button primary full-width" onClick={()=>void copy(nodeCredentials.url)}><Copy size={15}/>Copy worker connection URL</button><div className="credential-hint">The URL is a bearer credential. Direct peer transfers use WebRTC; the coordinator relays signaling only. A general task runner is not connected yet.</div></section></div>}
    {dialog && <div className="dialog-shade" onMouseDown={e=>{if(e.target===e.currentTarget)setDialog(null)}}><form className="credentials-dialog app-dialog" role="dialog" aria-modal="true" aria-labelledby="app-dialog-title" onSubmit={submitDialog}><button type="button" className="dialog-close" onClick={()=>setDialog(null)} aria-label="Close dialog"><X size={17}/></button><div className="dialog-icon">{dialog.kind==='input'?<Settings size={19}/>:<Shield size={19}/>}</div><div className="overline">{dialog.kind==='input'?'CLUSTER MANAGEMENT':'CONFIRM ACTION'}</div><h2 id="app-dialog-title">{dialog.title}</h2><p>{dialog.description}</p>{(dialog.kind==='input'||dialog.requiredValue)&&<><label className="field-label" htmlFor="app-dialog-value">{dialog.kind==='input'?dialog.label:'Type DELETE to confirm'}</label><input id="app-dialog-value" className="text-input" autoFocus value={dialogValue} onChange={e=>setDialogValue(e.target.value)} placeholder={dialog.kind==='input'?dialog.placeholder:dialog.requiredValue} data-testid="app-dialog-input" /></>}{dialog.kind==='confirm'&&<div className="credential-hint">Review this action before continuing.</div>}<div className="heading-buttons dialog-actions"><button type="button" className="manage-button" onClick={()=>setDialog(null)}>Cancel</button><button type="submit" className={`manage-button primary ${dialog.kind==='confirm'?'danger':''}`} disabled={dialog.kind==='input'?!dialogValue.trim():dialog.requiredValue!==undefined&&dialogValue!==dialog.requiredValue} data-testid="app-dialog-submit">{dialog.kind==='input'?dialog.submitLabel:dialog.confirmLabel}</button></div></form></div>}
  </div>
}

export default App

