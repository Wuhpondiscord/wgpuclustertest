import { useEffect, useRef, useState, type FormEvent } from 'react'
import { ArrowRight, Copy, Cpu, RefreshCw, Settings, Shield, X } from 'lucide-react'
import { PeerMeshNode } from './peer'
import { HolderCustody } from './custody'
import { executeRepair, type RepairTask } from './repair'
import { api, getIdentity, isRevokedAccess } from './api'
import { ComputeRunner, type RunnerState } from './compute-runner'
import { ContributorControls } from './ContributorControls'
import { MeshBrand, WorkspaceNav, type Workspace } from './WorkspaceNav'
import { AccountApplications } from './AccountApplications'
import { ClusterWorkspace, type Cluster, type ClusterInvite as ActiveInvite, type ClusterMember as Member } from './ClusterWorkspace'
import { allContributionSettings, forgetNode, getNodeTokens, isRemembered, rememberNode, saveContributionSettings, setNodeToken, subscribeContributionSettings, type ContributionSettings } from './contribution-settings'
import type { ClusterNode as Node, ComputeJob } from './compute-types'
import { reconstruct2Plus1, sha256Hex, type EncodedShard } from './sharding'

type InvitePreview = { cluster_id: string; cluster_name: string; expires_at: number; project_name?: string | null }
type NodeCreated = { node: Node; token: string }
type BrowserNodeConnection = { clusterId: string; paused: boolean; hasLock: boolean; lockPending: boolean; releaseLock: (() => void) | null; url: string; socket: WebSocket | null; retryTimer: number | null; heartbeatTimer: number | null; stableTimer: number | null; attempts: number; stopped: boolean; peerMesh: PeerMeshNode | null; custody: HolderCustody | null; repairRunning: string | null }
type UiDialog =
  | { kind: 'input'; title: string; description: string; label: string; value: string; placeholder?: string; submitLabel: string; onSubmit: (value: string) => void }
  | { kind: 'confirm'; title: string; description: string; confirmLabel: string; requiredValue?: string; onConfirm: () => void }

function App() {
  const [workspace, setWorkspace] = useState<Workspace>(location.hash === '#applications' ? 'applications' : 'clusters')
  useEffect(() => {
    const update = () => setWorkspace(location.hash === '#applications' ? 'applications' : 'clusters')
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  }, [])
  const [clusters, setClusters] = useState<Cluster[]>([])
  const [activeId, setActiveId] = useState('')
  const [loadedClusterId, setLoadedClusterId] = useState('')
  const [nodes, setNodes] = useState<Node[]>([])
  const [members, setMembers] = useState<Member[]>([])
  const [invites, setInvites] = useState<ActiveInvite[]>([])
  const [jobs, setJobs] = useState<ComputeJob[]>([])
  const [invite, setInvite] = useState<InvitePreview | null>(null)
  const [joinToken, setJoinToken] = useState(new URLSearchParams(window.location.search).get('invite') ?? '')
  const [joining, setJoining] = useState(false)
  const [shareLink, setShareLink] = useState('')
  const shareLinkInput = useRef<HTMLInputElement>(null)
  const startingBrowsers = useRef(new Set<string>())
  const [nodeCredentials, setNodeCredentials] = useState<{ name: string; key: string } | null>(null)
  const [dialog, setDialog] = useState<UiDialog | null>(null)
  const [dialogValue, setDialogValue] = useState('')
  const sockets = useRef(new Map<string, BrowserNodeConnection>())
  const runners = useRef(new Map<string, ComputeRunner>())
  const contributionOptions = useRef(allContributionSettings())
  const [contributions, setContributions] = useState(contributionOptions.current)
  const [runnerStates, setRunnerStates] = useState<Record<string, RunnerState>>({})
  const [, setCredentialRevision] = useState(0)
  const [runningJobIds, setRunningJobIds] = useState<string[]>([])
  const activeSelection = useRef(new URLSearchParams(location.search).get('cluster') ?? '')
  const refreshGeneration = useRef(0)
  const [busy, setBusy] = useState(true)
  const [coordinatorStatus, setCoordinatorStatus] = useState<'checking' | 'online' | 'offline'>('checking')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const active = clusters.find(c => c.id === activeId) ?? clusters[0]
  useEffect(() => { if (activeId) activeSelection.current = activeId }, [activeId])
  useEffect(() => { if (shareLink) shareLinkInput.current?.focus() }, [shareLink])

  async function refreshClusters(preferId?: string) {
    if (preferId) activeSelection.current = preferId
    const generation = ++refreshGeneration.current
    const data = await api<Cluster[]>('/api/clusters')
    if (generation !== refreshGeneration.current) return
    for (const [id, connection] of sockets.current) if (!data.some(cluster => cluster.id === connection.clusterId)) {
      stopContributor(id); forgetNode(id); stopBrowserNode(id)
    }
    setClusters(data)
    const selection = activeSelection.current
    const nextId = preferId && data.some(c => c.id === preferId) ? preferId : data.some(c => c.id === selection) ? selection : data[0]?.id ?? ''
    setActiveId(nextId)
    activeSelection.current = nextId
    if (nextId) {
      const selected = data.find(c => c.id === nextId)
      const [nextNodes, nextMembers, nextJobs, nextInvites] = await Promise.all([
        api<Node[]>(`/api/clusters/${nextId}/nodes`),
        api<Member[]>(`/api/clusters/${nextId}/members`),
        api<ComputeJob[]>(`/api/clusters/${nextId}/jobs`),
        selected?.role === 'owner' ? api<ActiveInvite[]>(`/api/clusters/${nextId}/invites`) : Promise.resolve([]),
      ])
      if (generation !== refreshGeneration.current || activeSelection.current !== nextId) return
      setNodes(nextNodes); setMembers(nextMembers)
      setJobs(nextJobs); setInvites(nextInvites); setLoadedClusterId(nextId)
      for (const [id, connection] of sockets.current) if (connection.clusterId === nextId && !nextNodes.some(node => node.id === id)) {
        stopContributor(id); forgetNode(id); stopBrowserNode(id)
      }
      const tokens = getNodeTokens()
      for (const node of nextNodes) {
        const token = tokens[node.id]
        if (token && !sockets.current.has(node.id)) {
          const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:'
          connectBrowserNode(node, `${wsProto}//${location.host}/api/nodes/connect?token=${token}`)
        }
      }
      void restoreContributors(data, nextNodes).catch(() => setError('Could not restore a saved contributor; refresh to retry.'))
    } else { setNodes([]); setMembers([]); setInvites([]); setJobs([]); setLoadedClusterId('') }
  }

  useEffect(() => {
    void getIdentity().catch(() => undefined)
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
    let stopped = false
    const timer = window.setInterval(() => {
      void api<ComputeJob[]>(`/api/clusters/${activeId}/jobs`).then(jobs => { if (!stopped) setJobs(jobs) }).catch(() => undefined)
    }, 5000)
    return () => { stopped = true; window.clearInterval(timer) }
  }, [activeId])

  useEffect(() => {
    if (!activeId) return
    const timer = window.setInterval(() => { void refreshClusters(activeSelection.current).catch(e => setError(e instanceof Error ? e.message : 'Refresh failed')) }, 12000)
    return () => window.clearInterval(timer)
  }, [activeId])

  useEffect(() => () => { for (const runner of runners.current.values()) runner.stop(); for (const id of sockets.current.keys()) stopBrowserNode(id) }, [])

  useEffect(() => subscribeContributionSettings((id, settings) => {
    contributionOptions.current = { ...contributionOptions.current, [id]: settings }
    setContributions(contributionOptions.current)
    if (!settings.enabled) runners.current.get(id)?.stop()
    else runners.current.get(id)?.settingsChanged()
    // Re-read membership before restoring any newly enabled contributor.
    if (settings.enabled) void refreshClusters(activeSelection.current).catch(() => undefined)
  }), [])

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
    await run(() => shareInvite(`/api/clusters/${active.id}/invites`, active.id))
  }
  async function shareInvite(path: string, clusterId: string) {
    const data = await api<{ token: string }>(path, { method: 'POST' })
    const url = `${location.origin}/?invite=${data.token}`
    setShareLink(url)
    try {
      await navigator.clipboard.writeText(url)
      setNotice('Invite link copied')
    } catch {
      setNotice('Select and copy the invite link below')
    }
    await refreshClusters(clusterId).catch(() => undefined)
  }
  async function join(contribute = false) {
    if (joining) return
    setJoining(true)
    try {
      await run(async () => {
        const joined = await api<Cluster>('/api/invites/join', { method: 'POST', body: JSON.stringify({ token: joinToken }) })
        setInvite(null)
        setJoinToken('')
        history.replaceState({}, '', location.pathname)
        await refreshClusters(joined.id)
        setNotice(`Joined “${joined.name}”`)
        if (contribute) await startThisBrowser(joined.id)
      })
    } finally {
      setJoining(false)
    }
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
      const created = await registerBrowserNode(active.id, name)
      setNodeCredentials({ name, key: created.token })
      await refreshClusters(active.id)
    }) } })
  }
  async function registerBrowserNode(clusterId: string, name: string) {
    const created = await api<NodeCreated>(`/api/clusters/${clusterId}/nodes`, { method: 'POST', body: JSON.stringify({ name }) })
    setNodeToken(created.node.id, created.token)
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
    connectBrowserNode(created.node, `${protocol}//${location.host}/api/nodes/connect?token=${created.token}`)
    return created
  }
  async function startThisBrowser(clusterId: string) {
    if (startingBrowsers.current.has(clusterId)) return
    startingBrowsers.current.add(clusterId)
    try {
      const tokens = getNodeTokens()
      const registered = await api<Node[]>(`/api/clusters/${clusterId}/nodes`)
      const eligible = registered.filter(node => tokens[node.id] && !node.paused)
      let node = eligible.find(node => settingsFor(node).enabled) ?? eligible[0]
      if (!node) {
        node = (await registerBrowserNode(clusterId, 'Browser contributor')).node
        updateContribution(node, { enabled: true, dutyCycle: 50 })
      } else {
        if (!sockets.current.has(node.id)) {
          const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
          connectBrowserNode(node, `${protocol}//${location.host}/api/nodes/connect?token=${tokens[node.id]}`)
        }
        updateContribution(node, { enabled: true })
      }
      startRunner(node)
      setNotice('Compute is enabled for this browser. Keep this tab open to contribute.')
      await refreshClusters(clusterId).catch(() => undefined)
    } finally {
      startingBrowsers.current.delete(clusterId)
    }
  }
  function connectBrowserNode(node: Node, url: string) {
    const id = node.id
    stopBrowserNode(id)
    const connection: BrowserNodeConnection = { clusterId: node.cluster_id, paused: node.paused, hasLock: !navigator.locks, lockPending: false, releaseLock: null, url, socket: null, retryTimer: null, heartbeatTimer: null, stableTimer: null, attempts: 0, stopped: false, peerMesh: null, custody: null, repairRunning: null }
    sockets.current.set(id, connection)
    openBrowserNode(id, connection)
  }
  function createPeerMesh(id: string, socket: WebSocket, custody: HolderCustody) {
    return new PeerMeshNode(id, (peerNodeId, payload) => {
      if (socket.readyState !== WebSocket.OPEN) throw new Error('Coordinator signaling connection is closed')
      socket.send(JSON.stringify({ type: 'signal', peer_node_id: peerNodeId, payload }))
    }, setNotice, (name, sha256, bytes) => custody.attest(name, sha256, bytes))
  }
  function openBrowserNode(id: string, connection: BrowserNodeConnection) {
    if (connection.stopped || sockets.current.get(id) !== connection || !navigator.onLine) return
    if (!connection.hasLock && navigator.locks) {
      if (connection.lockPending) return
      connection.lockPending = true
      void navigator.locks.request(`mesh-node-${id}`, { ifAvailable: true }, async lock => {
        connection.lockPending = false
        if (connection.stopped) return
        if (!lock) { setNotice('This node is active in another tab. Waiting for that tab to disconnect.'); scheduleBrowserReconnect(id, connection); return }
        connection.hasLock = true
        try { await new Promise<void>(resolve => { connection.releaseLock = resolve; openBrowserNode(id, connection) }) }
        finally { connection.hasLock = false; connection.releaseLock = null }
      }).catch(() => { connection.lockPending = false; setError('Could not acquire this browser node session') })
      return
    }
    let socket: WebSocket
    try { const endpoint = new URL(connection.url); endpoint.search = ''; socket = new WebSocket(endpoint.toString()) }
    catch { scheduleBrowserReconnect(id, connection); return }
    connection.socket = socket
    socket.addEventListener('open', () => {
      if (connection.stopped || sockets.current.get(id) !== connection) { socket.close(); return }
      socket.send(JSON.stringify({ type: 'authenticate', token: new URL(connection.url).searchParams.get('token') }))
      if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
      connection.stableTimer = window.setTimeout(() => { connection.attempts = 0; connection.stableTimer = null }, 60000)
      connection.heartbeatTimer = window.setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.send('{"type":"heartbeat"}') }, 15000)
      connection.peerMesh?.close()
      connection.custody?.close()
      connection.custody = new HolderCustody(id, socket)
      connection.peerMesh = createPeerMesh(id, socket, connection.custody)
      void refreshClusters(activeSelection.current).catch(() => undefined)
    })
    socket.addEventListener('message', event => {
      try {
        const message = JSON.parse(String(event.data)) as { type?: string; paused?: boolean; peer_node_id?: string; peer_node_ids?: string[]; task?: RepairTask; payload?: { description?: RTCSessionDescriptionInit; candidate?: RTCIceCandidateInit } }
        if (message.type === 'control') {
          if (typeof message.paused !== 'boolean') throw new Error('Invalid node control state')
          const changed = connection.paused !== message.paused
          connection.paused = message.paused === true
          if (changed) runners.current.get(id)?.transportChanged()
          if (message.paused) { connection.peerMesh?.close(); connection.peerMesh = null }
          else if (!connection.peerMesh && connection.custody) connection.peerMesh = createPeerMesh(id, socket, connection.custody)
          if (changed) setNotice(message.paused ? `${nameForNode(id)} paused` : `${nameForNode(id)} resumed`)
        }
        else if (message.type === 'peer_list') connection.peerMesh?.addPeers(message.peer_node_ids ?? [])
        else if (message.type === 'peer_joined' && message.peer_node_id) connection.peerMesh?.peerJoined(message.peer_node_id)
        else if (message.type === 'peer_left' && message.peer_node_id) connection.peerMesh?.peerLeft(message.peer_node_id)
        else if (message.type === 'signal' && message.peer_node_id && message.payload) void connection.peerMesh?.handleSignal(message.peer_node_id, message.payload)
        else if (message.type === 'repair_checkpoint' && message.task && message.task.worker_node_id === id && connection.peerMesh && connection.custody) {
          const task = message.task
          if (connection.repairRunning) {
            if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'repair_failed', repair_id: task.id }))
            return
          }
          connection.repairRunning = task.id
          const mesh = connection.peerMesh
          const custody = connection.custody
          setNotice(`Repairing checkpoint ${task.batch_index + 1} on a spare node…`)
          void executeRepair(task, mesh).then(() => custody.request('repair_complete', { repair_id: task.id }, 90_000)).catch(error => {
            if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'repair_failed', repair_id: task.id }))
            setNotice(`Checkpoint repair will retry: ${error instanceof Error ? error.message : 'peer transfer failed'}`)
          }).finally(() => { if (connection.repairRunning === task.id) connection.repairRunning = null })
        }
        else if (message.type === 'work_available') { for (const [nodeId, nodeConnection] of sockets.current) if (nodeConnection.clusterId === connection.clusterId) runners.current.get(nodeId)?.wake() }
        else if (message.type === 'holder_ready') { for (const runner of runners.current.values()) runner.wake(); void refreshClusters(activeSelection.current).catch(() => undefined) }
        else if (message.type === 'checkpoint_repaired') {
          setNotice('Checkpoint redundancy restored on a replacement holder')
          void refreshClusters(activeSelection.current).catch(() => undefined)
        }
      } catch { setError('A coordinator message could not be processed; reconnecting this node to resynchronize.'); socket.close() }
    })
    socket.addEventListener('close', () => {
      if (connection.heartbeatTimer !== null) window.clearInterval(connection.heartbeatTimer)
      if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
      connection.heartbeatTimer = null
      connection.stableTimer = null
      connection.peerMesh?.close(); connection.peerMesh = null
      connection.custody?.close(); connection.custody = null
      connection.repairRunning = null
      if (connection.socket === socket) connection.socket = null
      if (connection.stopped || sockets.current.get(id) !== connection) return
      runners.current.get(id)?.transportChanged()
      setNotice(`${nameForNode(id)} disconnected; retrying automatically`)
      void api<Node[]>(`/api/clusters/${connection.clusterId}/nodes`, { signal: AbortSignal.timeout(12_000) }).then(currentNodes => {
        if (sockets.current.get(id) === connection && !currentNodes.some(node => node.id === id)) {
          stopContributor(id); forgetNode(id); stopBrowserNode(id)
        }
      }).catch(error => {
        if (sockets.current.get(id) === connection && isRevokedAccess(error)) {
          stopContributor(id); forgetNode(id); stopBrowserNode(id)
          setError('This node no longer has cluster access. Its local contributor has stopped.')
        }
      })
      void refreshClusters(activeSelection.current).catch(() => undefined)
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
    connection.custody?.close()
    sockets.current.delete(id)
    runners.current.get(id)?.transportChanged()
    connection.socket?.close()
    connection.releaseLock?.()
  }
  function nameForNode(id: string) { return nodes.find(node => node.id === id)?.name ?? 'Node' }
  async function toggleNode(node: Node) {
    await run(async () => { await api(`/api/nodes/${node.id}/${node.paused ? 'resume' : 'pause'}`, { method: 'POST' }); await refreshClusters(active.id) })
  }
  async function removeNode(node: Node) {
    openConfirmDialog({ title: 'Remove node?', description: `Remove “${node.name}” from this cluster? Its connection token will stop working.`, confirmLabel: 'Remove node', onConfirm: () => { void run(async () => { await api(`/api/nodes/${node.id}`, { method: 'DELETE' }); stopBrowserNode(node.id); forgetNode(node.id); stopContributor(node.id); await refreshClusters(active.id) }) } })
  }
  async function testPeerTransfer() {
    if (!active) return
    const source = [...sockets.current.entries()].find(([id, connection]) => nodes.some(node => node.id === id && node.cluster_id === active.id && !node.paused) && connection.socket?.readyState === WebSocket.OPEN && connection.peerMesh)
    const target = nodes.find(node => node.cluster_id === active.id && node.online && !node.paused && node.holder_ready && node.id !== source?.[0])
    if (!source || !target || !source[1].peerMesh) { setError('Keep this tab connected to a node and connect a second node in the same cluster first.'); return }
    setError(''); setNotice('Establishing a direct peer channel…')
    const bytes = new TextEncoder().encode(JSON.stringify({ kind: 'checkpoint-transfer-probe', cluster_id: active.id, generated_at: Date.now(), payload: 'mesh-peer-transfer'.repeat(2048) }))
    const failures: string[] = []
    const candidates = nodes.filter(node => node.cluster_id === active.id && node.online && !node.paused && node.holder_ready && node.id !== source[0])
    for (const peer of candidates) {
      try {
        const result = await source[1].peerMesh.sendArtifact(peer.id, `mesh-p2p-probe-${crypto.randomUUID()}`, bytes)
        setNotice(`Peer transfer verified: ${result.bytes.toLocaleString()} bytes, SHA-256 ${result.sha256.slice(0, 16)}…`)
      } catch (e) { failures.push(`${peer.name}: ${e instanceof Error ? e.message : 'transfer failed'}`) }
    }
    if (failures.length) setError(`Peer check failed for ${failures.join('; ')}. Cross-network connections may require a TURN relay.`)
    else setNotice(`Peer transfers verified to all ${candidates.length} other connected node(s). Run this test on every device to check all network pairs.`)
  }
  async function createComputeJob(workload: ComputeJob['workload']) {
    if (!active) return
    await run(async () => {
      await api<ComputeJob>(`/api/clusters/${active.id}/jobs`, { method: 'POST', body: JSON.stringify({ workload, total_units: 1_200_000, batch_units: 100_000 }) })
      setJobs(await api<ComputeJob[]>(`/api/clusters/${active.id}/jobs`))
      setNotice(workload === 'vector-add' ? 'Created a 12-batch WebGPU vector-add job (workers without WebGPU use the bounded CPU fallback)' : 'Created a 12-batch recoverable CPU demo')
    })
  }
  function settingsFor(node: Node): ContributionSettings {
    return contributionOptions.current[node.id] ?? { clusterId: node.cluster_id, enabled: false, dutyCycle: 100, allowCpuFallback: true, pauseWhenHidden: false, allowInference: false }
  }
  function updateContribution(node: Node, patch: Partial<ContributionSettings>) {
    const settings = { ...settingsFor(node), ...patch }
    contributionOptions.current = { ...contributionOptions.current, [node.id]: settings }
    setContributions(contributionOptions.current)
    saveContributionSettings(node.id, settings)
    runners.current.get(node.id)?.settingsChanged()
    return settings
  }
  function stopContributor(id: string) {
    runners.current.get(id)?.stop()
    const settings = contributionOptions.current[id]
    if (settings) {
      const updated = { ...settings, enabled: false }
      contributionOptions.current = { ...contributionOptions.current, [id]: updated }
      setContributions(contributionOptions.current); saveContributionSettings(id, updated)
    }
  }
  function startRunner(node: Node, jobId?: string) {
    if (runners.current.has(node.id)) return
    const runner = new ComputeRunner({
      nodeId: node.id, clusterId: node.cluster_id, jobId,
      settings: () => settingsFor(node),
      getMesh: () => {
        const connection = sockets.current.get(node.id)
        return connection && !connection.paused && connection.socket?.readyState === WebSocket.OPEN && connection.custody?.isReady() ? connection.peerMesh : null
      },
      state: state => {
        if (runners.current.get(node.id) !== runner) return
        setRunnerStates(current => ({ ...current, [node.id]: state }))
        if (state.phase === 'error') { setError(state.message); updateContribution(node, { enabled: false }) }
      },
      notice: message => { if (activeSelection.current === node.cluster_id) setNotice(message) },
      jobs: jobs => { if (activeSelection.current === node.cluster_id) setJobs(jobs) },
    })
    runners.current.set(node.id, runner)
    if (jobId) setRunningJobIds(current => [...current, jobId])
    void runner.run().finally(() => {
      if (runners.current.get(node.id) === runner) runners.current.delete(node.id)
      if (jobId) setRunningJobIds(current => current.filter(id => id !== jobId))
    })
  }
  function toggleContribution(node: Node) {
    if (settingsFor(node).enabled || runners.current.has(node.id)) { stopContributor(node.id); return }
    const connection = sockets.current.get(node.id)
    if (!connection?.hasLock || connection.socket?.readyState !== WebSocket.OPEN) {
      setError('Wait for this browser node to connect. If it is active in another tab, manage its compute there.'); return
    }
    updateContribution(node, { enabled: true })
    startRunner(node)
  }
  async function restoreContributors(data: Cluster[], selectedNodes: Node[]) {
    const tokens = getNodeTokens()
    const clusterIds = new Set(Object.values(contributionOptions.current).filter(settings => settings.enabled).map(settings => settings.clusterId))
    for (const clusterId of clusterIds) {
      if (!data.some(cluster => cluster.id === clusterId)) continue
      const clusterNodes = selectedNodes.some(node => node.cluster_id === clusterId) ? selectedNodes : await api<Node[]>(`/api/clusters/${clusterId}/nodes`)
      for (const node of clusterNodes) {
        if (!tokens[node.id] || !contributionOptions.current[node.id]?.enabled) continue
        if (!sockets.current.has(node.id)) {
          const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
          connectBrowserNode(node, `${protocol}//${location.host}/api/nodes/connect?token=${tokens[node.id]}`)
        }
        startRunner(node)
      }
    }
  }
  async function runComputeJob(job: ComputeJob) {
    const tokens = getNodeTokens()
    const source = nodes.find(node => node.cluster_id === job.cluster_id && node.online && !node.paused && tokens[node.id] && !runners.current.has(node.id)
      && sockets.current.get(node.id)?.socket?.readyState === WebSocket.OPEN && sockets.current.get(node.id)?.hasLock)
    if (!source) { setError('Connect a local node, or stop its existing worker before starting a manual job. Contributing nodes automatically find jobs.'); return }
    setError(''); setNotice('Worker is claiming and processing job batches…')
    startRunner(source, job.id)
  }
  async function restoreLatestCheckpoint(job: ComputeJob) {
    const checkpoint = job.checkpoints[job.checkpoints.length - 1]
    if (!checkpoint || !active) return
    const worker = [...sockets.current.entries()].find(([id, connection]) =>
      Object.prototype.hasOwnProperty.call(getNodeTokens(), id)
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
    openConfirmDialog({ title: 'Revoke invite?', description: inv.project_name ? 'This link will stop granting access to project jobs, including for guests who already joined through it.' : 'This link will stop accepting new cluster members.', confirmLabel: 'Revoke invite', onConfirm: () => { void run(async () => { await api(`/api/clusters/${active.id}/invites/${inv.token}`, { method: 'DELETE' }); setShareLink(''); await refreshClusters(active.id); setNotice('Invite revoked') }) } })
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
      for (const id of removedNodeIds) { stopContributor(id); forgetNode(id); stopBrowserNode(id) }
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

  const accountPath = location.pathname.split('/').filter(Boolean).length === 2
  const localTokens = getNodeTokens()
  const activeContributors = nodes.filter(node => node.cluster_id === active?.id && localTokens[node.id] && contributions[node.id]?.enabled)
  const accountActions = {
    clusters,
    changed: () => void refreshClusters(activeSelection.current).catch(() => undefined),
    startBrowser: startThisBrowser,
    shareCompute: (app: string, cluster: string) => shareInvite(`/api/applications/${app}/contributor-invites`, cluster),
  }
  const sharedInvite = shareLink && <section className="resource-card share-link" aria-label="Share invite link">
    <h3>Share this link</h3>
    <p>Friends can open it in their browser and choose Join and contribute. The link expires after seven days.</p>
    <label>Invite link<input ref={shareLinkInput} readOnly value={shareLink} onFocus={event => event.currentTarget.select()}/></label>
    <div className="heading-buttons">
      <button className="manage-button" type="button" onClick={() => void run(() => copy(shareLink))}>Copy invite link</button>
      <button className="manage-button" type="button" onClick={() => setShareLink('')}>Close link</button>
    </div>
  </section>
  const currentWorkspace = accountPath ? 'applications' : workspace
  const detailsReady = active?.id === loadedClusterId
  const status = <>{error && <p role="alert" className="feedback error">{error}</p>}
    {notice && <p role="status" className="feedback success">{notice}</p>}
    {sharedInvite}</>

  function deleteJob(job: ComputeJob) {
    openConfirmDialog({
      title: 'Delete job?',
      description: 'Remove this job and its checkpoint metadata? Browser artifact bytes are retained until local storage is cleared.',
      confirmLabel: 'Delete job',
      onConfirm: () => void run(async () => {
        await api(`/api/jobs/${job.id}`, { method: 'DELETE' })
        await refreshClusters(activeSelection.current)
      }),
    })
  }

  return <div className="manage-app">
    <header className="manage-top">
      <MeshBrand/>
      <WorkspaceNav current={currentWorkspace} onChange={accountPath ? undefined : setWorkspace}/>
      <div className="header-utilities">
        <div className={`control-badge ${coordinatorStatus}`}><i/>{coordinatorStatus === 'online' ? 'Coordinator online' : coordinatorStatus === 'offline' ? 'Coordinator unavailable' : 'Checking coordinator'}</div>
        <button className="refresh-button" type="button" aria-label="Refresh" title="Refresh" onClick={() => void run(() => refreshClusters())}><RefreshCw size={16}/></button>
        <div id="mesh-profile-slot" className="header-profile"/>
      </div>
    </header>
    <main className="manage-main workspace-main">
      {invite && <section className="resource-card join-card" aria-label="Cluster invitation">
        <h2>Invitation to {invite.cluster_name}</h2>
        {invite.project_name && <p>You’re invited to contribute to <strong>{invite.project_name}</strong>. Your browser receives the inputs for jobs it takes. Project compute access lasts until the link expires or the owner revokes it.</p>}
        <p>Join and contribute to share this browser’s compute while this tab is open. You can stop at any time.</p>
        <div className="heading-buttons">
          <button className="manage-button primary" disabled={joining} onClick={() => void join(true)}>Join and contribute <ArrowRight size={15}/></button>
          <button className="manage-button" disabled={joining} onClick={() => void join()}>Join cluster</button>
          <button className="dismiss-button" disabled={joining} onClick={() => { setInvite(null); setJoinToken(''); history.replaceState({}, '', location.pathname) }} aria-label="Dismiss invitation"><X size={17}/></button>
        </div>
      </section>}
      <AccountApplications clusterId={accountPath ? undefined : active?.id} workspace={currentWorkspace} {...accountActions}
        status={currentWorkspace === 'applications' ? status : undefined}
        clusterPicker={!accountPath && clusters.length > 0 && <label className="cluster-picker">Compute cluster
          <select value={active?.id ?? ''} onChange={event => void run(() => refreshClusters(event.target.value))}>
            {clusters.map(cluster => <option key={cluster.id} value={cluster.id}>{cluster.name}</option>)}
          </select>
        </label>}/>
      <div hidden={currentWorkspace !== 'clusters'}>
        <ClusterWorkspace active={active} clusters={clusters} nodes={detailsReady ? nodes : []}
          jobs={jobs.filter(job => job.cluster_id === active?.id)} members={detailsReady ? members : []}
          invites={detailsReady ? invites : []} detailsReady={detailsReady} busy={busy} error={error} notice={notice}
          shareLink={currentWorkspace === 'clusters' ? sharedInvite : null}
          runningJobIds={runningJobIds} browserContributing={detailsReady && activeContributors.length > 0}
          renderContributor={node => localTokens[node.id] && <ContributorControls
            settings={contributions[node.id] ?? settingsFor(node)} state={runnerStates[node.id]} remembered={isRemembered(node.id)}
            onToggle={() => toggleContribution(node)} onChange={patch => updateContribution(node, patch)}
            onRemember={remember => { rememberNode(node.id, remember); setCredentialRevision(value => value + 1) }}/>}
          actions={{
            select: id => void run(() => refreshClusters(id)),
            create: createCluster, join: openInviteDialog, invite: makeInvite, rename: renameCluster, delete: deleteCluster, leave: leaveCluster,
            addNode: createNode, toggleNode, removeNode, removeMember, revokeInvite, testPeer: testPeerTransfer,
            createJob: createComputeJob, deleteJob, runJob: runComputeJob, restore: restoreLatestCheckpoint,
            startBrowser: () => { if (active) void run(() => startThisBrowser(active.id)) },
            stopBrowser: () => activeContributors.forEach(node => stopContributor(node.id)),
          }}/>
      </div>
    </main>
    {nodeCredentials && <div className="dialog-shade" onMouseDown={e=>{if(e.target===e.currentTarget)setNodeCredentials(null)}}><section className="credentials-dialog"><button className="dialog-close" onClick={()=>setNodeCredentials(null)} aria-label="Close"><X size={17}/></button><div className="dialog-icon"><Cpu size={19}/></div><div className="overline">NODE REGISTERED</div><h2>{nodeCredentials.name}</h2><p>This tab connects the browser node to the coordinator and its cluster peers. Keep it open while the node is online.</p><div className="credential-value">{nodeCredentials.key}</div><button className="manage-button primary full-width" onClick={()=>void copy(nodeCredentials.key)}><Copy size={15}/>Copy node connection key</button><div className="credential-hint">Save this server-generated key securely. It is a bearer credential for this node. Use Contribute compute on this node to opt into cluster jobs. Remember this device is optional.</div></section></div>}
    {dialog && <div className="dialog-shade" onMouseDown={e=>{if(e.target===e.currentTarget)setDialog(null)}}><form className="credentials-dialog app-dialog" role="dialog" aria-modal="true" aria-labelledby="app-dialog-title" onSubmit={submitDialog}><button type="button" className="dialog-close" onClick={()=>setDialog(null)} aria-label="Close dialog"><X size={17}/></button><div className="dialog-icon">{dialog.kind==='input'?<Settings size={19}/>:<Shield size={19}/>}</div><div className="overline">{dialog.kind==='input'?'CLUSTER MANAGEMENT':'CONFIRM ACTION'}</div><h2 id="app-dialog-title">{dialog.title}</h2><p>{dialog.description}</p>{(dialog.kind==='input'||dialog.requiredValue)&&<><label className="field-label" htmlFor="app-dialog-value">{dialog.kind==='input'?dialog.label:'Type DELETE to confirm'}</label><input id="app-dialog-value" className="text-input" autoFocus value={dialogValue} onChange={e=>setDialogValue(e.target.value)} placeholder={dialog.kind==='input'?dialog.placeholder:dialog.requiredValue} data-testid="app-dialog-input" /></>}{dialog.kind==='confirm'&&<div className="credential-hint">Review this action before continuing.</div>}<div className="heading-buttons dialog-actions"><button type="button" className="manage-button" onClick={()=>setDialog(null)}>Cancel</button><button type="submit" className={`manage-button primary ${dialog.kind==='confirm'?'danger':''}`} disabled={dialog.kind==='input'?!dialogValue.trim():dialog.requiredValue!==undefined&&dialogValue!==dialog.requiredValue} data-testid="app-dialog-submit">{dialog.kind==='input'?dialog.submitLabel:dialog.confirmLabel}</button></div></form></div>}
  </div>
}

export default App
