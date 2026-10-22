import { useEffect, useRef, useState } from 'react'
import { Activity, ArrowRight, Boxes, Check, Copy, Cpu, Link2, LogOut, Pause, Play, Plus, RefreshCw, Settings, Shield, Trash2, Users, X } from 'lucide-react'

type Cluster = { id: string; name: string; role: 'owner' | 'member'; members: number; nodes: number; online_nodes: number }
type Node = { id: string; cluster_id: string; name: string; paused: boolean; online: boolean; last_seen: number | null }
type Member = { id: string; role: 'owner' | 'member' }
type ActiveInvite = { token: string; expires_at: number }
type InvitePreview = { cluster_id: string; cluster_name: string; expires_at: number }
type NodeCreated = { node: Node; token: string }
type BrowserNodeConnection = { url: string; socket: WebSocket | null; retryTimer: number | null; heartbeatTimer: number | null; stableTimer: number | null; attempts: number; stopped: boolean }

function getIdentity() {
  let token = localStorage.getItem('mesh.identity')
  if (!token) { token = `${crypto.randomUUID().replace(/-/g, '')}${crypto.randomUUID().replace(/-/g, '')}`; localStorage.setItem('mesh.identity', token) }
  return token
}

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('X-Mesh-Identity', getIdentity())
  if (init.body) headers.set('Content-Type', 'application/json')
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) throw new Error((await response.text()) || `Request failed (${response.status})`)
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

function App() {
  const [clusters, setClusters] = useState<Cluster[]>([])
  const [activeId, setActiveId] = useState('')
  const [nodes, setNodes] = useState<Node[]>([])
  const [members, setMembers] = useState<Member[]>([])
  const [invites, setInvites] = useState<ActiveInvite[]>([])
  const [invite, setInvite] = useState<InvitePreview | null>(null)
  const [joinToken, setJoinToken] = useState(new URLSearchParams(window.location.search).get('invite') ?? '')
  const [nodeCredentials, setNodeCredentials] = useState<{ name: string; url: string } | null>(null)
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
      setNodes(nextNodes); setMembers(nextMembers)
      const selected = data.find(c => c.id === nextId)
      setInvites(selected?.role === 'owner' ? await api<ActiveInvite[]>(`/api/clusters/${nextId}/invites`) : [])
    } else { setNodes([]); setMembers([]); setInvites([]) }
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
  async function createCluster() {
    const name = window.prompt('Name your cluster')?.trim()
    if (!name) return
    await run(async () => { const c = await api<Cluster>('/api/clusters', { method: 'POST', body: JSON.stringify({ name }) }); await refreshClusters(c.id); setNotice(`Created “${c.name}”`) })
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
  async function createNode() {
    if (!active) return
    const name = window.prompt('Name this node (for example: Office GPU)')?.trim()
    if (!name) return
    await run(async () => {
      const created = await api<NodeCreated>(`/api/clusters/${active.id}/nodes`, { method: 'POST', body: JSON.stringify({ name }) })
      const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:'
      const url = `${wsProto}//${location.host}/api/nodes/connect?token=${created.token}`
      setNodeCredentials({ name, url })
      connectBrowserNode(created.node.id, url)
      await refreshClusters(active.id)
    })
  }
  function connectBrowserNode(id: string, url: string) {
    stopBrowserNode(id)
    const connection: BrowserNodeConnection = { url, socket: null, retryTimer: null, heartbeatTimer: null, stableTimer: null, attempts: 0, stopped: false }
    sockets.current.set(id, connection)
    openBrowserNode(id, connection)
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
      void refreshClusters(activeId).catch(() => undefined)
    })
    socket.addEventListener('message', event => {
      try { const message = JSON.parse(String(event.data)) as { type?: string; paused?: boolean }; if (message.type === 'control') setNotice(message.paused ? `${nameForNode(id)} paused` : `${nameForNode(id)} resumed`) } catch { /* Ignore unknown worker messages. */ }
    })
    socket.addEventListener('close', () => {
      if (connection.heartbeatTimer !== null) window.clearInterval(connection.heartbeatTimer)
      if (connection.stableTimer !== null) window.clearTimeout(connection.stableTimer)
      connection.heartbeatTimer = null
      connection.stableTimer = null
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
    sockets.current.delete(id)
    connection.socket?.close()
  }
  function nameForNode(id: string) { return nodes.find(node => node.id === id)?.name ?? 'Node' }
  async function toggleNode(node: Node) {
    await run(async () => { await api(`/api/nodes/${node.id}/${node.paused ? 'resume' : 'pause'}`, { method: 'POST' }); await refreshClusters(active.id) })
  }
  async function removeNode(node: Node) {
    if (!window.confirm(`Remove node “${node.name}”? Its connection token will stop working.`)) return
    await run(async () => { await api(`/api/nodes/${node.id}`, { method: 'DELETE' }); stopBrowserNode(node.id); await refreshClusters(active.id) })
  }
  async function removeMember(member: Member) {
    if (!active || member.role === 'owner' || !window.confirm(`Remove ${member.id} from “${active.name}”?`)) return
    await run(async () => { await api(`/api/clusters/${active.id}/members/${member.id}`, { method: 'DELETE' }); await refreshClusters(active.id); setNotice('Member removed') })
  }
  async function revokeInvite(inv: ActiveInvite) {
    if (!active || !window.confirm('Revoke this invite link? Anyone who has it will no longer be able to join.')) return
    await run(async () => { await api(`/api/clusters/${active.id}/invites/${inv.token}`, { method: 'DELETE' }); await refreshClusters(active.id); setNotice('Invite revoked') })
  }
  async function renameCluster() {
    if (!active || active.role !== 'owner') return
    const name = window.prompt(`Rename “${active.name}”`, active.name)?.trim()
    if (!name || name === active.name) return
    await run(async () => { const c = await api<Cluster>(`/api/clusters/${active.id}`, { method: 'PATCH', body: JSON.stringify({ name }) }); await refreshClusters(c.id); setNotice('Cluster renamed') })
  }
  async function deleteCluster() {
    if (!active || active.role !== 'owner') return
    if (window.prompt(`Permanently delete “${active.name}” and all its nodes and invites? Type DELETE to confirm.`) !== 'DELETE') return
    const removedNodeIds = nodes.map(node => node.id)
    await run(async () => {
      await api(`/api/clusters/${active.id}`, { method: 'DELETE' })
      removedNodeIds.forEach(stopBrowserNode)
      await refreshClusters()
      setNotice(`Deleted “${active.name}”`)
    })
  }
  async function leaveCluster() {
    if (!active || active.role !== 'member' || !window.confirm(`Leave “${active.name}”?`)) return
    await run(async () => { await api(`/api/clusters/${active.id}/leave`, { method: 'POST' }); await refreshClusters(); setNotice('You left the cluster') })
  }
  async function copy(text: string) {
    await navigator.clipboard.writeText(text); setNotice('Copied to clipboard')
  }

  return <div className="manage-app">
    <header className="manage-top"><a className="manage-brand" href="/"><span className="brand-glyph"><Boxes size={18}/></span>mesh<span className="brand-period">.</span></a><div className={`control-badge ${coordinatorStatus}`}><i/>{coordinatorStatus === 'online' ? 'Coordinator online' : coordinatorStatus === 'offline' ? 'Coordinator unavailable' : 'Checking coordinator'}</div><button className="refresh-button" title="Refresh" onClick={() => void run(() => refreshClusters())}><RefreshCw size={15}/></button></header>
    <main className="manage-main">
      {invite && <div className="invite-banner"><div><b>Invitation to {invite.cluster_name}</b><span>Join this cluster with your current Mesh identity.</span></div><button className="manage-button primary" onClick={() => void join(joinToken)}>Join cluster <ArrowRight size={15}/></button><button className="dismiss-button" onClick={() => { setInvite(null); setJoinToken(''); history.replaceState({}, '', location.pathname) }} aria-label="Dismiss"><X size={17}/></button></div>}
      <div className="manage-heading"><div><div className="overline">COMPUTE NETWORK</div><h1>Clusters</h1><p>Manage cluster membership, invitations, and connected nodes.</p></div><div className="heading-buttons"><button className="manage-button" onClick={() => { const value = window.prompt('Paste a cluster invite URL or token'); if (value) { const token = value.includes('invite=') ? new URL(value, location.origin).searchParams.get('invite') ?? '' : value.trim(); setJoinToken(token); void run(async () => { setInvite(await api<InvitePreview>(`/api/invites/${encodeURIComponent(token)}`)) }) } }}><Link2 size={15}/>Join by invite</button><button className="manage-button primary" onClick={() => void createCluster()}><Plus size={16}/>New cluster</button></div></div>
      {error && <div className="feedback error">{error}</div>}{notice && <div className="feedback success"><Check size={14}/>{notice}</div>}
      {busy ? <div className="empty-state">Connecting to coordinator…</div> : <div className="manage-layout">
        <aside className="cluster-rail"><div className="rail-title">YOUR CLUSTERS <span>{clusters.length}</span></div>{clusters.map(c=><button key={c.id} className={`rail-cluster ${active?.id===c.id?'active':''}`} onClick={()=>void run(()=>refreshClusters(c.id))}><span className="rail-icon">{c.name.slice(0,2).toUpperCase()}</span><span className="rail-label"><b>{c.name}</b><small>{c.role} · {c.members} members</small></span><span className={`rail-state ${c.online_nodes?'online':''}`}/></button>)}{!clusters.length&&<p className="rail-empty">No clusters yet.</p>}<button className="rail-create" onClick={() => void createCluster()}><Plus size={14}/>Create cluster</button><div className="identity-note"><Shield size={14}/><span>Your browser holds your private Mesh identity key. Clearing site data loses access to this anonymous account.</span></div></aside>
        <section className="cluster-content">{!active ? <div className="empty-state"><div className="empty-symbol"><Boxes size={22}/></div><h2>Start your first cluster</h2><p>Create a pool, then invite members and register their nodes.</p><button className="manage-button primary" onClick={() => void createCluster()}><Plus size={16}/>Create cluster</button></div> : <>
          <div className="cluster-summary"><div className="summary-ident"><span className="summary-icon">{active.name.slice(0,2).toUpperCase()}</span><div><div className="overline">{active.role === 'owner' ? 'OWNER' : 'MEMBER'} CLUSTER</div><h2>{active.name}</h2><span className="summary-sub">{active.members} members · {active.nodes} registered nodes</span></div></div><div className="summary-actions">{active.role==='owner'?<><button className="manage-button" onClick={() => void makeInvite()}><Link2 size={15}/>Invite members</button><button className="manage-button" onClick={() => void renameCluster()}><Settings size={15}/>Rename</button><button className="manage-button danger" onClick={() => void deleteCluster()}><Trash2 size={15}/>Delete cluster</button></>:<button className="manage-button" onClick={() => void leaveCluster()}><LogOut size={15}/>Leave cluster</button>}</div></div>
          <div className="resource-grid"><article className="resource-card"><div className="card-heading"><div><h3><Cpu size={16}/>Nodes</h3><p>Machines registered to this cluster</p></div><button className="manage-button small primary" onClick={() => void createNode()}><Plus size={14}/>Add node</button></div>{nodes.length===0?<div className="table-empty">No nodes registered. Add a node to get its secure connection URL.</div>:<div className="node-list">{nodes.map(n=><div className="node-row" key={n.id}><span className={`node-dot ${n.online&&!n.paused?'online':''}`}/><div className="node-name"><b>{n.name}</b><small>{n.online?'Connected':n.paused?'Paused':'Offline'}{n.last_seen?` · seen ${new Date(n.last_seen*1000).toLocaleTimeString()}`:''}</small></div><button className="node-action" title={n.paused?'Resume':'Pause'} onClick={() => void toggleNode(n)}>{n.paused?<Play size={14}/>:<Pause size={14}/>}</button><button className="node-action danger" title="Remove node" onClick={() => void removeNode(n)}><Trash2 size={14}/></button></div>)}</div>}</article>
            <article className="resource-card"><div className="card-heading"><div><h3><Users size={16}/>Members</h3><p>People with access to this cluster</p></div><span className="count-chip">{members.length}</span></div><div className="member-list">{members.map(m=><div className="member-row" key={m.id}><span className="member-avatar">{m.role==='owner'?'O':'M'}</span><div className="node-name"><b>{m.role==='owner'?'Cluster owner':m.id}</b><small>{m.role==='owner'?'Full cluster control':'Contributor access'}</small></div><span className={`role-chip ${m.role}`}>{m.role}</span>{active.role==='owner'&&m.role!=='owner'&&<button className="node-action danger" title="Remove member" onClick={()=>void removeMember(m)}><Trash2 size={14}/></button>}</div>)}</div>{active.role==='owner'&&<div className="invite-management"><div className="invite-management-title">ACTIVE INVITES <span>{invites.length}</span></div>{invites.length?invites.map(inv=><div className="invite-row" key={inv.token}><span className="invite-key"><Link2 size={12}/>{inv.token.slice(0,10)}…</span><span className="invite-expiry">expires {new Date(inv.expires_at*1000).toLocaleDateString()}</span><button className="node-action danger" title="Revoke invite" onClick={()=>void revokeInvite(inv)}><X size={13}/></button></div>):<div className="no-invites">No active links. Create one with Invite members.</div>}</div>}<div className="member-footnote">Removing a member also disconnects their nodes.</div></article></div>
          <div className="coordinator-note"><Activity size={16}/><div><b>Coordinator manages control; nodes do the compute.</b><span>Node registration and pause controls work now. A worker agent that performs WebGPU or native jobs is a separate next step.</span></div></div>
        </>}</section>
      </div>}
      <footer className="manage-footer">Mesh early access <span>·</span> Cluster state is saved by the coordinator</footer>
    </main>
    {nodeCredentials && <div className="dialog-shade" onMouseDown={e=>{if(e.target===e.currentTarget)setNodeCredentials(null)}}><section className="credentials-dialog"><button className="dialog-close" onClick={()=>setNodeCredentials(null)} aria-label="Close"><X size={17}/></button><div className="dialog-icon"><Cpu size={19}/></div><div className="overline">NODE REGISTERED</div><h2>{nodeCredentials.name}</h2><p>This browser tab connects to the node automatically. Do not open the WebSocket address as a webpage. It is only for a future native worker agent.</p><div className="credential-value">{nodeCredentials.url}</div><button className="manage-button primary full-width" onClick={()=>void copy(nodeCredentials.url)}><Copy size={15}/>Copy for native agent</button><div className="credential-hint">This prototype accepts heartbeats and pause commands; it does not dispatch compute jobs yet. Keep this tab open to keep the browser node connected.</div></section></div>}
  </div>
}

export default App

