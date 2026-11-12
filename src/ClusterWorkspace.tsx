import { useState, type ReactNode } from 'react'
import { Activity, Check, ChevronDown, Cpu, Layers3, Link2, LogOut, Monitor, Pause, Play, Plus, Settings, Trash2, Users, X } from 'lucide-react'
import { WorkspaceRail } from './WorkspaceRail'
import type { ClusterNode, ComputeCheckpoint, ComputeJob } from './compute-types'

export type Cluster = { id: string; name: string; role: 'owner' | 'member'; members: number; nodes: number; online_nodes: number }
export type ClusterMember = { id: string; role: 'owner' | 'member' }
export type ClusterInvite = { token: string; expires_at: number; project_name?: string | null; ml_only?: boolean }
type ClusterView = 'workers' | 'diagnostics' | 'access'
type ClusterActions = {
  select: (id: string) => void
  create: () => void
  join: () => void
  invite: () => void
  rename: () => void
  delete: () => void
  leave: () => void
  addNode: () => void
  toggleNode: (node: ClusterNode) => void
  removeNode: (node: ClusterNode) => void
  removeMember: (member: ClusterMember) => void
  revokeInvite: (invite: ClusterInvite) => void
  testPeer: () => void
  createJob: (workload: 'cpu-hash' | 'vector-add') => void
  deleteJob: (job: ComputeJob) => void
  runJob: (job: ComputeJob) => void
  restore: (job: ComputeJob) => void
  startBrowser: () => void
  stopBrowser: () => void
}

const views = [
  { id: 'workers', label: 'Workers', icon: Monitor },
  { id: 'diagnostics', label: 'Diagnostics', icon: Activity },
  { id: 'access', label: 'Members & invites', icon: Users },
] as const

function checkpointHealth(job: ComputeJob) {
  const checkpoint = job.checkpoints[job.checkpoints.length - 1]
  const label = checkpoint.durability === 'healthy' ? 'Healthy' : checkpoint.durability === 'degraded' ? 'Degraded' : 'Unavailable'
  return `${label} · ${checkpoint.available_shards}/${checkpoint.shards.length} holders online · ${checkpoint.verified_holders} verified holder receipts`
}

function checkpointDiagnostics(checkpoint: ComputeCheckpoint) {
  const execution = checkpoint.execution
  const capture = execution?.gpu_readback_ms === undefined ? '' : `, readback ${execution.gpu_readback_ms.toFixed(1)} ms (within compute, ${execution.gpu_buffers_reused ? 'reused buffers' : 'fresh buffers'})`
  const attempt = checkpoint.attempt
  const timings = execution ? `${execution.execution_mode} · self-reported: compute ${execution.compute_ms.toFixed(1)} ms${capture}, CPU validation ${execution.validation_ms.toFixed(1)} ms, encoding ${execution.encoding_ms.toFixed(1)} ms, transfer ${execution.transfer_ms.toFixed(1)} ms${execution.fallback_reason ? ` · ${execution.fallback_reason}` : ''}` : 'Execution timings unavailable for this older checkpoint'
  const reason = attempt?.previous_attempt_reason
  const end = attempt?.previous_attempt_ended_at_ms
  const failure = reason === 'disconnect' || reason === 'expiry'
  const recovery = reason ? ` · previous attempt: ${reason}${failure && attempt?.failure_detected_at_ms != null && checkpoint.committed_at_ms != null ? ` · coordinator detection → commit ${Math.max(0, checkpoint.committed_at_ms - attempt.failure_detected_at_ms)} ms` : !failure && end != null && checkpoint.committed_at_ms != null ? ` → commit ${Math.max(0, checkpoint.committed_at_ms - end)} ms` : ''}` : ''
  return `Worker ${checkpoint.worker_node_id?.slice(0, 8) ?? 'unknown'}${attempt ? ` · generation ${attempt.generation}${attempt.previous_worker_id && attempt.previous_worker_id !== attempt.worker_id ? ' · different-worker takeover' : ''}` : ''} · ${timings}${recovery}`
}

export function ClusterWorkspace({ active, clusters, nodes, jobs, members, invites, actions, runningJobIds, browserContributing, busy, detailsReady, error, notice, shareLink, renderContributor }: {
  active?: Cluster
  clusters: Cluster[]
  nodes: ClusterNode[]
  jobs: ComputeJob[]
  members: ClusterMember[]
  invites: ClusterInvite[]
  actions: ClusterActions
  runningJobIds: string[]
  browserContributing: boolean
  busy: boolean
  detailsReady: boolean
  error: string
  notice: string
  shareLink: ReactNode
  renderContributor: (node: ClusterNode) => ReactNode
}) {
  const [view, setView] = useState<ClusterView>('workers')
  const owner = active?.role === 'owner'
  const readyPeers = nodes.filter(node => node.online && !node.paused && node.holder_ready).length
  const activeJobs = jobs.filter(job => job.completed_batches < job.total_batches).length
  const identity = active && <>
    <span className="workspace-identity-icon"><Layers3 size={19}/></span>
    <span className="workspace-identity-name"><b>{active.name}</b><small>{owner ? 'Owner' : 'Member'} · {active.online_nodes} online</small></span>
  </>

  return <div className="workspace-shell cluster-workspace">
    <WorkspaceRail label="Cluster menu">{close => <>
      <div className="workspace-rail-label">Clusters</div>
      {active && (clusters.length > 1 ? <details className="workspace-switcher">
        <summary className="workspace-identity" aria-label="Switch cluster">
          {identity}
          <ChevronDown size={15}/>
        </summary>
        <div className="workspace-switcher-options" aria-label="Your clusters">
          {clusters.map(cluster => <button key={cluster.id} type="button" className="workspace-rail-item"
            aria-current={active.id === cluster.id ? 'true' : undefined} onClick={event => {
              event.currentTarget.closest('details')?.removeAttribute('open')
              actions.select(cluster.id)
              setView('workers')
              close()
            }}>
            <Layers3 size={16}/><span>{cluster.name} <small>{cluster.role}</small></span>
          </button>)}
        </div>
      </details> : <div className="workspace-identity">{identity}</div>)}
      {active && <nav className="workspace-rail-nav" aria-label="Cluster tools">
        {views.map(({ id, label, icon: Icon }) => <button key={id} type="button" className="workspace-rail-item"
          aria-pressed={view === id} onClick={() => { setView(id); close() }}><Icon size={18}/>{label}</button>)}
      </nav>}
      <div className="workspace-rail-actions">
        <button className="workspace-rail-item" type="button" onClick={actions.create}><Plus size={18}/>New cluster</button>
        <button className="workspace-rail-item" type="button" onClick={actions.join}><Link2 size={17}/>Join by invite</button>
      </div>
    </>}</WorkspaceRail>

    <div className={`workspace-detail${active ? ' with-context' : ''}`}>
      <header className="workspace-overview">
        <div className="workspace-title"><h1>{active?.name ?? 'Clusters'}</h1>
          {active && <span className="context-chip"><Cpu size={13}/>{owner ? 'Owner' : 'Member'}</span>}
        </div>
        {active && <dl className="workspace-metrics" aria-label="Cluster statistics">
          <div><dt><Monitor size={21}/>Cluster workers</dt><dd>{active.nodes} registered / {active.online_nodes} online</dd></div>
          <div><dt><Users size={21}/>Members</dt><dd>{active.members}</dd></div>
          <div><dt><Activity size={21}/>Unfinished compute jobs</dt><dd>{detailsReady ? activeJobs : '—'}</dd></div>
        </dl>}
      </header>
      <div className="workspace-primary">
        {error && <p className="feedback error" role="alert">{error}</p>}
        {notice && <p className="feedback success" role="status"><Check size={14}/>{notice}</p>}
        {shareLink}
        {busy || active && !detailsReady ? <p className="workspace-panel" role="status">Loading cluster…</p> : !active ? (
          <section className="workspace-panel workspace-empty">
            <span className="workspace-empty-icon"><Layers3 size={27}/></span>
            <h2>Start your first cluster</h2>
            <p>Create a cluster and share a link so others can contribute from their browsers.</p>
            <div className="heading-buttons mobile-empty-actions">
              <button className="manage-button primary" type="button" onClick={actions.create}><Plus size={16}/>Create a cluster</button>
              <button className="manage-button" type="button" onClick={actions.join}><Link2 size={16}/>Join with a link</button>
            </div>
          </section>
        ) : view === 'workers' ? (
          <section className="workspace-panel" aria-label="Cluster workers">
            <div className="workspace-panel-heading">
              <div><h2>Workers</h2><p>Browsers and nodes registered to this cluster.</p></div>
              <button className="manage-button small primary" type="button" onClick={actions.addNode}><Plus size={15}/>Add node</button>
            </div>
            {nodes.length === 0 ? <div className="workspace-empty-row">Start this browser or add a named node.</div> :
              <div className="node-list">{nodes.map(node => <div className="node-entry" key={node.id}>
                <div className="node-row">
                  <span className={`node-dot ${node.online && !node.paused ? 'online' : ''}`}/>
                  <div className="node-name">
                    <b>{node.name}</b>
                    <small>{node.paused ? 'Paused' : node.online ? (node.holder_ready ? 'Connected · storage verified' : 'Connected · refresh to enable storage verification') : 'Offline'}{node.last_seen ? ` · seen ${new Date(node.last_seen * 1000).toLocaleTimeString()}` : ''}</small>
                  </div>
                  <button type="button" className="node-action" aria-label={node.paused ? 'Resume node' : 'Pause node'} title={node.paused ? 'Resume' : 'Pause'} onClick={() => actions.toggleNode(node)}>{node.paused ? <Play size={15}/> : <Pause size={15}/>}</button>
                  <button type="button" className="node-action danger" aria-label="Remove node" title="Remove node" onClick={() => actions.removeNode(node)}><Trash2 size={15}/></button>
                </div>
                {renderContributor(node)}
              </div>)}</div>}
          </section>
        ) : view === 'diagnostics' ? (
          <>
            <section className="workspace-panel" aria-label="Peer connections">
              <div className="workspace-panel-heading">
                <div><h2>Peer connections</h2><p>{readyPeers} storage-verified {readyPeers === 1 ? 'node' : 'nodes'} online. A peer test needs at least two.</p></div>
                <button className="manage-button small" type="button" disabled={readyPeers < 2} onClick={actions.testPeer}>Test P2P</button>
              </div>
            </section>
            <section className="workspace-panel" aria-label="Cluster demo jobs">
              <div className="workspace-panel-heading">
                <div><h2>Cluster demo jobs</h2><p>Run CPU or WebGPU diagnostics and inspect recoverable checkpoints.</p></div>
                <div className="heading-buttons">
                  <button className="manage-button small" type="button" onClick={() => actions.createJob('cpu-hash')}>Create CPU demo</button>
                  <button className="manage-button small primary" type="button" onClick={() => actions.createJob('vector-add')}>Create WebGPU job</button>
                </div>
              </div>
              {jobs.length === 0 ? <div className="workspace-empty-row">No demo jobs yet.</div> : <div className="job-list">
                {jobs.map(job => {
                  const done = job.completed_batches >= job.total_batches
                  const running = runningJobIds.includes(job.id)
                  const checkpoint = job.checkpoints[job.checkpoints.length - 1]
                  const percent = Math.floor(job.completed_batches / job.total_batches * 100)
                  return <div className="job-row" key={job.id}>
                    <div className="job-title">
                      <b>{job.workload === 'vector-add' ? 'Vector addition (WebGPU with CPU fallback)' : 'Deterministic CPU hash'}</b>
                      <small>{job.total_units.toLocaleString()} elements · {job.completed_batches}/{job.total_batches} batches · {done ? 'Complete' : 'Available'}{checkpoint ? ` · ${job.checkpoints.length} recoverable checkpoint(s)` : ''}</small>
                      {checkpoint && <><small className="checkpoint-durability">{checkpointHealth(job)}</small><small className="execution-diagnostics">{checkpointDiagnostics(checkpoint)}</small></>}
                      <div className="job-progress" role="progressbar" aria-label="Batches completed" aria-valuenow={percent}><span style={{ width: `${percent}%` }}/></div>
                    </div>
                    <div className="heading-buttons">
                      {owner && <button type="button" aria-label="Delete job" title="Delete job" className="icon-button" onClick={() => actions.deleteJob(job)}><Trash2 size={16}/></button>}
                      {job.checkpoints.some(item => item.shards.length > 0) && <button className="manage-button small" type="button" onClick={() => actions.restore(job)}>Test restore</button>}
                      <button className="manage-button small" type="button" disabled={done || readyPeers < 2 || running} onClick={() => actions.runJob(job)}>{done ? 'Complete' : running ? 'Running…' : 'Run on this node'}</button>
                    </div>
                  </div>
                })}
              </div>}
              <details className="cluster-recovery-help"><summary>How checkpoints recover</summary><p>With two nodes, Mesh stores full copies. With three, it prefers two data shards plus XOR parity. Choose Test restore on a checkpoint to fetch verified shards and reconstruct the original artifact.</p></details>
            </section>
          </>
        ) : (
          <>
            <section className="workspace-panel" aria-label="Cluster members">
              <div className="workspace-panel-heading"><div><h2>Members</h2><p>People with access to this cluster.</p></div><span className="count-chip">{members.length}</span></div>
              <div className="member-list">{members.map(member => <div className="member-row" key={member.id}>
                <span className="member-avatar"><Users size={15}/></span>
                <div className="node-name"><b>{member.role === 'owner' ? 'Cluster owner' : member.id}</b><small>{member.role === 'owner' ? 'Full cluster control' : 'Contributor access'}</small></div>
                <span className={`role-chip ${member.role}`}>{member.role}</span>
                {owner && member.role !== 'owner' && <button type="button" className="node-action danger" aria-label="Remove member" title="Remove member" onClick={() => actions.removeMember(member)}><Trash2 size={15}/></button>}
              </div>)}</div>
              {owner && <p className="member-footnote">Removing a member also disconnects their nodes.</p>}
            </section>
            {owner && <section className="workspace-panel" aria-label="Active invites">
              <div className="workspace-panel-heading"><div><h2>Active invites</h2><p>Revoke links that should stop accepting new contributors.</p></div><span className="count-chip">{invites.length}</span></div>
              {invites.length ? invites.map(invite => <div className="invite-row" key={invite.token}>
                <span className="invite-key"><Link2 size={15}/>{invite.project_name ? `${invite.project_name}${invite.ml_only ? ' ML contributor' : ' contributor'}` : 'Cluster invite'}</span>
                <span className="invite-expiry">Expires {new Date(invite.expires_at * 1000).toLocaleDateString()}</span>
                <button type="button" className="node-action danger" aria-label="Revoke invite" title="Revoke invite" onClick={() => actions.revokeInvite(invite)}><X size={15}/></button>
              </div>) : <div className="workspace-empty-row">No active links.</div>}
            </section>}
          </>
        )}
      </div>
      {active && <aside className="workspace-inspector" aria-label="Cluster context">
        <section className="inspector-card">
          <h2>This browser</h2>
          <div className="inspector-identity"><Monitor size={20}/><strong>{browserContributing ? 'Contributing compute' : 'Ready to connect'}</strong></div>
          <p>{browserContributing ? 'Keep this tab open to contribute.' : 'Share compute with this cluster while this tab is open.'}</p>
          {browserContributing ? <button className="manage-button" type="button" onClick={actions.stopBrowser}>Stop this browser</button>
            : <button className="manage-button primary" type="button" disabled={!detailsReady} onClick={actions.startBrowser}>Start this browser</button>}
        </section>
        {owner && <section className="inspector-card">
          <h2>Invite link</h2><p>Let people join {active.name} and contribute compute.</p>
          <button className="manage-button" type="button" onClick={actions.invite}><Link2 size={16}/>Invite members</button>
          {view !== 'access' && <button className="inspector-text-button" type="button" onClick={() => setView('access')}>View active invites</button>}
        </section>}
        <section className="inspector-card">
          <h2>Cluster settings</h2>
          {owner ? <>
            <button className="manage-button" type="button" onClick={actions.rename}><Settings size={16}/>Rename cluster</button>
            <button className="manage-button danger" type="button" disabled={!detailsReady} onClick={actions.delete}><Trash2 size={16}/>Delete cluster</button>
          </> : <button className="manage-button" type="button" onClick={actions.leave}><LogOut size={16}/>Leave cluster</button>}
        </section>
      </aside>}
    </div>
  </div>
}
