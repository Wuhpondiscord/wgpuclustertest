import { createPortal } from 'react-dom'
import { Activity, ArrowLeft, Boxes, ChevronDown, FileCode2, Image as ImageIcon, Link2, LockKeyhole, Monitor, Play, Settings2, Users } from 'lucide-react'
import { prepareAvatar } from './profile-picture'
import { ApplicationRuntime } from './ApplicationRuntime'
import { VisionOverview, VisionStudio, type VisionStage, type VisionStageRequest, type VisionSummary } from './VisionStudio'
import { ImageBatchRuntime } from './ImageBatchRuntime'
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { api, setIdentity } from './api'

type Account = { username: string; activated: boolean; recovery_session?: boolean; two_factor_enabled: boolean; recovery_codes_remaining: number; avatar?: string | null }
type Application = { id: string; cluster_id: string; name: string; slug: string; owner: string; url: string; role: 'owner' | 'operator' | 'viewer'; members: Array<{ username: string; role: string }>; runtime_status: string; can_share_compute?: boolean }
type ComputeCluster = { id: string; name: string; nodes: number; online_nodes: number }

function Avatar({ username, jpeg }: { username: string; jpeg?: string | null }) {
  return <span className="profile-avatar" aria-hidden="true">
    {jpeg ? <img src={`data:image/jpeg;base64,${jpeg}`} alt=""/> : username.slice(0, 2).toUpperCase()}
  </span>
}

type Setup = { secret: string; challenge: string; uri: string }
const clearDeviceCredentials = () => {
  for (const key of ['mesh.node-tokens', 'mesh.remembered-node-tokens', 'mesh.contribution-settings']) { sessionStorage.removeItem(key); localStorage.removeItem(key) }
}
export function AccountApplications({ clusterId, changed, startBrowser, shareCompute, clusters, clusterPicker, workspace = 'applications' }: {
  workspace?: 'clusters' | 'applications'
  clusterId?: string
  clusters: ComputeCluster[]
  clusterPicker?: ReactNode
  changed: () => void
  startBrowser: (clusterId: string) => Promise<void>
  shareCompute: (appId: string, clusterId: string) => Promise<void>
}) {
  const [account, setAccount] = useState<Account | null>(null)
  const [applications, setApplications] = useState<Application[]>([])
  const [runtimeSummary, setRuntimeSummary] = useState<{ appId: string; activeRuns: number } | null>(null)
  const [deploymentSummary, setDeploymentSummary] = useState<{ appId: string; name?: string; revision?: string } | null>(null)
  const [visionSummary, setVisionSummary] = useState<VisionSummary | null>(null)
  const [visionRequest, setVisionRequest] = useState<VisionStageRequest | null>(null)
  const [studioTab, setStudioTab] = useState<'workflows' | 'runs' | 'vision' | 'access'>('workflows')
  const [editName, setEditName] = useState('')
  const [selected, setSelected] = useState<Application | null>(null)
  const [mode, setMode] = useState<'signup' | 'login' | 'security' | 'profile' | null>(null)
  const [profileSlot, setProfileSlot] = useState<HTMLElement | null>(null)
  const [avatarDraft, setAvatarDraft] = useState<string | null>(null)
  const [username, setUsername] = useState('')
  const [key, setKey] = useState('')
  const [code, setCode] = useState('')
  const [slug, setSlug] = useState<string | null>(null)
  const [name, setName] = useState('')
  const [member, setMember] = useState('')
  const [role, setRole] = useState<'viewer' | 'operator'>('viewer')
  const [setup, setSetup] = useState<Setup | null>(null)
  const [saved, setSaved] = useState<{ title: string; values: string[] } | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState('')
  const [deleteConfirmed, setDeleteConfirmed] = useState(false)
  const route = location.pathname.split('/').filter(Boolean)
  const appRoute = route.length === 2 && !['api', 'assets'].includes(route[0])
  const suggestedSlug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 32).replace(/^-|-$/g, '')
  const appSlug = slug ?? (suggestedSlug && suggestedSlug.length < 3 ? suggestedSlug + '-app' : suggestedSlug)
  const refreshVersion = useRef(0)
  async function refresh() {
    const generation = ++refreshVersion.current
    const current = sessionStorage.getItem('mesh.account-session') ? await api<Account>('/api/account') : null
    if (generation !== refreshVersion.current) return
    setAccount(current)
    const list = clusterId ? await api<Application[]>(`/api/clusters/${clusterId}/applications`) : []
    const routed = appRoute ? await api<Application>(`/api/applications/by-name/${encodeURIComponent(route[0])}/${encodeURIComponent(route[1])}`) : null
    if (generation !== refreshVersion.current) return
    setApplications(list)
    setSelected(previous => routed ?? list.find(app => app.id === previous?.id) ?? null)
  }
  useEffect(() => { let active = true; void refresh().catch(e => { if (active) setError(e.message) }); return () => { active = false; refreshVersion.current++ } }, [clusterId])
  useEffect(() => { setProfileSlot(document.getElementById('mesh-profile-slot')) }, [])
  useEffect(() => {
    const expired = () => { refreshVersion.current++; setAvatarDraft(null); setMode(null); setSaved(null); setAccount(null); setApplications([]); setSelected(null); setNotice('Sign in again to resume this account. Remembered node keys have been kept.') }
    window.addEventListener('mesh-authentication-required', expired)
    return () => window.removeEventListener('mesh-authentication-required', expired)
  }, [])
  useEffect(() => {
    if (!mode && !saved) return
    const dialogs = document.querySelectorAll<HTMLElement>('.account-applications [role="dialog"]')
    const activeDialog = dialogs[dialogs.length - 1]
    const focusable = () => [...(activeDialog?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled), a[href]') ?? [])]
    if (!activeDialog?.contains(document.activeElement)) focusable()[0]?.focus()
    const close = (event: KeyboardEvent) => {
      if (event.key === 'Tab') {
        const elements = focusable(); const index = elements.indexOf(document.activeElement as HTMLElement)
        if (elements.length && (index === -1 || event.shiftKey && index === 0 || !event.shiftKey && index === elements.length - 1)) { event.preventDefault(); elements[event.shiftKey ? elements.length - 1 : 0].focus() }
      }
      if (event.key === 'Escape' && !saved && !busy) {
        if (mode === 'profile') profileSlot?.querySelector<HTMLButtonElement>('.profile-trigger')?.focus()
        setMode(null); setKey(''); setCode(''); setSetup(null); setAvatarDraft(null)
      }
    }
    document.addEventListener('keydown', close); return () => document.removeEventListener('keydown', close)
  }, [mode, saved, busy, profileSlot])
  async function run(action: () => Promise<void>) {
    if (busy) return
    setBusy(true); setError(''); setNotice('')
    try { await action() } catch (error) { setError(error instanceof Error ? error.message : 'Request failed') } finally { setBusy(false) }
  }
  async function submit(event: FormEvent) {
    event.preventDefault()
    await run(async () => {
      if (mode === 'signup') {
        const result = await api<{ account: Account; key: string; session: string }>('/api/accounts', { method: 'POST', body: JSON.stringify({ username }) })
        setIdentity(result.session, result.account.username, true); setAccount(result.account); setSaved({ title: 'Save your account key', values: [result.key] }); setMode(null); changed()
      } else if (mode === 'login') {
        const result = await api<{ account: Account; session: string }>('/api/accounts/login', { method: 'POST', body: JSON.stringify({ username, key: key.trim(), code: code.trim() }) })
        if (localStorage.getItem('mesh.last-account') !== result.account.username) clearDeviceCredentials()
        setIdentity(result.session, result.account.username); location.reload()
      }
      setKey(''); setCode('')
    })
  }
  async function factor(action: 'setup' | 'enable' | 'disable' | 'rotate') {
    await run(async () => {
      if (action === 'setup') { setSetup(await api<Setup>('/api/account/totp/setup', { method: 'POST', body: JSON.stringify({ key: key.trim() }) })); setKey(''); return }
      if (action === 'enable') {
        const result = await api<{ session: string; recovery_codes: string[] }>('/api/account/totp/enable', { method: 'POST', body: JSON.stringify({ challenge: setup?.challenge, code: code.trim() }) })
        setIdentity(result.session); setSaved({ title: 'Save your recovery codes', values: result.recovery_codes }); setSetup(null)
      } else {
        const result = await api<{ session?: string; key?: string }>(action === 'rotate' ? '/api/account/key' : '/api/account/totp/disable', { method: 'POST', body: JSON.stringify({ key: key.trim(), code: code.trim() }) })
        if (result.session) setIdentity(result.session)
        if (result.key) setSaved({ title: 'Save your replacement account key', values: [result.key] })
        else setNotice('Authenticator disabled; previous recovery codes were revoked')
      }
      setKey(''); setCode(''); await refresh()
    })
  }
  async function create(event: FormEvent) {
    event.preventDefault(); await run(async () => {
      const application = await api<Application>(`/api/clusters/${clusterId}/applications`, { method: 'POST', body: JSON.stringify({ name, slug: appSlug }) })
      setSelected(application); setStudioTab('workflows'); setSlug(null); setName(''); await refresh()
    })
  }
  async function savePicture(jpeg: string | null) {
    await run(async () => {
      const result = await api<{ avatar: string | null }>('/api/account/avatar', jpeg
        ? { method: 'PUT', body: JSON.stringify({ jpeg }) }
        : { method: 'DELETE' })
      setAccount(previous => previous && { ...previous, avatar: result.avatar })
      setAvatarDraft(null)
      setNotice(jpeg ? 'Profile picture saved.' : 'Default profile picture restored.')
    })
  }

  function openMode(next: typeof mode) {
    if (!next && mode === 'profile') profileSlot?.querySelector<HTMLButtonElement>('.profile-trigger')?.focus()
    setError(''); setNotice(''); setAvatarDraft(null); setMode(next)
  }

  const signOut = () => void run(async () => {
    await api('/api/account/logout', { method: 'POST' })
    clearDeviceCredentials()
    sessionStorage.removeItem('mesh.account-session')
    localStorage.removeItem('mesh.identity')
    location.assign('/')
  })
  const controls = account
    ? <button className="profile-trigger" type="button" aria-label={`Open profile for @${account.username}`} onClick={() => openMode('profile')}>
      <Avatar username={account.username} jpeg={account.avatar}/><span>@{account.username}</span><ChevronDown size={14}/>
    </button>
    : <div className="header-auth"><button className="manage-button small" onClick={() => openMode('login')}>Sign in</button><button className="manage-button small primary" onClick={() => openMode('signup')}>Create account</button></div>
  const compute = clusters.find(cluster => cluster.id === (selected?.cluster_id ?? clusterId))
  const deployment = deploymentSummary?.appId === selected?.id ? deploymentSummary : null
  const activeRuns = runtimeSummary?.appId === selected?.id ? runtimeSummary?.activeRuns : undefined
  const viewers = selected?.members.filter(item => item.role === 'viewer').length ?? 0
  const operators = selected?.members.filter(item => item.role === 'operator').length ?? 0
  const currentTab = studioTab === 'access' && selected?.role !== 'owner' ? 'workflows' : studioTab
  const tools = [
    { id: 'workflows', label: 'Deployments', icon: FileCode2 },
    { id: 'runs', label: 'Runs', icon: Play },
    { id: 'vision', label: 'Vision studio', icon: ImageIcon },
    { id: 'access', label: 'Access & settings', icon: Settings2 },
  ] as const
  const openVisionStage = (stage: VisionStage) => {
    if (!selected) return
    setVisionRequest({ appId: selected.id, stage })
    setStudioTab('vision')
  }
  const currentVision = visionSummary?.appId === selected?.id ? visionSummary : null
  return <section className="account-applications" aria-label="Account and private applications">
    {profileSlot && createPortal(controls, profileSlot)}
    {error && !mode && <p className="feedback error" role="alert">{error}</p>}
    {notice && mode !== 'profile' && <p role="status" className="feedback success">{notice}</p>}
    {workspace === 'applications' && !appRoute && !selected && <header className="workspace-intro"><h1>Applications</h1><p>Choose an application to manage its workflows, runs, and detectors.</p></header>}
    <div className="application-content" data-view={currentTab} data-app-selected={Boolean(selected)} hidden={workspace !== 'applications'}>
      <aside className="application-rail">
        {appRoute ? <a className="rail-back" href="/#applications"><ArrowLeft size={15}/>All applications</a> : <h2>Your applications</h2>}
        {appRoute && selected && <div className="selected-app-name"><Boxes size={18}/><span><b>{selected.name}</b><small><LockKeyhole size={12}/>Private application</small></span></div>}
        {clusterPicker}
        {clusterId && <div className="application-links">{applications.map(app => <button key={app.id} className="app-list-button" aria-pressed={selected?.id === app.id} onClick={() => { if (app.id === selected?.id) return; setSelected(app); setDeleteConfirmed(false); setStudioTab('workflows') }}><Boxes size={16}/><span>{app.name} · {app.role}</span></button>)}{!applications.length && <p>No applications shared with this account.</p>}</div>}
        {account?.activated && clusterId && <details open={!applications.length}><summary>Create an application</summary><form className="application-form" onSubmit={event => void create(event)}><label>Application name<input required maxLength={80} value={name} onChange={e => setName(e.target.value)}/></label><label>Application slug<input required pattern="[a-z0-9][a-z0-9-]{1,30}[a-z0-9]" value={appSlug} onChange={e => setSlug(e.target.value)} placeholder="my-app" aria-describedby="application-slug-hint"/></label><p id="application-slug-hint">Suggested from the name. You can edit this part of the project URL.</p><button className="manage-button primary" disabled={busy}>Create private application</button></form></details>}
        {selected && <nav className="studio-tabs" aria-label="Application tools">
          {tools.filter(tool => tool.id !== 'access' || selected.role === 'owner').map(({ id, label, icon: Icon }) =>
            <button key={id} type="button" className="app-list-button" aria-pressed={currentTab === id} onClick={() => setStudioTab(id)}><Icon size={16}/>{label}</button>
          )}
        </nav>}

      </aside>
      {!selected && <div className="application-empty resource-card"><Boxes size={26}/><h3>{appRoute ? 'Private application' : 'Applications'}</h3>
        <p>{appRoute ? 'Sign in with an account that has access to this application.' : !clusterId ? 'Create or join a cluster before choosing an application.' : applications.length ? 'Select an application to manage workflows, runs, or a detector.' : 'Applications appear here when you create one or receive access.'}</p>
        {!clusterId && !appRoute && <a href="/#clusters">Open clusters</a>}
      </div>}
      {selected && <article className="application-detail">
        <header className="application-overview">
          <div className="application-title">
            <h3>{selected.name}</h3>
            <span className="context-chip"><LockKeyhole size={13}/>Private</span>
            <a className="app-permalink" href={selected.url} aria-label="Open application URL" title="Open application URL"><Link2 size={16}/></a>
          </div>
          {compute && <p className="application-location">Compute cluster <a className="app-compute-link" href={`/?cluster=${encodeURIComponent(compute.id)}#clusters`}>{compute.name}</a></p>}
          <dl className="app-metrics" aria-label="Application summary">
            <div><dt><FileCode2 size={17}/>Workflow</dt><dd>{deployment ? deployment.name ?? 'None deployed' : '—'}</dd></div>
            <div><dt><Boxes size={17}/>Deployed revision</dt><dd>{deployment?.revision ?? '—'}</dd></div>
            <div><dt><Monitor size={17}/>Cluster workers</dt><dd>{compute ? `${compute.online_nodes} online / ${compute.nodes} registered` : '—'}</dd></div>
            <div><dt><Users size={17}/>Access status</dt><dd>{viewers} viewers · {operators} operators</dd></div>
          </dl>
        </header>
        <div className="application-body">
        <div className="application-primary">
        <div hidden={currentTab !== 'vision'}><VisionStudio key={`vision-${selected.id}`} app={selected.id} role={selected.role} canShare={Boolean(selected.can_share_compute)} stageRequest={visionRequest} onSummary={setVisionSummary}/></div>
        <div hidden={currentTab !== 'workflows' && currentTab !== 'runs'}>
          <ApplicationRuntime key={selected.id} appId={selected.id} role={selected.role}
            view={currentTab === 'runs' ? 'runs' : 'workflows'}
            onShowRuns={() => setStudioTab('runs')}
            onRunsChange={setRuntimeSummary} onDeploymentChange={setDeploymentSummary}
            visionOverview={<VisionOverview summary={currentVision} role={selected.role} openStage={openVisionStage}/>}/>
          <div hidden={currentTab !== 'runs'}>
            <ImageBatchRuntime key={`images-${selected.id}`} app={selected.id} canSubmit={selected.role !== 'viewer'} canShare={Boolean(selected.can_share_compute)}/>
          </div>
        </div><div hidden={currentTab !== 'access'}>
        <div className="application-settings resource-card">
        <h3>Settings &amp; access</h3>
        <p>Application members: {selected.members.map(item => `${item.username} (${item.role})`).join(', ')}</p>
        {selected.role === 'owner' && <form className="application-form" onSubmit={event => { event.preventDefault(); void run(async () => { setSelected(await api<Application>(`/api/applications/${selected.id}`, { method: 'PATCH', body: JSON.stringify({ name: editName, expected_name: selected.name }) })); setEditName(''); await refresh() }) }}><label>New application name<input required maxLength={80} value={editName} onChange={event => setEditName(event.target.value)}/></label><button className="manage-button" disabled={busy || !editName.trim()}>Save application name</button><p>The application URL stays the same.</p></form>}
        {selected.role === 'owner' && <details><summary>Manage application access</summary><form className="application-form" onSubmit={event => { event.preventDefault(); void run(async () => { setSelected(await api<Application>(`/api/applications/${selected.id}/members`, { method: 'POST', body: JSON.stringify({ username: member, role }) })); setMember(''); await refresh() }) }}><label>Member username<input required value={member} onChange={event => setMember(event.target.value)}/></label><label>Application role<select value={role} onChange={event => setRole(event.target.value as 'viewer' | 'operator')}><option value="viewer">Viewer</option><option value="operator">Operator</option></select></label><button className="manage-button" disabled={busy}>Grant application access</button></form>
          {selected.members.filter(m => m.role !== 'owner').map(m => <button key={m.username} className="manage-button small" disabled={busy} onClick={() => void run(async () => { await api(`/api/applications/${selected.id}/members/${encodeURIComponent(m.username)}`, { method: 'DELETE' }); setSelected(await api<Application>(`/api/applications/${selected.id}`)); await refresh() })}>Revoke {m.username}</button>)}
          <div><label><input type="checkbox" checked={deleteConfirmed} onChange={event => setDeleteConfirmed(event.target.checked)}/>Confirm deletion of this application record</label><button className="manage-button danger" disabled={busy || !deleteConfirmed} onClick={() => void run(async () => { await api(`/api/applications/${selected.id}`, { method: 'DELETE' }); setSelected(null); setDeleteConfirmed(false); await refresh() })}>Delete application</button></div>
        </details>}
        </div></div>
        </div>
        <aside className="application-inspector" aria-label="Application context" hidden={currentTab === 'vision'}>
          <section className="inspector-card">
            <h4><Monitor size={17}/>Compute source</h4>
            {compute && <><a href={`/?cluster=${encodeURIComponent(compute.id)}#clusters`}>{compute.name}</a>
            <p>{compute.online_nodes} online / {compute.nodes} registered</p></>}
            <div className="inspector-status"><Activity size={16}/><span>Active application runs</span><b>{activeRuns ?? '—'}</b></div>
          </section>
          {selected.role !== 'viewer' && <section className="inspector-card">
            <h4><Link2 size={17}/>Contributor link</h4>
            <p>Invite a trusted worker to contribute compute to this application.</p>
            {selected.can_share_compute && <button className="manage-button" disabled={busy} onClick={() => void run(() => shareCompute(selected.id, selected.cluster_id))}><Link2 size={15}/>Share contributor link</button>}
            <button className="manage-button" disabled={busy} onClick={() => void run(() => startBrowser(selected.cluster_id))}>Contribute from this browser</button>
          </section>}
          <section className="inspector-card">
            <h4><Users size={17}/>App access</h4>
            <dl className="access-counts">
              <div><dt>Owners</dt><dd>{selected.members.length - viewers - operators}</dd></div>
              <div><dt>Operators</dt><dd>{operators}</dd></div>
              <div><dt>Viewers</dt><dd>{viewers}</dd></div>
            </dl>
            {selected.role === 'owner' && currentTab !== 'access' && <button className="manage-button" onClick={() => setStudioTab('access')}>Manage access</button>}
          </section>
        </aside>
        </div>
      </article>}
    </div>
    {mode === 'profile' && account && <div className="dialog-shade"><section className="credentials-dialog profile-dialog" role="dialog" aria-modal="true" aria-label="Your profile">
      <button className="dialog-close" aria-label="Close profile" disabled={busy} onClick={() => openMode(null)}>×</button>
      <div className="profile-heading"><Avatar username={account.username} jpeg={avatarDraft ?? account.avatar}/><div><h2>Your profile</h2><p>@{account.username}</p></div></div>
      {error && <p className="feedback error" role="alert">{error}</p>}
      {notice && <p role="status">{notice}</p>}
      {account.activated ? <div className="application-form">
        <label>Choose profile picture<input type="file" accept="image/jpeg,image/png,image/webp" disabled={busy} onChange={event => {
          const file = event.currentTarget.files?.[0]
          event.currentTarget.value = ''
          if (file) void run(async () => { setAvatarDraft(await prepareAvatar(file)) })
        }}/></label>
        <p>Mesh uses a centered square crop. Review the preview before saving.</p>
        <div className="profile-actions">
          {avatarDraft && <button className="manage-button primary" disabled={busy} onClick={() => void savePicture(avatarDraft)}>Save profile picture</button>}
          {avatarDraft && <button className="manage-button" disabled={busy} onClick={() => setAvatarDraft(null)}>Discard preview</button>}
          {account.avatar && !avatarDraft && <button className="manage-button" disabled={busy} onClick={() => void savePicture(null)}>Restore default picture</button>}
        </div>
        <div className="profile-actions profile-security">
          <button className="manage-button" disabled={busy} onClick={() => { setKey(''); setCode(''); openMode('security') }}>Account security</button>
          <button className="manage-button" disabled={busy} onClick={signOut}>Sign out</button>
        </div>
      </div> : <div className="profile-actions">
        <button className="manage-button primary" onClick={() => { setUsername(account.username); openMode('signup') }}>Finish account setup</button>
        <button className="manage-button" disabled={busy} onClick={signOut}>Sign out</button>
      </div>}
    </section></div>}
    {mode && mode !== 'profile' && <div className="dialog-shade"><section className="credentials-dialog" role="dialog" aria-modal="true" aria-label={mode === 'security' ? 'Account security' : mode === 'signup' ? 'Create account' : 'Sign in'}><button className="dialog-close" aria-label="Close account dialog" disabled={busy} onClick={() => { setMode(null); setKey(''); setCode(''); setSetup(null); setAvatarDraft(null) }}>×</button>
      <h2>{mode === 'security' ? 'Account security' : mode === 'signup' ? 'Create account' : 'Sign in'}</h2>
      {error && <p role="alert">{error}</p>}
      {mode !== 'security' ? <form className="application-form" onSubmit={event => void submit(event)}><label>Username<input autoFocus required autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} pattern="[a-z0-9][a-z0-9-]{1,30}[a-z0-9]"/></label>{mode === 'login' && <><label>Account key<input type="password" required autoComplete="off" value={key} onChange={e => setKey(e.target.value)}/></label><label>Authenticator or recovery code<input autoComplete="one-time-code" value={code} onChange={e => setCode(e.target.value)}/></label></>}<p>{mode === 'signup' ? 'Your current cluster memberships move to this named account. Save the issued key before closing its dialog.' : 'Account keys stay in this form only. This tab receives a temporary session after sign-in.'}</p><button className="manage-button primary" disabled={busy}>{mode === 'signup' ? 'Register username' : 'Sign in to account'}</button></form> : <div className="application-form">
        {account?.recovery_session && <p>You signed in with a recovery code. Enter your saved account key to reset the lost authenticator without spending another code, then set it up again.</p>}
        <p>{account?.two_factor_enabled ? `Authenticator enabled · ${account.recovery_codes_remaining} recovery codes remaining` : 'Use any authenticator supporting standard 6-digit TOTP codes.'}</p>
        {!setup && <><label>Current account key<input type="password" autoComplete="off" value={key} onChange={e => setKey(e.target.value)}/></label>{account?.two_factor_enabled && !account.recovery_session && <label>Authenticator or recovery code<input autoComplete="one-time-code" value={code} onChange={e => setCode(e.target.value)}/></label>}
          {!account?.two_factor_enabled && <button className="manage-button" disabled={busy || !key} onClick={() => void factor('setup')}>Set up authenticator</button>}
          {account?.two_factor_enabled && <button className="manage-button" disabled={busy || !key || (!code && !account.recovery_session)} onClick={() => void factor('disable')}>Disable authenticator</button>}
          <button className="manage-button" disabled={busy || !key || Boolean(account?.two_factor_enabled && !code)} onClick={() => void factor('rotate')}>Rotate account key</button></>}
        {setup && <><p>In your authenticator choose “Enter setup key”, select time-based codes, and enter this secret. Code interval: 30 seconds.</p><code className="account-secret">{setup.secret}</code><button className="manage-button" onClick={() => void navigator.clipboard.writeText(setup.secret)}>Copy authenticator secret</button><label>Six-digit verification code<input inputMode="numeric" autoComplete="one-time-code" maxLength={6} value={code} onChange={e => setCode(e.target.value)}/></label><button className="manage-button primary" disabled={busy || code.length !== 6} onClick={() => void factor('enable')}>Enable authenticator</button></>}
      </div>}
    </section></div>}
    {saved && <div className="dialog-shade"><section className="credentials-dialog" role="dialog" aria-modal="true" aria-label={saved.title}><h2>{saved.title}</h2><p>Shown once. Save these securely before continuing. Recovery codes are each usable once together with your account key.</p><div className="account-secret">{saved.values.map(value => <div key={value}>{value}</div>)}</div><button className="manage-button" onClick={() => void navigator.clipboard.writeText(saved.values.join('\n'))}>Copy saved credentials</button><button autoFocus className="manage-button primary" disabled={busy} onClick={() => void run(async () => {
      if (saved.title === 'Save your account key') { await api('/api/account/activate', { method: 'POST' }); localStorage.removeItem('mesh.identity') }
      if (saved.title === 'Save your replacement account key') { const result = await api<{ session: string }>('/api/account/key/confirm', { method: 'POST', body: JSON.stringify({ key: saved.values[0] }) }); setIdentity(result.session) }
      setSaved(null); await refresh(); changed()
    })}>I saved these securely</button></section></div>}
  </section>
}
