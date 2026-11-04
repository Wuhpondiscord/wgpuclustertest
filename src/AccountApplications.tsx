import { useEffect, useState, type FormEvent } from 'react'
import { api, setIdentity } from './api'

type Account = { username: string; activated: boolean; two_factor_enabled: boolean; recovery_codes_remaining: number }
type Application = { id: string; cluster_id: string; name: string; slug: string; owner: string; url: string; role: 'owner' | 'operator' | 'viewer'; members: Array<{ username: string; role: string }>; runtime_status: string }
type Setup = { secret: string; challenge: string; uri: string }
const clearDeviceCredentials = () => {
  for (const key of ['mesh.node-tokens', 'mesh.remembered-node-tokens', 'mesh.contribution-settings']) { sessionStorage.removeItem(key); localStorage.removeItem(key) }
}
export function AccountApplications({ clusterId, changed }: { clusterId?: string; changed: () => void }) {
  const [account, setAccount] = useState<Account | null>(null)
  const [applications, setApplications] = useState<Application[]>([])
  const [selected, setSelected] = useState<Application | null>(null)
  const [mode, setMode] = useState<'signup' | 'login' | 'security' | null>(null)
  const [username, setUsername] = useState('')
  const [key, setKey] = useState('')
  const [code, setCode] = useState('')
  const [slug, setSlug] = useState('')
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
  async function refresh() {
    const current = await api<Account | null>('/api/account'); setAccount(current)
    if (clusterId) setApplications(await api<Application[]>(`/api/clusters/${clusterId}/applications`))
    if (appRoute) setSelected(await api<Application>(`/api/applications/by-name/${encodeURIComponent(route[0])}/${encodeURIComponent(route[1])}`))
  }
  useEffect(() => { let active = true; void refresh().catch(e => { if (active) setError(e.message) }); return () => { active = false } }, [clusterId])
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
      if (event.key === 'Escape' && !saved && !busy) { setMode(null); setKey(''); setCode(''); setSetup(null) }
    }
    document.addEventListener('keydown', close); return () => document.removeEventListener('keydown', close)
  }, [mode, saved, busy])
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
      const application = await api<Application>(`/api/clusters/${clusterId}/applications`, { method: 'POST', body: JSON.stringify({ name, slug }) })
      setSelected(application); setSlug(''); setName(''); await refresh()
    })
  }
  return <section className="account-applications resource-card" aria-label="Account and private applications">
    <div className="card-heading"><div><h3>{account ? `@${account.username}` : 'Account'}</h3><p>{account ? `Authenticator ${account.two_factor_enabled ? 'enabled' : 'not enabled'}` : 'Choose a username and save your server-generated account key.'}</p></div><div className="heading-buttons">
      {account ? <>{!account.activated && <button className="manage-button small primary" onClick={() => { setUsername(account.username); setMode('signup') }}>Finish account setup</button>}<button className="manage-button small" disabled={!account.activated} onClick={() => { setMode('security'); setKey(''); setCode('') }}>Account security</button><button className="manage-button small" onClick={() => void run(async () => { await api('/api/account/logout', { method: 'POST' }); clearDeviceCredentials(); sessionStorage.removeItem('mesh.account-session'); localStorage.removeItem('mesh.identity'); location.assign('/') })}>Sign out</button></> : <><button className="manage-button small" onClick={() => setMode('login')}>Sign in</button><button className="manage-button small primary" onClick={() => setMode('signup')}>Create account</button></>}
    </div></div>
    {error && <p className="feedback error" role="alert">{error}</p>}{notice && <p role="status">{notice}</p>}
    {(clusterId || appRoute) && <div className="application-content"><h3>Private applications</h3><p>Cluster membership does not grant application access. Share access by username; keep your account key private.</p>
      {clusterId && <div className="application-links">{applications.map(app => <button key={app.id} className="manage-button" onClick={() => { setSelected(app); setDeleteConfirmed(false) }}>{app.name} · {app.role}</button>)}{!applications.length && <p>No applications shared with this account.</p>}</div>}
      {account?.activated && clusterId && <form className="application-form" onSubmit={event => void create(event)}><label>Application name<input required maxLength={80} value={name} onChange={e => setName(e.target.value)}/></label><label>Application slug<input required pattern="[a-z0-9][a-z0-9-]{1,30}[a-z0-9]" value={slug} onChange={e => setSlug(e.target.value)} placeholder="my-app"/></label><button className="manage-button primary" disabled={busy}>Create private application</button></form>}
      {selected && <article className="application-detail"><h3>{selected.name}</h3><a href={selected.url}>{location.origin}{selected.url}</a><p>Private management page · Runtime not deployed</p><p>Application members: {selected.members.map(m => `${m.username} (${m.role})`).join(', ')}</p>
        {selected.role === 'owner' && <><form className="application-form" onSubmit={event => { event.preventDefault(); void run(async () => { setSelected(await api<Application>(`/api/applications/${selected.id}/members`, { method: 'POST', body: JSON.stringify({ username: member, role }) })); setMember(''); await refresh() }) }}><label>Member username<input required value={member} onChange={event => setMember(event.target.value)}/></label><label>Application role<select value={role} onChange={event => setRole(event.target.value as 'viewer' | 'operator')}><option value="viewer">Viewer</option><option value="operator">Operator</option></select></label><button className="manage-button" disabled={busy}>Grant application access</button></form>
          {selected.members.filter(m => m.role !== 'owner').map(m => <button key={m.username} className="manage-button small" disabled={busy} onClick={() => void run(async () => { await api(`/api/applications/${selected.id}/members/${encodeURIComponent(m.username)}`, { method: 'DELETE' }); setSelected(await api<Application>(`/api/applications/${selected.id}`)); await refresh() })}>Revoke {m.username}</button>)}
          <div><label><input type="checkbox" checked={deleteConfirmed} onChange={event => setDeleteConfirmed(event.target.checked)}/>Confirm deletion of this application record</label><button className="manage-button danger" disabled={busy || !deleteConfirmed} onClick={() => void run(async () => { await api(`/api/applications/${selected.id}`, { method: 'DELETE' }); setSelected(null); setDeleteConfirmed(false); await refresh() })}>Delete application</button></div>
        </>}
      </article>}
    </div>}
    {mode && <div className="dialog-shade"><section className="credentials-dialog" role="dialog" aria-modal="true" aria-label={mode === 'security' ? 'Account security' : mode === 'signup' ? 'Create account' : 'Sign in'}><button className="dialog-close" aria-label="Close account dialog" disabled={busy} onClick={() => { setMode(null); setKey(''); setCode(''); setSetup(null) }}>×</button>
      <h2>{mode === 'security' ? 'Account security' : mode === 'signup' ? 'Create account' : 'Sign in'}</h2>
      {error && <p role="alert">{error}</p>}
      {mode !== 'security' ? <form className="application-form" onSubmit={event => void submit(event)}><label>Username<input autoFocus required autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} pattern="[a-z0-9][a-z0-9-]{1,30}[a-z0-9]"/></label>{mode === 'login' && <><label>Account key<input type="password" required autoComplete="off" value={key} onChange={e => setKey(e.target.value)}/></label><label>Authenticator or recovery code<input autoComplete="one-time-code" value={code} onChange={e => setCode(e.target.value)}/></label></>}<p>{mode === 'signup' ? 'Your current cluster memberships move to this named account. Save the issued key before closing its dialog.' : 'Account keys stay in this form only. This tab receives a temporary session after sign-in.'}</p><button className="manage-button primary" disabled={busy}>{mode === 'signup' ? 'Register username' : 'Sign in to account'}</button></form> : <div className="application-form">
        <p>{account?.two_factor_enabled ? `Authenticator enabled · ${account.recovery_codes_remaining} recovery codes remaining` : 'Use any authenticator supporting standard 6-digit TOTP codes.'}</p>
        {!setup && <><label>Current account key<input type="password" autoComplete="off" value={key} onChange={e => setKey(e.target.value)}/></label>{account?.two_factor_enabled && <label>Authenticator or recovery code<input autoComplete="one-time-code" value={code} onChange={e => setCode(e.target.value)}/></label>}
          {!account?.two_factor_enabled && <button className="manage-button" disabled={busy || !key} onClick={() => void factor('setup')}>Set up authenticator</button>}
          {account?.two_factor_enabled && <button className="manage-button" disabled={busy || !key || !code} onClick={() => void factor('disable')}>Disable authenticator</button>}
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
