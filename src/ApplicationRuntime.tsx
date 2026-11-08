import { useEffect, useRef, useState } from 'react'
import { api } from './api'
import { ApplicationDeployment } from './ApplicationDeployment'
import type { ApplicationInput, ApplicationJob } from './application-types'

type AppKey = { id: string; label: string; expires_at: number; read_only: boolean }
const samples: Record<ApplicationInput['operation'], ApplicationInput> = {
  vector_add: { operation: 'vector_add', a: [1, 2, 3], b: [4, 5, 6] },
  matmul: { operation: 'matmul', a: [1, 2, 3, 4], b: [5, 6, 7, 8], rows: 2, inner: 2, cols: 2 },
  embedding: { operation: 'embedding', model: 'minilm-l6-v2', texts: ['A cat sits on the mat.', 'A kitten rests on a rug.'], verification: 'replicated' },
}
export function ApplicationRuntime({ appId, role }: { appId: string; role: 'owner' | 'operator' | 'viewer' }) {
  const base = `/api/applications/${appId}`
  const [jobs, setJobs] = useState<ApplicationJob[]>([])
  const [keys, setKeys] = useState<AppKey[]>([])
  const [input, setInput] = useState(JSON.stringify(samples.vector_add, null, 2))
  const [label, setLabel] = useState('Developer SDK')
  const [readOnly, setReadOnly] = useState(false)
  const [issued, setIssued] = useState<string | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [metrics, setMetrics] = useState<{ completed_samples: number; result_received_p50_ms: number | null; result_received_p95_ms: number | null } | null>(null)
  const dialog = useRef<HTMLElement>(null)
  async function refresh(signal?: AbortSignal) {
    const [list, info] = await Promise.all([api<ApplicationJob[]>(`${base}/jobs`, { signal }), api<{ metrics: typeof metrics }>(`${base}/runtime`, { signal })])
    setJobs(list); setMetrics(info.metrics)
    if (role === 'owner') setKeys(await api<AppKey[]>(`${base}/keys`, { signal }))
  }
  useEffect(() => {
    const controller = new AbortController()
    let timer = 0
    const update = async () => {
      try { await refresh(controller.signal) } catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : 'Application refresh failed') }
      if (!controller.signal.aborted) timer = window.setTimeout(() => void update(), 4000)
    }
    void update()
    return () => { controller.abort(); window.clearTimeout(timer) }
  }, [appId, role])
  useEffect(() => {
    if (!issued) return
    const previous = document.activeElement as HTMLElement | null
    const buttons = () => [...(dialog.current?.querySelectorAll<HTMLButtonElement>('button') ?? [])]
    buttons()[0]?.focus()
    const trap = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { event.preventDefault(); setIssued(null) }
      if (event.key === 'Tab') {
        const items = buttons(); const index = items.indexOf(document.activeElement as HTMLButtonElement)
        if (index < 0 || event.shiftKey && index === 0 || !event.shiftKey && index === items.length - 1) { event.preventDefault(); items[event.shiftKey ? items.length - 1 : 0]?.focus() }
      }
    }
    document.addEventListener('keydown', trap)
    return () => { document.removeEventListener('keydown', trap); previous?.focus() }
  }, [issued])
  async function run(action: () => Promise<void>) {
    if (busy) return
    setBusy(true); setError('')
    try { await action(); await refresh() } catch (e) { setError(e instanceof Error ? e.message : 'Application request failed') } finally { setBusy(false) }
  }
  const example = `import { MeshClient } from '${location.origin}/sdk/mesh-client.mjs';
const mesh = new MeshClient({
  endpoint: '${location.origin}', applicationId: '${appId}',
  token: YOUR_SCOPED_API_KEY
});
const job = await mesh.embeddings(['Your text here']);
const result = await mesh.wait(job);
console.log(result.output);`
  return <section className="application-runtime" aria-label="Browser application runtime">
    <ApplicationDeployment appId={appId} role={role} onQueued={queued => {
      const ids = new Set(queued.map(job => job.id))
      setJobs(current => [...queued, ...current.filter(job => !ids.has(job.id))])
    }}/>
    <h3>Browser jobs</h3>
    <p>Run bounded numerical operations and text embeddings on connected browser workers. Workers must belong to this cluster and have owner or operator access to this application. Enable contribution on their device; opt into ML downloads for embeddings.</p>
    <p>Inputs and results are private to app members and approved workers. Worker devices can inspect their tasks; use trusted operators for sensitive data. This runtime executes jobs, rather than hosting an always-on web server.</p>
    {error && <p role="alert" className="feedback error">{error}</p>}
    {role !== 'viewer' && <form className="application-form" onSubmit={event => { event.preventDefault(); void run(async () => { const value: unknown = JSON.parse(input); await api(`${base}/jobs`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify(value) }) }) }}>
      <label>Operation template<select onChange={event => setInput(JSON.stringify(samples[event.target.value as ApplicationInput['operation']], null, 2))} defaultValue="vector_add"><option value="vector_add">Vector add</option><option value="matmul">Matrix multiply</option><option value="embedding">Text embeddings (ML)</option></select></label>
      <label>Job input JSON<textarea aria-label="Job input JSON" rows={12} maxLength={65536} value={input} onChange={event => setInput(event.target.value)}/></label>
      <p>Embeddings default to two distinct nodes agreeing. For one-device testing set <code>"verification": "single"</code>; that mode is unverified. Agreement is not a proof of honest computation.</p>
      <button className="manage-button primary" disabled={busy}>Queue application job</button>
    </form>}
    <div className="application-job-list" aria-label="Application jobs">
      {!jobs.length && <p>No jobs yet. Jobs wait until an approved contributor is connected.</p>}
      {jobs.map(job => <article className="runtime-job" key={job.id} data-testid="application-job"><strong>{job.deployment?.name ?? job.input.operation}</strong> · <span data-testid="application-job-status">{job.status}</span><div><code>{job.id}</code></div><p>{job.verification.split('_').join(' ')} · {job.replicas_received}/{job.replicas_required} results received</p>
        {job.error && <p role="alert">{job.error}</p>}
        {job.output && <><pre data-testid="application-job-output">{JSON.stringify({ shape: job.output.shape, values: job.output.values.slice(0, 16) }, null, 2)}{job.output.values.length > 16 ? '\n(first 16 values)' : ''}</pre><button className="manage-button small" onClick={() => { const url = URL.createObjectURL(new Blob([JSON.stringify(job.output)], { type: 'application/json' })); const a = document.createElement('a'); a.href = url; a.download = `mesh-${job.id}.json`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000) }}>Download result JSON</button></>}
        {job.result_received_elapsed_ms !== null && <p>Queue → result receipt: {job.result_received_elapsed_ms} ms · {job.workers.map(w => `${w.backend}: ${Math.round(w.compute_ms)} ms compute, ${Math.round(w.load_ms)} ms load`).join('; ')}</p>}
        {role !== 'viewer' && <button className="manage-button small danger" disabled={busy} onClick={() => void run(async () => { await api(`${base}/jobs/${job.id}`, { method: 'DELETE' }) })}>Delete job{job.status === 'running' ? ' and revoke lease' : ''}</button>}
      </article>)}
    </div>
    {metrics && <p>{metrics.completed_samples} completed samples · queue-to-result receipt p50 {metrics.result_received_p50_ms ?? '—'} ms / p95 {metrics.result_received_p95_ms ?? '—'} ms. These retained samples are not a latency guarantee; compute/load times are worker-reported.</p>}
    <details><summary>Developer SDK and asynchronous inference API</summary><p>POST <code>{base}/jobs</code> or <code>{base}/inference</code> returns 202 and a job ID. Poll its status URL. Use an application API key in the Authorization Bearer header; keep it on your backend, or enter it in a trusted local tool. A key embedded in a public website is public.</p><pre>{example}</pre><a href="/sdk/mesh-client.d.ts">SDK type declarations</a><p>Supported: vector_add, matmul, and minilm-l6-v2 text embeddings (384 dimensions, up to 4 texts, truncated to 128 tokens). Model weights are pinned and integrity checked. This is an asynchronous inference API; streaming chat is not implemented.</p></details>
    {role === 'owner' && <details><summary>Application API keys</summary><p>Scoped to this app, expire after 30 days. Keys are shown once and stored as hashes. Revoke them here independently of your account key.</p><form className="application-form" onSubmit={event => { event.preventDefault(); void run(async () => { const value = await api<{ key: string }>(`${base}/keys`, { method: 'POST', body: JSON.stringify({ label, read_only: readOnly }) }); setIssued(value.key) }) }}><label>API key label<input value={label} maxLength={80} required onChange={event => setLabel(event.target.value)}/></label><label><input type="checkbox" checked={readOnly} onChange={event => setReadOnly(event.target.checked)}/>Read only</label><button className="manage-button" disabled={busy}>Generate application API key</button></form>{keys.map(key => <p key={key.id}>{key.label} · {key.read_only ? 'read only' : 'submit and read'} · expires {new Date(key.expires_at * 1000).toLocaleDateString()} <button className="manage-button small danger" disabled={busy} onClick={() => void run(async () => { await api(`${base}/keys/${key.id}`, { method: 'DELETE' }) })}>Revoke {key.label}</button></p>)}</details>}
    {issued && <div className="dialog-shade"><section ref={dialog} role="dialog" aria-modal="true" aria-label="Save application API key" className="credentials-dialog"><h2>Save application API key</h2><p>Shown once. Save it securely; closing this dialog hides it permanently. You can revoke and replace it if needed.</p><code className="account-secret">{issued}</code><button className="manage-button" onClick={() => void navigator.clipboard.writeText(issued).catch(() => setError('Clipboard unavailable; select and copy the key above'))}>Copy application key</button><button className="manage-button primary" onClick={() => setIssued(null)}>I saved the application key</button></section></div>}
  </section>
}
