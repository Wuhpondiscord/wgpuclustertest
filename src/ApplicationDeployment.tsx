import { useEffect, useRef, useState } from 'react'
import { api, ApiError } from './api'
import type { ApplicationInput, ApplicationJob } from './application-types'
import template from '../mesh.json'

const MANIFEST_LIMIT = 32 * 1024
const COMMIT_SHA = /^[0-9a-f]{40}$/i
type Source = { kind: 'upload' } | { kind: 'github'; repository: string; commit: string }
type Deployment = {
  id: string
  deployed_at_ms: number
  source: Source
  manifest: { version: 1; name: string; jobs: Array<{ name: string; input: ApplicationInput }> }
}
type Candidate = { manifest: unknown; source: Source }

function repositoryPath(value: string) {
  const url = new URL(value.trim())
  const match = url.pathname.match(/^\/([a-zA-Z0-9-]{1,39})\/([a-zA-Z0-9._-]{1,100})\/?$/)
  if (url.origin !== 'https://github.com' || url.username || url.password || url.search || url.hash || !match) {
    throw new Error('Enter a public GitHub repository URL, such as https://github.com/owner/repo')
  }
  const repo = match[2].replace(/\.git$/, '')
  if (!repo || repo === '.' || repo === '..') throw new Error('Enter a GitHub repository name')
  return `${match[1]}/${repo}`
}

async function readLimited(response: Response, limit: number) {
  const reader = response.body?.getReader()
  if (!reader) throw new Error('GitHub returned an empty response')
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let bytes = 0
  let text = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return text + decoder.decode()
      bytes += value.byteLength
      if (bytes > limit) throw new Error('GitHub response exceeds the import size limit')
      text += decoder.decode(value, { stream: true })
    }
  } finally {
    await reader.cancel().catch(() => undefined)
    reader.releaseLock()
  }
}

async function importFromGithub(value: string, ref: string): Promise<Candidate> {
  const repository = repositoryPath(value)
  const init = {
    credentials: 'omit', referrerPolicy: 'no-referrer', redirect: 'error',
    signal: AbortSignal.timeout(15_000),
  } as const
  let commit = ref.trim()
  if (!COMMIT_SHA.test(commit)) {
    const url = new URL(`https://api.github.com/repos/${repository}/commits`)
    url.searchParams.set('per_page', '1')
    if (commit) url.searchParams.set('sha', commit)
    const response = await fetch(url, { ...init, cache: 'no-store' })
    if (!response.ok) {
      throw new Error(`GitHub could not resolve that repository or ref (HTTP ${response.status}). Use a public repo or upload mesh.json directly.`)
    }
    const result = JSON.parse(await readLimited(response, 256 * 1024))
    commit = result?.[0]?.sha
    if (typeof commit !== 'string' || !COMMIT_SHA.test(commit)) throw new Error('GitHub returned no commit for that ref')
  }
  commit = commit.toLowerCase()
  const response = await fetch(`https://raw.githubusercontent.com/${repository}/${commit}/mesh.json`, init)
  if (!response.ok) throw new Error(`Could not read mesh.json at that commit (HTTP ${response.status}). Put the file at the root of the public repository.`)
  const manifest: unknown = JSON.parse(await readLimited(response, MANIFEST_LIMIT))
  return { manifest, source: { kind: 'github', repository, commit } }
}

export function ApplicationDeployment({ appId, role, onQueued, onDeploymentChange }: {
  appId: string
  role: 'owner' | 'operator' | 'viewer'
  onQueued: (jobs: ApplicationJob[]) => void
  onDeploymentChange: (summary: { appId: string; name?: string; revision?: string }) => void
}) {
  const path = `/api/applications/${appId}/deployment`
  const [active, setActive] = useState<Deployment | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [candidate, setCandidate] = useState<Candidate | null>(null)
  const [editor, setEditor] = useState<string | null>(null)
  const [editRevision, setEditRevision] = useState<string | null>(null)
  const [repo, setRepo] = useState('')
  const [ref, setRef] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const pendingRun = useRef<{ revision: string; key: string } | null>(null)

  async function refresh(signal?: AbortSignal) {
    setActive(await api<Deployment | null>(path, { signal, cache: 'no-store' }))
    setLoaded(true)
  }

  useEffect(() => {
    const controller = new AbortController()
    void refresh(controller.signal).catch(error => {
      if (!controller.signal.aborted) setError(error.message)
    })
    return () => controller.abort()
  }, [path])

  useEffect(() => {
    if (loaded) onDeploymentChange({ appId, name: active?.manifest.name, revision: active?.id.slice(0, 8) })
  }, [active, appId, loaded, onDeploymentChange])

  async function perform(action: () => Promise<void>) {
    if (busy) return
    setBusy(true)
    setError('')
    setMessage('')
    try {
      await action()
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Deployment request failed')
      if (error instanceof ApiError && error.status === 409) await refresh().catch(() => undefined)
    } finally {
      setBusy(false)
    }
  }

  async function previewFile(file: File) {
    setCandidate(null)
    setEditor(null)
    if (file.size > MANIFEST_LIMIT) throw new Error('Upload a mesh.json file up to 32 KiB')
    const manifest: unknown = JSON.parse(await file.text())
    setCandidate({ manifest, source: { kind: 'upload' } })
  }

  async function deploy() {
    if (!candidate) return
    const deployment = await api<Deployment>(path, {
      method: 'POST',
      body: JSON.stringify({ ...candidate, expected_revision_id: editor !== null ? editRevision : active?.id ?? null }),
    })
    setActive(deployment)
    setCandidate(null)
    setEditor(null)
    setMessage('Deployment saved. Run it when your workers are ready.')
  }

  async function run(deployment: Deployment) {
    if (pendingRun.current?.revision !== deployment.id) {
      pendingRun.current = { revision: deployment.id, key: crypto.randomUUID() }
    }
    const jobs = await api<ApplicationJob[]>(`${path}/run`, {
      method: 'POST',
      headers: { 'Idempotency-Key': pendingRun.current.key },
      body: JSON.stringify({ revision_id: deployment.id }),
    })
    pendingRun.current = null
    onQueued(jobs)
    setMessage(`Queued ${jobs.length} workflow job${jobs.length === 1 ? '' : 's'}.`)
  }

  return <section className="deployment-panel workspace-panel" aria-label="Application deployment">
    <div className="workspace-panel-heading"><div><h3>Workflow deployment</h3><p>Preview a workflow definition, publish a revision, and run it on your cluster.</p></div></div>
    {error && <p role="alert" className="feedback error">{error}</p>}
    {message && <p role="status">{message}</p>}
    {loaded && !active && <div className="deployment-empty">No workflow deployed.</div>}
    {active && <article className="runtime-job deployed-workflow">
      <div className="application-title"><strong>{active.manifest.name}</strong><span className="published-chip">Deployed</span></div>
      <p>{active.manifest.jobs.length} jobs · {active.source.kind === 'github' ? `${active.source.repository} @ ${active.source.commit.slice(0, 12)}` : 'Local manifest'}</p>
      <p>Revision <code>{active.id.slice(0, 8)}</code> · Saved {new Date(active.deployed_at_ms).toLocaleString()}</p><details><summary>View deployed manifest</summary><pre>{JSON.stringify(active.manifest, null, 2)}</pre></details>
      <ul>{active.manifest.jobs.map(job => <li key={job.name}>{job.name} · {job.input.operation}</li>)}</ul>
      <a download="mesh.json" href={`data:application/json;charset=utf-8,${encodeURIComponent(JSON.stringify(active.manifest, null, 2))}`}>Download deployed manifest</a>
      {role !== 'viewer' && <button className="manage-button primary" type="button" disabled={busy} onClick={() => void perform(() => run(active))}>Run deployment</button>}
      {role === 'owner' && <button className="manage-button" disabled={busy} onClick={() => { setEditRevision(active.id); setEditor(JSON.stringify(active.manifest, null, 2)); setCandidate(null) }}>Edit deployed workflow</button>}
      {role === 'owner' && <button className="manage-button danger" type="button" disabled={busy} onClick={() => void perform(async () => {
        await api(`${path}/${active.id}`, { method: 'DELETE' })
        setActive(null)
        setMessage('Deployment removed.')
      })}>Remove deployment</button>}
    </article>}
    {role === 'owner' && editor !== null && <section className="manifest-editor" aria-label="Workflow editor">
      <h4>Edit workflow</h4><p>Changes stay in this draft until you preview and save. Running jobs keep their original revision.</p>
      <label>Workflow JSON<textarea spellCheck={false} value={editor} onChange={event => { setEditor(event.target.value); setCandidate(null) }}/></label>
      <button className="manage-button primary" disabled={busy} onClick={() => void perform(async () => {
        if (new TextEncoder().encode(editor).length > MANIFEST_LIMIT) throw new Error('Workflow exceeds 32 KiB')
        setCandidate({ manifest: JSON.parse(editor), source: { kind: 'upload' } })
      })}>Preview changes</button>
      <button className="manage-button" disabled={busy} onClick={() => { setEditor(null); setCandidate(null) }}>Cancel editing</button>
    </section>}
    {role === 'owner' && <details open={!active || !!candidate}>
      <summary>{active ? 'Replace deployment' : 'Upload or import a workflow'}</summary>
    {role === 'owner' && <div className="application-form">
      <button className="manage-button" type="button" disabled={busy || !loaded} onClick={() => {
        setError('')
        setMessage('')
        setEditor(null)
        setCandidate({ manifest: template, source: { kind: 'upload' } })
      }}>Load sample workflow</button>
      <p>One contributing node is enough to run the sample. Expect <code>[4, 6]</code> from vector addition and <code>[19, 22, 43, 50]</code> from matrix multiplication.</p>
    </div>}
      <div className="application-form">
        <a download="mesh.json" href={`data:application/json;charset=utf-8,${encodeURIComponent(JSON.stringify(template, null, 2))}`}>Download example mesh.json</a>
        <label>Upload mesh.json<input type="file" accept=".json,application/json" disabled={busy || !loaded} onChange={event => {
          const file = event.currentTarget.files?.[0]
          event.currentTarget.value = ''
          if (file) void perform(() => previewFile(file))
        }}/></label>
      </div>
      <form className="application-form" onSubmit={event => {
        event.preventDefault()
        void perform(async () => {
          setCandidate(null)
          setEditor(null)
          setCandidate(await importFromGithub(repo, ref))
        })
      }}>
        <label>Public GitHub repository<input type="url" value={repo} onChange={event => setRepo(event.target.value)} placeholder="https://github.com/owner/repo" required maxLength={200}/></label>
        <label>Branch or commit<input value={ref} onChange={event => setRef(event.target.value)} placeholder="Default branch" maxLength={255}/></label>
        <p>Place mesh.json at the repository root. For a private repository, upload its manifest file.</p>
        <button type="submit" className="manage-button" disabled={busy || !loaded}>Preview GitHub workflow</button>
      </form>
      {candidate && <div className="runtime-job">
        <div className="application-title"><strong>Workflow preview</strong><span className="context-chip">Draft</span></div>
        {candidate.source.kind === 'github' && <p>{candidate.source.repository} @ <code>{candidate.source.commit}</code></p>}
        <pre>{JSON.stringify(candidate.manifest, null, 2)}</pre>
        <button type="button" className="manage-button primary" disabled={busy} onClick={() => void perform(deploy)}>{active ? 'Replace deployment' : 'Save deployment'}</button>
        <button type="button" className="manage-button" disabled={busy} onClick={() => setCandidate(null)}>Discard preview</button>
      </div>}
    </details>}
    {!loaded && error && <button className="manage-button" type="button" disabled={busy} onClick={() => void perform(() => refresh())}>Reload deployment</button>}
  </section>
}
