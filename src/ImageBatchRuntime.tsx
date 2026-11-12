import { useEffect, useRef, useState } from 'react'
import { api, getIdentity } from './api'
import { imageLabels, imageSize, sampleImage } from './image-model'
import { MeshBrand } from './WorkspaceNav'

type Progress = { id: string; total: number; completed: number; failed: number; active: number; contributors: number }
type ImageRow = {
  index: number; worker_id: string; attempts: number; backend: string; compute_ms: number; load_ms: number
  predictions: Array<{ class_id: number; score: number }>
}
type Details = Progress & { results: ImageRow[] | null }
const describe = (error: unknown) => error instanceof Error ? error.message : 'Image runtime request failed'

export function ImageContributor({ app, invite, purpose = 'Contribute to image classification' }: { app: string; invite?: string; purpose?: string }) {
  const worker = useRef<Worker | null>(null)
  const [device] = useState(() => ({ id: crypto.randomUUID(), key: crypto.randomUUID() }))
  const stopTimer = useRef(0)
  const [enabled, setEnabled] = useState(false)
  const [message, setMessage] = useState('ML contribution is off')
  const [stopping, setStopping] = useState(false)
  useEffect(() => () => {
    worker.current?.terminate()
    worker.current = null
    window.clearTimeout(stopTimer.current)
  }, [])
  function finished(status: string) {
    worker.current?.terminate()
    worker.current = null
    window.clearTimeout(stopTimer.current)
    setEnabled(false); setStopping(false); setMessage(status)
  }
  function stop() {
    if (!worker.current) { finished('ML contribution stopped'); return }
    setStopping(true)
    worker.current.postMessage('stop')
    stopTimer.current = window.setTimeout(() => finished('ML contribution stopped'), 5000)
  }
  async function start() {
    if (worker.current || enabled) return
    setEnabled(true)
    setMessage('Connecting this browser')
    try {
      const current = new Worker(new URL('./image-batch-worker.ts', import.meta.url), { type: 'module' })
      worker.current = current
      current.onmessage = ({ data }) => {
        if (worker.current !== current) return
        if (data.stopped) finished(data.message)
        else setMessage(data.message)
      }
      current.onerror = () => { if (worker.current === current) finished('ML worker could not start') }
      const identity = invite ? undefined : await getIdentity()
      if (worker.current === current) current.postMessage({ app, worker: device.id, browserKey: device.key, identity, invite })
    } catch (error) {
      if (worker.current) finished(describe(error))
      else { setEnabled(false); setMessage(describe(error)) }
    }
  }
  return <section className="runtime-job" aria-label="Image worker">
    <strong>{purpose}</strong>
    <p>This browser downloads about 15 MB of model weights plus the runtime on its first job. It uses WebGPU when available and can fall back to the CPU. Keep this tab open to contribute.</p>
    <button type="button" className={enabled ? 'manage-button' : 'manage-button primary'} disabled={stopping} onClick={() => enabled ? stop() : void start()}>{enabled ? 'Stop ML worker' : 'Start ML worker'}</button>
    <p role="status">{message}{enabled && ` · Worker ${device.id.slice(0, 8)}`}</p>
  </section>
}

export function ImageInvitePage({ token }: { token: string }) {
  const [preview, setPreview] = useState<{ application_id: string; application_name: string; cluster_name: string }>()
  const [error, setError] = useState('')
  useEffect(() => {
    const controller = new AbortController()
    void fetch('/api/ml-invite', { headers: { 'X-Mesh-ML-Invite': token }, signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error('ML contributor link is invalid, expired, or revoked')
        setPreview(await response.json())
      }).catch(error => { if (!controller.signal.aborted) setError(describe(error)) })
    return () => controller.abort()
  }, [token])
  return <div className="manage-app">
    <header className="manage-top"><MeshBrand/></header>
    <main className="manage-main contributor-invite-main">
      <section className="workspace-panel" aria-label="ML contributor invitation">
        <h1>{preview ? `Contribute to ${preview.application_name}` : 'ML contributor invitation'}</h1>
        {error && <p role="alert">{error}</p>}
        {!error && !preview && <p role="status">Checking the invite link…</p>}
        {preview && <>
          <p>Cluster {preview.cluster_name}. Start the worker to join this project’s image pool in this tab. Your browser receives only the images it is assigned.</p>
          <ImageContributor app={preview.application_id} invite={token}/>
        </>}
      </section>
    </main>
  </div>
}

async function encodeImage(source: Blob) {
  if (source.size > 12 * 1024 * 1024) throw new Error('Each selected image must be 12 MB or smaller')
  const bitmap = await createImageBitmap(source)
  try {
    const canvas = document.createElement('canvas')
    canvas.width = canvas.height = imageSize
    const context = canvas.getContext('2d', { willReadFrequently: true })
    if (!context) throw new Error('Browser image resizing is unavailable')
    const crop = Math.min(bitmap.width, bitmap.height)
    context.fillStyle = '#fff'; context.fillRect(0, 0, imageSize, imageSize)
    context.drawImage(bitmap, (bitmap.width - crop) / 2, (bitmap.height - crop) / 2, crop, crop, 0, 0, imageSize, imageSize)
    const rgba = context.getImageData(0, 0, imageSize, imageSize).data
    let rgb = ''
    for (let index = 0; index < rgba.length; index += 4) rgb += String.fromCharCode(rgba[index], rgba[index + 1], rgba[index + 2])
    return btoa(rgb)
  } finally { bitmap.close() }
}

export function ImageBatchRuntime({ app, canSubmit, canShare }: { app: string; canSubmit: boolean; canShare: boolean }) {
  const base = `/api/applications/${app}/image-batches`
  const [open, setOpen] = useState(false)
  const [runs, setRuns] = useState<Progress[]>([])
  const [selected, setSelected] = useState('')
  const [results, setResults] = useState<ImageRow[]>([])
  const [labels, setLabels] = useState<Record<string, string>>({})
  const [files, setFiles] = useState<File[]>([])
  const [link, setLink] = useState('')
  const [busy, setBusy] = useState(false)
  const [status, setStatus] = useState('')
  const [error, setError] = useState('')
  const resultRun = useRef('')
  const run = runs.find(run => run.id === selected) ?? runs[0]
  const runId = run?.id
  const complete = Boolean(run && run.completed + run.failed === run.total)
  useEffect(() => {
    if (!open) return
    const controller = new AbortController()
    let timer = 0
    async function refresh() {
      try { setRuns(await api<Progress[]>(base, { signal: controller.signal })) }
      catch (error) { if (!controller.signal.aborted) setError(describe(error)) }
      if (!controller.signal.aborted) timer = window.setTimeout(() => void refresh(), 2500)
    }
    void refresh()
    return () => { controller.abort(); window.clearTimeout(timer) }
  }, [base, open])
  useEffect(() => {
    if (!open || !runId || !complete || resultRun.current === runId) return
    const controller = new AbortController()
    void api<Details>(`${base}/${runId}?results=true`, { signal: controller.signal })
      .then(value => {
        if (controller.signal.aborted) return
        resultRun.current = runId; setResults(value.results ?? [])
      }).catch(error => { if (!controller.signal.aborted) setError(describe(error)) })
    void imageLabels().then(setLabels).catch(() => undefined)
    return () => controller.abort()
  }, [base, open, complete, runId])
  async function perform(action: () => Promise<void>) {
    if (busy) return
    setBusy(true); setError(''); setStatus('')
    try { await action() } catch (error) { setError(describe(error)); setStatus('') } finally { setBusy(false) }
  }
  async function submit(images: string[]) {
    setStatus('Uploading resized images…')
    const created = await api<{ id: string }>(base, { method: 'POST', body: JSON.stringify({ images }), signal: AbortSignal.timeout(70_000) })
    setSelected(created.id)
    setRuns(current => [{ id: created.id, total: images.length, completed: 0, failed: 0, active: 0, contributors: 0 }, ...current.filter(item => item.id !== created.id)])
    setStatus(`Queued ${images.length} image${images.length === 1 ? '' : 's'}`)
  }
  async function sample(count: number) {
    setStatus('Loading the sample image…')
    const image = await encodeImage(await sampleImage())
    await submit(Array<string>(count).fill(image))
  }
  async function upload() {
    if (!files.length || files.length > 1024) throw new Error('Choose between 1 and 1,024 images')
    const images: string[] = []
    for (const file of files) {
      setStatus(`Preparing image ${images.length + 1} of ${files.length}…`)
      images.push(await encodeImage(file))
    }
    await submit(images)
  }
  async function share() {
    const response = await api<{ token: string }>(`/api/applications/${app}/contributor-invites?ml_only=true`, { method: 'POST' })
    const url = `${location.origin}/#ml-invite=${response.token}`
    setLink(url)
    try { await navigator.clipboard.writeText(url); setStatus('ML contributor link copied') }
    catch { setStatus('Select and copy the ML contributor link below') }
  }
  async function download() {
    if (!run) return
    const rows = resultRun.current === run.id ? results : (await api<Details>(`${base}/${run.id}?results=true`)).results ?? []
    const output = rows.map(row => ({ ...row, predictions: row.predictions.map(value => ({ ...value, label: labels[value.class_id] })) }))
    const url = URL.createObjectURL(new Blob([JSON.stringify(output)], { type: 'application/json' }))
    const anchor = document.createElement('a')
    anchor.href = url; anchor.download = `mesh-images-${run.id}.json`; anchor.click()
    window.setTimeout(() => URL.revokeObjectURL(url), 1000)
  }
  const displayedRows = resultRun.current === runId ? results : []
  return <details onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>Image classification (ML)</summary>
    <section aria-label="Image classification runtime">
      <h3>MobileNetV4 image classification</h3>
      <p>Classify up to 1,024 images in a batch. Each image is assigned separately. Results are model predictions reported by one browser per image. The Space holds batch data in memory for up to 30 minutes.</p>
      {error && <p role="alert" className="feedback error">{error}</p>}
      {status && <p role="status">{status}</p>}
      {canSubmit && <>
        <ImageContributor app={app}/>
        {canShare && <div className="application-form">
          <button className="manage-button" disabled={busy} onClick={() => void perform(share)}>Share ML contributor link</button>
          <p>Friends can open this link and start an ML worker. Revoke the link under Active invites on the cluster dashboard. Share it with people you trust to process the assigned images.</p>
          {link && <label>ML contributor link<input readOnly value={link} onFocus={event => event.currentTarget.select()}/></label>}
        </div>}
        <div className="application-form">
          <div className="heading-buttons">
            <button className="manage-button" disabled={busy} onClick={() => void perform(() => sample(1))}>Try sample image</button>
            <button className="manage-button" disabled={busy} onClick={() => void perform(() => sample(32))}>Try 32-image batch</button>
          </div>
          <label>Choose images<input type="file" accept="image/jpeg,image/png,image/webp" multiple onChange={event => setFiles(Array.from(event.target.files ?? []))}/></label>
          <button className="manage-button primary" disabled={busy || !files.length} onClick={() => void perform(upload)}>Classify {files.length || ''} selected images</button>
        </div>
      </>}
      {runs.length > 1 && <label className="application-form">Image run<select value={runId} onChange={event => setSelected(event.target.value)}>{runs.map(item =>
        <option key={item.id} value={item.id}>{item.total} images · {item.completed} completed · {item.id.slice(0, 8)}</option>
      )}</select></label>}
      {run && <div className="runtime-job">
        <strong>Image batch</strong>
        <p role="status" data-testid="image-batch-progress">{run.completed}/{run.total} classified · {run.active} active · {run.contributors} contributing {run.contributors === 1 ? 'browser' : 'browsers'}{run.failed ? ` · ${run.failed} failed` : ''}</p>
        {run.active === 0 && !complete && <p>Waiting for ML workers. Start this browser or share the ML contributor link.</p>}
        {displayedRows.length > 0 && <ol>
          {displayedRows.slice(0, 8).map(row => <li key={row.index}>
            <strong>Image {row.index + 1}</strong> · {labels[row.predictions[0].class_id] ?? `Class ${row.predictions[0].class_id}`} · {Math.round(row.predictions[0].score * 100)}%
            <small> · Worker {row.worker_id.slice(0, 8)} · {row.backend.toUpperCase()} · {row.attempts} {row.attempts === 1 ? 'attempt' : 'attempts'}</small>
          </li>)}
        </ol>}
        {complete && <button className="manage-button" disabled={busy} onClick={() => void perform(download)}>Download all predictions</button>}
        {canSubmit && <button className="manage-button danger" disabled={busy} onClick={() => void perform(async () => {
          await api(`${base}/${run.id}`, { method: 'DELETE' })
          setRuns(current => current.filter(item => item.id !== run.id))
        })}>Delete image batch</button>}
      </div>}
    </section>
  </details>
}
