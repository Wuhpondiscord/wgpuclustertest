import { useEffect, useMemo, useRef, useState } from 'react'
import { ArrowRight, Check, CloudUpload, Download, ImageUp, Play } from 'lucide-react'
import { api, ApiError } from './api'
import { ConfirmAction } from './ConfirmAction'
import { classColor, VisionCanvas } from './VisionCanvas'
import { VisionDatasetEditor } from './VisionDatasetEditor'
import { ImageContributor } from './ImageBatchRuntime'
import { backgroundLabel, detect, evaluateDetections, fitDetector, regionWindows, validateDetector, type Box, type Detection, type Detector } from './vision-detector'
import { cropRegion, downloadJson, readVisionImage, sampleDataset, validateDataset, type VisionImage } from './vision-data'

type Saved = { id: string; name: string; saved_at_ms: number; model: Detector }
type FeatureRow = { index: number; features: number[]; worker_id: string }
type Validation = { images: VisionImage[]; regions: Array<Array<Box & { features: number[] }>> }

type VisionStage = 'dataset' | 'train' | 'deploy'
export type VisionSummary = { appId: string; detector: { name: string; id: string } | null; datasetDirty: boolean }
const stages = [
  { id: 'dataset', label: 'Label images' },
  { id: 'train', label: 'Train & evaluate' },
  { id: 'deploy', label: 'Deploy & test' },
] as const

export function VisionStudio({ app, appName, role, canShare, compute, onSummary }: {
  app: string
  appName: string
  role: 'owner' | 'operator' | 'viewer'
  canShare: boolean
  compute?: { name: string; url: string }
  onSummary: (summary: VisionSummary) => void
}) {
  const endpoint = `/api/applications/${app}/detector`
  const [step, setStep] = useState<VisionStage>('dataset')
  const [saved, setSaved] = useState<Saved | null>(null)
  const [confirmation, setConfirmation] = useState<'publish' | 'remove' | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [draft, setDraft] = useState<Detector | null>(null)
  const [name, setName] = useState('Custom object detector')
  const [images, setImages] = useState<VisionImage[]>([])
  const [datasetDirty, setDatasetDirty] = useState(false)
  const [pendingPublication, setPendingPublication] = useState(false)
  const [validation, setValidation] = useState<Validation | null>(null)
  const [testImage, setTestImage] = useState<VisionImage | null>(null)
  const [prediction, setPrediction] = useState<Detection[] | null>(null)
  const [link, setLink] = useState('')
  const [busy, setBusy] = useState(false)
  const [status, setStatus] = useState('')
  const [error, setError] = useState('')
  const [workers, setWorkers] = useState(0)
  const [thisWorkerEnabled, setThisWorkerEnabled] = useState(false)
  const action = useRef<AbortController | null>(null)
  const revision = useRef<string | null>(null)
  useEffect(() => {
    const controller = new AbortController()
    void api<Saved | null>(endpoint, { signal: controller.signal }).then(value => {
      setSaved(value)
      revision.current = value?.id ?? null
      setLoaded(true)
    }).catch(error => { if (!controller.signal.aborted) setError(error.message) })
    return () => { controller.abort(); action.current?.abort() }
  }, [endpoint])
  useEffect(() => {
    if (!datasetDirty) return
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = '' }
    window.addEventListener('beforeunload', warn)
    return () => window.removeEventListener('beforeunload', warn)
  }, [datasetDirty])

  async function perform(operation: (signal: AbortSignal) => Promise<void>) {
    if (action.current) return
    const controller = new AbortController(); action.current = controller
    setBusy(true); setError(''); setStatus('')
    try { await operation(controller.signal) }
    catch (error) { setError(controller.signal.aborted ? 'Operation cancelled.' : error instanceof Error ? error.message : 'Vision request failed') }
    finally { action.current = null; setBusy(false) }
  }
  function changeDataset(next: VisionImage[], dirty = true) {
    try {
      setImages(next.length ? validateDataset({ version: 1, images: next }) : [])
      setDatasetDirty(dirty && next.length > 0)
      setDraft(null)
      setPendingPublication(false)
      setValidation(null)
      setStatus('')
      setError('')
      return true
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Invalid dataset')
      return false
    }
  }
  function exportDataset() {
    downloadJson('mesh-vision-dataset.json', { version: 1, images })
    setDatasetDirty(false)
    setStatus('Dataset exported.')
  }
  async function addImages(files: File[]) {
    await perform(async signal => {
      if (images.length + files.length > 32) throw new Error('Use up to 32 images')
      const additions: VisionImage[] = []
      for (const file of files) {
        signal.throwIfAborted()
        additions.push(await readVisionImage(file, file.name))
      }
      signal.throwIfAborted()
      changeDataset([...images, ...additions])
    })
  }
  async function importDataset(file: File) {
    await perform(async signal => {
      if (file.size > 8 * 1024 * 1024) throw new Error('Dataset exceeds 8 MiB')
      const content = JSON.parse(await file.text())
      signal.throwIfAborted()
      changeDataset(validateDataset(content), false)
    })
  }
  async function features(regions: Array<{ image: VisionImage; box: Box }>, signal: AbortSignal) {
    if (!regions.length || regions.length > 1024) throw new Error('Use between 1 and 1,024 regions per run')
    const payload: string[] = []
    for (const { image, box } of regions) { signal.throwIfAborted(); payload.push(await cropRegion(image, box)) }
    const base = `/api/applications/${app}/image-batches`
    const run = await api<{ id: string }>(`${base}?features=true`, { method: 'POST', body: JSON.stringify({ images: payload }), signal })
    try {
      for (;;) {
        signal.throwIfAborted()
        const progress = await api<{ completed: number; failed: number; total: number; contributors: number }>(`${base}/${run.id}`, { signal })
        setStatus(`Processing regions on the cluster · ${progress.completed}/${progress.total} · ${progress.contributors} contributing browsers. Start an ML worker if none are connected.`)
        if (progress.failed) throw new Error('A region exhausted its retries. Start a working ML contributor and try again.')
        if (progress.completed === progress.total) {
          const result = await api<{ results: FeatureRow[] }>(`${base}/${run.id}?results=true`, { signal })
          const rows = result.results.sort((a, b) => a.index - b.index)
          if (rows.length !== regions.length || rows.some((row, index) => row.index !== index || !Array.isArray(row.features))) throw new Error('Incomplete feature results')
          setWorkers(progress.contributors)
          return rows
        }
        await new Promise<void>((resolve, reject) => {
          const abort = () => { clearTimeout(timer); reject(signal.reason) }
          const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, 1500)
          signal.addEventListener('abort', abort, { once: true })
          if (signal.aborted) abort()
        })
      }
    } finally { await api(`${base}/${run.id}`, { method: 'DELETE', signal: AbortSignal.timeout(5000) }).catch(() => undefined) }
  }
  async function train(signal: AbortSignal) {
    validateDataset({ version: 1, images })
    const training = images.filter(image => image.split === 'train')
    const heldout = images.filter(image => image.split === 'validation')
    if (!heldout.length || heldout.some(image => !image.boxes.some(box => box.label !== backgroundLabel))) throw new Error('Mark at least one annotated image as validation. It will not be used to fit the detector.')
    if (heldout.some(image => training.some(train => train.data === image.data))) throw new Error('Training and validation must use different images')
    const examples = training.flatMap(image => image.boxes.map(box => ({ image, box })))
    const labels = [...new Set(examples.map(example => example.box.label))]
    if (!labels.includes(backgroundLabel) || labels.length < 2 || labels.length > 5 || labels.some(label => examples.filter(example => example.box.label === label).length < 2)) throw new Error('Draw at least two training boxes per object class and two background boxes. Use up to four object classes.')
    if (heldout.some(image => image.boxes.some(box => box.label !== backgroundLabel && !labels.includes(box.label)))) throw new Error('Validation labels must also have training examples')
    const windows = regionWindows()
    const regions = [...examples, ...heldout.flatMap(image => windows.map(box => ({ image, box })))]
    setStatus('Preparing training crops and validation windows…')
    const rows = await features(regions, signal)
    signal.throwIfAborted()
    const model = fitDetector(examples.map((example, index) => ({ label: example.box.label, features: rows[index].features })))
    setDraft(model); setPendingPublication(true); revision.current = saved?.id ?? null
    setValidation({ images: heldout, regions: heldout.map((_, index) => windows.map((box, offset) => ({ ...box, features: rows[examples.length + index * windows.length + offset].features }))) })
    setStatus('Training complete. Review the held-out results.')
  }
  const metrics = useMemo(() => draft && validation
    ? evaluateDetections(validation.regions.map(regions => detect(draft, regions)), validation.images.map(image => image.boxes))
    : null, [draft, validation])
  const classes = useMemo(() => [...new Set(images.flatMap(image => image.boxes.map(box => box.label)))]
    .filter(label => label !== backgroundLabel).sort(), [images])
  const heldOut = images.filter(image => image.split === 'validation').length
  const reviewingDraft = role === 'owner' && pendingPublication ? draft : null
  const deployStage = role === 'viewer' || step === 'deploy'

  useEffect(() => {
    if (loaded || datasetDirty) onSummary({ appId: app, detector: saved && { name: saved.name, id: saved.id }, datasetDirty })
  }, [app, datasetDirty, loaded, saved, onSummary])

  async function reloadDetector(signal: AbortSignal) {
    const deployment = await api<Saved | null>(endpoint, { signal })
    setSaved(deployment)
    setLoaded(true)
    setPrediction(null)
    if (!pendingPublication) revision.current = deployment?.id ?? null
  }
  async function publishDetector(signal: AbortSignal) {
    if (!reviewingDraft) return
    try {
      const deployment = await api<Saved>(endpoint, {
        method: 'POST',
        body: JSON.stringify({ name: name.trim(), model: reviewingDraft, expected_revision_id: revision.current }),
        signal,
      })
      setSaved(deployment)
      setPendingPublication(false)
      setPrediction(null)
      revision.current = deployment.id
      setStatus('Detector published. You can now test this revision.')
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) await reloadDetector(signal).catch(() => undefined)
      throw error
    }
  }
  async function removeDetector(signal: AbortSignal) {
    if (!saved) return
    await api(`${endpoint}/${saved.id}`, { method: 'DELETE', signal })
    setSaved(null)
    setPrediction(null)
    revision.current = null
    setStatus('Published detector removed.')
  }
  async function importDetector(file: File, signal: AbortSignal) {
    if (file.size > 48 * 1024) throw new Error('Model exceeds 48 KiB')
    const value = JSON.parse(await file.text())
    signal.throwIfAborted()
    setDraft(validateDetector(value.model))
    setName(typeof value.name === 'string' ? value.name.slice(0, 80) : 'Imported detector')
    setPendingPublication(true)
    setValidation(null)
    revision.current = saved?.id ?? null
    setStatus('Detector imported as a draft. Review it before publishing.')
  }
  async function testDetector(signal: AbortSignal) {
    if (!saved || !testImage) return
    const windows = regionWindows()
    const rows = await features(windows.map(box => ({ image: testImage, box })), signal)
    setPrediction(detect(saved.model, windows.map((box, index) => ({ ...box, features: rows[index].features }))))
    setStatus('Detection complete using the deployed revision.')
  }
  function chooseStage(next: VisionStage) {
    setStatus('')
    setError('')
    setStep(next)
  }

  return <section className="vision-studio" aria-label="Object detection studio">
    <header className="vision-studio-header">
      <div><div className="vision-studio-title"><h2>Vision Studio</h2><span className="context-chip">Preview</span></div>
        <p>{role === 'viewer' ? 'Inspect the detector published for this application.' : 'Label images, evaluate a detector, and test a published revision.'}</p>
      </div>
    </header>
    {role !== 'viewer' && <nav className="vision-steps" aria-label="Vision steps">
      {stages.map(({ id, label }, index) => {
        const complete = id === 'dataset' ? Boolean(validation) : id === 'train' ? Boolean(draft) : Boolean(saved) && !pendingPublication
        return <button key={id} type="button" disabled={busy} aria-label={`${index + 1} · ${label}`}
          aria-current={step === id ? 'step' : undefined} onClick={() => chooseStage(id)}>
          <span className="vision-step-number" data-complete={complete && step !== id} aria-hidden="true">
            {complete && step !== id ? <Check size={15}/> : index + 1}
          </span><span>{label}</span>
        </button>
      })}
    </nav>}
    {error && <p role="alert" className="feedback error">{error}</p>}
    {(status || busy) && <div className="vision-operation">
      <p role="status">{status || 'Working…'}</p>
      {busy && !confirmation && <button type="button" className="manage-button small" onClick={() => action.current?.abort()}>Cancel operation</button>}
    </div>}

    {role !== 'viewer' && <div hidden={step !== 'dataset'}><VisionDatasetEditor
      images={images} classes={classes} heldOut={heldOut} busy={busy} dirty={datasetDirty}
      addImages={files => void addImages(files)} importDataset={file => void importDataset(file)}
      loadSample={() => changeDataset(sampleDataset(), false)} save={exportDataset} onChange={changeDataset}
      onContinue={() => chooseStage('train')}/></div>}

    {role !== 'viewer' && step === 'train' && <section className="vision-stage" aria-label="Train and evaluate detector">
      <div className="vision-stage-header"><div><h3>Train &amp; evaluate</h3>
        <p>Cluster workers extract image features. Your browser fits the detector and evaluates it on held-out images.</p>
      </div></div>
      <div className="vision-stage-grid">
        <section className="vision-stage-card" aria-label="Training configuration">
          <h4>Training setup</h4>
          <dl className="vision-facts">
            <div><dt>Dataset split</dt><dd>{images.length - heldOut} training images · {heldOut} validation images</dd></div>
            <div><dt>Compute cluster</dt><dd>{compute ? <a href={compute.url}>{compute.name}</a> : 'Application compute'}</dd></div>
          </dl>
          <ol className="vision-training-process">
            <li>Extract features on cluster workers</li><li>Fit the detector in this browser</li><li>Measure detection on held-out images</li>
          </ol>
          {draft && <><p>{pendingPublication ? 'Detector draft' : 'Evaluated detector'}{validation ? ` · ${workers} contributing ${workers === 1 ? 'browser' : 'browsers'}` : ''} · {draft.counts.reduce((sum, count) => sum + count, 0)} training regions</p>
            <label className="vision-field">Minimum similarity
              <input type="number" min={0} max={1} step={0.05} disabled={busy} value={draft.threshold} onChange={event => {
                const threshold = event.currentTarget.valueAsNumber
                if (Number.isFinite(threshold)) {
                  setDraft({ ...draft, threshold: Math.max(0, Math.min(1, threshold)) })
                  setPendingPublication(true)
                }
              }}/>
            </label>
            <button type="button" className="manage-button" disabled={busy} onClick={() => downloadJson('mesh-detector.json', { name, model: draft })}><Download size={16}/>Download trained model</button>
          </>}
        </section>
        <section className="vision-stage-card" aria-label="Evaluation results">
          <h4>Evaluation results</h4>
          {metrics ? <><div className="validation-results" data-testid="detection-validation">
            <strong>Held-out detection at IoU ≥ 0.5</strong>
            <dl>
              <div><dt>Precision</dt><dd>{Math.round(metrics.precision * 100)}%</dd></div>
              <div><dt>Recall</dt><dd>{Math.round(metrics.recall * 100)}%</dd></div>
              <div><dt>Objects found</dt><dd>{metrics.matched}/{metrics.objects}</dd></div>
              <div><dt>Predictions</dt><dd>{metrics.predictions}</dd></div>
            </dl>
          </div>
          {metrics.precision < 0.5 && <p className="vision-warning">False positives outnumber correct detections. Add varied background examples or adjust the similarity threshold.</p>}
          <p className="vision-muted">Tuning on these {heldOut} held-out images can overfit the evaluation. Use new images to test the published model.</p>
          {validation && draft && <details className="vision-validation-images"><summary>View validation predictions</summary>
            {validation.images.map((image, index) => <VisionCanvas key={index} image={image} classes={draft.labels.filter(label => label !== backgroundLabel).sort()}
              detections={detect(draft, validation.regions[index])}/>)}
          </details>}</> : <p className="vision-result-empty">Held-out metrics appear after a training run.</p>}
        </section>
      </div>
      <footer className="vision-stage-footer">
        {draft && role === 'owner' && pendingPublication && <button type="button" className="manage-button primary" disabled={busy} onClick={() => chooseStage('deploy')}>Review detector<ArrowRight size={16}/></button>}
        <button type="button" className={`manage-button${draft && role === 'owner' && pendingPublication ? '' : ' primary'}`}
          disabled={busy || !loaded || !images.length} onClick={() => void perform(train)}>
          {draft ? 'Train again' : 'Train detector on cluster'}
        </button>
      </footer>
    </section>}

    {deployStage && <section className="vision-stage" aria-label="Deploy and test detector">
      <div className="vision-stage-header"><div><h3>{role === 'viewer' ? 'Published detector' : 'Deploy & test'}</h3>
        {role !== 'viewer' && <p>Publish a detector draft, then test the deployed revision on a new image.</p>}
      </div></div>
      {reviewingDraft && <section className="vision-stage-card vision-detector-draft" aria-label="Unpublished detector">
        <div><h4>Detector draft</h4><p className="vision-muted">Classes {reviewingDraft.labels.filter(label => label !== backgroundLabel).join(', ')}</p></div>
        <label className="vision-field">Detector name<input value={name} maxLength={80} disabled={busy} onChange={event => setName(event.target.value)}/></label>
        {revision.current !== (saved?.id ?? null) && <div className="vision-conflict">
          <p className="vision-warning">The published detector changed while you were editing this draft.</p>
          <button type="button" className="manage-button" disabled={busy} onClick={() => {
            revision.current = saved?.id ?? null
            setError('')
            setStatus('Draft is based on the current published revision.')
          }}>Use latest revision as publish base</button>
        </div>}
        <button type="button" className="manage-button primary" disabled={busy || !name.trim() || revision.current !== (saved?.id ?? null)}
          onClick={() => setConfirmation('publish')}><CloudUpload size={17}/>Publish detector</button>
      </section>}
      <div className="vision-stage-grid" data-single={role === 'viewer'}>
        <section className="vision-stage-card" aria-label="Deployed detector">
          <h4>Deployment details</h4>
          {!loaded ? <p role="status">Loading detector…</p> : saved ? <>
            <dl className="vision-facts">
              <div><dt>Detector</dt><dd>{saved.name}</dd></div>
              <div><dt>Revision</dt><dd><code>{saved.id.slice(0, 8)}</code> <span className="published-chip">Published</span></dd></div>
              <div><dt>Classes</dt><dd><ul className="vision-class-list">{saved.model.labels.filter(label => label !== backgroundLabel).sort().map((label, _, labels) =>
                <li key={label}><i style={{ background: classColor(label, labels) }}/>{label}</li>)}</ul></dd></div>
              <div><dt>Minimum similarity</dt><dd>{saved.model.threshold}</dd></div>
            </dl>
            <button type="button" className="manage-button" onClick={() => downloadJson('mesh-detector.json', { name: saved.name, model: saved.model })}><Download size={16}/>Download deployed detector</button>
          </> : <p className="vision-result-empty">No detector is published for this application.</p>}
          {role === 'owner' && <details className="vision-detector-settings"><summary>Detector settings</summary>
            <label className="vision-field">Import trained detector<input type="file" accept=".json,application/json" disabled={busy || !loaded || Boolean(reviewingDraft)}
              onChange={event => {
                const file = event.currentTarget.files?.[0]
                event.currentTarget.value = ''
                if (file) void perform(signal => importDetector(file, signal))
              }}/></label>
            <div className="vision-tools">
              <button type="button" className="manage-button" disabled={busy} onClick={() => void perform(reloadDetector)}>Reload deployed detector</button>
              {saved && !reviewingDraft && <button type="button" className="manage-button" disabled={busy} onClick={() => {
                setDraft(saved.model)
                setPendingPublication(true)
                setName(saved.name)
                revision.current = saved.id
                setValidation(null)
                chooseStage('train')
              }}>Edit deployed detector settings</button>}
              {saved && <button type="button" className="manage-button danger" disabled={busy} onClick={() => setConfirmation('remove')}>Remove detector</button>}
            </div>
          </details>}
        </section>
        {role !== 'viewer' && <section className="vision-stage-card vision-test" aria-label="Test on new image">
          <h4>Test on a new image</h4>
          {saved ? <>
            <label className="vision-file-control"><ImageUp size={17}/><span>{testImage ? 'Choose a different image' : 'Choose a test image'}</span>
              <input aria-label="Test image" type="file" accept="image/jpeg,image/png,image/webp" disabled={busy} onChange={event => {
                const file = event.currentTarget.files?.[0]
                event.currentTarget.value = ''
                if (file) void perform(async signal => {
                  const image = await readVisionImage(file, file.name)
                  signal.throwIfAborted()
                  setTestImage(image)
                  setPrediction(null)
                })
              }}/>
            </label>
            {testImage && <div className="vision-test-preview"><VisionCanvas key={testImage.data} image={testImage} classes={saved.model.labels.filter(label => label !== backgroundLabel).sort()} detections={prediction ?? []}/></div>}
            {prediction && <p className="vision-muted">{prediction.length ? `${prediction.length} predictions` : 'No objects found'}</p>}
            <div className="vision-tools">
              <button type="button" className={`manage-button${reviewingDraft ? '' : ' primary'}`} disabled={busy || !testImage} onClick={() => void perform(testDetector)}><Play size={16}/>Detect objects on cluster</button>
              {testImage && prediction && <button type="button" className="manage-button" onClick={() => downloadJson('mesh-detections.json', { revision: saved.id, image: testImage.name, boxes: prediction })}>Download bounding boxes</button>}
            </div>
          </> : <p className="vision-result-empty">Publish a detector to test it here.</p>}
        </section>}
      </div>
    </section>}

    {role !== 'viewer' && <div hidden={step === 'dataset' && !thisWorkerEnabled} className="vision-compute-tools">
      <ImageContributor app={app} purpose="Contribute to vision training and detection" secondary onEnabledChange={setThisWorkerEnabled}/>
      {canShare && step !== 'dataset' && <div>
        <button type="button" className="manage-button" disabled={busy} onClick={() => void perform(async signal => {
          const invite = await api<{ token: string }>(`/api/applications/${app}/contributor-invites?ml_only=true`, { method: 'POST', signal })
          setLink(`${location.origin}/#ml-invite=${invite.token}`)
        })}>Invite vision contributors</button>
        {link && <label className="vision-field">Vision contributor link<input readOnly value={link} onFocus={event => event.currentTarget.select()}/></label>}
      </div>}
    </div>}
    {role !== 'viewer' && <details className="vision-note"><summary>How the detector works</summary><p>Cluster workers extract frozen MobileNetV4 features. This browser fits a small prototype head and tests up to 35 windows per image. Small or crowded objects may be missed. Similarity scores measure feature agreement.</p></details>}

    {confirmation === 'publish' && reviewingDraft && <ConfirmAction title="Publish detector?" action="Confirm publish detector"
      onClose={() => setConfirmation(null)} onConfirm={() => void perform(publishDetector)}>
      <p>Publish <strong>{name.trim()}</strong> as the active detector for <strong>{appName}</strong>{saved ? `, replacing revision ${saved.id.slice(0, 8)}.` : '.'}</p>
      <p>Workflow revisions are published separately.</p>
    </ConfirmAction>}
    {confirmation === 'remove' && saved && <ConfirmAction title="Remove detector?" action="Confirm remove detector" danger
      onClose={() => setConfirmation(null)} onConfirm={() => void perform(removeDetector)}>
      <p>Remove <strong>{saved.name}</strong> (revision {saved.id.slice(0, 8)}) from <strong>{appName}</strong>.</p>
    </ConfirmAction>}
  </section>
}
