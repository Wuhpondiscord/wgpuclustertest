import { useEffect, useRef, useState, type PointerEvent } from 'react'
import { api } from './api'
import { ImageContributor } from './ImageBatchRuntime'
import { backgroundLabel, detect, evaluateDetections, fitDetector, regionWindows, validateDetector, type Box, type Detection, type Detector } from './vision-detector'
import { cropRegion, downloadJson, readVisionImage, sampleDataset, validateDataset, type VisionImage } from './vision-data'

type Saved = { id: string; name: string; saved_at_ms: number; model: Detector }
type FeatureRow = { index: number; features: number[]; worker_id: string }
type Validation = { images: VisionImage[]; regions: Array<Array<Box & { features: number[] }>> }

function BoxCanvas({ image, detections, onBox }: { image: VisionImage; detections?: Detection[]; onBox?: (box: Box) => void }) {
  const [start, setStart] = useState<{ x: number; y: number } | null>(null)
  const [draft, setDraft] = useState<Box | null>(null)
  const point = (event: PointerEvent<SVGSVGElement>) => {
    const bounds = event.currentTarget.getBoundingClientRect()
    return { x: Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width)), y: Math.max(0, Math.min(1, (event.clientY - bounds.top) / bounds.height)) }
  }
  return <div className="vision-canvas"><img src={image.data} alt={image.name}/><svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img" aria-label={onBox ? 'Draw a box around an object. Numeric box controls are also available below.' : 'Predicted bounding boxes'}
    onPointerDown={event => { if (!onBox) return; event.currentTarget.setPointerCapture(event.pointerId); setStart(point(event)) }}
    onPointerMove={event => { if (!start) return; const end = point(event); setDraft({ x: Math.min(start.x, end.x), y: Math.min(start.y, end.y), width: Math.abs(end.x - start.x), height: Math.abs(end.y - start.y) }) }}
    onPointerUp={() => { if (draft && draft.width >= 0.01 && draft.height >= 0.01) onBox?.(draft); setStart(null); setDraft(null) }} onPointerCancel={() => { setStart(null); setDraft(null) }}>
    {(detections ?? image.boxes).map((box, index) => <g key={index}><rect x={box.x * 100} y={box.y * 100} width={box.width * 100} height={box.height * 100} className={box.label === backgroundLabel ? 'background-box' : ''}/><text x={box.x * 100 + 1} y={box.y * 100 + 4}>{box.label === backgroundLabel ? 'background' : box.label}{'score' in box ? ` ${Number(box.score).toFixed(2)}` : ''}</text></g>)}
    {draft && <rect x={draft.x * 100} y={draft.y * 100} width={draft.width * 100} height={draft.height * 100}/>}
  </svg></div>
}

export function VisionStudio({ app, role, canShare }: { app: string; role: 'owner' | 'operator' | 'viewer'; canShare: boolean }) {
  const endpoint = `/api/applications/${app}/detector`
  const [step, setStep] = useState<'dataset' | 'train' | 'deploy'>('dataset')
  const [saved, setSaved] = useState<Saved | null>(null)
  const [removeConfirmed, setRemoveConfirmed] = useState(false)
  const [loaded, setLoaded] = useState(false)
  const [draft, setDraft] = useState<Detector | null>(null)
  const [name, setName] = useState('Custom object detector')
  const [images, setImages] = useState<VisionImage[]>([])
  const [selected, setSelected] = useState(0)
  const [label, setLabel] = useState('my-object')
  const [background, setBackground] = useState(false)
  const [numeric, setNumeric] = useState<Box>({ x: 0.1, y: 0.1, width: 0.5, height: 0.5 })
  const [validation, setValidation] = useState<Validation | null>(null)
  const [testImage, setTestImage] = useState<VisionImage | null>(null)
  const [prediction, setPrediction] = useState<Detection[] | null>(null)
  const [link, setLink] = useState('')
  const [busy, setBusy] = useState(false)
  const [status, setStatus] = useState('')
  const [error, setError] = useState('')
  const [workers, setWorkers] = useState(0)
  const action = useRef<AbortController | null>(null)
  const revision = useRef<string | null>(null)
  const current = images[selected]
  useEffect(() => {
    const controller = new AbortController()
    void api<Saved | null>(endpoint, { signal: controller.signal }).then(value => { setSaved(value); revision.current = value?.id ?? null; setLoaded(true); if (value) setStep('deploy') }).catch(error => { if (!controller.signal.aborted) setError(error.message) })
    return () => { controller.abort(); action.current?.abort() }
  }, [endpoint])
  async function perform(operation: (signal: AbortSignal) => Promise<void>) {
    if (action.current) return
    const controller = new AbortController(); action.current = controller
    setBusy(true); setError(''); setStatus('')
    try { await operation(controller.signal) }
    catch (error) { setError(controller.signal.aborted ? 'Operation cancelled. The deployed model is unchanged.' : error instanceof Error ? error.message : 'Vision request failed') }
    finally { action.current = null; setBusy(false) }
  }
  function invalidate() { setDraft(null); setValidation(null); setPrediction(null) }
  function addBox(box: Box) {
    const annotation = { ...box, label: background ? backgroundLabel : label.trim() }
    try {
      if (!current) return
      const next = images.map((image, index) => index === selected ? { ...image, boxes: [...image.boxes, annotation] } : image)
      validateDataset({ version: 1, images: next }); setImages(next); invalidate(); setError('')
    } catch (error) { setError(error instanceof Error ? error.message : 'Invalid box') }
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
    setDraft(model); setStep('train'); revision.current = saved?.id ?? null
    setValidation({ images: heldout, regions: heldout.map((_, index) => windows.map((box, offset) => ({ ...box, features: rows[examples.length + index * windows.length + offset].features }))) })
    setStatus('Training complete. Review held-out detection results before deploying.')
  }
  const metrics = draft && validation ? evaluateDetections(validation.regions.map(regions => detect(draft, regions)), validation.images.map(image => image.boxes)) : null
  return <section className="vision-studio" aria-label="Object detection studio">
    <div className="studio-heading"><h3>Vision studio</h3><span className="context-chip">Preview</span></div>
    {role !== 'viewer' && <nav className="studio-steps" aria-label="Vision steps">{(['dataset', 'train', 'deploy'] as const).map((value, index) => <button key={value} type="button" aria-pressed={step === value} onClick={() => setStep(value)}>{index + 1} · {value === 'dataset' ? 'Label images' : value === 'train' ? 'Train & evaluate' : 'Deploy & test'}</button>)}</nav>}
    <details className="studio-note"><summary>How this preview works</summary><p>Uses frozen MobileNetV4 features and a small prototype head. Cluster workers extract features for each region, then this tab fits the head. Detection searches 35 square windows per image and suppresses overlapping boxes. Small or crowded objects may be missed. Similarity scores are not probabilities.</p></details>
    {error && <p role="alert" className="feedback error">{error}</p>}{status && <p role="status">{status}</p>}
    {busy && <button className="manage-button" onClick={() => action.current?.abort()}>Cancel vision operation</button>}
    {role !== 'viewer' && <>
      <ImageContributor app={app} purpose="Contribute to vision training and detection"/>
      {canShare && <div><button className="manage-button" disabled={busy} onClick={() => void perform(async () => {
        const invite = await api<{ token: string }>(`/api/applications/${app}/contributor-invites?ml_only=true`, { method: 'POST' })
        setLink(`${location.origin}/#ml-invite=${invite.token}`)
      })}>Invite vision contributors</button>{link && <label>Vision contributor link<input readOnly value={link} onFocus={event => event.currentTarget.select()}/></label>}</div>}
      <section className="studio-panel" hidden={step !== 'dataset'} aria-label="Vision dataset"><h4>Image dataset</h4>
        <p>Up to 32 images and 16 boxes per image. Images stay in this tab until you start a run. Export the dataset to save your annotations. Only cropped regions are sent to trusted contributors.</p>
        <div className="heading-buttons"><button className="manage-button" disabled={busy || images.length > 0} onClick={() => { setImages(sampleDataset()); setSelected(0); invalidate() }}>Load box detection sample</button><button className="manage-button" disabled={busy || !images.length} onClick={() => downloadJson('mesh-vision-dataset.json', { version: 1, images })}>Export labeled dataset</button></div>
        <p>The sample uses colored blocks to exercise the workflow. It is not evidence of real-world object detection accuracy.</p>
        <label>Add training images<input type="file" multiple accept="image/jpeg,image/png,image/webp" disabled={busy} onChange={event => {
          const files = Array.from(event.target.files ?? []); event.currentTarget.value = ''
          void perform(async () => { if (images.length + files.length > 32) throw new Error('Use up to 32 images'); const next: VisionImage[] = []; for (const file of files) next.push(await readVisionImage(file, file.name)); setImages(validateDataset({ version: 1, images: [...images, ...next] })); invalidate() })
        }}/></label>
        <label>Import labeled dataset<input type="file" accept=".json,application/json" disabled={busy || images.length > 0} onChange={event => {
          const file = event.target.files?.[0]; event.currentTarget.value = ''
          if (file) void perform(async () => { if (file.size > 8 * 1024 * 1024) throw new Error('Dataset exceeds 8 MiB'); setImages(validateDataset(JSON.parse(await file.text()))); setSelected(0); invalidate() })
        }}/></label>
        {current && <div className="vision-layout">
          <div className="vision-image-area">
            <div className="vision-image-title"><strong>{current.name}</strong><span>{current.boxes.length} boxes</span></div>
            <BoxCanvas image={current} onBox={busy ? undefined : addBox}/>
            <div className="vision-filmstrip" role="group" aria-label="Dataset images">
              {images.map((image, index) => <button key={index} type="button" disabled={busy}
                aria-label={`Image ${index + 1}: ${image.name}`} aria-pressed={index === selected}
                onClick={() => setSelected(index)}><img src={image.data} alt=""/><span>{index + 1}</span></button>)}
            </div>
          </div>
          <div className="application-form vision-inspector">
            <h5>Annotations</h5>
            <label>Object label<input disabled={busy || background} value={label} maxLength={40} onChange={event => setLabel(event.target.value)}/></label>
            <label><input type="checkbox" disabled={busy} checked={background} onChange={event => setBackground(event.target.checked)}/>Label background (no target objects)</label>
            <label>Use this image for<select disabled={busy} value={current.split} onChange={event => { setImages(images.map((image, index) => index === selected ? { ...image, split: event.target.value as VisionImage['split'] } : image)); invalidate() }}><option value="train">Training</option><option value="validation">Held-out validation</option></select></label>
            <details><summary>Enter a box with the keyboard</summary><p>Coordinates are fractions of the full image, from 0 to 1.</p>{(['x', 'y', 'width', 'height'] as const).map(key => <label key={key}>Box {key}<input type="number" min="0" max="1" step="0.01" value={numeric[key]} onChange={event => setNumeric({ ...numeric, [key]: Number(event.target.value) })}/></label>)}<button disabled={busy} className="manage-button" onClick={() => addBox(numeric)}>Add bounding box</button></details>
            <ul>{current.boxes.map((box, index) => <li key={index}>{box.label === backgroundLabel ? 'background' : box.label} · {box.x.toFixed(2)}, {box.y.toFixed(2)} <button className="manage-button small" disabled={busy} onClick={() => { setImages(images.map((image, i) => i === selected ? { ...image, boxes: image.boxes.filter((_, b) => b !== index) } : image)); invalidate() }}>Remove box {index + 1}</button></li>)}</ul>
            <button className="manage-button danger" disabled={busy} onClick={() => { setImages(images.filter((_, index) => index !== selected)); setSelected(0); invalidate() }}>Remove selected image</button>
          </div></div>}
        <button className="manage-button primary" disabled={!images.length} onClick={() => setStep('train')}>Continue to training</button>
      </section>
      <section className="studio-panel" hidden={step !== 'train'}><h4>Detector training</h4><p>{images.filter(image => image.split === 'train').length} training images · {images.filter(image => image.split === 'validation').length} validation images</p>
        <button className="manage-button primary" disabled={busy || !loaded || !images.length} onClick={() => void perform(train)}>Train detector on cluster</button>
        {draft && <><p>Detector draft · {validation ? `${workers} contributing browsers · ` : ''} {draft.counts.reduce((a, b) => a + b, 0)} training regions</p><label>Minimum similarity<input type="number" min="0" max="1" step="0.05" disabled={busy} value={draft.threshold} onChange={event => setDraft({ ...draft, threshold: Math.max(0, Math.min(1, Number(event.target.value))) })}/></label>
          {metrics && <>{metrics.precision < 0.5 && <p className="feedback error">Validation has more false positives than correct detections. Add varied background examples or raise the similarity threshold before relying on this detector.</p>}<p data-testid="detection-validation">Held-out detection at IoU ≥ 0.5 · precision {(metrics.precision * 100).toFixed(0)}% · recall {(metrics.recall * 100).toFixed(0)}% · {metrics.matched}/{metrics.objects} objects found · {metrics.predictions} predictions</p><p>Tuning on this validation set makes it unsuitable as a final test set. Test the deployed model on new images.</p>{validation && <details><summary>View validation predictions</summary>{validation.images.map((image, index) => <BoxCanvas key={index} image={image} detections={detect(draft, validation.regions[index])}/>)}</details>}</>}
          <label>Detector name<input maxLength={80} value={name} disabled={busy} onChange={event => setName(event.target.value)}/></label>
          <button className="manage-button" disabled={busy} onClick={() => downloadJson('mesh-detector.json', { name, model: draft })}>Download trained model</button>
          {role === 'owner' && <button className="manage-button primary" disabled={busy || !name.trim()} onClick={() => void perform(async signal => {
            const deployment = await api<Saved>(endpoint, { method: 'POST', body: JSON.stringify({ name, model: draft, expected_revision_id: revision.current }), signal })
            setSaved(deployment); setStep('deploy'); setPrediction(null); revision.current = deployment.id; setStatus('Detector deployed. Test it below with new images.')
          })}>Deploy detector</button>}</>}
      </section>
    </>}
    <section className="studio-panel" hidden={role !== 'viewer' && step !== 'deploy'}><h4>Deployed detector</h4>
      {!loaded ? <p>Loading model…</p> : !saved ? <p>No custom detector deployed yet.</p> : <div className="detector-layout">
        <div className="detector-preview">
          <div className="vision-image-title"><strong>Test preview</strong>{prediction && <span>{prediction.length} objects detected</span>}</div>
          {testImage ? <BoxCanvas image={testImage} detections={prediction ?? []}/> : <div className="detector-empty">Choose a test image to preview detection.</div>}
          {testImage && prediction && <button className="manage-button" onClick={() => downloadJson('mesh-detections.json', { revision: saved.id, image: testImage.name, boxes: prediction })}>Download bounding boxes</button>}
        </div>
        <div className="vision-inspector">
          <h5>{saved.name}</h5>
          <dl className="detector-facts">
            <div><dt>Revision</dt><dd>{saved.id.slice(0, 8)}</dd></div>
            <div><dt>Classes</dt><dd>{saved.model.labels.filter(label => label !== backgroundLabel).join(', ')}</dd></div>
            <div><dt>Minimum similarity</dt><dd>{saved.model.threshold}</dd></div>
          </dl>
          {role !== 'viewer' && <><label>Test image<input type="file" accept="image/jpeg,image/png,image/webp" disabled={busy} onChange={event => { const file = event.target.files?.[0]; event.currentTarget.value = ''; if (file) void perform(async () => { setTestImage(await readVisionImage(file, file.name)); setPrediction(null) }) }}/></label>
            <button className="manage-button primary" disabled={busy || !testImage} onClick={() => void perform(async signal => {
              const windows = regionWindows(); const rows = await features(windows.map(box => ({ image: testImage!, box })), signal)
              setPrediction(detect(saved.model, windows.map((box, index) => ({ ...box, features: rows[index].features })))); setStatus('Detection complete using the deployed revision.')
            })}>Detect objects on cluster</button></>}
            <button className="manage-button" onClick={() => downloadJson('mesh-detector.json', { name: saved.name, model: saved.model })}>Download deployed detector</button>
          <details><summary>Detector settings</summary>
            <p>Saved {new Date(saved.saved_at_ms).toLocaleString()} · Backbone {saved.model.backbone_revision.slice(0, 12)}</p>
            {role === 'owner' && <button className="manage-button" disabled={busy} onClick={() => { setDraft(saved.model); setStep('train'); setName(saved.name); revision.current = saved.id; setValidation(null) }}>Edit deployed detector settings</button>}
            {role === 'owner' && <details><summary>Remove deployed detector</summary><label><input type="checkbox" checked={removeConfirmed} onChange={event => setRemoveConfirmed(event.target.checked)}/>Confirm removal of this detector</label><button className="manage-button danger" disabled={busy || !removeConfirmed} onClick={() => void perform(async signal => { await api(`${endpoint}/${saved.id}`, { method: 'DELETE', signal }); setSaved(null); setDraft(null); setValidation(null); setPrediction(null); setRemoveConfirmed(false); revision.current = null })}>Remove detector</button></details>}
          </details>
        </div>
      </div>}
      {role === 'owner' && <label>Import trained detector<input type="file" accept=".json,application/json" disabled={busy || !loaded} onChange={event => {
        const file = event.target.files?.[0]; event.currentTarget.value = ''
        if (file) void perform(async () => { if (file.size > 48 * 1024) throw new Error('Model exceeds 48 KiB'); const value = JSON.parse(await file.text()); setDraft(validateDetector(value.model)); setStep('train'); setName(typeof value.name === 'string' ? value.name.slice(0, 80) : 'Imported detector'); setValidation(null); revision.current = saved?.id ?? null; setStatus('Model imported as a draft. Review and deploy it above.') })
      }}/></label>}
      {role === 'owner' && <button className="manage-button" disabled={busy} onClick={() => void perform(async signal => { const value = await api<Saved | null>(endpoint, { signal }); setSaved(value); setLoaded(true); setDraft(null); setPrediction(null); setValidation(null); revision.current = value?.id ?? null })}>Reload deployed detector</button>}
    </section>
  </section>
}
