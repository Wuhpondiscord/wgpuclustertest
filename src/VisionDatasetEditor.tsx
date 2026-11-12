import { useRef, useState } from 'react'
import { ArrowRight, ChevronLeft, ChevronRight, Download, Hand, ImagePlus, Minus, MousePointer2, Plus, SquareDashed, Trash2 } from 'lucide-react'
import { classColor, classLabel, VisionCanvas, type VisionTool } from './VisionCanvas'
import { ConfirmAction } from './ConfirmAction'
import { backgroundLabel, type Annotation, type Box } from './vision-detector'
import type { VisionImage } from './vision-data'

const tools = [
  { id: 'select', label: 'Select box', icon: MousePointer2 },
  { id: 'draw', label: 'Draw box', icon: SquareDashed },
  { id: 'pan', label: 'Pan image', icon: Hand },
] as const
const fields = [{ key: 'x', label: 'Left' }, { key: 'y', label: 'Top' }, { key: 'width', label: 'Width' }, { key: 'height', label: 'Height' }] as const
const initialBox = { x: 0.1, y: 0.1, width: 0.5, height: 0.5 }

export function VisionDatasetEditor({ images, classes, heldOut, busy, dirty, addImages, importDataset, loadSample, save, onChange, onContinue }: {
  images: VisionImage[]
  classes: string[]
  heldOut: number
  busy: boolean
  dirty: boolean
  addImages: (files: File[]) => void
  importDataset: (file: File) => void
  loadSample: () => void
  save: () => void
  onChange: (images: VisionImage[]) => boolean
  onContinue: () => void
}) {
  const [index, setIndex] = useState(0)
  const [selected, setSelected] = useState<number | null>(null)
  const [label, setLabel] = useState('my-object')
  const [background, setBackground] = useState(false)
  const [numeric, setNumeric] = useState<Box>(initialBox)
  const [tool, setTool] = useState<VisionTool>('draw')
  const [zoom, setZoom] = useState(100)
  const [confirmRemoval, setConfirmRemoval] = useState(false)
  const viewport = useRef<HTMLDivElement>(null)
  const current = images[index]
  const currentBox = selected === null ? undefined : current?.boxes[selected]

  function chooseImage(next: number) {
    setIndex(next)
    setSelected(null)
    setNumeric(initialBox)
    setZoom(100)
    viewport.current?.scrollTo(0, 0)
  }
  function selectBox(next: number | null) {
    setSelected(next)
    const annotation = next === null ? undefined : current?.boxes[next]
    if (!annotation) return
    setNumeric(annotation)
    setBackground(annotation.label === backgroundLabel)
    if (annotation.label !== backgroundLabel) setLabel(annotation.label)
  }
  function replaceImage(next: VisionImage) {
    return onChange(images.map((image, position) => position === index ? next : image))
  }
  function saveBox(box: Box, target?: number, keepClass = false) {
    const rounded = (n: number) => Math.round(n * 1_000_000) / 1_000_000
    const boxLabel = keepClass && target !== undefined ? current.boxes[target].label : background ? backgroundLabel : label.trim()
    const annotation: Annotation = { x: rounded(box.x), y: rounded(box.y), width: rounded(box.width), height: rounded(box.height), label: boxLabel }
    const boxes = [...current.boxes]
    const position = target ?? boxes.length
    boxes[position] = annotation
    if (replaceImage({ ...current, boxes })) {
      setSelected(position)
      setNumeric(annotation)
    }
  }

  const files = <div className="vision-dataset-files">
    <label className="vision-file-control"><ImagePlus size={17}/><span>Add images</span>
      <input type="file" aria-label="Add training images" multiple accept="image/jpeg,image/png,image/webp" disabled={busy || images.length >= 32}
        onChange={event => {
          const selectedFiles = Array.from(event.currentTarget.files ?? [])
          event.currentTarget.value = ''
          if (selectedFiles.length) addImages(selectedFiles)
        }}/>
    </label>
    {!images.length && <label className="vision-file-control"><span>Import labeled dataset</span>
      <input type="file" aria-label="Import labeled dataset" accept=".json,application/json" disabled={busy}
        onChange={event => {
          const file = event.currentTarget.files?.[0]
          event.currentTarget.value = ''
          if (file) { chooseImage(0); importDataset(file) }
        }}/>
    </label>}
    {images.length ? <button className="manage-button" type="button" disabled={busy} onClick={save}><Download size={15}/>Export labeled dataset</button>
      : <button className="manage-button" type="button" disabled={busy} onClick={() => { chooseImage(0); loadSample() }}>Load box detection sample</button>}
  </div>

  return <section className="vision-stage" aria-label="Vision dataset">
    <div className="vision-stage-header">
      <div><h3>Label images</h3><p>Draw a box around each object and assign its class.</p></div>
      {files}
    </div>
    {current ? <div className="vision-edit-grid">
      <div className="vision-editor">
        <div className="vision-editor-bar">
          <div><strong>{current.name}</strong><small>Image {index + 1} of {images.length}</small></div>
          <div className="vision-icon-group" aria-label="Image navigation" role="group">
            <button type="button" className="vision-icon-button" aria-label="Previous image" disabled={busy || index === 0} onClick={() => chooseImage(index - 1)}><ChevronLeft size={18}/></button>
            <button type="button" className="vision-icon-button" aria-label="Next image" disabled={busy || index + 1 === images.length} onClick={() => chooseImage(index + 1)}><ChevronRight size={18}/></button>
          </div>
        </div>
        <div className="vision-editor-toolbar">
          <div className="vision-tools" role="group" aria-label="Annotation tools">
            {tools.map(({ id, label: toolLabel, icon: Icon }) => <button key={id} className="vision-tool-button" type="button"
              disabled={busy} aria-pressed={tool === id} onClick={() => setTool(id)}><Icon size={16}/>{toolLabel}</button>)}
          </div>
          <div className="vision-icon-group vision-zoom" role="group" aria-label="Image zoom">
            <button type="button" className="vision-icon-button" aria-label="Zoom out" disabled={zoom === 100} onClick={() => setZoom(value => value - 25)}><Minus size={16}/></button>
            <output aria-label="Zoom level">{zoom}%</output>
            <button type="button" className="vision-icon-button" aria-label="Zoom in" disabled={zoom === 200} onClick={() => setZoom(value => value + 25)}><Plus size={16}/></button>
          </div>
        </div>
        <div className="vision-viewport" ref={viewport} role="region" aria-label="Image annotation canvas" tabIndex={0}>
          <VisionCanvas key={index} image={current} classes={classes}
            editing={busy ? undefined : { tool, zoom, selected, viewport, onSelect: selectBox, onChange: (box, target) => saveBox(box, target, true) }}/>
        </div>
        <div className="vision-thumbnails" role="group" aria-label="Dataset images">
          {images.map((image, position) => <button key={position} type="button" disabled={busy} aria-label={`Image ${position + 1}: ${image.name}`}
            aria-pressed={position === index} onClick={() => chooseImage(position)}><img src={image.data} alt=""/><span>{position + 1}</span></button>)}
        </div>
      </div>
      <aside className="vision-annotation-panel" aria-label="Dataset and annotations">
        <div className="vision-dataset-summary">
          <h4>Dataset</h4>
          <strong>{images.length} / 32 images</strong>
          <p>{images.length - heldOut} training · {heldOut} held out</p>
          {classes.length > 0 && <ul className="vision-class-list" aria-label="Object classes">
            {classes.map(item => <li key={item}><i style={{ background: classColor(item, classes) }}/>{item}</li>)}
          </ul>}
        </div>
        <div className="vision-annotation-fields">
          <h4>Annotations <span className="context-chip">{current.boxes.length} / 16</span></h4>
          {current.boxes.length > 0 && <div className="vision-box-list" aria-label="Bounding boxes">
            {current.boxes.map((box, position) => <button key={position} type="button" disabled={busy} aria-pressed={position === selected}
              onClick={() => { selectBox(position); setTool('select') }}>
              <i style={{ background: classColor(box.label, classes) }}/>{position + 1}. {classLabel(box.label)}
            </button>)}
          </div>}
          <label>Object class<input disabled={busy || background} value={label} maxLength={40} onChange={event => setLabel(event.target.value)}/></label>
          <label className="vision-check"><input type="checkbox" disabled={busy} checked={background} onChange={event => setBackground(event.target.checked)}/>Label as background</label>
          {currentBox && <>
            <button type="button" className="manage-button" disabled={busy} onClick={() => saveBox(currentBox, selected!)}>Apply class to selected box</button>
            <button type="button" className="vision-text-button" disabled={busy} onClick={() => {
              if (replaceImage({ ...current, boxes: current.boxes.filter((_, position) => position !== selected) })) selectBox(null)
            }}><Trash2 size={15}/>Delete selected box</button>
          </>}
          <details className="vision-coordinates"><summary>Box coordinates</summary>
            <p>Enter percentages relative to the image.</p>
            <div>{fields.map(({ key, label: fieldLabel }) => <label key={key}>{fieldLabel} (%)
              <input type="number" min={0} max={100} step="any" disabled={busy} value={Number.isFinite(numeric[key]) ? Number((numeric[key] * 100).toFixed(4)) : ''}
                onChange={event => {
                  const value = event.currentTarget.valueAsNumber / 100
                  setNumeric(previous => ({ ...previous, [key]: value }))
                }}/>
            </label>)}</div>
            <button type="button" className="manage-button" disabled={busy} onClick={() => saveBox(numeric, selected ?? undefined)}>
              {currentBox ? 'Apply box coordinates' : 'Add bounding box'}
            </button>
          </details>
          <label>Image split<select disabled={busy} value={current.split}
            onChange={event => replaceImage({ ...current, split: event.currentTarget.value as VisionImage['split'] })}>
            <option value="train">Training</option><option value="validation">Held-out validation</option>
          </select></label>
          <button type="button" className="vision-text-button danger" disabled={busy} onClick={() => setConfirmRemoval(true)}>Remove this image</button>
          {confirmRemoval && <ConfirmAction title="Remove image?" action="Confirm remove image" danger
            onClose={() => setConfirmRemoval(false)} onConfirm={() => {
              if (onChange(images.filter((_, position) => position !== index))) chooseImage(Math.max(0, Math.min(index, images.length - 2)))
            }}><p>Remove <strong>{current.name}</strong> and its boxes from this dataset.</p></ConfirmAction>}
        </div>
      </aside>
    </div> : <div className="vision-empty">
      <ImagePlus size={26}/><h4>Add your first images</h4><p>Choose JPEG, PNG, or WebP files, import an exported dataset, or try the built-in sample.</p>
    </div>}
    <footer className="vision-stage-footer">
      <small>{dirty ? 'Local changes · export to keep your dataset.' : images.length ? 'Dataset changes stay in this tab.' : 'Images stay in this tab until you start a run.'}</small>
      <button type="button" className="manage-button primary" disabled={busy || !images.length} onClick={onContinue}>Continue to training<ArrowRight size={16}/></button>
    </footer>
  </section>
}
