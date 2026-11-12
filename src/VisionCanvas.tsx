import { useRef, useState, type CSSProperties, type PointerEvent, type RefObject } from 'react'
import { backgroundLabel, type Box, type Detection } from './vision-detector'
import type { VisionImage } from './vision-data'

type Point = { x: number; y: number }
type Corner = 'nw' | 'ne' | 'sw' | 'se'
export type VisionTool = 'select' | 'draw' | 'pan'
type Editing = {
  tool: VisionTool
  zoom: number
  selected: number | null
  viewport: RefObject<HTMLDivElement>
  onSelect: (index: number | null) => void
  onChange: (box: Box, index?: number) => void
}
type Gesture =
  | { kind: 'draw'; start: Point }
  | { kind: 'move' | Corner; start: Point; box: Box; index: number }
  | { kind: 'pan'; start: Point; scroll: Point }

const clamp = (value: number, min = 0, max = 1) => Math.min(max, Math.max(min, value))
const colors = ['#1858d5', '#007a46', '#863b97', '#a44809', '#3e5277']
export const classColor = (label: string, classes: string[]) => label === backgroundLabel ? '#705019' : colors[Math.max(0, classes.indexOf(label)) % colors.length]
export const classLabel = (label: string) => label === backgroundLabel ? 'Background' : label

function editedBox(gesture: Exclude<Gesture, { kind: 'pan' }>, point: Point): Box {
  const { start } = gesture
  if (gesture.kind === 'draw') return {
    x: Math.min(start.x, point.x), y: Math.min(start.y, point.y),
    width: Math.abs(point.x - start.x), height: Math.abs(point.y - start.y),
  }
  const { box, kind } = gesture
  if (kind === 'move') return {
    ...box, x: clamp(box.x + point.x - start.x, 0, 1 - box.width), y: clamp(box.y + point.y - start.y, 0, 1 - box.height),
  }
  const deltaX = point.x - start.x
  const deltaY = point.y - start.y
  const left = kind.endsWith('w') ? clamp(box.x + deltaX, 0, box.x + box.width - 0.01) : box.x
  const right = kind.endsWith('e') ? clamp(box.x + box.width + deltaX, box.x + 0.01, 1) : box.x + box.width
  const top = kind.startsWith('n') ? clamp(box.y + deltaY, 0, box.y + box.height - 0.01) : box.y
  const bottom = kind.startsWith('s') ? clamp(box.y + box.height + deltaY, box.y + 0.01, 1) : box.y + box.height
  return { x: left, y: top, width: right - left, height: bottom - top }
}

function boxStyle(box: Box, color: string): CSSProperties {
  return {
    left: `${box.x * 100}%`, top: `${box.y * 100}%`, width: `${box.width * 100}%`, height: `${box.height * 100}%`,
    '--box-color': color,
  } as CSSProperties
}

export function VisionCanvas({ image, classes, detections, editing }: {
  image: VisionImage
  classes: string[]
  detections?: Detection[]
  editing?: Editing
}) {
  const gesture = useRef<Gesture | null>(null)
  const [preview, setPreview] = useState<{ box: Box; index?: number } | null>(null)
  const [aspect, setAspect] = useState(0)
  const zoom = editing?.zoom ?? 100
  const boxes = detections ?? image.boxes
  const pointer = (event: PointerEvent<HTMLDivElement>) => {
    const bounds = event.currentTarget.getBoundingClientRect()
    return { x: clamp((event.clientX - bounds.left) / bounds.width), y: clamp((event.clientY - bounds.top) / bounds.height) }
  }
  function begin(event: PointerEvent<HTMLDivElement>) {
    if (!editing || event.button !== 0 || !event.isPrimary) return
    if (editing.tool === 'pan') {
      const viewport = editing.viewport.current
      if (!viewport) return
      gesture.current = { kind: 'pan', start: { x: event.clientX, y: event.clientY }, scroll: { x: viewport.scrollLeft, y: viewport.scrollTop } }
    } else {
      const start = pointer(event)
      if (editing.tool === 'draw') gesture.current = { kind: 'draw', start }
      else {
        const target = (event.target as HTMLElement).closest<HTMLElement>('[data-box]')
        const index = target ? Number(target.dataset.box) : null
        editing.onSelect(index)
        if (index === null) return
        const corner = (event.target as HTMLElement).dataset.corner as Corner | undefined
        gesture.current = { kind: corner ?? 'move', start, box: boxes[index], index }
      }
    }
    event.preventDefault()
    event.currentTarget.setPointerCapture(event.pointerId)
  }
  function move(event: PointerEvent<HTMLDivElement>) {
    const current = gesture.current
    if (!current || !event.isPrimary) return
    if (current.kind === 'pan') {
      editing?.viewport.current?.scrollTo(current.scroll.x + current.start.x - event.clientX, current.scroll.y + current.start.y - event.clientY)
    } else {
      setPreview({ box: editedBox(current, pointer(event)), index: 'index' in current ? current.index : undefined })
    }
  }
  function finish(event: PointerEvent<HTMLDivElement>) {
    const current = gesture.current
    if (!current || !event.isPrimary) return
    reset()
    if (current.kind === 'pan') return
    const point = pointer(event)
    if (current.kind !== 'draw' && Math.hypot(point.x - current.start.x, point.y - current.start.y) < 0.002) return
    const box = editedBox(current, point)
    if (box.width >= 0.009999 && box.height >= 0.009999) editing?.onChange(box, 'index' in current ? current.index : undefined)
  }
  function reset() {
    gesture.current = null
    setPreview(null)
  }
  return <div className="vision-frame" data-tool={editing?.tool}
    style={{ width: aspect ? `min(${zoom}%, calc(var(--vision-fit-height, 460px) * ${aspect * zoom / 100}))` : `${zoom}%` }}
    onPointerDown={begin} onPointerMove={move} onPointerUp={finish} onPointerCancel={reset} onLostPointerCapture={reset}>
    <img draggable={false} src={image.data} alt={image.name} onLoad={event => {
      const img = event.currentTarget
      setAspect(img.naturalWidth / img.naturalHeight)
    }}/>
    {boxes.map((box, index) => <div className="vision-box" key={index} data-box={index}
      data-selected={editing?.selected === index} style={boxStyle(preview?.index === index ? preview.box : box, classColor(box.label, classes))}>
      <span className="vision-box-tag" data-inside={box.y < 0.06}>{classLabel(box.label)}{'score' in box && ` ${Number(box.score).toFixed(2)}`}</span>
      {editing?.selected === index && editing.tool === 'select' && (['nw', 'ne', 'sw', 'se'] as const).map(corner =>
        <span key={corner} className="vision-box-handle" data-corner={corner} aria-hidden="true"/>)}
    </div>)}
    {preview && preview.index === undefined && <div className="vision-box vision-box-preview" style={boxStyle(preview.box, '#2563eb')}/>}
  </div>
}
