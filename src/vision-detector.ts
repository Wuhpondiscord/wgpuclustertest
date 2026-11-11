// Bounded region-prototype transfer learning. The frozen backbone is run by cluster workers.
export const backboneRevision = '3ba07f12712fa58fd6b3d661f9909c9e332c5005'
export const backgroundLabel = '__background__'
export type Box = { x: number; y: number; width: number; height: number }
export type Annotation = Box & { label: string }
export type Detection = Annotation & { score: number }
export type Detector = {
  format: 'mesh-region-prototypes-v1'; backbone_revision: string
  labels: string[]; prototypes: string[]; counts: number[]; threshold: number; margin: number
}
export type Example = { label: string; features: number[] }

export function normalize(values: number[]) {
  if (values.length !== 1000 || values.some(value => !Number.isFinite(value) || Math.abs(value) > 10000)) throw new Error('Invalid vision features')
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length
  const centered = values.map(value => value - mean)
  const length = Math.hypot(...centered)
  if (length < 1e-8) throw new Error('Vision features contain no usable signal')
  return centered.map(value => value / length)
}
function pack(values: number[]) {
  const bytes = new Uint8Array(values.length * 4)
  const view = new DataView(bytes.buffer)
  values.forEach((value, index) => view.setFloat32(index * 4, value, true))
  return btoa(Array.from(bytes, byte => String.fromCharCode(byte)).join(''))
}
export function unpack(value: string) {
  const bytes = Uint8Array.from(atob(value), byte => byte.charCodeAt(0))
  if (bytes.length !== 4000) throw new Error('Invalid prototype size')
  const view = new DataView(bytes.buffer)
  const values = Array.from({ length: 1000 }, (_, index) => view.getFloat32(index * 4, true))
  if (values.some(value => !Number.isFinite(value)) || Math.abs(Math.hypot(...values) - 1) > 0.001) throw new Error('Invalid prototype values')
  return values
}
export function fitDetector(examples: Example[]): Detector {
  const labels = [...new Set(examples.map(example => example.label))].sort()
  if (!labels.includes(backgroundLabel) || labels.length < 2 || labels.length > 5) throw new Error('Use 1–4 object classes and background examples')
  const counts = labels.map(label => examples.filter(example => example.label === label).length)
  if (counts.some(count => count < 2)) throw new Error('Each class and background need at least two training boxes')
  const prototypes = labels.map(label => {
    const sum = Array<number>(1000).fill(0)
    for (const example of examples.filter(example => example.label === label)) normalize(example.features).forEach((value, index) => { sum[index] += value })
    return pack(normalize(sum.map(value => value / examples.length)))
  })
  return { format: 'mesh-region-prototypes-v1', backbone_revision: backboneRevision, labels, prototypes, counts, threshold: 0.65, margin: 0.05 }
}
export function validateDetector(model: Detector) {
  if (model.format !== 'mesh-region-prototypes-v1' || model.backbone_revision !== backboneRevision || !Array.isArray(model.labels) || model.labels.length < 2 || model.labels.length > 5 || new Set(model.labels).size !== model.labels.length || !model.labels.includes(backgroundLabel)
    || model.labels.some(label => !/^[a-zA-Z0-9 _-]{1,40}$/.test(label)) || model.prototypes.length !== model.labels.length || model.counts.length !== model.labels.length
    || model.counts.some(count => !Number.isInteger(count) || count < 2 || count > 1024) || !(model.threshold >= 0 && model.threshold <= 1) || !(model.margin >= 0 && model.margin <= 1)) throw new Error('Invalid or incompatible detector')
  model.prototypes.forEach(unpack)
  return model
}
export function intersectionOverUnion(a: Box, b: Box) {
  const intersection = Math.max(0, Math.min(a.x + a.width, b.x + b.width) - Math.max(a.x, b.x)) * Math.max(0, Math.min(a.y + a.height, b.y + b.height) - Math.max(a.y, b.y))
  return intersection / (a.width * a.height + b.width * b.height - intersection || 1)
}
export function regionWindows(): Box[] {
  const windows: Box[] = [{ x: 0, y: 0, width: 1, height: 1 }]
  for (const size of [0.5, 0.75]) {
    const steps = size === 0.5 ? 4 : 2
    for (let y = 0; y <= steps; y++) for (let x = 0; x <= steps; x++) windows.push({ x: x * (1 - size) / steps, y: y * (1 - size) / steps, width: size, height: size })
  }
  return windows
}
export function detect(model: Detector, regions: Array<Box & { features: number[] }>): Detection[] {
  validateDetector(model)
  const prototypes = model.prototypes.map(unpack)
  const background = model.labels.indexOf(backgroundLabel)
  const proposed: Detection[] = []
  for (const region of regions) {
    const features = normalize(region.features)
    const scores = prototypes.map(prototype => prototype.reduce((sum, value, index) => sum + value * features[index], 0))
    const best = scores.reduce((index, score, current) => score > scores[index] ? current : index, 0)
    if (best !== background && scores[best] >= model.threshold && scores[best] - scores[background] >= model.margin) {
      const { x, y, width, height } = region
      proposed.push({ x, y, width, height, label: model.labels[best], score: scores[best] })
    }
  }
  const kept: Detection[] = []
  for (const box of proposed.sort((a, b) => b.score - a.score)) {
    if (!kept.some(previous => previous.label === box.label && intersectionOverUnion(previous, box) > 0.3)) kept.push(box)
    if (kept.length >= 20) break
  }
  return kept
}
export function evaluateDetections(predicted: Detection[][], expected: Annotation[][]) {
  let matched = 0, predictions = 0, objects = 0
  expected.forEach((annotations, index) => {
    const truth = annotations.filter(box => box.label !== backgroundLabel)
    const used = new Set<number>()
    objects += truth.length
    for (const detection of [...predicted[index]].sort((a, b) => b.score - a.score)) {
      predictions++
      let best = -1, overlap = 0.5
      truth.forEach((box, candidate) => { const iou = intersectionOverUnion(detection, box); if (!used.has(candidate) && box.label === detection.label && iou >= overlap) { best = candidate; overlap = iou } })
      if (best >= 0) { used.add(best); matched++ }
    }
  })
  return { matched, predictions, objects, precision: predictions ? matched / predictions : 0, recall: objects ? matched / objects : 0 }
}
