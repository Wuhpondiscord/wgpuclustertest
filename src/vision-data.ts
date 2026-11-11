import type { Annotation, Box } from './vision-detector'
export type VisionImage = { name: string; data: string; split: 'train' | 'validation'; boxes: Annotation[] }

export async function readVisionImage(file: Blob, name: string): Promise<VisionImage> {
  if (file.size > 12 * 1024 * 1024) throw new Error('Choose images smaller than 12 MB')
  const bitmap = await createImageBitmap(file)
  try {
    if (bitmap.width * bitmap.height > 40_000_000) throw new Error('Images must be smaller than 40 megapixels')
    const canvas = document.createElement('canvas')
    const ratio = Math.min(1, 768 / Math.max(bitmap.width, bitmap.height))
    canvas.width = Math.max(1, Math.round(bitmap.width * ratio)); canvas.height = Math.max(1, Math.round(bitmap.height * ratio))
    const context = canvas.getContext('2d')!
    context.fillStyle = '#fff'; context.fillRect(0, 0, canvas.width, canvas.height)
    context.drawImage(bitmap, 0, 0, canvas.width, canvas.height)
    return { name: name.slice(0, 100), data: canvas.toDataURL('image/jpeg', 0.85), split: 'train', boxes: [] }
  } finally { bitmap.close() }
}
export async function cropRegion(image: VisionImage, box: Box) {
  const bitmap = await createImageBitmap(await (await fetch(image.data)).blob())
  try {
    const canvas = document.createElement('canvas'); canvas.width = canvas.height = 96
    const context = canvas.getContext('2d', { willReadFrequently: true })!
    context.drawImage(bitmap, box.x * bitmap.width, box.y * bitmap.height, box.width * bitmap.width, box.height * bitmap.height, 0, 0, 96, 96)
    const rgba = context.getImageData(0, 0, 96, 96).data
    let bytes = ''
    for (let i = 0; i < rgba.length; i += 4) bytes += String.fromCharCode(rgba[i], rgba[i + 1], rgba[i + 2])
    return btoa(bytes)
  } finally { bitmap.close() }
}
export function validateDataset(input: unknown): VisionImage[] {
  const value = input as { version?: number; images?: VisionImage[] }
  if (value?.version !== 1 || !Array.isArray(value.images) || !value.images.length || value.images.length > 32) throw new Error('Use a Mesh dataset with 1–32 images')
  for (const image of value.images) {
    if (typeof image.name !== 'string' || image.name.length > 100 || typeof image.data !== 'string' || image.data.length > 700_000 || !/^data:image\/jpeg;base64,[A-Za-z0-9+/]+=*$/.test(image.data)
      || !['train', 'validation'].includes(image.split) || !Array.isArray(image.boxes) || image.boxes.length > 16) throw new Error('Invalid dataset image')
    for (const box of image.boxes) {
      if (typeof box.label !== 'string' || !/^[a-zA-Z0-9 _-]{1,40}$/.test(box.label) || !box.label.trim()
        || ![box.x, box.y, box.width, box.height].every(Number.isFinite) || box.x < 0 || box.y < 0 || box.width < 0.01 || box.height < 0.01 || box.x + box.width > 1.000001 || box.y + box.height > 1.000001) throw new Error('Invalid dataset bounding box')
    }
  }
  if (new TextEncoder().encode(JSON.stringify({ version: 1, images: value.images })).length > 8 * 1024 * 1024) throw new Error('Dataset exceeds 8 MiB. Use fewer or smaller images.')
  return value.images
}
export function downloadJson(name: string, value: unknown) {
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' }))
  const anchor = document.createElement('a'); anchor.download = name; anchor.href = url; anchor.click()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
export function sampleDataset(): VisionImage[] {
  return Array.from({ length: 8 }, (_, index) => {
    const canvas = document.createElement('canvas'); canvas.width = canvas.height = 256
    const context = canvas.getContext('2d')!
    context.fillStyle = `rgb(${35 + index},${35 + index},${35 + index})`; context.fillRect(0, 0, 256, 256)
    const label = index % 2 ? 'blue-block' : 'red-block'
    const x = index < 6 ? 0.125 + (index % 3) * 0.125 : 0.25
    const y = index < 6 ? 0.125 : 0.375
    context.fillStyle = index % 2 ? '#2166e8' : '#eb3030'; context.fillRect(x * 256, y * 256, 128, 128)
    return { name: `sample-${index + 1}.jpg`, data: canvas.toDataURL('image/jpeg', 0.85), split: index < 6 ? 'train' : 'validation', boxes: [
      { label, x, y, width: 0.5, height: 0.5 },
      { label: '__background__', x: 0, y: 0, width: 0.1, height: 0.1 },
    ] }
  })
}
