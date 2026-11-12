const AVATAR_SIZE = 96

export async function prepareAvatar(file: File): Promise<string> {
  if (!['image/jpeg', 'image/png', 'image/webp'].includes(file.type) || file.size > 8 * 1024 * 1024) {
    throw new Error('Choose a JPEG, PNG, or WebP image up to 8 MiB.')
  }

  const bitmap = await createImageBitmap(file).catch(() => {
    throw new Error('This image could not be opened.')
  })
  try {
    if (bitmap.width * bitmap.height > 25_000_000) throw new Error('Choose an image under 25 megapixels.')
    const canvas = document.createElement('canvas')
    canvas.width = canvas.height = AVATAR_SIZE
    const context = canvas.getContext('2d', { alpha: false })
    if (!context) throw new Error('Image editing is unavailable in this browser.')
    const side = Math.min(bitmap.width, bitmap.height)
    context.fillStyle = '#ffffff'
    context.fillRect(0, 0, AVATAR_SIZE, AVATAR_SIZE)
    context.drawImage(bitmap, (bitmap.width - side) / 2, (bitmap.height - side) / 2, side, side, 0, 0, AVATAR_SIZE, AVATAR_SIZE)
    const jpeg = canvas.toDataURL('image/jpeg', 0.78).split(',')[1]
    if (!jpeg) throw new Error('This image could not be prepared.')
    return jpeg
  } finally {
    bitmap.close()
  }
}
