export const imageSize = 96
export const modelRepository = 'onnx-community/mobilenetv4_conv_small.e2400_r224_in1k'
export const modelRevision = '3ba07f12712fa58fd6b3d661f9909c9e332c5005'
const modelRoot = `https://huggingface.co/${modelRepository}/resolve/${modelRevision}/`
const assets = new Map([
  [modelRoot + 'config.json', { bytes: 69_895, hash: 'ae99506e66f8e19bc73809e5b4c3ae31fc7c2bca53752dc7cb89c1e47831a335' }],
  [modelRoot + 'preprocessor_config.json', { bytes: 441, hash: '21bb2bccbd790ded22ca9ac77758578bae4d90d8fc578a612097bd38e9d40b2f' }],
  [modelRoot + 'onnx/model.onnx', { bytes: 15_086_122, hash: 'ca5b8dbf490f54e83c5d72f787821565447ea94347935f6b834cf0a4af0e8d37' }],
])

async function verified(response: Response, asset: { bytes: number; hash: string }) {
  if (!response.ok) throw new Error(`Model asset request failed (${response.status})`)
  const reader = response.body?.getReader()
  if (!reader) throw new Error('Model asset is empty')
  const data = new Uint8Array(asset.bytes)
  let offset = 0
  try {
    for (;;) {
      const chunk = await reader.read()
      if (chunk.done) break
      if (offset + chunk.value.length > data.length) throw new Error('Model asset exceeds its pinned size')
      data.set(chunk.value, offset)
      offset += chunk.value.length
    }
  } finally {
    await reader.cancel().catch(() => undefined)
  }
  const hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', data))]
    .map(value => value.toString(16).padStart(2, '0')).join('')
  if (offset !== data.length || hash !== asset.hash) throw new Error('Pinned model asset failed integrity verification')
  return new Response(data, { headers: response.headers })
}

export async function fetchImageModel(input: string | URL, init?: RequestInit) {
  const url = String(input)
  const asset = assets.get(url)
  if (!asset) throw new Error('Image runtime attempted to fetch an unapproved model asset')
  // Transformers uses range probes for metadata. These responses never enter a model session.
  if (new Headers(init?.headers).has('Range') || init?.method === 'HEAD') {
    return fetch(url, { ...init, credentials: 'omit' })
  }
  const cache = await globalThis.caches?.open('mesh-mobilenet-v4-pinned').catch(() => undefined)
  const cached = await cache?.match(url)
  if (cached) {
    try { return await verified(cached, asset) }
    catch { await cache?.delete(url) }
  }
  const response = await verified(await fetch(url, { ...init, credentials: 'omit' }), asset)
  await cache?.put(url, response.clone()).catch(() => undefined)
  return response
}

export async function imageLabels() {
  const response = await fetchImageModel(modelRoot + 'config.json')
  const config = await response.json() as { id2label: Record<string, string> }
  return config.id2label
}

export async function sampleImage() {
  const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/542437c29cc5273cfd29e25c77696f2b349badd7/tiger.jpg'
  return (await verified(await fetch(url, { credentials: 'omit', signal: AbortSignal.timeout(20_000) }), {
    bytes: 62_971, hash: '2ae007fbc16880208ba5fb699e52806cb0eadf41b12fc115cfd92b995b275fd8',
  })).blob()
}
