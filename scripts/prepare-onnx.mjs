import { mkdir, readdir, copyFile } from 'node:fs/promises'
const source = new URL('../node_modules/onnxruntime-web/dist/', import.meta.url)
const target = new URL('../public/ort/', import.meta.url)
await mkdir(target, { recursive: true })
for (const name of await readdir(source)) {
  if (/^ort-wasm.*\.(mjs|wasm)$/.test(name)) await copyFile(new URL(name, source), new URL(name, target))
}
