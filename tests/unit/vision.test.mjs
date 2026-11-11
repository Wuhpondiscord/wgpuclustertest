import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import ts from 'typescript'
const source = await readFile(new URL('../../src/vision-detector.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
const { fitDetector, detect, validateDetector, regionWindows, evaluateDetections, normalize } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
const vector = index => Array.from({ length: 1000 }, (_, i) => i === index ? 10 : 0)
const examples = ['__background__', 'cat', 'dog'].flatMap((label, index) => [{ label, features: vector(index) }, { label, features: vector(index).map(value => value * 2 + 10) }])
test('detector fitting exports a portable normalized head and rejects missing background or invalid features', () => {
  const model = fitDetector(examples)
  assert.deepEqual(model.counts, [2, 2, 2])
  assert.deepEqual(validateDetector(JSON.parse(JSON.stringify(model))), model)
  assert.throws(() => fitDetector(examples.filter(example => example.label !== '__background__')))
  assert.throws(() => normalize(Array(1000).fill(1)))
  assert.throws(() => normalize([NaN]))
  assert.throws(() => validateDetector({ ...model, backbone_revision: 'unknown' }))
  assert.throws(() => validateDetector({ ...model, prototypes: model.prototypes.map(() => 'AAAA') }))
})
test('region detection rejects background and suppresses duplicate boxes without losing separate objects', () => {
  const model = fitDetector(examples)
  const box = { x: 0, y: 0, width: 0.4, height: 0.4 }
  const found = detect(model, [
    { ...box, features: vector(1) }, { ...box, x: 0.01, features: vector(1) },
    { ...box, x: 0.6, features: vector(1) }, { ...box, y: 0.6, features: vector(0) },
  ])
  assert.equal(found.length, 2)
  assert.ok(found.every(box => box.label === 'cat'))
  const report = evaluateDetections([found], [[{ ...box, label: 'cat' }]])
  assert.equal(report.precision, 0.5)
  assert.equal(report.recall, 1)
  assert.equal(evaluateDetections([[]], [[{ ...box, label: 'cat' }]]).recall, 0)
})
test('detection windows are bounded and evaluation matches each ground-truth object only once', () => {
  const windows = regionWindows()
  assert.equal(windows.length, 35)
  assert.ok(windows.every(box => box.x >= 0 && box.y >= 0 && box.x + box.width <= 1 && box.y + box.height <= 1))
  const box = { ...windows[0], label: 'cat', score: 1 }
  assert.deepEqual(evaluateDetections([[box, box]], [[box]]), { matched: 1, predictions: 2, objects: 1, precision: 0.5, recall: 1 })
})

const dataSource = await readFile(new URL('../../src/vision-data.ts', import.meta.url), 'utf8')
const dataModule = ts.transpileModule(dataSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
const { validateDataset } = await import(`data:text/javascript;base64,${Buffer.from(dataModule).toString('base64')}`)
test('dataset imports reject external image URLs, invalid boxes and non-roundtrippable oversized exports', () => {
  const sample = { name: 'local.jpg', data: 'data:image/jpeg;base64,AAAA', split: 'train', boxes: [{ label: 'cat', x: 0, y: 0, width: 0.5, height: 0.5 }] }
  assert.equal(validateDataset({ version: 1, images: [sample] }).length, 1)
  assert.throws(() => validateDataset({ version: 1, images: [{ ...sample, data: 'https://example.com/image.jpg' }] }))
  assert.throws(() => validateDataset({ version: 1, images: [{ ...sample, boxes: [{ ...sample.boxes[0], x: 0.8 }] }] }))
  assert.throws(() => validateDataset({ version: 1, images: Array(14).fill({ ...sample, data: 'data:image/jpeg;base64,' + 'A'.repeat(650000) }) }), /8 MiB/)
})
