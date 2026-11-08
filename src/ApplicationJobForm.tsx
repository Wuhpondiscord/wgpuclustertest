import { useState } from 'react'
import type { ApplicationInput } from './application-types'

export const quickTestInput: ApplicationInput = { operation: 'vector_add', a: [1, 2, 3], b: [4, 5, 6] }

function numbers(text: string): number[] {
  const values = text.trim().split(/[\s,]+/).filter(Boolean).map(Number)
  if (values.length === 0 || values.some(value => !Number.isFinite(value) || Math.abs(value) > 1e6)) {
    throw new Error('Enter numbers between -1,000,000 and 1,000,000, separated by commas or spaces.')
  }
  return values
}

function matrix(text: string): number[][] {
  const rows = text.trim().split(/\r?\n/).filter(row => row.trim()).map(numbers)
  if (rows.length === 0 || rows.some(row => row.length !== rows[0].length)) {
    throw new Error('Enter one matrix row per line, with the same number of values on each line.')
  }
  return rows
}

export function ApplicationJobForm({ busy, onSubmit }: {
  busy: boolean
  onSubmit: (readInput: () => unknown) => void
}) {
  const [operation, setOperation] = useState<ApplicationInput['operation'] | 'json'>('vector_add')
  const [left, setLeft] = useState('1, 2, 3')
  const [right, setRight] = useState('4, 5, 6')
  const [matrixA, setMatrixA] = useState('1, 2\n3, 4')
  const [matrixB, setMatrixB] = useState('5, 6\n7, 8')
  const [texts, setTexts] = useState('A cat sits on the mat.\nA kitten rests on a rug.')
  const [verification, setVerification] = useState<'single' | 'replicated'>('replicated')
  const [json, setJson] = useState(JSON.stringify(quickTestInput, null, 2))

  function readInput(): unknown {
    if (operation === 'json') return JSON.parse(json)
    if (operation === 'vector_add') {
      const a = numbers(left)
      const b = numbers(right)
      if (a.length !== b.length) throw new Error('The two vectors must have the same number of values.')
      return { operation, a, b }
    }
    if (operation === 'matmul') {
      const a = matrix(matrixA)
      const b = matrix(matrixB)
      if (a[0].length !== b.length) throw new Error('Matrix A needs as many columns as Matrix B has rows.')
      return { operation, a: a.flat(), b: b.flat(), rows: a.length, inner: b.length, cols: b[0].length }
    }
    const lines = texts.split(/\r?\n/).map(text => text.trim()).filter(Boolean)
    if (lines.length < 1 || lines.length > 4) throw new Error('Enter one to four texts, each on its own line.')
    return { operation, model: 'minilm-l6-v2', texts: lines, verification }
  }

  return <form className="application-form" onSubmit={event => {
    event.preventDefault()
    onSubmit(readInput)
  }}>
    <label>Operation template<select value={operation} onChange={event => setOperation(event.target.value as typeof operation)}>
      <option value="vector_add">Vector add</option>
      <option value="matmul">Matrix multiply</option>
      <option value="embedding">Text embeddings (ML)</option>
      <option value="json">Advanced job JSON</option>
    </select></label>
    {operation === 'vector_add' && <>
      <label>Vector A<input value={left} onChange={event => setLeft(event.target.value)} maxLength={32768} required/></label>
      <label>Vector B<input value={right} onChange={event => setRight(event.target.value)} maxLength={32768} required/></label>
      <p>Enter the same number of values in each vector. Separate values with commas or spaces.</p>
    </>}
    {operation === 'matmul' && <>
      <label>Matrix A<textarea rows={3} value={matrixA} onChange={event => setMatrixA(event.target.value)} maxLength={32768} required/></label>
      <label>Matrix B<textarea rows={3} value={matrixB} onChange={event => setMatrixB(event.target.value)} maxLength={32768} required/></label>
      <p>Enter one row per line. Use commas or spaces between numbers.</p>
    </>}
    {operation === 'embedding' && <>
      <label>Text to embed<textarea rows={4} maxLength={8192} value={texts} onChange={event => setTexts(event.target.value)} required/></label>
      <label>Result verification<select value={verification} onChange={event => setVerification(event.target.value as typeof verification)}>
        <option value="replicated">Two nodes must agree</option>
        <option value="single">One node for testing (unverified)</option>
      </select></label>
      <p>Enter up to four texts, one per line. Enable ML inference on the contributing nodes. The browser fetches model files when it first runs an ML job.</p>
    </>}
    {operation === 'json' && <label>Job input JSON<textarea rows={12} maxLength={65536} value={json} onChange={event => setJson(event.target.value)} required/></label>}
    <button className="manage-button primary" disabled={busy}>Queue application job</button>
  </form>
}
