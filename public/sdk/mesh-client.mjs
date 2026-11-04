/** Browser/Node client for Mesh's bounded, asynchronous application runtime. */
export class MeshClient {
  constructor({ endpoint, applicationId, token, fetch: fetchImpl = globalThis.fetch }) {
    const url = new URL(endpoint)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error('Use an HTTP(S) coordinator origin')
    if (url.protocol !== 'https:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) throw new Error('Remote API keys require HTTPS')
    if (!/^[a-f0-9-]{36}$/i.test(applicationId) || !/^mak_[a-f0-9]{64}$/.test(token)) throw new Error('Use an application ID and scoped API key')
    this.endpoint = url.origin; this.applicationId = applicationId; this.fetch = fetchImpl.bind(globalThis)
    Object.defineProperty(this, 'token', { value: token, enumerable: false })
  }
  async request(path, { signal, body, idempotencyKey } = {}) {
    const headers = { Authorization: `Bearer ${this.token}` }
    if (body) headers['Content-Type'] = 'application/json'
    if (idempotencyKey) headers['Idempotency-Key'] = idempotencyKey
    const response = await this.fetch(`${this.endpoint}/api/applications/${this.applicationId}${path}`, {
      method: body ? 'POST' : 'GET', headers, signal, credentials: 'omit', cache: 'no-store',
      ...(body ? { body: JSON.stringify(body) } : {}),
    })
    if (!response.ok) {
      const data = await response.json().catch(() => null)
      const error = new Error(data?.source === 'mesh-coordinator' ? data.error : `Mesh request failed (${response.status})`)
      error.status = response.status; throw error
    }
    return response.json()
  }
  submit(input, options = {}) { return this.request('/jobs', { ...options, body: input, idempotencyKey: options.idempotencyKey ?? crypto.randomUUID() }) }
  getJob(id, { signal } = {}) {
    if (!/^[a-f0-9-]{36}$/i.test(id)) throw new Error('Invalid job ID')
    return this.request(`/jobs/${id}`, { signal })
  }
  async wait(job, { signal: parent, timeoutMs = 300_000, pollMs = 1000 } = {}) {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || !Number.isFinite(pollMs) || pollMs < 100) throw new Error('Invalid polling bounds')
    const signal = AbortSignal.any([...(parent ? [parent] : []), AbortSignal.timeout(timeoutMs)])
    const id = typeof job === 'string' ? job : job.id
    for (;;) {
      signal.throwIfAborted()
      const current = await this.getJob(id, { signal })
      if (current.status === 'completed') return current
      if (['failed', 'expired', 'cancelled'].includes(current.status)) throw new Error(current.error || `Mesh job ${current.status}`)
      await new Promise((resolve, reject) => {
        const abort = () => { clearTimeout(timer); reject(signal.reason) }
        const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, pollMs)
        signal.addEventListener('abort', abort, { once: true })
        if (signal.aborted) abort()
      })
    }
  }
  vectorAdd(a, b, options) { return this.submit({ operation: 'vector_add', a: Array.from(a), b: Array.from(b) }, options) }
  matmul({ a, b, rows, inner, cols }, options) { return this.submit({ operation: 'matmul', a: Array.from(a), b: Array.from(b), rows, inner, cols }, options) }
  embeddings(input, options = {}) {
    const { verification = 'replicated', model = 'minilm-l6-v2', ...requestOptions } = options
    return this.request('/inference', { ...requestOptions, body: { model, input: Array.from(input), verification }, idempotencyKey: options.idempotencyKey ?? crypto.randomUUID() })
  }
}
