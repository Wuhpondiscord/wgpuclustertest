type PendingReply = { resolve: () => void; reject: (error: Error) => void; timer: number }

function base64(bytes: ArrayBuffer) {
  return btoa(String.fromCharCode(...new Uint8Array(bytes)))
}

/** Session keys attest to storage on this authenticated node connection. */
export class HolderCustody {
  private pending = new Map<string, PendingReply>()
  private key = crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify'])
  private nonce = ''
  private publicKey = ''
  private closed = false
  private ready: Promise<void>
  private readyResolve!: () => void
  private readyReject!: (error: Error) => void
  private readyTimer: number

  constructor(private readonly nodeId: string, private readonly socket: WebSocket) {
    this.ready = new Promise<void>((resolve, reject) => { this.readyResolve = resolve; this.readyReject = reject })
    void this.ready.catch(() => undefined)
    void this.key.catch(error => this.readyReject(error instanceof Error ? error : new Error('Holder signing is unavailable')))
    this.readyTimer = window.setTimeout(() => this.readyReject(new Error('The coordinator did not establish a holder signing session')), 15_000)
    socket.addEventListener('message', this.onMessage)
    socket.addEventListener('close', this.close)
  }

  private onMessage = (event: MessageEvent) => {
    let message: { type?: string; nonce?: string; request_id?: string; ok?: boolean; error?: string }
    try { message = JSON.parse(String(event.data)) } catch { return }
    if (message.type === 'custody_welcome' && typeof message.nonce === 'string' && !this.nonce) {
      this.nonce = message.nonce
      void this.initialize().catch(error => this.readyReject(error instanceof Error ? error : new Error('Holder key registration failed')))
    } else if (message.type === 'node_reply' && message.request_id) {
      const pending = this.pending.get(message.request_id)
      if (!pending) return
      window.clearTimeout(pending.timer)
      this.pending.delete(message.request_id)
      if (message.ok) pending.resolve()
      else pending.reject(new Error(message.error ?? 'The coordinator rejected the holder operation'))
    }
  }

  private async initialize() {
    const pair = await this.key
    this.publicKey = base64(await crypto.subtle.exportKey('raw', pair.publicKey))
    await this.request('custody_key', { nonce: this.nonce, public_key: this.publicKey })
    window.clearTimeout(this.readyTimer)
    this.readyResolve()
  }

  request(type: string, fields: Record<string, unknown>, timeout = 12_000): Promise<void> {
    if (this.closed || this.socket.readyState !== WebSocket.OPEN) return Promise.reject(new Error('The authenticated holder connection is closed'))
    if (this.pending.size >= 32) return Promise.reject(new Error('Too many holder operations are pending'))
    const request_id = crypto.randomUUID()
    return new Promise<void>((resolve, reject) => {
      const timer = window.setTimeout(() => { this.pending.delete(request_id); reject(new Error('The coordinator did not confirm the holder operation')) }, timeout)
      this.pending.set(request_id, { resolve, reject, timer })
      try { this.socket.send(JSON.stringify({ type, request_id, ...fields })) }
      catch (error) { window.clearTimeout(timer); this.pending.delete(request_id); reject(error) }
    })
  }

  async attest(name: string, sha256: string, bytes: number) {
    await this.ready
    if (this.closed) throw new Error('The holder signing session is closed')
    const pair = await this.key
    const input = new TextEncoder().encode(JSON.stringify(['mesh-holder-v1', this.nonce, this.nodeId, name, sha256, bytes]))
    const signature = base64(await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, pair.privateKey, input))
    await this.request('custody_receipt', { receipt: {
      node_id: this.nodeId, artifact_name: name, sha256, bytes, nonce: this.nonce,
      public_key: this.publicKey, signature, observed_at: 0,
    } })
  }

  close = () => {
    if (this.closed) return
    this.closed = true
    window.clearTimeout(this.readyTimer)
    this.readyReject(new Error('The holder signing session closed'))
    for (const pending of this.pending.values()) { window.clearTimeout(pending.timer); pending.reject(new Error('The holder connection closed')) }
    this.pending.clear()
    this.socket.removeEventListener('message', this.onMessage)
    this.socket.removeEventListener('close', this.close)
  }
}

