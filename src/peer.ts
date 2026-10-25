const MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
const CHUNK_BYTES = 16 * 1024
const MAX_FRAME_CHARS = 24 * 1024
const BUFFER_LIMIT_BYTES = 256 * 1024
const MAX_CONCURRENT_TRANSFERS = 2

type SignalPayload = { description?: RTCSessionDescriptionInit; candidate?: RTCIceCandidateInit }
type TransferStart = { type: 'artifact_start'; transfer_id: string; name: string; total_bytes: number; chunks: number; sha256: string }
type TransferChunk = { type: 'artifact_chunk'; transfer_id: string; index: number; data: string }
type IncomingTransfer = { peerId: string; start: TransferStart; chunks: Map<number, Uint8Array>; bytes: number }
type PendingTransfer = { peerId: string; resolve: (digest: string) => void; reject: (error: Error) => void; timer: number }
type PendingArtifactRead = { peerId: string; name: string; resolve: (bytes: Uint8Array) => void; reject: (error: Error) => void; timer: number }

function encode(bytes: Uint8Array) {
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 0x8000) binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000))
  return btoa(binary)
}

function decode(base64: string): Uint8Array {
  const binary = atob(base64)
  const result = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) result[i] = binary.charCodeAt(i)
  return result
}

async function digest(bytes: Uint8Array) {
  const copy = bytes.slice().buffer
  const result = await crypto.subtle.digest('SHA-256', copy)
  return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, '0')).join('')
}

async function persistArtifact(key: string, value: { name: string; bytes: Uint8Array; sha256: string; receivedAt: number }) {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open('mesh-peer-artifacts', 1)
    request.onupgradeneeded = () => request.result.createObjectStore('artifacts')
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('Could not open local artifact storage'))
  })
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = db.transaction('artifacts', 'readwrite')
      transaction.objectStore('artifacts').put(value, key)
      transaction.oncomplete = () => resolve()
      transaction.onerror = () => reject(transaction.error ?? new Error('Could not persist received artifact'))
      transaction.onabort = () => reject(transaction.error ?? new Error('Artifact persistence was aborted'))
    })
  } finally { db.close() }
}

async function loadArtifact(name: string): Promise<Uint8Array | null> {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open('mesh-peer-artifacts', 1)
    request.onupgradeneeded = () => request.result.createObjectStore('artifacts')
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('Could not open local artifact storage'))
  })
  try {
    return await new Promise<Uint8Array | null>((resolve, reject) => {
      const request = db.transaction('artifacts', 'readonly').objectStore('artifacts').get(`artifact:${name}`)
      request.onsuccess = () => resolve((request.result as { bytes?: Uint8Array } | undefined)?.bytes ?? null)
      request.onerror = () => reject(request.error ?? new Error('Could not read local artifact storage'))
    })
  } finally { db.close() }
}

export class PeerMeshNode {
  private peers = new Map<string, RTCPeerConnection>()
  private channels = new Map<string, RTCDataChannel>()
  private incoming = new Map<string, IncomingTransfer>()
  private pending = new Map<string, PendingTransfer>()
  private pendingArtifactReads = new Map<string, PendingArtifactRead>()
  private closed = false

  constructor(
    private readonly nodeId: string,
    private readonly signal: (peerNodeId: string, payload: SignalPayload) => void,
    private readonly onNotice: (message: string) => void,
  ) {}

  addPeers(peerNodeIds: string[]) {
    for (const peerId of peerNodeIds) this.considerPeer(peerId)
  }

  peerJoined(peerId: string) {
    if (this.peers.has(peerId)) {
      this.peers.get(peerId)?.close()
      this.peers.delete(peerId)
      this.channels.delete(peerId)
    }
    this.considerPeer(peerId)
  }

  peerLeft(peerId: string) {
    this.peers.get(peerId)?.close()
    this.peers.delete(peerId)
    this.channels.delete(peerId)
    for (const [transferId, transfer] of this.incoming) if (transfer.peerId === peerId) this.incoming.delete(transferId)
    for (const [transferId, pending] of this.pending) if (pending.peerId === peerId) {
      window.clearTimeout(pending.timer)
      this.pending.delete(transferId)
      pending.reject(new Error('Peer disconnected during artifact transfer'))
    }
    for (const [key, pending] of this.pendingArtifactReads) if (pending.peerId === peerId) {
      window.clearTimeout(pending.timer)
      this.pendingArtifactReads.delete(key)
      pending.reject(new Error('Peer disconnected while a shard was being requested'))
    }
  }

  private considerPeer(peerId: string) {
    if (peerId === this.nodeId || this.closed || this.peers.has(peerId)) return
    const initiates = this.nodeId.localeCompare(peerId) < 0
    const peer = this.createPeer(peerId, initiates)
    if (initiates) void peer.createOffer().then(async offer => {
      await peer.setLocalDescription(offer)
      this.signal(peerId, { description: peer.localDescription ?? offer })
    }).catch(() => this.onNotice('Could not negotiate a direct peer connection'))
  }

  async handleSignal(peerId: string, payload: SignalPayload) {
    if (this.closed || peerId === this.nodeId) return
    const peer = this.peers.get(peerId) ?? this.createPeer(peerId, false)
    try {
      if (payload.description?.type === 'offer') {
        await peer.setRemoteDescription(payload.description)
        const answer = await peer.createAnswer()
        await peer.setLocalDescription(answer)
        this.signal(peerId, { description: peer.localDescription ?? answer })
      } else if (payload.description?.type === 'answer') {
        await peer.setRemoteDescription(payload.description)
      } else if (payload.candidate) {
        await peer.addIceCandidate(payload.candidate)
      }
    } catch { this.onNotice('Peer negotiation failed; the node will retry when it reconnects') }
  }

  private createPeer(peerId: string, initiator: boolean) {
    const peer = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] })
    this.peers.set(peerId, peer)
    peer.onicecandidate = event => { if (event.candidate) this.signal(peerId, { candidate: event.candidate.toJSON() }) }
    peer.onconnectionstatechange = () => {
      if ((peer.connectionState === 'failed' || peer.connectionState === 'closed') && this.peers.get(peerId) === peer) this.peerLeft(peerId)
    }
    peer.ondatachannel = event => this.bindChannel(peerId, event.channel)
    if (initiator) this.bindChannel(peerId, peer.createDataChannel('mesh-artifacts', { ordered: true }))
    return peer
  }

  private bindChannel(peerId: string, channel: RTCDataChannel) {
    channel.binaryType = 'arraybuffer'
    channel.bufferedAmountLowThreshold = 64 * 1024
    this.channels.set(peerId, channel)
    channel.onmessage = event => { void this.handleDataMessage(peerId, channel, event.data) }
    channel.onclose = () => { if (this.channels.get(peerId) === channel) this.channels.delete(peerId) }
  }

  private async handleDataMessage(peerId: string, channel: RTCDataChannel, data: unknown) {
    if (typeof data !== 'string' || data.length > MAX_FRAME_CHARS) return
    let message: Record<string, unknown>
    try { message = JSON.parse(data) as Record<string, unknown> } catch { return }
    if (message.type === 'artifact_ack' && typeof message.transfer_id === 'string' && typeof message.sha256 === 'string') {
      const pending = this.pending.get(message.transfer_id)
      if (pending?.peerId === peerId) { window.clearTimeout(pending.timer); this.pending.delete(message.transfer_id); pending.resolve(message.sha256) }
      return
    }
    if (message.type === 'artifact_get' && typeof message.name === 'string' && message.name.length > 0 && message.name.length <= 128) {
      try {
        const artifact = await loadArtifact(message.name)
        if (!artifact) { channel.send(JSON.stringify({ type: 'artifact_missing', name: message.name })); return }
        void this.sendArtifact(peerId, message.name, artifact, false).catch(() => undefined)
      } catch { channel.send(JSON.stringify({ type: 'artifact_missing', name: message.name })) }
      return
    }
    if (message.type === 'artifact_missing' && typeof message.name === 'string') {
      const key = `${peerId}:${message.name}`
      const pending = this.pendingArtifactReads.get(key)
      if (pending) { window.clearTimeout(pending.timer); this.pendingArtifactReads.delete(key); pending.reject(new Error(`Peer ${peerId} no longer has shard “${message.name}”`)) }
      return
    }
    if (message.type === 'artifact_start') {
      const start = message as unknown as TransferStart
      if (typeof start.transfer_id !== 'string' || start.transfer_id.length > 64 || !start.transfer_id || typeof start.name !== 'string' || !start.name || start.name.length > 128 || !Number.isSafeInteger(start.total_bytes) || start.total_bytes <= 0 || start.total_bytes > MAX_ARTIFACT_BYTES
        || !Number.isSafeInteger(start.chunks) || start.chunks < 0 || start.chunks > Math.ceil(MAX_ARTIFACT_BYTES / CHUNK_BYTES)
        || !/^[a-f0-9]{64}$/i.test(start.sha256) || start.chunks !== Math.ceil(start.total_bytes / CHUNK_BYTES)) return
      if (this.incoming.size >= MAX_CONCURRENT_TRANSFERS && !this.incoming.has(start.transfer_id)) return
      this.incoming.set(start.transfer_id, { peerId, start, chunks: new Map(), bytes: 0 })
      return
    }
    if (message.type === 'artifact_chunk' && typeof message.transfer_id === 'string' && typeof message.data === 'string') {
      const index = Number(message.index)
      if (!Number.isSafeInteger(index)) return
      const transfer = this.incoming.get(message.transfer_id)
      if (!transfer || index < 0 || index >= transfer.start.chunks || transfer.chunks.has(index)) return
      let bytes: Uint8Array
      try { bytes = decode(message.data) } catch { this.incoming.delete(message.transfer_id); return }
      if (bytes.length > CHUNK_BYTES || transfer.bytes + bytes.length > transfer.start.total_bytes) { this.incoming.delete(message.transfer_id); return }
      transfer.chunks.set(index, bytes)
      transfer.bytes += bytes.length
      if (transfer.chunks.size !== transfer.start.chunks) return
      const result = new Uint8Array(transfer.bytes)
      let offset = 0
      for (let index = 0; index < transfer.start.chunks; index++) {
        const chunk = transfer.chunks.get(index)
        if (!chunk) { this.incoming.delete(message.transfer_id); return }
        result.set(chunk, offset); offset += chunk.length
      }
      const actualHash = await digest(result)
      if (result.length !== transfer.start.total_bytes || actualHash !== transfer.start.sha256.toLowerCase()) {
        this.incoming.delete(message.transfer_id)
        channel.send(JSON.stringify({ type: 'artifact_nack', transfer_id: message.transfer_id, reason: 'hash_mismatch' }))
        return
      }
      try {
        await persistArtifact(`artifact:${transfer.start.name}`, { name: transfer.start.name, bytes: result, sha256: actualHash, receivedAt: Date.now() })
        channel.send(JSON.stringify({ type: 'artifact_ack', transfer_id: message.transfer_id, sha256: actualHash }))
        this.onNotice(`Verified peer artifact “${transfer.start.name}” (${result.length.toLocaleString()} bytes)`)
        const readKey = `${peerId}:${transfer.start.name}`
        const pendingRead = this.pendingArtifactReads.get(readKey)
        if (pendingRead) { window.clearTimeout(pendingRead.timer); this.pendingArtifactReads.delete(readKey); pendingRead.resolve(result) }
      } catch { channel.send(JSON.stringify({ type: 'artifact_nack', transfer_id: message.transfer_id, reason: 'storage_failed' })) }
      this.incoming.delete(message.transfer_id)
    }
    if (message.type === 'artifact_nack' && typeof message.transfer_id === 'string') {
      const pending = this.pending.get(message.transfer_id)
      if (pending?.peerId === peerId) { window.clearTimeout(pending.timer); this.pending.delete(message.transfer_id); pending.reject(new Error('Peer rejected the artifact: integrity check or storage failed')) }
    }
  }

  async sendArtifact(peerId: string, name: string, bytes: Uint8Array, persistLocally = true) {
    if (!name || name.length > 128) throw new Error('Peer artifact names must contain 1–128 characters')
    if (!bytes.length) throw new Error('Peer artifacts must contain at least one byte')
    if (bytes.length > MAX_ARTIFACT_BYTES) throw new Error('Peer artifact exceeds the 8 MiB transfer limit')
    const deadline = Date.now() + 20_000
    let channel = this.channels.get(peerId)
    while ((!channel || channel.readyState !== 'open') && Date.now() < deadline) {
      await new Promise(resolve => window.setTimeout(resolve, 100))
      channel = this.channels.get(peerId)
    }
    if (!channel || channel.readyState !== 'open') throw new Error('Direct peer channel did not become ready; the network may block peer connections')
    const transferId = crypto.randomUUID()
    const sha256 = await digest(bytes)
    const chunks = Math.ceil(bytes.length / CHUNK_BYTES)
    const acknowledged = new Promise<string>((resolve, reject) => {
      const timer = window.setTimeout(() => { this.pending.delete(transferId); reject(new Error('Peer did not confirm the artifact before timeout')) }, 30_000)
      this.pending.set(transferId, { peerId, resolve, reject, timer })
    })
    try {
      await this.sendFrame(channel, { type: 'artifact_start', transfer_id: transferId, name, total_bytes: bytes.length, chunks, sha256 } satisfies TransferStart)
      for (let index = 0; index < chunks; index++) {
        const part = bytes.subarray(index * CHUNK_BYTES, Math.min(bytes.length, (index + 1) * CHUNK_BYTES))
        await this.sendFrame(channel, { type: 'artifact_chunk', transfer_id: transferId, index, data: encode(part) } satisfies TransferChunk)
      }
    } catch (error) {
      const pending = this.pending.get(transferId)
      if (pending) { window.clearTimeout(pending.timer); this.pending.delete(transferId) }
      throw error
    }
    const receivedHash = await acknowledged
    if (receivedHash !== sha256) throw new Error('Peer acknowledged a different artifact digest')
    if (persistLocally) await persistArtifact(`artifact:${name}`, { name, bytes: bytes.slice(), sha256, receivedAt: Date.now() })
    return { transferId, sha256, bytes: bytes.length }
  }

  async storeArtifact(name: string, bytes: Uint8Array) {
    if (!name || name.length > 128 || !bytes.length || bytes.length > MAX_ARTIFACT_BYTES) throw new Error('Local shard is outside artifact limits')
    const sha256 = await digest(bytes)
    await persistArtifact(`artifact:${name}`, { name, bytes: bytes.slice(), sha256, receivedAt: Date.now() })
    return { sha256, bytes: bytes.length }
  }

  async getArtifact(peerId: string, name: string) {
    const local = await loadArtifact(name)
    if (local) return local
    if (peerId === this.nodeId) throw new Error(`This node no longer has shard “${name}”`)
    const deadline = Date.now() + 20_000
    let channel = this.channels.get(peerId)
    while ((!channel || channel.readyState !== 'open') && Date.now() < deadline) {
      await new Promise(resolve => window.setTimeout(resolve, 100))
      channel = this.channels.get(peerId)
    }
    if (!channel || channel.readyState !== 'open') throw new Error(`Peer ${peerId} is not connected`)
    const key = `${peerId}:${name}`
    if (this.pendingArtifactReads.has(key)) throw new Error('A request for this shard is already pending')
    const result = new Promise<Uint8Array>((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pendingArtifactReads.delete(key)
        reject(new Error(`Timed out requesting shard “${name}” from peer ${peerId}`))
      }, 30_000)
      this.pendingArtifactReads.set(key, { peerId, name, resolve, reject, timer })
    })
    try { await this.sendFrame(channel, { type: 'artifact_get', name }) }
    catch (error) {
      const pending = this.pendingArtifactReads.get(key)
      if (pending) { window.clearTimeout(pending.timer); this.pendingArtifactReads.delete(key) }
      throw error
    }
    return result
  }

  private async sendFrame(channel: RTCDataChannel, message: unknown) {
    const frame = JSON.stringify(message)
    if (frame.length > MAX_FRAME_CHARS) throw new Error('Peer transfer frame exceeds its size limit')
    while (channel.bufferedAmount > BUFFER_LIMIT_BYTES) {
      await new Promise<void>((resolve, reject) => {
        const timeout = window.setTimeout(() => reject(new Error('Peer data channel remained congested')), 15_000)
        channel.addEventListener('bufferedamountlow', () => { window.clearTimeout(timeout); resolve() }, { once: true })
        if (channel.readyState !== 'open') { window.clearTimeout(timeout); reject(new Error('Peer disconnected during transfer')) }
      })
    }
    if (channel.readyState !== 'open') throw new Error('Peer disconnected during transfer')
    channel.send(frame)
  }

  close() {
    this.closed = true
    for (const peer of this.peers.values()) peer.close()
    this.peers.clear(); this.channels.clear(); this.incoming.clear()
    for (const pending of this.pending.values()) { window.clearTimeout(pending.timer); pending.reject(new Error('Node connection closed')) }
    this.pending.clear()
    for (const pending of this.pendingArtifactReads.values()) {
      window.clearTimeout(pending.timer)
      pending.reject(new Error('Node connection closed while reading a shard'))
    }
    this.pendingArtifactReads.clear()
  }
}

