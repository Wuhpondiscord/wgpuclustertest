export type EncodedShard = { index: 0 | 1 | 2; bytes: Uint8Array }

export function encode2Plus1(input: Uint8Array): [EncodedShard, EncodedShard, EncodedShard] {
  if (!input.length || input.length > 8 * 1024 * 1024) throw new Error('Checkpoint must contain 1 byte to 8 MiB.')
  const shardLength = Math.ceil(input.length / 2)
  const dataA = new Uint8Array(shardLength)
  const dataB = new Uint8Array(shardLength)
  dataA.set(input.subarray(0, shardLength))
  dataB.set(input.subarray(shardLength))
  const parity = new Uint8Array(shardLength)
  for (let i = 0; i < shardLength; i++) parity[i] = dataA[i] ^ dataB[i]
  return [
    { index: 0, bytes: dataA },
    { index: 1, bytes: dataB },
    { index: 2, bytes: parity },
  ]
}

export function reconstruct2Plus1(shards: EncodedShard[], logicalBytes: number) {
  if (!Number.isSafeInteger(logicalBytes) || logicalBytes <= 0 || logicalBytes > 8 * 1024 * 1024 || shards.length !== 2) {
    throw new Error('Two valid checkpoint shards are required.')
  }
  const shardLength = Math.ceil(logicalBytes / 2)
  const a = shards.find(shard => shard.index === 0)?.bytes
  const b = shards.find(shard => shard.index === 1)?.bytes
  const p = shards.find(shard => shard.index === 2)?.bytes
  if (new Set(shards.map(shard => shard.index)).size !== 2 || shards.some(shard => shard.bytes.length !== shardLength)) {
    throw new Error('Shard indexes or lengths are invalid.')
  }
  const dataA = a ?? (b && p ? xor(b, p) : null)
  const dataB = b ?? (a && p ? xor(a, p) : null)
  if (!dataA || !dataB) throw new Error('The available shards cannot reconstruct this checkpoint.')
  const result = new Uint8Array(dataA.length + dataB.length)
  result.set(dataA)
  result.set(dataB, dataA.length)
  return result.subarray(0, logicalBytes)
}

function xor(left: Uint8Array, right: Uint8Array) {
  if (left.length !== right.length) throw new Error('Shard sizes do not match.')
  return Uint8Array.from(left, (value, index) => value ^ right[index])
}

export async function sha256Hex(bytes: Uint8Array) {
  const result = await crypto.subtle.digest('SHA-256', bytes.slice().buffer as ArrayBuffer)
  return Array.from(new Uint8Array(result), byte => byte.toString(16).padStart(2, '0')).join('')
}

