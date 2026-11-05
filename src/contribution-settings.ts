export type ContributionSettings = { clusterId: string; enabled: boolean; dutyCycle: number; allowCpuFallback: boolean; pauseWhenHidden: boolean; allowInference: boolean }
const TOKENS = 'mesh.node-tokens'
const REMEMBERED = 'mesh.remembered-node-tokens'
const SETTINGS = 'mesh.contribution-settings'
const CHANNEL = 'mesh.contribution-controls'

function read<T>(storage: Storage, key: string): Record<string, T> {
  try { const value: unknown = JSON.parse(storage.getItem(key) ?? '{}'); return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, T> : {} }
  catch { return {} }
}
export function getNodeTokens(): Record<string, string> {
  return Object.fromEntries(Object.entries({ ...read<string>(localStorage, REMEMBERED), ...read<string>(sessionStorage, TOKENS) })
    .filter(([id, token]) => /^[a-f0-9-]{36}$/i.test(id) && typeof token === 'string' && /^[a-f0-9]{64}$/i.test(token)))
}
export function setNodeToken(id: string, token: string) {
  sessionStorage.setItem(TOKENS, JSON.stringify({ ...read<string>(sessionStorage, TOKENS), [id]: token }))
}
export function isRemembered(id: string) { return Boolean(read<string>(localStorage, REMEMBERED)[id]) }
export function rememberNode(id: string, remember: boolean) {
  const tokens = read<string>(localStorage, REMEMBERED)
  const token = getNodeTokens()[id]
  if (token) setNodeToken(id, token)
  if (remember && token) tokens[id] = token
  else delete tokens[id]
  localStorage.setItem(REMEMBERED, JSON.stringify(tokens))
  const settings = getContributionSettings(id)
  if (settings) saveContributionSettings(id, settings)
}
export function forgetNode(id: string) {
  for (const [storage, key] of [[localStorage, REMEMBERED], [sessionStorage, TOKENS], [localStorage, SETTINGS], [sessionStorage, SETTINGS]] as const) {
    const values = read<unknown>(storage, key); delete values[id]; storage.setItem(key, JSON.stringify(values))
  }
}
export function allContributionSettings(): Record<string, ContributionSettings> {
  const settings = { ...read<ContributionSettings>(localStorage, SETTINGS), ...read<ContributionSettings>(sessionStorage, SETTINGS) }
  return Object.fromEntries(Object.entries(settings).filter(([, value]) => value && typeof value.clusterId === 'string' && typeof value.enabled === 'boolean')
    .map(([id, value]) => [id, { ...value, dutyCycle: [10, 25, 50, 100].includes(value.dutyCycle) ? value.dutyCycle : 100, allowCpuFallback: value.allowCpuFallback !== false, pauseWhenHidden: value.pauseWhenHidden === true, allowInference: value.allowInference === true }]))
}
export function getContributionSettings(id: string) { return allContributionSettings()[id] }
export function saveContributionSettings(id: string, value: ContributionSettings) {
  sessionStorage.setItem(SETTINGS, JSON.stringify({ ...read<ContributionSettings>(sessionStorage, SETTINGS), [id]: value }))
  const persisted = read<ContributionSettings>(localStorage, SETTINGS)
  if (isRemembered(id)) persisted[id] = value
  else delete persisted[id]
  localStorage.setItem(SETTINGS, JSON.stringify(persisted))
  // Session-only nodes can exist in duplicated tabs too. Broadcast controls
  // without persisting a credential or opt-in that the user did not remember.
  if (typeof BroadcastChannel !== 'undefined') {
    const channel = new BroadcastChannel(CHANNEL)
    channel.postMessage({ id, value }); channel.close()
  }
}

export function subscribeContributionSettings(changed: (id: string, value: ContributionSettings) => void) {
  const accept = (id: unknown, value: unknown) => {
    if (typeof id !== 'string' || !getNodeTokens()[id] || !value || typeof value !== 'object') return
    const item = value as Partial<ContributionSettings>
    if (typeof item.clusterId !== 'string' || typeof item.enabled !== 'boolean') return
    const normalized: ContributionSettings = { clusterId: item.clusterId, enabled: item.enabled,
      dutyCycle: [10, 25, 50, 100].includes(item.dutyCycle ?? 0) ? item.dutyCycle! : 100,
      allowCpuFallback: item.allowCpuFallback !== false, pauseWhenHidden: item.pauseWhenHidden === true, allowInference: item.allowInference === true }
    // Replace this tab's stale session override without writing/broadcasting back.
    sessionStorage.setItem(SETTINGS, JSON.stringify({ ...read<ContributionSettings>(sessionStorage, SETTINGS), [id]: normalized }))
    changed(id, normalized)
  }
  const storageChanged = (event: StorageEvent) => {
    if (event.storageArea !== localStorage || event.key !== SETTINGS || !event.newValue) return
    try { for (const [id, value] of Object.entries(JSON.parse(event.newValue))) accept(id, value) } catch { /* Ignore malformed external storage. */ }
  }
  window.addEventListener('storage', storageChanged)
  const channel = typeof BroadcastChannel !== 'undefined' ? new BroadcastChannel(CHANNEL) : null
  if (channel) channel.onmessage = event => accept(event.data?.id, event.data?.value)
  return () => { window.removeEventListener('storage', storageChanged); channel?.close() }
}
