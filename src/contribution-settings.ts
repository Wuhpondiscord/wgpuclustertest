export type ContributionSettings = { clusterId: string; enabled: boolean; dutyCycle: number; allowCpuFallback: boolean; pauseWhenHidden: boolean }
const TOKENS = 'mesh.node-tokens'
const REMEMBERED = 'mesh.remembered-node-tokens'
const SETTINGS = 'mesh.contribution-settings'

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
    .map(([id, value]) => [id, { ...value, dutyCycle: [10, 25, 50, 100].includes(value.dutyCycle) ? value.dutyCycle : 100, allowCpuFallback: value.allowCpuFallback !== false, pauseWhenHidden: value.pauseWhenHidden === true }]))
}
export function getContributionSettings(id: string) { return allContributionSettings()[id] }
export function saveContributionSettings(id: string, value: ContributionSettings) {
  sessionStorage.setItem(SETTINGS, JSON.stringify({ ...read<ContributionSettings>(sessionStorage, SETTINGS), [id]: value }))
  const persisted = read<ContributionSettings>(localStorage, SETTINGS)
  if (isRemembered(id)) persisted[id] = value
  else delete persisted[id]
  localStorage.setItem(SETTINGS, JSON.stringify(persisted))
}
