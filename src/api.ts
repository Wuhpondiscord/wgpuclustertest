export class ApiError extends Error {
  constructor(public readonly status: number, message: string, public readonly coordinator = false, public readonly code?: string) { super(message); this.name = 'ApiError' }
}

export function isRevokedAccess(error: unknown) {
  return error instanceof ApiError && error.coordinator && error.code !== 'account_session_required' && [401, 403, 404].includes(error.status)
}

let bootstrap: Promise<string> | null = null
export function setIdentity(session: string, username?: string, preserveAnonymous = false) {
  // Keep the actual pre-registration principal across a closed tab until save
  // confirmation. Prefer an existing anonymous key over a pending session.
  if (preserveAnonymous && !localStorage.getItem('mesh.identity')) {
    const previous = sessionStorage.getItem('mesh.account-session')
    if (previous) localStorage.setItem('mesh.identity', previous)
  }
  sessionStorage.setItem('mesh.account-session', session)
  if (!preserveAnonymous) localStorage.removeItem('mesh.identity')
  if (username) localStorage.setItem('mesh.last-account', username)
}
export async function getIdentity(): Promise<string> {
  const token = (typeof sessionStorage !== 'undefined' ? sessionStorage.getItem('mesh.account-session') : null) || localStorage.getItem('mesh.identity')
  if (token) return token
  if (!bootstrap) bootstrap = (async () => {
    const response = await fetch('/api/identities', { method: 'POST', cache: 'no-store' })
    if (!response.ok) throw new ApiError(response.status, 'Could not establish a Mesh identity; retry shortly')
    const value = await response.json() as { key: string }
    localStorage.setItem('mesh.identity', value.key)
    return value.key
  })().finally(() => { bootstrap = null })
  return bootstrap
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (path !== '/api/accounts/login') headers.set('X-Mesh-Identity', await getIdentity())
  if (init.body) headers.set('Content-Type', 'application/json')
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    const text = await response.text()
    let message = text || `Request failed (${response.status})`
    let coordinator = false
    let code: string | undefined
    if (response.headers.get('Content-Type')?.includes('application/json')) {
      try {
        const body = JSON.parse(text)
        if (body?.source === 'mesh-coordinator' && typeof body.error === 'string') { message = body.error; coordinator = true; code = typeof body.code === 'string' ? body.code : undefined }
      } catch { /* An intermediary's invalid response is never proof of revocation. */ }
    }
    if (code === 'account_session_required' && typeof window !== 'undefined' && headers.get('X-Mesh-Identity') === sessionStorage.getItem('mesh.account-session')) window.dispatchEvent(new Event('mesh-authentication-required'))
    throw new ApiError(response.status, message, coordinator, code)
  }
  return response.status === 204 ? undefined as T : response.json() as Promise<T>
}
