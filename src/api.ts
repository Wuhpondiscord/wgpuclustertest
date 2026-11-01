export class ApiError extends Error {
  constructor(public readonly status: number, message: string, public readonly coordinator = false) { super(message); this.name = 'ApiError' }
}

export function isRevokedAccess(error: unknown) {
  return error instanceof ApiError && error.coordinator && [401, 403, 404].includes(error.status)
}

export function getIdentity() {
  let token = localStorage.getItem('mesh.identity')
  if (!token) {
    token = `${crypto.randomUUID().replace(/-/g, '')}${crypto.randomUUID().replace(/-/g, '')}`
    localStorage.setItem('mesh.identity', token)
  }
  return token
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('X-Mesh-Identity', getIdentity())
  if (init.body) headers.set('Content-Type', 'application/json')
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    const text = await response.text()
    let message = text || `Request failed (${response.status})`
    let coordinator = false
    if (response.headers.get('Content-Type')?.includes('application/json')) {
      try {
        const body = JSON.parse(text)
        if (body?.source === 'mesh-coordinator' && typeof body.error === 'string') { message = body.error; coordinator = true }
      } catch { /* An intermediary's invalid response is never proof of revocation. */ }
    }
    throw new ApiError(response.status, message, coordinator)
  }
  return response.status === 204 ? undefined as T : response.json() as Promise<T>
}
