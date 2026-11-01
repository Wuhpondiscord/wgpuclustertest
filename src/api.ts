export class ApiError extends Error {
  constructor(public readonly status: number, message: string) { super(message); this.name = 'ApiError' }
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
  if (!response.ok) throw new ApiError(response.status, (await response.text()) || `Request failed (${response.status})`)
  return response.status === 204 ? undefined as T : response.json() as Promise<T>
}
