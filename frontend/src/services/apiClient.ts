export class ApiError extends Error {
  readonly status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? ''

export function apiUrl(path: string) {
  return `${API_BASE_URL.replace(/\/$/, '')}${path}`
}

async function requestJson<T>(path: string, init: RequestInit): Promise<T> {
  let response: Response
  try {
    response = await fetch(apiUrl(path), init)
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new ApiError('Cannot reach the MergeMind API. Start the FastAPI service and retry.', 0)
  }

  const payload: unknown = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = typeof payload === 'object' && payload !== null && 'detail' in payload
      ? String(payload.detail)
      : `The API request failed with status ${response.status}.`
    throw new ApiError(detail, response.status)
  }
  return payload as T
}

export function apiGet<T>(path: string, signal?: AbortSignal): Promise<T> {
  return requestJson<T>(path, { method: 'GET', headers: { Accept: 'application/json' }, signal })
}

export function apiPost<T>(path: string, body: unknown, headers: Record<string, string> = {}): Promise<T> {
  return requestJson<T>(path, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
}

export function apiUpload<T>(path: string, entityType: 'customer' | 'product', file: File): Promise<T> {
  const form = new FormData()
  form.set('entity_type', entityType)
  form.set('file', file)
  return requestJson<T>(path, { method: 'POST', headers: { Accept: 'application/json' }, body: form })
}
