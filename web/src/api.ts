const BASE = '/api/v1';
const TOKEN_KEY = 'smpp-token';

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(tok: string): void {
  localStorage.setItem(TOKEN_KEY, tok);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export async function login(username: string, password: string): Promise<string> {
  const res = await fetch(`${BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error('Credenciales invalidas');
  const data = await res.json();
  setToken(data.token);
  return data.role as string;
}

export async function apiFetch<T = unknown>(
  path: string,
  opts: RequestInit = {},
  extraHeaders: Record<string, string> = {},
): Promise<T> {
  const headers: Record<string, string> = {
    ...(opts.headers as Record<string, string> | undefined),
    ...extraHeaders,
  };
  const tok = getToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;
  if (opts.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(`${BASE}${path}`, { ...opts, headers });
  if (res.status === 401) {
    clearToken();
    location.hash = '#/login';
    throw new Error('Sesion expirada');
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Error ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
