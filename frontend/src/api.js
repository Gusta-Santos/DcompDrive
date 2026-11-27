// Chamadas ao CONTROLADOR (plano de controle). Caminho relativo: o Vite (dev) ou o
// nginx (Docker) encaminha /api para o controlador.
async function request(method, url, { body, params } = {}) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : ''
  const res = await fetch(url + qs, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  let data = null
  try { data = await res.json() } catch { /* sem corpo */ }
  if (!res.ok) throw new Error((data && (data.error || JSON.stringify(data.detail))) || `HTTP ${res.status}`)
  return data
}

export const api = {
  get: (url, params) => request('GET', url, { params }),
  post: (url, body) => request('POST', url, { body }),
  del: (url, params) => request('DELETE', url, { params }),
}
