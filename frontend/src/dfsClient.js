// Cliente do sistema distribuído rodando NO NAVEGADOR.
// Mesmo protocolo do cliente Python (backend/dfs/client/lib.py):
//   controlador -> só metadados;  nós de armazenamento <- blocos direto do navegador.
import { api } from './api'

const te = new TextEncoder()

function hex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

async function digest(alg, data) {
  const name = alg === 'sha512' ? 'SHA-512' : 'SHA-256'
  return hex(await crypto.subtle.digest(name, data))
}

async function importKey(b64) {
  const raw = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))
  return crypto.subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt'])
}

// formato: nonce(12) || ciphertext || tag(16); chunk_id como associated data
async function encryptChunk(key, chunkId, data) {
  const iv = crypto.getRandomValues(new Uint8Array(12))
  const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: te.encode(chunkId) }, key, data)
  const out = new Uint8Array(12 + ct.byteLength)
  out.set(iv)
  out.set(new Uint8Array(ct), 12)
  return out
}

async function decryptChunk(key, chunkId, blob) {
  const pt = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: blob.slice(0, 12), additionalData: te.encode(chunkId) }, key, blob.slice(12))
  return new Uint8Array(pt)
}

function requireWebCrypto() {
  if (!globalThis.crypto || !crypto.subtle) {
    throw new Error('WebCrypto indisponível: abra a interface via http://localhost (ou https)')
  }
}

// Envia ao 1º destino com o restante como pipeline; se a cabeça falhar, a próxima assume.
async function sendPipeline(targets, chunkId, payload, hash, alg) {
  const failed = []
  for (let i = 0; i < targets.length; i++) {
    const head = targets[i]
    const chain = targets.slice(i + 1).map((t) => ({ node_id: t.node_id, disk_id: t.disk_id, internal_url: t.internal_url }))
    let res
    try {
      res = await fetch(`${head.url}/chunks/${chunkId}?disk=${encodeURIComponent(head.disk_id)}`, {
        method: 'PUT',
        body: payload,
        headers: { 'X-Chunk-Hash': hash, 'X-Hash-Alg': alg, 'X-Pipeline': JSON.stringify(chain) },
      })
    } catch {
      failed.push(head.node_id)
      continue
    }
    const body = await res.json()
    if (res.status === 422) throw new Error(`nó ${head.node_id} rejeitou o bloco: ${body.error}`)
    for (const e of body.errors || []) failed.push(e.node_id)
    return { acks: body.acks || [], failed }
  }
  return { acks: [], failed }
}

async function uploadChunk(sess, ch, plain, key) {
  const payload = key ? await encryptChunk(key, ch.chunk_id, plain) : plain
  const hash = await digest(sess.hash_alg, payload)
  let acks = []
  const excluded = new Set()
  let targets = ch.targets
  for (let attempt = 0; attempt < 4; attempt++) {
    if (targets.length) {
      const r = await sendPipeline(targets, ch.chunk_id, payload, hash, sess.hash_alg)
      const have = new Set(acks.map((a) => a.disk_id))
      acks = acks.concat(r.acks.filter((a) => !have.has(a.disk_id)))
      r.failed.forEach((n) => excluded.add(n))
    }
    if (acks.length >= sess.write_quorum) break
    const alloc = await api.post(`/api/uploads/${sess.upload_id}/allocate`,
      { index: ch.index, exclude_nodes: [...excluded], have: acks })
    targets = alloc.targets
    if (!targets.length) break
  }
  if (acks.length < sess.write_quorum) {
    throw new Error(`bloco ${ch.index}: ${acks.length} réplica(s) confirmada(s), quórum W=${sess.write_quorum}`)
  }
  return { index: ch.index, size: payload.byteLength, hash, replicas: acks }
}

// executa tarefas com no máximo `limit` em paralelo, preservando a ordem do resultado
async function pool(items, limit, fn) {
  const out = new Array(items.length)
  let next = 0
  async function worker() {
    while (next < items.length) {
      const i = next++
      out[i] = await fn(items[i], i)
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))
  return out
}

export async function uploadFile(file, path, { replication = null, overwrite = false, onProgress } = {}) {
  requireWebCrypto()
  const sess = await api.post('/api/uploads', { path, size: file.size, replication, overwrite })
  const key = sess.encryption.enabled ? await importKey(sess.encryption.key) : null
  const renew = setInterval(() => api.post(`/api/uploads/${sess.upload_id}/renew`).catch(() => {}),
    Math.max(1000, (sess.lease_s * 1000) / 3))
  let done = 0
  try {
    // hash do arquivo inteiro (texto claro). Simplificação da interface web: lê o arquivo
    // todo em memória (WebCrypto não tem hash incremental). Para arquivos grandes, use a CLI.
    const fileHash = await digest(sess.hash_alg, await file.arrayBuffer())
    const chunks = await pool(sess.chunks, 3, async (ch) => {
      const start = ch.index * sess.chunk_size
      const plain = new Uint8Array(await file.slice(start, start + sess.chunk_size).arrayBuffer())
      const r = await uploadChunk(sess, ch, plain, key)
      done += plain.byteLength
      onProgress?.(done, file.size)
      return r
    })
    const commit = await api.post(`/api/uploads/${sess.upload_id}/commit`, { file_hash: fileHash, chunks })
    return { ...commit, warnings: sess.warnings, write_quorum: sess.write_quorum, encrypted: !!key }
  } catch (e) {
    api.del(`/api/uploads/${sess.upload_id}`).catch(() => {})
    throw e
  } finally {
    clearInterval(renew)
  }
}

async function fetchChunk(loc, ch, key) {
  const errors = []
  for (const rep of ch.replicas) {
    let res
    try {
      res = await fetch(`${rep.url}/chunks/${ch.chunk_id}?disk=${encodeURIComponent(rep.disk_id)}`)
    } catch {
      errors.push(`${rep.node_id}: inacessível`)
      continue
    }
    if (res.status === 409) { errors.push(`${rep.node_id}: réplica corrompida`); continue }
    if (!res.ok) { errors.push(`${rep.node_id}: HTTP ${res.status}`); continue }
    const data = new Uint8Array(await res.arrayBuffer())
    if ((await digest(loc.hash_alg, data)) !== ch.hash) {
      errors.push(`${rep.node_id}: hash incorreto`)
      api.post('/api/chunks/corrupt', { chunk_id: ch.chunk_id, node_id: rep.node_id, disk_id: rep.disk_id }).catch(() => {})
      continue
    }
    return key ? decryptChunk(key, ch.chunk_id, data) : data
  }
  throw new Error(`bloco ${ch.index} indisponível: ${errors.join('; ')}`)
}

export async function downloadFile(path, { onProgress } = {}) {
  requireWebCrypto()
  const loc = await api.get('/api/fs/locate', { path })
  const key = loc.encrypted ? await importKey(loc.key) : null
  let done = 0
  const parts = await pool(loc.chunks, 4, async (ch) => {
    const data = await fetchChunk(loc, ch, key)
    done += data.byteLength
    onProgress?.(done, loc.size)
    return data
  })
  const blob = new Blob(parts)
  const h = await digest(loc.hash_alg, await blob.arrayBuffer())
  if (h !== loc.file_hash) throw new Error('hash do arquivo completo não confere — download descartado')
  return { blob, hash: h, size: loc.size }
}
