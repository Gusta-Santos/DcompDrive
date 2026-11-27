<script setup>
import { onMounted, onUnmounted, ref } from 'vue'
import { api } from '../api'
import { downloadFile, uploadFile } from '../dfsClient'
import { human, when, STATE_CLASS } from '../format'

const cwd = ref('/')
const items = ref([])
const error = ref('')
const notice = ref('')
const info = ref(null)
const replication = ref(null)
const overwrite = ref(false)
const transfers = ref([])   // {name, kind, done, total}
let timer = null

const join = (dir, name) => (dir === '/' ? '' : dir) + '/' + name

function crumbs() {
  const parts = cwd.value.split('/').filter(Boolean)
  return [{ name: 'raiz', path: '/' }, ...parts.map((p, i) => ({ name: p, path: '/' + parts.slice(0, i + 1).join('/') }))]
}

async function load() {
  try {
    const r = await api.get('/api/fs/list', { path: cwd.value })
    items.value = r.items
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}

function go(path) {
  cwd.value = path
  info.value = null
  load()
}

async function act(fn, ok) {
  error.value = ''
  notice.value = ''
  try {
    await fn()
    if (ok) notice.value = ok
  } catch (e) {
    error.value = e.message
  }
  load()
}

function mkdir() {
  const name = prompt('Nome do novo diretório:')
  if (name) act(() => api.post('/api/fs/mkdir', { path: join(cwd.value, name), replication: replication.value || null }))
}

function rename(item) {
  const dst = prompt('Novo caminho (pode mover para outro diretório):', item.path)
  if (dst && dst !== item.path) act(() => api.post('/api/fs/rename', { src: item.path, dst }))
}

function remove(item) {
  if (!confirm(`Remover ${item.path}?`)) return
  if (item.type === 'dir') act(() => api.del('/api/fs/dir', { path: item.path, recursive: true }))
  else act(() => api.del('/api/fs/file', { path: item.path }))
}

async function showInfo(item) {
  try {
    info.value = await api.get('/api/fs/stat', { path: item.path })
  } catch (e) {
    error.value = e.message
  }
}

async function onUpload(ev) {
  const files = [...ev.target.files]
  ev.target.value = ''
  for (const f of files) {
    const t = { name: f.name, kind: 'envio', done: 0, total: f.size }
    transfers.value.push(t)
    await act(async () => {
      const r = await uploadFile(f, join(cwd.value, f.name), {
        replication: replication.value || null,
        overwrite: overwrite.value,
        onProgress: (d) => { t.done = d },
      })
      const warn = [...r.warnings]
      if (r.under_replicated_chunks) warn.push(`${r.under_replicated_chunks} bloco(s) abaixo de N — serão completados em segundo plano`)
      notice.value = `${f.name}: gravado (N=${r.replication}, W=${r.write_quorum}${r.encrypted ? ', cifrado' : ''}). ${warn.join(' ')}`
    })
    transfers.value = transfers.value.filter((x) => x !== t)
  }
}

async function download(item) {
  const t = { name: item.name, kind: 'download', done: 0, total: item.size }
  transfers.value.push(t)
  await act(async () => {
    const { blob } = await downloadFile(item.path, { onProgress: (d) => { t.done = d } })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = item.name
    a.click()
    setTimeout(() => URL.revokeObjectURL(a.href), 5000)
  }, `${item.name}: baixado e hash verificado`)
  transfers.value = transfers.value.filter((x) => x !== t)
}

onMounted(() => {
  load()
  timer = setInterval(load, 3000)   // estados mudam sozinhos (recuperação, falhas)
})
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <div class="panel">
    <div class="toolbar crumbs">
      <template v-for="(c, i) in crumbs()" :key="c.path">
        <span v-if="i" class="muted">/</span>
        <button class="link" @click="go(c.path)">{{ c.name }}</button>
      </template>
    </div>
    <div class="toolbar">
      <label><button @click="$refs.file.click()">Enviar arquivos</button></label>
      <input ref="file" type="file" multiple hidden @change="onUpload" />
      <button @click="mkdir">Novo diretório</button>
      <label>N <input v-model.number="replication" type="number" min="1" placeholder="padrão" /></label>
      <label><input v-model="overwrite" type="checkbox" /> sobrescrever</label>
    </div>
    <div v-for="t in transfers" :key="t.name + t.kind">
      {{ t.kind }} {{ t.name }} — {{ human(t.done) }} / {{ human(t.total) }}
      <div class="progress"><div :style="{ width: (t.total ? (100 * t.done) / t.total : 100) + '%' }"></div></div>
    </div>
    <div v-if="error" class="error">{{ error }}</div>
    <div v-if="notice" class="notice">{{ notice }}</div>

    <div class="scroll">
      <table>
        <thead>
          <tr><th>Nome</th><th>Tamanho</th><th>N</th><th>Estado</th><th class="hide-sm">Modificado</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-if="cwd !== '/'"><td colspan="6"><button class="link" @click="go(cwd.split('/').slice(0, -1).join('/') || '/')">..</button></td></tr>
          <tr v-for="it in items" :key="it.path">
            <td>
              <button v-if="it.type === 'dir'" class="link" @click="go(it.path)">{{ it.name }}/</button>
              <span v-else>{{ it.name }}</span>
            </td>
            <td>{{ it.type === 'file' ? human(it.size) : '' }}</td>
            <td>{{ it.replication || '' }}</td>
            <td>
              <span v-if="it.state" class="badge" :class="STATE_CLASS[it.state]">{{ it.state_label }}</span>
              <span v-if="it.encrypted" class="muted"> 🔒</span>
            </td>
            <td class="hide-sm">{{ when(it.modified_at) }}</td>
            <td style="white-space: nowrap">
              <template v-if="it.type === 'file' && it.state !== 'writing'">
                <button @click="download(it)">Baixar</button>
                <button @click="showInfo(it)">Info</button>
              </template>
              <button @click="rename(it)">Mover</button>
              <button @click="remove(it)">Remover</button>
            </td>
          </tr>
          <tr v-if="!items.length"><td colspan="6" class="muted">diretório vazio</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div v-if="info" class="panel">
    <h2>Metadados de {{ info.path }} <button class="link" @click="info = null">fechar</button></h2>
    <table>
      <tbody>
        <tr><th>Tamanho</th><td>{{ human(info.size) }} ({{ info.size }} bytes)</td></tr>
        <tr><th>Criado / modificado</th><td>{{ when(info.created_at) }} / {{ when(info.modified_at) }}</td></tr>
        <tr><th>Hash ({{ info.hash_alg }})</th><td class="mono">{{ info.hash }}</td></tr>
        <tr><th>Réplicas</th><td>N={{ info.replication }} · mínimo vivo={{ info.live_replicas_min }} · nós: {{ info.replica_nodes.join(', ') }}</td></tr>
        <tr><th>Estado</th><td><span class="badge" :class="STATE_CLASS[info.state]">{{ info.state_label }}</span> {{ info.encrypted ? '· cifrado (AES-256-GCM)' : '' }}</td></tr>
      </tbody>
    </table>
    <h2 style="margin-top: 12px">Blocos ({{ info.chunks.length }})</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>#</th><th>Bloco</th><th>Tamanho</th><th>Vivas</th><th>Localização das réplicas</th></tr></thead>
        <tbody>
          <tr v-for="c in info.chunks" :key="c.chunk_id">
            <td>{{ c.index }}</td>
            <td class="mono">{{ c.chunk_id.slice(0, 12) }}…</td>
            <td>{{ human(c.size) }}</td>
            <td>{{ c.live_replicas }}/{{ info.replication }}</td>
            <td>
              <span v-for="r in c.replicas" :key="r.disk_id" class="badge" :class="STATE_CLASS[r.state]" style="margin-right: 4px">
                {{ r.disk_id }} · {{ r.zone }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
