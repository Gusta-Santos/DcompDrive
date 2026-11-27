<script setup>
import { onMounted, onUnmounted, ref } from 'vue'
import { api } from '../api'
import { human, when, STATE_CLASS } from '../format'

const st = ref(null)
const events = ref([])
const error = ref('')
let timer = null

async function load() {
  try {
    const [s, e] = await Promise.all([api.get('/api/cluster/status'), api.get('/api/events', { limit: 60 })])
    st.value = s
    events.value = e.events
    error.value = ''
  } catch (e) {
    error.value = `controlador inacessível: ${e.message}`
  }
}

async function drain(n, on) {
  try {
    await api.post(`/api/nodes/${n.id}/${on ? 'drain' : 'undrain'}`)
  } catch (e) {
    error.value = e.message
  }
  load()
}

function details(ev) {
  const skip = new Set(['ts', 'level', 'event', 'component', 'instance'])
  return Object.entries(ev).filter(([k]) => !skip.has(k)).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ')
}

onMounted(() => {
  load()
  timer = setInterval(load, 2000)
})
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <div v-if="error" class="error">{{ error }}</div>
  <template v-if="st">
    <div class="panel">
      <h2>Resumo</h2>
      <div class="cards">
        <div class="card"><div class="v ok">{{ st.nodes_by_state.alive || 0 }}</div><div class="k">nós vivos</div></div>
        <div class="card"><div class="v warn">{{ st.nodes_by_state.suspect || 0 }}</div><div class="k">nós suspeitos</div></div>
        <div class="card"><div class="v bad">{{ st.nodes_by_state.dead || 0 }}</div><div class="k">nós mortos</div></div>
        <div v-for="(v, k) in st.files_by_state" :key="k" class="card">
          <div class="v" :class="STATE_CLASS[k]">{{ v }}</div><div class="k">arquivos {{ st.state_labels[k] }}</div>
        </div>
        <div class="card"><div class="v">{{ st.replication.in_progress.length }}</div><div class="k">re-replicações em andamento</div></div>
        <div class="card"><div class="v">{{ human(st.totals.bytes) }}</div><div class="k">dados lógicos ({{ st.totals.files }} arquivos)</div></div>
      </div>
      <p class="muted">
        N={{ st.config.replication_factor }} · W={{ st.config.write_quorum }} ·
        bloco={{ human(st.config.chunk_size) }} · hash={{ st.config.hash_algorithm }} ·
        cifra={{ st.config.encryption ? 'ligada' : 'desligada' }} ·
        limite de disco={{ st.config.disk_usage_limit * 100 }}% ·
        posicionamento={{ st.config.placement.policy }}
      </p>
    </div>

    <div class="panel">
      <h2>Nós de armazenamento</h2>
      <div class="scroll">
        <table>
          <thead><tr><th>Nó</th><th>Estado</th><th>Zona</th><th>Discos</th><th>Réplicas</th><th>Heartbeat</th><th></th></tr></thead>
          <tbody>
            <tr v-for="n in st.nodes" :key="n.id">
              <td><b>{{ n.id }}</b><div class="muted mono">{{ n.public_url }}</div></td>
              <td><span class="badge" :class="STATE_CLASS[n.label]">{{ n.label }}</span></td>
              <td>{{ n.zone }}</td>
              <td>
                <div v-for="d in n.disks" :key="d.id" style="margin-bottom: 4px">
                  <span :class="STATE_CLASS[d.status]">{{ d.id.split(':')[1] }}</span>
                  {{ human(d.used) }} / {{ human(d.total) }}
                  <div class="bar"><div :class="{ high: d.usage > st.config.disk_usage_limit }" :style="{ width: (d.usage || 0) * 100 + '%' }"></div></div>
                </div>
              </td>
              <td>{{ n.replicas }}</td>
              <td>há {{ n.last_heartbeat_age_s }}s</td>
              <td>
                <button v-if="!n.draining" :disabled="n.state !== 'alive'" @click="drain(n, true)">Drain</button>
                <button v-else @click="drain(n, false)">Cancelar drain</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="grid2">
      <div class="panel">
        <h2>Replicação</h2>
        <p>
          Blocos sub-replicados: <b>{{ st.replication.under_replicated_chunks }}</b> ·
          super-replicados: <b>{{ st.replication.over_replicated_chunks }}</b> ·
          indisponíveis: <b :class="{ bad: st.replication.unavailable_chunks }">{{ st.replication.unavailable_chunks }}</b>
        </p>
        <p>Concluídas: {{ st.replication.completed_total }} · falhas: {{ st.replication.failed_total }} · copiado: {{ human(st.replication.bytes_copied_total) }}</p>
        <table v-if="st.replication.in_progress.length">
          <thead><tr><th>Bloco</th><th>De → para</th><th>Tamanho</th><th>Vivas</th></tr></thead>
          <tbody>
            <tr v-for="t in st.replication.in_progress" :key="t.chunk_id">
              <td class="mono">{{ t.chunk_id.slice(0, 10) }}…</td>
              <td>{{ t.source }} → {{ t.target }}</td>
              <td>{{ human(t.size) }}</td>
              <td>{{ t.live }}/{{ t.wanted }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="st.replication.current_episode" class="info">
          Recuperação em curso desde {{ when(st.replication.current_episode.started) }} (causa: {{ st.replication.current_episode.cause }})
        </p>
        <h2 style="margin-top: 12px">Recuperações concluídas</h2>
        <table>
          <thead><tr><th>Causa</th><th>Duração</th><th>Blocos</th><th>Bytes</th></tr></thead>
          <tbody>
            <tr v-for="ep in [...st.replication.recent_episodes].reverse()" :key="ep.started">
              <td>{{ ep.cause }}</td><td>{{ ep.duration_s }}s</td><td>{{ ep.chunks_copied }}</td><td>{{ human(ep.bytes_copied) }}</td>
            </tr>
            <tr v-if="!st.replication.recent_episodes.length"><td colspan="4" class="muted">nenhuma ainda</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Eventos recentes do controlador</h2>
        <div class="events">
          <table>
            <tbody>
              <tr v-for="(e, i) in events" :key="i">
                <td class="muted" style="white-space: nowrap">{{ e.ts.slice(11, 19) }}</td>
                <td :class="{ warn: e.level === 'WARN', bad: e.level === 'ERROR' }">{{ e.event }}</td>
                <td class="mono">{{ details(e) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </template>
</template>
