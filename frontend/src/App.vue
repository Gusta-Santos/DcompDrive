<script setup>
import { ref } from 'vue'
import FileBrowser from './components/FileBrowser.vue'
import ClusterView from './components/ClusterView.vue'

const tab = ref('files')
</script>

<template>
  <header>
    <h1>DComp Drive</h1>
    <nav>
      <button :class="{ active: tab === 'files' }" @click="tab = 'files'">Arquivos</button>
      <button :class="{ active: tab === 'cluster' }" @click="tab = 'cluster'">Cluster</button>
    </nav>
  </header>
  <FileBrowser v-if="tab === 'files'" />
  <ClusterView v-else />
</template>
