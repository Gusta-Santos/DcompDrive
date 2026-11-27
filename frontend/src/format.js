export function human(n) {
  if (n === null || n === undefined) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  while (Math.abs(n) >= 1024 && i < units.length - 1) { n /= 1024; i++ }
  return i === 0 ? `${n} B` : `${n.toFixed(1)} ${units[i]}`
}

export function when(ts) {
  return ts ? new Date(ts * 1000).toLocaleString('pt-BR') : '-'
}

export const STATE_CLASS = {
  healthy: 'ok', under_replicated: 'warn', recovering: 'info', unavailable: 'bad', writing: 'info',
  alive: 'ok', suspect: 'warn', dead: 'bad', draining: 'info', drained: 'muted', failed: 'bad',
  disk_failed: 'bad',
}
