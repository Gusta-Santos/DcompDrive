import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Em desenvolvimento, /api e /metrics são encaminhados ao controlador.
// Os blocos vão DIRETO do navegador para os nós (URLs públicas que o controlador informa).
// 127.0.0.1 (e não "localhost"): o Node resolve localhost para IPv6 (::1) e o controlador escuta em IPv4.
const controller = process.env.DFS_CONTROLLER || 'http://127.0.0.1:8000'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': controller,
      '/metrics': controller,
    },
  },
})
