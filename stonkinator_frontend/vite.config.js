import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        //target: 'http://backend:8000',
        target: 'http://localhost:4000',
        changeOrigin: true,
        secure: false
      }
    },
    host: true,
    strictPort: true
  }
});