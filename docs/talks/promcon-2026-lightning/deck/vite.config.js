import { defineConfig } from 'vite'
import { viteSingleFile } from 'vite-plugin-singlefile'

// One self-contained dist/index.html: module scripts do not load from file://
// in every browser, and a deck must open from a USB stick with no server.
// assetsInlineLimit is raised so the woff2 fonts referenced from theme.css
// become data URIs instead of sibling files that a copied index.html would lose.
export default defineConfig({
  plugins: [viteSingleFile()],
  build: { target: 'es2020', assetsInlineLimit: 100_000_000, cssCodeSplit: false },
})
