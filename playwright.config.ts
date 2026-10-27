import { defineConfig } from '@playwright/test'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const baseURL = 'http://127.0.0.1:5173'

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 180_000,
  expect: { timeout: 20_000 },
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL,
    browserName: 'chromium',
    headless: true,
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
    launchOptions: { args: ['--disable-features=WebRtcHideLocalIpsWithMdns'] },
  },
  webServer: [
    {
      command: 'cargo run --manifest-path server/Cargo.toml --bin mesh-coordinator',
      url: 'http://127.0.0.1:7860/health',
      timeout: 180_000,
      reuseExistingServer: !process.env.CI,
      env: { PORT: '7860', MESH_DATA_FILE: join(tmpdir(), `mesh-e2e-${process.pid}.json`), MESH_REPAIR_GRACE_SECS: '1' },
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 5173 --strictPort',
      url: baseURL,
      timeout: 60_000,
      reuseExistingServer: !process.env.CI,
    },
  ],
})

