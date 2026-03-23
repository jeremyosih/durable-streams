#!/usr/bin/env node

import { runEffectPostgresServerCli } from "../dist/cli.js"

await runEffectPostgresServerCli().catch((error) => {
  console.error(`[durable-streams] Failed to start`, error)
  process.exit(1)
})
