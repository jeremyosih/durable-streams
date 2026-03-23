import * as fs from "node:fs"
import * as path from "node:path"
import { tmpdir } from "node:os"
import { afterAll, beforeAll, describe } from "vitest"
import { PostgresInstance } from "pg-embedded"
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import { EffectPostgresDurableStreamServer } from "../src"

describe(`Effect Postgres Server Implementation`, () => {
  let embeddedPostgres: PostgresInstance | undefined
  let databaseDir: string | undefined
  let server: EffectPostgresDurableStreamServer | undefined

  const config = { baseUrl: `` }

  beforeAll(async () => {
    const externalDatabaseUrl =
      process.env.DURABLE_STREAMS_TEST_DATABASE_URL ?? process.env.DATABASE_URL

    let databaseUrl = externalDatabaseUrl
    if (!databaseUrl) {
      databaseDir = fs.mkdtempSync(
        path.join(tmpdir(), `durable-streams-postgres-`)
      )
      embeddedPostgres = new PostgresInstance({
        dataDir: databaseDir,
        port: 5432,
        username: `postgres`,
        password: `password`,
        databaseName: `postgres`,
        persistent: false,
      })
      await embeddedPostgres.start()
      await embeddedPostgres.createDatabase(`durable_streams`)
      databaseUrl = `postgres://postgres:password@127.0.0.1:5432/durable_streams`
    }

    server = new EffectPostgresDurableStreamServer({
      databaseUrl: databaseUrl,
      port: 0,
      longPollTimeout: 500,
    })

    await server.start()
    config.baseUrl = server.url
  })

  afterAll(async () => {
    if (server) {
      await server.stop()
    }
    if (embeddedPostgres) {
      await embeddedPostgres.stop()
      await embeddedPostgres.cleanup()
    }
    if (databaseDir) {
      fs.rmSync(databaseDir, { recursive: true, force: true })
    }
  })

  runConformanceTests(config)
})
