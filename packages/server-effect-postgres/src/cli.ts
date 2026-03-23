import { Effect } from "effect"
import { EffectPostgresDurableStreamServer } from "./server"
import type { EffectPostgresCliOptions } from "./types"

export function parseEffectPostgresServerConfigFromEnv(
  env: NodeJS.ProcessEnv
): EffectPostgresCliOptions {
  const parseNumber = (
    key: string,
    value: string | undefined
  ): number | undefined => {
    if (value === undefined || value === ``) return undefined
    const parsed = Number(value)
    if (!Number.isFinite(parsed)) {
      throw new Error(`Invalid ${key}: ${value}`)
    }
    return parsed
  }

  return {
    host: env.HOST ?? `127.0.0.1`,
    port: parseNumber(`PORT`, env.PORT),
    longPollTimeout:
      parseNumber(`LONG_POLL_TIMEOUT_MS`, env.LONG_POLL_TIMEOUT_MS) ?? 30_000,
    compression:
      env.COMPRESSION === undefined
        ? true
        : ![`0`, `false`, `no`].includes(env.COMPRESSION.toLowerCase()),
    databaseUrl:
      env.DATABASE_URL ??
      env.DURABLE_STREAMS_DATABASE_URL ??
      `postgres://postgres:password@127.0.0.1:5432/durable_streams`,
    schema: env.DURABLE_STREAMS_SCHEMA ?? `public`,
    maxConnections: parseNumber(`MAX_CONNECTIONS`, env.MAX_CONNECTIONS),
    producerStateTtlMs: parseNumber(
      `PRODUCER_STATE_TTL_MS`,
      env.PRODUCER_STATE_TTL_MS
    ),
  }
}

function printUsage(): void {
  console.error(`Usage: durable-streams-effect-postgres-server`)
  console.error(``)
  console.error(`Environment variables:`)
  console.error(`  PORT                         Server port (default: 4437)`)
  console.error(`  HOST                         Bind host (default: 127.0.0.1)`)
  console.error(`  DATABASE_URL                 Postgres connection string`)
  console.error(
    `  DURABLE_STREAMS_DATABASE_URL Alternate Postgres connection string`
  )
  console.error(
    `  LONG_POLL_TIMEOUT_MS         Long-poll timeout in milliseconds`
  )
  console.error(`  MAX_CONNECTIONS              Postgres pool size`)
  console.error(
    `  PRODUCER_STATE_TTL_MS        Producer state TTL in milliseconds`
  )
  console.error(``)
}

export async function runEffectPostgresServerCli(): Promise<void> {
  const args = process.argv.slice(2)

  if (args.includes(`--help`) || args.includes(`-h`)) {
    printUsage()
    return
  }

  const options = parseEffectPostgresServerConfigFromEnv(process.env)
  const server = new EffectPostgresDurableStreamServer(options)

  const shutdown = async (signal: string): Promise<void> => {
    console.error(`[durable-streams] Received ${signal}, shutting down...`)
    process.off(`SIGINT`, onSigInt)
    process.off(`SIGTERM`, onSigTerm)
    try {
      await server.stop()
      process.exit(0)
    } catch (error) {
      console.error(`[durable-streams] Shutdown failed`, error)
      process.exit(1)
    }
  }

  const onSigInt = () => {
    void shutdown(`SIGINT`)
  }
  const onSigTerm = () => {
    void shutdown(`SIGTERM`)
  }

  process.on(`SIGINT`, onSigInt)
  process.on(`SIGTERM`, onSigTerm)

  const url = await Effect.runPromise(Effect.promise(() => server.start()))
  console.error(`[durable-streams] Listening on ${url}`)
}
