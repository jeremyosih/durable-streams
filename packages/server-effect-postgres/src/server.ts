import { Effect } from "effect"
import { DurableStreamTestServer } from "@durable-streams/server"
import { EffectPostgresStreamStore } from "./store"
import type {
  EffectPostgresDurableStreamServerOptions,
  EffectPostgresResolvedServerOptions,
} from "./types"

export class EffectPostgresDurableStreamServer {
  private readonly server: DurableStreamTestServer
  readonly options: EffectPostgresResolvedServerOptions

  constructor(options: EffectPostgresDurableStreamServerOptions) {
    this.options = {
      host: options.host ?? `127.0.0.1`,
      port: options.port ?? 4437,
      longPollTimeout: options.longPollTimeout ?? 30_000,
      compression: options.compression ?? true,
      cursorIntervalSeconds: options.cursorIntervalSeconds,
      cursorEpoch: options.cursorEpoch,
      databaseUrl: options.databaseUrl,
      schema: options.schema ?? `public`,
      maxConnections: options.maxConnections ?? 10,
      producerStateTtlMs: options.producerStateTtlMs ?? 7 * 24 * 60 * 60 * 1000,
    }

    this.server = new DurableStreamTestServer({
      ...this.options,
      store: new EffectPostgresStreamStore(options),
    })
  }

  async start(): Promise<string> {
    return await Effect.runPromise(Effect.promise(() => this.server.start()))
  }

  async stop(): Promise<void> {
    await Effect.runPromise(Effect.promise(() => this.server.stop()))
  }

  get url(): string {
    return this.server.url
  }

  clear(): void {
    this.server.clear()
  }
}
