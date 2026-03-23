import { Effect } from "effect"
import { DurableStreamTestServer } from "@durable-streams/server"
import { EffectPostgresStreamStore } from "./store"
import type { EffectPostgresDurableStreamServerOptions } from "./types"

export class EffectPostgresDurableStreamServer {
  private readonly server: DurableStreamTestServer

  constructor(options: EffectPostgresDurableStreamServerOptions) {
    this.server = new DurableStreamTestServer({
      ...options,
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
