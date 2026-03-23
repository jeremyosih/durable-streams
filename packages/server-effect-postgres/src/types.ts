import type {
  DurableStreamStore,
  StreamLifecycleHook,
  TestServerOptions,
} from "@durable-streams/server"

export interface EffectPostgresStoreOptions {
  /**
   * Postgres connection string. When omitted, PG* environment variables are used.
   */
  databaseUrl?: string
  /**
   * Logical schema prefix for the durable streams tables.
   * Defaults to "public".
   */
  schema?: string
  /**
   * Maximum number of pooled Postgres connections.
   * Defaults to 10.
   */
  maxConnections?: number
  /**
   * Producer state retention in milliseconds.
   * Defaults to 7 days.
   */
  producerStateTtlMs?: number
}

export interface EffectPostgresDurableStreamServerOptions
  extends
    Omit<TestServerOptions, `dataDir` | `store`>,
    EffectPostgresStoreOptions {
  onStreamCreated?: StreamLifecycleHook
  onStreamDeleted?: StreamLifecycleHook
}

export interface EffectPostgresCliOptions extends EffectPostgresDurableStreamServerOptions {
  host?: string
  port?: number
  longPollTimeout?: number
  compression?: boolean
}

export interface EffectPostgresResolvedServerOptions
  extends
    Omit<
      EffectPostgresDurableStreamServerOptions,
      | `host`
      | `port`
      | `longPollTimeout`
      | `compression`
      | `schema`
      | `maxConnections`
      | `producerStateTtlMs`
    >,
    Required<
      Pick<
        EffectPostgresCliOptions,
        | `host`
        | `port`
        | `longPollTimeout`
        | `compression`
        | `schema`
        | `maxConnections`
        | `producerStateTtlMs`
      >
    > {}

export interface WaiterResult {
  messages: Array<{
    data: Uint8Array
    offset: string
    timestamp: number
  }>
  timedOut: boolean
  streamClosed?: boolean
}

export interface WaiterEntry {
  path: string
  offset: string
  resolve: (result: WaiterResult) => void
  timeoutId: ReturnType<typeof setTimeout>
}

export interface InitializedEffectPostgresStore extends DurableStreamStore {
  readonly options: Required<EffectPostgresStoreOptions>
}
