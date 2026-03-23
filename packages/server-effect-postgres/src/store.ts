import postgres from "postgres"
import type {
  AppendOptions,
  AppendResult,
  DurableStreamStore,
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "@durable-streams/server"
import type { EffectPostgresStoreOptions } from "./types"
import type { Sql } from "postgres"

const EMPTY_OFFSET = `0000000000000000_0000000000000000`

interface StreamRow {
  path: string
  content_type: string | null
  current_offset: string
  last_seq: string | null
  ttl_seconds: number | null
  expires_at: string | null
  created_at_ms: string
  closed: boolean
  closed_by_producer_id: string | null
  closed_by_epoch: number | null
  closed_by_seq: number | null
}

interface MessageRow {
  message_offset: string
  data: Uint8Array
  created_at_ms: string
}

interface ProducerRow {
  producer_id: string
  epoch: number
  last_seq: number
  last_updated_ms: string
}

interface Waiter {
  path: string
  offset: string
  resolve: (result: {
    messages: Array<StreamMessage>
    timedOut: boolean
    streamClosed?: boolean
  }) => void
  timeoutId: ReturnType<typeof setTimeout>
}

function normalizeContentType(contentType: string | undefined): string {
  if (!contentType) return ``
  return contentType.split(`;`)[0]!.trim().toLowerCase()
}

function processJsonAppend(
  data: Uint8Array,
  isInitialCreate = false
): Uint8Array {
  const text = new TextDecoder().decode(data)

  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    throw new Error(`Invalid JSON`)
  }

  let result: string
  if (Array.isArray(parsed)) {
    if (parsed.length === 0) {
      if (isInitialCreate) {
        return new Uint8Array(0)
      }
      throw new Error(`Empty arrays are not allowed`)
    }
    result = parsed.map((item) => JSON.stringify(item)).join(`,`) + `,`
  } else {
    result = JSON.stringify(parsed) + `,`
  }

  return new TextEncoder().encode(result)
}

function formatJsonResponse(data: Uint8Array): Uint8Array {
  if (data.length === 0) {
    return new TextEncoder().encode(`[]`)
  }

  let text = new TextDecoder().decode(data).trimEnd()
  if (text.endsWith(`,`)) {
    text = text.slice(0, -1)
  }

  return new TextEncoder().encode(`[${text}]`)
}

function mapPostgresValue(
  value: string | null | undefined
): string | undefined {
  return value ?? undefined
}

function rowToStream(
  row: StreamRow,
  producerRows: Array<ProducerRow> = []
): Stream {
  const producers =
    producerRows.length > 0
      ? new Map<string, ProducerState>(
          producerRows.map((producer) => [
            producer.producer_id,
            {
              epoch: producer.epoch,
              lastSeq: producer.last_seq,
              lastUpdated: Number(producer.last_updated_ms),
            },
          ])
        )
      : undefined

  return {
    path: row.path,
    contentType: mapPostgresValue(row.content_type),
    messages: [],
    currentOffset: row.current_offset,
    lastSeq: mapPostgresValue(row.last_seq),
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: mapPostgresValue(row.expires_at),
    createdAt: Number(row.created_at_ms),
    producers,
    closed: row.closed,
    closedBy:
      row.closed_by_producer_id !== null &&
      row.closed_by_epoch !== null &&
      row.closed_by_seq !== null
        ? {
            producerId: row.closed_by_producer_id,
            epoch: row.closed_by_epoch,
            seq: row.closed_by_seq,
          }
        : undefined,
  }
}

function rowToMessage(row: MessageRow): StreamMessage {
  return {
    offset: row.message_offset,
    data: row.data,
    timestamp: Number(row.created_at_ms),
  }
}

function isExpired(row: StreamRow, now: number): boolean {
  if (row.expires_at) {
    const expiryTime = new Date(row.expires_at).getTime()
    if (!Number.isFinite(expiryTime) || now >= expiryTime) {
      return true
    }
  }

  if (row.ttl_seconds !== null) {
    const expiryTime = Number(row.created_at_ms) + row.ttl_seconds * 1000
    if (now >= expiryTime) {
      return true
    }
  }

  return false
}

function parseOffset(offset: string): { readSeq: number; byteOffset: number } {
  const [readSeqPart = `0`, byteOffsetPart = `0`] = offset.split(`_`)
  return {
    readSeq: Number(readSeqPart),
    byteOffset: Number(byteOffsetPart),
  }
}

function makeOffset(readSeq: number, byteOffset: number): string {
  return `${String(readSeq).padStart(16, `0`)}_${String(byteOffset).padStart(16, `0`)}`
}

function coerceBytea(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value
  }
  if (Buffer.isBuffer(value)) {
    return new Uint8Array(value)
  }
  return new Uint8Array()
}

function validateProducerRow(
  producer: ProducerRow | undefined,
  producerId: string,
  epoch: number,
  seq: number
): ProducerValidationResult {
  const now = Date.now()

  if (!producer) {
    if (seq !== 0) {
      return {
        status: `sequence_gap`,
        expectedSeq: 0,
        receivedSeq: seq,
      }
    }
    return {
      status: `accepted`,
      isNew: true,
      producerId,
      proposedState: {
        epoch,
        lastSeq: 0,
        lastUpdated: now,
      },
    }
  }

  if (epoch < producer.epoch) {
    return { status: `stale_epoch`, currentEpoch: producer.epoch }
  }

  if (epoch > producer.epoch) {
    if (seq !== 0) {
      return { status: `invalid_epoch_seq` }
    }
    return {
      status: `accepted`,
      isNew: true,
      producerId,
      proposedState: {
        epoch,
        lastSeq: 0,
        lastUpdated: now,
      },
    }
  }

  if (seq <= producer.last_seq) {
    return {
      status: `duplicate`,
      lastSeq: producer.last_seq,
    }
  }

  if (seq === producer.last_seq + 1) {
    return {
      status: `accepted`,
      isNew: false,
      producerId,
      proposedState: {
        epoch,
        lastSeq: seq,
        lastUpdated: now,
      },
    }
  }

  return {
    status: `sequence_gap`,
    expectedSeq: producer.last_seq + 1,
    receivedSeq: seq,
  }
}

function contentTypeMatches(
  a: string | undefined,
  b: string | undefined
): boolean {
  return (
    (normalizeContentType(a) || `application/octet-stream`) ===
    (normalizeContentType(b) || `application/octet-stream`)
  )
}

async function createSchema(sql: Sql): Promise<void> {
  await sql`
    create table if not exists durable_streams (
      path text primary key,
      content_type text,
      current_offset text not null,
      last_seq text,
      ttl_seconds integer,
      expires_at timestamptz,
      created_at_ms bigint not null,
      closed boolean not null default false,
      closed_by_producer_id text,
      closed_by_epoch integer,
      closed_by_seq integer
    )
  `

  await sql`
    create table if not exists durable_stream_messages (
      stream_path text not null references durable_streams(path) on delete cascade,
      message_offset text not null,
      byte_offset bigint not null,
      data bytea not null,
      created_at_ms bigint not null,
      primary key (stream_path, message_offset)
    )
  `

  await sql`
    create index if not exists durable_stream_messages_stream_byte_idx
      on durable_stream_messages (stream_path, byte_offset)
  `

  await sql`
    create table if not exists durable_stream_producers (
      stream_path text not null references durable_streams(path) on delete cascade,
      producer_id text not null,
      epoch integer not null,
      last_seq integer not null,
      last_updated_ms bigint not null,
      primary key (stream_path, producer_id)
    )
  `
}

export class EffectPostgresStreamStore implements DurableStreamStore {
  private readonly sql: Sql
  private readonly waiters = new Set<Waiter>()
  private initialized = false

  constructor(options: EffectPostgresStoreOptions) {
    const connectionTarget = options.databaseUrl ?? process.env[`DATABASE_URL`]
    this.sql =
      connectionTarget !== undefined
        ? postgres(connectionTarget, {
            max: options.maxConnections ?? 10,
            idle_timeout: 20,
            connect_timeout: 10,
            prepare: false,
            onnotice: () => {},
            transform: {
              value: {
                from: (value, column) =>
                  column.type === 17 ? coerceBytea(value) : value,
              },
            },
          })
        : postgres({
            max: options.maxConnections ?? 10,
            idle_timeout: 20,
            connect_timeout: 10,
            prepare: false,
            onnotice: () => {},
            transform: {
              value: {
                from: (value, column) =>
                  column.type === 17 ? coerceBytea(value) : value,
              },
            },
          })
  }

  async initialize(): Promise<void> {
    if (this.initialized) return
    try {
      await createSchema(this.sql)
    } catch (error) {
      throw new Error(`Failed to initialize Postgres store`, { cause: error })
    }
    this.initialized = true
  }

  private asSql(tx: unknown): Sql {
    return tx as Sql
  }

  async create(
    path: string,
    options: {
      contentType?: string
      ttlSeconds?: number
      expiresAt?: string
      initialData?: Uint8Array
      closed?: boolean
    } = {}
  ): Promise<Stream> {
    const existing = await this.get(path)
    if (existing) {
      const contentMatches = contentTypeMatches(
        existing.contentType,
        options.contentType
      )
      const ttlMatches = existing.ttlSeconds === options.ttlSeconds
      const expiresMatches = existing.expiresAt === options.expiresAt
      const closedMatches =
        (existing.closed ?? false) === (options.closed ?? false)

      if (contentMatches && ttlMatches && expiresMatches && closedMatches) {
        return existing
      }

      throw new Error(
        `Stream already exists with different configuration: ${path}`
      )
    }

    const createdAt = Date.now()

    await this.sql.begin(async (tx) => {
      const sql = this.asSql(tx)
      await sql`
        insert into durable_streams (
          path,
          content_type,
          current_offset,
          last_seq,
          ttl_seconds,
          expires_at,
          created_at_ms,
          closed
        ) values (
          ${path},
          ${options.contentType ?? null},
          ${EMPTY_OFFSET},
          ${null},
          ${options.ttlSeconds ?? null},
          ${options.expiresAt ? new Date(options.expiresAt) : null},
          ${createdAt},
          ${false}
        )
      `
    })

    if (options.initialData) {
      await this.append(path, options.initialData, {
        contentType: options.contentType,
        isInitialCreate: true,
      })
    }

    if (options.closed) {
      await this.sql`
        update durable_streams
        set closed = true
        where path = ${path}
      `
    }

    const created = await this.get(path)
    if (!created) {
      throw new Error(`Stream not found: ${path}`)
    }
    return created
  }

  async get(path: string): Promise<Stream | undefined> {
    const row = await this.getRowIfNotExpired(path)
    if (!row) return undefined

    const producers = await this.sql<Array<ProducerRow>>`
      select producer_id, epoch, last_seq, last_updated_ms
      from durable_stream_producers
      where stream_path = ${path}
    `

    return rowToStream(row, producers)
  }

  async has(path: string): Promise<boolean> {
    return (await this.getRowIfNotExpired(path)) !== undefined
  }

  async delete(path: string): Promise<boolean> {
    this.cancelWaitersForStream(path, true)

    const deleted = await this.sql<Array<Pick<StreamRow, `path`>>>`
      delete from durable_streams
      where path = ${path}
      returning path
    `

    return deleted.length > 0
  }

  async append(
    path: string,
    data: Uint8Array,
    options: AppendOptions & { isInitialCreate?: boolean } = {}
  ): Promise<StreamMessage | AppendResult | null> {
    return await this.sql.begin(async (tx) => {
      const sql = this.asSql(tx)
      const streamRows = await sql<Array<StreamRow>>`
        select
          path,
          content_type,
          current_offset,
          last_seq,
          ttl_seconds,
          expires_at::text,
          created_at_ms::text,
          closed,
          closed_by_producer_id,
          closed_by_epoch,
          closed_by_seq
        from durable_streams
        where path = ${path}
        for update
      `

      const row = streamRows[0]
      if (!row) {
        throw new Error(`Stream not found: ${path}`)
      }

      if (isExpired(row, Date.now())) {
        await sql`delete from durable_streams where path = ${path}`
        throw new Error(`Stream not found: ${path}`)
      }

      if (row.closed) {
        if (
          options.producerId &&
          row.closed_by_producer_id === options.producerId &&
          row.closed_by_epoch === options.producerEpoch &&
          row.closed_by_seq === options.producerSeq
        ) {
          return {
            message: null,
            streamClosed: true,
            producerResult: {
              status: `duplicate`,
              lastSeq: options.producerSeq,
            },
          }
        }

        return {
          message: null,
          streamClosed: true,
        }
      }

      if (options.contentType && row.content_type) {
        if (!contentTypeMatches(row.content_type, options.contentType)) {
          throw new Error(
            `Content-type mismatch: expected ${row.content_type}, got ${options.contentType}`
          )
        }
      }

      let producerResult: ProducerValidationResult | undefined
      if (
        options.producerId !== undefined &&
        options.producerEpoch !== undefined &&
        options.producerSeq !== undefined
      ) {
        const producerRows = await sql<Array<ProducerRow>>`
          select producer_id, epoch, last_seq, last_updated_ms
          from durable_stream_producers
          where stream_path = ${path} and producer_id = ${options.producerId}
          for update
        `

        producerResult = validateProducerRow(
          producerRows[0],
          options.producerId,
          options.producerEpoch,
          options.producerSeq
        )

        if (producerResult.status !== `accepted`) {
          return { message: null, producerResult }
        }
      }

      if (
        options.seq !== undefined &&
        row.last_seq !== null &&
        options.seq <= row.last_seq
      ) {
        throw new Error(`Sequence conflict: ${options.seq} <= ${row.last_seq}`)
      }

      let processedData = data
      if (
        normalizeContentType(row.content_type ?? undefined) ===
        `application/json`
      ) {
        processedData = processJsonAppend(
          data,
          options.isInitialCreate ?? false
        )
        if (processedData.length === 0) {
          return null
        }
      }

      const { readSeq, byteOffset } = parseOffset(row.current_offset)
      const nextByteOffset = byteOffset + processedData.length
      const nextOffset = makeOffset(readSeq, nextByteOffset)
      const now = Date.now()

      await sql`
        insert into durable_stream_messages (
          stream_path,
          message_offset,
          byte_offset,
          data,
          created_at_ms
        ) values (
          ${path},
          ${nextOffset},
          ${nextByteOffset},
          ${Buffer.from(processedData)},
          ${now}
        )
      `

      if (producerResult?.status === `accepted`) {
        await sql`
          insert into durable_stream_producers (
            stream_path,
            producer_id,
            epoch,
            last_seq,
            last_updated_ms
          ) values (
            ${path},
            ${producerResult.producerId},
            ${producerResult.proposedState.epoch},
            ${producerResult.proposedState.lastSeq},
            ${producerResult.proposedState.lastUpdated}
          )
          on conflict (stream_path, producer_id)
          do update set
            epoch = excluded.epoch,
            last_seq = excluded.last_seq,
            last_updated_ms = excluded.last_updated_ms
        `
      }

      await sql`
        update durable_streams
        set
          current_offset = ${nextOffset},
          last_seq = ${options.seq ?? row.last_seq},
          closed = ${options.close ? true : row.closed},
          closed_by_producer_id = ${
            options.close && options.producerId
              ? options.producerId
              : row.closed_by_producer_id
          },
          closed_by_epoch = ${
            options.close && options.producerEpoch !== undefined
              ? options.producerEpoch
              : row.closed_by_epoch
          },
          closed_by_seq = ${
            options.close && options.producerSeq !== undefined
              ? options.producerSeq
              : row.closed_by_seq
          }
        where path = ${path}
      `

      const message: StreamMessage = {
        data: processedData,
        offset: nextOffset,
        timestamp: now,
      }

      queueMicrotask(() => {
        this.notifyWaiters(path).catch((error) => {
          console.error(`Failed to notify waiters`, error)
        })
      })

      if (producerResult || options.close) {
        return {
          message,
          producerResult,
          streamClosed: options.close,
        }
      }

      return message
    })
  }

  async appendWithProducer(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    const result = await this.append(path, data, options)
    if (result && typeof result === `object` && `message` in result) {
      return result
    }
    return { message: result }
  }

  async closeStream(
    path: string
  ): Promise<{ finalOffset: string; alreadyClosed: boolean } | null> {
    return await this.sql.begin(async (tx) => {
      const sql = this.asSql(tx)
      const rows = await sql<Array<StreamRow>>`
        select
          path,
          content_type,
          current_offset,
          last_seq,
          ttl_seconds,
          expires_at::text,
          created_at_ms::text,
          closed,
          closed_by_producer_id,
          closed_by_epoch,
          closed_by_seq
        from durable_streams
        where path = ${path}
        for update
      `

      const row = rows[0]
      if (!row) return null

      const alreadyClosed = row.closed
      await sql`
        update durable_streams
        set closed = true
        where path = ${path}
      `

      queueMicrotask(() => {
        this.resolveWaiters(path, {
          messages: [],
          timedOut: false,
          streamClosed: true,
        })
      })

      return {
        finalOffset: row.current_offset,
        alreadyClosed,
      }
    })
  }

  async closeStreamWithProducer(
    path: string,
    options: {
      producerId: string
      producerEpoch: number
      producerSeq: number
    }
  ): Promise<{
    finalOffset: string
    alreadyClosed: boolean
    producerResult?: ProducerValidationResult
  } | null> {
    return await this.sql.begin(async (tx) => {
      const sql = this.asSql(tx)
      const rows = await sql<Array<StreamRow>>`
        select
          path,
          content_type,
          current_offset,
          last_seq,
          ttl_seconds,
          expires_at::text,
          created_at_ms::text,
          closed,
          closed_by_producer_id,
          closed_by_epoch,
          closed_by_seq
        from durable_streams
        where path = ${path}
        for update
      `

      const row = rows[0]
      if (!row) return null

      if (row.closed) {
        if (
          row.closed_by_producer_id === options.producerId &&
          row.closed_by_epoch === options.producerEpoch &&
          row.closed_by_seq === options.producerSeq
        ) {
          return {
            finalOffset: row.current_offset,
            alreadyClosed: true,
            producerResult: {
              status: `duplicate`,
              lastSeq: options.producerSeq,
            },
          }
        }

        return {
          finalOffset: row.current_offset,
          alreadyClosed: true,
          producerResult: { status: `stream_closed` },
        }
      }

      const producerRows = await sql<Array<ProducerRow>>`
        select producer_id, epoch, last_seq, last_updated_ms
        from durable_stream_producers
        where stream_path = ${path} and producer_id = ${options.producerId}
        for update
      `

      const producerResult = validateProducerRow(
        producerRows[0],
        options.producerId,
        options.producerEpoch,
        options.producerSeq
      )

      if (producerResult.status !== `accepted`) {
        return {
          finalOffset: row.current_offset,
          alreadyClosed: row.closed,
          producerResult,
        }
      }

      await sql`
        insert into durable_stream_producers (
          stream_path,
          producer_id,
          epoch,
          last_seq,
          last_updated_ms
        ) values (
          ${path},
          ${producerResult.producerId},
          ${producerResult.proposedState.epoch},
          ${producerResult.proposedState.lastSeq},
          ${producerResult.proposedState.lastUpdated}
        )
        on conflict (stream_path, producer_id)
        do update set
          epoch = excluded.epoch,
          last_seq = excluded.last_seq,
          last_updated_ms = excluded.last_updated_ms
      `

      await sql`
        update durable_streams
        set
          closed = true,
          closed_by_producer_id = ${options.producerId},
          closed_by_epoch = ${options.producerEpoch},
          closed_by_seq = ${options.producerSeq}
        where path = ${path}
      `

      queueMicrotask(() => {
        this.resolveWaiters(path, {
          messages: [],
          timedOut: false,
          streamClosed: true,
        })
      })

      return {
        finalOffset: row.current_offset,
        alreadyClosed: false,
        producerResult,
      }
    })
  }

  async read(
    path: string,
    offset?: string
  ): Promise<{ messages: Array<StreamMessage>; upToDate: boolean }> {
    const row = await this.getRowIfNotExpired(path)
    if (!row) {
      throw new Error(`Stream not found: ${path}`)
    }

    if (!offset || offset === `-1`) {
      const rows = await this.sql<Array<MessageRow>>`
        select message_offset, data, created_at_ms::text
        from durable_stream_messages
        where stream_path = ${path}
        order by byte_offset asc
      `

      return {
        messages: rows.map(rowToMessage),
        upToDate: true,
      }
    }

    const effectiveOffset = offset === `now` ? row.current_offset : offset
    const { byteOffset } = parseOffset(effectiveOffset)

    if (byteOffset >= parseOffset(row.current_offset).byteOffset) {
      return { messages: [], upToDate: true }
    }

    const rows = await this.sql<Array<MessageRow>>`
      select message_offset, data, created_at_ms::text
      from durable_stream_messages
      where stream_path = ${path}
        and byte_offset > ${byteOffset}
      order by byte_offset asc
    `

    return {
      messages: rows.map(rowToMessage),
      upToDate: true,
    }
  }

  async formatResponse(
    path: string,
    messages: Array<StreamMessage>
  ): Promise<Uint8Array> {
    const stream = await this.get(path)
    if (!stream) {
      throw new Error(`Stream not found: ${path}`)
    }

    const totalSize = messages.reduce(
      (sum, message) => sum + message.data.length,
      0
    )
    const concatenated = new Uint8Array(totalSize)
    let offset = 0
    for (const message of messages) {
      concatenated.set(message.data, offset)
      offset += message.data.length
    }

    if (normalizeContentType(stream.contentType) === `application/json`) {
      return formatJsonResponse(concatenated)
    }

    return concatenated
  }

  async waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: Array<StreamMessage>
    timedOut: boolean
    streamClosed?: boolean
  }> {
    const stream = await this.get(path)
    if (!stream) {
      throw new Error(`Stream not found: ${path}`)
    }

    const existing = await this.read(path, offset)
    if (existing.messages.length > 0) {
      return {
        messages: existing.messages,
        timedOut: false,
        streamClosed: stream.closed,
      }
    }

    if (stream.closed && offset === stream.currentOffset) {
      return { messages: [], timedOut: false, streamClosed: true }
    }

    return await new Promise((resolve) => {
      const waiter: Waiter = {
        path,
        offset,
        resolve: (result) => {
          clearTimeout(waiter.timeoutId)
          this.waiters.delete(waiter)
          resolve(result)
        },
        timeoutId: setTimeout(async () => {
          this.waiters.delete(waiter)
          const currentStream = await this.get(path)
          resolve({
            messages: [],
            timedOut: true,
            streamClosed: currentStream?.closed,
          })
        }, timeoutMs),
      }

      this.waiters.add(waiter)
    })
  }

  async getCurrentOffset(path: string): Promise<string | undefined> {
    return (await this.getRowIfNotExpired(path))?.current_offset
  }

  async clear(): Promise<void> {
    this.cancelAllWaits()
    await this
      .sql`truncate durable_stream_producers, durable_stream_messages, durable_streams`
  }

  cancelAllWaits(): void {
    for (const waiter of Array.from(this.waiters)) {
      clearTimeout(waiter.timeoutId)
      waiter.resolve({ messages: [], timedOut: false })
    }
    this.waiters.clear()
  }

  async list(): Promise<Array<string>> {
    const rows = await this.sql<Array<{ path: string }>>`
      select path from durable_streams order by path asc
    `
    return rows.map((row) => row.path)
  }

  async close(): Promise<void> {
    this.cancelAllWaits()
    await this.sql.end()
  }

  private async getRowIfNotExpired(
    path: string
  ): Promise<StreamRow | undefined> {
    const rows = await this.sql<Array<StreamRow>>`
      select
        path,
        content_type,
        current_offset,
        last_seq,
        ttl_seconds,
        expires_at::text,
        created_at_ms::text,
        closed,
        closed_by_producer_id,
        closed_by_epoch,
        closed_by_seq
      from durable_streams
      where path = ${path}
    `

    const row = rows[0]
    if (!row) return undefined

    if (isExpired(row, Date.now())) {
      await this.delete(path)
      return undefined
    }

    return row
  }

  private cancelWaitersForStream(path: string, streamClosed?: boolean): void {
    for (const waiter of Array.from(this.waiters)) {
      if (waiter.path !== path) continue
      clearTimeout(waiter.timeoutId)
      this.waiters.delete(waiter)
      waiter.resolve({
        messages: [],
        timedOut: false,
        streamClosed,
      })
    }
  }

  private async notifyWaiters(path: string): Promise<void> {
    const relevantWaiters = Array.from(this.waiters).filter(
      (waiter) => waiter.path === path
    )

    for (const waiter of relevantWaiters) {
      const result = await this.read(path, waiter.offset)
      if (result.messages.length > 0) {
        clearTimeout(waiter.timeoutId)
        this.waiters.delete(waiter)
        waiter.resolve({
          messages: result.messages,
          timedOut: false,
        })
      }
    }
  }

  private resolveWaiters(
    path: string,
    result: {
      messages: Array<StreamMessage>
      timedOut: boolean
      streamClosed?: boolean
    }
  ): void {
    for (const waiter of Array.from(this.waiters)) {
      if (waiter.path !== path) continue
      clearTimeout(waiter.timeoutId)
      this.waiters.delete(waiter)
      waiter.resolve(result)
    }
  }
}
