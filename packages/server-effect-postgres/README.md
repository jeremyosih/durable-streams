# @durable-streams/server-effect-postgres

Effect v4 + Postgres durable stream server for the Durable Streams protocol.

This package builds on the shared `@durable-streams/server` HTTP implementation
and provides a transactional Postgres-backed store with Effect-managed
lifecycle.

## Status

This package targets protocol conformance first:

- durable stream create / append / read / delete
- long-poll and SSE live reads
- JSON mode
- stream closure
- idempotent producers
- TTL / expires-at

## Installation

```bash
pnpm add @durable-streams/server-effect-postgres
```

## Usage

```ts
import { EffectPostgresDurableStreamServer } from "@durable-streams/server-effect-postgres"

const server = new EffectPostgresDurableStreamServer({
  port: 4437,
  databaseUrl: "postgres://postgres:password@127.0.0.1:5432/durable_streams",
})

await server.start()
console.log(server.url)
```

## Environment

You can provide connection settings directly or via `databaseUrl`.

## Schema

The server initializes the required tables automatically on startup:

- `durable_streams`
- `durable_stream_messages`
- `durable_stream_producers`

Appends and producer state updates happen in a single SQL transaction while the
target stream row is locked with `SELECT ... FOR UPDATE`.

## Testing

The package reuses `@durable-streams/server-conformance-tests` to verify
protocol behavior against a real Postgres instance.
