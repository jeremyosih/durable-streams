# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Durable Streams is a monorepo with a TypeScript core (client, server, state, CLI, conformance tests) and a Go production server (caddy-plugin). See `CLAUDE.md` for the canonical command reference and testing philosophy.

### System requirements

- **Node.js >= 22** and **pnpm 10.25.0** (already available in the VM snapshot).
- **Go >= 1.25** is required for building `packages/caddy-plugin`. The VM snapshot installs Go 1.25 at `/usr/local/go`; ensure `PATH` includes `/usr/local/go/bin` (already added to `~/.bashrc`).

### Quick reference (see `CLAUDE.md` and `README.md` for full details)

| Task                  | Command                                                     |
| --------------------- | ----------------------------------------------------------- |
| Install deps          | `pnpm install`                                              |
| Build TS packages     | `pnpm build` (excludes caddy-plugin and examples)           |
| Lint                  | `pnpm lint` (or `pnpm lint:fix`)                            |
| Typecheck             | `pnpm typecheck`                                            |
| Run conformance tests | `pnpm test:run`                                             |
| Start dev server      | `pnpm start:dev` (runs on port 4437)                        |
| Build caddy-plugin    | `cd packages/caddy-plugin && go build -o caddy ./cmd/caddy` |
| Go tests              | `cd packages/caddy-plugin && go test ./...`                 |

### Gotchas

- The `pnpm build` command includes `client-dotnet` and `client-swift` packages which require .NET SDK and Swift respectively. These are **not** installed in the Cloud VM. If the default `pnpm build` fails, filter them out: `pnpm -r --filter '!@durable-streams-internal/client-dotnet' --filter '!@durable-streams-internal/client-swift' --filter '!@durable-streams-internal/caddy-plugin' --filter '!@durable-streams/example-*' --filter '!docs' build`
- After `pnpm install`, you may see warnings about "Ignored build scripts" (esbuild, msgpackr-extract, etc.). These are non-blocking and do not affect functionality.
- The dev server (`pnpm start:dev`) uses an in-memory store by default — no external databases needed.
- When writing to a stream via curl, you **must** set the `Content-Type` header to match the stream's content type (set at creation). Otherwise you'll get a 409 content-type mismatch error.
- The pre-commit hook runs `pnpm lint-staged` (ESLint on `*.ts`/`*.tsx`, Prettier on `*.json`/`*.md`/`*.yml`/`*.yaml`).
