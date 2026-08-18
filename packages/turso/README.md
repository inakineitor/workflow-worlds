# Turso World

[![npm version](https://img.shields.io/npm/v/@workflow-worlds/turso.svg)](https://www.npmjs.com/package/@workflow-worlds/turso)
[![license](https://img.shields.io/npm/l/@workflow-worlds/turso.svg)](https://github.com/mizzle-dev/workflow-worlds/blob/main/LICENSE)

A Turso and libSQL backed World implementation for Workflow DevKit spec v6.

## Features

- Workflow spec v6 event-sourced storage with dense, per-run event slot IDs
- Materialized runs, steps, hooks, waits, and attributes
- Durable polling queue with stable message IDs, retries, delayed delivery, and stale-lease recovery
- Run-isolated persistent streams with live reads and paginated chunk access
- Embedded SQLite files or remote Turso databases
- Optional automatic migrations for embedded applications

## Installation

```bash
pnpm add @workflow-worlds/turso @libsql/client
```

When installing directly from this repository, use the package path and an immutable commit:

```json
{
  "dependencies": {
    "@workflow-worlds/turso": "github:inakineitor/workflow-worlds#<commit>&path:/packages/turso"
  }
}
```

## Usage

Configure Workflow to load the package as its World:

```typescript
export default {
  experimental: {
    workflow: {
      world: '@workflow-worlds/turso',
    },
  },
};
```

The package exports `createWorld` for direct use:

```typescript
import { createWorld } from '@workflow-worlds/turso';

const world = createWorld({
  databaseUrl: 'file:workflow.db',
  autoMigrate: true,
});
```

Remote Turso uses the same API:

```typescript
const world = createWorld({
  databaseUrl: 'libsql://your-database.turso.io',
  authToken: process.env.WORKFLOW_TURSO_AUTH_TOKEN,
  autoMigrate: true,
});
```

## Database migrations

Automatic migrations are disabled by default for library consumers. Enable them through `autoMigrate: true` or:

```bash
export WORKFLOW_TURSO_AUTO_MIGRATE=1
```

You can also apply migrations explicitly:

```bash
pnpm exec workflow-turso-setup
```

Migrations are idempotent. The Workflow v5 migration preserves existing v0.2.2 entities and converts legacy stream data into the run-isolated stream tables. The spec v6 migration changes event identity to `(runId, eventId)` and marks newly created runs for dense slot IDs. It does not rewrite or mark existing runs, which continue using their original monotonic ULID event IDs.

## Configuration

| Variable | Description | Default |
| --- | --- | --- |
| `WORKFLOW_TURSO_DATABASE_URL` | Embedded file or remote libSQL URL | `file:workflow.db` |
| `WORKFLOW_TURSO_AUTH_TOKEN` | Remote Turso authentication token | None |
| `WORKFLOW_TURSO_AUTO_MIGRATE` | Apply pending migrations during `world.start()` | Disabled |
| `WORKFLOW_SERVICE_URL` | Explicit Workflow callback URL | Automatically detected |
| `WORKFLOW_CONCURRENCY` | Maximum concurrent queue deliveries | `20` |
| `WORKFLOW_QUEUE_NAMESPACE` | Optional Workflow queue namespace | None |

Programmatic options take precedence over environment variables.

```typescript
interface TursoWorldConfig {
  databaseUrl?: string;
  authToken?: string;
  baseUrl?: string;
  concurrency?: number;
  idempotencyTtlMs?: number;
  maxRetries?: number;
  pollIntervalMs?: number;
  autoMigrate?: boolean;
  namespace?: string;
}
```

## Development

The package requires Node.js 22 or newer.

```bash
pnpm install
pnpm --filter @workflow-worlds/turso build
pnpm --filter @workflow-worlds/turso test
pnpm --filter @workflow-worlds/turso typecheck
```
