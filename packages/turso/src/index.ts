import { createClient } from '@libsql/client';
import type { World } from '@workflow/world';
import {
  reenqueueActiveRuns,
  resolveQueueNamespace,
  SPEC_VERSION_CURRENT,
} from '@workflow/world';
import { migrateClient, configureClient } from './migrations.js';
import { createQueue } from './queue.js';
import { createStorage } from './storage.js';
import { createStreamer } from './streamer.js';

export interface TursoWorldConfig {
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

function envFlag(value: string | undefined): boolean {
  return value === '1' || value === 'true';
}

export function createWorld(config: TursoWorldConfig = {}): World {
  const databaseUrl =
    config.databaseUrl ??
    process.env.WORKFLOW_TURSO_DATABASE_URL ??
    'file:workflow.db';
  const authToken =
    config.authToken ?? process.env.WORKFLOW_TURSO_AUTH_TOKEN ?? undefined;
  const concurrency =
    config.concurrency ??
    (Number.parseInt(process.env.WORKFLOW_CONCURRENCY ?? '20', 10) || 20);
  const autoMigrate =
    config.autoMigrate ?? envFlag(process.env.WORKFLOW_TURSO_AUTO_MIGRATE);
  const namespace = resolveQueueNamespace(config.namespace);
  const client = createClient({ url: databaseUrl, authToken });
  const storage = createStorage({ client });
  const queue = createQueue({
    client,
    baseUrl: config.baseUrl,
    concurrency,
    idempotencyTtlMs: config.idempotencyTtlMs,
    maxRetries: config.maxRetries,
    pollIntervalMs: config.pollIntervalMs,
  });
  const streamer = createStreamer({
    client,
    pollIntervalMs: config.pollIntervalMs,
  });
  let startPromise: Promise<void> | undefined;
  let closed = false;

  return {
    specVersion: SPEC_VERSION_CURRENT,
    capabilities: {
      hookRetention: { active: true },
    },
    ...storage,
    ...streamer,
    getDeploymentId: queue.getDeploymentId,
    queue: queue.queue,
    createQueueHandler: queue.createQueueHandler,
    async start() {
      if (closed) {
        throw new Error('The Turso World is closed');
      }
      startPromise ??= (async () => {
        if (autoMigrate) {
          await migrateClient(client, databaseUrl);
        } else {
          await configureClient(client, databaseUrl);
        }
        await queue.start();
        await reenqueueActiveRuns(
          storage.runs,
          queue.queue,
          'world-turso',
          namespace
        );
      })();
      await startPromise;
    },
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      await queue.close();
      await streamer.close();
      await client.close();
    },
  };
}

export * from './drizzle/schema.js';
export { migrateDatabase } from './migrations.js';
