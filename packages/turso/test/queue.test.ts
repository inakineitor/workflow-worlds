import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient } from '@libsql/client';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { migrateDatabase } from '../src/migrations.js';
import { createQueue } from '../src/queue.js';

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })
    )
  );
});

describe('persistent queue', () => {
  it('wakes an idle poller during shutdown', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'workflow-turso-close-'));
    directories.push(directory);
    const databaseUrl = `file:${join(directory, 'workflow.db')}`;
    await migrateDatabase({ databaseUrl });
    const client = createClient({ url: databaseUrl });
    const queue = createQueue({ client, pollIntervalMs: 60_000 });
    await queue.start();

    const outcome = await Promise.race([
      queue.close().then(() => 'closed' as const),
      new Promise<'timed-out'>((resolve) =>
        setTimeout(() => resolve('timed-out'), 250)
      ),
    ]);

    expect(outcome).toBe('closed');
    await client.close();
  });

  it('redelivers a message whose processing lease expired', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'workflow-turso-queue-'));
    directories.push(directory);
    const databaseUrl = `file:${join(directory, 'workflow.db')}`;
    await migrateDatabase({ databaseUrl });
    const client = createClient({ url: databaseUrl });
    const received: unknown[] = [];
    const server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      request.on('end', () => {
        received.push(JSON.parse(Buffer.concat(chunks).toString()));
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ ok: true }));
      });
    });
    await new Promise<void>((resolve) =>
      server.listen(0, '127.0.0.1', resolve)
    );
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Expected an HTTP server port');
    }

    const now = new Date();
    await client.execute({
      sql: `INSERT INTO queue_messages
            (message_id, queue_name, payload, status, lock_token, attempt,
             max_attempts, not_before, lease_until, created_at, updated_at)
            VALUES (?, ?, ?, 'processing', ?, 1, 3, ?, ?, ?, ?)`,
      args: [
        'msg_stale',
        '__wkf_workflow_stale-run',
        JSON.stringify({ runId: 'wrun_stale' }),
        'stale-lock',
        now.toISOString(),
        new Date(now.getTime() - 1_000).toISOString(),
        now.toISOString(),
        now.toISOString(),
      ],
    });

    const queue = createQueue({
      client,
      baseUrl: `http://127.0.0.1:${address.port}`,
      pollIntervalMs: 10,
    });
    await queue.start();
    await vi.waitFor(
      async () => {
        const result = await client.execute({
          sql: 'SELECT status FROM queue_messages WHERE message_id = ?',
          args: ['msg_stale'],
        });
        expect(result.rows[0]?.status).toBe('completed');
      },
      { timeout: 5_000, interval: 20 }
    );

    expect(received).toEqual([{ runId: 'wrun_stale' }]);
    await queue.close();
    await client.close();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  });
});
