import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@libsql/client';
import { afterEach, describe, expect, it } from 'vitest';

const directories: string[] = [];
const testDirectory = dirname(fileURLToPath(import.meta.url));

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })
    )
  );
});

describe('automatic migrations', () => {
  it('initializes a fresh database and remains idempotent', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'workflow-turso-'));
    directories.push(directory);
    const databaseUrl = `file:${join(directory, 'workflow.db')}`;
    const { createWorld } = await import('../dist/index.js');

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const world = createWorld({ databaseUrl, autoMigrate: true });
      await world.start?.();
      await world.close?.();
    }

    const client = createClient({ url: databaseUrl });
    const result = await client.execute(
      `SELECT name FROM sqlite_master
       WHERE type = 'table' AND name IN (
         'workflow_runs', 'workflow_waits', 'workflow_streams',
         'workflow_stream_chunks', 'queue_messages'
       )`
    );
    await client.close();

    expect(result.rows.map((row) => row.name).sort()).toEqual([
      'queue_messages',
      'workflow_runs',
      'workflow_stream_chunks',
      'workflow_streams',
      'workflow_waits',
    ]);
  });

  it('upgrades a v0.2.2 database and preserves legacy stream chunks', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'workflow-turso-upgrade-'));
    directories.push(directory);
    const databaseUrl = `file:${join(directory, 'workflow.db')}`;
    const client = createClient({ url: databaseUrl });
    const legacyMigrations = [
      ['0000_skinny_ikaris.sql', 1764715828986],
      ['0001_cute_captain_flint.sql', 1770499353289],
      ['0002_add_step_retry_after.sql', 1770600000000],
    ] as const;

    await client.execute(`CREATE TABLE workflow_migrations (
      id SERIAL PRIMARY KEY,
      hash text NOT NULL,
      created_at numeric
    )`);
    for (const [filename, createdAt] of legacyMigrations) {
      const migration = await readFile(
        join(testDirectory, '..', 'src', 'drizzle', 'migrations', filename),
        'utf8'
      );
      for (const statement of migration.split('--> statement-breakpoint')) {
        if (statement.trim()) {
          await client.execute(statement);
        }
      }
      await client.execute({
        sql: `INSERT INTO workflow_migrations (hash, created_at)
              VALUES (?, ?)`,
        args: [
          createHash('sha256').update(migration).digest('hex'),
          createdAt,
        ],
      });
    }

    await client.execute({
      sql: `INSERT INTO stream_runs (run_id, stream_name, created_at)
            VALUES (?, ?, ?)`,
      args: ['run-legacy', 'output', '2026-01-01T00:00:00.000Z'],
    });
    for (const [chunkId, data, isEof] of [
      ['chnk_1', new TextEncoder().encode('hello'), 0],
      ['chnk_2', new TextEncoder().encode(' world'), 0],
      ['chnk_3', null, 1],
    ] as const) {
      await client.execute({
        sql: `INSERT INTO stream_chunks
              (chunk_id, stream_name, data, is_eof, created_at)
              VALUES (?, 'output', ?, ?, '2026-01-01T00:00:00.000Z')`,
        args: [chunkId, data, isEof],
      });
    }
    await client.close();

    const { migrateDatabase } = await import('../dist/index.js');
    await migrateDatabase({ databaseUrl });

    const upgraded = createClient({ url: databaseUrl });
    const stream = await upgraded.execute(
      `SELECT tail_index, done FROM workflow_streams
       WHERE run_id = 'run-legacy' AND stream_name = 'output'`
    );
    const chunks = await upgraded.execute(
      `SELECT chunk_index, data FROM workflow_stream_chunks
       WHERE run_id = 'run-legacy' AND stream_name = 'output'
       ORDER BY chunk_index`
    );
    await upgraded.close();

    expect(stream.rows[0]).toMatchObject({ tail_index: 1, done: 1 });
    expect(chunks.rows.map((row) => row.chunk_index)).toEqual([0, 1]);
    expect(
      new TextDecoder().decode(
        Buffer.concat(chunks.rows.map((row) => new Uint8Array(row.data as ArrayBuffer)))
      )
    ).toBe('hello world');
  });
});
