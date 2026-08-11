import { setTimeout as sleep } from 'node:timers/promises';
import type { Client, InValue, Row } from '@libsql/client';
import type { Streamer } from '@workflow/world';

const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;

export interface StreamerConfig {
  client: Client;
  pollIntervalMs?: number;
}

export type TursoStreamer = Streamer & {
  close(): Promise<void>;
};

function toBytes(value: unknown): Uint8Array {
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === 'string') {
    return new TextEncoder().encode(value);
  }
  return new Uint8Array();
}

function encodeChunk(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function parseCursor(cursor: string | undefined): number {
  if (cursor === undefined) {
    return 0;
  }
  const value = Number(cursor);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid stream cursor: ${cursor}`);
  }
  return value;
}

function rowIndex(row: Row): number {
  return Number(row.chunk_index);
}

export function createStreamer(config: StreamerConfig): TursoStreamer {
  const { client } = config;
  const pollIntervalMs = config.pollIntervalMs ?? 50;
  const readers = new Set<AbortController>();
  let closed = false;

  async function ensureStream(runId: string, name: string): Promise<void> {
    const now = new Date().toISOString();
    await client.execute({
      sql: `INSERT OR IGNORE INTO workflow_streams
            (run_id, stream_name, tail_index, done, created_at, updated_at)
            VALUES (?, ?, -1, 0, ?, ?)`,
      args: [runId, name, now, now],
    });
  }

  async function appendChunks(
    runId: string,
    name: string,
    chunks: readonly (string | Uint8Array)[]
  ): Promise<void> {
    if (closed) {
      throw new Error('The Turso streamer is closed');
    }
    if (chunks.length === 0) {
      return;
    }

    const transaction = await client.transaction('write');
    try {
      const now = new Date().toISOString();
      await transaction.execute({
        sql: `INSERT OR IGNORE INTO workflow_streams
              (run_id, stream_name, tail_index, done, created_at, updated_at)
              VALUES (?, ?, -1, 0, ?, ?)`,
        args: [runId, name, now, now],
      });
      const stream = await transaction.execute({
        sql: `SELECT tail_index, done FROM workflow_streams
              WHERE run_id = ? AND stream_name = ?`,
        args: [runId, name],
      });
      const row = stream.rows[0];
      if (!row) {
        throw new Error(`Unable to initialize stream ${name}`);
      }
      if (Number(row.done) === 1) {
        throw new Error(`Cannot write to closed stream ${name}`);
      }

      let nextIndex = Number(row.tail_index) + 1;
      for (const chunk of chunks) {
        await transaction.execute({
          sql: `INSERT INTO workflow_stream_chunks
                (run_id, stream_name, chunk_index, data, created_at)
                VALUES (?, ?, ?, ?, ?)`,
          args: [runId, name, nextIndex, encodeChunk(chunk) as InValue, now],
        });
        nextIndex += 1;
      }
      await transaction.execute({
        sql: `UPDATE workflow_streams
              SET tail_index = ?, updated_at = ?
              WHERE run_id = ? AND stream_name = ?`,
        args: [nextIndex - 1, now, runId, name],
      });
      await transaction.commit();
    } catch (error) {
      transaction.close();
      throw error;
    }
  }

  const streams: Streamer['streams'] = {
    async write(runId, name, chunk) {
      await appendChunks(runId, name, [chunk]);
    },

    async writeMulti(runId, name, chunks) {
      await appendChunks(runId, name, chunks);
    },

    async close(runId, name) {
      if (closed) {
        throw new Error('The Turso streamer is closed');
      }
      await ensureStream(runId, name);
      await client.execute({
        sql: `UPDATE workflow_streams
              SET done = 1, updated_at = ?
              WHERE run_id = ? AND stream_name = ?`,
        args: [new Date().toISOString(), runId, name],
      });
    },

    async get(runId, name, requestedStartIndex = 0) {
      const controller = new AbortController();
      readers.add(controller);

      let startIndex = requestedStartIndex;
      if (startIndex < 0) {
        const info = await streams.getInfo(runId, name);
        startIndex = Math.max(0, info.tailIndex + 1 + startIndex);
      }

      return new ReadableStream<Uint8Array>({
        start(streamController) {
          void (async () => {
            let nextIndex = startIndex;
            try {
              while (!controller.signal.aborted) {
                const result = await client.execute({
                  sql: `SELECT chunk_index, data FROM workflow_stream_chunks
                        WHERE run_id = ? AND stream_name = ? AND chunk_index >= ?
                        ORDER BY chunk_index ASC LIMIT 100`,
                  args: [runId, name, nextIndex],
                });

                for (const row of result.rows) {
                  const index = rowIndex(row);
                  streamController.enqueue(Uint8Array.from(toBytes(row.data)));
                  nextIndex = index + 1;
                }

                const info = await streams.getInfo(runId, name);
                if (info.done && nextIndex > info.tailIndex) {
                  streamController.close();
                  return;
                }

                await sleep(pollIntervalMs, undefined, {
                  signal: controller.signal,
                  ref: false,
                });
              }
            } catch (error) {
              if (!controller.signal.aborted) {
                streamController.error(error);
              }
            } finally {
              readers.delete(controller);
            }
          })();
        },
        cancel() {
          controller.abort();
          readers.delete(controller);
        },
      });
    },

    async list(runId) {
      const result = await client.execute({
        sql: `SELECT stream_name FROM workflow_streams
              WHERE run_id = ? ORDER BY stream_name ASC`,
        args: [runId],
      });
      return result.rows.map((row) => String(row.stream_name));
    },

    async getChunks(runId, name, options) {
      const startIndex = parseCursor(options?.cursor);
      const limit = Math.min(
        MAX_PAGE_SIZE,
        Math.max(1, options?.limit ?? DEFAULT_PAGE_SIZE)
      );
      const result = await client.execute({
        sql: `SELECT chunk_index, data FROM workflow_stream_chunks
              WHERE run_id = ? AND stream_name = ? AND chunk_index >= ?
              ORDER BY chunk_index ASC LIMIT ?`,
        args: [runId, name, startIndex, limit + 1],
      });
      const hasMore = result.rows.length > limit;
      const rows = result.rows.slice(0, limit);
      const data = rows.map((row) => ({
        index: rowIndex(row),
        data: Uint8Array.from(toBytes(row.data)),
      }));
      const last = data.at(-1);
      const info = await streams.getInfo(runId, name);

      return {
        data,
        cursor: hasMore && last ? String(last.index + 1) : null,
        hasMore,
        done: info.done,
      };
    },

    async getInfo(runId, name) {
      const result = await client.execute({
        sql: `SELECT tail_index, done FROM workflow_streams
              WHERE run_id = ? AND stream_name = ?`,
        args: [runId, name],
      });
      const row = result.rows[0];
      return {
        tailIndex: row ? Number(row.tail_index) : -1,
        done: row ? Number(row.done) === 1 : false,
      };
    },
  };

  return {
    streams,
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      for (const reader of readers) {
        reader.abort();
      }
      readers.clear();
    },
  };
}
