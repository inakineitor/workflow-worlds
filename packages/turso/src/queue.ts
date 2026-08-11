import { setTimeout as sleep } from 'node:timers/promises';
import type { Client, InValue } from '@libsql/client';
import {
  createWorkflowBaseUrl,
  createWorkflowHealthEndpoint,
  createWorkflowUrl,
} from '@workflow/utils';
import { getWorkflowPort } from '@workflow/utils/get-port';
import {
  MessageId,
  parseQueueName,
  type Queue,
  type QueuePrefix,
  type ValidQueueName,
} from '@workflow/world';
import { decode, encode } from 'cbor-x';
import { monotonicFactory } from 'ulid';
import { z } from 'zod';
import { debug } from './utils.js';

const generateUlid = monotonicFactory();
const LEASE_DURATION_MS = 30_000;

export interface QueueConfig {
  client: Client;
  baseUrl?: string;
  concurrency?: number;
  idempotencyTtlMs?: number;
  maxRetries?: number;
  pollIntervalMs?: number;
}

export type TursoQueue = Queue & {
  start(): Promise<void>;
  close(): Promise<void>;
};

interface ClaimedMessage {
  messageId: MessageId;
  queueName: ValidQueueName;
  payload: string;
  headers?: Record<string, string>;
  attempt: number;
  lockToken: string;
}

function calculateBackoffDelay(attempt: number): number {
  const delay = Math.min(1000 * 2 ** Math.max(0, attempt - 1), 60_000);
  return Math.round(delay + delay * 0.2 * (Math.random() * 2 - 1));
}

function serializePayload(value: unknown): string {
  return JSON.stringify(value, (_key, item) =>
    item instanceof Uint8Array
      ? {
          __type: 'Uint8Array',
          data: Buffer.from(item).toString('base64'),
        }
      : item
  );
}

function deserializePayload(value: string): unknown {
  return JSON.parse(value, (_key, item) =>
    item !== null &&
    typeof item === 'object' &&
    item.__type === 'Uint8Array' &&
    typeof item.data === 'string'
      ? new Uint8Array(Buffer.from(item.data, 'base64'))
      : item
  );
}

function deserializeHeaders(value: unknown): Record<string, string> | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const bytes =
    value instanceof ArrayBuffer
      ? new Uint8Array(value)
      : ArrayBuffer.isView(value)
        ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
        : undefined;
  return bytes ? (decode(bytes) as Record<string, string>) : undefined;
}

export function createQueue(config: QueueConfig): TursoQueue {
  const { client } = config;
  const maxConcurrency = config.concurrency ?? 20;
  const maxRetries = config.maxRetries ?? 48;
  const pollIntervalMs = config.pollIntervalMs ?? 100;
  const inFlight = new Set<Promise<void>>();
  let running = false;
  let closing = false;
  let pollPromise: Promise<void> | undefined;
  let resolvedBaseUrl: Promise<string> | undefined;

  async function getExecutionBaseUrl(): Promise<string> {
    resolvedBaseUrl ??= (async () => {
      const configured =
        config.baseUrl ??
        process.env.WORKFLOW_SERVICE_URL ??
        process.env.WORKFLOW_LOCAL_BASE_URL;
      if (configured) {
        return createWorkflowBaseUrl(configured);
      }

      const configuredPort = Number(process.env.PORT);
      if (Number.isInteger(configuredPort) && configuredPort > 0) {
        return createWorkflowBaseUrl(`http://localhost:${configuredPort}`);
      }

      const detectedPort = await getWorkflowPort({
        endpoint: createWorkflowHealthEndpoint(),
      });
      if (typeof detectedPort !== 'number') {
        throw new Error('Unable to resolve the Workflow server URL');
      }
      return createWorkflowBaseUrl(`http://localhost:${detectedPort}`);
    })();
    return resolvedBaseUrl;
  }

  async function claimMessage(): Promise<ClaimedMessage | undefined> {
    const now = new Date();
    const nowString = now.toISOString();
    const leaseUntil = new Date(now.getTime() + LEASE_DURATION_MS).toISOString();
    const lockToken = `lock_${generateUlid()}`;
    const transaction = await client.transaction('write');

    try {
      const result = await transaction.execute({
        sql: `SELECT message_id, queue_name, payload, headers, attempt
              FROM queue_messages
              WHERE (
                status = 'pending'
                OR (status = 'processing' AND lease_until <= ?)
              )
              AND (not_before IS NULL OR not_before <= ?)
              ORDER BY created_at ASC
              LIMIT 1`,
        args: [nowString, nowString],
      });
      const row = result.rows[0];
      if (!row) {
        await transaction.commit();
        return undefined;
      }

      const messageId = MessageId.parse(row.message_id);
      const update = await transaction.execute({
        sql: `UPDATE queue_messages
              SET status = 'processing', lock_token = ?, lease_until = ?,
                  updated_at = ?
              WHERE message_id = ?
                AND (status = 'pending' OR lease_until <= ?)`,
        args: [lockToken, leaseUntil, nowString, messageId, nowString],
      });
      if (update.rowsAffected !== 1) {
        await transaction.commit();
        return undefined;
      }

      await transaction.commit();
      return {
        messageId,
        queueName: row.queue_name as ValidQueueName,
        payload: String(row.payload),
        headers: deserializeHeaders(row.headers),
        attempt: Math.max(1, Number(row.attempt) || 1),
        lockToken,
      };
    } catch (error) {
      transaction.close();
      throw error;
    }
  }

  async function updateClaim(
    message: ClaimedMessage,
    sql: string,
    args: InValue[]
  ): Promise<void> {
    await client.execute({
      sql: `${sql} WHERE message_id = ? AND lock_token = ?`,
      args: [...args, message.messageId, message.lockToken],
    });
  }

  async function reschedule(
    message: ClaimedMessage,
    delayMs: number,
    incrementAttempt: boolean
  ): Promise<void> {
    const nextAttempt = incrementAttempt ? message.attempt + 1 : message.attempt;
    await updateClaim(
      message,
      `UPDATE queue_messages
       SET status = 'pending', not_before = ?, attempt = ?, lock_token = NULL,
           lease_until = NULL, updated_at = ?`,
      [
        new Date(Date.now() + Math.max(0, delayMs)).toISOString(),
        nextAttempt,
        new Date().toISOString(),
      ]
    );
  }

  async function failOrRetry(message: ClaimedMessage): Promise<void> {
    if (message.attempt >= maxRetries) {
      const now = new Date().toISOString();
      await updateClaim(
        message,
        `UPDATE queue_messages
         SET status = 'failed', processed_at = ?, updated_at = ?,
             lock_token = NULL, lease_until = NULL`,
        [now, now]
      );
      return;
    }

    await reschedule(message, calculateBackoffDelay(message.attempt), true);
  }

  async function processMessage(message: ClaimedMessage): Promise<void> {
    try {
      const baseUrl = await getExecutionBaseUrl();
      const response = await fetch(createWorkflowUrl(baseUrl, { type: 'flow' }), {
        method: 'POST',
        headers: {
          ...message.headers,
          'content-type': 'application/json',
          'x-vqs-queue-name': message.queueName,
          'x-vqs-message-id': message.messageId,
          'x-vqs-message-attempt': String(message.attempt),
        },
        body: message.payload,
      });
      const responseText = await response.text();

      if (response.ok) {
        let timeoutSeconds: number | undefined;
        try {
          const parsed = JSON.parse(responseText) as { timeoutSeconds?: unknown };
          if (
            typeof parsed.timeoutSeconds === 'number' &&
            Number.isFinite(parsed.timeoutSeconds) &&
            parsed.timeoutSeconds >= 0
          ) {
            timeoutSeconds = parsed.timeoutSeconds;
          }
        } catch {}

        if (timeoutSeconds !== undefined) {
          await reschedule(message, timeoutSeconds * 1000, false);
          return;
        }

        const now = new Date().toISOString();
        await updateClaim(
          message,
          `UPDATE queue_messages
           SET status = 'completed', processed_at = ?, updated_at = ?,
               lock_token = NULL, lease_until = NULL`,
          [now, now]
        );
        return;
      }

      debug('Workflow queue delivery failed', {
        messageId: message.messageId,
        status: response.status,
        response: responseText,
      });
      await failOrRetry(message);
    } catch (error) {
      resolvedBaseUrl = undefined;
      debug('Workflow queue delivery failed', error);
      await failOrRetry(message);
    }
  }

  async function poll(): Promise<void> {
    while (running && !closing) {
      let claimed = false;
      while (inFlight.size < maxConcurrency && running && !closing) {
        const message = await claimMessage();
        if (!message) {
          break;
        }
        claimed = true;
        const task = processMessage(message).finally(() => inFlight.delete(task));
        inFlight.add(task);
      }

      if (!claimed) {
        await sleep(pollIntervalMs, undefined, { ref: false });
      }
    }
  }

  const queue: Queue['queue'] = async (queueName, message, options) => {
    parseQueueName(queueName);
    const messageId = MessageId.parse(`msg_${generateUlid()}`);
    const now = new Date();
    const notBefore = new Date(
      now.getTime() + Math.max(0, options?.delaySeconds ?? 0) * 1000
    ).toISOString();

    try {
      await client.execute({
        sql: `INSERT INTO queue_messages
              (message_id, queue_name, payload, idempotency_key, status,
               attempt, max_attempts, not_before, headers, created_at, updated_at)
              VALUES (?, ?, ?, ?, 'pending', 1, ?, ?, ?, ?, ?)`,
        args: [
          messageId,
          queueName,
          serializePayload(message),
          options?.idempotencyKey ?? null,
          maxRetries,
          notBefore,
          options?.headers ? encode(options.headers) : null,
          now.toISOString(),
          now.toISOString(),
        ],
      });
    } catch (error) {
      if (!options?.idempotencyKey) {
        throw error;
      }
      const existing = await client.execute({
        sql: `SELECT message_id FROM queue_messages
              WHERE idempotency_key = ?
                AND status IN ('pending', 'processing')
              LIMIT 1`,
        args: [options.idempotencyKey],
      });
      const existingId = existing.rows[0]?.message_id;
      if (existingId) {
        return { messageId: MessageId.parse(existingId) };
      }
      throw error;
    }

    return { messageId };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (
    prefix: QueuePrefix,
    handler
  ) => {
    const HeaderSchema = z.object({
      'x-vqs-queue-name': z.string(),
      'x-vqs-message-id': z.string(),
      'x-vqs-message-attempt': z.coerce.number().int().positive(),
      'x-vercel-id': z.string().optional(),
    });

    return async (request) => {
      const parsedHeaders = HeaderSchema.safeParse(
        Object.fromEntries(request.headers)
      );
      if (!parsedHeaders.success) {
        return Response.json({ error: 'Missing required queue headers' }, { status: 400 });
      }

      const queueName = parsedHeaders.data[
        'x-vqs-queue-name'
      ] as ValidQueueName;
      const parsedQueue = parseQueueName(queueName);
      if (parsedQueue.prefix !== prefix) {
        return Response.json({ error: 'Unhandled queue' }, { status: 400 });
      }

      let body: unknown;
      try {
        body = deserializePayload(await request.text());
      } catch {
        return Response.json({ error: 'Invalid queue body' }, { status: 400 });
      }

      try {
        const result = await handler(body, {
          attempt: parsedHeaders.data['x-vqs-message-attempt'],
          queueName,
          messageId: MessageId.parse(
            parsedHeaders.data['x-vqs-message-id']
          ),
          requestId: parsedHeaders.data['x-vercel-id'],
        });
        return Response.json(result ?? { ok: true });
      } catch (error) {
        debug('Workflow queue handler failed', error);
        return Response.json({ error: String(error) }, { status: 500 });
      }
    };
  };

  return {
    getDeploymentId: async () => process.env.DEPLOYMENT_ID ?? 'dpl_turso',
    queue,
    createQueueHandler,
    async start() {
      if (running) {
        return;
      }
      closing = false;
      running = true;
      pollPromise = poll();
    },
    async close() {
      if (!running) {
        return;
      }
      closing = true;
      running = false;
      await pollPromise;
      await Promise.allSettled(inFlight);
      inFlight.clear();
    },
  };
}
