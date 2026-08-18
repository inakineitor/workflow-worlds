import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient, type Client } from '@libsql/client';
import type { Storage } from '@workflow/world';
import {
  eventIdToSlot,
  slotToEventId,
  SPEC_VERSION_CURRENT,
  SPEC_VERSION_SUPPORTS_COMPRESSION,
} from '@workflow/world';
import { afterEach, describe, expect, it } from 'vitest';
import { createStorage as createSharedStorage } from './setup.js';

const isolatedResources: Array<{ clients: Client[]; directory: string }> = [];

afterEach(async () => {
  for (const resource of isolatedResources.splice(0)) {
    await Promise.all(resource.clients.map((client) => client.close()));
    await rm(resource.directory, { recursive: true, force: true });
  }
});

async function createRun(
  storage: Storage,
  runId: string,
  specVersion = SPEC_VERSION_CURRENT
) {
  return storage.events.create(runId, {
    eventType: 'run_created',
    specVersion,
    eventData: {
      deploymentId: 'deployment-v6',
      workflowName: 'workflow//test//slots',
      input: new Uint8Array(),
    },
  });
}

async function createIsolatedStorages(count: number): Promise<Storage[]> {
  const directory = await mkdtemp(join(tmpdir(), 'workflow-turso-slots-'));
  const databaseUrl = `file:${join(directory, 'workflow.db')}`;
  const { migrateDatabase } = await import('../dist/index.js');
  const { createStorage } = await import('../dist/storage.js');
  await migrateDatabase({ databaseUrl });

  const clients = Array.from({ length: count }, () =>
    createClient({ url: databaseUrl })
  );
  await Promise.all(
    clients.flatMap((client) => [
      client.execute('PRAGMA journal_mode = WAL'),
      client.execute('PRAGMA busy_timeout = 5000'),
    ])
  );
  isolatedResources.push({ clients, directory });
  return clients.map((client) => createStorage({ client }));
}

describe('Workflow v6 event slots', () => {
  it('starts each run at slot one and scopes event identity to the run', async () => {
    const { storage } = await createSharedStorage();
    const first = await createRun(
      storage,
      'wrun_01M0SLOTSCOPE000000000001'
    );
    const second = await createRun(
      storage,
      'wrun_01M0SLOTSCOPE000000000002'
    );

    expect(first.event?.eventId).toBe(slotToEventId(1));
    expect(second.event?.eventId).toBe(slotToEventId(1));
    await expect(
      storage.events.get(
        'wrun_01M0SLOTSCOPE000000000002',
        first.event!.eventId
      )
    ).resolves.toMatchObject({
      runId: 'wrun_01M0SLOTSCOPE000000000002',
    });
  });

  it('reports events skipped by a stale writer without rejecting its write', async () => {
    const { storage } = await createSharedStorage();
    const runId = 'wrun_01M0SLOTREPORT00000000001';
    await createRun(storage, runId);
    await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'attr-first',
      eventData: {
        changes: [{ key: 'first', value: '1' }],
        writer: { type: 'workflow' },
      },
    });

    const result = await storage.events.create(
      runId,
      {
        eventType: 'attr_set',
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: 'attr-stale',
        eventData: {
          changes: [{ key: 'stale', value: '2' }],
          writer: { type: 'workflow' },
        },
      },
      { eventCount: 1 }
    );

    expect(result.event?.eventId).toBe(slotToEventId(3));
    expect(result.events?.map((event) => event.eventId)).toEqual([
      slotToEventId(2),
    ]);
    expect(result).toMatchObject({ cursor: null, hasMore: false });
  });

  it('does not consume a slot when an entity write is rejected', async () => {
    const { storage } = await createSharedStorage();
    const runId = 'wrun_01M0SLOTREJECT00000000001';
    await createRun(storage, runId);
    const stepCreated = {
      eventType: 'step_created' as const,
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'step-duplicate',
      eventData: {
        stepName: 'step//test//duplicate',
        input: new Uint8Array(),
      },
    };
    await storage.events.create(runId, stepCreated);
    await expect(storage.events.create(runId, stepCreated)).rejects.toMatchObject({
      status: 409,
    });

    const next = await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'attr-after-rejection',
      eventData: {
        changes: [{ key: 'after', value: 'rejection' }],
        writer: { type: 'workflow' },
      },
    });
    expect(next.event?.eventId).toBe(slotToEventId(3));
  });

  it('uses slots for lazy steps, hook conflicts, and active-run hook events', async () => {
    const { storage } = await createSharedStorage();
    const runId = 'wrun_01M0SLOTSYNTHETIC00000001';
    const ownerRunId = 'wrun_01M0SLOTHOOKOWNER0000001';
    await createRun(storage, runId);
    await createRun(storage, ownerRunId);

    await storage.events.create(runId, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'step-lazy',
      eventData: {
        stepName: 'step//test//lazy-slots',
        input: new Uint8Array(),
      },
    });
    await storage.events.create(ownerRunId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-owner',
      eventData: { token: 'token-v6-slot-conflict', isWebhook: false },
    });
    const conflict = await storage.events.create(runId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-conflict',
      eventData: { token: 'token-v6-slot-conflict', isWebhook: false },
    });
    expect(conflict.event?.eventType).toBe('hook_conflict');

    await storage.events.create(runId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-active',
      eventData: { token: 'token-v6-slot-active', isWebhook: false },
    });
    await storage.events.create(runId, {
      eventType: 'hook_received',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-active',
      eventData: {
        token: 'token-v6-slot-active',
        payload: new Uint8Array(),
      },
    });

    const page = await storage.events.list({
      runId,
      pagination: { sortOrder: 'asc', limit: 20 },
    });
    expect(page.data.map((event) => event.eventId)).toEqual(
      Array.from({ length: page.data.length }, (_, index) =>
        slotToEventId(index + 1)
      )
    );
    expect(page.data.map((event) => event.eventType)).toEqual([
      'run_created',
      'step_created',
      'step_started',
      'hook_conflict',
      'hook_created',
      'hook_received',
    ]);
  });

  it('allocates dense slots across concurrent clients', async () => {
    const storages = await createIsolatedStorages(2);
    const runId = 'wrun_01M0SLOTCONCURRENT0000001';
    await createRun(storages[0], runId);

    await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        storages[index % storages.length].events.create(runId, {
          eventType: 'attr_set',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: `attr-concurrent-${index}`,
          eventData: {
            changes: [{ key: `key-${index}`, value: String(index) }],
            writer: { type: 'workflow' },
          },
        })
      )
    );

    const page = await storages[0].events.list({
      runId,
      pagination: { sortOrder: 'asc', limit: 20 },
    });
    expect(page.data.map((event) => eventIdToSlot(event.eventId))).toEqual(
      Array.from({ length: 9 }, (_, index) => index + 1)
    );
  });

  it('keeps markerless v5 runs on monotonic ULIDs', async () => {
    const { storage } = await createSharedStorage();
    const runId = 'wrun_01M0SLOTLEGACY0000000001';
    const created = await createRun(
      storage,
      runId,
      SPEC_VERSION_SUPPORTS_COMPRESSION
    );
    const next = await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_SUPPORTS_COMPRESSION,
      correlationId: 'attr-legacy',
      eventData: {
        changes: [{ key: 'legacy', value: 'true' }],
        writer: { type: 'workflow' },
      },
    });

    expect(eventIdToSlot(created.event!.eventId)).toBeNull();
    expect(eventIdToSlot(next.event!.eventId)).toBeNull();
    expect(created.event!.eventId < next.event!.eventId).toBe(true);
  });
});
