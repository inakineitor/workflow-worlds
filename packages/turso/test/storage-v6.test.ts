import { describe, expect, it } from 'vitest';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { createStorage } from './setup.js';

describe('Workflow v6 storage', () => {
  it('materializes attributes, waits, lazy steps, and hook metadata', async () => {
    const { storage } = await createStorage();
    const runId = 'wrun_01KZSTORAGEV500000000000001';
    const created = await storage.events.create(runId, {
      eventType: 'run_created',
      specVersion: SPEC_VERSION_CURRENT,
      eventData: {
        deploymentId: 'deployment-v6',
        workflowName: 'workflow//test//storage',
        input: new Uint8Array([1, 2, 3]),
        attributes: { tenant: 'marcus' },
        encryptionPublicKey: 'public-key',
      },
    });

    expect(created.run).toMatchObject({
      runId,
      specVersion: SPEC_VERSION_CURRENT,
      attributes: { tenant: 'marcus' },
      encryptionPublicKey: 'public-key',
    });

    const attributed = await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'attr-1',
      eventData: {
        changes: [
          { key: 'tenant', value: 'inakineitor' },
          { key: 'phase', value: 'testing' },
        ],
        writer: { type: 'workflow' },
      },
    });
    expect(attributed.run?.attributes).toEqual({
      tenant: 'inakineitor',
      phase: 'testing',
    });

    const wait = await storage.events.create(runId, {
      eventType: 'wait_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'wait-1',
      eventData: { resumeAt: new Date('2030-01-01T00:00:00.000Z') },
    });
    expect(wait.wait).toMatchObject({ status: 'waiting', specVersion: SPEC_VERSION_CURRENT });
    const completedWait = await storage.events.create(runId, {
      eventType: 'wait_completed',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'wait-1',
    });
    expect(completedWait.wait?.status).toBe('completed');

    const lazyStep = await storage.events.create(runId, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'step-1',
      eventData: {
        stepName: 'step//test//lazy',
        input: new Uint8Array([4, 5]),
      },
    });
    expect(lazyStep.stepCreated).toBe(true);
    expect(lazyStep.step).toMatchObject({
      stepId: 'step-1',
      status: 'running',
      attempt: 1,
    });

    const hook = await storage.events.create(runId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-1',
      eventData: {
        token: 'token-storage-v6',
        metadata: new Uint8Array([6]),
        isWebhook: true,
        isSystem: true,
      },
    });
    expect(hook.hook).toMatchObject({
      isWebhook: true,
      isSystem: true,
      resumeContext: {
        deploymentId: 'deployment-v6',
        workflowName: 'workflow//test//storage',
      },
    });
  });

  it('scopes correlation lookups and event reads to a run', async () => {
    const { storage } = await createStorage();
    for (const suffix of ['a', 'b']) {
      const runId = `wrun_01KZCORRELATION00000000000${suffix}`;
      await storage.events.create(runId, {
        eventType: 'run_created',
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          deploymentId: 'deployment-v6',
          workflowName: 'workflow//test//correlation',
          input: new Uint8Array(),
        },
      });
      await storage.events.create(runId, {
        eventType: 'step_created',
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: 'step-shared',
        eventData: {
          stepName: 'step//test//shared',
          input: new Uint8Array(),
        },
      });
    }

    const page = await storage.events.listByCorrelationId({
      runId: 'wrun_01KZCORRELATION00000000000a',
      correlationId: 'step-shared',
    });
    expect(page.data).toHaveLength(1);
    const event = await storage.events.get(
      'wrun_01KZCORRELATION00000000000a',
      page.data[0].eventId
    );
    expect(event.runId).toBe('wrun_01KZCORRELATION00000000000a');
  });

  it('returns the last event cursor when a page has no more results', async () => {
    const { storage } = await createStorage();
    const runId = 'wrun_01KZEVENTCURSOR000000000001';
    await storage.events.create(runId, {
      eventType: 'run_created',
      specVersion: SPEC_VERSION_CURRENT,
      eventData: {
        deploymentId: 'deployment-v6',
        workflowName: 'workflow//test//cursor',
        input: new Uint8Array(),
      },
    });
    await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'attr-cursor',
      eventData: {
        changes: [{ key: 'phase', value: 'first' }],
        writer: { type: 'workflow' },
      },
    });

    const initial = await storage.events.list({
      runId,
      pagination: { limit: 10 },
    });
    expect(initial).toMatchObject({ hasMore: false });
    expect(initial.cursor).toBe(initial.data.at(-1)?.eventId);

    await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'attr-cursor-next',
      eventData: {
        changes: [{ key: 'phase', value: 'second' }],
        writer: { type: 'workflow' },
      },
    });
    const incremental = await storage.events.list({
      runId,
      pagination: { limit: 10, cursor: initial.cursor ?? undefined },
    });
    expect(incremental.data).toHaveLength(1);
    expect(incremental.cursor).toBe(incremental.data[0].eventId);
    expect(incremental.hasMore).toBe(false);
  });

  it('preserves concurrent disjoint attribute writes', async () => {
    const { storage } = await createStorage();
    const runId = 'wrun_01KZATTRCONCURRENT0000000001';
    await storage.events.create(runId, {
      eventType: 'run_created',
      specVersion: SPEC_VERSION_CURRENT,
      eventData: {
        deploymentId: 'deployment-v6',
        workflowName: 'workflow//test//attributes',
        input: new Uint8Array(),
      },
    });

    await Promise.all(
      ['a', 'b', 'c'].map((key, index) =>
        storage.events.create(runId, {
          eventType: 'attr_set',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: `attr-concurrent-${key}`,
          eventData: {
            changes: [{ key, value: String(index + 1) }],
            writer: { type: 'workflow' },
          },
        })
      )
    );

    await expect(storage.runs.get(runId)).resolves.toMatchObject({
      attributes: { a: '1', b: '2', c: '3' },
    });
  });

  it('reports hook conflicts with the owning run', async () => {
    const { storage } = await createStorage();
    const ownerRunId = 'wrun_01KZHOOKOWNER0000000000001';
    const contenderRunId = 'wrun_01KZHOOKCONTENDER000000001';
    for (const runId of [ownerRunId, contenderRunId]) {
      await storage.events.create(runId, {
        eventType: 'run_created',
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          deploymentId: 'deployment-v6',
          workflowName: 'workflow//test//hooks',
          input: new Uint8Array(),
        },
      });
    }
    await storage.events.create(ownerRunId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'owner-hook',
      eventData: { token: 'token-conflict-v6', isWebhook: false },
    });

    const conflict = await storage.events.create(contenderRunId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'contender-hook',
      eventData: { token: 'token-conflict-v6', isWebhook: false },
    });
    expect(conflict.event).toMatchObject({
      eventType: 'hook_conflict',
      eventData: {
        token: 'token-conflict-v6',
        conflictingRunId: ownerRunId,
      },
    });
  });

  it('retains requested hooks after terminal runs and uses typed not-found errors', async () => {
    const { storage } = await createStorage();
    const runId = 'wrun_01KZHOOKRETENTION00000000001';
    await storage.events.create(runId, {
      eventType: 'run_created',
      specVersion: SPEC_VERSION_CURRENT,
      eventData: {
        deploymentId: 'deployment-v6',
        workflowName: 'workflow//test//retention',
        input: new Uint8Array(),
      },
    });
    await storage.events.create(runId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'retained-hook',
      eventData: {
        token: 'token-retained-v6',
        isWebhook: false,
        tokenRetentionUntil: new Date(Date.now() + 60_000),
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_completed',
      specVersion: SPEC_VERSION_CURRENT,
      eventData: { output: new Uint8Array() },
    });

    await expect(storage.hooks.getByToken('token-retained-v6')).resolves.toMatchObject({
      runId,
      hookId: 'retained-hook',
    });
    await expect(
      storage.events.create(runId, {
        eventType: 'hook_received',
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: 'retained-hook',
        eventData: {
          token: 'token-retained-v6',
          payload: new Uint8Array(),
        },
      })
    ).rejects.toMatchObject({ name: 'RunExpiredError' });
    await expect(storage.hooks.getByToken('token-missing-v6')).rejects.toMatchObject({
      name: 'HookNotFoundError',
      token: 'token-missing-v6',
    });
  });
});
