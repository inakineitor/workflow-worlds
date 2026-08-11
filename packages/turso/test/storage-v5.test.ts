import { describe, expect, it } from 'vitest';
import { createStorage } from './setup.js';

describe('Workflow v5 storage', () => {
  it('materializes attributes, waits, lazy steps, and hook metadata', async () => {
    const { storage } = await createStorage();
    const runId = 'wrun_01KZSTORAGEV500000000000001';
    const created = await storage.events.create(runId, {
      eventType: 'run_created',
      specVersion: 5,
      eventData: {
        deploymentId: 'deployment-v5',
        workflowName: 'workflow//test//storage',
        input: new Uint8Array([1, 2, 3]),
        attributes: { tenant: 'marcus' },
        encryptionPublicKey: 'public-key',
      },
    });

    expect(created.run).toMatchObject({
      runId,
      specVersion: 5,
      attributes: { tenant: 'marcus' },
      encryptionPublicKey: 'public-key',
    });

    const attributed = await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: 5,
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
      specVersion: 5,
      correlationId: 'wait-1',
      eventData: { resumeAt: new Date('2030-01-01T00:00:00.000Z') },
    });
    expect(wait.wait).toMatchObject({ status: 'waiting', specVersion: 5 });
    const completedWait = await storage.events.create(runId, {
      eventType: 'wait_completed',
      specVersion: 5,
      correlationId: 'wait-1',
    });
    expect(completedWait.wait?.status).toBe('completed');

    const lazyStep = await storage.events.create(runId, {
      eventType: 'step_started',
      specVersion: 5,
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
      specVersion: 5,
      correlationId: 'hook-1',
      eventData: {
        token: 'token-storage-v5',
        metadata: new Uint8Array([6]),
        isWebhook: true,
        isSystem: true,
      },
    });
    expect(hook.hook).toMatchObject({
      isWebhook: true,
      isSystem: true,
      resumeContext: {
        deploymentId: 'deployment-v5',
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
        specVersion: 5,
        eventData: {
          deploymentId: 'deployment-v5',
          workflowName: 'workflow//test//correlation',
          input: new Uint8Array(),
        },
      });
      await storage.events.create(runId, {
        eventType: 'step_created',
        specVersion: 5,
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
      specVersion: 5,
      eventData: {
        deploymentId: 'deployment-v5',
        workflowName: 'workflow//test//cursor',
        input: new Uint8Array(),
      },
    });
    await storage.events.create(runId, {
      eventType: 'attr_set',
      specVersion: 5,
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
      specVersion: 5,
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
});
