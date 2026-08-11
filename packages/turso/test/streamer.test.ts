import { describe, expect, it } from 'vitest';
import { createStreamer } from './setup.js';

async function readAll(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    chunks.push(value);
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

describe('Workflow v5 streams', () => {
  it('isolates equal stream names by run and exposes chunk metadata', async () => {
    const { streamer } = await createStreamer();
    await streamer.streams.writeMulti?.('run-stream-a', 'output', [
      'hello',
      new TextEncoder().encode(' world'),
    ]);
    await streamer.streams.write('run-stream-b', 'output', 'other');
    await streamer.streams.close('run-stream-a', 'output');
    await streamer.streams.close('run-stream-b', 'output');

    expect(await streamer.streams.list('run-stream-a')).toEqual(['output']);
    expect(await streamer.streams.getInfo('run-stream-a', 'output')).toEqual({
      tailIndex: 1,
      done: true,
    });
    expect(
      await streamer.streams.getChunks('run-stream-a', 'output', { limit: 1 })
    ).toMatchObject({
      data: [{ index: 0 }],
      cursor: '1',
      hasMore: true,
      done: true,
    });
    expect(await readAll(await streamer.streams.get('run-stream-a', 'output'))).toBe(
      'hello world'
    );
    expect(await readAll(await streamer.streams.get('run-stream-b', 'output'))).toBe(
      'other'
    );
  });

  it('supports negative start indexes', async () => {
    const { streamer } = await createStreamer();
    await streamer.streams.writeMulti?.('run-stream-negative', 'output', [
      'one',
      'two',
      'three',
    ]);
    await streamer.streams.close('run-stream-negative', 'output');

    expect(
      await readAll(await streamer.streams.get('run-stream-negative', 'output', -2))
    ).toBe('twothree');
  });
});
