import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import { ENV } from '../../src/env';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { RedisClient } from '../../src/redis/redis-types';

export async function sendTestEvent(
  eventServer: EventObserverServer,
  body: any = { test: 1 },
  throwOnError = true
) {
  const res = await fetch(eventServer.url + '/test_path', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (throwOnError && res.status !== 200) {
    throw new Error(`Failed to POST event: ${res.status}`);
  }
  return res;
}

const testClients = new Set<StacksEventStream>();

export async function closeTestClients() {
  for (const client of testClients) {
    await client.stop();
  }
  testClients.clear();
}

export async function createTestClient(
  lastMsgId = '0',
  streamType: StacksEventStreamType = StacksEventStreamType.all,
  onSequentialMsgError: (error: Error) => void
) {
  const callerLine = getCallerLine();
  const client = new StacksEventStream({
    redisUrl: ENV.REDIS_URL,
    eventStreamType: streamType,
    lastMessageId: lastMsgId,
    redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    appName: 'snp-client-test',
  });
  await client.connect({ waitForReady: true });
  testClients.add(client);

  let lastReceivedMsgId = parseInt(client.lastMessageId.split('-')[0]);
  client.events.on('msgReceived', ({ id }) => {
    const msgId = parseInt(id.split('-')[0]);
    // Validate that the msg ids are sequential for 'all' stream type.
    if (streamType === StacksEventStreamType.all && msgId !== lastReceivedMsgId + 1) {
      onSequentialMsgError(
        new Error(`Out of sequence msg: ${lastReceivedMsgId} -> ${msgId} - ${callerLine}`)
      );
    }
    lastReceivedMsgId = msgId;
  });

  return client;
}

export async function redisFlushAllWithPrefix(prefix: string, client: RedisClient) {
  const keys = await client.keys(`${prefix}*`);
  if (keys.length > 0) {
    const result = await client.del(keys);
    expect(result).toBe(keys.length);
  }
}

export function withTimeout<T = void>(promise: Promise<T>, ms?: number): Promise<T> {
  const callerLine = getCallerLine();
  const timeout = ms ?? Math.floor(0.9 * parseInt(process.env.JEST_TEST_TIMEOUT as string));
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timeout after ${timeout}ms - ${callerLine}`)),
      timeout
    );
    promise
      .then(value => resolve(value))
      .catch(err => reject(err as Error))
      .finally(() => clearTimeout(timer));
  });
}

// Returns the stack frame (source file and line number) of the caller of the caller of this function.
// There are cleaner ways to do this with prepareStackTrace & captureStackTrace but they are not accurate
// due to how source mapping works (e.g. ts-node and ts-jest).
function getCallerLine(): string {
  const stack = new Error().stack ?? '';
  const stackLines = stack.split('\n');
  // stackLines[0] = 'Error'
  // stackLines[1] = this function
  // stackLines[2] = caller
  // stackLines[3] = caller's caller
  return (stackLines[3] ?? '').trim().replace(/^at\s+/, '') ?? 'Failed to get caller info';
}

/**
 * The synchronous version of `jest.test` includes a `done` callback that can be called with an error
 * in order to fail the test. This function provides a similar API for async tests, which for some reason
 * jest does not provide.
 */
export function testWithFailCb(fn: (onError: (error: any) => void) => Promise<void>) {
  return new Promise<void>((resolve, reject) => {
    fn(error => reject(error as Error)).then(resolve, reject);
  });
}
