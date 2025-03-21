import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import { ENV } from '../../src/env';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { RedisClient } from '../../src/redis/redis-types';

/**  Ensure that all msgs are received in order and with no gaps. */
function ensureSequenceMsgOrder(client: StacksEventStream) {
  let lastReceivedMsgId = parseInt(client.lastMessageId.split('-')[0]);
  client.events.on('msgReceived', ({ id }) => {
    const msgId = parseInt(id.split('-')[0]);
    expect(msgId).toBe(lastReceivedMsgId + 1);
    lastReceivedMsgId = msgId;
  });
}

export async function sendTestEvent(eventServer: EventObserverServer, body: any = { test: 1 }) {
  const res = await fetch(eventServer.url + '/test_path', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (res.status !== 200) {
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

export async function createTestClient(lastMsgId = '0') {
  const client = new StacksEventStream({
    redisUrl: ENV.REDIS_URL,
    eventStreamType: StacksEventStreamType.all,
    lastMessageId: lastMsgId,
    redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    appName: 'snp-client-test',
  });
  await client.connect({ waitForReady: true });
  testClients.add(client);
  ensureSequenceMsgOrder(client);
  return client;
}

export async function redisFlushAllWithPrefix(prefix: string, client: RedisClient) {
  const keys = await client.keys(`${prefix}*`);
  const result = await client.del(keys);
  expect(result).toBe(keys.length);
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
