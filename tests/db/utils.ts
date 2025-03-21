import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import { ENV } from '../../src/env';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { RedisClient } from '../../src/redis/redis-types';

/**  Ensure that all msgs are received in order and with no gaps. */
export function ensureSequenceMsgOrder(client: StacksEventStream) {
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

export const testClients = new Set<StacksEventStream>();

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
  return client;
}

export async function redisFlushAllWithPrefix(prefix: string, client: RedisClient) {
  const keys = await client.keys(`${prefix}*`);
  const result = await client.del(keys);
  expect(result).toBe(keys.length);
}
