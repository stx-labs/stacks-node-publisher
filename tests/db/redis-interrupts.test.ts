import * as assert from 'node:assert/strict';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import * as Docker from 'dockerode';

describe('Redis interrupts', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;
  let redisDockerContainer: Docker.Container;

  beforeAll(async () => {
    db = await PgStore.connect();

    redisBroker = new RedisBroker({
      redisUrl: ENV.REDIS_URL,
      redisStreamKeyPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      db: db,
    });
    await redisBroker.connect({ waitForReady: true });

    const promRegistry = new Registry();
    eventServer = new EventObserverServer({
      promRegistry: promRegistry,
      eventMessageHandler: async (eventPath, eventBody, httpReceiveTimestamp) => {
        const dbResult = await db.insertMessage(eventPath, eventBody, httpReceiveTimestamp);
        await redisBroker.addStacksMessage({
          timestamp: dbResult.timestamp,
          sequenceNumber: dbResult.sequence_number,
          eventPath,
          eventBody,
        });
      },
    });
    await eventServer.start({ port: 0, host: '127.0.0.1' });

    const redisContainerId = process.env['_REDIS_DOCKER_CONTAINER_ID']!;
    redisDockerContainer = new Docker().getContainer(redisContainerId);
  });

  afterAll(async () => {
    await eventServer.close();
    await db.close();
    await redisBroker.close();
  });

  async function sendTestEvent(body: any = { test: 1 }) {
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

  async function createTestClient() {
    const client = new StacksEventStream({
      redisUrl: ENV.REDIS_URL,
      eventStreamType: StacksEventStreamType.all,
      lastMessageId: '0',
      redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      appName: 'snp-client-test',
    });
    await client.connect({ waitForReady: true });
    return client;
  }

  test('events-observer POST success when redis unavailable', async () => {
    await redisDockerContainer.stop();
    const testEventBody = { test: 'redis_stopped' };
    const postEventResult = await sendTestEvent(testEventBody);
    expect(postEventResult.status).toBe(200);
    const lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });

  // TODO: there's no way to specify a socket timeout in the node-redis client,
  // see: https://github.com/redis/node-redis/issues/1358
  // However, an abort signal can be specified in each command which we should consider using.
  /*
  test('events-observer POST success when redis connection does not respond in time', async () => {
    // This keeps the socket open but doesn't respond
    await redisDockerContainer.pause();
    const testEventBody = { test: 'redis_paused' };
    const postEventResult = await sendTestEvent();
    expect(postEventResult.status).toBe(200);
    const lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
    console.log('here');
  });
  */
});
