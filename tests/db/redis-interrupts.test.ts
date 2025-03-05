import * as assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import * as Docker from 'dockerode';
import { waiter } from '@hirosystems/api-toolkit';
import { sleep } from '../../src/helpers';
import { createTestClient, ensureSequenceMsgOrder, sendTestEvent } from './utils';

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

  beforeEach(async () => {
    await redisDockerContainer.restart();
  });

  test('events-observer POST success when redis unavailable', async () => {
    await redisDockerContainer.stop();
    const testEventBody = { test: 'redis_stopped' };
    const postEventResult = await sendTestEvent(eventServer, testEventBody);
    expect(postEventResult.status).toBe(200);
    const lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });

  test('client connect succeeds once redis is available', async () => {
    const lastDbMsg = await db.getLastMessage();
    const client = await createTestClient(lastDbMsg?.sequence_number);
    ensureSequenceMsgOrder(client);
    const testMsg1 = { test: randomUUID() };
    let lastMsgWaiter = waiter<any>();
    client.start((_id, _timestamp, _path, body) => {
      lastMsgWaiter.finish(body);
      return Promise.resolve();
    });

    // Client receives msg when redis is available
    await sendTestEvent(eventServer, testMsg1);
    let lastMsg = await lastMsgWaiter;
    lastMsgWaiter = waiter();
    expect(lastMsg).toEqual(testMsg1);

    // Client does not receive msg when redis is unavailable
    await redisDockerContainer.stop();
    const testMsg2 = { test: randomUUID() };
    await sendTestEvent(eventServer, testMsg2);
    expect(client.connectionStatus).toBe('reconnecting');
    await sleep(100);
    expect(lastMsgWaiter.isFinished).toBe(false);

    // Client receives msg once redis is available again
    await redisDockerContainer.start();
    lastMsg = await lastMsgWaiter;
    expect(client.connectionStatus).toBe('connected');
    expect(lastMsg).toEqual(testMsg2);

    await client.stop();
    expect(client.connectionStatus).toBe('ended');
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
