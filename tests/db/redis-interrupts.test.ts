import * as assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import * as Docker from 'dockerode';
import { waiter } from '@hirosystems/api-toolkit';
import { sleep, waiterNew } from '../../src/helpers';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  sendTestEvent,
} from './utils';
import { once } from 'node:events';

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
    eventServer = new EventObserverServer({ promRegistry, db, redisBroker });
    await eventServer.start({ port: 0, host: '127.0.0.1' });

    const redisContainerId = process.env['_REDIS_DOCKER_CONTAINER_ID']!;
    redisDockerContainer = new Docker().getContainer(redisContainerId);
  });

  afterAll(async () => {
    await closeTestClients();
    await eventServer.close();
    await db.close();
    await redisBroker.close();
  });

  beforeEach(async () => {
    await redisDockerContainer.restart();
    await Promise.all([
      once(redisBroker.client, 'ready'),
      once(redisBroker.ingestionClient, 'ready'),
      once(redisBroker.listeningClient, 'ready'),
    ]);
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

  test('Redis ingestion client connection is unavailable during event insertion', async () => {
    let lastDbMsg = await db.getLastMessage();

    ENV.DB_MSG_BATCH_SIZE = 10;
    ENV.MAX_MSG_LAG = 100;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    const client = await createTestClient(lastDbMsg?.sequence_number);

    const addRedisMsgThrow = waiterNew<{ msgId: string }>();
    const onRedisAddMsg = redisBroker._testRegisterOnAddStacksMsg(async msgId => {
      onRedisAddMsg.unregister();
      addRedisMsgThrow.finish({ msgId });
      await Promise.resolve();
      throw new Error('test redis add msg error');
    });

    const firstMsgsReceived = waiterNew<{ originalClientId: string }>();
    client.start(async (_id, _timestamp, _path, _body) => {
      if (!firstMsgsReceived.isFinished) {
        // Grab the original client ID before the client reconnects
        firstMsgsReceived.finish({ originalClientId: client.clientId });
      }
      return Promise.resolve();
    });

    // Process all msgs
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    const throwMsgPayload = { test: 'throw_on_this_msg' };
    await sendTestEvent(eventServer, throwMsgPayload);

    // Wait for redis server data to be wiped during the backfilling process
    const thrownMsgId = await addRedisMsgThrow;

    // Check that the msg we threw on during redis ingestion is what we expect
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    expect(lastDbMsg).toMatchObject({
      sequence_number: thrownMsgId.msgId,
      content: JSON.stringify(throwMsgPayload),
    });

    // We expect the client to not have received the message we threw on (simulating redis ingestion failure)
    expect(parseInt(client.lastMessageId.split('-')[0])).toBeLessThan(
      parseInt(thrownMsgId.msgId.split('-')[0])
    );

    // Send the next event which should trigger gap detection in the redis-broker ingestion,
    // and cause the client to reconnect to backfill missing messages.
    let msgGapDetected = false;
    redisBroker.events.once('ingestionMsgGapDetected', () => {
      msgGapDetected = true;
    });
    await sendTestEvent(eventServer, { test: 'post_throw_msg' });
    expect(msgGapDetected).toBe(true);

    // Ensure client was able to reconnect and receive the missing messages
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
          resolve();
        }
      });
      if (client.lastMessageId.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
        resolve();
      }
    });

    // The original client consumer redis stream should be pruned
    const { originalClientId } = await firstMsgsReceived;
    const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
    const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The original client consumer group on the global stream should be pruned
    const clientGroupKey = redisBroker.getClientGlobalStreamGroupKey(originalClientId);
    const globalStreamGroupExists = await redisBroker.client
      .xInfoConsumers(redisBroker.globalStreamKey, clientGroupKey)
      .then(
        () => {
          throw new Error('Expected xInfoConsumers to reject');
        },
        (error: Error) => {
          if (error?.message.includes('NOGROUP')) {
            return false;
          } else {
            throw error;
          }
        }
      );
    expect(globalStreamGroupExists).toBe(false);

    await client.stop();
    ENV.reload();
  });

  test('Redis server data wiped during event insertion', async () => {
    let lastDbMsg = await db.getLastMessage();

    ENV.DB_MSG_BATCH_SIZE = 10;
    ENV.MAX_MSG_LAG = 100;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    const client = await createTestClient(lastDbMsg?.sequence_number);

    // right after pg data is inserted, wipe redis data before inserting into redis
    const onRedisWiped = waiterNew<{ msgId: string }>();
    const onRedisAddMsg = redisBroker._testRegisterOnAddStacksMsg(async msgId => {
      onRedisAddMsg.unregister();
      await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
      onRedisWiped.finish({ msgId });
    });

    const firstMsgsReceived = waiterNew<{ originalClientId: string }>();
    client.start(async (_id, _timestamp, _path, _body) => {
      if (!firstMsgsReceived.isFinished) {
        // Grab the original client ID before the client reconnects
        firstMsgsReceived.finish({ originalClientId: client.clientId });
      }
      return Promise.resolve();
    });

    // Process all msgs
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    const onMsgAddedToEmptyRedisDb = waiterNew();
    redisBroker.events.once('ingestionToEmptyRedisDb', () => {
      onMsgAddedToEmptyRedisDb.finish();
    });

    const onClientRedisGroupDestroyed = waiterNew();
    client.events.once('redisConsumerGroupDestroyed', () => {
      onClientRedisGroupDestroyed.finish();
    });

    const emptyRedisMsgPayload = { test: 'redis_empty_on_this_msg' };
    await sendTestEvent(eventServer, emptyRedisMsgPayload);

    // Wait for redis server data to be wiped
    const wipedMsgId = await onRedisWiped;

    // Ensure the empty redis db condition was triggered
    await onMsgAddedToEmptyRedisDb;

    // Check that the msg we wiped after pg insertion is what we expect
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    expect(lastDbMsg).toMatchObject({
      sequence_number: wipedMsgId.msgId,
      content: JSON.stringify(emptyRedisMsgPayload),
    });

    // Ensure client was able to reconnect and receive the missing messages
    lastDbMsg = await db.getLastMessage();
    assert(lastDbMsg);
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
          resolve();
        }
      });
      if (client.lastMessageId.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
        resolve();
      }
    });

    // Client should notice the consumer group is destroyed
    await onClientRedisGroupDestroyed;

    // The original client consumer redis stream should be pruned
    const { originalClientId } = await firstMsgsReceived;
    const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
    const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The original client consumer group on the global stream should be pruned
    const clientGroupKey = redisBroker.getClientGlobalStreamGroupKey(originalClientId);
    const globalStreamGroupExists = await redisBroker.client
      .xInfoConsumers(redisBroker.globalStreamKey, clientGroupKey)
      .then(
        () => {
          throw new Error('Expected xInfoConsumers to reject');
        },
        (error: Error) => {
          if (error?.message.includes('NOGROUP')) {
            return false;
          } else {
            throw error;
          }
        }
      );
    expect(globalStreamGroupExists).toBe(false);

    await client.stop();
    ENV.reload();
  });
});
