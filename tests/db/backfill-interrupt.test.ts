import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { sleep, waiterNew } from '../../src/helpers';
import { once, EventEmitter } from 'node:events';
import { createTestClient, ensureSequenceMsgOrder, sendTestEvent } from './utils';
import { ClientKillFilters } from '@redis/client/dist/lib/commands/CLIENT_KILL';
import * as assert from 'node:assert';

describe('Backfill tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;

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
  });

  afterAll(async () => {
    await eventServer.close();
    await db.close();
    await redisBroker.close();
  });

  test('Client stalls for MAX_IDLE_TIME_MS during pg backfill', async () => {
    const lastDbMsg = await db.getLastMessage();

    ENV.MAX_IDLE_TIME_MS = 200;

    ENV.DB_MSG_BATCH_SIZE = 10;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    let backfillHit = waiterNew<string>();
    const onBackfill = redisBroker._testRegisterOnPgBackfillLoop(async msgId => {
      backfillHit.finish(msgId);
      onBackfill.unregister();
      return Promise.resolve();
    });

    const msgEvents = new EventEmitter();
    const clientStallStartedWaiter = waiterNew();
    const client = await createTestClient(lastDbMsg?.sequence_number);
    ensureSequenceMsgOrder(client);
    client.start(async (id, _timestamp, _path, _body) => {
      msgEvents.emit('msg', id);
      if (backfillHit.isFinished) {
        backfillHit = waiterNew();
        clientStallStartedWaiter.finish();
        await sleep(ENV.MAX_IDLE_TIME_MS * 2);
      }
    });

    const msgSender = setInterval(() => {
      void sendTestEvent(eventServer, { test: 'msgPump' });
    }, 200);

    // Wait for the client to begin the msg ingestion stall
    await clientStallStartedWaiter;

    // The client consumer redis stream should still be alive
    const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
    const clientStreamInfo = await redisBroker.client.xInfoStream(clientStreamKey);
    let clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(1);
    expect(clientStreamInfo).toBeTruthy();
    expect(clientStreamInfo.length).toBeGreaterThan(0);

    // The client consumer group on the global stream should still be alive
    const clientGroupKey = redisBroker.getClientGlobalStreamGroupKey(client.clientId);
    const globalStreamGroupInfo = await redisBroker.client.xInfoConsumers(
      redisBroker.globalStreamKey,
      clientGroupKey
    );
    expect(globalStreamGroupInfo).toBeTruthy();
    expect(globalStreamGroupInfo.length).toBeGreaterThan(0);

    // The client redis stream group should be pruned after the MAX_IDLE_TIME_MS
    const clientConsumerGroupDestroyed = once(client.events, 'redisConsumerGroupDestroyed');
    // The server should the client consumer group
    const clientPruned = once(redisBroker.events, 'idleConsumerPruned');
    await Promise.all([clientConsumerGroupDestroyed, clientPruned]);

    // The client consumer redis stream should be pruned
    clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The client consumer group on the global stream should be pruned
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

    // Ensure client is able to reconnect and continue processing messages
    const latestDbMsg = await db.getLastMessage();
    await new Promise<void>(resolve => {
      msgEvents.on('msg', (id: string) => {
        if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    clearInterval(msgSender);
    await client.stop();
    ENV.reload();
  });

  test('Client lags past MAX_MSG_LAG threshold during pg backfill', async () => {
    const lastDbMsg = await db.getLastMessage();

    ENV.MAX_IDLE_TIME_MS = 60_000;
    ENV.MAX_MSG_LAG = 100;

    ENV.DB_MSG_BATCH_SIZE = 10;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 2;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    let backfillHit = waiterNew<string>();
    const onBackfill = redisBroker._testRegisterOnPgBackfillLoop(async msgId => {
      backfillHit.finish(msgId);
      onBackfill.unregister();
      return Promise.resolve();
    });

    const msgEvents = new EventEmitter();
    const clientStallStartedWaiter = waiterNew();
    const client = await createTestClient(lastDbMsg?.sequence_number);
    ensureSequenceMsgOrder(client);
    client.start(async (id, _timestamp, _path, _body) => {
      msgEvents.emit('msg', id);
      if (backfillHit.isFinished) {
        clientStallStartedWaiter.finish();
        await sleep(300);
      }
    });

    const msgSender = setInterval(() => {
      void sendTestEvent(eventServer, { test: 'msgPump' });
    }, 200);

    // Wait for the client to begin the msg ingestion stall
    await clientStallStartedWaiter;

    // The client consumer redis stream should still be alive
    const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
    const clientStreamInfo = await redisBroker.client.xInfoStream(clientStreamKey);
    let clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(1);
    expect(clientStreamInfo).toBeTruthy();
    expect(clientStreamInfo.length).toBeGreaterThan(0);

    // The client consumer group on the global stream should still be alive
    const clientGroupKey = redisBroker.getClientGlobalStreamGroupKey(client.clientId);
    const globalStreamGroupInfo = await redisBroker.client.xInfoConsumers(
      redisBroker.globalStreamKey,
      clientGroupKey
    );
    expect(globalStreamGroupInfo).toBeTruthy();
    expect(globalStreamGroupInfo.length).toBeGreaterThan(0);

    // The client redis stream group should be pruned after the MAX_MSG_LAG threshold is hit
    const clientConsumerGroupDestroyed = once(client.events, 'redisConsumerGroupDestroyed');

    // The server should prune the client consumer group after the MAX_MSG_LAG threshold is hit
    const clientPruned = once(redisBroker.events, 'laggingConsumerPruned');

    // Queue up over MAX_MSG_LAG messages to force the client to be pruned
    for (let i = 0; i < ENV.MAX_MSG_LAG * 3; i++) {
      await sendTestEvent(eventServer, { laggingMsgNumber: i });
    }

    await clientPruned;

    // Remove the client msg ingestion sleep to allow it to catch up
    backfillHit = waiterNew();

    await clientConsumerGroupDestroyed;

    // The client consumer redis stream should be pruned
    clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The client consumer group on the global stream should be pruned
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

    // Ensure client is able to reconnect and continue processing messages
    const latestDbMsg = await db.getLastMessage();
    await new Promise<void>(resolve => {
      msgEvents.on('msg', (id: string) => {
        if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    clearInterval(msgSender);
    await client.stop();
    ENV.reload();
  });

  test('Client redis connection error during pg backfill', async () => {
    let lastDbMsg = await db.getLastMessage();

    ENV.DB_MSG_BATCH_SIZE = 10;
    ENV.MAX_MSG_LAG = 100;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    const client = await createTestClient(lastDbMsg?.sequence_number);
    ensureSequenceMsgOrder(client);

    const backfillHit = waiterNew();
    const onBackfill = redisBroker._testRegisterOnPgBackfillLoop(async _msgId => {
      const clientRedisConnectionID = await client.client.clientId();
      const clientKillCount = await redisBroker.client.clientKill({
        filter: ClientKillFilters.ID,
        id: clientRedisConnectionID,
      });
      expect(clientKillCount).toBe(1);
      expect(client.connectionStatus).toBe('reconnecting');

      backfillHit.finish();

      onBackfill.unregister();
      return Promise.resolve();
    });

    const firstMsgsReceived = waiterNew<{ originalClientId: string }>();
    client.start(async (_id, _timestamp, _path, _body) => {
      if (!firstMsgsReceived.isFinished) {
        // Grab the original client ID before the client reconnects
        firstMsgsReceived.finish({ originalClientId: client.clientId });
      }
      return Promise.resolve();
    });

    // Wait for client redis connection to be killed during the backfilling process
    await backfillHit;

    const { originalClientId } = await firstMsgsReceived;

    // Client should reconnect and continue processing messages
    lastDbMsg = await db.getLastMessage();
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    // Send over ENV.MAX_MSG_LAG messages to force the old and now disconnected stream to be pruned
    for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
      await sendTestEvent(eventServer, { laggingMsgNumber: i });
    }

    // The client consumer redis stream should be pruned
    const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
    const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The client consumer group on the global stream should be pruned
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

  test('Server redis connection for client is killed during pg backfill', async () => {
    let lastDbMsg = await db.getLastMessage();

    ENV.DB_MSG_BATCH_SIZE = 10;
    ENV.MAX_MSG_LAG = 100;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent(eventServer, { backfillMsgNumber: i });
    }

    const client = await createTestClient(lastDbMsg?.sequence_number);
    ensureSequenceMsgOrder(client);

    const backfillHit = waiterNew();
    const onBackfill = redisBroker._testRegisterOnPgBackfillLoop(async _msgId => {
      const perConsumerClient = [...redisBroker.perConsumerClients].find(
        ([_, entry]) => entry.clientId === client.clientId
      )?.[0];
      assert(perConsumerClient);
      const perConsumerClientRedisConnectionID = await perConsumerClient.clientId();
      const clientKillCount = await redisBroker.client.clientKill({
        filter: ClientKillFilters.ID,
        id: perConsumerClientRedisConnectionID,
      });
      expect(clientKillCount).toBe(1);

      backfillHit.finish();

      onBackfill.unregister();
      return Promise.resolve();
    });

    const firstMsgsReceived = waiterNew<{ originalClientId: string }>();
    client.start(async (_id, _timestamp, _path, _body) => {
      if (!firstMsgsReceived.isFinished) {
        // Grab the original client ID before the client reconnects
        firstMsgsReceived.finish({ originalClientId: client.clientId });
      }
      return Promise.resolve();
    });

    // Wait for per-consumer redis client connection to be killed during the backfilling process
    await backfillHit;

    // Client should notice the consumer group is destroyed
    await once(client.events, 'redisConsumerGroupDestroyed');

    // New consumer redis client should be created
    const [newConsumerClient] = (await once(redisBroker.events, 'perConsumerClientCreated')) as [
      { clientId: string },
    ];
    expect(newConsumerClient.clientId).toBe(client.clientId);

    const { originalClientId } = await firstMsgsReceived;

    // Client should reconnect and continue processing messages
    lastDbMsg = await db.getLastMessage();
    await new Promise<void>(resolve => {
      client.events.on('msgReceived', ({ id }) => {
        if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });
    });

    // Send over ENV.MAX_MSG_LAG messages to force the old and now disconnected stream to be pruned
    for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
      await sendTestEvent(eventServer, { laggingMsgNumber: i });
    }

    // The client consumer redis stream should be pruned
    const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
    const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
    expect(clientStreamExists).toBe(0);

    // The client consumer group on the global stream should be pruned
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
