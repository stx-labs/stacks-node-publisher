import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import { sleep, waiterNew } from '../../src/helpers';
import { once, EventEmitter } from 'node:events';

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

  async function sendTestEvent(body: any = { test: 1 }) {
    const res = await fetch(eventServer.url + '/test_path', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.status !== 200) {
      throw new Error(`Failed to POST event: ${res.status}`);
    }
  }

  async function createTestClient(lastMsgId = '0') {
    const client = new StacksEventStream({
      redisUrl: ENV.REDIS_URL,
      eventStreamType: StacksEventStreamType.all,
      lastMessageId: lastMsgId,
      redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      appName: 'snp-client-test',
    });
    await client.connect({ waitForReady: true });
    return client;
  }

  test('Client stalls for MAX_IDLE_TIME_MS during pg backfill', async () => {
    const lastDbMsg = await db.getLastMessage();

    ENV.MAX_IDLE_TIME_MS = 200;

    ENV.DB_MSG_BATCH_SIZE = 10;
    const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
    for (let i = 0; i < msgFillCount; i++) {
      await sendTestEvent({ backfillMsgNumber: i });
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
    client.start(async (id, _timestamp, _path, _body) => {
      msgEvents.emit('msg', id);
      if (backfillHit.isFinished) {
        backfillHit = waiterNew();
        clientStallStartedWaiter.finish();
        await sleep(ENV.MAX_IDLE_TIME_MS * 2);
      }
    });

    const msgSender = setInterval(() => {
      void sendTestEvent({ test: 'msgPump' });
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
      await sendTestEvent({ backfillMsgNumber: i });
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
    client.start(async (id, _timestamp, _path, _body) => {
      msgEvents.emit('msg', id);
      if (backfillHit.isFinished) {
        clientStallStartedWaiter.finish();
        await sleep(300);
      }
    });

    const msgSender = setInterval(() => {
      void sendTestEvent({ test: 'msgPump' });
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
      await sendTestEvent({ laggingMsgNumber: i });
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
});
