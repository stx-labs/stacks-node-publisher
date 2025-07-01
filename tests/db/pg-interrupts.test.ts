import * as assert from 'node:assert/strict';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import * as Docker from 'dockerode';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  sendTestEvent,
  testWithFailCb,
} from './utils';
import { once } from 'node:events';
import { timeout, waiter } from '@hirosystems/api-toolkit';
import { StacksEventStreamType } from '../../client/src';

describe('Postgres interrupts', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;
  let redisDockerContainer: Docker.Container;
  let postgresDockerContainer: Docker.Container;

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

    const pgContainerId = process.env['_PG_DOCKER_CONTAINER_ID']!;
    postgresDockerContainer = new Docker().getContainer(pgContainerId);
  });

  afterAll(async () => {
    await closeTestClients();
    await eventServer.close();
    await db.close();
    await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
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

  test('event-observer server returns non-200 on failed pg insertion', async () => {
    // Send a test event to ensure the last ingested msg is not the one we're about to send
    const lastIngestedMsg = { test: 'pg_okay' };
    await sendTestEvent(eventServer, lastIngestedMsg);

    const onPgMsgInsertWatier = waiter();
    db._testHooks!.onMsgInserting.register(async () => {
      if (!onPgMsgInsertWatier.isFinished) {
        onPgMsgInsertWatier.finish();
        throw new Error('test pg insert error');
      }
      await Promise.resolve();
    });
    const testEventBody = { test: 'pg_ingestion_error' };
    let postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(false);
    expect(onPgMsgInsertWatier.isFinished).toBe(true);

    // Expect the last ingested msg is _not_ the one we just tried to send
    let lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(lastIngestedMsg);

    // Retry the failed event insertion (like stacks-core would)
    postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(true);

    // Ensure last ingested msg is the one we just sent
    lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });

  test('event-observer server returns non-200 when pg is down', async () => {
    // Send a test event to ensure the last ingested msg is not the one we're about to send
    const lastIngestedMsg = { test: 'pg_okay' };
    await sendTestEvent(eventServer, lastIngestedMsg);

    // Stop the postgres container
    await postgresDockerContainer.stop();
    const testEventBody = { test: 'pg_stopped' };
    let postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(false);

    // Restart postgres container
    await postgresDockerContainer.start();

    // Wait for the db to be ready
    while (true) {
      try {
        await db.sql`SELECT 1`;
        break;
      } catch (_error) {
        await timeout(50);
      }
    }

    // Expect the last ingested msg is _not_ the one we just tried to send
    let lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(lastIngestedMsg);

    // Retry the failed event insertion (like stacks-core would)
    postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(true);

    // Ensure last ingested msg is the one we just sent
    lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });

  test('client recovers after pg error during backfilling', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(
        lastDbMsg?.sequence_number,
        StacksEventStreamType.all,
        fail
      );

      let backfillQueries = 0;
      const onBackfillQueryError = waiter<{ msgId: string }>();
      const onPgBackfillQuery = redisBroker._testHooks!.onBeforePgBackfillQuery.register(
        async msgId => {
          backfillQueries++;
          if (backfillQueries === 2) {
            // On the second backfill pg query, stop the postgres container to trigger an error
            await postgresDockerContainer.stop();
            onPgBackfillQuery.unregister();
            onBackfillQueryError.finish({ msgId });
          }
        }
      );

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      client.start(async (_id, _timestamp, _path, _body) => {
        if (!firstMsgsReceived.isFinished) {
          // Grab the original client ID before the client reconnects
          firstMsgsReceived.finish({ originalClientId: client.clientId });
        }
        return Promise.resolve();
      });

      const onClientRedisGroupDestroyed = waiter();
      client.events.once('redisConsumerGroupDestroyed', () => {
        onClientRedisGroupDestroyed.finish();
      });

      // Wait for the backfill pg query to error, then restart pg
      await onBackfillQueryError;
      // Restart postgres container and wait for it to be ready
      await postgresDockerContainer.start();
      while (true) {
        try {
          await db.sql`SELECT 1`;
          break;
        } catch (_error) {
          await timeout(50);
        }
      }

      // Ensure client was able to reconnect and receive the missing messages
      lastDbMsg = await db.getLastMessage();
      assert(lastDbMsg);
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (client.lastMessageId.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });

      // Client should notice the consumer group was destroyed
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
});
