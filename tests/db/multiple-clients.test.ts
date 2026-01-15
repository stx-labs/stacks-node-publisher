import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  sendTestEvent,
  testWithFailCb,
  withTimeout,
} from './utils';
import { waiter } from '@hirosystems/api-toolkit';
import { StacksEventStreamType } from '../../client/src';

describe('Multiple clients tests', () => {
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
    eventServer = new EventObserverServer({ promRegistry, db, redisBroker });
    await eventServer.start({ port: 0, host: '127.0.0.1' });
  });

  afterAll(async () => {
    await closeTestClients();
    await eventServer.close();
    await db.close();
    await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
    await redisBroker.close();
  });

  afterEach(async () => {
    await closeTestClients();
    await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
  });

  test('Multiple clients started at the same last msg id', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.MAX_IDLE_TIME_MS = 200;
      ENV.LIVE_STREAM_BATCH_SIZE = 10;
      ENV.DB_MSG_BATCH_SIZE = 10;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client1 = await createTestClient(
        lastDbMsg?.sequence_number,
        StacksEventStreamType.all,
        fail
      );
      const client2 = await createTestClient(
        lastDbMsg?.sequence_number,
        StacksEventStreamType.all,
        fail
      );

      lastDbMsg = await db.getLastMessage();
      const client1BackfillCompleteWaiter = waiter<{ clientId: string }>();
      const client2BackfillCompleteWaiter = waiter<{ clientId: string }>();

      client1.start(
        async () => ({ messageId: client1.lastProcessedMessageId }),
        async (id: string, _timestamp: string, _path: string, _body: unknown) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            client1BackfillCompleteWaiter.finish({ clientId: client1.clientId });
          }
          await Promise.resolve();
        }
      );
      client2.start(
        async () => ({ messageId: client2.lastProcessedMessageId }),
        async (id: string, _timestamp: string, _path: string, _body: unknown) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            client2BackfillCompleteWaiter.finish({ clientId: client2.clientId });
          }
          await Promise.resolve();
        }
      );

      // Wait for both clients to finish backfilling
      const [client1OrigId, client2OrigId] = await Promise.all([
        withTimeout(client1BackfillCompleteWaiter),
        withTimeout(client2BackfillCompleteWaiter),
      ]);

      // Send new messages for live-streaming
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      // Ensure each client is able to continue processing all remaining messages
      const latestDbMsg = await db.getLastMessage();
      const client1CaughtUp = new Promise<void>(resolve => {
        client1.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (client1.lastProcessedMessageId.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });

      const client2CaughtUp = new Promise<void>(resolve => {
        client2.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (client2.lastProcessedMessageId.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });

      await Promise.all([
        // wait for client 1
        withTimeout(client1CaughtUp),
        // wait for client 2
        withTimeout(client2CaughtUp),
      ]);

      // Clients should have the original connection ID from when backfilling and until caught up after live-streaming
      expect(client1.clientId).toBe(client1OrigId.clientId);
      expect(client2.clientId).toBe(client2OrigId.clientId);

      await client1.stop();
      await client2.stop();
      ENV.reload();
    });
  });

  test('Multiple clients started from different last msg ids', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.MAX_IDLE_TIME_MS = 200;
      ENV.LIVE_STREAM_BATCH_SIZE = 10;
      ENV.DB_MSG_BATCH_SIZE = 10;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;

      // Client 1 will start from and ealier message than client 2
      const client1 = await createTestClient(
        lastDbMsg?.sequence_number,
        StacksEventStreamType.all,
        fail
      );

      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }
      lastDbMsg = await db.getLastMessage();
      // Client 2 will start from the latest message
      const client2 = await createTestClient(
        lastDbMsg?.sequence_number,
        StacksEventStreamType.all,
        fail
      );

      const client1BackfillCompleteWaiter = waiter<{ clientId: string }>();
      const client2BackfillCompleteWaiter = waiter<{ clientId: string }>();

      client1.start(
        async () => ({ messageId: client1.lastProcessedMessageId }),
        async (id: string, _timestamp: string, _path: string, _body: unknown) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            client1BackfillCompleteWaiter.finish({ clientId: client1.clientId });
          }
          await Promise.resolve();
        }
      );
      client2.start(
        async () => ({ messageId: client2.lastProcessedMessageId }),
        async (id: string, _timestamp: string, _path: string, _body: unknown) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            client2BackfillCompleteWaiter.finish({ clientId: client2.clientId });
          }
          await Promise.resolve();
        }
      );
      if (client2.lastProcessedMessageId.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
        client2BackfillCompleteWaiter.finish({ clientId: client2.clientId });
      }

      // Wait for both clients to finish backfilling
      const [client1OrigId, client2OrigId] = await Promise.all([
        withTimeout(client1BackfillCompleteWaiter),
        withTimeout(client2BackfillCompleteWaiter),
      ]);

      // Send new messages for live-streaming
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      // Ensure each client is able to continue processing all remaining messages
      const latestDbMsg = await db.getLastMessage();
      const client1CaughtUp = new Promise<void>(resolve => {
        client1.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (client1.lastProcessedMessageId.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });

      const client2CaughtUp = new Promise<void>(resolve => {
        client2.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (client2.lastProcessedMessageId.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
          resolve();
        }
      });

      await Promise.all([
        // wait for client 1
        withTimeout(client1CaughtUp),
        // wait for client 2
        withTimeout(client2CaughtUp),
      ]);

      // Clients should have the original connection ID from when backfilling and until caught up after live-streaming
      expect(client1.clientId).toBe(client1OrigId.clientId);
      expect(client2.clientId).toBe(client2OrigId.clientId);

      await client1.stop();
      await client2.stop();
      ENV.reload();
    });
  });
});
