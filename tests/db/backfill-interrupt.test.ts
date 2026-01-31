import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { once, EventEmitter } from 'node:events';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  sendTestEvent,
  testWithFailCb,
  withTimeout,
} from './utils';
import * as assert from 'node:assert';
import { timeout, waiter } from '@hirosystems/api-toolkit';
import { Message } from '../../client/src/messages';

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

  test('Client stalls for MAX_IDLE_TIME_MS during pg backfill', async () => {
    await testWithFailCb(async fail => {
      const lastDbMsg = await db.getLastMessage();

      ENV.MAX_IDLE_TIME_MS = 200;

      ENV.DB_MSG_BATCH_SIZE = 10;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      let backfillHit = waiter<string>();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async msgId => {
        backfillHit.finish(msgId);
        onBackfill.unregister();
        return Promise.resolve();
      });

      const clientStallStartedWaiter = waiter();
      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, _message: Message) => {
          if (backfillHit.isFinished) {
            backfillHit = waiter();
            clientStallStartedWaiter.finish();
            await timeout(ENV.MAX_IDLE_TIME_MS * 2);
          }
        }
      );

      // Wait for the client to begin the msg ingestion stall
      await withTimeout(clientStallStartedWaiter);

      // The client consumer redis stream should still be alive
      const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
      const clientStreamInfo = await redisBroker.client.xInfoStream(clientStreamKey);
      let clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(1);
      expect(clientStreamInfo).toBeTruthy();
      expect(clientStreamInfo.length).toBeGreaterThan(0);

      // The client consumer group on the chain tip stream should still be alive
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(client.clientId);
      const chainTipStreamGroupInfo = await redisBroker.client.xInfoConsumers(
        redisBroker.chainTipStreamKey,
        clientGroupKey
      );
      expect(chainTipStreamGroupInfo).toBeTruthy();
      expect(chainTipStreamGroupInfo.length).toBeGreaterThan(0);

      // The client redis stream group should be pruned after the MAX_IDLE_TIME_MS
      const clientConsumerGroupDestroyed = withTimeout(
        new Promise<void>(resolve =>
          client.events.once('redisConsumerGroupDestroyed', () => {
            resolve();
          })
        )
      );

      // The server should prune the client consumer group
      const clientPruned = withTimeout(
        new Promise<void>(resolve =>
          redisBroker.events.once('idleConsumerPruned', () => {
            resolve();
          })
        )
      );

      // Await for >MAX_IDLE_TIME_MS then send event to trigger prune
      await timeout(ENV.MAX_IDLE_TIME_MS * 1.5);
      await sendTestEvent(eventServer, { test: 'msgPump' });

      await Promise.all([clientConsumerGroupDestroyed, clientPruned]);

      // The client consumer redis stream should be pruned
      clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(0);

      // The client consumer group on the chain tip stream should be pruned
      const chainTipStreamGroupExists = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
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
      expect(chainTipStreamGroupExists).toBe(false);

      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      // Ensure client is able to reconnect and continue processing messages
      const latestDbMsg = await db.getLastMessage();
      await withTimeout(
        new Promise<void>(resolve => {
          client.events.on('msgReceived', ({ id }) => {
            if (id.split('-')[0] === latestDbMsg?.sequence_number.split('-')[0]) {
              resolve();
            }
          });
          if (
            client.lastProcessedMessageId.split('-')[0] ===
            latestDbMsg?.sequence_number.split('-')[0]
          ) {
            resolve();
          }
        })
      );

      await client.stop();
      ENV.reload();
    });
  });

  test('Client in backfill mode is not affected by MAX_MSG_LAG until promoted to live streaming', async () => {
    // With the new design, during postgres backfill there is NO consumer group on the global
    // stream. This means:
    // 1. The client cannot be "demoted" during backfill (it's not promoted yet)
    // 2. MAX_MSG_LAG only applies once the client catches up and transitions to live streaming
    // 3. The chain tip stream can be trimmed freely during client backfill
    await testWithFailCb(async fail => {
      const lastDbMsg = await db.getLastMessage();

      ENV.MAX_IDLE_TIME_MS = 60_000;
      ENV.MAX_MSG_LAG = 100;

      ENV.DB_MSG_BATCH_SIZE = 10;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 2;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      let backfillHit = waiter<string>();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async msgId => {
        backfillHit.finish(msgId);
        onBackfill.unregister();
        return Promise.resolve();
      });

      const msgEvents = new EventEmitter();
      const clientStallStartedWaiter = waiter();
      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (id: string, _timestamp: string, _message: Message) => {
          msgEvents.emit('msg', id);
          if (backfillHit.isFinished) {
            clientStallStartedWaiter.finish();
            await timeout(300);
          }
        }
      );

      const msgSender = setInterval(() => {
        void sendTestEvent(eventServer, { test: 'msgPump' });
      }, 200);

      // Wait for the client to begin the msg ingestion stall
      await clientStallStartedWaiter;

      // The client consumer redis stream should still be alive
      const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
      const clientStreamInfo = await redisBroker.client.xInfoStream(clientStreamKey);
      const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(1);
      expect(clientStreamInfo).toBeTruthy();
      expect(clientStreamInfo.length).toBeGreaterThan(0);

      // During backfill, there should be NO consumer group on the chain tip stream
      // (consumer group is only created when client catches up with chain tip stream)
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(client.clientId);
      const chainTipStreamGroupExists = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
        .then(
          () => true,
          (error: Error) => {
            if (error?.message.includes('NOGROUP')) {
              return false;
            } else {
              throw error;
            }
          }
        );
      expect(chainTipStreamGroupExists).toBe(false);

      // Queue up over MAX_MSG_LAG messages - this should NOT affect the backfilling client
      for (let i = 0; i < ENV.MAX_MSG_LAG * 3; i++) {
        await sendTestEvent(eventServer, { laggingMsgNumber: i });
      }

      // Give some time for any potential (incorrect) pruning to occur
      await timeout(500);

      // The client stream should still exist (not pruned during backfill)
      const clientStreamStillExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamStillExists).toBe(1);

      // Set up promotion listener BEFORE allowing client to continue (to avoid race condition)
      const clientPromoted = once(redisBroker.events, 'consumerPromotedToLiveStream');

      // Remove the client msg ingestion sleep to allow it to catch up
      backfillHit = waiter();

      // Wait for the client to be promoted to live streaming after catching up
      await clientPromoted;

      // Now the consumer group should exist on the chain tip stream
      const chainTipStreamGroupExistsAfterPromotion = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
        .then(
          () => true,
          (error: Error) => {
            if (error?.message.includes('NOGROUP')) {
              return false;
            } else {
              throw error;
            }
          }
        );
      expect(chainTipStreamGroupExistsAfterPromotion).toBe(true);

      // Ensure client continues processing messages
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

  test('Client redis connection error during pg backfill', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);

      const backfillHit = waiter();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async _msgId => {
        try {
          const clientRedisConnectionID = await client.client.clientId();
          const clientKillCount = await redisBroker.client.clientKill({
            filter: 'ID',
            id: clientRedisConnectionID,
          });
          expect(clientKillCount).toBe(1);
          backfillHit.finish();
        } catch (error) {
          backfillHit.reject(error as Error);
        }

        onBackfill.unregister();
      });

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, _message: Message) => {
          if (!firstMsgsReceived.isFinished) {
            // Grab the original client ID before the client reconnects
            firstMsgsReceived.finish({ originalClientId: client.clientId });
          }
          return Promise.resolve();
        }
      );

      // Wait for client redis connection to be killed during the backfilling process
      await withTimeout(backfillHit);

      const { originalClientId } = await withTimeout(firstMsgsReceived);

      // Client should reconnect and continue processing messages
      lastDbMsg = await db.getLastMessage();
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      // Send over ENV.MAX_MSG_LAG messages to force the old and now disconnected stream to be pruned
      for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
        await sendTestEvent(eventServer, { laggingMsgNumber: i });
      }

      // The client consumer redis stream should be pruned
      const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
      const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(0);

      // The client consumer group on the chain tip stream should be pruned
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(originalClientId);
      const chainTipStreamGroupExists = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
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
      expect(chainTipStreamGroupExists).toBe(false);

      await client.stop();
      ENV.reload();
    });
  });

  test('Server redis connection for client is killed during pg backfill', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);

      const backfillHit = waiter();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async _msgId => {
        const perConsumerClient = [...redisBroker.perConsumerClients].find(
          ([_, entry]) => entry.clientId === client.clientId
        )?.[0];
        assert(perConsumerClient);
        const perConsumerClientRedisConnectionID = await perConsumerClient.clientId();
        const clientKillCount = await redisBroker.client.clientKill({
          filter: 'ID',
          id: perConsumerClientRedisConnectionID,
        });
        expect(clientKillCount).toBe(1);

        backfillHit.finish();

        onBackfill.unregister();
        return Promise.resolve();
      });

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, _message: Message) => {
          if (!firstMsgsReceived.isFinished) {
            // Grab the original client ID before the client reconnects
            firstMsgsReceived.finish({ originalClientId: client.clientId });
          }
          return Promise.resolve();
        }
      );

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
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      // Send over ENV.MAX_MSG_LAG messages to force the old and now disconnected stream to be pruned
      for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
        await sendTestEvent(eventServer, { laggingMsgNumber: i });
      }

      // The client consumer redis stream should be pruned
      const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
      const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(0);

      // The client consumer group on the chain tip stream should be pruned
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(originalClientId);
      const chainTipStreamGroupExists = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
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
      expect(chainTipStreamGroupExists).toBe(false);

      await client.stop();
      ENV.reload();
    });
  });

  test('Server global redis connection is killed during pg backfill', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);

      const backfillHit = waiter();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async _msgId => {
        const redisBrokerGlobalClientIds = await Promise.all(
          [redisBroker.client, redisBroker.listeningClient, redisBroker.ingestionClient].map(
            client => client.clientId()
          )
        );
        await Promise.all(
          redisBrokerGlobalClientIds.map(async clientId => {
            const clientKillCount = await client.client.clientKill({
              filter: 'ID',
              id: clientId,
            });
            expect(clientKillCount).toBe(1);
          })
        );

        backfillHit.finish();

        onBackfill.unregister();
        return Promise.resolve();
      });

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, _message: Message) => {
          if (!firstMsgsReceived.isFinished) {
            // Grab the original client ID before the client reconnects
            firstMsgsReceived.finish({ originalClientId: client.clientId });
          }
          return Promise.resolve();
        }
      );

      // Wait for redis-broker's global redis client connection to be killed during the backfilling process
      await backfillHit;

      const { originalClientId } = await firstMsgsReceived;

      // Client should be unaffected and continue processing messages
      lastDbMsg = await db.getLastMessage();
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      // Send over new messages to verify client is still receiving them
      for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
        await sendTestEvent(eventServer, { laggingMsgNumber: i });
      }
      lastDbMsg = await db.getLastMessage();
      assert(lastDbMsg);
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      // Original client ID should not have changed
      expect(originalClientId).toBe(client.clientId);

      // The client consumer redis stream should still be alive
      const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
      const clientStreamInfo = await redisBroker.client.xInfoStream(clientStreamKey);
      const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(1);
      expect(clientStreamInfo).toBeTruthy();
      expect(clientStreamInfo.length).toBe(0);

      // The client consumer group on the chain tip stream should still be alive
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(originalClientId);
      const chainTipStreamGroupInfo = await redisBroker.client.xInfoConsumers(
        redisBroker.chainTipStreamKey,
        clientGroupKey
      );
      expect(chainTipStreamGroupInfo).toBeTruthy();
      expect(chainTipStreamGroupInfo.length).toBeGreaterThan(0);

      await client.stop();
      ENV.reload();
    });
  });

  test('Redis server data is wiped (flushall) during pg backfill', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);

      const backfillHit = waiter();
      const onBackfill = redisBroker._testHooks!.onPgBackfillLoop.register(async _msgId => {
        await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
        backfillHit.finish();
        onBackfill.unregister();
        return Promise.resolve();
      });

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, _message: Message) => {
          if (!firstMsgsReceived.isFinished) {
            // Grab the original client ID before the client reconnects
            firstMsgsReceived.finish({ originalClientId: client.clientId });
          }
          return Promise.resolve();
        }
      );

      const onFirstMsgsReceived = await withTimeout(firstMsgsReceived);

      // Wait for redis server data to be wiped during the backfilling process
      const onBackfillHit = withTimeout(backfillHit);

      // Client should notice the consumer group is destroyed
      const onRedisConsumerGroupDestroyed = withTimeout(
        once(client.events, 'redisConsumerGroupDestroyed')
      );

      // New consumer redis client should be created
      const onPerConsumerClientCreated = withTimeout(
        once(redisBroker.events, 'perConsumerClientCreated')
      );

      await Promise.all([onBackfillHit, onRedisConsumerGroupDestroyed, onPerConsumerClientCreated]);

      const [newConsumerClient] = (await onPerConsumerClientCreated) as [{ clientId: string }];
      expect(newConsumerClient.clientId).toBe(client.clientId);

      const { originalClientId } = onFirstMsgsReceived;

      // Client should reconnect and continue processing messages
      lastDbMsg = await db.getLastMessage();
      assert(lastDbMsg);
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg?.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      // The original client consumer redis stream should be pruned
      const clientStreamKey = redisBroker.getClientStreamKey(originalClientId);
      const clientStreamExists = await redisBroker.client.exists(clientStreamKey);
      expect(clientStreamExists).toBe(0);

      // The original client consumer group on the chain tip stream should be pruned
      const clientGroupKey = redisBroker.getClientChainTipStreamGroupKey(originalClientId);
      const chainTipStreamGroupExists = await redisBroker.client
        .xInfoConsumers(redisBroker.chainTipStreamKey, clientGroupKey)
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
      expect(chainTipStreamGroupExists).toBe(false);

      await client.stop();
      ENV.reload();
    });
  });

  test('Msg ingested to pg exactly in the middle of client transition from backfilling to live-streaming', async () => {
    await testWithFailCb(async fail => {
      let lastDbMsg = await db.getLastMessage();

      ENV.DB_MSG_BATCH_SIZE = 10;
      ENV.MAX_MSG_LAG = 100;
      const msgFillCount = ENV.DB_MSG_BATCH_SIZE * 3;
      for (let i = 0; i < msgFillCount; i++) {
        await sendTestEvent(eventServer, { backfillMsgNumber: i });
      }

      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);

      const onTransitionToLive = waiter();
      const transitionMsgPayload = { msg: 'msg added during backfill to livestream transition' };
      const hook = redisBroker._testHooks!.onLiveStreamTransition.register(async () => {
        await sendTestEvent(eventServer, transitionMsgPayload);
        onTransitionToLive.finish();
        hook.unregister();
      });

      const firstMsgsReceived = waiter<{ originalClientId: string }>();
      const onTransitionMsgReceived = waiter();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (_id: string, _timestamp: string, message: Message) => {
          if (!firstMsgsReceived.isFinished) {
            // Grab the original client ID before the client reconnects
            firstMsgsReceived.finish({ originalClientId: client.clientId });
          }
          if (!onTransitionMsgReceived.isFinished) {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            if ((message.payload as any).msg === transitionMsgPayload.msg) {
              onTransitionMsgReceived.finish();
            }
          }
          return Promise.resolve();
        }
      );

      // Wait for first msg received
      const onFirstMsgsReceived = await withTimeout(firstMsgsReceived);

      // Wait for msg to be ingested during client transition to live streaming
      await withTimeout(onTransitionToLive);

      // Ensure client receives the msg ingested during transition to live streaming
      await withTimeout(onTransitionMsgReceived);

      // ClientID should be the same as from when first msg received (no resets)
      const { originalClientId } = onFirstMsgsReceived;
      expect(client.clientId).toEqual(originalClientId);

      // Send over new messages to verify client is still receiving them
      for (let i = 0; i < ENV.MAX_MSG_LAG * 2; i++) {
        await sendTestEvent(eventServer, { finalMsgsNumber: i });
      }
      lastDbMsg = await db.getLastMessage();
      assert(lastDbMsg);
      await new Promise<void>(resolve => {
        client.events.on('msgReceived', ({ id }) => {
          if (id.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]) {
            resolve();
          }
        });
        if (
          client.lastProcessedMessageId.split('-')[0] === lastDbMsg.sequence_number.split('-')[0]
        ) {
          resolve();
        }
      });

      await client.stop();
      ENV.reload();
    });
  });
});
