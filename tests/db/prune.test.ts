import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import {
  closeTestClients,
  createTestClient,
  sendTestEvent,
  testWithFailCb,
  withTimeout,
} from './utils';
import { once } from 'node:events';
import { waiter } from '@hirosystems/api-toolkit';

describe('Prune tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;

  beforeAll(async () => {
    db = await PgStore.connect();

    ENV.CLEANUP_INTERVAL_MS = 120_000;
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
    await redisBroker.close();
  });

  test('clients connecting during chain tip stream trim', async () => {
    await testWithFailCb(async fail => {
      // Global stream not yet initialized
      let trimResult = await redisBroker.trimChainTipStream();
      expect(trimResult).toEqual({ result: 'no_stream_exists' });

      await sendTestEvent(eventServer);

      // No consumers, expect trim to maxlen
      trimResult = await redisBroker.trimChainTipStream();
      expect(trimResult).toEqual({ result: 'trimmed_maxlen' });

      // Create a new live-streaming client
      const lastDbMsg = await db.getLastMessage();
      const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);
      // Wait for the client to be promoted to live streaming so it creates its consumer group.
      const promoted = once(redisBroker.events, 'consumerPromotedToLiveStream');
      // Wait for the client to receive a live-streamed message.
      const liveStreamWaiter = waiter<number>();
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (id: string) => {
          liveStreamWaiter.finish(parseInt(id.split('-')[0]));
          return Promise.resolve();
        }
      );
      await withTimeout(promoted);
      await sendTestEvent(eventServer);
      const lastClientMsgId = await liveStreamWaiter;

      // Expect trim to minid of the last msg received
      trimResult = await redisBroker.trimChainTipStream();
      expect(trimResult).toEqual({ result: 'trimmed_minid', id: lastClientMsgId });
      await client.stop();

      // Create a client that will try to create a new consumer group on the chain tip stream
      // during the trim operation.
      const newClient = await createTestClient(undefined, '*', fail);
      const testFn = redisBroker._testHooks!.onTrimChainTipStreamGetGroups.register(async () => {
        const promoted = once(redisBroker.events, 'consumerPromotedToLiveStream');
        newClient.start(
          async () => Promise.resolve({ messageId: newClient.lastProcessedMessageId }),
          async () => Promise.resolve()
        );
        await promoted;
        testFn.unregister();
      });
      // Expect the trim to be aborted.
      trimResult = await redisBroker.trimChainTipStream();
      await newClient.stop();
      expect(trimResult?.result).toBe('aborted');
    });
  });
});
