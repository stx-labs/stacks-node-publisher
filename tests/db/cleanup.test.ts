import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { StacksMessageStream } from '../../client/src';
import {
  closeTestClients,
  createTestClient,
  sendTestEvent,
  testWithFailCb,
  withTimeout,
} from './utils';
import { once } from 'node:events';
import { timeout, waiter } from '@stacks/api-toolkit';

describe('Cleanup tests', () => {
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

  describe('chain tip stream trim', () => {
    test('client connections are handled during trim', async () => {
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

  describe('client prune', () => {
    test('client reconnects after no-message timeout on orphaned stream', async () => {
      // Use a unique prefix so the server doesn't process this client's handshake, simulating an
      // orphaned stream where no messages are ever delivered.
      const noMsgTimeoutMs = 2000;
      const client = new StacksMessageStream({
        appName: 'snp-client-test-timeout',
        redisUrl: ENV.REDIS_URL,
        redisStreamPrefix: 'test-no-msg-timeout:',
        options: { noMessageTimeoutMs: noMsgTimeoutMs },
      });
      await client.connect({ waitForReady: true });

      client.start(
        async () => Promise.resolve(null),
        async () => Promise.resolve()
      );

      // Wait for the first ingestEventStream call to initialize and set its clientId.
      const connectionAnnounced = once(client.events, 'connectionAnnounced');
      const [{ clientId: firstClientId }] = await withTimeout(connectionAnnounced);

      // No messages will arrive since the server doesn't know about this prefix. After the
      // no-message timeout fires, the client should reconnect with a new clientId.
      const noMessageTimeoutReconnect = once(client.events, 'noMessageTimeoutReconnect');
      const [{ clientId: noMessageTimeoutClientId }] = await withTimeout(noMessageTimeoutReconnect);
      expect(noMessageTimeoutClientId).toBe(firstClientId);

      // The client should reconnect with a new clientId.
      const newConnectionAnnounced = once(client.events, 'connectionAnnounced');
      const [{ clientId: newClientId }] = await withTimeout(newConnectionAnnounced);
      expect(newClientId).not.toBe(firstClientId);

      await client.stop();
    });

    test('slow backfill client is not pruned because it is not idle', async () => {
      await testWithFailCb(async fail => {
        ENV.DB_MSG_BATCH_SIZE = 10;
        ENV.MAX_IDLE_TIME_MS = 200;
        ENV.MAX_STUCK_TIME_MS = 120_000;
        ENV.CLIENT_REDIS_STREAM_MAX_LEN = 10;

        // Fill some messages so the client has data to backfill
        const msgCount = 20;
        for (let i = 0; i < msgCount; i++) {
          await sendTestEvent(eventServer, { slowClientTest: i });
        }
        // Run cleanup to trim the chain tip stream. This will force the client to be in backfill
        // mode only.
        await redisBroker.cleanup();

        // The client will stall on the first received message, leaving pending entries in its
        // consumer stream (pelCount > 0).
        const client = await createTestClient(null, '*', fail);
        const stallStarted = waiter();
        const stall = waiter();
        client.start(
          async () => Promise.resolve(null),
          async () => {
            if (!stallStarted.isFinished) {
              stallStarted.finish();
            }
            // Stall for longer than MAX_IDLE_TIME_MS but much less than MAX_STUCK_TIME_MS
            await stall;
          }
        );

        // Wait for the client to start processing (and stalling on) the first message
        await withTimeout(stallStarted);
        // Wait long enough for the client stream consumer to exceed MAX_IDLE_TIME_MS
        await timeout(ENV.MAX_IDLE_TIME_MS * 2);

        // Run prune — the client should NOT be pruned because it has pending messages, so the
        // threshold is MAX_STUCK_TIME_MS (much higher) instead of MAX_IDLE_TIME_MS.
        await redisBroker.cleanup();
        const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
        const exists = await redisBroker.client.exists(clientStreamKey);
        expect(exists).toBe(1);

        stall.finish();
        await client.stop();
      });
    });

    test('clients are not pruned if there is no chain activity', async () => {
      await testWithFailCb(async fail => {
        ENV.MAX_IDLE_TIME_MS = 200;

        // Send some events so the chain tip stream is populated and lastChainTipMsgTime is set.
        const msgCount = 3;
        for (let i = 0; i < msgCount; i++) {
          await sendTestEvent(eventServer, { noChainActivityTest: i });
        }

        // Create a live-streaming client
        const lastDbMsg = await db.getLastMessage();
        const client = await createTestClient(lastDbMsg?.sequence_number, '*', fail);
        const promoted = once(redisBroker.events, 'consumerPromotedToLiveStream');
        const liveStreamWaiter = waiter();
        client.start(
          async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
          async () => {
            liveStreamWaiter.finish();
            return Promise.resolve();
          }
        );
        await withTimeout(promoted);

        // Send one more event and wait for the client to receive it, confirming the client
        // stream consumer has a valid activeTime.
        await sendTestEvent(eventServer, { noChainActivityTest: 'live' });
        await withTimeout(liveStreamWaiter);

        // Now stop sending events and wait longer than MAX_IDLE_TIME_MS. This simulates a
        // quiet chain where no new blocks arrive. The client's activeTime will go stale
        // because no messages are being delivered to the client stream.
        await timeout(ENV.MAX_IDLE_TIME_MS * 2);

        // Run cleanup — the client should NOT be pruned because the chain itself has been
        // quiet (lastChainTipMsgTime is also stale), so idle detection is skipped.
        await redisBroker.cleanup();

        // Verify the client stream still exists
        const clientStreamKey = redisBroker.getClientStreamKey(client.clientId);
        const exists = await redisBroker.client.exists(clientStreamKey);
        expect(exists).toBe(1);

        await client.stop();
      });
    });
  });
});
