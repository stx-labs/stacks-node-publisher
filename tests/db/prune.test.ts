import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { closeTestClients, createTestClient, sendTestEvent, testWithFailCb } from './utils';
import { StacksEventStreamType } from '../../client/src';

describe('Prune tests', () => {
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
    await redisBroker.close();
  });

  /*
  // Client msg pump
  let lastClientMsgId = 0;
  let clientResponseWaiter = waiter<number>();
  let clientContinueWaiter = waiter();

  client.start(async id => {
    lastClientMsgId = parseInt(id.split('-')[0]);
    clientResponseWaiter.finish(lastClientMsgId);
    await clientContinueWaiter;
    clientResponseWaiter = waiter();
    clientContinueWaiter = waiter();
  });
  */

  test('clients connecting during global stream trim', async () => {
    await testWithFailCb(async fail => {
      // Global stream not yet initialized
      let trimResult = await redisBroker.trimGlobalStream();
      expect(trimResult).toEqual({ result: 'no_stream_exists' });

      await sendTestEvent(eventServer);

      // No consumers, expect trim to maxlen
      trimResult = await redisBroker.trimGlobalStream();
      expect(trimResult).toEqual({ result: 'trimmed_maxlen' });

      const client = await createTestClient(undefined, StacksEventStreamType.all, fail);

      const lastClientMsgId = await new Promise<number>(resolve => {
        client.start(
          async () => ({ messageId: client.lastProcessedMessageId }),
          async (id: string) => {
            resolve(parseInt(id.split('-')[0]));
            return Promise.resolve();
          }
        );
      });

      // One consumer still processing a msg, expect trim to minid of the last msg received
      trimResult = await redisBroker.trimGlobalStream();
      expect(trimResult).toEqual({ result: 'trimmed_minid', id: lastClientMsgId });
      await client.stop();

      const testFn = redisBroker._testHooks!.onTrimGlobalStreamGetGroups.register(async () => {
        // This is called in the middle of the trim operation, add a new consumer
        const newClient = await createTestClient(undefined, StacksEventStreamType.all, fail);
        // Wait for the client to receive a message so that we know its group is registered on the server
        await new Promise<void>(resolve => {
          newClient.start(
            async () => ({ messageId: newClient.lastProcessedMessageId }),
            async () => {
              resolve();
              return Promise.resolve();
            }
          );
        });
        await newClient.stop();
        testFn.unregister();
      });
      // Expect the trim to be aborted because a new consumer was added
      trimResult = await redisBroker.trimGlobalStream();
      expect(trimResult.result).toBe('aborted');
    });
  });
});
