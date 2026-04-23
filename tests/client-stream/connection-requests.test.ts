import { timeout, waiter } from '@stacks/api-toolkit';
import {
  createTestClient,
  setupIntegrationTestEnv,
  teardownIntegrationTestEnv,
  testWithFailCb,
  IntegrationTestEnv,
  sendTestEvent,
  withTimeout,
} from '../utils.js';
import { before, after, test, describe } from 'node:test';

describe('Connection request handling', () => {
  let env: IntegrationTestEnv;

  before(async () => {
    env = await setupIntegrationTestEnv();
  }, { timeout: 30_000 });

  after(async () => {
    await teardownIntegrationTestEnv(env);
  });

  test('drops malformed connection requests and continues processing valid requests', { timeout: 60_000 }, async () => {
    await testWithFailCb(async fail => {
      const connectionStreamKey = `${env.redisBroker.redisStreamKeyPrefix}connection_stream`;
      const malformedMsgId = await env.redisBroker.client.xAdd(connectionStreamKey, '*', {
        client_id: 'malformed-client',
        app_name: 'malformed-app',
        last_index_block_hash: '',
        last_block_height: '',
        last_message_id: '',
        selected_paths: '{"invalid_json"',
      });

      const client = await createTestClient(null, '*', fail);
      const firstMessage = waiter<string>();
      client.start(
        () => Promise.resolve(null),
        async id => {
          if (!firstMessage.isFinished) {
            firstMessage.finish(id);
          }
          return Promise.resolve();
        }
      );

      await sendTestEvent(env.eventServer, { test: 'connection-request-after-malformed' });
      await withTimeout(firstMessage, 30_000);

      // The malformed connection request should be explicitly deleted from the stream.
      await withTimeout(
        (async () => {
          while (true) {
            const malformedMsg = await env.redisBroker.client.xRange(
              connectionStreamKey,
              malformedMsgId,
              malformedMsgId
            );
            if (malformedMsg.length === 0) {
              return;
            }
            await timeout(50);
          }
        })(),
        5_000
      );

      await client.stop();
    });
  });
});
