import * as assert from 'node:assert/strict';
import { waiter } from '@stacks/api-toolkit';
import {
  createTestClient,
  setupIntegrationTestEnv,
  teardownIntegrationTestEnv,
  testWithFailCb,
  IntegrationTestEnv,
  withTimeout,
} from '../utils';

describe('Stackerdb ingestion tests', () => {
  let env: IntegrationTestEnv;

  beforeAll(async () => {
    env = await setupIntegrationTestEnv({
      dumpFile: './tests/dumps/stackerdb-sample-events.tsv.gz',
    });
  }, 60_000);

  afterAll(async () => {
    await teardownIntegrationTestEnv(env);
  });

  test('stream messages', async () => {
    await testWithFailCb(async fail => {
      const lastDbMsg = await env.db.getLastMessage();
      assert(lastDbMsg);
      const lastDbMsgId = parseInt(lastDbMsg.sequence_number.split('-')[0]);
      const client = await createTestClient(undefined, '*', error => {
        fail(error);
      });

      const allMsgsReceivedWaiter = waiter();

      let lastReceivedMsgId = 0;
      client.start(
        async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
        async (id: string) => {
          const msgId = parseInt(id.split('-')[0]);
          expect(msgId).toBe(lastReceivedMsgId + 1);
          lastReceivedMsgId = msgId;
          // Check if all msgs that are in pg have been received by the client
          if (msgId === lastDbMsgId) {
            allMsgsReceivedWaiter.finish();
          }
          return Promise.resolve();
        }
      );

      await withTimeout(allMsgsReceivedWaiter, 60_000);

      await client.stop();
    });
  }, 60_000);
});
