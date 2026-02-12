import * as assert from 'node:assert/strict';
import { waiter } from '@stacks/api-toolkit';
import {
  createTestClient,
  setupIntegrationTestEnv,
  teardownIntegrationTestEnv,
  testWithFailCb,
  IntegrationTestEnv,
  withTimeout,
} from './utils';
import { Message, MessagePath } from '../../client/src/messages';

describe('Message filters', () => {
  let env: IntegrationTestEnv;

  beforeAll(async () => {
    env = await setupIntegrationTestEnv({
      dumpFile: './tests/dumps/stackerdb-sample-events.tsv.gz',
    });
  }, 60_000);

  afterAll(async () => {
    await teardownIntegrationTestEnv(env);
  });

  test('no filter sends all messages', async () => {
    await testWithFailCb(async fail => {
      const lastDbMsg = await env.db.getLastMessage();
      assert(lastDbMsg);
      const client = await createTestClient(null, '*', error => {
        fail(error);
      });

      let messagesReceived = 0;
      const lastMsgId = waiter();
      client.start(
        async () => Promise.resolve(null),
        async (id: string) => {
          messagesReceived++;
          if (id === `${lastDbMsg.sequence_number}-0`) {
            lastMsgId.finish();
          }
          return Promise.resolve();
        }
      );

      await withTimeout(lastMsgId, 10_000);
      const countRes = await env.db.sql<
        { count: string }[]
      >`SELECT COUNT(*) AS count FROM messages`;
      assert.equal(parseInt(countRes[0].count), messagesReceived);

      await client.stop();
    });
  }, 10_000);

  test('filter excludes signer messages', async () => {
    await testWithFailCb(async fail => {
      const client = await createTestClient(
        null,
        [
          MessagePath.NewBlock,
          MessagePath.NewBurnBlock,
          MessagePath.NewMempoolTx,
          MessagePath.DropMempoolTx,
          MessagePath.NewMicroblocks,
          MessagePath.AttachmentsNew,
        ],
        error => {
          fail(error);
        }
      );

      let messagesReceived = 0;
      const lastMsgId = waiter();
      client.start(
        async () => Promise.resolve(null),
        async (id: string, _timestamp: string, message: Message) => {
          messagesReceived++;
          if (
            message.path === MessagePath.StackerDbChunks ||
            message.path === MessagePath.ProposalResponse
          ) {
            fail(new Error(`Unexpected message received: ${message.path}`));
          }
          // Corresponds to the last mempool tx message in the dump file
          if (id === `5103-0`) {
            lastMsgId.finish();
          }
          return Promise.resolve();
        }
      );

      await withTimeout(lastMsgId, 10_000);
      assert.equal(messagesReceived, 1135);

      await client.stop();
    });
  }, 10_000);
});
