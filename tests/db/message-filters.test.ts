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

  describe('stream messages filtered by stream type', () => {
    test('chain events', async () => {
      await testWithFailCb(async fail => {
        const lastDbMsg = await env.db.getLastMessage();
        assert(lastDbMsg);
        const client = await createTestClient(
          undefined,
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
        const allMsgsReceivedWaiter = waiter();

        let messagesReceived = 0;
        client.start(
          async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
          async (id: string, _timestamp: string, message: Message) => {
            messagesReceived++;
            if (id === '5399-0') {
              allMsgsReceivedWaiter.finish();
            }
            if (
              message.path === MessagePath.StackerDbChunks ||
              message.path === MessagePath.ProposalResponse
            ) {
              fail(new Error(`Unexpected message received: ${message.path}`));
            }
            return Promise.resolve();
          }
        );

        await withTimeout(allMsgsReceivedWaiter, 60_000);
        assert.equal(messagesReceived, 1430);

        await client.stop();
      });
    }, 60_000);

    test('confirmed chain events', async () => {
      await testWithFailCb(async fail => {
        const lastDbMsg = await env.db.getLastMessage();
        assert(lastDbMsg);
        const client = await createTestClient(
          undefined,
          [MessagePath.NewBlock, MessagePath.NewBurnBlock],
          error => {
            fail(error);
          }
        );
        const allMsgsReceivedWaiter = waiter();

        let messagesReceived = 0;
        client.start(
          async () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
          async (id: string, _timestamp: string, message: Message) => {
            messagesReceived++;
            if (id === '5396-0') {
              allMsgsReceivedWaiter.finish();
            }
            if (
              message.path === MessagePath.StackerDbChunks ||
              message.path === MessagePath.ProposalResponse ||
              message.path === MessagePath.NewMempoolTx ||
              message.path === MessagePath.DropMempoolTx ||
              message.path === MessagePath.NewMicroblocks ||
              message.path === MessagePath.AttachmentsNew
            ) {
              fail(new Error(`Unexpected message received: ${message.path}`));
            }
            return Promise.resolve();
          }
        );

        await withTimeout(allMsgsReceivedWaiter, 60_000);
        assert.equal(messagesReceived, 983);

        await client.stop();
      });
    }, 60_000);
  });
});
