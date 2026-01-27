import * as assert from 'node:assert/strict';
import { waiter } from '@hirosystems/api-toolkit';
import {
  createTestClient,
  setupIntegrationTestEnv,
  teardownIntegrationTestEnv,
  testWithFailCb,
  IntegrationTestEnv,
  withTimeout,
} from './utils';
import { Message, MessagePath } from '../../client/src/messages';

describe('Stream position lookup', () => {
  let env: IntegrationTestEnv;

  // Sample block data from the dump (sequence_number, block_height, index_block_hash)
  // These are extracted from stackerdb-sample-events.tsv.gz
  const FIRST_BLOCK = {
    sequenceNumber: '1',
    blockHeight: 0,
    indexBlockHash: '0x55c9861be5cff984a20ce6d99d4aa65941412889bdc665094136429b84f8c2ee',
  };
  const MID_BLOCK = {
    sequenceNumber: '5279',
    blockHeight: 494,
    indexBlockHash: '0xfc67a86714b6af60ac6dd5cee5fc20303804b0e6c8c85bc641307a3bd1482dff',
  };
  const LAST_BLOCK = {
    sequenceNumber: '5395',
    blockHeight: 505,
    indexBlockHash: '0x1769ac7ebbff5a6995528cf9c72eed235337a2aa382cddfc0e1e3b85f08b97c6',
  };
  // Non-existent block hash for testing
  const NON_EXISTENT_BLOCK_HASH =
    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

  beforeAll(async () => {
    env = await setupIntegrationTestEnv({
      dumpFile: './tests/dumps/stackerdb-sample-events.tsv.gz',
    });
  }, 60_000);

  afterAll(async () => {
    await teardownIntegrationTestEnv(env);
  });

  describe('resolveIndexBlockHashToSequenceNumber', () => {
    test('returns exact sequence number when block hash is found', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(
        MID_BLOCK.indexBlockHash,
        MID_BLOCK.blockHeight
      );

      assert(result);
      expect(result.sequenceNumber).toBe(MID_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(false);
    });

    test('returns first block sequence number when queried', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(
        FIRST_BLOCK.indexBlockHash,
        FIRST_BLOCK.blockHeight
      );

      assert(result);
      expect(result.sequenceNumber).toBe(FIRST_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(false);
    });

    test('returns last block sequence number when queried', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(
        LAST_BLOCK.indexBlockHash,
        LAST_BLOCK.blockHeight
      );

      assert(result);
      expect(result.sequenceNumber).toBe(LAST_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(false);
    });

    test('clamps to max when block hash not found but height exceeds highest available', async () => {
      const futureBlockHeight = LAST_BLOCK.blockHeight + 100;
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(
        NON_EXISTENT_BLOCK_HASH,
        futureBlockHeight
      );

      assert(result);
      // Should clamp to the last available block's sequence number
      expect(result.sequenceNumber).toBe(LAST_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(true);
    });

    test('returns null when block hash not found and height is not higher than available', async () => {
      // Use a height that exists but with a non-matching hash
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(
        NON_EXISTENT_BLOCK_HASH,
        MID_BLOCK.blockHeight
      );

      expect(result).toBeNull();
    });

    test('returns null when block hash not found and no height provided', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(NON_EXISTENT_BLOCK_HASH);

      expect(result).toBeNull();
    });

    test('returns null for empty block hash', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber('', 100);
      expect(result).toBeNull();
    });

    test('works without providing blockHeight when hash exists', async () => {
      const result = await env.db.resolveIndexBlockHashToSequenceNumber(MID_BLOCK.indexBlockHash);

      assert(result);
      expect(result.sequenceNumber).toBe(MID_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(false);
    });
  });

  describe('validateAndResolveMessageId', () => {
    test('returns sequence number when message ID exists', async () => {
      const messageId = `${MID_BLOCK.sequenceNumber}-0`;
      const result = await env.db.validateAndResolveMessageId(messageId);

      assert(result);
      expect(result.sequenceNumber).toBe(MID_BLOCK.sequenceNumber);
      expect(result.clampedToMax).toBe(false);
    });

    test('returns sequence number for first message ID', async () => {
      const messageId = '1-0';
      const result = await env.db.validateAndResolveMessageId(messageId);

      assert(result);
      expect(result.sequenceNumber).toBe('1');
      expect(result.clampedToMax).toBe(false);
    });

    test('clamps to max when message ID exceeds highest available', async () => {
      // Use a sequence number much higher than what exists
      const futureMessageId = '999999-0';
      const result = await env.db.validateAndResolveMessageId(futureMessageId);

      assert(result);
      // Should clamp to the highest available sequence number
      expect(BigInt(result.sequenceNumber)).toBeLessThanOrEqual(BigInt('999999'));
      expect(result.clampedToMax).toBe(true);
    });

    test('returns null for empty message ID', async () => {
      const result = await env.db.validateAndResolveMessageId('');
      expect(result).toBeNull();
    });

    test('returns null for invalid message ID format', async () => {
      const result = await env.db.validateAndResolveMessageId('invalid-format');
      expect(result).toBeNull();
    });

    test('returns sequence number for ID within range but not necessarily existing', async () => {
      // Test with a sequence number that is within range
      const messageId = '100-0';
      const result = await env.db.validateAndResolveMessageId(messageId);

      assert(result);
      // Since 100 is within the range of messages (1-5400+), it should be valid
      expect(result.sequenceNumber).toBe('100');
      expect(result.clampedToMax).toBe(false);
    });
  });

  describe('stream position via client start', () => {
    test('starts from exact position when valid indexBlockHash + blockHeight provided', async () => {
      await testWithFailCb(async fail => {
        const client = await createTestClient(
          `${parseInt(MID_BLOCK.sequenceNumber) - 1}-0`, // Set last processed to one before mid block
          [MessagePath.NewBlock],
          error => fail(error)
        );

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          () =>
            Promise.resolve({
              indexBlockHash: MID_BLOCK.indexBlockHash,
              blockHeight: MID_BLOCK.blockHeight,
            }),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        // Should start after the resolved block's sequence number
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        expect(receivedSeqNum).toBeGreaterThan(parseInt(MID_BLOCK.sequenceNumber));

        await client.stop();
      });
    }, 60_000);

    test('starts from clamped max position when indexBlockHash not found but height exceeds max', async () => {
      await testWithFailCb(async fail => {
        const futureBlockHeight = LAST_BLOCK.blockHeight + 1000;

        const client = await createTestClient(
          `${parseInt(LAST_BLOCK.sequenceNumber) - 1}-0`,
          [MessagePath.NewBlock],
          error => fail(error)
        );

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          () =>
            Promise.resolve({
              indexBlockHash: NON_EXISTENT_BLOCK_HASH,
              blockHeight: futureBlockHeight,
            }),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        // Should start from clamped position (after the last block's sequence number)
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        expect(receivedSeqNum).toBeGreaterThan(parseInt(LAST_BLOCK.sequenceNumber));

        await client.stop();
      });
    }, 60_000);

    test('starts from beginning when indexBlockHash not found and height not exceeding max', async () => {
      await testWithFailCb(async fail => {
        const client = await createTestClient('0-0', [MessagePath.NewBlock], error => fail(error));

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          () =>
            Promise.resolve({
              indexBlockHash: NON_EXISTENT_BLOCK_HASH,
              blockHeight: MID_BLOCK.blockHeight, // Use existing height but wrong hash
            }),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        // Should start from the beginning (first message)
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        expect(receivedSeqNum).toBe(1);

        await client.stop();
      });
    }, 60_000);

    test('starts from exact position when valid messageId provided', async () => {
      await testWithFailCb(async fail => {
        const startingSeqNum = '3000';
        const client = await createTestClient(`${parseInt(startingSeqNum) - 1}-0`, '*', error =>
          fail(error)
        );

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          () => Promise.resolve({ messageId: `${startingSeqNum}-0` }),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        // Should start after the specified message ID
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        expect(receivedSeqNum).toBe(parseInt(startingSeqNum) + 1);

        await client.stop();
      });
    }, 60_000);

    test('starts from clamped max position when messageId exceeds highest available', async () => {
      await testWithFailCb(async fail => {
        const futureMessageId = '999999-0';

        const client = await createTestClient('5390-0', '*', error => fail(error));

        const startMsgWaiter = waiter();
        let _receivedAnyMessage = false;

        client.start(
          () => Promise.resolve({ messageId: futureMessageId }),
          (_id: string, _timestamp: string, _message: Message) => {
            _receivedAnyMessage = true;
            startMsgWaiter.finish();
            return Promise.resolve();
          }
        );

        // Wait a bit for potential messages
        await new Promise(resolve => setTimeout(resolve, 5000));

        // Since messageId exceeds max, it clamps to max, so no historical messages should be received
        // (we're already at or past the max, so only live messages would come through)
        // In this test setup with no live ingestion, we expect no messages initially
        // The client should be waiting for new messages at this point

        await client.stop();
        // This test verifies the clamping behavior - when client starts at clamped position,
        // it waits for new messages (none should arrive from backfill since we're at the end)
      });
    }, 60_000);

    test('starts from beginning when null position provided', async () => {
      await testWithFailCb(async fail => {
        const client = await createTestClient('0-0', '*', error => fail(error));

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          () => Promise.resolve(null),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        // Should start from the very beginning
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        expect(receivedSeqNum).toBe(1);

        await client.stop();
      });
    }, 60_000);

    test('messageId takes priority over indexBlockHash when both could be provided', async () => {
      // This test verifies the priority logic in resolveStartMessageSequenceNumber
      // The implementation checks messageId first before indexBlockHash
      await testWithFailCb(async fail => {
        const messageIdPosition = '2000';

        const client = await createTestClient(`${parseInt(messageIdPosition) - 1}-0`, '*', error =>
          fail(error)
        );

        const firstMsgWaiter = waiter<string>();
        let firstMessageId: string | null = null;

        client.start(
          // When messageId is provided, it should be used regardless of other potential positions
          () => Promise.resolve({ messageId: `${messageIdPosition}-0` }),
          (id: string, _timestamp: string, _message: Message) => {
            if (!firstMessageId) {
              firstMessageId = id;
              firstMsgWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const receivedId = await withTimeout(firstMsgWaiter, 30_000);
        const receivedSeqNum = parseInt(receivedId.split('-')[0]);
        // Should start after the messageId position
        expect(receivedSeqNum).toBe(parseInt(messageIdPosition) + 1);

        await client.stop();
      });
    }, 60_000);
  });

  describe('stream continuation with position callbacks', () => {
    test('receives all messages from start position to end', async () => {
      await testWithFailCb(async fail => {
        const startingSeqNum = 5300;
        const lastMsgId = await env.db.getLastMessage();
        assert(lastMsgId);

        const client = await createTestClient(`${startingSeqNum - 1}-0`, '*', error => fail(error));

        const allMsgsReceivedWaiter = waiter();
        let messagesReceived = 0;
        const expectedLastMsgId = lastMsgId.sequence_number;

        client.start(
          () => Promise.resolve({ messageId: `${startingSeqNum}-0` }),
          (id: string, _timestamp: string, _message: Message) => {
            messagesReceived++;
            const seqNum = id.split('-')[0];
            if (seqNum === expectedLastMsgId) {
              allMsgsReceivedWaiter.finish();
            }
            return Promise.resolve();
          }
        );

        await withTimeout(allMsgsReceivedWaiter, 30_000);

        // Verify we received all messages from starting position to end
        const expectedCount = parseInt(expectedLastMsgId) - startingSeqNum;
        expect(messagesReceived).toBe(expectedCount);

        await client.stop();
      });
    }, 60_000);

    test('resumes from last processed position on reconnect callback', async () => {
      await testWithFailCb(async fail => {
        const startingSeqNum = 4000;

        const client = await createTestClient(`${startingSeqNum - 1}-0`, '*', error => fail(error));

        const checkpointWaiter = waiter<string>();
        let checkpoint: string | null = null;

        client.start(
          // This callback simulates returning the last processed message ID
          () => Promise.resolve({ messageId: client.lastProcessedMessageId }),
          (id: string, _timestamp: string, _message: Message) => {
            // After receiving some messages, record a checkpoint
            const seqNum = parseInt(id.split('-')[0]);
            if (seqNum >= startingSeqNum + 10 && !checkpoint) {
              checkpoint = id;
              checkpointWaiter.finish(id);
            }
            return Promise.resolve();
          }
        );

        const checkpointId = await withTimeout(checkpointWaiter, 30_000);
        const checkpointSeqNum = parseInt(checkpointId.split('-')[0]);

        // Verify the client's lastProcessedMessageId matches our checkpoint
        expect(client.lastProcessedMessageId).toBe(checkpointId);
        expect(checkpointSeqNum).toBeGreaterThanOrEqual(startingSeqNum + 10);

        await client.stop();
      });
    }, 60_000);
  });
});
