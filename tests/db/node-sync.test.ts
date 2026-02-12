import * as assert from 'node:assert/strict';
import {
  setupIntegrationTestEnv,
  teardownIntegrationTestEnv,
  IntegrationTestEnv,
  loadEventsDump,
} from './utils';

describe('Node synchronization', () => {
  let env: IntegrationTestEnv;

  const DUMP_FILE = './tests/dumps/stackerdb-sample-events.tsv.gz';

  // Sample block data from the dump (sequence_number, block_height, index_block_hash)
  // These are extracted from stackerdb-sample-events.tsv.gz
  const BLOCK_50 = {
    sequenceNumber: '488',
    blockHeight: 50,
    indexBlockHash: '0x1fd613a7a80f52379cedaa57fd1d3b26daf34057d05155e4a352b8acf90eea61',
  };
  const BLOCK_100 = {
    sequenceNumber: '1061',
    blockHeight: 100,
    indexBlockHash: '0x5489c498ab1221056b0e2b6dfda009a80a8d979f07f4f61c8e7a551a20c9c884',
  };
  const BLOCK_495 = {
    sequenceNumber: '5284',
    blockHeight: 495,
    indexBlockHash: '0xfc67a86714b6af60ac6dd5cee5fc20303804b0e6c8c85bc641307a3bd1482dff',
  };
  const BLOCK_505 = {
    sequenceNumber: '5402',
    blockHeight: 505,
    indexBlockHash: '0x1769ac7ebbff5a6995528cf9c72eed235337a2aa382cddfc0e1e3b85f08b97c6',
  };

  beforeAll(async () => {
    env = await setupIntegrationTestEnv();
  }, 60_000);

  afterAll(async () => {
    await teardownIntegrationTestEnv(env);
  });

  test('syncs ordered blocks on an empty database', async () => {
    await loadEventsDump(env.eventServer, env.redisBroker, DUMP_FILE, {
      stopAtMsgId: BLOCK_100.sequenceNumber,
    });
    const lastBlock = await env.db.getLastIngestedBlockIdentifier();
    assert(lastBlock);
    expect(lastBlock.index_block_hash).toBe(BLOCK_100.indexBlockHash);
    expect(lastBlock.block_height).toBe(BLOCK_100.blockHeight);
  });

  test('rejects block with missing parent', async () => {
    await expect(
      loadEventsDump(env.eventServer, env.redisBroker, DUMP_FILE, {
        // Would start at block 495, which has a missing parent
        startAtMsgId: BLOCK_495.sequenceNumber,
      })
    ).rejects.toThrow();
    const lastBlock = await env.db.getLastIngestedBlockIdentifier();
    assert(lastBlock);
    // Should still be on the last block we ingested
    expect(lastBlock.index_block_hash).toBe(BLOCK_100.indexBlockHash);
    expect(lastBlock.block_height).toBe(BLOCK_100.blockHeight);
  });

  test('ignore off-order genesis block', async () => {
    await expect(
      loadEventsDump(env.eventServer, env.redisBroker, DUMP_FILE, {
        startAtMsgId: '1',
        stopAtMsgId: '1',
      })
    ).resolves.not.toThrow();
    const lastBlock = await env.db.getLastIngestedBlockIdentifier();
    assert(lastBlock);
    expect(lastBlock.index_block_hash).toBe(BLOCK_100.indexBlockHash);
  });

  test('ignores previously ingested blocks', async () => {
    const lastMessage = await env.db.getLastMessage();
    assert(lastMessage);
    const lastSequenceNumber = lastMessage.sequence_number;

    // Try from block 50 to block 100, should not ingest any messages
    await expect(
      loadEventsDump(env.eventServer, env.redisBroker, DUMP_FILE, {
        startAtMsgId: BLOCK_50.sequenceNumber,
        stopAtMsgId: BLOCK_100.sequenceNumber,
      })
    ).resolves.not.toThrow();

    // Should still be on the last block we ingested
    const lastBlock = await env.db.getLastIngestedBlockIdentifier();
    assert(lastBlock);
    expect(lastBlock.index_block_hash).toBe(BLOCK_100.indexBlockHash);
    expect(lastBlock.block_height).toBe(BLOCK_100.blockHeight);
    // Last message ID is the same
    const newLastMessage = await env.db.getLastMessage();
    assert(newLastMessage);
    expect(newLastMessage.sequence_number).toBe(lastSequenceNumber);
  });

  test('continues ingesting after new block is received', async () => {
    await loadEventsDump(env.eventServer, env.redisBroker, DUMP_FILE, {
      startAtMsgId: BLOCK_100.sequenceNumber,
    });
    const lastBlock = await env.db.getLastIngestedBlockIdentifier();
    assert(lastBlock);
    expect(lastBlock.index_block_hash).toBe(BLOCK_505.indexBlockHash);
    expect(lastBlock.block_height).toBe(BLOCK_505.blockHeight);
  });
});
