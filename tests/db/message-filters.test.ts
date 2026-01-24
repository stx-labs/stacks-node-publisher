import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import * as assert from 'node:assert/strict';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { waiter } from '@hirosystems/api-toolkit';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  testWithFailCb,
  withTimeout,
} from './utils';
import { Message, MessagePath } from '../../client/src/messages';

describe('Message filters', () => {
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

    // insert stacks-node events dump
    const payloadDumpFile = './tests/dumps/stackerdb-sample-events.tsv.gz';
    const rl = readline.createInterface({
      input: fs.createReadStream(payloadDumpFile).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
    // Suppress noisy logs during bulk insertion test
    const spyInfoLogs = [
      jest.spyOn(eventServer.logger, 'info').mockImplementation(() => {}),
      jest.spyOn(redisBroker.logger, 'info').mockImplementation(() => {}),
    ];
    for await (const line of rl) {
      const [_id, timestamp, path, payload] = line.split('\t');
      // use fetch to POST the payload to the event server
      const res = await fetch(eventServer.url + path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Original-Timestamp': timestamp },
        body: payload,
      });
      if (res.status !== 200) {
        throw new Error(`Failed to POST event: ${path} - ${payload.slice(0, 100)}`);
      }
    }
    rl.close();
    spyInfoLogs.forEach(spy => spy.mockRestore());
  }, 60_000);

  afterAll(async () => {
    await closeTestClients();
    await eventServer.close();
    await db.close();
    await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
    await redisBroker.close();
  });

  describe('stream messages filtered by stream type', () => {
    test('chain events', async () => {
      await testWithFailCb(async fail => {
        const lastDbMsg = await db.getLastMessage();
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
        const lastDbMsg = await db.getLastMessage();
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
