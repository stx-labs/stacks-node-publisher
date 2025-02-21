import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import * as assert from 'node:assert/strict';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { createClient } from 'redis';

describe('Stackerdb ingestion tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;

  beforeAll(async () => {
    db = await PgStore.connect();

    redisBroker = new RedisBroker({
      redisUrl: ENV.REDIS_URL,
      redisStreamKeyPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    });
    await redisBroker.connect({ waitForReady: true });

    const promRegistry = new Registry();
    eventServer = new EventObserverServer({
      promRegistry: promRegistry,
      eventMessageHandler: async (eventPath, eventBody, httpReceiveTimestamp) => {
        const dbResult = await db.insertMessage(eventPath, eventBody, httpReceiveTimestamp);
        await redisBroker.addStacksMessage({
          timestamp: dbResult.timestamp,
          sequenceNumber: dbResult.sequence_number,
          eventPath,
          eventBody,
        });
      },
    });
    await eventServer.start({ port: 0, host: '127.0.0.1' });

    // insert stacks-node events dump
    const payloadDumpFile = './tests/dumps/stackerdb-sample-events.tsv.gz';
    const rl = readline.createInterface({
      input: fs.createReadStream(payloadDumpFile).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
    const spyInfoLog = jest.spyOn(eventServer.logger, 'info').mockImplementation(() => {}); // Suppress noisy logs during bulk insertion test
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
    spyInfoLog.mockRestore();
  });

  afterAll(async () => {
    await eventServer.close();
    await db.close();
    await redisBroker.close();
  });

  test('stream messages from redis', async () => {
    const appRedisClient = createClient({
      url: ENV.REDIS_URL,
      name: 'salt-n-pepper-server-client-test',
    });
    await appRedisClient.connect();
    const streamKey = ENV.REDIS_STREAM_KEY_PREFIX + 'all';

    const queuedMessageCount = await appRedisClient.xLen(streamKey);
    expect(queuedMessageCount).toBeGreaterThan(0);

    let lastMsgId = '0';
    let messagedProcessed = 0;
    for (let i = 0; i < queuedMessageCount; i++) {
      const streamMessages = await appRedisClient.xRead(
        { key: streamKey, id: lastMsgId },
        { BLOCK: 3000, COUNT: 1 }
      );
      assert.ok(streamMessages);
      expect(streamMessages).toHaveLength(1);
      expect(streamMessages[0].messages).toHaveLength(1);
      lastMsgId = streamMessages[0].messages[0].id;
      messagedProcessed++;
    }
    expect(messagedProcessed).toBe(queuedMessageCount);
    expect(lastMsgId).toBe(`${queuedMessageCount}-0`);
    await appRedisClient.quit();
  });
});
