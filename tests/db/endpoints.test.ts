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
import { StacksEventStream, StacksEventStreamType } from '../../client/src';
import { timeout } from '@hirosystems/api-toolkit';
import { buildPromServer } from '../../src/prom/prom-server';
import { FastifyInstance } from 'fastify';

describe('Endpoint tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;
  let promServer: FastifyInstance;

  beforeAll(async () => {
    db = await PgStore.connect();

    redisBroker = new RedisBroker({
      redisUrl: ENV.REDIS_URL,
      redisStreamKeyPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      db: db,
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

    promServer = await buildPromServer({ registry: promRegistry });
    await promServer.listen({ host: ENV.OBSERVER_HOST, port: 0 });

    // insert stacks-node events dump
    const payloadDumpFile = './tests/dumps/epoch-3-transition.tsv.gz';
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
    await promServer.close();
  });

  test('status endpoint check', async () => {
    const res = await fetch(eventServer.url + '/status');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toMatchObject({
      server_version: expect.stringMatching(/^salt-n-pepper v/),
      status: 'ready',
    });
  });

  test('prom server test', async () => {
    const addrs = promServer.addresses();
    const promUrl = `http://${addrs[0].address}:${addrs[0].port}/metrics`;
    const res = await fetch(promUrl);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toMatch(/^text\/plain/);
    const body = await res.text();
    expect(body).toMatch(/http_request_duration_seconds_bucket/);
    const expectedLinePrefix =
      'http_request_duration_seconds_count{method="POST",route="/new_block",status_code="200"}';
    expect(body).toMatch(new RegExp(`^${expectedLinePrefix}\\s*\\d+$`, 'm'));
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

  /* Robustness test scenarios:
   *  - Multiple clients started at the same time with the same lastMessageId
   *  - Multiple clients started at the same time with different lastMessageIds
   *  - Client stalls for MAX_IDLE during pg backfill
   *  - Client stalls for MAX_IDLE during redis live-streaming
   *  - Client activity is within MAX_IDLE but exceeds MAX_MSG_LAG threshold during pg backfill
   *  - Client activity is within MAX_IDLE but exceeds MAX_MSG_LAG threshold during redis live-streaming
   *  - Client redis network connection is killed during pg backfill
   *  - Client redis network connection is killed during redis live-streaming
   *  - Server redis connection for client is killed during pg backfill
   *  - Server redis connection for client is killed during redis live-streaming
   *  - Server global redis connection is killed during pg backfill
   *  - Server global redis connection is killed during redis live-streaming
   *  - Redis server data is wiped (redis-cli flushall) during pg backfill
   *  - Redis server data is wiped (redis-cli flushall) during redis live-streaming
   *  - Redis ingestion client connection is killed and unavailable right after pg insertion
   *  - Redis server data is wiped (redis-cli flushall) right after pg insertion
   *  - Msg added to pg exactly in the middle of client transition from backfilling to live-streaming
   */

  test('client lib test', async () => {
    redisBroker.testRegisterOnLiveStreamTransition(async () => {
      for (let i = 0; i < 10; i++) {
        const res = await fetch(eventServer.url + '/test_path', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ test: 'transition test data', i }),
        });
        if (res.status !== 200) {
          throw new Error(`Failed to POST event: ${res.status}`);
        }
      }
    });

    const streamDrainedCb = redisBroker.testOnLiveStreamDrained(async () => {
      for (let i = 0; i < 10; i++) {
        const res = await fetch(eventServer.url + '/test_path', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ test: 'stream drained data', i }),
        });
        if (res.status !== 200) {
          throw new Error(`Failed to POST event: ${res.status}`);
        }
      }
      streamDrainedCb.unregister();
    });

    let lastMsgId = '0';
    const client = new StacksEventStream({
      redisUrl: ENV.REDIS_URL,
      eventStreamType: StacksEventStreamType.all,
      lastMessageId: lastMsgId,
      redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    });
    await client.connect({ waitForReady: true });
    let messagesProcessed = 0;
    let lastTimestamp = 0;
    client.start(async (id, timestamp, path, body) => {
      expect(id).toEqual(`${parseInt(lastMsgId.split('-')[0]) + 1}-0`);
      lastMsgId = id;

      expect(typeof path).toBe('string');
      expect(path).not.toBe('');

      expect(typeof body).toBe('object');
      expect(Object.entries(body as object).length).toBeGreaterThan(0);

      expect(parseInt(timestamp)).toBeGreaterThanOrEqual(lastTimestamp);
      lastTimestamp = parseInt(timestamp);

      messagesProcessed++;
      await Promise.resolve();
    });
    await timeout(500);
    await client.stop();
    expect(messagesProcessed).toBeGreaterThan(0);
  });
});
