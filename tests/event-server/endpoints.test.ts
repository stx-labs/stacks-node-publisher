import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import assert from 'node:assert/strict';
import { PgStore } from '../../src/pg/pg-store.js';
import { EventObserverServer } from '../../src/event-observer/event-server.js';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker.js';
import { ENV } from '../../src/env.js';
import { createClient } from 'redis';
import { StacksMessageStream } from '../../client/src/index.js';
import { timeout } from '@stacks/api-toolkit';
import { buildPromServer } from '../../src/prom/prom-server.js';
import { FastifyInstance } from 'fastify';
import { Message } from '../../client/src/messages/index.js';
import { before, after, test, describe } from 'node:test';

describe('Endpoint tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;
  let promServer: FastifyInstance;

  before(async () => {
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

    promServer = await buildPromServer({ registry: promRegistry });
    await promServer.listen({ host: ENV.OBSERVER_HOST, port: 0 });

    // insert stacks-node events dump
    const payloadDumpFile = './tests/dumps/epoch-3-transition.tsv.gz';
    const rl = readline.createInterface({
      input: fs.createReadStream(payloadDumpFile).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
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
  });

  after(async () => {
    await eventServer.close();
    await db.close();
    await redisBroker.close();
    await promServer.close();
  });

  test('status endpoint check', async () => {
    const res = await fetch(eventServer.url + '/status');
    assert.strictEqual(res.status, 200);
    const body = await res.json();
    assert.match(body.server_version, /^stacks-node-publisher v/);
    assert.strictEqual(body.status, 'ready');
  });

  test('prom server test', async () => {
    const addrs = promServer.addresses();
    const promUrl = `http://${addrs[0].address}:${addrs[0].port}/metrics`;
    const res = await fetch(promUrl);
    assert.strictEqual(res.status, 200);
    assert.match(res.headers.get('content-type')!, /^text\/plain/);
    const body = await res.text();
    assert.match(body, /snp_http_request_duration_seconds_bucket/);
    const expectedLinePrefix =
      'snp_http_request_duration_seconds_count{method="POST",route="/new_block",status_code="200"}';
    assert.match(body, new RegExp(`^${expectedLinePrefix}\\s*\\d+$`, 'm'));
  });

  test('event metrics are registered and populated', async () => {
    const addrs = promServer.addresses();
    const promUrl = `http://${addrs[0].address}:${addrs[0].port}/metrics`;
    const res = await fetch(promUrl);
    const body = await res.text();

    const metricValue = (line: string): number => {
      const match = body.match(
        new RegExp(`^${line.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+(.+)$`, 'm')
      );
      assert.notStrictEqual(match, null);
      return parseFloat(match![1]);
    };

    // HTTP request duration summary should be present for /new_block POSTs
    assert.match(body, /snp_http_request_summary_seconds\{/);

    // Last event received timestamp should be a populated unix timestamp.
    // Avoid asserting strict recency here because the bulk dump is ingested in beforeAll
    // and slower CI / DB performance can legitimately make the last event older.
    const lastEventTs = metricValue('snp_stacks_event_last_received_timestamp{route="/new_block"}');
    const nowSeconds = Date.now() / 1000;
    assert.strictEqual(Number.isFinite(lastEventTs), true);
    assert.ok(lastEventTs > 0);
    assert.ok(lastEventTs <= nowSeconds + 1);

    // Last block height should be > 0 after ingesting the dump
    const blockHeight = metricValue('snp_stacks_last_block_height');
    assert.ok(blockHeight > 0);

    // Block action "write" count should match the number of blocks ingested
    const blockWrites = metricValue('snp_stacks_block_action_total{action="write"}');
    assert.ok(blockWrites > 0);

    // Skip mode should be 0 (the dump has a clean chain with no repeated blocks)
    const skipMode = metricValue('snp_stacks_event_skip_mode');
    assert.strictEqual(skipMode, 0);

    // Event errors counter: no errors expected, but the metric type should be registered
    assert.match(body, /^# TYPE snp_stacks_event_errors_total counter$/m);

    // Payload bytes histogram should have observations for /new_block
    assert.match(body, /snp_stacks_event_payload_bytes_count\{route="\/new_block"\}/);
    const payloadCount = metricValue('snp_stacks_event_payload_bytes_count{route="/new_block"}');
    assert.ok(payloadCount > 0);

    // Queue depth should be low after all events have been processed (may be 1 if the
    // last task's `finally` update races with the metric scrape)
    const queueDepth = metricValue('snp_stacks_event_queue_depth');
    assert.ok(queueDepth <= 1);
  });

  test.skip('stream messages from redis', async () => {
    const appRedisClient = createClient({
      url: ENV.REDIS_URL,
      name: 'stacks-node-publisher-server-client-test',
    });
    await appRedisClient.connect();
    const streamKey = ENV.REDIS_STREAM_KEY_PREFIX + 'all';

    const queuedMessageCount = await appRedisClient.xLen(streamKey);
    assert.ok(queuedMessageCount > 0);

    let lastMsgId = '0';
    let messagedProcessed = 0;
    for (let i = 0; i < queuedMessageCount; i++) {
      const streamMessages = (await appRedisClient.xRead(
        { key: streamKey, id: lastMsgId },
        { BLOCK: 3000, COUNT: 1 }
      )) as { name: string; messages: { id: string; message: Record<string, string> }[] }[] | null;
      assert.ok(streamMessages);
      assert.strictEqual(streamMessages.length, 1);
      assert.strictEqual(streamMessages[0].messages.length, 1);
      lastMsgId = streamMessages[0].messages[0].id;
      messagedProcessed++;
    }
    assert.strictEqual(messagedProcessed, queuedMessageCount);
    assert.strictEqual(lastMsgId, `${queuedMessageCount}-0`);
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

  test.skip('client lib test', async () => {
    redisBroker._testHooks!.onLiveStreamTransition.register(async () => {
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

    const streamDrainedCb = redisBroker._testHooks!.onLiveStreamDrained.register(async () => {
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
    const client = new StacksMessageStream({
      appName: 'stacks-node-publisher-server-client-test',
      redisUrl: ENV.REDIS_URL,
      redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      options: {
        selectedMessagePaths: '*',
      },
    });
    await client.connect({ waitForReady: true });
    let messagesProcessed = 0;
    let lastTimestamp = 0;
    client.start(
      async () => Promise.resolve(lastMsgId === '0' ? null : { messageId: lastMsgId }),
      async (id: string, timestamp: string, message: Message) => {
        assert.strictEqual(id, `${parseInt(lastMsgId.split('-')[0]) + 1}-0`);
        lastMsgId = id;

        assert.strictEqual(typeof message.path, 'string');
        assert.notStrictEqual(message.path, '');

        assert.strictEqual(typeof message.payload, 'object');
        assert.ok(Object.entries(message.payload as object).length > 0);

        assert.ok(parseInt(timestamp) >= lastTimestamp);
        lastTimestamp = parseInt(timestamp);

        messagesProcessed++;
        await Promise.resolve();
      }
    );
    await timeout(500);
    await client.stop();
    assert.ok(messagesProcessed > 0);
  });
});
