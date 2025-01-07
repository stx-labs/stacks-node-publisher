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

    redisBroker = new RedisBroker({ redisUrl: ENV.REDIS_URL });
    await redisBroker.connect({ waitForReady: true });

    const promRegistry = new Registry();
    eventServer = new EventObserverServer({
      promRegistry: promRegistry,
      eventMessageHandler: async (eventPath, eventBody) => {
        const dbResult = await db.insertMessage(eventPath, eventBody);
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
    for await (const line of rl) {
      const [_id, _timestamp, path, payload] = line.split('\t');
      // use fetch to POST the payload to the event server
      const res = await fetch(eventServer.url + path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
      });
      if (res.status !== 200) {
        console.error(`Failed to POST event: ${path} - ${payload}`);
        throw new Error(`Failed to POST event: ${path} - ${payload}`);
      }
    }
    rl.close();
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

  test('client lib test', async () => {
    let lastMsgId = '0';
    const client = new StacksEventStream({
      redisUrl: ENV.REDIS_URL,
      eventStreamType: StacksEventStreamType.all,
      lastMessageId: lastMsgId,
    });
    await client.connect({ waitForReady: true });
    let messagesProcessed = 0;
    let lastTimestamp = 0;
    client.start(async (id, timestamp, path, body) => {
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
