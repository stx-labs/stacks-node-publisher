import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import { timeout } from '@hirosystems/api-toolkit';

describe('Endpoint tests', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;

  beforeAll(async () => {
    db = await PgStore.connect();

    redisBroker = new RedisBroker({ redisUrl: ENV.REDIS_URL });
    await redisBroker.connect({ waitForReady: true });

    eventServer = new EventObserverServer({
      promRegistry: new Registry(),
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

    await timeout(10000000);
  });

  afterAll(async () => {
    await eventServer.close();
    await db.close();
    await redisBroker.close();
  });

  test('1+1', () => {
    console.log('1+1', 1 + 1);
  });
});
