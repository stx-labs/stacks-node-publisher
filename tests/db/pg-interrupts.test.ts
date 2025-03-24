import * as assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgStore } from '../../src/pg/pg-store';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { Registry } from 'prom-client';
import { RedisBroker } from '../../src/redis/redis-broker';
import { ENV } from '../../src/env';
import * as Docker from 'dockerode';
import { waiter } from '@hirosystems/api-toolkit';
import { sleep, waiterNew } from '../../src/helpers';
import {
  closeTestClients,
  createTestClient,
  redisFlushAllWithPrefix,
  sendTestEvent,
} from './utils';
import { once } from 'node:events';

describe('Postgres interrupts', () => {
  let db: PgStore;
  let redisBroker: RedisBroker;
  let eventServer: EventObserverServer;
  let redisDockerContainer: Docker.Container;
  let postgresDockerContainer: Docker.Container;

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

    const redisContainerId = process.env['_REDIS_DOCKER_CONTAINER_ID']!;
    redisDockerContainer = new Docker().getContainer(redisContainerId);

    const pgContainerId = process.env['_PG_DOCKER_CONTAINER_ID']!;
    postgresDockerContainer = new Docker().getContainer(pgContainerId);
  });

  afterAll(async () => {
    await closeTestClients();
    await eventServer.close();
    await db.close();
    await redisFlushAllWithPrefix(redisBroker.redisStreamKeyPrefix, redisBroker.client);
    await redisBroker.close();
  });

  beforeEach(async () => {
    await redisDockerContainer.restart();
    await Promise.all([
      once(redisBroker.client, 'ready'),
      once(redisBroker.ingestionClient, 'ready'),
      once(redisBroker.listeningClient, 'ready'),
    ]);
  });

  test('event-observer server returns non-200 on failed pg insertion', async () => {
    // Send a test event to ensure the last ingested msg is not the one we're about to send
    const lastIngestedMsg = { test: 'pg_okay' };
    await sendTestEvent(eventServer, lastIngestedMsg);

    const onPgMsgInsertWatier = waiterNew();
    db._testHooks!.onMsgInserting.register(async () => {
      if (!onPgMsgInsertWatier.isFinished) {
        onPgMsgInsertWatier.finish();
        throw new Error('test pg insert error');
      }
      await Promise.resolve();
    });
    const testEventBody = { test: 'pg_ingestion_error' };
    let postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(false);
    expect(onPgMsgInsertWatier.isFinished).toBe(true);

    // Expect the last ingested msg is _not_ the one we just tried to send
    let lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(lastIngestedMsg);

    // Retry the failed event insertion (like stacks-core would)
    postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(true);

    // Ensure last ingested msg is the one we just sent
    lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });

  test('event-observer server returns non-200 when pg is down', async () => {
    // Send a test event to ensure the last ingested msg is not the one we're about to send
    const lastIngestedMsg = { test: 'pg_okay' };
    await sendTestEvent(eventServer, lastIngestedMsg);

    // Stop the postgres container
    await postgresDockerContainer.stop();
    const testEventBody = { test: 'pg_stopped' };
    let postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(false);

    // Restart postgres container
    await postgresDockerContainer.start();

    // Wait for the db to be ready
    while (true) {
      try {
        await db.sql`SELECT 1`;
        break;
      } catch (_error) {
        await sleep(50);
      }
    }

    // Expect the last ingested msg is _not_ the one we just tried to send
    let lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(lastIngestedMsg);

    // Retry the failed event insertion (like stacks-core would)
    postEventResult = await sendTestEvent(eventServer, testEventBody, false);
    expect(postEventResult.ok).toBe(true);

    // Ensure last ingested msg is the one we just sent
    lastDbMsg = await db.getLastMessage();
    assert.ok(lastDbMsg);
    expect(JSON.parse(lastDbMsg.content)).toEqual(testEventBody);
  });
});
