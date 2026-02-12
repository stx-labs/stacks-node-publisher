import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import { Registry } from 'prom-client';
import { StacksMessageStream, SelectedMessagePaths } from '../../client/src';
import { ENV } from '../../src/env';
import { EventObserverServer } from '../../src/event-observer/event-server';
import { PgStore } from '../../src/pg/pg-store';
import { RedisBroker } from '../../src/redis/redis-broker';
import { RedisClient } from '../../src/redis/redis-types';

export type IntegrationTestEnv = {
  db: PgStore;
  redisBroker: RedisBroker;
  eventServer: EventObserverServer;
};

/**
 * Sets up a test environment with PgStore, RedisBroker, and EventObserverServer.
 * Optionally loads a dump file into the database.
 */
export async function setupIntegrationTestEnv(options?: {
  /** Path to a .tsv.gz dump file to load into the database */
  dumpFile?: string;
  /** Skip all events in the dump file before this message ID (inclusive — this is the first event ingested) */
  dumpStartAtMsgId?: string;
  /** Stop ingestion at this message ID (inclusive — this is the last event ingested) */
  dumpStopAtMsgId?: string;
}): Promise<IntegrationTestEnv> {
  const db = await PgStore.connect();

  const redisBroker = new RedisBroker({
    redisUrl: ENV.REDIS_URL,
    redisStreamKeyPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    db: db,
  });
  await redisBroker.connect({ waitForReady: true });

  const promRegistry = new Registry();
  const eventServer = new EventObserverServer({ promRegistry, db, redisBroker });
  await eventServer.start({ port: 0, host: '127.0.0.1' });

  if (options?.dumpFile) {
    await loadEventsDump(eventServer, redisBroker, options.dumpFile, {
      startAtMsgId: options.dumpStartAtMsgId,
      stopAtMsgId: options.dumpStopAtMsgId,
    });
  }

  return { db, redisBroker, eventServer };
}

/**
 * Tears down the test environment created by `setupTestEnv`.
 */
export async function teardownIntegrationTestEnv(env: IntegrationTestEnv): Promise<void> {
  await closeTestClients();
  await env.eventServer.close();
  await env.db.close();
  await redisFlushAllWithPrefix(env.redisBroker.redisStreamKeyPrefix, env.redisBroker.client);
  await env.redisBroker.close();
}

/**
 * Loads a .tsv.gz events dump file into the event server.
 */
export async function loadEventsDump(
  eventServer: EventObserverServer,
  redisBroker: RedisBroker,
  dumpFile: string,
  range?: { startAtMsgId?: string; stopAtMsgId?: string }
): Promise<void> {
  const rl = readline.createInterface({
    input: fs.createReadStream(dumpFile).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });
  // Suppress noisy logs during bulk insertion
  const spyInfoLogs = [
    jest.spyOn(eventServer.logger, 'info').mockImplementation(() => {}),
    jest.spyOn(eventServer.logger, 'debug').mockImplementation(() => {}),
    jest.spyOn(redisBroker.logger, 'info').mockImplementation(() => {}),
    jest.spyOn(redisBroker.logger, 'debug').mockImplementation(() => {}),
  ];
  let skipping = !!range?.startAtMsgId;
  for await (const line of rl) {
    const [id, timestamp, path, payload] = line.split('\t');
    if (skipping) {
      if (id === range!.startAtMsgId) {
        skipping = false;
      } else {
        continue;
      }
    }
    const res = await fetch(eventServer.url + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Original-Timestamp': timestamp },
      body: payload,
    });
    if (res.status !== 200) {
      const msg = await res.text();
      throw new Error(`Failed to POST event ${path}: ${msg}`);
    }
    if (range?.stopAtMsgId && id === range.stopAtMsgId) {
      break;
    }
  }
  rl.close();
  spyInfoLogs.forEach(spy => spy.mockRestore());
}

export async function sendTestEvent(
  eventServer: EventObserverServer,
  body: any = { test: 1 },
  throwOnError = true
) {
  const res = await fetch(eventServer.url + '/test_path', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (throwOnError && res.status !== 200) {
    throw new Error(`Failed to POST event: ${res.status}`);
  }
  return res;
}

const testClients = new Set<StacksMessageStream>();

export async function closeTestClients() {
  for (const client of testClients) {
    await client.stop();
  }
  testClients.clear();
}

export async function createTestClient(
  lastMsgId: string | null = null,
  selectedMessagePaths: SelectedMessagePaths = '*',
  onSequentialMsgError: (error: Error) => void
) {
  const client = new StacksMessageStream({
    appName: 'snp-client-test',
    redisUrl: ENV.REDIS_URL,
    redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    options: {
      selectedMessagePaths,
    },
  });
  await client.connect({ waitForReady: true });
  testClients.add(client);

  // Set the initial lastProcessedMessageId for sequential validation if provided
  if (lastMsgId) {
    client.lastProcessedMessageId = lastMsgId;

    // Track the starting message ID for sequential validation
    const callerLine = getCallerLine();
    let lastReceivedMsgId = parseInt(lastMsgId.split('-')[0]);
    client.events.on('msgReceived', ({ id }) => {
      const msgId = parseInt(id.split('-')[0]);
      // Validate that the msg ids are sequential for 'all' stream type.
      if (selectedMessagePaths === '*' && msgId !== lastReceivedMsgId + 1) {
        onSequentialMsgError(
          new Error(`Out of sequence msg: ${lastReceivedMsgId} -> ${msgId} - ${callerLine}`)
        );
      }
      lastReceivedMsgId = msgId;
    });
  }

  return client;
}

export async function redisFlushAllWithPrefix(prefix: string, client: RedisClient) {
  try {
    const keys = await client.keys(`${prefix}*`);
    if (keys.length > 0) {
      await client.del(keys);
    }
  } catch (error) {
    console.error('Error flushing test Redis with prefix', prefix, error);
  }
}

export function withTimeout<T = void>(promise: Promise<T>, ms?: number): Promise<T> {
  const callerLine = getCallerLine();
  const timeout = ms ?? Math.floor(0.9 * parseInt(process.env.JEST_TEST_TIMEOUT as string));
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timeout after ${timeout}ms - ${callerLine}`)),
      timeout
    );
    promise
      .then(value => resolve(value))
      .catch(err => reject(err as Error))
      .finally(() => clearTimeout(timer));
  });
}

// Returns the stack frame (source file and line number) of the caller of the caller of this function.
// There are cleaner ways to do this with prepareStackTrace & captureStackTrace but they are not accurate
// due to how source mapping works (e.g. ts-node and ts-jest).
function getCallerLine(): string {
  const stack = new Error().stack ?? '';
  const stackLines = stack.split('\n');
  // stackLines[0] = 'Error'
  // stackLines[1] = this function
  // stackLines[2] = caller
  // stackLines[3] = caller's caller
  return (stackLines[3] ?? '').trim().replace(/^at\s+/, '') ?? 'Failed to get caller info';
}

/**
 * The synchronous version of `jest.test` includes a `done` callback that can be called with an error
 * in order to fail the test. This function provides a similar API for async tests, which for some reason
 * jest does not provide.
 */
export function testWithFailCb(fn: (onError: (error: any) => void) => Promise<void>) {
  return new Promise<void>((resolve, reject) => {
    fn(error => reject(error as Error)).then(resolve, reject);
  });
}
