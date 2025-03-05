import { PgStore } from './pg/pg-store';
import { ENV } from './env';
import { isProdEnv } from './helpers';
import { buildProfilerServer, logger, registerShutdownConfig } from '@hirosystems/api-toolkit';
import { EventObserverServer } from './event-observer/event-server';
import { buildPromServer } from './prom/prom-server';
import { Registry, collectDefaultMetrics } from 'prom-client';
import { RedisBroker } from './redis/redis-broker';

async function initApp() {
  logger.info(`Initializing in ${ENV.RUN_MODE} run mode...`);
  const isReadonly = ENV.RUN_MODE === 'readonly';
  const db = await PgStore.connect({
    skipMigrations: isReadonly,
    createSchema: !isReadonly,
  });

  // TODO: consider the following runmodes:
  // - ingestion (must only be one instance):
  //    * http event observer server (that stacks-node(s) POST to)
  //    * persisting events to postgres
  //    * writing events to the redis global stream
  // - broker (can be multiple instances):
  //    * listening and handling snp client connection requests
  //    * backfilling msgs from postgres to the client-specific redis streams
  //    * buffering msgs from the redis global stream to the client-specific redis streams
  // - prune (should only be one instance):
  //    * periodically deleting idle client-specific redis streams
  //    * periodically trimming old msgs from the redis global stream

  // Setup default prometheus metrics
  const promRegistry = new Registry();
  collectDefaultMetrics({ register: promRegistry });

  // Setup redis client
  const redisBroker = new RedisBroker({
    redisUrl: ENV.REDIS_URL,
    redisStreamKeyPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
    db,
  });
  registerShutdownConfig({
    name: 'Redis client',
    forceKillable: false,
    handler: async () => {
      await redisBroker.close();
    },
  });
  logger.info('Initializing redis client...');
  // Start redis client connection but don't wait for it to be ready, because we want to start the
  // event server and persist messages to postgres as soon as possible, even if redis is not ready.
  await redisBroker.connect({ waitForReady: false });

  // Setup stacks-node http event observer http server
  const eventServer = new EventObserverServer({
    promRegistry,
    eventMessageHandler: async (eventPath, eventBody, httpReceiveTimestamp) => {
      // Storing the event in postgres in critical, if this fails then throw so the observer server
      // returns a non-200 and the stacks-node will retry the event POST.
      const dbResult = await db.insertMessage(eventPath, eventBody, httpReceiveTimestamp);
      // TODO: This should be fire-and-forget into a serialized promise queue, because writing the event
      // to redis is not critical and we don't want to slow down the event observer server & pg writes.
      // For example, even if redis takes a few hundreds milliseconds, we don't want to block the
      // stack-node(s) for any longer than absolutely necessary. This especially important during genesis
      // syncs and also for the high-precision stackerdb_chunk event timestamps used by clients like
      // the signer-metrics-api.
      // The promise queue should be limited to 1 concurrency to ensure the order of events is maintained,
      // and should have a reasonable max queue length to prevent memory exhaustion. If the limit is reached
      // then the redis write will just be skipped for this message, and the redis-broker layer already knows
      // how to handle this case (e.g. detecting msg gaps and backfilling from postgres).
      await redisBroker.addStacksMessage({
        timestamp: dbResult.timestamp,
        sequenceNumber: dbResult.sequence_number,
        eventPath,
        eventBody,
      });
    },
  });
  registerShutdownConfig({
    name: 'Event observer server',
    forceKillable: false,
    handler: async () => {
      await eventServer.close();
    },
  });
  logger.info('Initializing event server...');
  await eventServer.start({ host: ENV.OBSERVER_HOST, port: ENV.OBSERVER_PORT });

  // Setup prometheus metrics http server
  if (isProdEnv) {
    const promServer = await buildPromServer({ registry: promRegistry });
    registerShutdownConfig({
      name: 'Prometheus Server',
      forceKillable: false,
      handler: async () => {
        await promServer.close();
      },
    });
    await promServer.listen({ host: ENV.OBSERVER_HOST, port: ENV.PROMETHEUS_PORT });
  }

  // Setup nodejs profiler http server
  const profilerServer = await buildProfilerServer();
  registerShutdownConfig({
    name: 'Profiler Server',
    forceKillable: false,
    handler: async () => {
      await profilerServer.close();
    },
  });
  await profilerServer.listen({ host: ENV.OBSERVER_HOST, port: ENV.PROFILER_PORT });

  registerShutdownConfig({
    name: 'DB',
    forceKillable: false,
    handler: async () => {
      await db.close();
    },
  });
}

registerShutdownConfig();
initApp()
  .then(() => {
    logger.info('App initialized');
  })
  .catch((error: unknown) => {
    logger.error(error, `App failed to start`);
    process.exit(1);
  });
