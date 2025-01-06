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

  // Setup default prometheus metrics
  const promRegistry = new Registry();
  collectDefaultMetrics({ register: promRegistry });

  // Setup redis client
  const redisBroker = new RedisBroker({ redisUrl: ENV.REDIS_URL });
  registerShutdownConfig({
    name: 'Redis client',
    forceKillable: false,
    handler: async () => {
      await redisBroker.close();
    },
  });
  logger.info('Initializing redis client...');
  // TODO: initial connection should have a retry mechanism
  await redisBroker.connect();

  // Setup stacks-node http event observer http server
  const eventMessageHandler = redisBroker.eventMessageHandlerInserter(db);
  const eventServer = new EventObserverServer({ promRegistry, eventMessageHandler });
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
