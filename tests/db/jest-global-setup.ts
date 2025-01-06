import * as Docker from 'dockerode';
import { connectPostgres } from '@hirosystems/api-toolkit';
import { createClient } from 'redis';
import { sleep } from '../../src/helpers';

const testContainerLabel = 'salt-n-pepper-tests';

const pgConfig = {
  PGHOST: '127.0.0.1',
  PGPORT: '',
  PGUSER: 'test',
  PGPASSWORD: 'test',
  PGDATABASE: 'testdb',
};

function isDockerImagePulled(docker: Docker, imgName: string) {
  return docker
    .getImage(imgName)
    .inspect()
    .then(
      () => true,
      () => false
    );
}

async function pullDockerImage(docker: Docker, imgName: string) {
  await new Promise<void>((resolve, reject) => {
    docker.pull(imgName, {}, (err, stream) => {
      if (err || !stream) return reject(err as Error);
      docker.modem.followProgress(stream, err => (err ? reject(err) : resolve()), console.log);
    });
  });
}

async function pruneContainers(docker: Docker, label: string) {
  const containers = await docker.listContainers({ all: true, filters: { label: [label] } });
  for (const container of containers) {
    const c = docker.getContainer(container.Id);
    if (container.State !== 'exited') {
      await c.stop().catch(_err => {});
    }
    await c.remove();
  }
  return containers.length;
}

async function startContainer({
  docker,
  image,
  ports,
  env,
}: {
  docker: Docker;
  image: string;
  ports: number[];
  env: string[];
}): Promise<{ image: string; bindedPorts: { [port: number]: number } }> {
  try {
    const imgPulled = await isDockerImagePulled(docker, image);
    if (!imgPulled) {
      console.log(`Pulling ${image} image...`);
      await pullDockerImage(docker, image);
    }
    console.log(`Creating ${image} container...`);
    const exposedPorts = ports.reduce((acc, port) => ({ ...acc, [`${port}/tcp`]: {} }), {});
    const portBindings = ports.reduce(
      (acc, port) => ({ ...acc, [`${port}/tcp`]: [{ HostPort: '0' }] }),
      {}
    );
    const container = await docker.createContainer({
      Labels: { [testContainerLabel]: 'true' },
      Image: image,
      ExposedPorts: exposedPorts,
      HostConfig: { PortBindings: portBindings },
      Env: env,
    });

    console.log(`Starting ${image} container...`);
    await container.start();

    // Inspect container to get the host port assigned
    const containerInfo = await container.inspect();
    const bindedPorts = ports.reduce<{ [port: number]: number }>((acc, port) => {
      const hostPort = containerInfo.NetworkSettings.Ports[`${port}/tcp`]?.[0]?.HostPort;
      if (!hostPort) {
        throw new Error(`HostPort not found for port ${port}`);
      }
      acc[port] = parseInt(hostPort);
      return acc;
    }, {});
    console.log(`${image} container started on ports ${JSON.stringify(bindedPorts)}`);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const containerIds = ((globalThis as any).__TEST_DOCKER_CONTAINER_IDS as string[]) ?? [];
    containerIds.push(container.id);
    Object.assign(globalThis, { __TEST_DOCKER_CONTAINER_IDS: containerIds });

    return { image, bindedPorts };
  } catch (error) {
    console.error('Error starting PostgreSQL container:', error);
    throw error;
  }
}

// Helper function to wait for PostgreSQL to be ready
async function waitForPostgres(): Promise<void> {
  const sql = await connectPostgres({
    usageName: 'salt-n-pepper-pg-tests',
    connectionArgs: {
      host: pgConfig.PGHOST,
      port: parseInt(pgConfig.PGPORT),
      user: pgConfig.PGUSER,
      password: pgConfig.PGPASSWORD,
      database: pgConfig.PGDATABASE,
    },
  });
  await sql`SELECT 1`;
  console.log('Postgres is ready');
}

async function waitForRedis(): Promise<void> {
  const redisClient = createClient({ url: process.env['REDIS_URL'] });
  redisClient.on('error', err => console.error('Redis Client Error', err));
  while (true) {
    try {
      await redisClient.connect();
      console.log('Connected to Redis successfully!');
      break;
    } catch (error) {
      console.error(`Failed to connect to Redis:`, error);
      await sleep(100);
      break;
    }
  }
  await redisClient.disconnect();
}

// Jest global setup
export default async function setup(): Promise<void> {
  const docker = new Docker();
  const prunedCount = await pruneContainers(docker, testContainerLabel);
  if (prunedCount > 0) {
    console.log(`Pruned ${prunedCount} existing test docker containers`);
  }

  const startPg = async () => {
    const pgPort = 5432;
    const pgContainer = await startContainer({
      docker,
      image: 'postgres:17',
      ports: [pgPort],
      env: [
        `POSTGRES_USER=${pgConfig.PGUSER}`,
        `POSTGRES_PASSWORD=${pgConfig.PGPASSWORD}`,
        `POSTGRES_DB=${pgConfig.PGDATABASE}`,
        `POSTGRES_PORT=${pgPort}`,
      ],
    });
    pgConfig.PGPORT = pgContainer.bindedPorts[pgPort].toString();
    console.log(`${pgContainer.image} container started on port ${pgConfig.PGPORT}`);
    for (const entry of Object.entries(pgConfig)) {
      process.env[entry[0]] = entry[1];
    }
    // Wait for the database to be ready
    await waitForPostgres();
  };

  const startRedis = async () => {
    const redisPort = 6379;
    const redisContainer = await startContainer({
      docker,
      image: 'redis:7',
      ports: [redisPort],
      env: [],
    });
    process.env['REDIS_URL'] = `redis://localhost:${redisContainer.bindedPorts[redisPort]}`;
    // wait for redis to be ready
    await waitForRedis();
  };

  const startServices = await Promise.allSettled([startPg(), startRedis()]);
  for (const service of startServices) {
    if (service.status === 'rejected') {
      throw service.reason;
    }
  }
}
