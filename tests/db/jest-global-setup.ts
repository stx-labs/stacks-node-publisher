import * as net from 'node:net';
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
    await c.remove({ v: true, force: true });
  }
  await docker.pruneContainers({ filters: { label: [label] } });
  return containers.length;
}

async function startContainer(args: {
  docker: Docker;
  image: string;
  ports: number[];
  env: string[];
}) {
  const { docker, image, ports, env } = args;
  try {
    const imgPulled = await isDockerImagePulled(docker, image);
    if (!imgPulled) {
      console.log(`Pulling ${image} image...`);
      await pullDockerImage(docker, image);
    }
    console.log(`Creating ${image} container...`);
    const exposedPorts = ports.reduce((acc, port) => ({ ...acc, [`${port}/tcp`]: {} }), {});
    const portBindings: Record<string, { HostPort: string }[]> = {};
    const freePorts = await findFreePorts(ports.length);
    ports.forEach((port, index) => {
      portBindings[`${port}/tcp`] = [{ HostPort: freePorts[index].toString() }];
    });
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

    return { image, bindedPorts, containerId: container.id };
  } catch (error) {
    console.error('Error starting PostgreSQL container:', error);
    throw error;
  }
}

async function findFreePorts(count: number) {
  const servers = await Promise.all(
    Array.from({ length: count }, () => {
      return new Promise<net.Server>((resolve, reject) => {
        const server = net.createServer();
        server.listen(0, () => resolve(server)).on('error', reject);
      });
    })
  );
  const ports = await Promise.all(
    servers.map(server => {
      const { port } = server.address() as net.AddressInfo;
      return new Promise<number>(resolve => server.close(() => resolve(port)));
    })
  );
  return ports;
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
  const redisClient = createClient({
    url: process.env['REDIS_URL'],
    name: 'salt-n-pepper-server-tests',
  });
  redisClient.on('error', (err: Error) => console.error('Redis Client Error', err));
  redisClient.once('ready', () => console.log('Connected to Redis successfully!'));
  while (true) {
    try {
      await redisClient.connect();
      break;
    } catch (error) {
      console.error(`Failed to connect to Redis:`, error);
      await sleep(100);
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
    for (const entry of Object.entries(pgConfig)) {
      process.env[entry[0]] = entry[1];
    }
    process.env['_PG_DOCKER_CONTAINER_ID'] = pgContainer.containerId;
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
    process.env['REDIS_URL'] = `redis://127.0.0.1:${redisContainer.bindedPorts[redisPort]}`;
    process.env['_REDIS_DOCKER_CONTAINER_ID'] = redisContainer.containerId;
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
