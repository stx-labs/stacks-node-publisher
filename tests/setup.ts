import type { DockerTestContainerConfig } from '@stacks/api-test-toolkit';
import { dockerTestDown, dockerTestUp } from '@stacks/api-test-toolkit';

process.env.PGHOST = '127.0.0.1';
process.env.PGPORT = '5490';
process.env.PGUSER = 'test';
process.env.PGPASSWORD = 'test';
process.env.PGDATABASE = 'testdb';
process.env.CLEANUP_INTERVAL_MS = '1000';
process.env.CLIENT_REDIS_BACKPRESSURE_POLL_MS = '50';
process.env.REDIS_URL = 'redis://127.0.0.1:6379';
process.env['_PG_DOCKER_CONTAINER_ID'] = 'stacks-node-publisher-tests-postgres';
process.env['_REDIS_DOCKER_CONTAINER_ID'] = 'stacks-node-publisher-tests-redis';

function testContainers(): DockerTestContainerConfig[] {
  const postgres: DockerTestContainerConfig = {
    image: 'postgres:17',
    name: `stacks-node-publisher-tests-postgres`,
    ports: [{ host: 5490, container: 5432 }],
    env: [
      'POSTGRES_USER=test',
      'POSTGRES_PASSWORD=test',
      'POSTGRES_DB=testdb',
      'POSTGRES_PORT=5432',
    ],
    healthcheck: 'pg_isready -U test',
  };

  const redis: DockerTestContainerConfig = {
    image: 'redis:7',
    name: `stacks-node-publisher-tests-redis`,
    host: '0.0.0.0',
    ports: [{ host: 6379, container: 6379 }],
    waitPort: 6379,
  };

  return [postgres, redis];
}

export async function globalSetup() {
  const containers = testContainers();
  for (const config of containers) {
    await dockerTestUp({ config });
  }
  process.stdout.write(`[testenv:snp] all containers ready\n`);
}

export async function globalTeardown() {
  const containers = testContainers();
  for (const config of [...containers].reverse()) {
    await dockerTestDown({ config });
  }
  process.stdout.write(`[testenv:snp] all containers removed\n`);
}
