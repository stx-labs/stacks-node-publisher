import { logger } from '@hirosystems/api-toolkit';
import { ENV } from '../../src/env';

beforeAll(() => {
  // use a random PGSCHEMA for each test to avoid conflicts
  process.env.PGSCHEMA = `test_${crypto.randomUUID()}`;
  logger.info(`Using PGSCHEMA: ${process.env.PGSCHEMA}`);

  // use a random redis stream prefix for each test to avoid conflicts
  process.env.REDIS_STREAM_KEY_PREFIX = `test_${crypto.randomUUID()}`;
  logger.info(`Using REDIS_STREAM_KEY_PREFIX: ${process.env.REDIS_STREAM_KEY_PREFIX}`);

  process.env.CLEANUP_INTERVAL_MS = '1000';

  ENV.reload();
});
