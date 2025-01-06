import { logger } from '@hirosystems/api-toolkit';
import { ENV } from '../../src/env';

beforeAll(() => {
  // use a random PGSCHEMA for each test to avoid conflicts
  process.env.PGSCHEMA = `test_${crypto.randomUUID()}`;
  logger.info(`Using PGSCHEMA: ${process.env.PGSCHEMA}`);
  ENV.reload();
});
