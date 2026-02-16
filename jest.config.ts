import { createDefaultPreset, type JestConfigWithTsJest } from 'ts-jest';

const isDebugging = process.env.NODE_OPTIONS?.includes('--inspect');
const debugTestTimeout = 60_000;
const defaultTestTimeout = 10_000;
const testTimeout = isDebugging ? debugTestTimeout : defaultTestTimeout;
process.env.JEST_TEST_TIMEOUT = testTimeout.toString();

const transform = { ...createDefaultPreset().transform };
const jestConfig: JestConfigWithTsJest = {
  testEnvironment: 'node',
  // The v8 coverage provider is inaccurate in this repo
  // coverageProvider: 'v8',
  collectCoverageFrom: ['src/**/*.ts', 'migrations/*.ts'],
  testTimeout: testTimeout,
  maxWorkers: 1,
  projects: [
    {
      transform,
      displayName: 'event-server',
      testMatch: ['**/tests/event-server/**/*.test.ts'],
      globalSetup: './tests/jest-global-setup.ts',
      globalTeardown: './tests/jest-global-teardown.ts',
      setupFilesAfterEnv: ['./tests/jest-setup.ts'],
    },
    {
      transform,
      displayName: 'redis-broker',
      testMatch: ['**/tests/redis-broker/**/*.test.ts'],
      globalSetup: './tests/jest-global-setup.ts',
      globalTeardown: './tests/jest-global-teardown.ts',
      setupFilesAfterEnv: ['./tests/jest-setup.ts'],
    },
    {
      transform,
      displayName: 'client-stream',
      testMatch: ['**/tests/client-stream/**/*.test.ts'],
      globalSetup: './tests/jest-global-setup.ts',
      globalTeardown: './tests/jest-global-teardown.ts',
      setupFilesAfterEnv: ['./tests/jest-setup.ts'],
    },
  ],
};

export default jestConfig;
