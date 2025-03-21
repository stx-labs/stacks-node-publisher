import { createDefaultPreset, type JestConfigWithTsJest } from 'ts-jest';

const isDebugging = process.env.NODE_OPTIONS?.includes('--inspect');
const debugTestTimeout = 60_000;
const defaultTestTimeout = 10_000;
const testTimeout = isDebugging ? debugTestTimeout : defaultTestTimeout;
process.env.JEST_TEST_TIMEOUT = testTimeout.toString();

const transform = { ...createDefaultPreset().transform };
const jestConfig: JestConfigWithTsJest = {
  testEnvironment: 'node',
  coverageProvider: 'v8',
  collectCoverageFrom: ['src/**/*.ts', 'migrations/*.ts'],
  testTimeout: testTimeout,
  maxWorkers: 1,
  projects: [
    {
      transform,
      displayName: 'unit-tests',
      testMatch: ['**/tests/unit/**/*.test.ts'],
    },
    {
      transform,
      displayName: 'db-tests',
      testMatch: ['**/tests/db/**/*.test.ts'],
      globalSetup: './tests/db/jest-global-setup.ts',
      globalTeardown: './tests/db/jest-global-teardown.ts',
      setupFilesAfterEnv: ['./tests/db/jest-setup.ts'],
    },
  ],
};

export default jestConfig;
