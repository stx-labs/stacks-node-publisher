import { timeout } from '@hirosystems/api-toolkit';
import * as events from 'node:events';
import { WorkerManager } from '../../src/worker-stuff/parent';
import workerModule from '../../src/worker-stuff/my-worker';

describe('Worker tests', () => {
  let workerManager: WorkerManager<number, string>;

  beforeEach(async () => {
    const manager = await WorkerManager.init(workerModule);
    workerManager = manager;
  });

  afterEach(async () => {
    await workerManager.close();
  });

  test('worker debugging', async () => {
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => {
        return workerManager.exec(i);
      })
    );

    console.log(results);
  });

  test('give it to me straight', async () => {
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => {
        return workerModule.processTask(i);
      })
    );

    console.log(results);
  });
});
