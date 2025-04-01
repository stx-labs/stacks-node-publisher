import { timeout } from '@hirosystems/api-toolkit';
import * as events from 'node:events';
import { WorkerManager } from '../../src/worker-stuff/parent';
import * as workerModule from '../../src/worker-stuff/my-worker';

describe('Worker tests', () => {
  test('worker debugging', async () => {
    const manager = new WorkerManager(workerModule);
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) => {
        return manager.exec(i);
      })
    );

    console.log(results);
  });

  test('give it to me straight', async () => {
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) => {
        return workerModule.processTask(i);
      })
    );

    console.log(results);
  });
});
