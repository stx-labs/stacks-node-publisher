import { timeout } from '@hirosystems/api-toolkit';
import * as events from 'node:events';
import { WorkerManager } from '../../src/worker-stuff/parent';
import workerModule, { MyCustomError } from '../../src/worker-stuff/my-worker';
import { addKnownErrorConstructor } from '../../src/worker-stuff/error-serialize';

describe('Worker tests', () => {
  let workerManager: WorkerManager<number, string>;

  beforeEach(async () => {
    addKnownErrorConstructor(MyCustomError);
    const manager = await WorkerManager.init(workerModule);
    workerManager = manager;
  });

  afterEach(async () => {
    await workerManager.close();
  });

  test('worker debugging', async () => {
    const results = await Promise.allSettled(
      Array.from({ length: 10 }, (_, i) => {
        return workerManager.exec(i);
      })
    );
    const ff = results[5];
    if (ff.status === 'rejected') {
      const err = ff.reason;
      const isCorrect = err instanceof MyCustomError;
      console.log('isCorrect', isCorrect);
    }
    console.log(results);
  });

  test('give it to me straight', async () => {
    const results = await Promise.allSettled(
      Array.from({ length: 10 }, (_, i) => {
        return Promise.resolve().then(() => workerModule.processTask(i));
      })
    );
    const ff = results[5];
    if (ff.status === 'rejected') {
      const err = ff.reason;
      const isCorrect = err instanceof MyCustomError;
      console.log('isCorrect', isCorrect);
    }
    console.log(results);
  });
});
