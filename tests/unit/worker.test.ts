import * as assert from 'node:assert/strict';
import { WorkerManager } from '../../src/worker-stuff/parent';
import workerModule, { MyCustomError } from '../../src/worker-stuff/my-worker';
import { addKnownErrorConstructor } from '../../src/worker-stuff/error-serialize';
import { stopwatch } from '@hirosystems/api-toolkit';

describe('Worker tests', () => {
  let workerManager: WorkerManager<number, string>;
  const workerCount = 4;
  const cpuPeggedTimeMs = 500;

  beforeEach(async () => {
    addKnownErrorConstructor(MyCustomError);
    console.time('worker manager init');
    const manager = await WorkerManager.init(workerModule, { workerCount });
    console.timeEnd('worker manager init');
    workerManager = manager;
  });

  afterEach(async () => {
    await workerManager.close();
  });

  test('run tasks with workers', async () => {
    const watch = stopwatch();
    const results = await Promise.allSettled(
      Array.from({ length: workerCount }, async (_, i) => {
        console.time(`task ${i}`);
        const res = await workerManager.exec(i);
        console.timeEnd(`task ${i}`);
        return res;
      })
    );

    // All tasks should complete roughly within the time in takes for one task to complete
    // because the tasks are run in parallel on different threads.
    expect(watch.getElapsed()).toBeLessThan(cpuPeggedTimeMs * 1.75);

    // Test that error de/ser across worker thread boundary works as expected
    const rejectedResult = results[3];
    assert(rejectedResult.status === 'rejected');
    expect(rejectedResult.reason).toBeInstanceOf(MyCustomError);
  });

  test('run tasks on main thread', async () => {
    const watch = stopwatch();
    const results = await Promise.allSettled(
      Array.from({ length: workerCount }, (_, i) => {
        return Promise.resolve().then(() => workerModule.processTask(i));
      })
    );

    // All tasks should take at least as long as taskCount * cpuPeggedTimeMs because 
    // they are run synchronously on the main thread.
    expect(watch.getElapsed()).toBeGreaterThanOrEqual(workerCount * cpuPeggedTimeMs);

    const rejectedResult = results[3];
    assert(rejectedResult.status === 'rejected');
    expect(rejectedResult.reason).toBeInstanceOf(MyCustomError);
  });
});
