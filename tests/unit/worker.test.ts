import * as assert from 'node:assert/strict';
import * as os from 'node:os';
import { WorkerManager } from '../../src/worker-stuff/parent';
import workerModule, { MyCustomError } from '../../src/worker-stuff/my-worker';
import { addKnownErrorConstructor } from '../../src/worker-stuff/error-serialize';
import { stopwatch } from '@hirosystems/api-toolkit';

describe('Worker tests', () => {
  let workerManager: Awaited<ReturnType<typeof initWorkerManager>>;
  const workerCount = Math.min(4, os.cpus().length);
  const cpuPeggedTimeMs = 500;

  function initWorkerManager() {
    return WorkerManager.init(workerModule, { workerCount });
  }

  beforeEach(async () => {
    addKnownErrorConstructor(MyCustomError);
    console.time('worker manager init');
    const manager = await initWorkerManager();
    console.timeEnd('worker manager init');
    workerManager = manager;
  });

  afterEach(async () => {
    await workerManager.close();
  });

  test('run tasks with workers', async () => {
    const watch = stopwatch();
    const taskPromises = Array.from({ length: workerCount }, async (_, i) => {
      console.time(`task ${i}`);
      const res = await workerManager.exec(i, cpuPeggedTimeMs);
      console.timeEnd(`task ${i}`);
      return res;
    });

    // Ensure all workers were assigned a task
    expect(workerManager.busyWorkerCount).toBe(workerCount);
    expect(workerManager.idleWorkerCount).toBe(0);

    const results = await Promise.allSettled(taskPromises);

    // All tasks should complete roughly within the time in takes for one task to complete
    // because the tasks are run in parallel on different threads.
    expect(watch.getElapsed()).toBeLessThan(cpuPeggedTimeMs * 1.75);

    // Ensure tasks returned in expected order:
    for (let i = 0; i < workerCount - 1; i++) {
      const result = results[i];
      assert(result.status === 'fulfilled');
      expect(result.value).toBe(i.toString());
    }

    // Test that error de/ser across worker thread boundary works as expected
    const rejectedResult = results[3];
    assert(rejectedResult.status === 'rejected');
    expect(rejectedResult.reason).toBeInstanceOf(MyCustomError);
  });

  test('run tasks on main thread', async () => {
    const watch = stopwatch();
    const results = await Promise.allSettled(
      Array.from({ length: workerCount }, (_, i) => {
        return Promise.resolve().then(() => workerModule.processTask(i, cpuPeggedTimeMs));
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
