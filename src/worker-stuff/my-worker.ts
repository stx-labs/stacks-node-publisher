// import { timeout } from '@hirosystems/api-toolkit';

export function processTask(req: number): Promise<string> {
  let res = 2;
  for (let i = 0; i < 1_000_000_000; i++) {
    res = i + req;
  }
  // await timeout(1);
  return Promise.resolve(res.toString());
}

export const workerModule = module;

export default {
  workerModule: module,
  processTask,
};
