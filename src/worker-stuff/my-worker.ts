// import { timeout } from '@hirosystems/api-toolkit';

function processTask(req: number) {
  let res = 2;
  for (let i = 0; i < 100_000_000; i++) {
    res = i + req;
  }
  // await timeout(1);
  // return Promise.resolve(res.toString());
  return res.toString();
}

// export const workerModule = module;

export default {
  workerModule: module,
  processTask,
};
