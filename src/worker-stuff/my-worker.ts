/** Block the thread for `ms` milliseconds */
function sleepSync(ms: number) {
  const int32 = new Int32Array(new SharedArrayBuffer(4));
  Atomics.wait(int32, 0, 0, ms);
}

function processTask(req: number) {
  sleepSync(500);
  if (req === 3) {
    throw createError();
  }
  return req.toString();
}

export class MyCustomError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

function createError() {
  const error = new MyCustomError(`Error at req`);
  Object.assign(error, { code: 123 });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
  (error as any).randoProp = {
    foo: 'bar',
    baz: 123,
  };
  return error;
}

export default {
  workerModule: module,
  processTask,
};
