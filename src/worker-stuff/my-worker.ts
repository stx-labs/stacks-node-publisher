import { addKnownErrorConstructor } from './error-serialize';

function processTask(req: number) {
  if (req === 5) {
    throw createError();
  }
  let res = 2;
  // for (let i = 0; i < 100_000_000; i++) {
  for (let i = 0; i < 100; i++) {
    res = i + req;
  }
  return res.toString();
}

export default {
  workerModule: module,
  processTask,
};

export class MyCustomError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = this.constructor.name;
  }
}
addKnownErrorConstructor(MyCustomError);

function createError() {
  const createInternalError = () => {
    const error = new MyCustomError(`Error at req`);
    Object.assign(error, { code: 123 });
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
    (error as any).randoProp = {
      foo: 'bar',
      baz: 123,
    };
    return error;
  };
  return createInternalError();
}
