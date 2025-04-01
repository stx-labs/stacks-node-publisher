function processTask(req: number) {
  if (req === 5) {
    throw createError();
  }
  let res = 2;
  for (let i = 0; i < 100_000_000; i++) {
    res = i + req;
  }
  return res.toString();
}

export default {
  workerModule: module,
  processTask,
};

function createError() {
  const createInternalError = () => {
    const error = new TypeError(`Error at req`);
    error.name = `WorkerError`;
    Object.assign(error, { code: 123 });
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    (error as any).randoProp = {
      foo: 'bar',
      baz: 123,
    };
    return error;
  };
  return createInternalError();
}
