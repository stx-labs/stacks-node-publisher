import { waiter, Waiter } from '@hirosystems/api-toolkit';
import * as WorkerThreads from 'node:worker_threads';
import * as os from 'node:os';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';

/*
export default function callsites() {
  const _prepareStackTrace = Error.prepareStackTrace;
  try {
    let result: NodeJS.CallSite[] = [];
    Error.prepareStackTrace = (_, callSites) => {
      const callSitesWithoutCurrent = callSites.slice(1);
      result = callSitesWithoutCurrent;
      return callSitesWithoutCurrent;
    };
    const _err = new Error().stack;
    return result;
  } finally {
    Error.prepareStackTrace = _prepareStackTrace;
  }
}

export function callerCallsite({ depth = 0 } = {}) {
  const callers = [];
  const callerFileSet = new Set<string>();

  for (const callsite of callsites()) {
    const fileName = callsite.getFileName();
    const hasReceiver = callsite.getTypeName() !== null && fileName;

    if (fileName && !callerFileSet.has(fileName)) {
      callerFileSet.add(fileName);
      callers.unshift(callsite);
    }

    if (hasReceiver) {
      return callers[depth];
    }
  }
}
*/

type WorkerDataInterface = {
  workerFile: string;
};

type WorkerPoolModuleInterface<TReq, TResp> =
  | {
      workerModule: NodeJS.Module;
      processTask: (req: TReq) => Promise<TResp> | TResp;
    }
  | {
      default: {
        workerModule: NodeJS.Module;
        processTask: (req: TReq) => Promise<TResp> | TResp;
      };
    };

type WorkerReqMsg<TReq> = {
  msgId: number;
  req: TReq;
};

type WorkerRespMsg<TResp> = {
  msgId: number;
} & (
  | {
      resp: TResp;
      error?: null;
    }
  | {
      resp?: null;
      error: unknown;
    }
);

/**
 * Invokes a function that may return a value or a promise, and passes the result
 * to a callback in a consistent format. Handles both synchronous and asynchronous cases,
 * ensuring type safety and avoiding unnecessary async transitions for sync functions.
 */
function getMaybePromiseResult<T>(
  fn: () => T | Promise<T>,
  cb: (result: { ok: T; err?: null } | { ok?: null; err: unknown }) => void
): void {
  try {
    const maybePromise = fn();
    if (maybePromise instanceof Promise) {
      maybePromise.then(
        ok => cb({ ok }),
        (err: unknown) => cb({ err })
      );
    } else {
      cb({ ok: maybePromise });
    }
  } catch (err: unknown) {
    cb({ err });
  }
}

export class WorkerManager<TReq, TResp> {
  private readonly workers = new Set<WorkerThreads.Worker>();
  private readonly idleWorkers: WorkerThreads.Worker[] = [];

  private readonly jobQueue: WorkerReqMsg<TReq>[] = [];
  private readonly msgRequests: Map<number, Waiter<TResp>> = new Map();
  private lastMsgId = 0;

  readonly workerCount: number;
  readonly workerFile: string;

  readonly events = new EventEmitter<{
    workersReady: [];
  }>();

  public static init<TReq, TResp>(
    workerModule: WorkerPoolModuleInterface<TReq, TResp>,
    opts: { workerCount?: number } = {}
  ) {
    const workerManager = new WorkerManager(workerModule, opts);
    return new Promise<WorkerManager<TReq, TResp>>(resolve => {
      workerManager.events.once('workersReady', () => {
        resolve(workerManager);
      });
    });
  }

  constructor(
    workerModule: WorkerPoolModuleInterface<TReq, TResp>,
    opts: { workerCount?: number } = {}
  ) {
    if (!WorkerThreads.isMainThread) {
      throw new Error(`${this.constructor.name} must be instantiated in the main thread`);
    }

    if ('default' in workerModule) {
      this.workerFile = workerModule.default.workerModule.filename;
    } else {
      this.workerFile = workerModule.workerModule.filename;
    }
    this.workerCount = opts.workerCount ?? os.cpus().length;
    this.createWorkerPool();
  }

  exec(req: TReq): Promise<TResp> {
    if (this.lastMsgId >= Number.MAX_SAFE_INTEGER) {
      this.lastMsgId = 0;
    }
    const msgId = this.lastMsgId++;
    const replyWaiter = waiter<TResp>();
    this.msgRequests.set(msgId, replyWaiter);
    const reqMsg: WorkerReqMsg<TReq> = {
      msgId,
      req,
    };
    this.jobQueue.push(reqMsg);
    this.assignJobs();
    return replyWaiter;
  }

  createWorkerPool() {
    let workersReady = 0;
    for (let i = 0; i < this.workerCount; i++) {
      const workerData: WorkerDataInterface = {
        workerFile: this.workerFile,
      };
      const workerOpt: WorkerThreads.WorkerOptions = {
        workerData,
      };
      if (path.extname(__filename) === '.ts') {
        if (process.env.NODE_ENV !== 'test') {
          console.error(
            'Worker threads are being created with ts-node outside of a test environment.'
          );
        }
        workerOpt.execArgv = ['-r', 'ts-node/register'];
      }
      const worker = new WorkerThreads.Worker(__filename, workerOpt);
      worker.unref();
      this.workers.add(worker);
      worker.on('error', err => {
        console.error(`Worker error`, err);
      });
      worker.on('messageerror', err => {
        console.error(`Worker message error`, err);
      });
      worker.on('message', (message: unknown) => {
        if (message === 'ready') {
          this.idleWorkers.push(worker);
          this.assignJobs();
          workersReady++;
          if (workersReady === this.workerCount) {
            this.events.emit('workersReady');
          }
          return;
        }
        this.idleWorkers.push(worker);
        this.assignJobs();
        const msg = message as WorkerRespMsg<TResp>;
        const replyWaiter = this.msgRequests.get(msg.msgId);
        if (replyWaiter) {
          if (msg.resp) {
            replyWaiter.resolve(msg.resp);
          } else {
            replyWaiter.reject(msg.error as Error);
          }
          this.msgRequests.delete(msg.msgId);
        } else {
          console.error('Received unexpected message from worker', msg);
        }
      });
    }
  }

  private assignJobs() {
    while (this.idleWorkers.length > 0 && this.jobQueue.length > 0) {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const worker = this.idleWorkers.shift()!;
      const job = this.jobQueue.shift();
      worker.postMessage(job);
    }
  }

  async close() {
    await Promise.all(
      [...this.workers].map(worker => {
        return worker.terminate().then(() => this.workers.delete(worker));
      })
    );
  }
}

if (!WorkerThreads.isMainThread && (WorkerThreads.workerData as WorkerDataInterface)?.workerFile) {
  const { workerFile } = WorkerThreads.workerData as WorkerDataInterface;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const workerModule = require(workerFile) as WorkerPoolModuleInterface<unknown, unknown>;
  const parentPort = WorkerThreads.parentPort as WorkerThreads.MessagePort;
  const processTask = (() => {
    if ('default' in workerModule) {
      return workerModule.default.processTask;
    } else {
      return workerModule.processTask;
    }
  })();
  parentPort.on('messageerror', err => {
    console.error(`Worker thread message error`, err);
  });
  parentPort.on('message', (message: unknown) => {
    const msg = message as WorkerReqMsg<unknown>;
    console.log(`Worker thread ${WorkerThreads.threadId} handling msg ${msg.msgId}`);
    getMaybePromiseResult(
      () => processTask(msg.req),
      result => {
        if (result.ok) {
          const reply: WorkerRespMsg<unknown> = {
            msgId: msg.msgId,
            resp: result.ok,
          };
          parentPort.postMessage(reply);
        } else {
          // TODO: serializer error
          console.error(`Worker thread message error`, result.err);
          const reply: WorkerRespMsg<unknown> = {
            msgId: msg.msgId,
            error: result.err,
          };
          parentPort.postMessage(reply);
        }
      }
    );
  });
  parentPort.postMessage('ready');
}
