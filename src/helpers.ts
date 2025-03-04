import { addAbortListener, EventEmitter } from 'node:events';

export const isDevEnv = process.env.NODE_ENV === 'development';
export const isTestEnv = process.env.NODE_ENV === 'test';
export const isProdEnv =
  process.env.NODE_ENV === 'production' ||
  process.env.NODE_ENV === 'prod' ||
  !process.env.NODE_ENV ||
  (!isTestEnv && !isDevEnv);

/** Convert a unix timestamp in milliseconds to an ISO string */
export function unixTimeMillisecondsToISO(timestampMilliseconds: number): string {
  return new Date(timestampMilliseconds).toISOString();
}

/** Convert a unix timestamp in seconds to an ISO string */
export function unixTimeSecondsToISO(timestampSeconds: number): string {
  return unixTimeMillisecondsToISO(timestampSeconds * 1000);
}

const DisposeSymbol: typeof Symbol.dispose = Symbol.dispose ?? Symbol.for('nodejs.dispose');

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason as Error);
      return;
    }
    const disposable = signal ? addAbortListener(signal, onAbort) : undefined;
    const timeout = setTimeout(() => {
      disposable?.[DisposeSymbol]();
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(timeout);
      reject((signal?.reason as Error) ?? new Error('Aborted'));
    }
  });
}

/**
 * Similar to `node:events.once` but with a predicate to filter events and supports typed EventEmitters
 */
export function waitForEvent<T extends Record<string, any[]>, K extends keyof T>(
  emitter: EventEmitter<T>,
  event: K,
  predicate: (...args: T[K]) => boolean,
  signal?: AbortSignal
): Promise<T[K]> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason as Error);
      return;
    }
    const disposable = signal ? addAbortListener(signal, onAbort) : undefined;
    const handler = (...args: T[K]) => {
      if (predicate(...args)) {
        disposable?.[DisposeSymbol]();
        (emitter as EventEmitter).off(event as string, handler);
        resolve(args);
      }
    };
    (emitter as EventEmitter).on(event as string, handler);
    function onAbort() {
      (emitter as EventEmitter).off(event as string, handler);
      reject((signal?.reason as Error) ?? new Error('Aborted'));
    }
  });
}

export function waiterNew<T = void>(): import('@hirosystems/api-toolkit').Waiter<T> {
  let resolveFn: (result: T) => void;
  const promise = new Promise<T>(resolve => {
    resolveFn = resolve;
  });
  const completer = {
    finish: (result: T) => {
      void Object.assign(promise, { isFinished: true });
      resolveFn(result);
    },
    isFinished: false,
  };
  return Object.assign(promise, completer);
}
