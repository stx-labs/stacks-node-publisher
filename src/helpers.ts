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

export type Waiter<T = void> = Promise<T> & {
  /** Alias for `resolve` */
  finish: (result: T) => void;
  resolve: (result: T) => void;
  reject: (error: Error) => void;
  isFinished: boolean;
  isResolved: boolean;
  isRejected: boolean;
};

export function waiterNew<T = void>(): Waiter<T> {
  let resolveFn: (result: T) => void;
  let rejectFn: (error: Error) => void;
  const promise = new Promise<T>((resolve, reject) => {
    resolveFn = resolve;
    rejectFn = reject;
  });
  const completer = {
    finish: (result: T) => completer.resolve(result),
    resolve: (result: T) => {
      void Object.assign(promise, { isFinished: true, isResolved: true });
      resolveFn(result);
    },
    reject: (error: Error) => {
      void Object.assign(promise, { isFinished: true, isRejected: true });
      rejectFn(error);
    },
    isFinished: false,
    isResolved: false,
    isRejected: false,
  };
  return Object.assign(promise, completer);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function createTestHook<T extends (...args: any[]) => Promise<void>>() {
  const callbacks = new Set<T>();

  const register = (cb: T) => {
    callbacks.add(cb);
    return {
      unregister: () => callbacks.delete(cb),
    };
  };
  return Object.assign(callbacks, { register });
}
