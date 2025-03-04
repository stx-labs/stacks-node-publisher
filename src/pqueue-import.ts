import type PQueue from 'p-queue' with { 'resolution-mode': 'import' };
export type { PQueue };

// Workaround for jest not yet supporting `require(esm)` capabilities.

export const PQueueModule = {
  get class() {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const inst: typeof PQueue = null as any;
    if (inst === null) {
      throw new Error('p-queue module is not initialized');
    }
    return inst;
  },
};

let importPromise: Promise<void> | null = null;
if (importPromise === null) {
  importPromise = import('p-queue')
    .then(({ default: PQueue }) => {
      Object.defineProperty(PQueueModule, 'class', {
        get() {
          return PQueue;
        },
      });
    })
    .catch((error: unknown) => {
      console.error('Error importing PQueue', error);
      throw error;
    });
}
