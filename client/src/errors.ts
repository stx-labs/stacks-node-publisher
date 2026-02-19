/**
 * Error thrown when no messages are received for a given timeout.
 */
export class NoMessageTimeoutError extends Error {
  constructor(
    public readonly elapsedMs: number,
    public readonly timeoutMs: number
  ) {
    super(`No messages received for ${elapsedMs}ms (timeout: ${timeoutMs}ms)`);
    this.name = 'NoMessageTimeoutError';
  }
}

/**
 * Error thrown when an error occurs while ingesting a message.
 */
export class MessageIngestionError extends Error {
  constructor(public readonly cause: unknown) {
    super(`Error ingesting message: ${cause}`);
    this.name = 'MessageIngestionError';
  }
}
