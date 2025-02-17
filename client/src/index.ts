import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, timeout, waiter, Waiter } from '@hirosystems/api-toolkit';
import { randomUUID } from 'node:crypto';

export type StreamedStacksEventCallback = (
  id: string,
  timestamp: string,
  path: string,
  payload: any
) => Promise<void>;

export enum StacksEventStreamType {
  chainEvents = 'chain_events',
  signerEvents = 'signer_events',
  all = 'all',
}

export class StacksEventStream {
  private readonly client: RedisClientType;
  private readonly eventStreamType: StacksEventStreamType;
  private readonly clientId = randomUUID();
  private lastMessageId: string;
  private readonly redisStreamPrefix: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter<void>;

  private readonly logger = defaultLogger.child({ module: 'StacksEventStream' });

  constructor(args: {
    redisUrl?: string;
    eventStreamType: StacksEventStreamType;
    lastMessageId?: string;
    redisStreamPrefix?: string;
  }) {
    this.client = createClient({ url: args.redisUrl });
    this.eventStreamType = args.eventStreamType;
    this.lastMessageId = args.lastMessageId ?? '0'; // Automatically start at the first message.
    this.abort = new AbortController();
    this.streamWaiter = waiter();
    this.redisStreamPrefix = args.redisStreamPrefix ?? '';

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) => this.logger.error(err, 'Redis error'));
    this.client.on('reconnecting', () => this.logger.info('Reconnecting to Redis'));
    this.client.on('ready', () => this.logger.info('Redis connection ready'));
  }

  async connect({ waitForReady }: { waitForReady: boolean }) {
    // Taken from `RedisBroker`.
    if (waitForReady) {
      while (true) {
        try {
          await this.client.connect();
          this.logger.info('Connected to Redis');
          break;
        } catch (err) {
          this.logger.error(err as Error, 'Error connecting to Redis, retrying...');
          await timeout(500);
        }
      }
    } else {
      void this.client.connect().catch((err: unknown) => {
        this.logger.error(err as Error, 'Error connecting to Redis, retrying...');
        void timeout(500).then(() => this.connect({ waitForReady }));
      });
    }
  }

  start(callback: StreamedStacksEventCallback) {
    this.logger.info('Starting event stream ingestion');
    this.ingestEventStream(callback)
      .then(() => {
        this.streamWaiter.finish();
      })
      .catch((err: unknown) => {
        this.logger.error(err as Error, 'event stream error');
      });
  }

  private async ingestEventStream(eventCallback: StreamedStacksEventCallback): Promise<void> {
    try {
      const streamKey = this.redisStreamPrefix + this.eventStreamType;
      while (!this.abort.signal.aborted) {
        const results = await this.client.xRead(
          { key: streamKey, id: this.lastMessageId },
          {
            COUNT: 1,
            BLOCK: 1000, // Wait 1 second for new events.
          }
        );
        if (!results || results.length === 0) {
          continue;
        }
        for (const stream of results) {
          for (const item of stream.messages) {
            await eventCallback(
              item.id,
              item.message.timestamp,
              item.message.path,
              JSON.parse(item.message.body)
            );
            this.lastMessageId = item.id;
            // Acknowledge the message so it won't be read again.
            await this.client.xDel(streamKey, item.id);
          }
        }
      }
    } catch (error: unknown) {
      this.logger.error(error as Error, 'Error reading or acknowledging from stream');
    }
  }

  async stop() {
    this.abort.abort();
    await this.streamWaiter;
    await this.client.quit();
  }

  // Announce connection via Redis stream
  async announceConnection() {
    const msg = {
      client_id: this.clientId,
      last_message_id: this.lastMessageId,
    };
    await this.client.xAdd(this.redisStreamPrefix + 'connection_stream', '*', msg);
  }
}
