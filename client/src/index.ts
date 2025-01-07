import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, waiter, Waiter } from '@hirosystems/api-toolkit';

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
  private lastMessageId: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter<void>;

  private readonly logger = defaultLogger.child({ module: 'StacksEventStream' });

  constructor(args: {
    redisUrl?: string;
    eventStreamType: StacksEventStreamType;
    lastMessageId?: string;
  }) {
    this.client = createClient({ url: args.redisUrl });
    this.eventStreamType = args.eventStreamType;
    this.lastMessageId = args.lastMessageId ?? '0'; // Automatically start at the first message.
    this.abort = new AbortController();
    this.streamWaiter = waiter();
  }

  async connect() {
    await new Promise<void>((resolve, reject) => {
      this.client.on('error', err => {
        this.logger.error(err as Error, 'Redis error');
        reject(err as Error);
      });
      this.client.on('ready', () => {
        this.logger.info('Connected to Redis');
        resolve();
      });
      this.client.connect().catch((err: unknown) => {
        this.logger.error(err as Error, 'Error connecting to Redis');
        reject(err as Error);
      });
    });
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
      while (!this.abort.signal.aborted) {
        const results = await this.client.xRead(
          [{ key: this.eventStreamType, id: this.lastMessageId }],
          {
            COUNT: 1,
            BLOCK: 1000, // Wait 1 second for new events.
          }
        );
        if (results && results.length > 0) {
          for (const stream of results) {
            for (const item of stream.messages) {
              const content = JSON.parse(item.message.content) as {
                timestamp: string;
                path: string;
                body: string;
              };
              await eventCallback(
                item.id,
                content.timestamp,
                content.path,
                JSON.parse(content.body)
              );
              this.lastMessageId = item.id;
            }
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
}
