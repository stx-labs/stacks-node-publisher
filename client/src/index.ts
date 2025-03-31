import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, timeout, waiter, Waiter } from '@hirosystems/api-toolkit';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';

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
  readonly client: RedisClientType;
  private readonly eventStreamType: StacksEventStreamType;
  clientId = randomUUID();
  lastMessageId: string;
  private readonly redisStreamPrefix: string;
  private readonly appName: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter<void>;

  private readonly logger = defaultLogger.child({ module: 'StacksEventStream' });

  private readonly GROUP_NAME = 'primary_group';
  private readonly CONSUMER_NAME = 'primary_consumer';
  private readonly msgBatchSize: number;

  connectionStatus: 'not_started' | 'connected' | 'reconnecting' | 'ended' = 'not_started';

  readonly events = new EventEmitter<{
    redisConsumerGroupDestroyed: [];
    msgReceived: [{ id: string }];
  }>();

  constructor(args: {
    redisUrl?: string;
    eventStreamType: StacksEventStreamType;
    lastMessageId?: string;
    redisStreamPrefix?: string;
    appName: string;
    msgBatchSize?: number;
  }) {
    this.eventStreamType = args.eventStreamType;
    this.lastMessageId = args.lastMessageId ?? '0'; // Automatically start at the first message.
    this.abort = new AbortController();
    this.streamWaiter = waiter();
    this.redisStreamPrefix = args.redisStreamPrefix ?? '';
    if (this.redisStreamPrefix !== '' && !this.redisStreamPrefix.endsWith(':')) {
      this.redisStreamPrefix += ':';
    }
    this.appName = this.sanitizeRedisClientName(args.appName);
    this.msgBatchSize = args.msgBatchSize ?? 100;

    this.client = createClient({
      url: args.redisUrl,
      name: this.redisClientName,
      disableOfflineQueue: true,
    });

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) => this.logger.error(err, 'Redis error'));
    this.client.on('reconnecting', () => {
      this.connectionStatus = 'reconnecting';
      this.logger.info('Reconnecting to Redis');
    });
    this.client.on('ready', () => {
      this.connectionStatus = 'connected';
      this.logger.info('Redis connection ready');
    });
    this.client.on('end', () => {
      this.connectionStatus = 'ended';
      this.logger.info('Redis connection ended');
    });
  }

  // Sanitize the redis client name to only include valid characters (same approach used in the StackExchange.RedisClient https://github.com/StackExchange/StackExchange.Redis/pull/2654/files)
  sanitizeRedisClientName(name: string): string {
    const nameSanitizer = /[^!-~]+/g;
    return name.trim().replace(nameSanitizer, '-');
  }

  get redisClientName() {
    return `${this.redisStreamPrefix}snp-consumer:${this.appName}:${this.clientId}`;
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
    const runIngest = async () => {
      while (!this.abort.signal.aborted) {
        try {
          await this.ingestEventStream(callback);
        } catch (error: unknown) {
          if (this.abort.signal.aborted) {
            this.logger.info('Event stream ingestion aborted');
            break;
          } else if ((error as Error).message?.includes('NOGROUP')) {
            // The redis stream doesn't exist. This can happen if the redis server was restarted,
            // or if the client is idle/offline, or if the client is processing messages too slowly.
            // If this code path is reached, then we're obviously online so we just need to re-initialize
            // the connection.
            this.logger.error(
              error as Error,
              `The redis stream group for this client was destroyed by the server`
            );
            this.events.emit('redisConsumerGroupDestroyed');
            // re-announce connection, re-create group, etc
            continue;
          } else {
            // TODO: what are other expected errors and how should we handle them? For now we just retry
            // forever.
            this.logger.error(error as Error, 'Error reading or acknowledging from stream');
            this.logger.info('Reconnecting to redis stream in 1 second...');
            await timeout(1000);
            continue;
          }
        }
      }
    };
    void runIngest()
      .then(() => {
        this.streamWaiter.finish();
      })
      .catch((err: unknown) => {
        this.logger.error(err as Error, 'event ingestion stream error');
      });
  }

  private async ingestEventStream(eventCallback: StreamedStacksEventCallback): Promise<void> {
    // Reset clientId for each new connection, this prevents race-conditions around cleanup
    // for any previous connections.
    this.clientId = randomUUID();
    this.logger.info(`Connecting to redis stream with clientId: ${this.clientId}`);
    const streamKey = `${this.redisStreamPrefix}client:${this.eventStreamType}:${this.clientId}`;
    await this.client.clientSetName(this.redisClientName);

    const handshakeMsg = {
      client_id: this.clientId,
      last_message_id: this.lastMessageId,
      app_name: this.appName,
    };

    await this.client
      .multi()
      // Announce connection
      .xAdd(this.redisStreamPrefix + 'connection_stream', '*', handshakeMsg)
      // Create group for this stream
      .xGroupCreate(streamKey, this.GROUP_NAME, this.lastMessageId, {
        MKSTREAM: true,
      })
      .exec();

    while (!this.abort.signal.aborted) {
      // Because we specify the lastMessageId during the announce handshake and the above
      // xGroupCreate, use '>' here to get the next messages.
      const results = await this.client.xReadGroup(
        this.GROUP_NAME,
        this.CONSUMER_NAME,
        {
          key: streamKey,
          id: '>',
        },
        {
          COUNT: this.msgBatchSize,
          BLOCK: 1000, // Wait 1 second for new events.
        }
      );
      if (!results || results.length === 0) {
        continue;
      }
      for (const stream of results) {
        if (stream.messages.length > 0) {
          this.logger.debug(
            `Received messages ${stream.messages[0].id} - ${stream.messages[stream.messages.length - 1].id}`
          );
        }
        for (const item of stream.messages) {
          await eventCallback(
            item.id,
            item.message.timestamp,
            item.message.path,
            JSON.parse(item.message.body)
          );
          this.lastMessageId = item.id;

          this.events.emit('msgReceived', { id: item.id });

          await this.client
            .multi()
            // Acknowledge the message so that it is removed from the server's Pending Entries List (PEL)
            .xAck(streamKey, this.GROUP_NAME, item.id)
            // Delete the message from the stream so that it doesn't get reprocessed
            .xDel(streamKey, item.id)
            .exec();
        }
      }
    }
  }

  async stop() {
    this.abort.abort();
    await this.streamWaiter;
    await this.client.quit().catch((error: unknown) => {
      if ((error as Error).message?.includes('client is closed')) {
        // ignore
      } else {
        throw error;
      }
    });
  }
}
