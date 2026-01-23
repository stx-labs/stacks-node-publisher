import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, timeout, waiter, Waiter } from '@hirosystems/api-toolkit';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { Message, MessagePath } from './messages';

/**
 * The starting position for the event stream. Can be either an index block hash with block height
 * or a message ID.
 * - `indexBlockHash` + `blockHeight`: The index block hash and height of the Stacks block to start
 *   from. The backend will resolve this to the corresponding message ID. If the block hash doesn't
 *   exist but the height is higher than the highest available, it will start from the highest
 *   available block.
 * - `messageId`: The message ID to start from. The backend will validate this ID exists and is not
 *   greater than the highest available ID.
 *
 * If neither is provided or validation fails, the stream will start from the beginning.
 */
export type StreamStartingPosition =
  | { indexBlockHash: string; blockHeight: number }
  | { messageId: string }
  | null;

/**
 * The callback function for retrieving the starting position for the event stream. Should return
 * either an index block hash with block height or a message ID. This callback is used to determine
 * the starting message ID for the event stream and may be called periodically on reconnection.
 */
export type StreamStartingPositionCallback = () => Promise<StreamStartingPosition>;

/**
 * The callback function for event stream ingestion. Will be called for each message in the event
 * stream. The callback should return a promise that resolves when the message has been processed.
 */
export type MessageCallback = (
  /** The message ID. */
  id: string,
  /** The timestamp of the message. */
  timestamp: string,
  /** The message */
  message: Message
) => Promise<void>;

/**
 * The type of events to ingest depending on the client's Stacks needs.
 */
export enum StacksEventStreamType {
  /** All blockchain and mempool events, excluding signer messages. */
  chainEvents = 'chain_events',
  /** Only confirmed blockchain events: blocks and burn blocks. */
  confirmedChainEvents = 'confirmed_chain_events',
  /** Only signer messages. */
  signerEvents = 'signer_events',
  /** All events. */
  all = 'all',
}

/**
 * A client for the Stacks SNP event stream.
 */
export class StacksEventStream {
  static readonly GROUP_NAME = 'primary_group';
  static readonly CONSUMER_NAME = 'primary_consumer';

  readonly client: RedisClientType;
  private readonly eventStreamType: StacksEventStreamType;
  clientId = randomUUID();
  private readonly redisStreamPrefix: string;
  private readonly appName: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter<void>;

  private readonly logger = defaultLogger.child({ module: 'StacksEventStream' });
  private readonly msgBatchSize: number;

  /** The last message ID that was processed by this client. */
  lastProcessedMessageId: string = '0';

  connectionStatus: 'not_started' | 'connected' | 'reconnecting' | 'ended' = 'not_started';

  readonly events = new EventEmitter<{
    redisConsumerGroupDestroyed: [];
    msgReceived: [{ id: string }];
  }>();

  constructor(args: {
    appName: string;
    redisUrl?: string;
    redisStreamPrefix?: string;
    options?: {
      eventStreamType?: StacksEventStreamType;
      msgBatchSize?: number;
    };
  }) {
    this.eventStreamType = args.options?.eventStreamType ?? StacksEventStreamType.all;
    this.abort = new AbortController();
    this.streamWaiter = waiter();
    this.redisStreamPrefix = args.redisStreamPrefix ?? '';
    if (this.redisStreamPrefix !== '' && !this.redisStreamPrefix.endsWith(':')) {
      this.redisStreamPrefix += ':';
    }
    this.appName = this.sanitizeRedisClientName(args.appName);
    this.msgBatchSize = args.options?.msgBatchSize ?? 100;

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

  start(
    startingPositionCallback: StreamStartingPositionCallback,
    messageCallback: MessageCallback
  ) {
    this.logger.info('Starting event stream ingestion');
    const runIngest = async () => {
      while (!this.abort.signal.aborted) {
        try {
          const startingPosition = await startingPositionCallback();
          this.logger.info(`Starting position: ${JSON.stringify(startingPosition)}`);
          await this.ingestEventStream(startingPosition, messageCallback);
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

  private async ingestEventStream(
    startingPosition: StreamStartingPosition,
    eventCallback: MessageCallback
  ): Promise<void> {
    // Reset clientId for each new connection, this prevents race-conditions around cleanup
    // for any previous connections.
    this.clientId = randomUUID();
    this.logger.info(`Connecting to redis stream with clientId: ${this.clientId}`);
    const streamKey = `${this.redisStreamPrefix}client:${this.clientId}`;
    await this.client.clientSetName(this.redisClientName);

    const handshakeMsg: Record<string, string> = {
      client_id: this.clientId,
      last_index_block_hash:
        startingPosition && 'indexBlockHash' in startingPosition
          ? startingPosition.indexBlockHash
          : '',
      last_block_height:
        startingPosition && 'blockHeight' in startingPosition
          ? startingPosition.blockHeight.toString()
          : '',
      last_message_id:
        startingPosition && 'messageId' in startingPosition ? startingPosition.messageId : '',
      app_name: this.appName,
      stream_type: this.eventStreamType,
    };

    // Announce connection to the backend
    await this.client.xAdd(this.redisStreamPrefix + 'connection_stream', '*', handshakeMsg);

    // Wait for the backend to create the consumer group on our stream.
    // The backend will resolve the index_block_hash to a sequence number and create the group.
    this.logger.info('Waiting for backend to create consumer group...');
    while (!this.abort.signal.aborted) {
      try {
        // Try to create a consumer in the group - this will succeed if the group exists
        await this.client.xGroupCreateConsumer(
          streamKey,
          StacksEventStream.GROUP_NAME,
          StacksEventStream.CONSUMER_NAME
        );
        this.logger.info('Consumer group ready, starting to read messages');
        break;
      } catch (err) {
        if ((err as Error).message?.includes('NOGROUP')) {
          // Group not created yet, wait and retry
          await timeout(100);
          continue;
        }
        throw err;
      }
    }

    while (!this.abort.signal.aborted) {
      // The backend creates the group with the correct starting position based on index_block_hash,
      // so we use '>' here to get only messages after the last message ID.
      const results = await this.client.xReadGroup(
        StacksEventStream.GROUP_NAME,
        StacksEventStream.CONSUMER_NAME,
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
          await eventCallback(item.id, item.message.timestamp, {
            path: item.message.path as MessagePath,
            payload: JSON.parse(item.message.body),
          });

          this.lastProcessedMessageId = item.id;
          this.events.emit('msgReceived', { id: item.id });

          await this.client
            .multi()
            // Acknowledge the message so that it is removed from the server's Pending Entries List (PEL)
            .xAck(streamKey, StacksEventStream.GROUP_NAME, item.id)
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
