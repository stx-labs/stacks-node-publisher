import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, timeout, waiter, Waiter } from '@hirosystems/api-toolkit';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { Message, MessagePath } from './messages';

/**
 * The starting position for the message stream. Can be either an index block hash with block height
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
export type StreamPosition =
  | { indexBlockHash: string; blockHeight: number }
  | { messageId: string }
  | null;

/**
 * The callback function for retrieving the starting position for the event stream. Should return
 * either an index block hash with block height or a message ID. This callback is used to determine
 * the starting message ID for the event stream and may be called periodically on reconnection.
 */
export type StreamPositionCallback = () => Promise<StreamPosition>;

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
 * Message paths to include in a message stream, where `*` means client wants to receive all
 * messages.
 */
export type SelectedMessagePaths = MessagePath[] | '*';

/**
 * Stacks message stream options.
 */
export type StreamOptions = {
  /**
   * Message paths to include in a message stream, where `*` means client wants to receive all
   * messages.
   */
  selectedMessagePaths?: SelectedMessagePaths;
  /**
   * The batch size for the message stream.
   */
  batchSize?: number;
};

/**
 * A client for a Stacks core node message stream.
 */
export class StacksMessageStream {
  static readonly GROUP_NAME = 'primary_group';
  static readonly CONSUMER_NAME = 'primary_consumer';

  readonly client: RedisClientType;
  private readonly selectedPaths: SelectedMessagePaths;
  clientId = randomUUID();
  private readonly redisStreamPrefix: string;
  private readonly appName: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter;

  private readonly logger = defaultLogger.child({ module: 'StacksEventStream' });
  private readonly msgBatchSize: number;

  /** The last message ID that was processed by this client. */
  lastProcessedMessageId: string = '0-0';

  connectionStatus: 'not_started' | 'connected' | 'reconnecting' | 'ended' = 'not_started';

  readonly events = new EventEmitter<{
    redisConsumerGroupDestroyed: [];
    msgReceived: [{ id: string }];
  }>();

  constructor(args: {
    appName: string;
    redisUrl?: string;
    redisStreamPrefix?: string;
    options?: StreamOptions;
  }) {
    this.selectedPaths = args.options?.selectedMessagePaths ?? '*';
    this.abort = new AbortController();
    this.streamWaiter = waiter();
    this.redisStreamPrefix = args.redisStreamPrefix ?? '';
    if (this.redisStreamPrefix !== '' && !this.redisStreamPrefix.endsWith(':')) {
      this.redisStreamPrefix += ':';
    }
    this.appName = this.sanitizeRedisClientName(args.appName);
    this.msgBatchSize = args.options?.batchSize ?? 100;

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

  // Sanitize the redis client name to only include valid characters (same approach used in the
  // StackExchange.RedisClient https://github.com/StackExchange/StackExchange.Redis/pull/2654/files)
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

  start(positionCallback: StreamPositionCallback, messageCallback: MessageCallback) {
    this.logger.info('Starting stream ingestion');
    const runIngest = async () => {
      while (!this.abort.signal.aborted) {
        try {
          const startingPosition = await positionCallback();
          this.logger.info(`Starting position: ${JSON.stringify(startingPosition)}`);
          await this.ingestEventStream(startingPosition, messageCallback);
        } catch (error: unknown) {
          if (this.abort.signal.aborted) {
            this.logger.info('Stream ingestion aborted');
            break;
          } else if ((error as Error).message?.includes('NOGROUP')) {
            // The redis stream doesn't exist. This can happen if the redis server was restarted,
            // or if the client is idle/offline, or if the client is processing messages too slowly.
            // If this code path is reached, then we're obviously online so we just need to re-initialize
            // the connection.
            this.logger.error(
              error as Error,
              `The Redis stream group for this client was destroyed by the server`
            );
            this.events.emit('redisConsumerGroupDestroyed');
            // re-announce connection, re-create group, etc
            continue;
          } else {
            // TODO: what are other expected errors and how should we handle them? For now we just retry
            // forever.
            this.logger.error(error as Error, 'Error reading or acknowledging from stream');
            this.logger.info('Reconnecting to Redis stream in 1 second...');
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
        this.logger.error(err as Error, 'Ingestion stream error');
      });
  }

  private async ingestEventStream(
    startingPosition: StreamPosition,
    eventCallback: MessageCallback
  ): Promise<void> {
    // Reset clientId for each new connection, this prevents race-conditions around cleanup
    // for any previous connections.
    this.clientId = randomUUID();
    this.logger.info(`Connecting to Redis stream with clientId: ${this.clientId}`);
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
      selected_paths: JSON.stringify(this.selectedPaths),
    };

    // Announce connection to the backend and wait until the server acknowledges by consuming the
    // handshake message
    const connectionStreamKey = this.redisStreamPrefix + 'connection_stream';
    const handshakeMsgId = await this.client.xAdd(connectionStreamKey, '*', handshakeMsg);
    this.logger.info(
      `Sent handshake message ${handshakeMsgId}, waiting for server to acknowledge...`
    );
    while (!this.abort.signal.aborted) {
      const msgExists = await this.client.xRange(
        connectionStreamKey,
        handshakeMsgId,
        handshakeMsgId
      );
      if (msgExists.length === 0) {
        // Message was consumed by the server
        this.logger.info('Server acknowledged connection handshake');
        break;
      }
      await timeout(100);
    }

    // Start reading messages from the stream.
    while (!this.abort.signal.aborted) {
      // The backend creates the group with the correct starting position, so we use '>' here to
      // get only messages after the last message ID.
      const results = await this.client.xReadGroup(
        StacksMessageStream.GROUP_NAME,
        StacksMessageStream.CONSUMER_NAME,
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
            .xAck(streamKey, StacksMessageStream.GROUP_NAME, item.id)
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
