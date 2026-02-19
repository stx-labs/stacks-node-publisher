import { createClient, RedisClientType } from 'redis';
import { logger as defaultLogger, timeout, waiter, Waiter } from '@stacks/api-toolkit';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { Message, MessagePath } from './messages';

export * from './messages';

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
   * messages. Defaults to `*`.
   */
  selectedMessagePaths?: SelectedMessagePaths;
  /**
   * The batch size for the message stream. Controls how many messages are read from the stream at
   * once. Defaults to 100.
   */
  batchSize?: number;
  /**
   * Maximum time in milliseconds to wait without receiving any messages before reconnecting. This
   * prevents the client from spinning forever on an orphaned stream if the server restarts and
   * fails to clean up the client's consumer group. Defaults to 120_000 (2 minutes). Set to 0 to
   * disable.
   */
  noMessageTimeoutMs?: number;
};

/** Type for xReadGroup response entries (Redis v5) */
type XReadGroupResponseEntry = {
  name: string;
  messages: { id: string; message: Record<string, string> }[];
};

/**
 * Error thrown when no messages are received for a given timeout.
 */
class NoMessageTimeoutError extends Error {
  constructor(
    public readonly elapsedMs: number,
    public readonly timeoutMs: number
  ) {
    super(`No messages received for ${elapsedMs}ms (timeout: ${timeoutMs}ms), reconnecting`);
    this.name = 'NoMessageTimeoutError';
  }
}

class MessageIngestionError extends Error {
  constructor(public readonly cause: unknown) {
    super(`Error ingesting message: ${cause}`);
    this.name = 'MessageIngestionError';
  }
}

/**
 * A client for a Stacks core node message stream.
 */
export class StacksMessageStream {
  static readonly GROUP_NAME = 'primary_group';
  static readonly CONSUMER_NAME = 'primary_consumer';

  readonly client: RedisClientType;
  public clientId = randomUUID();

  private readonly selectedPaths: SelectedMessagePaths;
  private readonly redisStreamPrefix: string;
  private readonly appName: string;

  private readonly abort: AbortController;
  private readonly streamWaiter: Waiter;

  private readonly logger = defaultLogger.child({ module: 'StacksMessageStream' });
  private readonly msgBatchSize: number;
  private readonly noMessageTimeoutMs: number;

  /** For testing purposes only. The last message ID that was processed by this client. */
  public lastProcessedMessageId: string = '0-0';
  /** For testing purposes only. The connection status of the client. */
  public connectionStatus: 'not_started' | 'connected' | 'reconnecting' | 'ended' = 'not_started';

  readonly events = new EventEmitter<{
    redisConsumerGroupDestroyed: [];
    connectionAnnounced: [{ clientId: string }];
    noMessageTimeoutReconnect: [{ clientId: string }];
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
    this.noMessageTimeoutMs = args.options?.noMessageTimeoutMs ?? 120_000;

    this.client = createClient({
      url: args.redisUrl,
      name: this.redisClientName,
      disableOfflineQueue: true,
    });

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) =>
      this.logger.error(err, `Redis error for client ${this.clientId}`)
    );
    this.client.on('reconnecting', () => {
      this.connectionStatus = 'reconnecting';
      this.logger.info(`Reconnecting to Redis for client ${this.clientId}`);
    });
    this.client.on('ready', () => {
      this.connectionStatus = 'connected';
      this.logger.info(`Redis connection ready for client ${this.clientId}`);
    });
    this.client.on('end', () => {
      this.connectionStatus = 'ended';
      this.logger.info(`Redis connection ended for client ${this.clientId}`);
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
          this.logger.info(`Connected to Redis for client ${this.clientId}`);
          break;
        } catch (err) {
          this.logger.error(
            err as Error,
            `Error connecting to Redis for client ${this.clientId}, retrying...`
          );
          await timeout(500);
        }
      }
    } else {
      void this.client.connect().catch((err: unknown) => {
        this.logger.error(
          err as Error,
          `Error connecting to Redis for client ${this.clientId}, retrying...`
        );
        void timeout(500).then(() => this.connect({ waitForReady }));
      });
    }
  }

  start(positionCallback: StreamPositionCallback, messageCallback: MessageCallback) {
    this.logger.info(`Starting stream ingestion for client ${this.clientId}`);
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
          } else if (error instanceof NoMessageTimeoutError) {
            this.logger.info(
              `No messages received for ${error.elapsedMs}ms (timeout: ${error.timeoutMs}ms), restarting stream`
            );
            continue;
          } else if (error instanceof MessageIngestionError) {
            this.logger.error(error.cause as Error, `Error ingesting message: ${error.cause}`);
            continue;
          } else if ((error as Error).message?.includes('NOGROUP')) {
            // The redis stream doesn't exist. This can happen if the redis server was restarted,
            // or if the client is idle/offline, or if the client is processing messages too slowly.
            // If this code path is reached, then we're obviously online so we just need to re-initialize
            // the connection.
            this.logger.error(
              error as Error,
              `The Redis stream group for this client was destroyed by the server for client ${this.clientId}`
            );
            this.events.emit('redisConsumerGroupDestroyed');
            // re-announce connection, re-create group, etc
            continue;
          } else {
            // TODO: what are other expected errors and how should we handle them? For now we just retry
            // forever.
            this.logger.error(
              error as Error,
              `Error reading or acknowledging from stream for client ${this.clientId}`
            );
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
    messageCallback: MessageCallback
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

    await this.client
      .multi()
      // Announce connection
      .xAdd(this.redisStreamPrefix + 'connection_stream', '*', handshakeMsg)
      // Create group for this stream
      .xGroupCreate(streamKey, StacksMessageStream.GROUP_NAME, '$', {
        MKSTREAM: true,
      })
      .exec();
    this.events.emit('connectionAnnounced', { clientId: this.clientId });

    // Start reading messages from the stream.
    let lastMessageTime = Date.now();
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
      if (!results) {
        // Check if we've been starved of messages for too long. This can happen if the server
        // restarts and fails to clean up this client's stream/group, leaving us polling an
        // orphaned stream that will never receive new messages.
        if (this.noMessageTimeoutMs > 0) {
          const elapsed = Date.now() - lastMessageTime;
          if (elapsed > this.noMessageTimeoutMs) {
            this.events.emit('noMessageTimeoutReconnect', { clientId: this.clientId });
            throw new NoMessageTimeoutError(elapsed, this.noMessageTimeoutMs);
          }
        }
        continue;
      }
      for (const stream of results as XReadGroupResponseEntry[]) {
        if (stream.messages.length > 0) {
          this.logger.debug(
            `Received messages ${stream.messages[0].id} - ${stream.messages[stream.messages.length - 1].id} with client ${this.clientId}`
          );
          lastMessageTime = Date.now();
        }
        for (const item of stream.messages) {
          try {
            await messageCallback(item.id, item.message.timestamp, {
              path: item.message.path as MessagePath,
              payload: JSON.parse(item.message.body),
            });
          } catch (error: unknown) {
            throw new MessageIngestionError(error);
          }
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
    await this.client.close().catch((error: unknown) => {
      if ((error as Error).message?.includes('client is closed')) {
        // ignore
      } else {
        throw error;
      }
    });
  }
}
