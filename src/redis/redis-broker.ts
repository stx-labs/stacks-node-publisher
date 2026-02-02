import { ClientClosedError, createClient, WatchError } from 'redis';
import { logger as defaultLogger, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { PgStore } from '../pg/pg-store';
import { createTestHook, isTestEnv } from '../helpers';
import {
  closeRedisClient,
  destroyRedisClient,
  RedisClient,
  XInfoGroupsResponse,
  XInfoStreamEntry,
  XReadGroupResponseEntry,
} from './redis-types';
import { unwrapRedisMultiErrorReply, xInfoStreamFull, xInfoStreamsFull } from './redis-util';
import { EventEmitter } from 'node:events';
import { SelectedMessagePaths } from '../../client/src';
import { MessagePath } from '../../client/src/messages';

/**
 * A Stacks message to be added to the Redis stream. These messages come from the Stacks node
 * and are added to the Redis stream for consumption by clients.
 */
type StacksMessage = {
  timestamp: string;
  sequenceNumber: string;
  eventPath: string;
  eventBody: string;
};

/** Identifies a Stacks client on the Redis broker. */
type StacksClient = {
  client: RedisClient;
  appName: string;
  clientId: string;
  selectedPaths: SelectedMessagePaths;
};

/** Identifies an actual Stacks client connection on the Redis broker. */
type StacksClientConnection = StacksClient & {
  clientStreamKey: string;
  chainTipStreamGroupKey: string;
  chainTipStreamConsumerKey: string;
  lastStreamedSequenceNumber: string;
};

/**
 * The result of a chain tip stream trim operation.
 * - `no_stream_exists`: the chain tip stream does not exist.
 * - `trimmed_minid`: the chain tip stream was trimmed to the minimum ID.
 * - `trimmed_maxlen`: the chain tip stream was trimmed to the maximum length.
 * - `aborted`: the trim operation was aborted.
 */
type ChainTipStreamTrimResult =
  | { result: 'no_stream_exists' }
  | { result: 'trimmed_minid'; id: number }
  | { result: 'trimmed_maxlen' }
  | { result: 'aborted' };

/**
 * A Redis broker for the Stacks message stream. This broker is responsible for adding messages to
 * the Redis stream and for consuming messages from the Redis stream.
 *
 * It uses three types of Redis streams:
 * - **Chain tip stream**: The primary stream where Stacks node messages are ingested. A consumer
 *   group is created per client to enable live-streaming from the chain tip.
 * - **Connection stream**: Receives new client connection requests. Clients write to this stream to
 *   initiate a connection, and the broker processes these requests.
 * - **Client streams**: Per-client streams (`client:{clientId}`) where messages are written for
 *   consumption. Messages are either backfilled from Postgres or forwarded from the chain tip
 *   stream.
 *
 * It uses three Redis clients:
 * - `client`: the primary client for the Stacks message stream.
 * - `ingestionClient`: a client for ingesting messages into the Redis stream.
 * - `listeningClient`: a client for listening for new connections and serving clients.
 *
 * It also uses a map of per-consumer clients to serve each client individually.
 */
export class RedisBroker {
  client: RedisClient;
  ingestionClient: RedisClient;
  listeningClient: RedisClient;
  perConsumerClients = new Map<RedisClient, { clientId: string }>();
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });

  readonly redisStreamKeyPrefix: string;
  readonly chainTipStreamKey: string;

  readonly abortController = new AbortController();
  readonly db: PgStore;

  private cleanupIntervalTimer: NodeJS.Timeout | null = null;

  readonly events = new EventEmitter<{
    /** Emitted when a consumer is pruned due to being idle (not consuming) for too long */
    idleConsumerPruned: [{ clientId: string }];
    /** Emitted when a consumer falls behind MAX_MSG_LAG and is demoted from live streaming to postgres backfill */
    consumerDemotedToBackfill: [{ clientId: string }];
    /** Emitted when a consumer catches up with the chain tip stream and is promoted to live streaming */
    consumerPromotedToLiveStream: [{ clientId: string }];
    perConsumerClientCreated: [{ clientId: string }];
    ingestionMsgGapDetected: [];
    ingestionToEmptyRedisDb: [];
    redisClientsConnected: [];
  }>();

  _testHooks = isTestEnv
    ? {
        onLiveStreamTransition: createTestHook<() => Promise<void>>(),
        onLiveStreamDrained: createTestHook<() => Promise<void>>(),
        onTrimChainTipStreamGetGroups: createTestHook<() => Promise<void>>(),
        onPgBackfillLoop: createTestHook<(msgId: string) => Promise<void>>(),
        onAddStacksMsg: createTestHook<(msgId: string) => Promise<void>>(),
        onBeforePgBackfillQuery: createTestHook<(msgId: string) => Promise<void>>(),
        onBeforeLivestreamXReadGroup: createTestHook<(msgId: string) => Promise<void>>(),
        onDemoteToBackfill: createTestHook<(clientId: string) => Promise<void>>(),
      }
    : null;

  constructor(args: { redisUrl: string | undefined; redisStreamKeyPrefix: string; db: PgStore }) {
    this.db = args.db;
    this.redisStreamKeyPrefix = args.redisStreamKeyPrefix;
    if (this.redisStreamKeyPrefix !== '' && !this.redisStreamKeyPrefix.endsWith(':')) {
      this.redisStreamKeyPrefix += ':';
    }
    this.chainTipStreamKey = this.redisStreamKeyPrefix + 'global_stream';

    this.client = createClient({
      url: args.redisUrl,
      disableOfflineQueue: true,
      name: `${this.redisStreamKeyPrefix}snp-primary-client`,
    });

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) => this.logger.error(err, 'Redis error'));
    this.client.on('reconnecting', () => this.logger.info('Reconnecting to Redis'));
    this.client.on('ready', () => this.logger.info('Redis connection ready'));

    // Create a separate client for ingestion that doesn't use the offline queue because
    // we implement logic for this ourselves in a way that avoid backpressure issues.
    this.ingestionClient = this.client.duplicate({
      disableOfflineQueue: true,
      name: `${this.redisStreamKeyPrefix}snp-ingestion`,
    });
    this.ingestionClient.on('error', (err: Error) =>
      this.logger.error(err, 'Redis error on ingestion client')
    );

    this.listeningClient = this.client.duplicate({
      disableOfflineQueue: true,
      name: `${this.redisStreamKeyPrefix}snp-client-listener`,
    });
    this.listeningClient.on('error', (err: Error) =>
      this.logger.error(err, 'Redis error on client connection listener')
    );
  }

  get chainTipStreamGroupVersionKey() {
    return `${this.chainTipStreamKey}:group_version`;
  }

  connect<TWaitForReady extends boolean = false>(args: {
    waitForReady: TWaitForReady;
  }): TWaitForReady extends true ? Promise<void> : undefined {
    this.logger.info(`Using REDIS_STREAM_KEY_PREFIX: '${this.redisStreamKeyPrefix}'`);
    this.logger.info(`Connecting to Redis at ${ENV.REDIS_URL} ...`);
    // Note that the default redis client connect strategy is to retry indefinitely,
    // and so the `client.connect()` call should only actually throw from fatal errors like
    // a an invalid connection URL.
    const connectClients = async () => {
      try {
        await this.client.connect();
        await this.ingestionClient.connect();
        await this.listeningClient.connect();

        const primaryClientID = await this.client.clientId();
        const ingestionClientID = await this.ingestionClient.clientId();
        const listeningClientID = await this.listeningClient.clientId();
        this.logger.info(
          `Connected to Redis, client ID: ${primaryClientID}, ingestion client ID: ${ingestionClientID}, listening client ID: ${listeningClientID}`
        );
        this.events.emit('redisClientsConnected');

        // Start periodic cleanup interval for trimming chain tip stream and pruning idle clients
        if (!this.cleanupIntervalTimer) {
          this.cleanupIntervalTimer = setInterval(() => {
            void this.cleanup();
          }, ENV.CLEANUP_INTERVAL_MS);
          this.logger.info(
            `Started cleanup interval timer with period ${ENV.CLEANUP_INTERVAL_MS}ms`
          );
        }
      } catch (err) {
        this.logger.error(err as Error, 'Fatal error connecting to Redis');
        throw err;
      }
    };

    const connectionListener = async () => {
      while (!this.abortController.signal.aborted) {
        try {
          await this.listenForConnections(this.listeningClient);
        } catch (error) {
          if (this.abortController.signal.aborted) {
            break;
          } else {
            this.logger.error(error as Error, 'Error listening for connections');
            await timeout(1000);
          }
        }
      }
    };

    if (args.waitForReady) {
      return (async () => {
        await connectClients();
        void connectionListener();
      })() as TWaitForReady extends true ? Promise<void> : undefined;
    } else {
      void connectClients()
        .then(() => {
          void connectionListener();
        })
        .catch((err: unknown) => {
          if (!this.abortController.signal.aborted) {
            this.logger.error(err as Error, 'Fatal error connecting to Redis');
            process.exit(1);
          }
        });
      return undefined as TWaitForReady extends true ? Promise<void> : undefined;
    }
  }

  async close() {
    this.abortController.abort();
    if (this.cleanupIntervalTimer) {
      clearInterval(this.cleanupIntervalTimer);
      this.cleanupIntervalTimer = null;
    }
    await closeRedisClient(this.client).catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing primary redis client connection');
      }
    });
    await closeRedisClient(this.ingestionClient).catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing ingestion redis client connection');
      }
    });
    await closeRedisClient(this.listeningClient).catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing listening redis client connection');
      }
    });
    await Promise.all(
      [...this.perConsumerClients].map(([client]) =>
        closeRedisClient(client).catch((error: unknown) => {
          if (!(error instanceof ClientClosedError)) {
            this.logger.debug(error, 'Error closing per-consumer redis client connection');
          }
        })
      )
    );
  }

  /**
   * Add a Stacks message to the Redis stream. This message comes from the Stacks node and is added
   * to the Redis stream for consumption by clients.
   * @param args - The Stacks message to add to the Redis stream.
   */
  public async addStacksMessage(args: StacksMessage) {
    try {
      if (this._testHooks) {
        for (const cb of this._testHooks.onAddStacksMsg) {
          await cb(args.sequenceNumber);
        }
      }
      await this.addStacksMessageInternal(args);
    } catch (error) {
      this.logger.error(error, 'Failed to add message to Redis');
    }
  }

  private async addStacksMessageInternal(args: StacksMessage) {
    // Redis stream message IDs are <millisecondsTime>-<sequenceNumber>.
    // However, we don't fully trust our timestamp to always increase monotonically (e.g. NTP glitches),
    // so we'll just use the sequence number as the timestamp.
    const messageId = `${args.sequenceNumber}-0`;

    try {
      // If redis was unreachable when the last message(s) were added to the chain tip stream,
      // then we need to make sure we don't create msg gaps in the stream. This can be done by
      // checking the last message ID in the stream and making sure it's exactly 1 less than the
      // sequence number of this new message.
      //
      // If there's a gap then we could either backfill from pg, which is tricky. It needs an
      // atomic switch from backfilling to live-streaming and backpressure handling. It could
      // also take a while to complete so unclear on what to do when subsequent messages come in.
      //
      // So instead, we DEL the chain tip stream and all client streams, which triggers a re-initialization
      // of all clients so that they backfill the missing msgs from pg. This is simpler but could cause a
      // lot of churn if redis is frequently unreachable and/or unavailable for a long time.
      const streamInfo = await this.ingestionClient
        .xInfoStream(this.chainTipStreamKey)
        .catch((error: unknown) => {
          if ((error as Error).message?.includes('ERR no such key')) {
            return null;
          }
          throw error;
        });

      if (!streamInfo || !streamInfo.groups) {
        this.events.emit('ingestionToEmptyRedisDb');
        this.logger.info(`Message being added to an empty redis server, msgId=${messageId}`);
      }

      // If there are groups (consumers) on the stream and the stream isn't new/empty, then check for gaps.
      const lastEntry = streamInfo?.['last-entry'] as XInfoStreamEntry | undefined;
      if (streamInfo && streamInfo.groups > 0 && lastEntry) {
        const lastEntryId = parseInt(lastEntry.id.split('-')[0]);
        if (lastEntryId + 1 < parseInt(args.sequenceNumber)) {
          this.events.emit('ingestionMsgGapDetected');
          this.logger.warn(
            `Detected gap in chain tip stream, lastEntryId=${lastEntryId}, sequenceNumber=${args.sequenceNumber}`
          );
          // Delete the chain tip stream and all client streams.
          const groups = await this.ingestionClient.xInfoGroups(this.chainTipStreamKey);
          const multi = this.ingestionClient.multi();
          multi.del(this.chainTipStreamKey);
          for (const group of groups) {
            const clientStream = this.getClientStreamKey(group.name.split(':').at(-1) ?? '');
            multi.del(clientStream);
          }
          await multi.exec();
        }
      }

      const globalRedisMsg = {
        timestamp: args.timestamp,
        path: args.eventPath,
        body: args.eventBody,
      };
      await this.ingestionClient.xAdd(this.chainTipStreamKey, messageId, globalRedisMsg);
    } catch (error) {
      // Ignore error if it's a duplicate message, which could happen if a previous xadd succeeded
      // on the server but failed to send the response back to the client (e.g. network error).
      if ((error as Error).message?.includes('XADD is equal or smaller than the target')) {
        this.logger.warn(`Ignore duplicate message ID ${args.sequenceNumber}`);
      } else {
        this.logger.error(error as Error, 'Failed to add message to global Redis stream');
        throw error;
      }
    }
  }

  /**
   * Run cleanup tasks: trim the chain tip stream and prune idle clients.
   * This is called periodically by the cleanup interval timer.
   */
  private async cleanup() {
    if (this.abortController.signal.aborted) {
      return;
    }
    try {
      await this.trimChainTipStream();
      await this.pruneIdleClients();
    } catch (error) {
      if (!this.abortController.signal.aborted) {
        this.logger.error(error as Error, 'Error running cleanup tasks');
      }
    }
  }

  async listenForConnections(listeningClient: RedisClient) {
    const connectionStreamKey = this.redisStreamKeyPrefix + 'connection_stream';
    while (!this.abortController.signal.aborted) {
      // TODO: if client.close() is called, will throw an error? if so handle gracefully
      const connectionRequests = await listeningClient.xRead(
        { key: connectionStreamKey, id: '0-0' },
        {
          BLOCK: 1000, // wait for 1 second for new messages to allow abort signal to be checked
        }
      );
      if (!connectionRequests) {
        continue;
      }
      for (const request of connectionRequests as XReadGroupResponseEntry[]) {
        for (const msg of request.messages) {
          const msgId = msg.id;
          const msgPayload = msg.message;

          const clientId = msgPayload['client_id'];
          const lastIndexBlockHash = msgPayload['last_index_block_hash'] ?? '';
          const lastBlockHeight = msgPayload['last_block_height'] ?? '';
          const lastMessageId = msgPayload['last_message_id'] ?? '';
          const appName = msgPayload['app_name'];
          const selectedPaths: SelectedMessagePaths =
            msgPayload['selected_paths'] !== '*'
              ? (JSON.parse(msgPayload['selected_paths']) as MessagePath[])
              : '*';
          this.logger.info(msgPayload, `New client connection: ${clientId}, app: ${appName}`);

          // Fire-and-forget promise so multiple clients can connect and backfill and live-stream at once
          const logger = this.logger.child({ clientId, appName });
          const dedicatedClient = this.client.duplicate({
            name: `${this.redisStreamKeyPrefix}snp-producer:${appName}:${clientId}`,
            disableOfflineQueue: true,
          });

          this.perConsumerClients.set(dedicatedClient, { clientId });
          this.events.emit('perConsumerClientCreated', { clientId });

          dedicatedClient.on('error', (err: Error) => {
            if (!this.abortController.signal.aborted) {
              logger.error(err, `Redis error on dedicated client connection for client`);
            }
          });

          // Determine the starting message sequence number for this client by resolving its
          // starting position.
          const startSequenceNumber = await this.resolveClientStartMessageSequenceNumber(
            { lastIndexBlockHash, lastBlockHeight, lastMessageId },
            logger
          );
          // Once we have the starting position, delete the connection request message. This ensures
          // any errors won't make us lose this connection request.
          await listeningClient.xDel(connectionStreamKey, msgId);

          // Serve the client by creating the consumer group and backfilling messages from postgres.
          void this.streamMessagesToClient(
            { client: dedicatedClient, appName, clientId, selectedPaths },
            startSequenceNumber,
            logger
          )
            .catch(async (error: unknown) => {
              error = unwrapRedisMultiErrorReply(error as Error) ?? error;
              if ((error as Error).message?.includes('NOGROUP')) {
                logger.warn(error as Error, `Consumer group not found for client (likely pruned)`);
              } else if (!this.abortController.signal.aborted) {
                logger.error(error as Error, `Error processing msgs for consumer stream`);
              }
              const groupKey = this.getClientChainTipStreamGroupKey(clientId);
              const clientStreamKey = this.getClientStreamKey(clientId);
              await this.client
                .multi()
                // Destroy the chain tip stream consumer group for this client
                .xGroupDestroy(this.chainTipStreamKey, groupKey)
                // Destroy the stream for this client (notifies the client via NOGROUP error on xReadGroup)
                .del(clientStreamKey)
                .exec()
                .catch((error: unknown) => {
                  if (!this.abortController.signal.aborted) {
                    error = unwrapRedisMultiErrorReply(error as Error) ?? error;
                    logger.warn(error, `Error cleaning up client connection`);
                  }
                });
            })
            .finally(() => {
              // Close the dedicated client connection after handling the client
              this.perConsumerClients.delete(dedicatedClient);
              closeRedisClient(dedicatedClient).catch((error: unknown) => {
                if (!this.abortController.signal.aborted) {
                  logger.warn(error as Error, `Error closing dedicated client connection`);
                }
              });
            });
        }
      }
    }
  }

  getClientStreamKey(clientId: string) {
    return `${this.redisStreamKeyPrefix}client:${clientId}`;
  }

  getClientChainTipStreamGroupKey(clientId: string) {
    return `${this.redisStreamKeyPrefix}client_group:${clientId}`;
  }

  /**
   * Handles a client connection by backfilling messages from postgres first, then transitioning
   * to live-streaming from the chain tip stream once caught up. If the client falls behind during
   * live-streaming, it is demoted back to postgres backfilling until it catches up again.
   *
   * This design ensures:
   * 1. During backfilling, no consumer group blocks chain tip stream trimming
   * 2. Full historical sync is possible without being pruned for "lagging"
   * 3. Slow clients gracefully degrade to postgres backfill instead of being disconnected
   *
   * @param stacksClient - The Stacks client to use for the connection
   * @param startSequenceNumber - The resolved starting message sequence number
   * @param logger - The logger to use for logging
   */
  async streamMessagesToClient(
    stacksClient: StacksClient,
    startSequenceNumber: string,
    logger: typeof this.logger
  ) {
    await stacksClient.client.connect();
    const conn: StacksClientConnection = {
      client: stacksClient.client,
      appName: stacksClient.appName,
      clientId: stacksClient.clientId,
      selectedPaths: stacksClient.selectedPaths,
      clientStreamKey: this.getClientStreamKey(stacksClient.clientId),
      chainTipStreamGroupKey: this.getClientChainTipStreamGroupKey(stacksClient.clientId),
      chainTipStreamConsumerKey: `${this.redisStreamKeyPrefix}consumer:${stacksClient.clientId}`,
      lastStreamedSequenceNumber: startSequenceNumber,
    };

    // Outer loop: cycles between backfill (Phase 1) and live streaming (Phase 2).
    // A client can be demoted from live streaming back to backfill if it falls behind.
    while (!this.abortController.signal.aborted) {
      // ═══════════════════════════════════════════════════════════════════════════════
      // PHASE 1: Backfill from Postgres
      // ═══════════════════════════════════════════════════════════════════════════════
      // No consumer group is created during this phase, so the chain tip stream can be
      // trimmed freely without being blocked by this client's position.
      logger.info(
        { appName: stacksClient.appName, clientId: stacksClient.clientId },
        `Starting postgres backfill for client starting at sequence ${conn.lastStreamedSequenceNumber}`
      );
      const { globalConsumerGroupCreated } = await this.backfillClientStreamFromPostgres(
        conn,
        logger
      );

      // If consumer group not created yet (chain tip stream didn't exist or was empty during backfill),
      // create it now at '$' to start receiving new messages
      if (!globalConsumerGroupCreated) {
        logger.info(
          { appName: stacksClient.appName, clientId: stacksClient.clientId },
          `Postgres backfill complete. Creating consumer group at '$' (no overlap needed) for client starting at sequence ${conn.lastStreamedSequenceNumber}`
        );
        await this.createClientChainTipStreamConsumerGroup(conn);
        this.events.emit('consumerPromotedToLiveStream', { clientId: conn.clientId });
      }

      if (this._testHooks) {
        for (const cb of this._testHooks.onLiveStreamTransition) {
          await cb();
        }
      }

      // ═══════════════════════════════════════════════════════════════════════════════
      // PHASE 2: Live streaming from chain tip stream
      // ═══════════════════════════════════════════════════════════════════════════════
      // Read from the chain tip stream using the consumer group and write to the client's stream.
      // If the client falls behind (exceeds MAX_MSG_LAG), demote back to Phase 1.
      logger.info(
        { appName: stacksClient.appName, clientId: stacksClient.clientId },
        `Starting live streaming for client at msg ID ${conn.lastStreamedSequenceNumber}`
      );
      const { demotedToBackfill } = await this.feedClientStreamFromChainTipStream(conn, logger);
      // If we exited the live streaming loop without being demoted, we're shutting down
      if (!demotedToBackfill) {
        break;
      }
      logger.info(
        { appName: stacksClient.appName, clientId: stacksClient.clientId },
        `Client demoted to backfill mode, resuming from sequence ${conn.lastStreamedSequenceNumber}`
      );
    }
  }

  /**
   * Creates a consumer group for a client on the chain tip stream.
   * @param conn - The client connection to use for the consumer group.
   * @param startAtSequence - The sequence number to start the consumer group at. If
   * not provided, the consumer group will be created at '$'.
   */
  private async createClientChainTipStreamConsumerGroup(
    conn: StacksClientConnection,
    startAtSequence?: string
  ) {
    await conn.client
      .multi()
      .xGroupCreate(
        this.chainTipStreamKey,
        conn.chainTipStreamGroupKey,
        startAtSequence ? `${startAtSequence}-0` : '$',
        {
          MKSTREAM: true,
        }
      )
      .xGroupCreateConsumer(
        this.chainTipStreamKey,
        conn.chainTipStreamGroupKey,
        conn.chainTipStreamConsumerKey
      )
      .incr(this.chainTipStreamGroupVersionKey)
      .exec();
  }

  private async destroyClientChainTipStreamConsumerGroup(conn: StacksClientConnection) {
    await conn.client
      .xGroupDestroy(this.chainTipStreamKey, conn.chainTipStreamGroupKey)
      .catch((error: unknown) => {
        // Ignore NOGROUP errors - the group was already destroyed
        if (!(error as Error).message?.includes('NOGROUP')) {
          throw error;
        }
      });
  }

  /**
   * Backfills the client stream from postgres, starting from the given sequence number up until it
   * reaches the global live-stream.
   * @param conn - The client connection to use for the backfill.
   * @param logger - The logger to use for logging.
   * @returns Whether the consumer group was created.
   */
  private async backfillClientStreamFromPostgres(
    conn: StacksClientConnection,
    logger: typeof this.logger
  ): Promise<{ globalConsumerGroupCreated: boolean }> {
    let globalConsumerGroupCreated = false;

    while (!this.abortController.signal.aborted) {
      if (this._testHooks) {
        for (const cb of this._testHooks.onBeforePgBackfillQuery) {
          await cb(conn.lastStreamedSequenceNumber);
        }
      }

      const dbResults = await this.db
        .getMessageBatch({
          afterSequenceNumber: conn.lastStreamedSequenceNumber,
          selectedMessagePaths: conn.selectedPaths,
          batchSize: ENV.DB_MSG_BATCH_SIZE,
        })
        .catch((error: unknown) => {
          logger.error(error as Error, `Error querying messages from postgres during backfill`);
          throw error;
        });
      if (dbResults.length === 0) {
        logger.debug(
          { appName: conn.appName, clientId: conn.clientId },
          `Finished backfilling messages from postgres to client stream`
        );
        break;
      }

      conn.lastStreamedSequenceNumber = dbResults[dbResults.length - 1].sequence_number;
      logger.debug(
        { appName: conn.appName, clientId: conn.clientId },
        `Queried ${dbResults.length} messages from postgres (messages ${dbResults[0].sequence_number} to ${conn.lastStreamedSequenceNumber})`
      );

      // Write batch to client stream
      const multi = conn.client.multi();
      for (const row of dbResults) {
        multi.xAdd(conn.clientStreamKey, `${row.sequence_number}-0`, {
          timestamp: row.timestamp,
          path: row.path,
          body: JSON.stringify(row.content),
        });
      }
      await multi.exec();

      if (this._testHooks) {
        for (const cb of this._testHooks.onPgBackfillLoop) {
          await cb(dbResults[0].sequence_number);
        }
      }

      // Check if we've caught up with the chain tip stream (only if consumer group not yet created)
      if (!globalConsumerGroupCreated) {
        const chainTipStreamInfo = await conn.client
          .xInfoStream(this.chainTipStreamKey)
          .catch((error: unknown) => {
            if ((error as Error).message?.includes('ERR no such key')) {
              return null; // Chain tip stream doesn't exist yet
            }
            throw error;
          });

        if (chainTipStreamInfo) {
          const firstEntry = chainTipStreamInfo['first-entry'] as XInfoStreamEntry | undefined;
          const firstEntryId = firstEntry ? parseInt(firstEntry.id.split('-')[0]) : 0;

          if (parseInt(conn.lastStreamedSequenceNumber) >= firstEntryId) {
            // We've caught up with the chain tip stream - create consumer group now
            logger.debug(
              { appName: conn.appName, clientId: conn.clientId },
              `Backfill caught up with chain tip stream. lastBackfilled=${conn.lastStreamedSequenceNumber}, firstEntry=${firstEntryId}`
            );

            await this.createClientChainTipStreamConsumerGroup(
              conn,
              conn.lastStreamedSequenceNumber
            );
            globalConsumerGroupCreated = true;
            this.events.emit('consumerPromotedToLiveStream', {
              clientId: conn.clientId,
            });
            // Continue backfilling to exhaust postgres, but consumer group now captures new messages
          }
        }
      } else {
        // Consumer group already exists - advance its position as we continue backfilling.
        // This ensures we don't receive duplicate messages when transitioning to live streaming.
        await conn.client
          .xGroupSetId(
            this.chainTipStreamKey,
            conn.chainTipStreamGroupKey,
            `${conn.lastStreamedSequenceNumber}-0`
          )
          .catch((error: unknown) => {
            // NOGROUP error can happen if the group was destroyed (e.g., by pruning)
            if (!(error as Error).message?.includes('NOGROUP')) {
              logger.error(error as Error, `Error advancing consumer group position`);
              throw error;
            }
          });
      }

      // Backpressure handling to avoid overwhelming redis memory
      while (!this.abortController.signal.aborted) {
        const clientStreamLen = await conn.client.xLen(conn.clientStreamKey);
        if (clientStreamLen <= ENV.CLIENT_REDIS_STREAM_MAX_LEN) {
          break;
        } else {
          logger.debug(
            { appName: conn.appName, clientId: conn.clientId },
            `Client stream length is ${clientStreamLen}, waiting for backpressure to clear while backfilling...`
          );
          await timeout(ENV.CLIENT_REDIS_BACKPRESSURE_POLL_MS);
        }
      }
    }

    return { globalConsumerGroupCreated };
  }

  /**
   * Feeds the client stream with messages from the chain tip stream, starting from the given
   * sequence number.
   * @param conn - The client connection to use for the feeding.
   * @param logger - The logger to use for logging.
   * @returns Whether the client was demoted to backfill because it fell behind the chain tip
   * stream.
   */
  private async feedClientStreamFromChainTipStream(
    conn: StacksClientConnection,
    logger: typeof this.logger
  ): Promise<{ demotedToBackfill: boolean }> {
    let demotedToBackfill = false;

    while (!this.abortController.signal.aborted && !demotedToBackfill) {
      if (this._testHooks) {
        for (const cb of this._testHooks.onBeforeLivestreamXReadGroup) {
          await cb(conn.lastStreamedSequenceNumber);
        }
      }

      const messages = await conn.client.xReadGroup(
        conn.chainTipStreamGroupKey,
        conn.chainTipStreamConsumerKey,
        {
          key: this.chainTipStreamKey,
          id: '>',
        },
        {
          COUNT: ENV.LIVE_STREAM_BATCH_SIZE,
          BLOCK: 1000,
        }
      );

      if (messages) {
        for (const stream of messages as XReadGroupResponseEntry[]) {
          if (stream.messages.length > 0) {
            const lastReceivedMsgId = stream.messages[stream.messages.length - 1].id;
            logger.debug(
              { appName: conn.appName, clientId: conn.clientId },
              `Received messages ${stream.messages[0].id} - ${lastReceivedMsgId} from chain tip stream`
            );
            const multi = conn.client.multi();
            for (const { id, message } of stream.messages) {
              // Only forward messages that are in the client's selected paths
              if (
                conn.selectedPaths === '*' ||
                conn.selectedPaths.includes(message.path as MessagePath)
              ) {
                multi.xAdd(conn.clientStreamKey, id, message);
              }
            }
            // Acknowledge message for this consumer group
            multi.xAck(this.chainTipStreamKey, conn.chainTipStreamGroupKey, lastReceivedMsgId);
            await multi.exec();
            conn.lastStreamedSequenceNumber = lastReceivedMsgId.split('-')[0];
          }
        }

        // Backpressure handling
        while (!this.abortController.signal.aborted) {
          const clientStreamLen = await conn.client.xLen(conn.clientStreamKey);
          if (clientStreamLen <= ENV.CLIENT_REDIS_STREAM_MAX_LEN) {
            break;
          } else {
            logger.debug(
              { appName: conn.appName, clientId: conn.clientId },
              `Client stream length is ${clientStreamLen}, waiting for backpressure to clear while live-streaming...`
            );
            await timeout(ENV.CLIENT_REDIS_BACKPRESSURE_POLL_MS);
          }
        }
      } else {
        if (this._testHooks) {
          for (const cb of this._testHooks.onLiveStreamDrained) {
            await cb();
          }
        }
      }

      // Periodically check if we're falling behind the chain tip stream
      const chainTipStreamInfo = await conn.client
        .xInfoStream(this.chainTipStreamKey)
        .catch((error: unknown) => {
          if ((error as Error).message?.includes('ERR no such key')) {
            return null;
          }
          throw error;
        });

      if (chainTipStreamInfo) {
        const lastEntry = chainTipStreamInfo['last-entry'] as XInfoStreamEntry | undefined;
        const lastEntryId = lastEntry ? parseInt(lastEntry.id.split('-')[0]) : 0;
        const currentPos = parseInt(conn.lastStreamedSequenceNumber);
        const lag = lastEntryId - currentPos;

        if (lag > ENV.MAX_MSG_LAG) {
          logger.warn(
            { appName: conn.appName, clientId: conn.clientId },
            `Client falling behind chain tip stream (lag=${lag}, threshold=${ENV.MAX_MSG_LAG}), demoting to postgres backfill`
          );

          await this.destroyClientChainTipStreamConsumerGroup(conn);
          if (this._testHooks) {
            for (const cb of this._testHooks.onDemoteToBackfill) {
              await cb(conn.clientId);
            }
          }
          this.events.emit('consumerDemotedToBackfill', {
            clientId: conn.clientId,
          });
          demotedToBackfill = true;
          // Loop back to Phase 1 (backfill)
        }
      }
    }

    return { demotedToBackfill };
  }

  /**
   * Gets the starting message ID for a client by resolving the starting position to a sequence
   * number.
   * @param startingPosition - The starting position containing either lastIndexBlockHash or
   * lastMessageId
   * @param logger - The logger to use for logging
   * @returns The starting message ID
   */
  private async resolveClientStartMessageSequenceNumber(
    startingPosition: {
      lastIndexBlockHash: string;
      lastBlockHeight: string;
      lastMessageId: string;
    },
    logger: typeof this.logger
  ): Promise<string> {
    let sequenceNumber: string = '0';

    // Resolve the starting position to a sequence number for backfilling.
    // Priority: lastMessageId > lastIndexBlockHash > start from beginning
    if (startingPosition.lastMessageId) {
      // Validate the message ID exists and is not greater than the highest available
      const validationResult = await this.db.resolveMessageIdToStreamPosition(
        startingPosition.lastMessageId
      );
      if (validationResult) {
        sequenceNumber = validationResult.sequenceNumber;
        if (validationResult.clampedToMax) {
          logger.info(
            `Message ID ${startingPosition.lastMessageId} exceeds highest available, clamped to: ${sequenceNumber}-0`
          );
        } else {
          logger.info(`Using validated message ID: ${sequenceNumber}-0`);
        }
      } else {
        logger.warn(
          `Message ID ${startingPosition.lastMessageId} is invalid or database is empty, starting from beginning`
        );
      }
    } else if (startingPosition.lastIndexBlockHash) {
      // Resolve the index block hash to a sequence number
      const lastBlockHeight = startingPosition.lastBlockHeight
        ? parseInt(startingPosition.lastBlockHeight)
        : undefined;
      const resolutionResult = await this.db.resolveBlockIdentifierToStreamPosition(
        startingPosition.lastIndexBlockHash,
        lastBlockHeight
      );
      if (resolutionResult) {
        sequenceNumber = resolutionResult.sequenceNumber;
        if (resolutionResult.clampedToMax) {
          logger.info(
            `Block hash ${startingPosition.lastIndexBlockHash} not found but height ${lastBlockHeight} exceeds highest available, clamped to: ${sequenceNumber}-0`
          );
        } else {
          logger.info(
            `Resolved block hash ${startingPosition.lastIndexBlockHash} to sequence number ${sequenceNumber}-0`
          );
        }
      } else {
        logger.warn(
          `Unable to resolve index block hash ${startingPosition.lastIndexBlockHash} to message sequence number, starting from beginning`
        );
      }
    } else {
      logger.info('No starting position provided, starting from beginning');
    }

    return sequenceNumber;
  }

  /**
   * Cleanup old messages that are no longer needed by any consumers. This works by determining the
   * oldest message in the chain tip stream that has been acknowledged by all consumer groups and trims
   * the stream to that message.
   */
  async trimChainTipStream(): Promise<ChainTipStreamTrimResult> {
    // Use an optimistic redis transaction to trim the chain tip stream in order to prevent a race-condition
    // where a new consumer is added while we're trimming the stream. Otherwise that new consumer could miss
    // messages.
    // Create an isolated client for the WATCH transaction (required for atomic WATCH + MULTI/EXEC)
    const isolatedClient = this.client.duplicate();
    try {
      await isolatedClient.connect();

      // Watch the chain tip stream group version key to ensure that this trim operation is aborted if
      // any new consumer groups are added while we're trimming.
      await isolatedClient.watch(this.chainTipStreamGroupVersionKey);

      let groups: XInfoGroupsResponse;
      try {
        groups = await isolatedClient.xInfoGroups(this.chainTipStreamKey);
      } catch (error) {
        if ((error as Error).message?.includes('ERR no such key')) {
          // Chain tip stream doesn't exist yet so nothing to trim
          this.logger.info(`Trim performed when chain tip stream doesn't exist yet`);
          return { result: 'no_stream_exists' } as const;
        } else {
          throw error;
        }
      }

      if (this._testHooks) {
        for (const testFn of this._testHooks.onTrimChainTipStreamGetGroups) {
          await testFn();
        }
      }

      let minDeliveredId: number | null = null;
      for (const group of groups) {
        const lastDeliveredID = parseInt(String(group['last-delivered-id']).split('-')[0]);
        if (minDeliveredId === null || lastDeliveredID < minDeliveredId) {
          minDeliveredId = lastDeliveredID;
        }
      }

      if (minDeliveredId) {
        this.logger.info(`Trimming chain tip stream to min delivered ID ${minDeliveredId}`);
        // All entries that have an ID lower than minDeliveredId will be evicted
        await isolatedClient
          .multi()
          .xTrim(this.chainTipStreamKey, 'MINID', minDeliveredId, {
            strategyModifier: '=', // '~'
          })
          .exec();
        return { result: 'trimmed_minid', id: minDeliveredId } as const;
      } else {
        this.logger.info(`No groups are active, trimming chain tip stream to the last message`);
        // No groups are active, so we can trim the stream to 1, we keep the last message
        // so the ingestion code can still read the last message ID.
        await isolatedClient
          .multi()
          .xTrim(this.chainTipStreamKey, 'MAXLEN', 1, {
            strategyModifier: '=', // '~'
          })
          .exec();
        return { result: 'trimmed_maxlen' } as const;
      }
    } catch (error) {
      if (error instanceof WatchError) {
        // The transaction aborted because a consumer group was added while we were trimming
        this.logger.info(`Trimming aborted due to new group added`);
        return { result: 'aborted' } as const;
      } else {
        this.logger.error(error as Error, 'Error trimming chain tip stream');
        throw error;
      }
    } finally {
      destroyRedisClient(isolatedClient);
    }
  }

  /**
   * Clean up idle/offline clients. This function handles two scenarios:
   *
   * 1. **Idle consumers on the chain tip stream**: If a consumer hasn't interacted with Redis for
   *    MAX_IDLE_TIME_MS, it's considered dead/offline and is pruned. This frees up resources and
   *    allows the chain tip stream to be trimmed.
   *
   * 2. **Dangling client streams**: Client streams with no active consumers are cleaned up.
   *
   * Note: Slow clients (those falling behind on message consumption) are NOT pruned here.
   * Instead, they self-demote to postgres backfill mode via the streamMessages() function.
   * This allows for graceful degradation without losing the client's stream state.
   */
  async pruneIdleClients() {
    // Helper function to prune a client group and stream together.
    const prune = async (groupId: string, clientStreamKey: string): Promise<void> => {
      const clientId = clientStreamKey.split(':').at(-1) ?? '';
      await this.client
        .multi()
        .xGroupDestroy(this.chainTipStreamKey, groupId)
        .del(clientStreamKey)
        .exec()
        .catch((error: unknown) => {
          error = unwrapRedisMultiErrorReply(error as Error) ?? error;
          this.logger.warn(error, `Error while pruning idle client groups`);
        });
      this.events.emit('idleConsumerPruned', { clientId });
    };

    // Get the full stream info to check for idle consumers on the chain tip stream
    const fullStreamInfo = await xInfoStreamFull(this.client, this.chainTipStreamKey);

    // Part 1: Check for idle consumers on the chain tip stream
    for (const group of fullStreamInfo.groups) {
      if (group.consumers.length > 1) {
        this.logger.error(
          `Multiple consumers for chain tip stream group ${group.name}: ${group.consumers.length}`
        );
      }
      for (const consumer of group.consumers) {
        const idleMs = Date.now() - consumer.seenTime;
        const isIdle = idleMs > ENV.MAX_IDLE_TIME_MS;

        if (isIdle) {
          const clientId = consumer.name.split(':').at(-1) ?? '';
          const clientStreamKey = this.getClientStreamKey(clientId);
          this.logger.info(
            `Detected idle consumer group on chain tip stream, client: ${clientId}, idle ms: ${idleMs}`
          );
          await prune(group.name, clientStreamKey);
        }
      }
    }

    // Part 2: Check for dangling/idle client streams
    const clientStreamKeys = await this.client.keys(this.getClientStreamKey('*'));
    const clientStreamFullInfos = await xInfoStreamsFull(this.client, clientStreamKeys);

    for (const streamInfo of clientStreamFullInfos) {
      if (!streamInfo.response) {
        // Stream was deleted between fetching the list of keys and getting info
        continue;
      }

      const {
        stream: clientStreamKey,
        response: { groups },
      } = streamInfo;

      // Check for dangling streams with no consumers
      if (groups.length === 0 || groups[0].consumers.length === 0) {
        this.logger.warn(`Dangling client stream ${clientStreamKey}`);
        const clientId = clientStreamKey.split(':').at(-1) ?? '';
        const groupId = this.getClientChainTipStreamGroupKey(clientId);
        await prune(groupId, clientStreamKey);
        continue;
      }

      if (groups.length > 1) {
        this.logger.error(`Multiple groups for client stream ${clientStreamKey}: ${groups.length}`);
      }

      // Check for idle consumers on the client stream
      for (const group of groups) {
        if (group.consumers.length > 1) {
          this.logger.error(
            `Multiple consumers for client stream ${clientStreamKey}: ${group.consumers.length}`
          );
        }
        for (const consumer of group.consumers) {
          const idleMs = Date.now() - consumer.seenTime;

          if (idleMs > ENV.MAX_IDLE_TIME_MS) {
            const clientId = clientStreamKey.split(':').at(-1) ?? '';
            const groupId = this.getClientChainTipStreamGroupKey(clientId);

            this.logger.info(`Detected idle client stream ${clientId}, idle ms: ${idleMs}`);
            await prune(groupId, clientStreamKey);
          }
        }
      }
    }
  }
}
