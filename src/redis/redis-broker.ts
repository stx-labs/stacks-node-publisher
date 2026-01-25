import { ClientClosedError, createClient, WatchError } from 'redis';
import { logger as defaultLogger, stopwatch, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { PgStore } from '../pg/pg-store';
import { createTestHook, isTestEnv } from '../helpers';
import type { RedisClient, XInfoGroupsResponse } from './redis-types';
import { unwrapRedisMultiErrorReply, xInfoStreamFull, xInfoStreamsFull } from './redis-util';
import { EventEmitter } from 'node:events';
import { SelectedMessagePaths } from '../../client/src';
import { MessagePath } from '../../client/src/messages';

type StacksMessage = {
  timestamp: string;
  sequenceNumber: string;
  eventPath: string;
  eventBody: string;
};

export class RedisBroker {
  client: RedisClient;
  ingestionClient: RedisClient;
  listeningClient: RedisClient;
  perConsumerClients = new Map<RedisClient, { clientId: string }>();
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });

  readonly redisStreamKeyPrefix: string;
  readonly globalStreamKey: string;

  readonly abortController = new AbortController();
  readonly db: PgStore;

  readonly events = new EventEmitter<{
    idleConsumerPruned: [{ clientId: string }];
    laggingConsumerPruned: [{ clientId: string }];
    perConsumerClientCreated: [{ clientId: string }];
    ingestionMsgGapDetected: [];
    ingestionToEmptyRedisDb: [];
    redisClientsConnected: [];
  }>();

  _testHooks = isTestEnv
    ? {
        onLiveStreamTransition: createTestHook<() => Promise<void>>(),
        onLiveStreamDrained: createTestHook<() => Promise<void>>(),
        onTrimGlobalStreamGetGroups: createTestHook<() => Promise<void>>(),
        onPgBackfillLoop: createTestHook<(msgId: string) => Promise<void>>(),
        onAddStacksMsg: createTestHook<(msgId: string) => Promise<void>>(),
        onBeforePgBackfillQuery: createTestHook<(msgId: string) => Promise<void>>(),
        onBeforeLivestreamXReadGroup: createTestHook<(msgId: string) => Promise<void>>(),
      }
    : null;

  constructor(args: { redisUrl: string | undefined; redisStreamKeyPrefix: string; db: PgStore }) {
    this.db = args.db;
    this.redisStreamKeyPrefix = args.redisStreamKeyPrefix;
    if (this.redisStreamKeyPrefix !== '' && !this.redisStreamKeyPrefix.endsWith(':')) {
      this.redisStreamKeyPrefix += ':';
    }
    this.globalStreamKey = this.redisStreamKeyPrefix + 'global_stream';

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

  get globalStreamGroupVersionKey() {
    return `${this.globalStreamKey}:group_version`;
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
    await this.client.disconnect().catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing primary redis client connection');
      }
    });
    await this.ingestionClient.disconnect().catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing ingestion redis client connection');
      }
    });
    await this.listeningClient.disconnect().catch((error: unknown) => {
      if (!(error instanceof ClientClosedError)) {
        this.logger.debug(error, 'Error closing listening redis client connection');
      }
    });
    await Promise.all(
      [...this.perConsumerClients].map(([client]) =>
        client.disconnect().catch((error: unknown) => {
          if (!(error instanceof ClientClosedError)) {
            this.logger.debug(error, 'Error closing per-consumer redis client connection');
          }
        })
      )
    );
  }

  public async addStacksMessage(args: StacksMessage) {
    try {
      if (this._testHooks) {
        for (const cb of this._testHooks.onAddStacksMsg) {
          await cb(args.sequenceNumber);
        }
      }
      await this.handleMsg(args);
    } catch (error) {
      this.logger.error(error, 'Failed to add message to Redis');
    }
  }

  private async handleMsg(args: StacksMessage) {
    // Redis stream message IDs are <millisecondsTime>-<sequenceNumber>.
    // However, we don't fully trust our timestamp to always increase monotonically (e.g. NTP glitches),
    // so we'll just use the sequence number as the timestamp.
    const messageId = `${args.sequenceNumber}-0`;

    try {
      // If redis was unreachable when the last message(s) were added to the global stream,
      // then we need to make sure we don't create msg gaps in the stream. This can be done by
      // checking the last message ID in the stream and making sure it's exactly 1 less than the
      // sequence number of this new message.
      //
      // If there's a gap then we could either backfill from pg, which is tricky. It needs an
      // atomic switch from backfilling to live-streaming and backpressure handling. It could
      // also take a while to complete so unclear on what to do when subsequent messages come in.
      //
      // So instead, we DEL the global stream and all client streams, which triggers a re-initialization
      // of all clients so that they backfill the missing msgs from pg. This is simpler but could cause a
      // lot of churn if redis is frequently unreachable and/or unavailable for a long time.
      const streamInfo = await this.ingestionClient
        .xInfoStream(this.globalStreamKey)
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
      if (streamInfo && streamInfo.groups > 0 && streamInfo.lastEntry) {
        const lastEntryId = parseInt(streamInfo.lastEntry.id.split('-')[0]);
        if (lastEntryId + 1 < parseInt(args.sequenceNumber)) {
          this.events.emit('ingestionMsgGapDetected');
          this.logger.warn(
            `Detected gap in global stream, lastEntryId=${lastEntryId}, sequenceNumber=${args.sequenceNumber}`
          );
          // Delete the global stream and all client streams.
          const groups = await this.ingestionClient.xInfoGroups(this.globalStreamKey);
          const multi = this.ingestionClient.multi();
          multi.del(this.globalStreamKey);
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
      await this.ingestionClient.xAdd(this.globalStreamKey, messageId, globalRedisMsg);
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

    // TODO: these should be debounced or called on some interval or theshold, not on every message.
    // Consider:
    // - create a dedicated redis client used for pruning.
    // - run this on an configurable interval.
    // - keep track in nodejs memory of how many messages have been added since the last prune,
    //   and if that count exceeds a threshold then run the prune (faster than the interval).
    await this.trimGlobalStream();
    await this.pruneIdleClients();
  }

  async listenForConnections(listeningClient: RedisClient) {
    const connectionStreamKey = this.redisStreamKeyPrefix + 'connection_stream';
    while (!this.abortController.signal.aborted) {
      // TODO: if client.quit() is called, will throw an error? if so handle gracefully
      const connectionRequests = await listeningClient.xRead(
        { key: connectionStreamKey, id: '0-0' },
        {
          BLOCK: 1000, // wait for 1 second for new messages to allow abort signal to be checked
        }
      );
      if (!connectionRequests || connectionRequests.length === 0) {
        continue;
      }
      for (const request of connectionRequests) {
        for (const msg of request.messages) {
          const msgId = msg.id;
          const msgPayload = msg.message;

          // Delete the connection request messsage after receiving
          await listeningClient.xDel(connectionStreamKey, msgId);

          const clientId = msgPayload['client_id'];
          const lastIndexBlockHash = msgPayload['last_index_block_hash'] ?? '';
          const lastBlockHeight = msgPayload['last_block_height'] ?? '';
          const lastMessageId = msgPayload['last_message_id'] ?? '';
          const appName = msgPayload['app_name'];
          const selectedPaths: SelectedMessagePaths =
            msgPayload['selected_paths'] !== '*'
              ? (JSON.parse(msgPayload['selected_paths']) as MessagePath[])
              : '*';
          this.logger.info(
            msgPayload,
            `RedisBroker new client connection: ${clientId}, app: ${appName}`
          );

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
          const startSequenceNumber = await this.resolveStartMessageSequenceNumber(
            { lastIndexBlockHash, lastBlockHeight, lastMessageId },
            logger
          );
          // Handle the client connection by creating the consumer group and backfilling messages
          // from postgres.
          void this.handleClientConnection(
            dedicatedClient,
            clientId,
            startSequenceNumber,
            selectedPaths,
            logger
          )
            .catch(async (error: unknown) => {
              error = unwrapRedisMultiErrorReply(error as Error) ?? error;
              if ((error as Error).message?.includes('NOGROUP')) {
                logger.warn(error as Error, `Consumer group not found for client (likely pruned)`);
              } else if (!this.abortController.signal.aborted) {
                logger.error(error as Error, `Error processing msgs for consumer stream`);
              }
              const groupKey = this.getClientGlobalStreamGroupKey(clientId);
              const clientStreamKey = this.getClientStreamKey(clientId);
              await this.client
                .multi()
                // Destroy the global stream consumer group for this client
                .xGroupDestroy(this.globalStreamKey, groupKey)
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
              dedicatedClient.quit().catch((error: unknown) => {
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

  getClientGlobalStreamGroupKey(clientId: string) {
    return `${this.redisStreamKeyPrefix}client_group:${clientId}`;
  }

  /**
   * Handles a client connection by creating a consumer group for the global stream, backfilling
   * messages from postgres, and then starting to live-stream messages from the global stream to the
   * client's stream.
   * @param client - The redis client to use for the connection
   * @param clientId - The ID of the client
   * @param startSequenceNumber - The resolved starting message sequence number
   * @param selectedPaths - The message paths to fill in this stream
   * @param logger - The logger to use for logging
   */
  async handleClientConnection(
    client: typeof this.client,
    clientId: string,
    startSequenceNumber: string,
    selectedPaths: SelectedMessagePaths,
    logger: typeof this.logger
  ) {
    await client.connect();

    const clientStreamKey = this.getClientStreamKey(clientId);
    const groupKey = this.getClientGlobalStreamGroupKey(clientId);
    const consumerKey = `${this.redisStreamKeyPrefix}consumer:${clientId}`;

    let lastQueriedSequenceNumber = startSequenceNumber;

    // We need to create a unique redis stream for this client, then backfill it with messages
    // starting from the resolved sequence number. Backfilling is performed by reading messages from
    // postgres, then writing them to the client's redis stream.
    //
    // Once we've backfilled the stream, we can start streaming messages live from the global redis
    // stream to the client redis stream.
    //
    // This is a bit tricky because we need to do this atomically so that no messages are missed
    // during the switch from backfilling to live-streaming.

    // First, we create a consumer group for the global stream for this client. This ensures that
    // the global stream will not discard new messages that this client might after we've finished
    // backfilling from postgres. The special key `$` instructs redis to hold onto all new messages,
    // which could be added to the stream right after the postgres backfilling is complete, but
    // before we transition to live streaming.
    const msgId = '$';
    await client
      .multi()
      .xGroupCreate(this.globalStreamKey, groupKey, msgId, { MKSTREAM: true })
      .xGroupCreateConsumer(this.globalStreamKey, groupKey, consumerKey)
      .incr(this.globalStreamGroupVersionKey)
      .exec();
    logger.info(`Consumer group created on global stream for client`);

    // Next, we need to backfill the redis stream with messages from postgres. NOTE: do not perform
    // the backfilling within a sql transaction because it will use up a connection from the pool
    // for the duration of the backfilling, which could be a long time for large backfills.
    while (!this.abortController.signal.aborted) {
      if (this._testHooks) {
        for (const cb of this._testHooks.onBeforePgBackfillQuery) {
          await cb(lastQueriedSequenceNumber);
        }
      }
      // TODO: Should we catch ephemeral connection errors here and retry? otherwise an error here
      // will abort this consumer and the client will re-init and backfill again. Could use
      // `isPgConnectionError(err)` to check for retryable errors N amount of times before throwing
      // and giving up completely here.
      //
      // TODO: Move sql code to a readonly-only pg-store class, and consider making the interface
      // with the persisted-storage layer agnostic to whatever storage is used.
      const dbResults = await this.db.sql<
        { sequence_number: string; timestamp: string; path: string; content: string }[]
      >`
        SELECT
          sequence_number,
          (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS timestamp,
          path,
          content
        FROM messages
        WHERE sequence_number > ${lastQueriedSequenceNumber} ${
          selectedPaths === '*'
            ? this.db.sql``
            : this.db.sql`AND path IN ${this.db.sql(selectedPaths)}`
        }
        ORDER BY sequence_number ASC
        LIMIT ${ENV.DB_MSG_BATCH_SIZE}
      `.catch((error: unknown) => {
        logger.error(error as Error, `Error querying messages from postgres during backfill`);
        throw error;
      });

      if (dbResults.length > 0) {
        lastQueriedSequenceNumber = dbResults[dbResults.length - 1].sequence_number;
        logger.debug(
          `Queried ${dbResults.length} messages from postgres for client (messages ${dbResults[0].sequence_number} to ${lastQueriedSequenceNumber})`
        );
        // Update the global stream group to the last message ID so it's not holding onto messages
        // that are already backfilled, using XGROUP SETID.
        // TODO: this can throw NOGROUP error if the group is destroyed, handle that gracefully
        await client
          .xGroupSetId(this.globalStreamKey, groupKey, `${lastQueriedSequenceNumber}-0`)
          .catch((error: unknown) => {
            logger.error(error as Error, `Error setting last message ID for group`);
            throw error;
          });
      } else {
        logger.debug(`Finished backfilling messages from postgres to client stream`);
        break;
      }

      // xAdd all msgs at once.
      let multi = client.multi();
      for (const row of dbResults) {
        const messageId = `${row.sequence_number}-0`;
        const redisMsg = {
          timestamp: row.timestamp,
          path: row.path,
          body: row.content,
        };
        multi = multi.xAdd(clientStreamKey, messageId, redisMsg);
      }
      await multi.exec();

      if (dbResults.length > 0) {
        if (this._testHooks) {
          for (const cb of this._testHooks.onPgBackfillLoop) {
            // Only used by tests
            await cb(dbResults[0].sequence_number);
          }
        }
      }

      // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream length
      // is below a certain threshold before continuing.
      const timer = stopwatch();
      while (!this.abortController.signal.aborted) {
        const clientStreamLen = await client.xLen(clientStreamKey);
        if (clientStreamLen <= ENV.CLIENT_REDIS_STREAM_MAX_LEN) {
          break;
        } else {
          await timeout(ENV.CLIENT_REDIS_BACKPRESSURE_POLL_MS);
          if (timer.getElapsedSeconds() > 5) {
            logger.debug(
              `Client stream length is ${clientStreamLen}, waiting for backpressure to clear while backfilling...`
            );
            timer.restart();
          }
        }
      }
    }

    if (this._testHooks) {
      for (const cb of this._testHooks.onLiveStreamTransition) {
        // Only used by tests, performs xAdd on the global stream
        await cb();
      }
    }

    // Now we can start streaming live messages from the global redis stream to the client redis stream.
    // Read from the global stream using the consumer group we created above, and write to the client's stream.
    logger.info(`Starting live streaming for client at msg ID ${lastQueriedSequenceNumber}`);

    while (!this.abortController.signal.aborted) {
      if (this._testHooks) {
        for (const cb of this._testHooks.onBeforeLivestreamXReadGroup) {
          await cb(lastQueriedSequenceNumber);
        }
      }

      const messages = await client.xReadGroup(
        groupKey,
        consumerKey,
        {
          key: this.globalStreamKey,
          id: '>',
        },
        {
          COUNT: ENV.LIVE_STREAM_BATCH_SIZE,
          BLOCK: 1000,
        }
      );

      if (messages && messages.length > 0) {
        for (const stream of messages) {
          if (stream.messages.length > 0) {
            const lastReceivedMsgId = stream.messages[stream.messages.length - 1].id;
            logger.debug(
              `Received messages ${stream.messages[0].id} - ${lastReceivedMsgId} from global stream`
            );
            let multi = client.multi();
            for (const { id, message } of stream.messages) {
              // Filter messages based on message path
              if (selectedPaths === '*' || selectedPaths.includes(message.path as MessagePath)) {
                multi = multi.xAdd(clientStreamKey, id, message);
              }
            }
            // Acknowledge message for this consumer group
            multi = multi.xAck(this.globalStreamKey, groupKey, lastReceivedMsgId);
            await multi.exec();
            lastQueriedSequenceNumber = lastReceivedMsgId;
          }
        }

        // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream
        // length is below a certain threshold before continuing.
        const timer = stopwatch();
        while (!this.abortController.signal.aborted) {
          const clientStreamLen = await client.xLen(clientStreamKey);
          if (clientStreamLen <= ENV.CLIENT_REDIS_STREAM_MAX_LEN) {
            break;
          } else {
            await timeout(ENV.CLIENT_REDIS_BACKPRESSURE_POLL_MS);
            if (timer.getElapsedSeconds() > 5) {
              logger.debug(
                `Client stream length is ${clientStreamLen}, waiting for backpressure to clear while live-streaming...`
              );
              timer.restart();
            }
          }
        }
      } else {
        if (this._testHooks) {
          for (const cb of this._testHooks.onLiveStreamTransition) {
            // Only used by tests, performs xAdd on the global stream
            await cb();
          }
        }
      }
    }
  }

  /**
   * Gets the starting message ID for a client by resolving the starting position to a sequence
   * number.
   * @param startingPosition - The starting position containing either lastIndexBlockHash or
   * lastMessageId
   * @param logger - The logger to use for logging
   * @returns The starting message ID
   */
  private async resolveStartMessageSequenceNumber(
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
      const validationResult = await this.db.validateAndResolveMessageId(
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
      const resolutionResult = await this.db.resolveIndexBlockHashToSequenceNumber(
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
   * oldest message in the global stream that has been acknowledged by all consumer groups and trims
   * the stream to that message.
   */
  async trimGlobalStream() {
    try {
      // Use an optimistic redis transaction to trim the global stream in order to prevent a race-condition
      // where a new consumer is added while we're trimming the stream. Otherwise that new consumer could miss
      // messages.
      return await this.client.executeIsolated(async isolatedClient => {
        // Watch the global stream group version key to ensure that this trim operation is aborted if
        // any new consumer groups are added while we're trimming.
        await isolatedClient.watch(this.globalStreamGroupVersionKey);

        let groups: XInfoGroupsResponse;
        try {
          groups = await isolatedClient.xInfoGroups(this.globalStreamKey);
        } catch (error) {
          if ((error as Error).message?.includes('ERR no such key')) {
            // Global stream doesn't exist yet so nothing to trim
            this.logger.info(`Trim performed when global stream doesn't exist yet`);
            return { result: 'no_stream_exists' } as const;
          } else {
            throw error;
          }
        }

        if (this._testHooks) {
          for (const testFn of this._testHooks.onTrimGlobalStreamGetGroups) {
            await testFn();
          }
        }

        let minDeliveredId: number | null = null;
        for (const group of groups) {
          const lastDeliveredID = parseInt(group.lastDeliveredId.split('-')[0]);
          if (minDeliveredId === null || lastDeliveredID < minDeliveredId) {
            minDeliveredId = lastDeliveredID;
          }
        }

        if (minDeliveredId) {
          this.logger.info(`Trimming global stream to min delivered ID ${minDeliveredId}`);
          // All entries that have an ID lower than minDeliveredId will be evicted
          await isolatedClient
            .multi()
            .xTrim(this.globalStreamKey, 'MINID', minDeliveredId, {
              strategyModifier: '=', // '~'
            })
            .exec();
          return { result: 'trimmed_minid', id: minDeliveredId } as const;
        } else {
          this.logger.info(`No groups are active, trimming global stream to the last message`);
          // No groups are active, so we can trim the stream to 1, we keep the last message
          // so the ingestion code can still read the last message ID.
          await isolatedClient
            .multi()
            .xTrim(this.globalStreamKey, 'MAXLEN', 1, {
              strategyModifier: '=', // '~'
            })
            .exec();
          return { result: 'trimmed_maxlen' } as const;
        }
      });
    } catch (error) {
      if (error instanceof WatchError) {
        // The transaction aborted because a consumer group was added while we were trimming
        this.logger.info(`Trimming aborted due to new group added`);
        return { result: 'aborted' } as const;
      } else {
        this.logger.error(error as Error, 'Error trimming global stream');
        throw error;
      }
    }
  }

  /**
   * Clean up idle/offline clients. Detect slow clients which are not consuming fast enough to keep
   * up with the global stream, otherwise the global stream will continue to grow and OOM redis.
   */
  async pruneIdleClients() {
    const fullStreamInfo = await xInfoStreamFull(this.client, this.globalStreamKey);
    const lastEntryID = fullStreamInfo.lastGeneratedId
      ? parseInt(fullStreamInfo.lastGeneratedId.split('-')[0])
      : null;

    for (const group of fullStreamInfo.groups) {
      if (group.consumers.length > 1) {
        this.logger.error(
          `Multiple consumers for global stream group ${group.name}: ${group.consumers.length}`
        );
      }
      for (const consumer of group.consumers) {
        // TODO: this last ID comparison assumes consecutive integers for the message IDs, which may not always be the case.
        const msgsBehind = lastEntryID
          ? lastEntryID - parseInt(group.lastDeliveredId.split('-')[0])
          : 0;
        const idleMs = Date.now() - consumer.seenTime;
        const isIdle = idleMs > ENV.MAX_IDLE_TIME_MS;
        const isTooSlow = msgsBehind > ENV.MAX_MSG_LAG;
        if (isIdle || isTooSlow) {
          const clientId = consumer.name.split(':').at(-1) ?? '';
          const clientStreamKey = this.getClientStreamKey(clientId);
          this.logger.info(
            `Detected idle or slow consumer group, client: ${clientId}, idle ms: ${idleMs}, msgs behind: ${msgsBehind}`
          );
          await this.client
            .multi()
            // When the group is destroyed here, the live-streaming loop for this client is notified
            // via NOGROUP error on xReadGroup and exits.
            .xGroupDestroy(this.globalStreamKey, group.name)
            // Destroy the client stream, if there's still an online client then
            // they will be notified via NOGROUP error on xReadGroup and re-init.
            .del(clientStreamKey)
            .exec()
            .catch((error: unknown) => {
              error = unwrapRedisMultiErrorReply(error as Error) ?? error;
              this.logger.warn(error, `Error while pruning idle client groups`);
            });
          if (isIdle) {
            this.events.emit('idleConsumerPruned', { clientId });
          }
          if (isTooSlow) {
            this.events.emit('laggingConsumerPruned', { clientId });
          }
        }
      }
    }

    // Check for idle client streams and clean them up
    const clientStreamKeys = await this.client.keys(this.getClientStreamKey('*'));
    const clientStreamFullInfos = await xInfoStreamsFull(this.client, clientStreamKeys);

    for (const streamInfo of clientStreamFullInfos) {
      if (!streamInfo.response) {
        // no stream exists for this key (i.e. stream was deleted in between fetching the list of keys then info)
        continue;
      }

      const {
        stream: clientStreamKey,
        response: { groups },
      } = streamInfo;

      if (groups.length === 0 || groups[0].consumers.length === 0) {
        // Found a "dangling" client stream with no consumers, destroy the group and delete the stream
        this.logger.warn(`Dangling client stream ${clientStreamKey}`);
        await this.client.del(clientStreamKey).catch((error: unknown) => {
          error = unwrapRedisMultiErrorReply(error as Error) ?? error;
          this.logger.warn(error, `Error while pruning idle client stream`);
        });
      }
      if (groups.length > 1) {
        this.logger.error(`Multiple groups for client stream ${clientStreamKey}: ${groups.length}`);
      }
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
            const groupId = this.getClientGlobalStreamGroupKey(clientId);
            const globalStreamGroup = fullStreamInfo.groups.find(g => g.name === groupId);
            const globalStreamMsgsBehind =
              lastEntryID && globalStreamGroup
                ? lastEntryID - parseInt(globalStreamGroup.lastDeliveredId.split('-')[0])
                : 0;
            const clientStreamMsgsBehind = lastEntryID
              ? lastEntryID - parseInt(group.lastDeliveredId.split('-')[0])
              : 0;

            this.logger.info(
              `Detected idle client stream ${clientId}, idle ms: ${idleMs}, msgs behind: global=${globalStreamMsgsBehind}, client=${clientStreamMsgsBehind}`
            );
            await this.client
              .multi()
              // Destroy the global stream consumer group for this client
              .xGroupDestroy(this.globalStreamKey, groupId)
              // Destroy the client stream group and delete the client stream
              .del(clientStreamKey)
              .exec()
              .catch((error: unknown) => {
                error = unwrapRedisMultiErrorReply(error as Error) ?? error;
                this.logger.warn(error, `Error while pruning idle client stream`);
              });
            this.events.emit('idleConsumerPruned', { clientId });
          }
        }
      }
    }
  }
}
