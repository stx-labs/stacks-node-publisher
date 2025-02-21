import { createClient } from 'redis';
import { logger as defaultLogger, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { PgStore } from '../pg/pg-store';
import { sleep } from '../helpers';
import { XInfoConsumersResponse, XInfoGroupsResponse, XReadGroupResponse } from './redis-types';

enum StreamType {
  ALL = 'all',
}

export class RedisBroker {
  client: ReturnType<typeof createClient>;
  ingestionClient: ReturnType<typeof createClient>;
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });

  readonly redisStreamKeyPrefix: string;
  readonly globalStreamKey: string;

  readonly abortController = new AbortController();
  readonly db: PgStore;

  private readonly CLIENT_GROUP_NAME = 'primary_group';

  testOnLiveStreamTransitionCbs = new Set<() => Promise<void>>();
  testRegisterOnLiveStreamTransition(cb: () => Promise<void>) {
    this.testOnLiveStreamTransitionCbs.add(cb);
    return { unregister: () => this.testOnLiveStreamTransitionCbs.delete(cb) };
  }

  testRegisterOnLiveStreamTransitionCbs = new Set<() => Promise<void>>();
  testOnLiveStreamDrained(cb: () => Promise<void>) {
    this.testRegisterOnLiveStreamTransitionCbs.add(cb);
    return { unregister: () => this.testRegisterOnLiveStreamTransitionCbs.delete(cb) };
  }

  constructor(args: { redisUrl: string | undefined; redisStreamKeyPrefix: string; db: PgStore }) {
    this.db = args.db;
    this.client = createClient({
      url: args.redisUrl,
      name: 'salt-n-pepper-server',
    });
    this.redisStreamKeyPrefix = args.redisStreamKeyPrefix;
    this.globalStreamKey = args.redisStreamKeyPrefix + 'global_stream';

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) => this.logger.error(err, 'Redis error'));
    this.client.on('reconnecting', () => this.logger.info('Reconnecting to Redis'));
    this.client.on('ready', () => this.logger.info('Redis connection ready'));

    // Create a separate client for ingestion that doesn't use the offline queue because
    // we implement logic for this ourselves in a way that avoid backpressure issues.
    this.ingestionClient = this.client.duplicate({
      disableOfflineQueue: true,
      name: 'snp-ingestion',
    });
    this.ingestionClient.on('error', (err: Error) =>
      this.logger.error(err, 'Redis error on ingestion client')
    );
  }

  async connect({ waitForReady }: { waitForReady: boolean }) {
    this.logger.info(`Using REDIS_STREAM_KEY_PREFIX: '${this.redisStreamKeyPrefix}'`);
    this.logger.info(`Connecting to Redis at ${ENV.REDIS_URL} ...`);
    // Note that the default redis client connect strategy is to retry indefinitely,
    // and so the `client.connect()` call should only actually throw from fatal errors like
    // a an invalid connection URL.
    const connectClients = async () => {
      try {
        await this.client.connect();
        await this.ingestionClient.connect();
        const primaryClientID = await this.client.clientId();
        const ingestionClientID = await this.ingestionClient.clientId();
        this.logger.info(
          `Connected to Redis, client ID: ${primaryClientID}, ingestion client ID: ${ingestionClientID}`
        );
      } catch (err) {
        this.logger.error(err as Error, 'Fatal error connecting to Redis');
        throw err;
      }
    };

    if (waitForReady) {
      await connectClients();
    } else {
      void connectClients().catch((err: unknown) => {
        this.logger.error(err as Error, 'Fatal error connecting to Redis');
        process.exit(1);
      });
    }

    const listeningClient = this.client.duplicate();
    listeningClient.on('error', (err: Error) =>
      this.logger.error(err, 'Redis error on client connection listener')
    );

    const connectionListener = async () => {
      await listeningClient.connect();
      while (!this.abortController.signal.aborted) {
        try {
          await this.listenForConnections(listeningClient);
        } catch (error) {
          this.logger.error(error as Error, 'Error listening for connections');
          await sleep(1000);
        }
      }
      // Close the listening client connection after handling the connections
      listeningClient.quit().catch((error: unknown) => {
        this.logger.error(error as Error, 'Error closing listening client connection');
      });
    };
    void connectionListener();
  }

  async close() {
    this.abortController.abort();
    await this.client.quit();
  }

  public async addStacksMessage(args: {
    timestamp: string;
    sequenceNumber: string;
    eventPath: string;
    eventBody: string;
  }) {
    try {
      await this.handleMsg(args);
    } catch (error) {
      this.logger.error(error, 'Failed to add message to Redis');
    }
  }

  private async handleMsg(args: {
    timestamp: string;
    sequenceNumber: string;
    eventPath: string;
    eventBody: string;
  }) {
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

      // If there are groups (consumers) on the stream and the stream isn't new/empty, then check for gaps.
      if (streamInfo && streamInfo.groups > 0 && streamInfo.lastEntry) {
        const lastEntryId = parseInt(streamInfo.lastEntry.id.split('-')[0]);
        if (lastEntryId + 1 < parseInt(args.sequenceNumber)) {
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

    // TODO: this should be debounced or called on some interval or theshold, not on every message
    await this.trimGlobalStream();
    // TODO: this should be debounced or called on some interval or theshold, not on every message
    await this.pruneIdleClients();
  }

  async listenForConnections(listeningClient: typeof this.client) {
    const connectionStreamKey = this.redisStreamKeyPrefix + 'connection_stream';
    while (!this.abortController.signal.aborted) {
      // TODO: if client.quit() is called, will throw an error? if so handle gracefully
      const connections = await listeningClient.xRead(
        { key: connectionStreamKey, id: '0-0' },
        {
          BLOCK: 1000, // wait for 1 second for new messages to allow abort signal to be checked
        }
      );
      if (!connections || connections.length === 0) {
        continue;
      }
      for (const connection of connections) {
        for (const msg of connection.messages) {
          const msgId = msg.id;
          const msgPayload = msg.message;

          // Delete the connection request messsage after receiving
          await listeningClient.xDel(connectionStreamKey, msgId);

          const clientId = msgPayload['client_id'];
          const lastMessageId = msgPayload['last_message_id'];
          this.logger.info(`New client connection: ${clientId}, lastMessageId: ${lastMessageId})`);

          // Fire-and-forget promise so multiple clients can connect and backfill and live-stream at once
          const dedicatedClient = this.client.duplicate();
          dedicatedClient.on('error', (err: Error) => {
            this.logger.error(
              err,
              `Redis error on dedicated client connection for client ${clientId}`
            );
          });
          void this.handleClientConnection(dedicatedClient, clientId, lastMessageId)
            .catch((error: unknown) => {
              this.logger.error(error as Error, `Error handling client connection for ${clientId}`);
            })
            .finally(() => {
              // Close the dedicated client connection after handling the client
              dedicatedClient.quit().catch((error: unknown) => {
                this.logger.error(
                  error as Error,
                  `Error closing dedicated client connection for ${clientId}`
                );
              });
            });
        }
      }
    }
  }

  getClientStreamKey(clientId: string) {
    return `${this.redisStreamKeyPrefix}client:${StreamType.ALL}:${clientId}`;
  }

  getClientGlobalStreamGroupKey(clientId: string) {
    return `${this.redisStreamKeyPrefix}client_group:${clientId}`;
  }

  async handleClientConnection(
    client: typeof this.client,
    clientId: string,
    lastMessageId: string
  ) {
    await client.connect();

    const DB_MSG_BATCH_SIZE = 100;
    const LIVE_STREAM_BATCH_SIZE = 100;
    const CLIENT_REDIS_STREAM_MAX_LEN = 100;
    const CLIENT_REDIS_BACKPRESSURE_POLL_MS = 100;

    const clientStreamKey = this.getClientStreamKey(clientId);
    const groupKey = this.getClientGlobalStreamGroupKey(clientId);
    const consumerKey = `${this.redisStreamKeyPrefix}consumer:${clientId}`;

    // We need to create a unique redis stream for this client, then backfill it with messages starting
    // from the lastMessageId provided by the client. Backfilling is performed by reading messages from
    // postgres, then writing them to the client's redis stream.
    //
    // Once we've backfilled the stream, we can start streaming messages live from the global redis
    // stream to the client redis stream.
    //
    // This is a bit tricky because we need to do this atomically so that no messages are missed during the
    // switch from backfilling to live-streaming.

    // First, we create a consumer group for the global stream for this client. This ensures that the global
    // stream will not discard new messages that this client might after we've finished backfilling from postgres.
    // The special key `$` instructs redis to hold onto all new messages, which could be added to the stream
    // right after the postgres backfilling is complete, but before we transition to live streaming.
    const msgId = '$';
    await this.client
      .multi()
      .xGroupCreate(this.globalStreamKey, groupKey, msgId, { MKSTREAM: true })
      .xGroupCreateConsumer(this.globalStreamKey, groupKey, consumerKey)
      .exec();
    this.logger.info(`Consumer group ${groupKey} created for client ${clientId}.`);

    // Next, we need to backfill the redis stream with messages from postgres.
    // NOTE: do not perform the backfilling within a sql transaction because it will use up a connection
    // from the pool for the duration of the backfilling, which could be a long time for large backfills.
    let lastMsgId = '0-0';
    let lastQueriedSequenceNumber = lastMessageId.split('-')[0];
    while (!this.abortController.signal.aborted) {
      const dbResults = await this.db.sql<
        { sequence_number: string; timestamp: string; path: string; content: string }[]
      >`
        SELECT
          sequence_number,
          (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS timestamp,
          path,
          content
        FROM messages
        WHERE sequence_number > ${lastQueriedSequenceNumber}
        ORDER BY sequence_number ASC
        LIMIT ${DB_MSG_BATCH_SIZE}
      `;
      const msgsQueried = dbResults.length;

      if (msgsQueried > 0) {
        lastQueriedSequenceNumber = dbResults[dbResults.length - 1].sequence_number;
        this.logger.debug(
          `Queried ${msgsQueried} messages from postgres for client ${clientId} (messages ${dbResults[0].sequence_number} to ${lastQueriedSequenceNumber})`
        );
      } else {
        this.logger.debug(`Finished backfilling messages from postgres for client ${clientId}`);
        break;
      }

      for (const row of dbResults) {
        // TODO: this can be optimized with redis pipelining:
        // https://github.com/redis/node-redis/tree/master/packages/redis#auto-pipelining
        const messageId = `${row.sequence_number}-0`;
        const redisMsg = {
          timestamp: row.timestamp,
          path: row.path,
          body: row.content,
        };
        await client.xAdd(clientStreamKey, messageId, redisMsg);
        lastMsgId = messageId;
      }

      // Update the global stream group to the last message ID so it's not holding onto messages
      // that are already backfilled, using XGROUP SETID.
      // TODO: this can throw NOGROUP error if the group is destroyed, handle that gracefully
      await client
        .xGroupSetId(this.globalStreamKey, groupKey, lastMsgId)
        .catch((error: unknown) => {
          console.error(error);
          throw error;
        });

      // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream length
      // is below a certain threshold before continuing.
      while (
        !this.abortController.signal.aborted &&
        (await client.xLen(clientStreamKey)) > CLIENT_REDIS_STREAM_MAX_LEN
      ) {
        await sleep(CLIENT_REDIS_BACKPRESSURE_POLL_MS);
      }
    }

    for (const cb of this.testOnLiveStreamTransitionCbs) {
      // Only used by tests, performs xAdd on the global stream
      await cb();
    }

    // Now we can start streaming live messages from the global redis stream to the client redis stream.
    // Read from the global stream using the consumer group we created above, and write to the client's stream.
    this.logger.debug(`Starting live streaming for client ${clientId} from ID ${lastMsgId}`);

    while (!this.abortController.signal.aborted) {
      let messages: XReadGroupResponse;
      try {
        messages = await client.xReadGroup(
          groupKey,
          consumerKey,
          {
            key: this.globalStreamKey,
            id: '>',
          },
          {
            COUNT: LIVE_STREAM_BATCH_SIZE,
            BLOCK: 1000,
          }
        );
      } catch (error) {
        if ((error as Error).message.includes('NOGROUP')) {
          this.logger.warn(error as Error, `Consumer group not found for client ${clientId}`);
          await client
            .multi()
            // Destroy the global stream consumer group for this client
            .xGroupDestroy(this.globalStreamKey, groupKey)
            // Destroy the stream for this client (notifies the client via NOGROUP error on xReadGroup)
            .del(clientStreamKey)
            .exec();
          break;
        } else {
          this.logger.error(
            error as Error,
            `Error reading from global stream for client ${clientId}`
          );
          break;
        }
      }

      if (messages) {
        // TODO: this can be optimized with redis pipelining:
        // https://github.com/redis/node-redis/tree/master/packages/redis#auto-pipelining
        for (const stream of messages) {
          for (const msg of stream.messages) {
            const { id, message } = msg;

            this.logger.debug(`Received message ${id} from global stream for client ${clientId}`);

            // TODO: pipeline these xAdd and xAck calls

            // Process message (send to client)
            await client.xAdd(clientStreamKey, id, message);

            // Acknowledge message for this consumer group
            await client.xAck(this.globalStreamKey, groupKey, id);

            lastMsgId = id;
          }
        }

        // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream length
        // is below a certain threshold before continuing.
        while (
          !this.abortController.signal.aborted &&
          (await client.xLen(clientStreamKey)) > CLIENT_REDIS_STREAM_MAX_LEN
        ) {
          await sleep(CLIENT_REDIS_BACKPRESSURE_POLL_MS);
        }
      } else {
        for (const cb of this.testRegisterOnLiveStreamTransitionCbs) {
          // Only used by tests, performs xAdd on the global stream
          await cb();
        }
      }
    }
  }

  // Cleanup old message that are no longer needed by any consumers. This works by determining the oldest
  // message in the global stream that has been acknowledged by all consumer groups and trims the stream
  // to that message.
  async trimGlobalStream() {
    const startLen = await this.client.xLen(this.globalStreamKey);
    const groups = await this.client.xInfoGroups(this.globalStreamKey);
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
      await this.client.xTrim(this.globalStreamKey, 'MINID', minDeliveredId, {
        strategyModifier: '=', // '~'
      });
    } else {
      this.logger.info(`No groups are active, trimming global stream to the last message`);
      // No groups are active, so we can trim the stream to 1, we keep the last message
      // so the ingestion code can still read the last message ID.
      await this.client.xTrim(this.globalStreamKey, 'MAXLEN', 1, {
        strategyModifier: '=', // '~'
      });
    }
    const endLen = await this.client.xLen(this.globalStreamKey);
    this.logger.info(`Trimmed global stream from length ${startLen} to ${endLen}`);
  }

  async pruneIdleClients() {
    // Clean up idle/offline clients. Detect slow clients which are not consuming fast enough to keep
    // up with the global stream, otherwise the global stream will continue to grow and OOM redis.
    let MAX_IDLE_TIME = 60_000; // 1 minute
    let MAX_MSG_LAG = 200;

    const debugging = true;
    if (debugging) {
      MAX_IDLE_TIME = 2_000; // 2 seconds
      MAX_MSG_LAG = 102;
    }

    // TODO: this can use `XINFO STREAM key [FULL [COUNT 1]]` to also get the groups and consumers atomicly in
    // a single call. It's not supported in the redis library yet, so it needs manual parsing.

    const globalStreamInfo = await this.client.xInfoStream(this.globalStreamKey);
    const lastEntryID = globalStreamInfo.lastEntry?.id
      ? parseInt(globalStreamInfo.lastEntry.id.split('-')[0])
      : null;
    const globalGroups = await this.client.xInfoGroups(this.globalStreamKey);
    const globalConsumers = await Promise.all(
      globalGroups.map(group => this.client.xInfoConsumers(this.globalStreamKey, group.name))
    );
    const globalClients = globalGroups.map((group, i) => ({
      group,
      consumers: globalConsumers[i],
    }));
    for (const { group, consumers } of globalClients) {
      if (consumers.length > 1) {
        this.logger.error(
          `Multiple consumers for global stream group ${group.name}: ${consumers.length}`
        );
      }
      for (const consumer of consumers) {
        const msgsBehind = lastEntryID
          ? lastEntryID - parseInt(group.lastDeliveredId.split('-')[0])
          : 0;
        const isIdle = consumer.idle > MAX_IDLE_TIME;
        const isTooSlow = msgsBehind > MAX_MSG_LAG;
        if (isIdle || isTooSlow) {
          const clientId = consumer.name.split(':').at(-1) ?? '';
          const clientStreamKey = this.getClientStreamKey(clientId);
          this.logger.info(
            `Detected idle or slow consumer group, client: ${clientId}, idle ms: ${consumer.idle}, msgs behind: ${msgsBehind}`
          );
          await this.client
            .multi()
            // When the group is destroyed here, the live-streaming loop for this client is notified
            // via NOGROUP error on xReadGroup and exits.
            .xGroupDestroy(this.globalStreamKey, group.name)
            // Destroy the client stream, if there's still an online client then
            // they will be notified via NOGROUP error on xReadGroup and re-init.
            .del(clientStreamKey)
            .exec();
        }
      }
    }

    // Check for idle client streams and clean them up
    const clientStreamKeys = await this.client.keys(this.getClientStreamKey('*'));
    // TODO: this can use `XINFO STREAM key [FULL [COUNT 1]]` to also get the groups and consumers atomicly in
    // a single call. It's not supported in the redis library yet, so it needs manual parsing.
    const clientStreamResponse = await Promise.all(
      clientStreamKeys.flatMap(key => [
        this.client.xInfoConsumers(key, this.CLIENT_GROUP_NAME).catch((error: unknown) => {
          if (
            (error as Error).message?.includes('ERR no such key') ||
            (error as Error).message?.includes('NOGROUP')
          ) {
            return [];
          } else {
            this.logger.error(error as Error, `Error performing xInfoConsumers for ${key}`);
            throw error;
          }
        }),
        this.client.xInfoGroups(key).catch((error: unknown) => {
          if ((error as Error).message?.includes('ERR no such key')) {
            return [];
          } else {
            this.logger.error(error as Error, `Error performing xInfoGroups for ${key}`);
            throw error;
          }
        }),
      ])
    );
    const clientStreamInfo = clientStreamKeys.map((clientStreamKey, i) => ({
      clientStreamKey,
      consumers: clientStreamResponse[i * 2] as XInfoConsumersResponse,
      groups: clientStreamResponse[i * 2 + 1] as XInfoGroupsResponse,
    }));
    for (const { clientStreamKey, consumers, groups } of clientStreamInfo) {
      if (consumers.length === 0 || groups.length === 0) {
        // Found a "dangling" client stream with no consumers, destroy the group and delete the stream
        this.logger.warn(`Dangling client stream ${clientStreamKey}`);
        await this.client.del(clientStreamKey);
      }
      if (consumers.length > 1) {
        this.logger.error(
          `Multiple consumers for client stream ${clientStreamKey}: ${consumers.length}`
        );
      }
      if (groups.length > 1) {
        this.logger.error(`Multiple groups for client stream ${clientStreamKey}: ${groups.length}`);
      }
      for (const consumer of consumers) {
        if (consumer.idle > MAX_IDLE_TIME) {
          const clientId = clientStreamKey.split(':').at(-1) ?? '';
          const groupId = this.getClientGlobalStreamGroupKey(clientId);
          const globalStreamGroup = globalGroups.find(g => g.name === groupId);
          const globalStreamMsgsBehind =
            lastEntryID && globalStreamGroup
              ? lastEntryID - parseInt(globalStreamGroup.lastDeliveredId.split('-')[0])
              : 0;
          const clientStreamMsgsBehind = lastEntryID
            ? lastEntryID - parseInt(groups[0].lastDeliveredId.split('-')[0])
            : 0;

          this.logger.info(
            `Detected idle client stream ${clientId}, idle ms: ${consumer.idle}, msgs behind: global=${globalStreamMsgsBehind}, client=${clientStreamMsgsBehind}`
          );
          await this.client
            .multi()
            // Destroy the global stream consumer group for this client
            .xGroupDestroy(this.globalStreamKey, groupId)
            // Destroy the client stream group and delete the client stream
            .del(clientStreamKey)
            .exec();
        }
      }
    }
  }

  async getGroupMetadata(client: typeof this.client, groupKey: string) {
    const xlen = await client.xLen(this.globalStreamKey);
    const groupsTest1 = await client.xInfoGroups(this.globalStreamKey);
    const consumersTest1 = await Promise.all(
      groupsTest1.map(group => client.xPending(this.globalStreamKey, group.name))
    );
    const consumerTest2 = await client.xPending(this.globalStreamKey, groupKey);
    const consumerTest3 = await client.xInfoConsumers(this.globalStreamKey, groupKey);
    const res = { xlen, groupsTest1, consumersTest1, consumerTest2, consumerTest3 };
    console.log(JSON.stringify(res, null, 2));
    return res;
  }
}
