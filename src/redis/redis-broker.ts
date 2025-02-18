import { createClient } from 'redis';
import { logger as defaultLogger, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { PgStore } from '../pg/pg-store';
import { sleep } from '../helpers';
import { XReadGroupReturnType } from './redis-types';

enum StreamType {
  ALL = 'all',
}

export class RedisBroker {
  private client: ReturnType<typeof createClient>;
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });

  readonly redisStreamKeyPrefix: string;
  readonly globalStreamKey: string;

  readonly abortController = new AbortController();
  readonly db: PgStore;

  testOnLiveStreamTransitionCbs = new Set<() => Promise<void>>();
  testRegisterOnLiveStreamTransition(cb: () => Promise<void>) {
    this.testOnLiveStreamTransitionCbs.add(cb);
    return { unregister: () => this.testOnLiveStreamTransitionCbs.delete(cb) };
  }

  testRegisterOnLiveStreamTransitionCbs = new Set<() => Promise<void>>();
  testOnLiveStreamDrained(cb: () => Promise<void>) {
    this.testRegisterOnLiveStreamTransitionCbs.add(cb);
    return { unregister: () => this.testOnLiveStreamTransitionCbs.delete(cb) };
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
  }

  async connect({ waitForReady }: { waitForReady: boolean }) {
    this.logger.info(`Using REDIS_STREAM_KEY_PREFIX: '${this.redisStreamKeyPrefix}'`);
    this.logger.info(`Connecting to Redis at ${ENV.REDIS_URL} ...`);
    // Note that the default redis client connect strategy is to retry indefinitely,
    // and so the `client.connect()` call should only actually throw from fatal errors like
    // a an invalid connection URL, but we'll add some retry logic here just in case.
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
    void this.listenForConnections().catch((error: unknown) => {
      this.logger.error(error as Error, 'Error listening for connections');
    });
  }

  async close() {
    this.abortController.abort();
    await this.client.quit();
  }

  async addStacksMessage(args: {
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
      const globalRedisMsg = {
        timestamp: args.timestamp,
        path: args.eventPath,
        body: args.eventBody,
      };
      await this.client.xAdd(this.globalStreamKey, messageId, globalRedisMsg);
    } catch (error) {
      this.logger.error(error as Error, 'Failed to add message to global Redis stream');
      throw error;
    }

    // TODO: this should be debounced or called on some interval or theshold, not on every message
    await this.trimGlobalStream();
  }

  async listenForConnections() {
    const listeningClient = this.client.duplicate();
    await listeningClient.connect();
    while (!this.abortController.signal.aborted) {
      // TODO: if client.quit() is called, will throw an error? if so handle gracefully
      const connectionStreamKey = this.redisStreamKeyPrefix + 'connection_stream';
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
        const connectionName = connection.name;
        for (const msg of connection.messages) {
          const msgId = msg.id;
          const msgPayload = msg.message;
          console.log(`New message in ${connectionName} - ${msgId}: ${JSON.stringify(msgPayload)}`);

          // Delete the connection request messsage after receiving
          await listeningClient.xDel(connectionStreamKey, msgId);

          const clientId = msgPayload['client_id'];
          const lastMessageId = msgPayload['last_message_id'];

          // Fire-and-forget promise so multiple clients can connect and backfill at once
          void this.handleClientConnection(clientId, lastMessageId).catch((error: unknown) => {
            this.logger.error(error as Error, `Error handling client connection for ${clientId}`);
          });
        }
      }
    }
  }

  async handleClientConnection(clientId: string, lastMessageId: string) {
    const client = this.client.duplicate();
    await client.connect();

    const clientStreamKey = `${this.redisStreamKeyPrefix}${StreamType.ALL}:${clientId}`;
    const groupKey = `${this.redisStreamKeyPrefix}client_group:${clientId}`;
    const consumerKey = `${this.redisStreamKeyPrefix}consumer_${clientId}`;

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
    // stream will not discard new messages for this client while we're backfilling from postgres.
    // The special key `$` instructs redis to hold onto all new messages, which could be added to the stream
    // right after the postgres backfilling is complete, but before we transition to live streaming.
    const msgId = '$';
    await client.xGroupCreate(this.globalStreamKey, groupKey, msgId, { MKSTREAM: true });
    await client.xGroupCreateConsumer(this.globalStreamKey, groupKey, consumerKey);
    this.logger.info(`Consumer group ${groupKey} created for client ${clientId}.`);

    // Next, we need to backfill the redis stream with messages from postgres.
    // NOTE: do not perform the backfilling within a sql transaction because it will use up a connection
    // from the pool for the duration of the backfilling, which could be a long time for large backfills.
    let lastMsgId = '0-0';
    let lastQueriedSequenceNumber = lastMessageId;
    while (true) {
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
        LIMIT 100
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
      await client.xGroupSetId(this.globalStreamKey, groupKey, lastMsgId);

      // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream length
      // is below a certain threshold before continuing.
      while ((await client.xLen(clientStreamKey)) > 100) {
        await sleep(100);
      }
    }

    for (const cb of this.testOnLiveStreamTransitionCbs) {
      // Only used by tests, performs xAdd on the global stream
      await cb();
    }

    // Now we can start streaming live messages from the global redis stream to the client redis stream.
    // Read from the global stream using the consumer group we created above, and write to the client's stream.
    this.logger.debug(`Starting live streaming for client ${clientId} from ID ${lastMsgId}`);

    while (true) {
      let messages: XReadGroupReturnType;
      try {
        messages = await client.xReadGroup(
          groupKey,
          consumerKey,
          {
            key: this.globalStreamKey,
            id: '>',
          },
          { BLOCK: 1000, COUNT: 100 }
        );
      } catch (error) {
        if ((error as Error).message.includes('NOGROUP')) {
          this.logger.warn(error as Error, `Consumer group not found for client ${clientId}`);
          // Destory the global stream consumer group for this client
          await client.xGroupDestroy(this.globalStreamKey, groupKey);
          // Destory the redis stream for this client
          await client.del(clientStreamKey);
          // TODO: the client is using xRead and will not be notified when this stream is deleted,
          // the client stream should use a consumer group so that we can perform xGroupDestroy
          // and the client will see the NOGROUP error from using XREADGROUP and can handle it gracefully.
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

            // TODO: add backpressure to avoid overwhelming redis memory, e.g. wait until the stream length
            // is below a certain threshold before continuing

            // Process message (send to client)
            await client.xAdd(clientStreamKey, id, message);

            // Acknowledge message for this consumer group
            await client.xAck(this.globalStreamKey, groupKey, id);

            lastMsgId = id;
          }
        }

        // Backpressure handling to avoid overwhelming redis memory. Wait until the client stream length
        // is below a certain threshold before continuing.
        while ((await client.xLen(clientStreamKey)) > 100) {
          await sleep(100);
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
      this.logger.info(`No groups are active, trimming global stream to 0`);
      // No groups are active, so we can trim the stream to 0
      await this.client.xTrim(this.globalStreamKey, 'MAXLEN', 0, {
        strategyModifier: '=', // '~'
      });
    }
    const endLen = await this.client.xLen(this.globalStreamKey);
    this.logger.info(`Trimmed global stream from length ${startLen} to ${endLen}`);
  }

  // TODO: this should be called on a schedule
  async pruneIdleClients() {
    let MAX_IDLE_TIME = 60_000; // 1 minute
    const debugging = true;
    if (debugging) {
      MAX_IDLE_TIME = 2_000; // 2 seconds
    }
    const groups = await this.client.xInfoGroups(this.globalStreamKey);
    const groupConsumers = await Promise.all(
      groups.map(group => {
        return this.client
          .xInfoConsumers(this.globalStreamKey, group.name)
          .then(consumers => ({ group, consumers }));
      })
    );
    for (const { group, consumers } of groupConsumers) {
      if (consumers.length > 1) {
        this.logger.error(`Multiple consumers for group ${group.name}: ${consumers.length}`);
      }
      for (const consumer of consumers) {
        if (consumer.idle > MAX_IDLE_TIME) {
          this.logger.info(`Pruning idle consumer group ${group.name}`);
          // TODO: when the group is destroyed here, the live-streaming loop needs to be aware
          // and stop reading from the global stream and let the client know that the connection is closed
          await this.client.xGroupDestroy(this.globalStreamKey, group.name);
        }
      }
    }
    // TODO: should also check for clients that aren't idle but are very slow and causing backpressure
    // issues on the global stream. They should be terminated and when they reconnect they will enter
    // the backfilling phase again.
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
