import { createClient } from 'redis';
import { logger as defaultLogger, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { PgStore } from '../pg/pg-store';

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
    await this.listenForConnections();
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

    /*
    try {
      const redisMsg = {
        timestamp: args.timestamp,
        path: args.eventPath,
        body: args.eventBody,
      };
      const streamKey = this.redisStreamKeyPrefix + 'all';
      await this.addMessage(streamKey, messageId, redisMsg);
    } catch (error) {
      this.logger.error(error as Error, 'Failed to add message to Redis');
      throw error;
    }
    */
  }

  async addMessage(streamKey: string, messageId: string, message: Record<string, string | Buffer>) {
    const addResult = await this.client.xAdd(streamKey, messageId, message);
    return addResult;
  }

  async listenForConnections() {
    while (!this.abortController.signal.aborted) {
      // TODO: if client.quit() is called, will throw an error? if so handle gracefully
      const connectionStreamKey = this.redisStreamKeyPrefix + 'connection_stream';
      const connections = await this.client.xRead(
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
          await this.client.xDel(connectionStreamKey, msgId);

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
    try {
      // The special key `$` instructs redis to hold onto all new messages.
      // TODO: ensure no message-skipping race-conditions around the timing of this redis command and the
      // following postgres query, given how the message ingestion is also writing messages to postgres
      // then the global redis stream. If so, we may need to use the `0` message ID to instruct redis
      // to hold onto all current messages until we start live streaming from the global redis stream.
      const msgId = '$';
      await this.client.xGroupCreate(this.globalStreamKey, groupKey, msgId, { MKSTREAM: true });
      await this.client.xGroupCreateConsumer(this.globalStreamKey, groupKey, consumerKey);
      console.log(`Consumer group "${groupKey}" created for client "${clientId}".`);
    } catch (err) {
      if ((err as Error).message?.includes('BUSYGROUP')) {
        this.logger.error(err as Error, `Consumer group alreaady exists for client ${clientId}`);
      }
      throw err;
    }

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
          content::text
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
        await this.client.xAdd(clientStreamKey, messageId, redisMsg);
        lastMsgId = messageId;
      }
      // TODO: add backpressure to avoid overwhelming redis memory, e.g. wait until the stream length
      // is below a certain threshold before continuing

      // Update the global stream group to the last message ID so it's not holding onto messages
      // that are already backfilled, using XGROUP SETID.
      await this.client.xGroupSetId(this.globalStreamKey, groupKey, lastMsgId);
    }

    // Now we can start streaming live messages from the global redis stream to the client redis stream.
    // Read from the global stream using the consumer group we created above, and write to the client's stream.

    console.log(`Starting live streaming for client "${clientId}" from ID "${lastMsgId}"`);

    while (true) {
      const messages = await this.client.xReadGroup(
        groupKey,
        consumerKey,
        { key: this.globalStreamKey, id: lastMsgId },
        { BLOCK: 1000, COUNT: 100 }
      );

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
            await this.client.xAdd(clientStreamKey, id, message);

            // Acknowledge message for this consumer group
            await this.client.xAck(this.globalStreamKey, groupKey, id);

            lastMsgId = id;
          }
        }
      }
    }
  }
}
