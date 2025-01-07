import { createClient } from 'redis';
import { logger as defaultLogger, timeout } from '@hirosystems/api-toolkit';
import { ENV } from '../env';

export class RedisBroker {
  private client: ReturnType<typeof createClient>;
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });

  constructor(args: { redisUrl?: string }) {
    this.client = createClient({
      url: args.redisUrl,
      name: 'salt-n-pepper-server',
    });

    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', (err: Error) => this.logger.error(err, 'Redis error'));
    this.client.on('reconnecting', () => this.logger.info('Reconnecting to Redis'));
    this.client.on('ready', () => this.logger.info('Redis connection ready'));
  }

  async connect({ waitForReady }: { waitForReady: boolean }) {
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
  }

  async close() {
    await this.client.quit();
  }

  async addStacksMessage(args: {
    timestamp: string;
    sequenceNumber: string;
    eventPath: string;
    eventBody: string;
  }) {
    try {
      // Redis stream message IDs are <millisecondsTime>-<sequenceNumber>.
      // However, we don't fully trust our timestamp to always increase monotonically (e.g. NTP glitches),
      // so we'll just use the sequence number as the timestamp.
      const messageId = `${args.sequenceNumber}-0`;
      const redisMsg = {
        timestamp: args.timestamp,
        path: args.eventPath,
        body: args.eventBody,
      };
      const streamKey = ENV.REDIS_STREAM_KEY_PREFIX + 'all';
      await this.addMessage(streamKey, messageId, redisMsg);
    } catch (error) {
      this.logger.error(error as Error, 'Failed to add message to Redis');
      throw error;
    }
  }

  async addMessage(streamKey: string, messageId: string, message: Record<string, string | Buffer>) {
    const addResult = await this.client.xAdd(streamKey, messageId, message);
    return addResult;
  }
}
