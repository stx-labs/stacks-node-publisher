import { createClient } from 'redis';
import { logger as defaultLogger } from '@hirosystems/api-toolkit';
import { EventMessageHandler } from '../event-observer/event-server';
import { PgStore } from '../pg/pg-store';

export class RedisBroker {
  private client: ReturnType<typeof createClient>;
  readonly logger = defaultLogger.child({ module: 'RedisBroker' });
  private _connectionStarted = false;

  constructor(args: { redisUrl?: string }) {
    this.client = createClient({ url: args.redisUrl });
    // Must have a listener for 'error' events to avoid unhandled exceptions
    this.client.on('error', err => {
      if (!this._connectionStarted) {
        console.error('Redis Client Error', err);
      }
    });
  }

  async connect() {
    this._connectionStarted = true;
    await new Promise<void>((resolve, reject) => {
      this.client.on('error', err => {
        this.logger.error(err as Error, 'Error connecting to Redis');
        reject(err as Error);
      });
      this.client.on('ready', () => {
        this.logger.info('Connected to Redis');
        resolve();
      });
      this.client.connect().catch((err: unknown) => {
        this.logger.error(err as Error, 'Error connecting to Redis');
        reject(err as Error);
      });
    });
  }

  async close() {
    await this.client.quit();
  }

  eventMessageHandlerInserter(db: PgStore): EventMessageHandler {
    return async (eventPath: string, eventBody: string) => {
      const insertResult = await db.insertMessage(eventPath, eventBody);
      const messageId = `${insertResult.timestamp}-${insertResult.sequence_number}`;
      const redisMsg = {
        timestamp: insertResult.timestamp,
        path: eventPath,
        body: eventBody,
      };
      // TODO: figure out stream key
      await this.addMessage('some stream key', messageId, JSON.stringify(redisMsg));
    };
  }

  async addMessage(streamKey: string, messageId: string, message: string) {
    const addResult = await this.client.xAdd(streamKey, messageId, { content: message });
    return addResult;
  }
}
