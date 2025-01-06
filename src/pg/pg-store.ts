import { ENV } from '../env';
import {
  BasePgStore,
  PgConnectionArgs,
  PgSqlClient,
  connectPostgres,
  logger,
  runMigrations,
} from '@hirosystems/api-toolkit';
import * as path from 'path';
import { sleep } from '../helpers';

export const MIGRATIONS_DIR = path.join(__dirname, '../../migrations');

export class PgStore extends BasePgStore {
  static async connect(opts?: {
    skipMigrations?: boolean;
    /** If a PGSCHEMA is run `CREATE SCHEMA IF NOT EXISTS schema_name` */
    createSchema?: boolean;
  }): Promise<PgStore> {
    const pgConfig: PgConnectionArgs = {
      host: ENV.PGHOST,
      port: ENV.PGPORT,
      user: ENV.PGUSER,
      password: ENV.PGPASSWORD,
      database: ENV.PGDATABASE,
      schema: ENV.PGSCHEMA,
    };
    const sql = await connectPostgres({
      usageName: 'salt-n-pepper-pg-store',
      connectionArgs: pgConfig,
      connectionConfig: {
        poolMax: ENV.PG_CONNECTION_POOL_MAX,
        idleTimeout: ENV.PG_IDLE_TIMEOUT,
        maxLifetime: ENV.PG_MAX_LIFETIME,
      },
    });

    if (pgConfig.schema && opts?.createSchema !== false) {
      await sql`CREATE SCHEMA IF NOT EXISTS ${sql(pgConfig.schema)}`;
    }
    if (opts?.skipMigrations !== true) {
      while (true) {
        try {
          await runMigrations(MIGRATIONS_DIR, 'up', pgConfig);
          break;
        } catch (error) {
          if (/Another migration is already running/i.test((error as Error).message)) {
            logger.warn('Another migration is already running, retrying...');
            await sleep(100);
            continue;
          }
          throw error;
        }
      }
    }
    return new PgStore(sql);
  }

  constructor(sql: PgSqlClient) {
    super(sql);
  }

  /*
  async function handleMessage(message) {
    const insertQuery = `
      INSERT INTO messages (content)
      VALUES ($1)
      RETURNING sequence_number, EXTRACT(EPOCH FROM created_at)::BIGINT AS timestamp
    `;
    const values = [message];
  
    try {
      // Insert into PostgreSQL and retrieve sequence_number and timestamp
      const result = await pgPool.query(insertQuery, values);
      const { sequence_number, timestamp } = result.rows[0];
  
      // Generate Redis stream message ID using timestamp and sequence number
      const messageId = `${timestamp}-${sequence_number}`;
  
      // Add message to Redis stream
      await redisClient.xadd('your-stream-key', messageId, 'content', message);
  
      console.log(`Message saved to Redis with ID: ${messageId}`);
    } catch (err) {
      console.error('Error saving message:', err);
    }
  }
    */

  public async insertMessage(
    eventPath: string,
    content: string
  ): Promise<{ sequence_number: string; timestamp: string }> {
    const insertQuery = await this.sql<{ sequence_number: string; timestamp: string }[]>`
      INSERT INTO messages (path, content)
      VALUES (${eventPath}, ${content}::jsonb)
      RETURNING sequence_number, EXTRACT(EPOCH FROM created_at)::BIGINT AS timestamp
    `;
    if (insertQuery.length !== 1) {
      throw new Error('Expected a single row to be returned');
    }
    const { sequence_number, timestamp } = insertQuery[0];
    return { sequence_number, timestamp };
  }
}
