import { ENV } from '../env';
import {
  BasePgStore,
  PgConnectionArgs,
  PgSqlClient,
  connectPostgres,
  logger,
  runMigrations,
  timeout,
} from '@hirosystems/api-toolkit';
import * as path from 'path';
import { createTestHook, isTestEnv } from '../helpers';

export const MIGRATIONS_DIR = path.join(__dirname, '../../migrations');

export class PgStore extends BasePgStore {
  _testHooks = isTestEnv
    ? {
        onMsgInserting: createTestHook<() => Promise<void>>(),
      }
    : null;

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
      logger.info('Running pg migrations, this may take a while...');
      while (true) {
        try {
          await runMigrations(MIGRATIONS_DIR, 'up', pgConfig);
          break;
        } catch (error) {
          if (/Another migration is already running/i.test((error as Error).message)) {
            logger.warn('Another migration is already running, retrying...');
            await timeout(100);
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

  public async insertMessage(
    eventPath: string,
    content: string,
    httpReceiveTimestamp: Date
  ): Promise<{ sequence_number: string; timestamp: string }> {
    if (this._testHooks) {
      for (const hook of this._testHooks.onMsgInserting) {
        await hook();
      }
    }
    const insertQuery = await this.sql<{ sequence_number: string; timestamp: string }[]>`
      INSERT INTO messages (created_at, path, content)
      VALUES (${httpReceiveTimestamp}, ${eventPath}, ${content}::jsonb)
      RETURNING sequence_number, (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS timestamp
    `;
    if (insertQuery.length !== 1) {
      throw new Error('Expected a single row to be returned');
    }
    const { sequence_number, timestamp } = insertQuery[0];
    return { sequence_number, timestamp };
  }

  public async getLastMessage() {
    const dbResults = await this.sql<
      { sequence_number: string; timestamp: string; path: string; content: string }[]
    >`
      SELECT
        sequence_number,
        (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS timestamp,
        path,
        content
      FROM messages
      ORDER BY sequence_number DESC
      LIMIT 1
    `;
    if (dbResults.length === 0) {
      return null;
    }
    return dbResults[0];
  }

  public async resolveIndexBlockHashToSequenceNumber(
    indexBlockHash: string
  ): Promise<string | null> {
    if (!indexBlockHash || indexBlockHash === '') return null;
    const result = await this.sql<{ sequence_number: string }[]>`
      SELECT sequence_number
      FROM messages
      WHERE path = '/new_block'
        AND content->>'index_block_hash' = ${indexBlockHash}
      LIMIT 1
    `;
    return result[0]?.sequence_number ?? null;
  }
}
