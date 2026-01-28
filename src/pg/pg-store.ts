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
      VALUES (${httpReceiveTimestamp}, ${eventPath}, ${JSON.parse(content)})
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
        content::text AS content
      FROM messages
      ORDER BY sequence_number DESC
      LIMIT 1
    `;
    if (dbResults.length === 0) {
      return null;
    }
    return dbResults[0];
  }

  /**
   * Resolves an index block hash to a sequence number.
   * - If the block hash is found, returns its sequence number.
   * - If the block hash is not found but blockHeight is provided and is higher than the highest
   *   available block, returns the sequence number of the highest available block (clamped).
   * - If the block hash is not found and blockHeight is not higher, returns null.
   */
  public async resolveIndexBlockHashToSequenceNumber(
    indexBlockHash: string,
    blockHeight?: number
  ): Promise<{ sequenceNumber: string; clampedToMax: boolean } | null> {
    if (!indexBlockHash || indexBlockHash === '') return null;
    // Block 0 can appear at any time randomly. To be safe, we don't allow it to be used as a
    // starting point.
    if (blockHeight === 0) return null;

    // First, try to find the exact block hash (get the latest if multiple exist)
    const exactMatch = await this.sql<{ sequence_number: string }[]>`
      SELECT sequence_number
      FROM messages
      WHERE path = '/new_block'
        AND content->>'index_block_hash' = ${indexBlockHash}
      ORDER BY sequence_number DESC
      LIMIT 1
    `;
    if (exactMatch.count > 0) {
      return { sequenceNumber: exactMatch[0].sequence_number, clampedToMax: false };
    }

    // Block hash not found - check if we should clamp to the highest available
    if (blockHeight !== undefined) {
      // Get the highest block and its sequence number (sorted by sequence_number, not block_height,
      // since sequence_number is the canonical ordering)
      const maxBlock = await this.sql<{ sequence_number: string; block_height: number }[]>`
        SELECT sequence_number, (content->>'block_height')::int AS block_height
        FROM messages
        WHERE path = '/new_block'
        ORDER BY sequence_number DESC
        LIMIT 1
      `;
      if (maxBlock.count > 0 && blockHeight > maxBlock[0].block_height) {
        // The requested block height is higher than our highest - clamp to max
        return { sequenceNumber: maxBlock[0].sequence_number, clampedToMax: true };
      }
    }

    // Block hash not found and not eligible for clamping
    return null;
  }

  /**
   * Validates a message ID and returns the resolved sequence number.
   * - If the message ID is valid (exists or is within range), returns it as-is.
   * - If the message ID exceeds the highest available, returns the highest available (clamped).
   * - If the message ID is empty/invalid or no messages exist, returns null.
   */
  public async validateAndResolveMessageId(
    messageId: string
  ): Promise<{ sequenceNumber: string; clampedToMax: boolean } | null> {
    if (!messageId || messageId === '') return null;

    // Extract the sequence number from the message ID (format: "sequenceNumber-0")
    const sequenceNumber = messageId.split('-')[0];
    if (!sequenceNumber || isNaN(parseInt(sequenceNumber))) {
      return null;
    }

    // Check if this sequence number exists or is within the valid range
    const result = await this.sql<{ max_sequence: string; exists: boolean }[]>`
      SELECT
        (SELECT MAX(sequence_number) FROM messages) AS max_sequence,
        EXISTS(SELECT 1 FROM messages WHERE sequence_number = ${sequenceNumber}) AS exists
    `;
    if (result.count === 0 || !result[0].max_sequence) {
      // No messages in database
      return null;
    }

    const maxSequence = BigInt(result[0].max_sequence);
    const requestedSequence = BigInt(sequenceNumber);

    // If the requested sequence is greater than the max, clamp to max
    if (requestedSequence > maxSequence) {
      return { sequenceNumber: result[0].max_sequence, clampedToMax: true };
    }

    // Return the sequence number (it's valid - either exists or is within range)
    return { sequenceNumber, clampedToMax: false };
  }
}
