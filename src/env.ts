import { Static, Type } from '@sinclair/typebox';
import envSchema from 'env-schema';

const schema = Type.Object({
  /**
   * Run mode for this service. Allows you to control how the service runs, typically
   * in an auto-scaled environment. Available values are:
   * * `default`: Runs background jobs and the REST API server (this is the default)
   * * `writeonly`: Runs only background jobs
   * * `readonly`: Runs only the REST API server
   */
  RUN_MODE: Type.Enum(
    { default: 'default', readonly: 'readonly', writeonly: 'writeonly' },
    { default: 'default' }
  ),
  /** Specifies which Stacks network this API is indexing */
  NETWORK: Type.Enum({ mainnet: 'mainnet', testnet: 'testnet' }, { default: 'mainnet' }),

  /** Host/interface to listen on for the event-observer HTTP server */
  OBSERVER_HOST: Type.String({ default: '0.0.0.0' }),
  /** Port to listen on for the event-observer HTTP server */
  OBSERVER_PORT: Type.Number({ default: 3022, minimum: 0, maximum: 65535 }),

  /** Port in which to serve prometheus metrics */
  PROMETHEUS_PORT: Type.Number({ default: 9154 }),
  /** Port in which to serve the profiler */
  PROFILER_PORT: Type.Number({ default: 9119 }),

  PGHOST: Type.String(),
  PGPORT: Type.Number({ default: 5432, minimum: 0, maximum: 65535 }),
  PGUSER: Type.String(),
  PGPASSWORD: Type.String(),
  PGDATABASE: Type.String(),
  PGSCHEMA: Type.Optional(Type.String()),
  /** Limit to how many concurrent connections can be created */
  PG_CONNECTION_POOL_MAX: Type.Number({ default: 10 }),
  PG_IDLE_TIMEOUT: Type.Number({ default: 0 }),
  PG_MAX_LIFETIME: Type.Number({ default: 0 }),

  REDIS_URL: Type.Optional(Type.String()),
  REDIS_STREAM_KEY_PREFIX: Type.String({ default: '' }),

  /** Size of the batch of messages to read from pg and write to redis during backfilling. */
  DB_MSG_BATCH_SIZE: Type.Integer({ default: 100 }),
  /**
   * Cursor page size for streaming rows from pg during backfilling. Controls how many rows are
   * held in memory at once, reducing peak memory usage for large messages.
   */
  DB_MSG_CURSOR_SIZE: Type.Integer({ default: 10 }),
  /** Max size of the batch of messages to read from redis global stream and write into the consumer stream during live-streaming. */
  LIVE_STREAM_BATCH_SIZE: Type.Integer({ default: 100 }),
  /** Max number of msgs in a consumer stream before backpressure (waiting) is applied. */
  CLIENT_REDIS_STREAM_MAX_LEN: Type.Integer({ default: 100 }),
  /** Interval (ms) to poll for backpressure on the consumer stream. */
  CLIENT_REDIS_BACKPRESSURE_POLL_MS: Type.Integer({ default: 1_000 }),
  /** Max time (ms) before pruning an empty consumer stream. */
  MAX_IDLE_TIME_MS: Type.Integer({ default: 60_000 }),
  /**
   * Max time (ms) before pruning a consumer stream that has pending (unacknowledged) messages. This
   * covers the case where a client crashes mid-processing, leaving orphaned pending entries. Set
   * higher than the longest expected message-processing time to avoid killing slow consumers.
   */
  MAX_STUCK_TIME_MS: Type.Integer({ default: 3_600_000 }),
  /**
   * Max number of messages that a consumer stream can lag behind compared to the last chain tip
   * message before it's considered slow and demoted to backfill.
   */
  MAX_MSG_LAG: Type.Integer({ default: 2000 }),
  /**
   * Interval (ms) for running Redis cleanup tasks (trimming chain tip stream and pruning idle
   * clients).
   */
  CLEANUP_INTERVAL_MS: Type.Integer({ default: 60_000 }),
});
type Env = Static<typeof schema>;

function getEnv() {
  const env = {};
  function reload() {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Object.keys(env).forEach(key => delete (env as Record<string, any>)[key]);
    return Object.assign(env, {
      reload,
      ...envSchema<Env>({
        schema: schema,
        dotenv: true,
      }),
    });
  }
  return reload();
}

export const ENV = getEnv();
