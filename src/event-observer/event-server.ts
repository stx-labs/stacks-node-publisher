import { createServer, IncomingMessage, ServerResponse, Server } from 'node:http';
import { logger as defaultLogger, SERVER_VERSION } from '@hirosystems/api-toolkit';
import { AddressInfo } from 'node:net';
import { Counter, Histogram, Registry, Summary } from 'prom-client';
import PQueue from 'p-queue';
import { PgStore } from '../pg/pg-store';
import { RedisBroker } from '../redis/redis-broker';
import { ENV } from '../env';

export class EventObserverServer {
  readonly server: Server;
  readonly logger = defaultLogger.child({ module: 'EventObserver' });
  readonly promRegistry: Registry;
  readonly promMetrics: ReturnType<typeof this.setupPromMetrics>;
  readonly queue = new PQueue({ concurrency: 1 });
  // readonly redisWriteQueue = new PQueue({ concurrency: 1 });

  readonly db: PgStore;
  readonly redisBroker: RedisBroker;

  constructor(args: { db: PgStore; redisBroker: RedisBroker; promRegistry: Registry }) {
    this.promRegistry = args.promRegistry;
    this.promMetrics = this.setupPromMetrics();
    this.server = createServer((req, res) => this.requestListener(req, res));
    this.db = args.db;
    this.redisBroker = args.redisBroker;
  }

  async eventMessageHandler(
    eventPath: string,
    eventBody: string,
    httpReceiveTimestamp: Date
  ): Promise<void> {
    // Storing the event in postgres is critical, if this fails then throw so the observer server
    // returns a non-200 and the stacks-node will retry the event POST.
    const dbResult = await this.db.insertMessage(eventPath, eventBody, httpReceiveTimestamp);
    // TODO: This should be fire-and-forget into a serialized promise queue, because writing the event
    // to redis is not critical and we don't want to slow down the event observer server & pg writes.
    // For example, even if redis takes a few hundred milliseconds, we don't want to block the
    // stacks-node(s) for any longer than absolutely necessary. This especially important during genesis
    // syncs and also for the high-precision stackerdb_chunk event timestamps used by clients like
    // the signer-metrics-api.
    // The promise queue should be limited to 1 concurrency to ensure the order of events is maintained,
    // and should have a reasonable max queue length to prevent memory exhaustion. If the limit is reached
    // then the redis write will just be skipped for this message, and the redis-broker layer already knows
    // how to handle this case (e.g. detecting msg gaps and backfilling from postgres).

    await this.redisBroker.addStacksMessage({
      timestamp: dbResult.timestamp,
      sequenceNumber: dbResult.sequence_number,
      eventPath,
      eventBody,
    });

    /*
    if (this.redisWriteQueue.size <= ENV.REDIS_WRITE_QUEUE_MAX_SIZE) {
      void this.redisWriteQueue.add(async () => {
        await this.redisBroker.addStacksMessage({
          timestamp: dbResult.timestamp,
          sequenceNumber: dbResult.sequence_number,
          eventPath,
          eventBody,
        });
      });
    } else {
      this.logger.warn(
        `Redis write queue is full, skipping write for message ${dbResult.sequence_number}`
      );
    }
      */
  }

  get url(): string {
    if (!this.server.listening) {
      throw new Error('Event server is not listening');
    }
    const address = this.server.address() as AddressInfo;
    return `http://${address.address}:${address.port}`;
  }

  setupPromMetrics() {
    const reqCounter = new Counter({
      registers: [this.promRegistry],
      name: 'http_request_duration_seconds_count',
      help: 'Total number of HTTP requests',
      labelNames: ['method', 'route', 'status_code'],
    });

    const reqDurationHistogram = new Histogram({
      registers: [this.promRegistry],
      name: 'http_request_duration_seconds_bucket',
      help: 'request duration in seconds',
      labelNames: ['method', 'route', 'status_code'],
      buckets: [0.05, 0.1, 0.5, 1, 3, 5, 10],
    });

    const reqDurationSummary = new Summary({
      registers: [this.promRegistry],
      name: 'http_request_summary_seconds',
      help: 'request duration in seconds summary',
      labelNames: ['method', 'route', 'status_code'],
      percentiles: [0.5, 0.9, 0.95, 0.99],
    });

    return { reqCounter, reqDurationHistogram, reqDurationSummary };
  }

  async start({ host, port }: { host: string; port: number }): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.on('error', err => {
        this.logger.error(err, 'Error starting event server');
        reject(err);
      });
      this.server.listen({ host, port }, () => {
        this.logger.info(`Event server listening on ${this.url}`);
        resolve();
      });
    });
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.logger.info(`Closing event server...`);
      this.server.close(err => {
        if (err) {
          this.logger.error(err, 'Error closing event server');
          reject(err);
        } else {
          this.logger.info('Event server closed');
          resolve();
        }
      });
    });
    await this.queue.onEmpty();
    // await this.queue.onIdle();
    // await this.redisWriteQueue.onIdle();
  }

  requestListener(req: IncomingMessage, res: ServerResponse) {
    const startTime = process.hrtime(); // Start time for duration tracking
    const url = new URL(req.url as string, `http://${req.headers.host}`);

    const httpReceiveTimestamp = new Date(
      (req.headers['x-original-timestamp'] as string) ?? Date.now()
    );

    if (req.method === 'GET' && /^\/(?:status\/?)?$/.test(url.pathname)) {
      const status = {
        server_version: `salt-n-pepper ${SERVER_VERSION.tag} (${SERVER_VERSION.branch}:${SERVER_VERSION.commit})`,
        status: 'ready',
      };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(status, null, 2));
      return;
    }

    if (req.method !== 'POST') {
      this.logger.error(`Received non-POST request: ${req.method} ${req.url}`);
      res.writeHead(405); // Method Not Allowed
      res.end('Only POST requests are allowed');
      return;
    }

    // Verify that content-type is application/json
    if (!req.headers['content-type']?.includes('application/json')) {
      this.logger.error(`Received non-JSON content-type: ${req.headers['content-type']}`);
      res.writeHead(415); // Unsupported Media Type
      res.end('Content-Type must be application/json');
      return;
    }

    const eventPath = url.pathname;
    const method = req.method as string;
    const contentLength = req.headers['content-length'];
    this.logger.info(
      { url: eventPath, contentLength: contentLength },
      `Received event POST request: ${req.url}, len: ${contentLength}`
    );

    const logResponse = (statusCode: number) => {
      const duration = process.hrtime(startTime);
      const durationInSeconds = duration[0] + duration[1] / 1e9;
      this.logger.info(
        {
          statusCode: statusCode,
          method: req.method,
          url: url.pathname,
          responseTime: durationInSeconds,
          contentLength: contentLength,
        },
        'request completed'
      );
      const labels = {
        method: method,
        route: url.pathname,
        status_code: statusCode.toString(),
      };
      this.promMetrics.reqCounter.inc(labels);
      this.promMetrics.reqDurationHistogram.observe(labels, durationInSeconds);
      this.promMetrics.reqDurationSummary.observe(labels, durationInSeconds);
    };

    let body = '';

    // Set the encoding to 'utf8' so that the data chunks are strings directly
    req.setEncoding('utf8');

    req.on('data', (chunk: string) => {
      body += chunk; // Accumulate the data chunks as a string
    });

    req.on('end', () => {
      // Body has been fully received
      void this.queue.add(async () => {
        try {
          await this.eventMessageHandler(eventPath, body, httpReceiveTimestamp);
          res.writeHead(200, { 'Content-Type': 'text/plain' });
          res.end('Received successfully!');
          logResponse(200);
        } catch (err) {
          this.logger.error(
            err,
            `Error processing event http POST payload: ${eventPath}, len: ${contentLength}`
          );
          res.writeHead(500);
          res.end('Internal Server Error during message processing');
          logResponse(500);
        }
      });
    });

    req.on('error', err => {
      this.logger.error(
        err,
        `Error reading event http POST payload: ${eventPath}, len: ${contentLength}`
      );
      res.writeHead(500);
      res.end('Internal Server Error during message reading');
      logResponse(500);
    });
  }
}
