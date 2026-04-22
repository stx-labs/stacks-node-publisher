import { createServer, IncomingMessage, ServerResponse, Server } from 'node:http';
import { logger as defaultLogger, SERVER_VERSION } from '@stacks/api-toolkit';
import { AddressInfo } from 'node:net';
import { Registry } from 'prom-client';
import PQueue from 'p-queue';
import { PgStore } from '../pg/pg-store';
import { RedisBroker } from '../redis/redis-broker';
import { EventMetrics, registerEventMetrics } from './event-metrics';

export class EventObserverServer {
  readonly server: Server;
  readonly logger = defaultLogger.child({ module: 'EventObserver' });
  readonly promMetrics: EventMetrics;
  readonly queue = new PQueue({ concurrency: 1 });

  readonly db: PgStore;
  readonly redisBroker: RedisBroker;

  /**
   * When `true`, all incoming messages are silently skipped until the next new block arrives. This
   * flag is activated if we start receiving old messages, indicating the Stacks node is still
   * catching up to us.
   */
  private skipMessages = false;

  constructor(args: { db: PgStore; redisBroker: RedisBroker; promRegistry: Registry }) {
    this.promMetrics = registerEventMetrics(args.promRegistry);
    this.server = createServer((req, res) => this.requestListener(req, res));
    this.db = args.db;
    this.redisBroker = args.redisBroker;
  }

  async eventMessageHandler(
    eventPath: string,
    eventBody: string,
    httpReceiveTimestamp: Date
  ): Promise<string> {
    let resultMessage = '';

    // Messages are parsed into a JSON object for validation and to make sure they are stored as
    // JSONB in the database.
    const jsonBody = JSON.parse(eventBody) as Record<string, unknown>;
    let timestamp: string | undefined;
    let sequenceNumber: string | undefined;

    // Handle `/new_block` messages specially to ensure block continuity and gracefully handle
    // repeated blocks if the Stacks node is behind us.
    if (eventPath === '/new_block') {
      const blockHeight = jsonBody.block_height as number;
      const indexBlockHash = jsonBody.index_block_hash as string;
      const parentIndexBlockHash = jsonBody.parent_index_block_hash as string;

      const blockResult = await this.db.insertNewBlockMessage(
        eventPath,
        jsonBody,
        httpReceiveTimestamp,
        blockHeight,
        indexBlockHash,
        parentIndexBlockHash
      );
      this.promMetrics.blockActionTotal.inc({ action: blockResult.action });
      switch (blockResult.action) {
        case 'write': {
          if (this.skipMessages) {
            this.logger.info(
              `Resuming event ingestion at block ${blockHeight} ${indexBlockHash}, Stacks node has caught up`
            );
            this.skipMessages = false;
            this.promMetrics.eventSkipMode.set(0);
          }
          if (!blockResult.sequence_number || !blockResult.timestamp) {
            throw new Error('Expected sequence_number and timestamp for written block');
          }
          this.promMetrics.lastBlockHeight.set(blockHeight);
          timestamp = blockResult.timestamp;
          sequenceNumber = blockResult.sequence_number;
          resultMessage = `Inserted ${eventPath} message, seq: ${sequenceNumber}, block: ${blockHeight} ${indexBlockHash}`;
          break;
        }
        case 'skip': {
          this.skipMessages = true;
          this.promMetrics.eventSkipMode.set(1);
          return `Repeated block detected at ${blockHeight} ${indexBlockHash}, skipping until Stacks node catches up`;
        }
        case 'ignore': {
          return `Ignoring repeated genesis block (height 0)`;
        }
        case 'reject': {
          const message = `Rejecting block ${blockHeight} ${indexBlockHash}: parent block ${parentIndexBlockHash} not found`;
          this.logger.error(message);
          throw new Error(message);
        }
      }
    } else {
      if (this.skipMessages) {
        return `Skipping ${eventPath} message, Stacks node still catching up`;
      }
      // Storing the event in postgres is critical, if this fails then throw so the observer server
      // returns a non-200 and the stacks-node will retry the event POST.
      const dbResult = await this.db.insertMessage(eventPath, jsonBody, httpReceiveTimestamp);
      timestamp = dbResult.timestamp;
      sequenceNumber = dbResult.sequence_number;
      resultMessage = `Inserted ${eventPath} message, seq: ${sequenceNumber}`;
    }

    // TODO: This should be fire-and-forget into a serialized promise queue, because writing the
    // event to redis is not critical and we don't want to slow down the event observer server & pg
    // writes. For example, even if redis takes a few hundred milliseconds, we don't want to block
    // the stacks-node(s) for any longer than absolutely necessary. This especially important during
    // genesis syncs and also for the high-precision stackerdb_chunk event timestamps used by
    // clients like the signer-metrics-api.
    //
    // The promise queue should be limited to 1 concurrency to ensure the order of events is
    // maintained, and should have a reasonable max queue length to prevent memory exhaustion. If
    // the limit is reached then the redis write will just be skipped for this message, and the
    // redis-broker layer already knows how to handle this case (e.g. detecting msg gaps and
    // backfilling from postgres).
    await this.redisBroker.addStacksMessage({
      timestamp,
      sequenceNumber,
      eventPath,
      eventBody,
    });
    return resultMessage;
  }

  get url(): string {
    if (!this.server.listening) {
      throw new Error('Event server is not listening');
    }
    const address = this.server.address() as AddressInfo;
    return `http://${address.address}:${address.port}`;
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
  }

  requestListener(req: IncomingMessage, res: ServerResponse) {
    const startTime = process.hrtime(); // Start time for duration tracking
    const url = new URL(req.url as string, `http://${req.headers.host}`);

    const httpReceiveTimestamp = new Date(
      (req.headers['x-original-timestamp'] as string) ?? Date.now()
    );

    if (req.method === 'GET' && /^\/(?:status\/?)?$/.test(url.pathname)) {
      const status = {
        server_version: `stacks-node-publisher ${SERVER_VERSION.tag} (${SERVER_VERSION.branch}:${SERVER_VERSION.commit})`,
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

    const logResponse = (statusCode: number, resultMessage?: string) => {
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
        `Request completed${resultMessage ? `: ${resultMessage}` : ''} in ${durationInSeconds.toFixed(3)}s`
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
      this.promMetrics.eventPayloadBytes.observe(
        { route: eventPath },
        Buffer.byteLength(body, 'utf8')
      );
      this.promMetrics.lastEventReceivedTimestamp.set({ route: eventPath }, Date.now() / 1000);
      const task = this.queue.add(async () => {
        try {
          const result = await this.eventMessageHandler(eventPath, body, httpReceiveTimestamp);
          res.writeHead(200, { 'Content-Type': 'text/plain' });
          res.end('Received successfully!');
          logResponse(200, result);
        } catch (err) {
          this.logger.error(
            err,
            `Error processing event http POST payload: ${eventPath}, len: ${contentLength}`
          );
          this.promMetrics.eventErrorsTotal.inc({ route: eventPath });
          res.writeHead(500, { 'Content-Type': 'text/plain' });
          res.end('Internal Server Error during message processing');
          logResponse(500);
        }
      });
      this.promMetrics.eventQueueDepth.set(this.queue.size + this.queue.pending);
      void task.finally(() => {
        this.promMetrics.eventQueueDepth.set(this.queue.size + this.queue.pending);
      });
    });

    req.on('error', err => {
      this.logger.error(
        err,
        `Error reading event http POST payload: ${eventPath}, len: ${contentLength}`
      );
      this.promMetrics.eventErrorsTotal.inc({ route: eventPath });
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal Server Error during message reading');
      logResponse(500);
    });
  }
}
