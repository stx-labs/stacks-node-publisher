import { Counter, Gauge, Histogram, Registry, Summary } from 'prom-client';

export function registerEventMetrics(registry: Registry) {
  const reqCounter = new Counter({
    registers: [registry],
    name: 'snp_http_request_duration_seconds_count',
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'route', 'status_code'],
  });

  const reqDurationHistogram = new Histogram({
    registers: [registry],
    name: 'snp_http_request_duration_seconds_bucket',
    help: 'request duration in seconds',
    labelNames: ['method', 'route', 'status_code'],
    buckets: [0.05, 0.1, 0.5, 1, 3, 5, 10],
  });

  const reqDurationSummary = new Summary({
    registers: [registry],
    name: 'snp_http_request_summary_seconds',
    help: 'request duration in seconds summary',
    labelNames: ['method', 'route', 'status_code'],
    percentiles: [0.5, 0.9, 0.95, 0.99],
  });

  const lastEventReceivedTimestamp = new Gauge({
    registers: [registry],
    name: 'snp_stacks_event_last_received_timestamp',
    help: 'Unix timestamp of the last event received, labeled by route',
    labelNames: ['route'],
  });

  const lastBlockHeight = new Gauge({
    registers: [registry],
    name: 'snp_stacks_last_block_height',
    help: 'The latest block height received from /new_block events',
  });

  const blockActionTotal = new Counter({
    registers: [registry],
    name: 'snp_stacks_block_action_total',
    help: 'Count of /new_block outcomes by action (write, skip, reject, ignore)',
    labelNames: ['action'],
  });

  const eventSkipMode = new Gauge({
    registers: [registry],
    name: 'snp_stacks_event_skip_mode',
    help: 'Whether the server is in skip mode (1) because the Stacks node is catching up',
  });

  const eventErrorsTotal = new Counter({
    registers: [registry],
    name: 'snp_stacks_event_errors_total',
    help: 'Total event processing failures by route',
    labelNames: ['route'],
  });

  const eventPayloadBytes = new Histogram({
    registers: [registry],
    name: 'snp_stacks_event_payload_bytes',
    help: 'Size of incoming event payloads in bytes, labeled by route',
    labelNames: ['route'],
    buckets: [1_000, 10_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000],
  });

  const eventQueueDepth = new Gauge({
    registers: [registry],
    name: 'snp_stacks_event_queue_depth',
    help: 'Current depth of the event processing queue',
  });

  return {
    reqCounter,
    reqDurationHistogram,
    reqDurationSummary,
    lastEventReceivedTimestamp,
    lastBlockHeight,
    blockActionTotal,
    eventSkipMode,
    eventErrorsTotal,
    eventPayloadBytes,
    eventQueueDepth,
  };
}

export type EventMetrics = ReturnType<typeof registerEventMetrics>;
