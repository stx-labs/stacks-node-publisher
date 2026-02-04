<div align="center">

[![codecov](https://codecov.io/gh/hirosystems/stacks-node-publisher/graph/badge.svg?token=wASmDf3iQU)](https://codecov.io/gh/hirosystems/stacks-node-publisher)

</div>

# Stacks Node Publisher

A store-and-stream service for Stacks blockchain events. Stacks Node Publisher persists Stacks node events to PostgreSQL and streams them to consumers via Redis, enabling APIs to sync and replay events from arbitrary starting points with high availability.

## Features

### Event Streaming
- **Real-time streaming**: Stream Stacks events to multiple consumers simultaneously via Redis Streams
- **Historical replay**: Backfill events from any starting point (block hash, block height, or message ID)
- **Selective subscriptions**: Filter events by message path to receive only relevant event types
- **Automatic reconnection**: Clients automatically reconnect and resume from their last position

### Supported Event Types
| Event | Path | Description |
|-------|------|-------------|
| New Block | `/new_block` | Block data including transactions, events, and miner rewards |
| New Burn Block | `/new_burn_block` | Bitcoin block anchoring information |
| New Mempool TX | `/new_mempool_tx` | Transactions entering the mempool |
| Drop Mempool TX | `/drop_mempool_tx` | Transactions removed from the mempool |
| StackerDB Chunks | `/stackerdb_chunks` | Signer and StackerDB data chunks |
| Proposal Response | `/proposal_response` | Miner block proposal responses |
| New Microblocks | `/new_microblocks` | Microblock data (legacy) |
| Attachments | `/attachments/new` | Attachment data (legacy) |

### High Availability
- **Multi-node support**: Connect multiple Stacks nodes for redundancy
- **Automatic failover**: Consumers reconnect and resume seamlessly
- **Backpressure handling**: Built-in flow control prevents memory exhaustion
- **Idle client pruning**: Automatic cleanup of stale consumer connections

### Persistence
- **PostgreSQL storage**: All events are persisted for historical queries
- **Point-in-time recovery**: Support for database snapshots and restoration
- **Message deduplication**: Handles duplicate messages gracefully

## Client Usage

Install the client package:

```bash
npm install @stacks/stacks-node-publisher-client
```

### Basic Example

```typescript
import {
  StacksMessageStream,
  MessagePath,
  StreamPosition,
  Message,
} from '@stacks/stacks-node-publisher-client';

// Create the stream client
const stream = new StacksMessageStream({
  appName: 'my-stacks-api',
  redisUrl: 'redis://localhost:6379',
  options: {
    // Subscribe to specific event types (or use '*' for all)
    selectedMessagePaths: [MessagePath.NewBlock, MessagePath.NewBurnBlock],
    // Number of messages to fetch per batch
    batchSize: 100,
  },
});

// Connect to Redis
await stream.connect({ waitForReady: true });

// Define where to start streaming from
const getStartPosition = async (): Promise<StreamPosition> => {
  // Option 1: Start from a specific block
  // return { indexBlockHash: '0x1234...', blockHeight: 150000 };

  // Option 2: Start from a specific message ID
  // return { messageId: '1234567-0' };

  // Option 3: Start from the beginning
  return null;
};

// Handle incoming messages
const handleMessage = async (id: string, timestamp: string, message: Message) => {
  console.log(`Received message ${id} at ${timestamp}`);

  switch (message.path) {
    case MessagePath.NewBlock:
      console.log(`New block: ${message.payload.block_height}`);
      // Process block data...
      break;
    case MessagePath.NewBurnBlock:
      console.log(`New burn block: ${message.payload.burn_block_height}`);
      break;
    // Handle other message types...
  }
};

// Start streaming
stream.start(getStartPosition, handleMessage);

// To stop the stream gracefully
// await stream.stop();
```

### Handling New Blocks

```typescript
import { MessagePath, NewBlockMessage } from '@stacks/stacks-node-publisher-client';

const handleMessage = async (id: string, timestamp: string, message: Message) => {
  if (message.path === MessagePath.NewBlock) {
    const block: NewBlockMessage = message.payload;

    console.log(`Block ${block.block_height} (${block.block_hash})`);
    console.log(`  Transactions: ${block.transactions.length}`);
    console.log(`  Events: ${block.events.length}`);

    // Process transactions
    for (const tx of block.transactions) {
      if (tx.status === 'success') {
        console.log(`  TX ${tx.txid} succeeded`);
      }
    }

    // Process events (STX transfers, contract events, NFT/FT operations, etc.)
    for (const event of block.events) {
      switch (event.type) {
        case 'stx_transfer_event':
          console.log(`  STX transfer: ${event.stx_transfer_event.amount} from ${event.stx_transfer_event.sender}`);
          break;
        case 'contract_event':
          console.log(`  Contract event from ${event.contract_event.contract_identifier}`);
          break;
        // Handle other event types...
      }
    }
  }
};
```

## Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RUN_MODE` | `default` | `default`, `readonly`, or `writeonly` |
| `NETWORK` | `mainnet` | `mainnet` or `testnet` |
| `OBSERVER_HOST` | `0.0.0.0` | Event observer HTTP host |
| `OBSERVER_PORT` | `3022` | Event observer HTTP port |
| `PROMETHEUS_PORT` | `9154` | Prometheus metrics port |
| `PGHOST` | - | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGUSER` | - | PostgreSQL user |
| `PGPASSWORD` | - | PostgreSQL password |
| `PGDATABASE` | - | PostgreSQL database |
| `REDIS_URL` | - | Redis connection URL |
| `REDIS_STREAM_KEY_PREFIX` | `""` | Prefix for Redis keys |
| `DB_MSG_BATCH_SIZE` | `100` | Batch size for DB reads during backfill |
| `CLIENT_REDIS_STREAM_MAX_LEN` | `100` | Max messages in client stream before backpressure |
| `MAX_IDLE_TIME_MS` | `60000` | Max idle time before client is pruned |
| `MAX_MSG_LAG` | `2000` | Max message lag before slow client is pruned |

## Development

### Prerequisites

- Node.js (see `.nvmrc` for version)
- Docker (for local PostgreSQL and Redis)

### Setup

```bash
# Install dependencies
npm install

# Start local PostgreSQL and Redis
npm run testenv:run

# Run database migrations
npm run migrate up

# Start the service
npm run start-ts
```

### Testing

```bash
# Run all tests
npm test

# Run unit tests only
npm run test:unit

# Run database tests only
npm run test:db
```

## License

[GPL-3.0](LICENSE)
