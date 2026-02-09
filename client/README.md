# @stacks/node-publisher-client

A TypeScript client library for consuming Stacks blockchain events from the [Stacks Node Publisher](https://github.com/stx-labs/stacks-node-publisher) service.

## Installation

```bash
npm install @stacks/node-publisher-client
```

## Quick Start

```typescript
import {
  StacksMessageStream,
  MessagePath,
  StreamPosition,
  Message,
} from '@stacks/node-publisher-client';

// Create the stream client
const stream = new StacksMessageStream({
  appName: 'my-app',
  redisUrl: 'redis://localhost:6379',
});

// Connect to Redis
await stream.connect({ waitForReady: true });

// Define where to start streaming from
const getStartPosition = async (): Promise<StreamPosition> => {
  return null; // Start from the beginning
};

// Handle incoming messages
const handleMessage = async (id: string, timestamp: string, message: Message) => {
  console.log(`Received ${message.path} at ${timestamp}`);
};

// Start streaming
stream.start(getStartPosition, handleMessage);
```

## API Reference

### `StacksMessageStream`

The main client class for connecting to and consuming events from a Stacks Node Publisher service.

#### Constructor Options

```typescript
new StacksMessageStream({
  appName: string;           // Required: Unique identifier for your application
  redisUrl?: string;         // Redis connection URL (default: localhost)
  redisStreamPrefix?: string; // Prefix for Redis stream keys
  options?: {
    selectedMessagePaths?: MessagePath[] | '*'; // Filter by message types (default: '*')
    batchSize?: number;      // Messages per batch (default: 100)
  };
});
```

#### Methods

##### `connect(options: { waitForReady: boolean }): Promise<void>`

Connects to the Redis server.

- `waitForReady: true` - Blocks until connected (recommended for startup)
- `waitForReady: false` - Connects in the background

```typescript
await stream.connect({ waitForReady: true });
```

##### `start(positionCallback, messageCallback): void`

Starts consuming the event stream.

- `positionCallback: () => Promise<StreamPosition>` - Called to determine where to start/resume streaming
- `messageCallback: (id, timestamp, message) => Promise<void>` - Called for each received message

```typescript
stream.start(
  async () => ({ indexBlockHash: '0x...', blockHeight: 150000 }),
  async (id, timestamp, message) => {
    // Process message
  }
);
```

##### `stop(): Promise<void>`

Gracefully stops the stream and closes the Redis connection.

```typescript
await stream.stop();
```

### `StreamPosition`

Defines where to start or resume the event stream.

```typescript
type StreamPosition =
  | { indexBlockHash: string; blockHeight: number } // Start from a specific block
  | { messageId: string }                           // Start from a specific message ID
  | null;                                           // Start from the beginning
```

## Message Types

### `MessagePath`

Enum of all available message paths:

| Path | Description |
|------|-------------|
| `MessagePath.NewBlock` | New Stacks block with transactions and events |
| `MessagePath.NewBurnBlock` | Bitcoin anchor block information |
| `MessagePath.NewMempoolTx` | Transactions entering the mempool |
| `MessagePath.DropMempoolTx` | Transactions removed from the mempool |
| `MessagePath.StackerDbChunks` | Signer and StackerDB data chunks |
| `MessagePath.NewMicroblocks` | Microblock data (legacy) |
| `MessagePath.ProposalResponse` | Miner block proposal responses |
| `MessagePath.AttachmentsNew` | Attachment data (legacy) |

### `NewBlockMessage`

Contains full block data including:

- Block metadata (hash, height, timestamps)
- All transactions with execution results
- Events (STX transfers, contract events, NFT/FT operations)
- Miner rewards
- Signer information (epoch 3+)

```typescript
interface NewBlockMessage {
  block_hash: string;
  block_height: number;
  index_block_hash: string;
  burn_block_hash: string;
  burn_block_height: number;
  burn_block_time: number;
  transactions: NewBlockTransaction[];
  events: NewBlockEvent[];
  matured_miner_rewards: MinerReward[];
  // ... additional fields
}
```

### `NewBurnBlockMessage`

Bitcoin block anchoring information:

```typescript
interface NewBurnBlockMessage {
  burn_block_hash: string;
  burn_block_height: number;
  burn_amount: number;           // BTC satoshis
  reward_recipients: { recipient: string; amt: number }[];
  reward_slot_holders: string[]; // Bitcoin addresses
}
```

### `NewMempoolTxMessage`

Array of raw hex-encoded transactions entering the mempool:

```typescript
type NewMempoolTxMessage = string[];
```

### `DropMempoolTxMessage`

Transactions removed from the mempool:

```typescript
interface DropMempoolTxMessage {
  dropped_txids: string[];
  reason: 'ReplaceByFee' | 'ReplaceAcrossFork' | 'TooExpensive' | 'StaleGarbageCollect' | 'Problematic';
  new_txid: string | null;
}
```

### `StackerDbChunksMessage`

Signer and StackerDB data chunks:

```typescript
interface StackerDbChunksMessage {
  contract_id: { issuer: [number, number[]]; name: string };
  modified_slots: {
    slot_id: number;
    slot_version: number;
    data: string;    // hex string
    sig: string;     // hex signature
  }[];
}
```

## Usage Examples

### Filtering by Message Type

```typescript
const stream = new StacksMessageStream({
  appName: 'block-indexer',
  redisUrl: 'redis://localhost:6379',
  options: {
    selectedMessagePaths: [MessagePath.NewBlock, MessagePath.NewBurnBlock],
  },
});
```

### Resuming from Last Processed Block

```typescript
const getStartPosition = async (): Promise<StreamPosition> => {
  const lastBlock = await db.getLastProcessedBlock();
  if (lastBlock) {
    return {
      indexBlockHash: lastBlock.indexBlockHash,
      blockHeight: lastBlock.height,
    };
  }
  return null; // Start from beginning if no prior state
};

stream.start(getStartPosition, handleMessage);
```

### Processing Block Events

```typescript
import { MessagePath, NewBlockMessage, NewBlockEventType } from '@stacks/node-publisher-client';

const handleMessage = async (id: string, timestamp: string, message: Message) => {
  if (message.path !== MessagePath.NewBlock) return;

  const block: NewBlockMessage = message.payload;

  for (const event of block.events) {
    switch (event.type) {
      case NewBlockEventType.StxTransfer:
        console.log(`STX Transfer: ${event.stx_transfer_event.amount} microSTX`);
        console.log(`  From: ${event.stx_transfer_event.sender}`);
        console.log(`  To: ${event.stx_transfer_event.recipient}`);
        break;

      case NewBlockEventType.Contract:
        console.log(`Contract Event: ${event.contract_event.contract_identifier}`);
        console.log(`  Topic: ${event.contract_event.topic}`);
        break;

      case NewBlockEventType.NftMint:
        console.log(`NFT Minted: ${event.nft_mint_event.asset_identifier}`);
        break;
    }
  }
};
```

### Handling Reconnection

The client automatically handles reconnection. You can listen for the `redisConsumerGroupDestroyed` event to perform additional cleanup:

```typescript
stream.events.on('redisConsumerGroupDestroyed', () => {
  console.log('Consumer group was destroyed, will automatically reconnect');
  // Optionally refresh your position callback state
});
```

## TypeScript Support

This package is written in TypeScript and includes full type definitions. All message types are fully typed:

```typescript
import type {
  Message,
  MessagePath,
  StreamPosition,
  NewBlockMessage,
  NewBlockEvent,
  NewBlockTransaction,
  NewBurnBlockMessage,
  NewMempoolTxMessage,
  DropMempoolTxMessage,
  StackerDbChunksMessage,
  ClarityAbi,
} from '@stacks/node-publisher-client';
```

## License

GPL-3.0-only
