import { AttachmentsNewMessage } from './attachments-new.js';
import { DropMempoolTxMessage } from './drop-mempool-tx.js';
import { NewBlockMessage } from './new-block.js';
import { NewBurnBlockMessage } from './new-burn-block.js';
import { NewMempoolTxMessage } from './new-mempool-tx.js';
import { NewMicroblocksMessage } from './new-microblocks.js';
import { StackerDbChunksMessage } from './stackerdb-chunks.js';

export * from './attachments-new.js';
export * from './drop-mempool-tx.js';
export * from './new-block.js';
export * from './new-burn-block.js';
export * from './new-mempool-tx.js';
export * from './new-microblocks.js';
export * from './stackerdb-chunks.js';

/**
 * The path of the Stacks message as sent by the Stacks node.
 */
export enum MessagePath {
  NewBlock = '/new_block',
  NewBurnBlock = '/new_burn_block',
  NewMempoolTx = '/new_mempool_tx',
  DropMempoolTx = '/drop_mempool_tx',
  NewMicroblocks = '/new_microblocks',
  StackerDbChunks = '/stackerdb_chunks',
  ProposalResponse = '/proposal_response',
  AttachmentsNew = '/attachments/new',
}

/**
 * A message from the Stacks node. Consists of a path and a payload.
 */
export type Message =
  | { path: MessagePath.NewBlock; payload: NewBlockMessage }
  | { path: MessagePath.NewBurnBlock; payload: NewBurnBlockMessage }
  | { path: MessagePath.NewMempoolTx; payload: NewMempoolTxMessage }
  | { path: MessagePath.DropMempoolTx; payload: DropMempoolTxMessage }
  | { path: MessagePath.StackerDbChunks; payload: StackerDbChunksMessage }
  | { path: MessagePath.NewMicroblocks; payload: NewMicroblocksMessage }
  | { path: MessagePath.AttachmentsNew; payload: AttachmentsNewMessage }
  // TODO: Message implementation
  | { path: MessagePath.ProposalResponse; payload: unknown };
