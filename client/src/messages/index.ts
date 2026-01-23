import { DropMempoolTxMessage } from './drop-mempool-tx';
import { NewBlockMessage } from './new-block';
import { NewBurnBlockMessage } from './new-burn-block';
import { NewMempoolTxMessage } from './new-mempool-tx';
import { StackerDbChunksMessage } from './stackerdb-chunks';

export * from './drop-mempool-tx';
export * from './new-block';
export * from './new-burn-block';
export * from './new-mempool-tx';
export * from './stackerdb-chunks';

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

export type Message =
  | { path: MessagePath.NewBlock; payload: NewBlockMessage }
  | { path: MessagePath.NewBurnBlock; payload: NewBurnBlockMessage }
  | { path: MessagePath.NewMempoolTx; payload: NewMempoolTxMessage }
  | { path: MessagePath.DropMempoolTx; payload: DropMempoolTxMessage }
  | { path: MessagePath.StackerDbChunks; payload: StackerDbChunksMessage }
  // TODO: Message implementations
  | { path: MessagePath.NewMicroblocks; payload: unknown }
  | { path: MessagePath.ProposalResponse; payload: unknown }
  | { path: MessagePath.AttachmentsNew; payload: unknown };
