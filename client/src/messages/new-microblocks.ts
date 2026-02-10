import { NewBlockEvent, NewBlockTransaction } from './new-block';

export interface NewMicroblocksTransaction extends NewBlockTransaction {
  microblock_sequence: number;
  microblock_hash: string;
  microblock_parent_hash: string;
}

export interface NewMicroblocksMessage {
  parent_index_block_hash: string;
  burn_block_hash: string;
  burn_block_height: number;
  burn_block_timestamp: number;
  // TODO(mb): assume this is too hard to get from the stacks-node event
  // parent_block_hash: string;
  transactions: NewMicroblocksTransaction[];
  events: NewBlockEvent[];
}
