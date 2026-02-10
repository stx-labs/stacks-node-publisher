export interface NewBurnBlockRewardRecipient {
  /** Bitcoin address (b58 encoded). */
  recipient: string;
  /** Amount in BTC satoshis. */
  amt: number;
}

/** PoX payout event */
export interface NewBurnBlockPoxTransactionRewardRecipient {
  recipient: string;
  amt: number;
  utxo_idx: number;
}

/** PoX payout transaction */
export interface NewBurnBlockPoxTransaction {
  txid: string;
  reward_recipients: NewBurnBlockPoxTransactionRewardRecipient[];
}

export interface NewBurnBlockMessage {
  /** The hash of the burn block */
  burn_block_hash: string;
  /** The height of the burn block */
  burn_block_height: number;
  /** The reward recipients */
  reward_recipients: NewBurnBlockRewardRecipient[];
  /**
   * Array of the Bitcoin addresses that would validly receive PoX commitments during this block.
   * These addresses may not actually receive rewards during this block if the block is faster
   * than miners have an opportunity to commit.
   */
  reward_slot_holders: string[];
  /** The amount of BTC burned during the burn block */
  burn_amount: number;
  /** The consensus hash of the burn block */
  consensus_hash: string;
  /** The hash of the parent burn block */
  parent_burn_block_hash: string;
  /** The individual transaction information from signers */
  pox_transactions: NewBurnBlockPoxTransaction[];
}
