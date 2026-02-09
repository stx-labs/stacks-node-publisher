export type DropMempoolTxReasonType =
  | 'ReplaceByFee'
  | 'ReplaceAcrossFork'
  | 'TooExpensive'
  | 'StaleGarbageCollect'
  | 'Problematic';

/** Message sent when a transaction is dropped from the mempool. */
export interface DropMempoolTxMessage {
  dropped_txids: string[];
  reason: DropMempoolTxReasonType;
  new_txid: string | null;
}
