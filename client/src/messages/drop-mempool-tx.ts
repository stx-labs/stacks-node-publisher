export type CoreNodeDropMempoolTxReasonType =
  | 'ReplaceByFee'
  | 'ReplaceAcrossFork'
  | 'TooExpensive'
  | 'StaleGarbageCollect'
  | 'Problematic';

/** Message sent when a transaction is dropped from the mempool. */
export interface CoreNodeDropMempoolTxMessage {
  dropped_txids: string[];
  reason: CoreNodeDropMempoolTxReasonType;
  new_txid: string | null;
}
