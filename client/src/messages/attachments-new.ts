export interface AttachmentsNewMessage {
  attachment_index: number;
  index_block_hash: string;
  block_height: string; // string quoted integer?
  content_hash: string;
  contract_id: string;
  /** Hex serialized Clarity value */
  metadata: string;
  tx_id: string;
  /* Hex encoded attachment content bytes */
  content: string;
}
