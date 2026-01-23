export interface StackerDbChunksModifiedSlot {
  /** Slot identifier (unique for each DB instance) */
  slot_id: number; // u32
  /** Slot version (a lamport clock) */
  slot_version: number; // u32
  /** Chunk data (use the sha512_256 hashed of this for generating a signature) */
  data: string; // hex string
  /** signature over the above */
  sig: string; // hex string (65 bytes)
}

export interface StackerDbChunksMessage {
  contract_id: {
    issuer: [number, number[]];
    name: string;
  };
  modified_slots: StackerDbChunksModifiedSlot[];
}
