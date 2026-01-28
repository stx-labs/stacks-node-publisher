import { ColumnDefinitions, MigrationBuilder } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export function up(pgm: MigrationBuilder): void {
  // Index for efficiently finding /new_block messages by index_block_hash (latest first)
  pgm.sql(`
    CREATE INDEX messages_path_index_block_hash_idx
    ON messages ((content->>'index_block_hash'), sequence_number DESC)
    WHERE path = '/new_block';
  `);
  // Index for efficiently finding the highest sequence_number for /new_block messages
  pgm.sql(`
    CREATE INDEX messages_new_block_sequence_idx
    ON messages (sequence_number DESC)
    WHERE path = '/new_block';
  `);
}

export function down(pgm: MigrationBuilder): void {
  pgm.sql(`DROP INDEX IF EXISTS messages_path_index_block_hash_idx;`);
  pgm.sql(`DROP INDEX IF EXISTS messages_new_block_sequence_idx;`);
}
