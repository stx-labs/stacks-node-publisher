import { ColumnDefinitions, MigrationBuilder } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export function up(pgm: MigrationBuilder): void {
  pgm.sql(`
    CREATE INDEX messages_path_index_block_hash_idx
    ON messages (path, (content->>'index_block_hash'))
    WHERE path = '/new_block';
  `);
}

export function down(pgm: MigrationBuilder): void {
  pgm.sql(`DROP INDEX IF EXISTS messages_path_index_block_hash_idx;`);
}
