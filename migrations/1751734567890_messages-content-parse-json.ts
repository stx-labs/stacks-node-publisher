import { ColumnDefinitions, MigrationBuilder } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export function up(pgm: MigrationBuilder): void {
  // Convert content column from escaped JSON strings to properly parsed JSON objects.
  pgm.sql(`
    UPDATE messages 
    SET content = (content #>> '{}')::jsonb 
    WHERE jsonb_typeof(content) = 'string';
  `);
}

export function down(pgm: MigrationBuilder): void {
  pgm.sql(`
    UPDATE messages 
    SET content = to_jsonb(content::text) 
    WHERE jsonb_typeof(content) != 'string';
  `);
}
