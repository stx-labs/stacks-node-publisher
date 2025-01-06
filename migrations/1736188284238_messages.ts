import { MigrationBuilder, ColumnDefinitions } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export function up(pgm: MigrationBuilder): void {
  pgm.createTable('messages', {
    // BIGSERIAL PRIMARY KEY -- Auto-incrementing 64-bit integer
    sequence_number: {
      type: 'bigserial',
      primaryKey: true,
    },
    // TIMESTAMPTZ DEFAULT NOW() -- Automatically set the current timestamp
    created_at: {
      type: 'timestamptz',
      default: pgm.func('(NOW())'),
    },
    // event url path
    path: {
      type: 'text',
      notNull: true,
    },
    // event post body content
    content: {
      type: 'jsonb',
      notNull: true,
    },
  });
}
