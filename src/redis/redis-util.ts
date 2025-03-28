import { ErrorReply, MultiErrorReply } from 'redis';
import type { RedisClient } from './redis-types';

/** If the error is a MultiErrorReply instance with 1 entry, extract and return that, otherwise return null */
export function unwrapRedisMultiErrorReply(error: Error) {
  if (error instanceof MultiErrorReply) {
    const innerErrors = [...error.errors()];
    if (innerErrors.length === 1 && innerErrors[0] instanceof ErrorReply) {
      return innerErrors[0];
    }
  }
  return null;
}

function parseXInfoStreamFull(result: unknown) {
  const resp = result as unknown[];
  return {
    length: resp[1] as number,
    radixTreeKeys: resp[3] as number,
    radixTreeNodes: resp[5] as number,
    lastGeneratedId: resp[7] as string | null,
    maxDeletedEntryId: resp[9] as string | null,
    entriesAdded: resp[11] as number,
    recordedFirstEntryId: resp[13] as string | null,
    entries: (resp[15] as unknown[][]).map((entry: unknown[]) => ({
      id: entry[0] as string,
      msg: entry[1],
    })),
    groups: (resp[17] as unknown[][]).map((group: unknown[]) => ({
      name: group[1] as string,
      lastDeliveredId: group[3] as string,
      entriesRead: group[5] as number | null,
      lag: group[7] as number | null,
      pelCount: group[9] as number | null,
      pending: group[11] as unknown[],
      consumers: (group[13] as unknown[][]).map((c: unknown[]) => ({
        name: c[1] as string,
        seenTime: c[3] as number,
        activeTime: c[5] as number,
        pelCount: c[7] as number,
        pending: c[9] as unknown[],
      })),
    })),
  };
}

export type XInfoStreamFullResponse = ReturnType<typeof parseXInfoStreamFull>;

/** `XINFO STREAM key FULL COUNT 1` It's not supported in the redis library yet, so it needs manual parsing. */
export function xInfoStreamFull(
  client: RedisClient,
  stream: string
): Promise<XInfoStreamFullResponse> {
  const cmd = ['XINFO', 'STREAM', stream, 'FULL', 'COUNT', '1'];
  return client.sendCommand(cmd).then(result => parseXInfoStreamFull(result));
}

/**
 * Perform `XINFO STREAM key FULL COUNT 1` for multiple streams. If a stream doesn't exist then its entry in
 * the response array will be null. ]
 */
export async function xInfoStreamsFull(
  client: RedisClient,
  streams: string[]
): Promise<{ stream: string; response: XInfoStreamFullResponse | null }[]> {
  return await Promise.all(
    streams.map(stream =>
      client
        .sendCommand(['XINFO', 'STREAM', stream, 'FULL', 'COUNT', '1'])
        .catch((error: unknown) => {
          if ((error as Error).message?.includes('ERR no such key')) {
            return null;
          }
          throw error;
        })
        .then(result => {
          const response = result ? parseXInfoStreamFull(result) : null;
          return {
            stream,
            response,
          };
        })
    )
  );
}
