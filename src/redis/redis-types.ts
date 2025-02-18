/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { RedisClientType } from 'redis';

// The redis library defines types in convoluted ways that make referencing them difficult.
// This file is a helper to extract the types we need for our code.

/** Helper function to get the return type for the default overload signature of xReadGroup */
async function xReadGroupType() {
  const xReadGroupFn: RedisClientType['xReadGroup'] = null as any;
  return await xReadGroupFn(null as any, null as any, null as any);
}

export type XReadGroupReturnType = Awaited<ReturnType<typeof xReadGroupType>>;
