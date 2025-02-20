/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { RedisClientType } from 'redis';
// The redis library defines types in convoluted ways that make referencing them difficult.
// This file is a helper to extract the types we need for our code.

/** Helper function to get the return type of xReadGroup (default overload signature) */
async function xReadGroupType() {
  const xReadGroupFn: RedisClientType['xReadGroup'] = null as any;
  return await xReadGroupFn(null as any, null as any, null as any);
}

export type XReadGroupResponse = Awaited<ReturnType<typeof xReadGroupType>>;

/** Helper function to get the return type for xInfoConsumers (default overload signature)  */
async function xInfoConsumersType() {
  const xInfoConsumersFn: RedisClientType['xInfoConsumers'] = null as any;
  return await xInfoConsumersFn(null as any, null as any);
}
export type XInfoConsumersResponse = Awaited<ReturnType<typeof xInfoConsumersType>>;

/** Helper function to get the return type for the default overload signature of xInfoGroups */
async function xInfoGroupsType() {
  const xInfoGroupsFn: RedisClientType['xInfoGroups'] = null as any;
  return await xInfoGroupsFn(null as any);
}
export type XInfoGroupsResponse = Awaited<ReturnType<typeof xInfoGroupsType>>;
