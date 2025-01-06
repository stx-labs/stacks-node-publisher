import { createClient, RedisClientType } from "redis";

export type StreamedStacksEventCallback = (
  id: string,
  timestamp: number,
  path: string,
  payload: any
) => Promise<void>;

const STREAM = "stacks";

export class StacksEventStream {
  private readonly client: RedisClientType;
  private readonly group: string;
  private readonly consumer: string;
  private readonly messageId: string;
  private readonly abort: AbortController;

  constructor(redisUrl: string, group: string, messageId: string = "0-0") {
    this.client = createClient({ url: redisUrl });
    this.group = group;
    this.consumer = "test";
    this.messageId = messageId;
    this.abort = new AbortController();
  }

  async start(callback: StreamedStacksEventCallback) {
    try {
      const client = await this.client.connect();
      while (!this.abort.signal.aborted) {
        const results = await client.xReadGroup(
          this.group,
          this.consumer,
          [{ key: STREAM, id: this.messageId }],
          {
            COUNT: 1,
            BLOCK: 1000, // Wait at most 1 second for new messages
          }
        );
        if (results && results.length > 0) {
          for (const stream of results) {
            for (const message of stream.messages) {
              console.log("Stream message:", message);
              const timestamp = message.timestamp as number;
              const path = message.path as string;
              const payload = message.payload;

              await callback(message.id, timestamp, path, payload);
              await client.xAck(STREAM, this.group, message.id);
              console.log("Message acknowledged:", message.id);
            }
          }
        }
      }
    } catch (error) {
      console.error("Error reading or acknowledging from stream:", error);
    }
  }

  async stop() {
    this.abort.abort();
    await this.client.disconnect();
  }
}
