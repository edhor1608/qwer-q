import { QwerQConnection } from "./connection.js";
import type {
  QMessage,
  PublishOptions,
  PublishResult,
  ConsumeOptions,
} from "./types.js";

/**
 * A message with a typed, deserialized payload.
 */
export interface TypedMessage<T> extends Omit<QMessage, "payload"> {
  data: T;
  rawPayload: Buffer;
}

/**
 * Typed wrappers around QwerQConnection for JSON payloads.
 * Provides generic publish<T>() and subscribe<T>() with optional validation.
 */
export class TypedClient {
  constructor(private conn: QwerQConnection) {}

  /** Publish a JSON-serializable value to a queue. */
  async publish<T>(
    queue: string,
    data: T,
    options?: PublishOptions,
  ): Promise<PublishResult> {
    const payload = Buffer.from(JSON.stringify(data), "utf-8");
    return this.conn.publish(queue, payload, options);
  }

  /**
   * Subscribe to a queue with automatic JSON deserialization.
   *
   * @param validate Optional validation function. If it throws or returns false,
   *   the message is nacked with requeue=false (sent to DLQ).
   */
  async subscribe<T>(
    queue: string,
    handler: (msg: TypedMessage<T>) => void | Promise<void>,
    options?: ConsumeOptions & {
      validate?: (data: unknown) => data is T;
    },
  ): Promise<void> {
    const validate = options?.validate;

    await this.conn.subscribe(
      queue,
      async (msg) => {
        let data: T;
        try {
          data = JSON.parse(msg.payload.toString("utf-8")) as T;
        } catch {
          // Unparseable JSON - nack to DLQ
          this.conn.nack(msg.messageId, false);
          return;
        }

        if (validate) {
          try {
            if (!validate(data)) {
              this.conn.nack(msg.messageId, false);
              return;
            }
          } catch {
            this.conn.nack(msg.messageId, false);
            return;
          }
        }

        const typed: TypedMessage<T> = {
          messageId: msg.messageId,
          queue: msg.queue,
          data,
          rawPayload: msg.payload,
          headers: msg.headers,
          attempt: msg.attempt,
          publishedAt: msg.publishedAt,
        };

        await handler(typed);
        this.conn.ack(msg.messageId);
      },
      options,
    );
  }

  /** Access the underlying connection. */
  get connection(): QwerQConnection {
    return this.conn;
  }
}
