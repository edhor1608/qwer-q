/** Wire protocol version. */
export const PROTOCOL_VERSION = 1;

/** Maximum frame size (16 MB). */
export const MAX_FRAME_SIZE = 16 * 1024 * 1024;

/** Client -> Server opcodes. */
export const OpCode = {
  PUBLISH: 0x01,
  CONSUME: 0x03,
  ACK: 0x05,
  NACK: 0x06,
  EXTEND_VISIBILITY: 0x08,
  SCHEMA_REGISTER: 0x10,
  SCHEMA_GET: 0x11,
  CALL: 0x20,
  SCHEMA_LIST: 0x30,
  QUEUE_LIST: 0x32,
} as const;

/** Server -> Client opcodes. */
export const OpCodeResponse = {
  PUBLISH_ACK: 0x02,
  MESSAGE: 0x04,
  ERROR: 0x07,
  EXTEND_VISIBILITY_ACK: 0x09,
  SCHEMA_RESPONSE: 0x12,
  CALL_RESPONSE: 0x21,
  SCHEMA_LIST_RESP: 0x31,
  QUEUE_LIST_RESP: 0x33,
} as const;

export type OpCodeValue =
  | (typeof OpCode)[keyof typeof OpCode]
  | (typeof OpCodeResponse)[keyof typeof OpCodeResponse];

export interface QwerQOptions {
  host: string;
  port: number;
  /** Auto-reconnect on disconnect. Default: true */
  reconnect?: boolean;
  /** Max reconnect attempts. Default: Infinity */
  maxReconnectAttempts?: number;
  /** Initial reconnect delay in ms. Default: 250 */
  reconnectDelay?: number;
  /** Max reconnect delay in ms. Default: 30000 */
  maxReconnectDelay?: number;
}

export interface PublishOptions {
  headers?: Record<string, string>;
  messageId?: string;
  idempotencyKey?: string;
}

export interface ConsumeOptions {
  prefetch?: number;
  visibilityTimeout?: number;
}

export interface QMessage {
  messageId: string;
  queue: string;
  payload: Buffer;
  headers: Record<string, string>;
  attempt: number;
  publishedAt: number;
}

export interface PublishResult {
  messageId: string;
}

export interface CallOptions {
  headers?: Record<string, string>;
  timeoutMs?: number;
}

export interface CallResult {
  payload: Buffer;
  headers: Record<string, string>;
}

export interface ExtendVisibilityResult {
  newVisibleAt: number;
}

export interface QueueInfo {
  name: string;
  messageCount: number;
  inFlightCount: number;
}

export interface SchemaInfo {
  queue: string;
  messageType: string;
  version: number;
}

export class BrokerError extends Error {
  constructor(
    public readonly code: number,
    message: string,
  ) {
    super(message);
    this.name = "BrokerError";
  }
}
