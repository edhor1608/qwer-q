import * as net from "node:net";
import { EventEmitter } from "node:events";
import { FrameDecoder, encodeFrame, type Frame } from "./protocol.js";
import {
  OpCode,
  OpCodeResponse,
  BrokerError,
  type OpCodeValue,
  type QwerQOptions,
  type QMessage,
  type PublishResult,
  type PublishOptions,
  type ConsumeOptions,
  type CallOptions,
  type CallResult,
  type ExtendVisibilityResult,
  type QueueInfo,
  type SchemaInfo,
} from "./types.js";
import {
  encodePublishRequest,
  decodePublishResponse,
  encodeConsumeRequest,
  decodeMessage,
  encodeAckRequest,
  encodeNackRequest,
  decodeErrorResponse,
  encodeExtendVisibilityRequest,
  decodeExtendVisibilityResponse,
  encodeCallRequest,
  decodeCallResponse,
  encodeQueueListRequest,
  decodeQueueListResponse,
  encodeSchemaListRequest,
  decodeSchemaListResponse,
  encodeSchemaRegisterRequest,
  decodeSchemaRegisterResponse,
} from "./codec.js";

export interface QwerQConnectionEvents {
  connect: [];
  disconnect: [error?: Error];
  reconnecting: [attempt: number];
  error: [error: Error];
  message: [message: QMessage];
}

interface PendingRequest {
  resolve: (frame: Frame) => void;
  reject: (error: Error) => void;
  expectedOpcode: OpCodeValue;
}

const DEFAULT_OPTIONS: Required<Omit<QwerQOptions, "host" | "port">> = {
  reconnect: true,
  maxReconnectAttempts: Infinity,
  reconnectDelay: 250,
  maxReconnectDelay: 30_000,
};

export class QwerQConnection extends EventEmitter<QwerQConnectionEvents> {
  private socket: net.Socket | null = null;
  private decoder = new FrameDecoder();
  private opts: Required<QwerQOptions>;
  private connected = false;
  private closing = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Queue of pending request-response pairs.
   * The broker processes commands sequentially per connection, so we use a FIFO queue.
   * MESSAGE frames (from consume) are handled separately since they arrive asynchronously.
   */
  private pending: PendingRequest[] = [];

  /** Message handler for consume mode. */
  private messageHandler: ((msg: QMessage) => void) | null = null;

  /** The queue name this connection is consuming from (if any). */
  private consumeQueue: string | null = null;

  constructor(options: QwerQOptions) {
    super();
    this.opts = { ...DEFAULT_OPTIONS, ...options } as Required<QwerQOptions>;
  }

  /** Connect to the broker. Resolves when the TCP connection is established. */
  async connect(): Promise<void> {
    if (this.connected) return;
    this.closing = false;
    return this._connect();
  }

  private _connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({
        host: this.opts.host,
        port: this.opts.port,
      });

      socket.on("connect", () => {
        this.socket = socket;
        this.connected = true;
        this.reconnectAttempt = 0;
        this.decoder.reset();
        this.emit("connect");
        resolve();
      });

      socket.on("data", (chunk: Buffer) => {
        try {
          this.decoder.push(chunk);
          const frames = this.decoder.readAll();
          for (const frame of frames) {
            this._handleFrame(frame);
          }
        } catch (err) {
          this.emit("error", err as Error);
          this.socket?.destroy();
        }
      });

      socket.on("error", (err: Error) => {
        this.emit("error", err);
        if (!this.connected) {
          reject(err);
        }
      });

      socket.on("close", () => {
        const wasConnected = this.connected;
        this.connected = false;
        this.socket = null;

        // Reject all pending requests
        const pendingCopy = this.pending.splice(0);
        for (const req of pendingCopy) {
          req.reject(new Error("Connection closed"));
        }

        if (wasConnected) {
          this.emit("disconnect");
        }

        if (!this.closing && this.opts.reconnect) {
          this._scheduleReconnect();
        }
      });
    });
  }

  private _handleFrame(frame: Frame): void {
    // MESSAGE frames are delivered asynchronously (from consume)
    if (frame.opcode === OpCodeResponse.MESSAGE) {
      const msg = decodeMessage(frame.payload);
      const qMessage: QMessage = {
        messageId: msg.messageId,
        queue: msg.queue,
        payload: msg.payload,
        headers: msg.headers,
        attempt: msg.attempt,
        publishedAt: msg.publishedAt,
      };
      if (this.messageHandler) {
        this.messageHandler(qMessage);
      }
      this.emit("message", qMessage);
      return;
    }

    // ERROR frames can be responses to pending requests
    if (frame.opcode === OpCodeResponse.ERROR) {
      const err = decodeErrorResponse(frame.payload);
      const pending = this.pending.shift();
      if (pending) {
        pending.reject(new BrokerError(err.code, err.message));
      } else {
        this.emit("error", new BrokerError(err.code, err.message));
      }
      return;
    }

    // Match response to the first pending request
    const pending = this.pending.shift();
    if (pending) {
      if (frame.opcode === pending.expectedOpcode) {
        pending.resolve(frame);
      } else {
        pending.reject(
          new Error(
            `Unexpected opcode: expected 0x${pending.expectedOpcode.toString(16)}, got 0x${frame.opcode.toString(16)}`,
          ),
        );
      }
    }
  }

  private _scheduleReconnect(): void {
    if (this.closing) return;
    if (this.reconnectAttempt >= this.opts.maxReconnectAttempts) {
      this.emit("error", new Error("Max reconnect attempts reached"));
      return;
    }

    this.reconnectAttempt++;
    const delay = Math.min(
      this.opts.reconnectDelay * Math.pow(2, this.reconnectAttempt - 1),
      this.opts.maxReconnectDelay,
    );

    // Add jitter (+-25%)
    const jitter = delay * (0.75 + Math.random() * 0.5);

    this.emit("reconnecting", this.reconnectAttempt);

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this._connect();
        // Re-subscribe if we were consuming
        if (this.consumeQueue && this.messageHandler) {
          await this._sendConsume(this.consumeQueue, this._consumeOptions);
        }
      } catch {
        // Will be handled by the close event triggering another reconnect
      }
    }, jitter);
  }

  private _consumeOptions: ConsumeOptions = {};

  /** Send a frame and wait for the expected response opcode. */
  private _sendAndWait(
    opcode: OpCodeValue,
    payload: Buffer,
    expectedOpcode: OpCodeValue,
  ): Promise<Frame> {
    return new Promise((resolve, reject) => {
      if (!this.socket || !this.connected) {
        reject(new Error("Not connected"));
        return;
      }

      this.pending.push({ resolve, reject, expectedOpcode });

      const frame = encodeFrame(opcode, payload);
      this.socket.write(frame, (err) => {
        if (err) {
          // Mark as failed — will be skipped by _handleFrame
          const idx = this.pending.findIndex((p) => p.resolve === resolve);
          if (idx !== -1) this.pending.splice(idx, 1);
          reject(err);
          // If this was the only pending, no FIFO issue.
          // If there are others, the connection is likely dead anyway.
        }
      });
    });
  }

  /** Send a frame without waiting for a response (fire-and-forget). */
  private _send(opcode: OpCodeValue, payload: Buffer): void {
    if (!this.socket || !this.connected) {
      throw new Error("Not connected");
    }
    const frame = encodeFrame(opcode, payload);
    this.socket.write(frame);
  }

  // --- Public API ---

  /** Publish a message to a queue. */
  async publish(
    queue: string,
    payload: Buffer,
    options?: PublishOptions,
  ): Promise<PublishResult> {
    const encoded = encodePublishRequest(
      queue,
      payload,
      options?.headers,
      options?.messageId,
      options?.idempotencyKey,
    );
    const frame = await this._sendAndWait(
      OpCode.PUBLISH,
      encoded,
      OpCodeResponse.PUBLISH_ACK,
    );
    return decodePublishResponse(frame.payload);
  }

  /**
   * Subscribe to a queue. The handler is called for each delivered message.
   * Only one subscription per connection (matches the Go broker behavior).
   */
  async subscribe(
    queue: string,
    handler: (msg: QMessage) => void | Promise<void>,
    options?: ConsumeOptions,
  ): Promise<void> {
    this.messageHandler = handler;
    this.consumeQueue = queue;
    this._consumeOptions = options ?? {};
    await this._sendConsume(queue, options);
  }

  private async _sendConsume(
    queue: string,
    options?: ConsumeOptions,
  ): Promise<void> {
    if (!this.socket || !this.connected) {
      throw new Error("Not connected");
    }
    const encoded = encodeConsumeRequest(
      queue,
      options?.prefetch,
      options?.visibilityTimeout,
    );
    // Consume has no immediate response - the broker starts pushing MESSAGE frames
    const frame = encodeFrame(OpCode.CONSUME, encoded);
    this.socket.write(frame);
  }

  /** Acknowledge a message (fire-and-forget). */
  ack(messageId: string): void {
    const encoded = encodeAckRequest(messageId);
    this._send(OpCode.ACK, encoded);
  }

  /** Negatively acknowledge a message (fire-and-forget). */
  nack(messageId: string, requeue = true): void {
    const encoded = encodeNackRequest(messageId, requeue);
    this._send(OpCode.NACK, encoded);
  }

  /** Extend the visibility timeout for an in-flight message. */
  async extendVisibility(
    messageId: string,
    extensionSeconds: number,
  ): Promise<ExtendVisibilityResult> {
    const encoded = encodeExtendVisibilityRequest(messageId, extensionSeconds);
    const frame = await this._sendAndWait(
      OpCode.EXTEND_VISIBILITY,
      encoded,
      OpCodeResponse.EXTEND_VISIBILITY_ACK,
    );
    return decodeExtendVisibilityResponse(frame.payload);
  }

  /** RPC-style call: publish to a queue and wait for a response. */
  async call(
    queue: string,
    payload: Buffer,
    options?: CallOptions,
  ): Promise<CallResult> {
    const encoded = encodeCallRequest(
      queue,
      payload,
      options?.headers,
      options?.timeoutMs,
    );
    const frame = await this._sendAndWait(
      OpCode.CALL,
      encoded,
      OpCodeResponse.CALL_RESPONSE,
    );
    return decodeCallResponse(frame.payload);
  }

  /** List all queues. */
  async listQueues(): Promise<QueueInfo[]> {
    const encoded = encodeQueueListRequest();
    const frame = await this._sendAndWait(
      OpCode.QUEUE_LIST,
      encoded,
      OpCodeResponse.QUEUE_LIST_RESP,
    );
    return decodeQueueListResponse(frame.payload);
  }

  /** List all registered schemas. */
  async listSchemas(): Promise<SchemaInfo[]> {
    const encoded = encodeSchemaListRequest();
    const frame = await this._sendAndWait(
      OpCode.SCHEMA_LIST,
      encoded,
      OpCodeResponse.SCHEMA_LIST_RESP,
    );
    return decodeSchemaListResponse(frame.payload);
  }

  /** Register a schema for a queue. */
  async registerSchema(
    queue: string,
    descriptor: Buffer,
    messageType: string,
  ): Promise<{ schemaId: number; version: number }> {
    const encoded = encodeSchemaRegisterRequest(queue, descriptor, messageType);
    const frame = await this._sendAndWait(
      OpCode.SCHEMA_REGISTER,
      encoded,
      OpCodeResponse.SCHEMA_RESPONSE,
    );
    return decodeSchemaRegisterResponse(frame.payload);
  }

  /** Whether the connection is currently established. */
  get isConnected(): boolean {
    return this.connected;
  }

  /** Close the connection. */
  async close(): Promise<void> {
    this.closing = true;
    this.messageHandler = null;
    this.consumeQueue = null;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    // Reject all pending
    const pendingCopy = this.pending.splice(0);
    for (const req of pendingCopy) {
      req.reject(new Error("Connection closing"));
    }

    return new Promise((resolve) => {
      if (!this.socket) {
        resolve();
        return;
      }
      this.socket.once("close", () => resolve());
      this.socket.end();
    });
  }
}
