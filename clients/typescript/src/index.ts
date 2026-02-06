// Core connection
export { QwerQConnection } from "./connection.js";
export type { QwerQConnectionEvents } from "./connection.js";

// Typed wrappers
export { TypedClient } from "./typed.js";
export type { TypedMessage } from "./typed.js";

// Protocol internals (for advanced usage)
export { encodeFrame, FrameDecoder } from "./protocol.js";
export type { Frame } from "./protocol.js";

// Types
export {
  OpCode,
  OpCodeResponse,
  PROTOCOL_VERSION,
  MAX_FRAME_SIZE,
  BrokerError,
} from "./types.js";
export type {
  OpCodeValue,
  QwerQOptions,
  PublishOptions,
  ConsumeOptions,
  CallOptions,
  CallResult,
  QMessage,
  PublishResult,
  ExtendVisibilityResult,
  QueueInfo,
  SchemaInfo,
} from "./types.js";
