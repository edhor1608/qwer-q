/**
 * Minimal protobuf encoder/decoder for qwer-q wire messages.
 *
 * Covers: varint, string, bytes, uint32, int64, map<string,string>.
 * No external dependencies.
 *
 * Protobuf wire types:
 *   0 = varint, 2 = length-delimited (string, bytes, submessage)
 */

// --- Varint ---

function encodeVarint(value: number | bigint): Buffer {
  const bytes: number[] = [];
  let v = typeof value === "bigint" ? value : BigInt(value);
  if (v < 0n) {
    // Protobuf uses two's-complement for negative int64
    v = v + (1n << 64n);
  }
  do {
    let b = Number(v & 0x7fn);
    v >>= 7n;
    if (v > 0n) b |= 0x80;
    bytes.push(b);
  } while (v > 0n);
  return Buffer.from(bytes);
}

function decodeVarint(buf: Buffer, offset: number): [bigint, number] {
  let result = 0n;
  let shift = 0n;
  let pos = offset;
  while (pos < buf.length) {
    const b = buf[pos]!;
    result |= BigInt(b & 0x7f) << shift;
    pos++;
    if ((b & 0x80) === 0) break;
    shift += 7n;
  }
  return [result, pos];
}

// --- Tag ---

function encodeTag(fieldNumber: number, wireType: number): Buffer {
  return encodeVarint((fieldNumber << 3) | wireType);
}

// --- Field encoders ---

function encodeStringField(fieldNumber: number, value: string): Buffer {
  if (value === "") return Buffer.alloc(0);
  const tag = encodeTag(fieldNumber, 2);
  const data = Buffer.from(value, "utf-8");
  const len = encodeVarint(data.length);
  return Buffer.concat([tag, len, data]);
}

function encodeBytesField(fieldNumber: number, value: Buffer): Buffer {
  if (value.length === 0) return Buffer.alloc(0);
  const tag = encodeTag(fieldNumber, 2);
  const len = encodeVarint(value.length);
  return Buffer.concat([tag, len, value]);
}

function encodeUint32Field(fieldNumber: number, value: number): Buffer {
  if (value === 0) return Buffer.alloc(0);
  const tag = encodeTag(fieldNumber, 0);
  return Buffer.concat([tag, encodeVarint(value)]);
}

function encodeInt64Field(fieldNumber: number, value: number | bigint): Buffer {
  const v = typeof value === "bigint" ? value : BigInt(value);
  if (v === 0n) return Buffer.alloc(0);
  const tag = encodeTag(fieldNumber, 0);
  return Buffer.concat([tag, encodeVarint(v)]);
}

function encodeMapField(
  fieldNumber: number,
  map: Record<string, string>,
): Buffer {
  const entries = Object.entries(map);
  if (entries.length === 0) return Buffer.alloc(0);

  const parts: Buffer[] = [];
  for (const [key, val] of entries) {
    // Each map entry is a submessage: key=1(string), value=2(string)
    const entryData = Buffer.concat([
      encodeStringField(1, key),
      encodeStringField(2, val),
    ]);
    const tag = encodeTag(fieldNumber, 2);
    const len = encodeVarint(entryData.length);
    parts.push(tag, len, entryData);
  }
  return Buffer.concat(parts);
}

function encodeOptionalStringField(
  fieldNumber: number,
  value: string | undefined,
): Buffer {
  if (value === undefined || value === "") return Buffer.alloc(0);
  return encodeStringField(fieldNumber, value);
}

function encodeBoolField(fieldNumber: number, value: boolean): Buffer {
  if (!value) return Buffer.alloc(0);
  const tag = encodeTag(fieldNumber, 0);
  return Buffer.concat([tag, encodeVarint(1)]);
}

// --- Field decoders ---

interface DecodedFields {
  [fieldNumber: number]: Array<{ wireType: number; data: Buffer | bigint }>;
}

function decodeFields(buf: Buffer): DecodedFields {
  const fields: DecodedFields = {};
  let pos = 0;

  while (pos < buf.length) {
    const [tagValue, newPos] = decodeVarint(buf, pos);
    pos = newPos;
    const fieldNumber = Number(tagValue >> 3n);
    const wireType = Number(tagValue & 7n);

    if (!fields[fieldNumber]) fields[fieldNumber] = [];

    switch (wireType) {
      case 0: {
        // varint
        const [value, nextPos] = decodeVarint(buf, pos);
        pos = nextPos;
        fields[fieldNumber]!.push({ wireType, data: value });
        break;
      }
      case 2: {
        // length-delimited
        const [length, lenPos] = decodeVarint(buf, pos);
        pos = lenPos;
        const len = Number(length);
        const data = Buffer.from(buf.subarray(pos, pos + len));
        pos += len;
        fields[fieldNumber]!.push({ wireType, data });
        break;
      }
      case 5: {
        // 32-bit fixed
        const data = BigInt(buf.readUInt32LE(pos));
        pos += 4;
        fields[fieldNumber]!.push({ wireType, data });
        break;
      }
      case 1: {
        // 64-bit fixed
        const data = buf.readBigUInt64LE(pos);
        pos += 8;
        fields[fieldNumber]!.push({ wireType, data });
        break;
      }
      default:
        throw new Error(`Unsupported wire type: ${wireType}`);
    }
  }
  return fields;
}

function getString(fields: DecodedFields, fieldNumber: number): string {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return "";
  const entry = entries[0]!;
  if (entry.data instanceof Buffer) return entry.data.toString("utf-8");
  return "";
}

function getBytes(fields: DecodedFields, fieldNumber: number): Buffer {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return Buffer.alloc(0);
  const entry = entries[0]!;
  if (entry.data instanceof Buffer) return entry.data;
  return Buffer.alloc(0);
}

function getUint32(fields: DecodedFields, fieldNumber: number): number {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return 0;
  const entry = entries[0]!;
  if (typeof entry.data === "bigint") return Number(entry.data);
  return 0;
}

function getInt64(fields: DecodedFields, fieldNumber: number): number {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return 0;
  const entry = entries[0]!;
  if (typeof entry.data === "bigint") return Number(entry.data);
  return 0;
}

function getBool(fields: DecodedFields, fieldNumber: number): boolean {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return false;
  const entry = entries[0]!;
  if (typeof entry.data === "bigint") return entry.data !== 0n;
  return false;
}

function getMap(
  fields: DecodedFields,
  fieldNumber: number,
): Record<string, string> {
  const entries = fields[fieldNumber];
  if (!entries || entries.length === 0) return {};
  const result: Record<string, string> = {};
  for (const entry of entries) {
    if (entry.data instanceof Buffer) {
      const sub = decodeFields(entry.data);
      const key = getString(sub, 1);
      const val = getString(sub, 2);
      if (key) result[key] = val;
    }
  }
  return result;
}

function getSubmessages(
  fields: DecodedFields,
  fieldNumber: number,
): Buffer[] {
  const entries = fields[fieldNumber];
  if (!entries) return [];
  return entries
    .filter((e) => e.data instanceof Buffer)
    .map((e) => e.data as Buffer);
}

// --- Public encode/decode for each protobuf message ---

/** PublishRequest: queue=1, payload=2, headers=3, message_id=4, idempotency_key=5 */
export function encodePublishRequest(
  queue: string,
  payload: Buffer,
  headers?: Record<string, string>,
  messageId?: string,
  idempotencyKey?: string,
): Buffer {
  return Buffer.concat([
    encodeStringField(1, queue),
    encodeBytesField(2, payload),
    headers ? encodeMapField(3, headers) : Buffer.alloc(0),
    encodeOptionalStringField(4, messageId),
    encodeOptionalStringField(5, idempotencyKey),
  ]);
}

/** PublishResponse: message_id=1 */
export function decodePublishResponse(buf: Buffer): { messageId: string } {
  const fields = decodeFields(buf);
  return { messageId: getString(fields, 1) };
}

/** ConsumeRequest: queue=1, prefetch=2, visibility_timeout=3 */
export function encodeConsumeRequest(
  queue: string,
  prefetch?: number,
  visibilityTimeout?: number,
): Buffer {
  return Buffer.concat([
    encodeStringField(1, queue),
    encodeUint32Field(2, prefetch ?? 0),
    encodeUint32Field(3, visibilityTimeout ?? 0),
  ]);
}

/** Message: message_id=1, queue=2, payload=3, headers=4, attempt=5, published_at=6 */
export function decodeMessage(buf: Buffer): {
  messageId: string;
  queue: string;
  payload: Buffer;
  headers: Record<string, string>;
  attempt: number;
  publishedAt: number;
} {
  const fields = decodeFields(buf);
  return {
    messageId: getString(fields, 1),
    queue: getString(fields, 2),
    payload: getBytes(fields, 3),
    headers: getMap(fields, 4),
    attempt: getUint32(fields, 5),
    publishedAt: getInt64(fields, 6),
  };
}

/** AckRequest: message_id=1 */
export function encodeAckRequest(messageId: string): Buffer {
  return encodeStringField(1, messageId);
}

/** NackRequest: message_id=1, requeue=2 */
export function encodeNackRequest(messageId: string, requeue: boolean): Buffer {
  return Buffer.concat([
    encodeStringField(1, messageId),
    encodeBoolField(2, requeue),
  ]);
}

/** ErrorResponse: code=1, message=2 */
export function decodeErrorResponse(buf: Buffer): {
  code: number;
  message: string;
} {
  const fields = decodeFields(buf);
  return {
    code: getUint32(fields, 1),
    message: getString(fields, 2),
  };
}

/** ExtendVisibilityRequest: message_id=1, extension_seconds=2 */
export function encodeExtendVisibilityRequest(
  messageId: string,
  extensionSeconds: number,
): Buffer {
  return Buffer.concat([
    encodeStringField(1, messageId),
    encodeUint32Field(2, extensionSeconds),
  ]);
}

/** ExtendVisibilityResponse: new_visible_at=1 */
export function decodeExtendVisibilityResponse(buf: Buffer): {
  newVisibleAt: number;
} {
  const fields = decodeFields(buf);
  return { newVisibleAt: getInt64(fields, 1) };
}

/** CallRequest: queue=1, payload=2, headers=3, timeout_ms=4 */
export function encodeCallRequest(
  queue: string,
  payload: Buffer,
  headers?: Record<string, string>,
  timeoutMs?: number,
): Buffer {
  return Buffer.concat([
    encodeStringField(1, queue),
    encodeBytesField(2, payload),
    headers ? encodeMapField(3, headers) : Buffer.alloc(0),
    encodeUint32Field(4, timeoutMs ?? 0),
  ]);
}

/** CallResponse: payload=1, headers=2 */
export function decodeCallResponse(buf: Buffer): {
  payload: Buffer;
  headers: Record<string, string>;
} {
  const fields = decodeFields(buf);
  return {
    payload: getBytes(fields, 1),
    headers: getMap(fields, 2),
  };
}

/** QueueListRequest: empty message */
export function encodeQueueListRequest(): Buffer {
  return Buffer.alloc(0);
}

/** QueueListResponse: queues=1 (repeated QueueInfo) */
export function decodeQueueListResponse(buf: Buffer): Array<{
  name: string;
  messageCount: number;
  inFlightCount: number;
}> {
  const fields = decodeFields(buf);
  const subs = getSubmessages(fields, 1);
  return subs.map((sub) => {
    const f = decodeFields(sub);
    return {
      name: getString(f, 1),
      messageCount: getUint32(f, 2),
      inFlightCount: getUint32(f, 3),
    };
  });
}

/** SchemaListRequest: empty message */
export function encodeSchemaListRequest(): Buffer {
  return Buffer.alloc(0);
}

/** SchemaListResponse: schemas=1 (repeated SchemaInfo) */
export function decodeSchemaListResponse(buf: Buffer): Array<{
  queue: string;
  messageType: string;
  version: number;
}> {
  const fields = decodeFields(buf);
  const subs = getSubmessages(fields, 1);
  return subs.map((sub) => {
    const f = decodeFields(sub);
    return {
      queue: getString(f, 1),
      messageType: getString(f, 2),
      version: getUint32(f, 3),
    };
  });
}

/** SchemaRegisterRequest: queue=1, descriptor=2, message_type=3 */
export function encodeSchemaRegisterRequest(
  queue: string,
  descriptor: Buffer,
  messageType: string,
): Buffer {
  return Buffer.concat([
    encodeStringField(1, queue),
    encodeBytesField(2, descriptor),
    encodeStringField(3, messageType),
  ]);
}

/** SchemaRegisterResponse: schema_id=1, version=2 */
export function decodeSchemaRegisterResponse(buf: Buffer): {
  schemaId: number;
  version: number;
} {
  const fields = decodeFields(buf);
  return {
    schemaId: getUint32(fields, 1),
    version: getUint32(fields, 2),
  };
}

// Re-export internal helpers for testing
export { encodeVarint, decodeVarint, decodeFields, encodeTag };
