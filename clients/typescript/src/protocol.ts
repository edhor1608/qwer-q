import { PROTOCOL_VERSION, MAX_FRAME_SIZE, type OpCodeValue } from "./types.js";

/**
 * A decoded wire protocol frame.
 * Format: [4-byte length (big-endian)][1-byte version][1-byte opcode][payload]
 * Length = version + opcode + payload (excludes the 4-byte length field itself).
 */
export interface Frame {
  version: number;
  opcode: OpCodeValue;
  payload: Buffer;
}

/** Encode a frame to wire format. */
export function encodeFrame(opcode: OpCodeValue, payload: Buffer): Buffer {
  const length = 2 + payload.length; // version + opcode + payload
  const frame = Buffer.allocUnsafe(4 + length);
  frame.writeUInt32BE(length, 0);
  frame[4] = PROTOCOL_VERSION;
  frame[5] = opcode;
  payload.copy(frame, 6);
  return frame;
}

/**
 * Frame decoder that handles partial TCP reads.
 * Feed it chunks via push(), get complete frames from read().
 */
export class FrameDecoder {
  private buffer = Buffer.alloc(0);

  /** Append data from the socket. */
  push(chunk: Buffer): void {
    this.buffer =
      this.buffer.length === 0
        ? Buffer.from(chunk)
        : Buffer.concat([this.buffer, chunk]) as Buffer<ArrayBuffer>;
  }

  /** Try to read one complete frame. Returns null if not enough data. */
  read(): Frame | null {
    // Need at least 4 bytes for length
    if (this.buffer.length < 4) return null;

    const length = this.buffer.readUInt32BE(0);

    if (length > MAX_FRAME_SIZE) {
      throw new Error(`Frame too large: ${length} bytes (max ${MAX_FRAME_SIZE})`);
    }
    if (length < 2) {
      throw new Error("Frame too small for header");
    }

    const totalSize = 4 + length;
    if (this.buffer.length < totalSize) return null;

    const version = this.buffer[4]!;
    const opcode = this.buffer[5]! as OpCodeValue;
    const payload = Buffer.from(this.buffer.subarray(6, totalSize));

    // Advance buffer past this frame
    this.buffer = this.buffer.subarray(totalSize);

    return { version, opcode, payload };
  }

  /** Read all complete frames currently buffered. */
  readAll(): Frame[] {
    const frames: Frame[] = [];
    let frame: Frame | null;
    while ((frame = this.read()) !== null) {
      frames.push(frame);
    }
    return frames;
  }

  /** Reset the decoder, discarding any buffered data. */
  reset(): void {
    this.buffer = Buffer.alloc(0);
  }
}
