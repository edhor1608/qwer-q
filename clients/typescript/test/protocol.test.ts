import { describe, it, expect } from "vitest";
import { encodeFrame, FrameDecoder } from "../src/protocol.js";
import { OpCode, OpCodeResponse, PROTOCOL_VERSION } from "../src/types.js";

describe("encodeFrame", () => {
  it("encodes a frame with correct wire format", () => {
    const payload = Buffer.from([0xaa, 0xbb, 0xcc]);
    const frame = encodeFrame(OpCode.PUBLISH, payload);

    // Length = 2 (version + opcode) + 3 (payload) = 5
    expect(frame.readUInt32BE(0)).toBe(5);
    expect(frame[4]).toBe(PROTOCOL_VERSION);
    expect(frame[5]).toBe(OpCode.PUBLISH);
    expect(frame.subarray(6)).toEqual(payload);
    expect(frame.length).toBe(4 + 5);
  });

  it("encodes an empty payload frame", () => {
    const frame = encodeFrame(OpCode.ACK, Buffer.alloc(0));
    expect(frame.readUInt32BE(0)).toBe(2); // version + opcode only
    expect(frame.length).toBe(6);
  });
});

describe("FrameDecoder", () => {
  it("decodes a complete frame in one chunk", () => {
    const payload = Buffer.from("hello");
    const encoded = encodeFrame(OpCode.PUBLISH, payload);

    const decoder = new FrameDecoder();
    decoder.push(encoded);
    const frame = decoder.read();

    expect(frame).not.toBeNull();
    expect(frame!.version).toBe(PROTOCOL_VERSION);
    expect(frame!.opcode).toBe(OpCode.PUBLISH);
    expect(frame!.payload).toEqual(payload);
  });

  it("handles partial reads (split across chunks)", () => {
    const payload = Buffer.from("world");
    const encoded = encodeFrame(OpCode.CONSUME, payload);

    const decoder = new FrameDecoder();

    // Feed first 3 bytes (partial length header)
    decoder.push(encoded.subarray(0, 3));
    expect(decoder.read()).toBeNull();

    // Feed up to the middle of payload
    decoder.push(encoded.subarray(3, 8));
    expect(decoder.read()).toBeNull();

    // Feed the rest
    decoder.push(encoded.subarray(8));
    const frame = decoder.read();

    expect(frame).not.toBeNull();
    expect(frame!.opcode).toBe(OpCode.CONSUME);
    expect(frame!.payload).toEqual(payload);
  });

  it("decodes multiple frames from a single chunk", () => {
    const payload1 = Buffer.from("first");
    const payload2 = Buffer.from("second");
    const encoded = Buffer.concat([
      encodeFrame(OpCode.PUBLISH, payload1),
      encodeFrame(OpCodeResponse.PUBLISH_ACK, payload2),
    ]);

    const decoder = new FrameDecoder();
    decoder.push(encoded);
    const frames = decoder.readAll();

    expect(frames).toHaveLength(2);
    expect(frames[0]!.opcode).toBe(OpCode.PUBLISH);
    expect(frames[0]!.payload).toEqual(payload1);
    expect(frames[1]!.opcode).toBe(OpCodeResponse.PUBLISH_ACK);
    expect(frames[1]!.payload).toEqual(payload2);
  });

  it("throws on frame exceeding max size", () => {
    // Create a fake length header with a huge value
    const badFrame = Buffer.alloc(4);
    badFrame.writeUInt32BE(0x01000001, 0); // 16MB + 1

    const decoder = new FrameDecoder();
    decoder.push(badFrame);
    expect(() => decoder.read()).toThrow("Frame too large");
  });

  it("throws on frame too small", () => {
    const badFrame = Buffer.alloc(4);
    badFrame.writeUInt32BE(1, 0); // length=1, too small for version+opcode

    const decoder = new FrameDecoder();
    decoder.push(Buffer.concat([badFrame, Buffer.from([0x01])]));
    expect(() => decoder.read()).toThrow("Frame too small");
  });

  it("handles byte-at-a-time feeding", () => {
    const payload = Buffer.from("test");
    const encoded = encodeFrame(OpCode.ACK, payload);

    const decoder = new FrameDecoder();
    for (let i = 0; i < encoded.length - 1; i++) {
      decoder.push(encoded.subarray(i, i + 1));
      expect(decoder.read()).toBeNull();
    }
    decoder.push(encoded.subarray(encoded.length - 1));
    const frame = decoder.read();
    expect(frame).not.toBeNull();
    expect(frame!.payload).toEqual(payload);
  });

  it("reset clears buffer", () => {
    const decoder = new FrameDecoder();
    decoder.push(Buffer.from([0x00, 0x00, 0x00])); // partial
    decoder.reset();
    expect(decoder.read()).toBeNull();

    // Should work normally after reset
    const payload = Buffer.from("ok");
    decoder.push(encodeFrame(OpCode.PUBLISH, payload));
    const frame = decoder.read();
    expect(frame).not.toBeNull();
    expect(frame!.payload).toEqual(payload);
  });
});

describe("frame roundtrip", () => {
  it("encode -> decode is identity for all opcodes", () => {
    const allOpcodes = [
      ...Object.values(OpCode),
      ...Object.values(OpCodeResponse),
    ];

    for (const opcode of allOpcodes) {
      const payload = Buffer.from(`payload-for-${opcode}`);
      const encoded = encodeFrame(opcode, payload);

      const decoder = new FrameDecoder();
      decoder.push(encoded);
      const frame = decoder.read();

      expect(frame, `roundtrip failed for opcode 0x${opcode.toString(16)}`).not.toBeNull();
      expect(frame!.opcode).toBe(opcode);
      expect(frame!.payload).toEqual(payload);
    }
  });
});
