import { describe, it, expect } from "vitest";
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
  encodeVarint,
  decodeVarint,
  decodeFields,
  encodeTag,
} from "../src/codec.js";

describe("varint encoding", () => {
  it("encodes small values", () => {
    const buf = encodeVarint(1);
    expect(buf).toEqual(Buffer.from([0x01]));
  });

  it("encodes 0", () => {
    // Note: encodeVarint(0) produces [0x00] which is correct
    const buf = encodeVarint(0);
    expect(buf).toEqual(Buffer.from([0x00]));
  });

  it("encodes multi-byte values", () => {
    // 300 = 0b100101100 -> [0xAC, 0x02]
    const buf = encodeVarint(300);
    expect(buf).toEqual(Buffer.from([0xac, 0x02]));
  });

  it("roundtrips through decode", () => {
    const values = [0, 1, 127, 128, 255, 256, 300, 16383, 16384, 65535, 100000];
    for (const v of values) {
      const encoded = encodeVarint(v);
      const [decoded, pos] = decodeVarint(encoded, 0);
      expect(Number(decoded), `roundtrip failed for ${v}`).toBe(v);
      expect(pos).toBe(encoded.length);
    }
  });

  it("roundtrips large int64 values", () => {
    const v = 1706745600000; // Unix millis
    const encoded = encodeVarint(v);
    const [decoded] = decodeVarint(encoded, 0);
    expect(Number(decoded)).toBe(v);
  });
});

describe("PublishRequest encoding", () => {
  it("encodes basic publish request", () => {
    const payload = Buffer.from("hello");
    const encoded = encodePublishRequest("test-queue", payload);

    // Should be valid protobuf - decode the fields back
    const fields = decodeFields(encoded);
    // field 1 = queue (string)
    expect(fields[1]![0]!.data).toBeInstanceOf(Buffer);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("test-queue");
    // field 2 = payload (bytes)
    expect(fields[2]![0]!.data).toBeInstanceOf(Buffer);
    expect(fields[2]![0]!.data as Buffer).toEqual(payload);
  });

  it("encodes with headers", () => {
    const encoded = encodePublishRequest(
      "q",
      Buffer.from("data"),
      { "x-trace": "abc123" },
    );
    const fields = decodeFields(encoded);
    // field 3 = headers (map entries)
    expect(fields[3]).toBeDefined();
    expect(fields[3]!.length).toBe(1);
    // Each map entry is a submessage with key=1, value=2
    const entry = decodeFields(fields[3]![0]!.data as Buffer);
    expect((entry[1]![0]!.data as Buffer).toString()).toBe("x-trace");
    expect((entry[2]![0]!.data as Buffer).toString()).toBe("abc123");
  });

  it("encodes with optional fields", () => {
    const encoded = encodePublishRequest(
      "q",
      Buffer.from("x"),
      undefined,
      "msg-123",
      "idem-456",
    );
    const fields = decodeFields(encoded);
    expect((fields[4]![0]!.data as Buffer).toString()).toBe("msg-123");
    expect((fields[5]![0]!.data as Buffer).toString()).toBe("idem-456");
  });
});

describe("PublishResponse decoding", () => {
  it("decodes message_id from protobuf bytes", () => {
    // Manually encode: field 1 (string) = "msg-abc"
    const tag = encodeTag(1, 2);
    const data = Buffer.from("msg-abc");
    const len = encodeVarint(data.length);
    const encoded = Buffer.concat([tag, len, data]);

    const result = decodePublishResponse(encoded);
    expect(result.messageId).toBe("msg-abc");
  });
});

describe("ConsumeRequest encoding", () => {
  it("encodes with defaults (omitting zero values)", () => {
    const encoded = encodeConsumeRequest("my-queue");
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("my-queue");
    // prefetch and visibility_timeout should be absent (zero values not encoded)
    expect(fields[2]).toBeUndefined();
    expect(fields[3]).toBeUndefined();
  });

  it("encodes with prefetch and visibility timeout", () => {
    const encoded = encodeConsumeRequest("q", 10, 60);
    const fields = decodeFields(encoded);
    expect(Number(fields[2]![0]!.data as bigint)).toBe(10);
    expect(Number(fields[3]![0]!.data as bigint)).toBe(60);
  });
});

describe("Message decoding", () => {
  it("decodes a complete message", () => {
    // Build a protobuf Message by hand
    const tag1 = Buffer.concat([encodeTag(1, 2), encodeVarint(5), Buffer.from("msg01")]);
    const tag2 = Buffer.concat([encodeTag(2, 2), encodeVarint(6), Buffer.from("queue1")]);
    const payload = Buffer.from("hello");
    const tag3 = Buffer.concat([encodeTag(3, 2), encodeVarint(payload.length), payload]);
    // headers: map entry
    const mapKey = Buffer.concat([encodeTag(1, 2), encodeVarint(3), Buffer.from("key")]);
    const mapVal = Buffer.concat([encodeTag(2, 2), encodeVarint(3), Buffer.from("val")]);
    const mapEntry = Buffer.concat([mapKey, mapVal]);
    const tag4 = Buffer.concat([encodeTag(4, 2), encodeVarint(mapEntry.length), mapEntry]);
    // attempt = 2
    const tag5 = Buffer.concat([encodeTag(5, 0), encodeVarint(2)]);
    // published_at = 1706745600000
    const tag6 = Buffer.concat([encodeTag(6, 0), encodeVarint(1706745600000)]);

    const encoded = Buffer.concat([tag1, tag2, tag3, tag4, tag5, tag6]);
    const msg = decodeMessage(encoded);

    expect(msg.messageId).toBe("msg01");
    expect(msg.queue).toBe("queue1");
    expect(msg.payload).toEqual(payload);
    expect(msg.headers).toEqual({ key: "val" });
    expect(msg.attempt).toBe(2);
    expect(msg.publishedAt).toBe(1706745600000);
  });

  it("decodes message with empty fields as defaults", () => {
    const msg = decodeMessage(Buffer.alloc(0));
    expect(msg.messageId).toBe("");
    expect(msg.queue).toBe("");
    expect(msg.payload).toEqual(Buffer.alloc(0));
    expect(msg.headers).toEqual({});
    expect(msg.attempt).toBe(0);
    expect(msg.publishedAt).toBe(0);
  });
});

describe("AckRequest encoding", () => {
  it("encodes message_id", () => {
    const encoded = encodeAckRequest("msg-xyz");
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("msg-xyz");
  });
});

describe("NackRequest encoding", () => {
  it("encodes message_id and requeue=true", () => {
    const encoded = encodeNackRequest("msg-xyz", true);
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("msg-xyz");
    expect(Number(fields[2]![0]!.data as bigint)).toBe(1);
  });

  it("omits requeue when false (protobuf default)", () => {
    const encoded = encodeNackRequest("msg-xyz", false);
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("msg-xyz");
    // requeue=false is the protobuf default, so field 2 should not be present
    expect(fields[2]).toBeUndefined();
  });
});

describe("ErrorResponse decoding", () => {
  it("decodes code and message", () => {
    // code=3, message="queue full"
    const tag1 = Buffer.concat([encodeTag(1, 0), encodeVarint(3)]);
    const msg = Buffer.from("queue full");
    const tag2 = Buffer.concat([encodeTag(2, 2), encodeVarint(msg.length), msg]);
    const encoded = Buffer.concat([tag1, tag2]);

    const result = decodeErrorResponse(encoded);
    expect(result.code).toBe(3);
    expect(result.message).toBe("queue full");
  });
});

describe("ExtendVisibility roundtrip", () => {
  it("encodes request and decodes response", () => {
    const encoded = encodeExtendVisibilityRequest("msg-1", 30);
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("msg-1");
    expect(Number(fields[2]![0]!.data as bigint)).toBe(30);

    // Build a response: new_visible_at = 1706745630000
    const respTag = Buffer.concat([encodeTag(1, 0), encodeVarint(1706745630000)]);
    const resp = decodeExtendVisibilityResponse(respTag);
    expect(resp.newVisibleAt).toBe(1706745630000);
  });
});

describe("CallRequest/Response", () => {
  it("encodes call request", () => {
    const encoded = encodeCallRequest(
      "rpc-queue",
      Buffer.from("req-data"),
      { "x-id": "123" },
      5000,
    );
    const fields = decodeFields(encoded);
    expect((fields[1]![0]!.data as Buffer).toString()).toBe("rpc-queue");
    expect((fields[2]![0]!.data as Buffer).toString()).toBe("req-data");
    expect(Number(fields[4]![0]!.data as bigint)).toBe(5000);
  });

  it("decodes call response", () => {
    const payload = Buffer.from("resp-data");
    const tag1 = Buffer.concat([encodeTag(1, 2), encodeVarint(payload.length), payload]);
    // headers map entry
    const mapKey = Buffer.concat([encodeTag(1, 2), encodeVarint(6), Buffer.from("status")]);
    const mapVal = Buffer.concat([encodeTag(2, 2), encodeVarint(2), Buffer.from("ok")]);
    const mapEntry = Buffer.concat([mapKey, mapVal]);
    const tag2 = Buffer.concat([encodeTag(2, 2), encodeVarint(mapEntry.length), mapEntry]);

    const resp = decodeCallResponse(Buffer.concat([tag1, tag2]));
    expect(resp.payload).toEqual(payload);
    expect(resp.headers).toEqual({ status: "ok" });
  });
});

describe("QueueList roundtrip", () => {
  it("encodes empty request", () => {
    const encoded = encodeQueueListRequest();
    expect(encoded.length).toBe(0);
  });

  it("decodes response with queues", () => {
    // Build QueueInfo submessages
    const q1Name = Buffer.from("queue-a");
    const q1 = Buffer.concat([
      encodeTag(1, 2), encodeVarint(q1Name.length), q1Name,
      encodeTag(2, 0), encodeVarint(42),
      encodeTag(3, 0), encodeVarint(5),
    ]);
    const q2Name = Buffer.from("queue-b");
    const q2 = Buffer.concat([
      encodeTag(1, 2), encodeVarint(q2Name.length), q2Name,
      encodeTag(2, 0), encodeVarint(0),
      encodeTag(3, 0), encodeVarint(0),
    ]);

    // Wrap in QueueListResponse: queues=1 (repeated)
    const resp = Buffer.concat([
      encodeTag(1, 2), encodeVarint(q1.length), q1,
      encodeTag(1, 2), encodeVarint(q2.length), q2,
    ]);

    const queues = decodeQueueListResponse(resp);
    expect(queues).toHaveLength(2);
    expect(queues[0]).toEqual({ name: "queue-a", messageCount: 42, inFlightCount: 5 });
    expect(queues[1]).toEqual({ name: "queue-b", messageCount: 0, inFlightCount: 0 });
  });
});

describe("SchemaList roundtrip", () => {
  it("encodes empty request", () => {
    const encoded = encodeSchemaListRequest();
    expect(encoded.length).toBe(0);
  });

  it("decodes response with schemas", () => {
    const qName = Buffer.from("orders");
    const mType = Buffer.from("OrderEvent");
    const info = Buffer.concat([
      encodeTag(1, 2), encodeVarint(qName.length), qName,
      encodeTag(2, 2), encodeVarint(mType.length), mType,
      encodeTag(3, 0), encodeVarint(3),
    ]);

    const resp = Buffer.concat([
      encodeTag(1, 2), encodeVarint(info.length), info,
    ]);

    const schemas = decodeSchemaListResponse(resp);
    expect(schemas).toHaveLength(1);
    expect(schemas[0]).toEqual({ queue: "orders", messageType: "OrderEvent", version: 3 });
  });
});

describe("multiple headers in map", () => {
  it("encodes and decodes multiple map entries", () => {
    const encoded = encodePublishRequest(
      "q",
      Buffer.from("x"),
      { "key1": "val1", "key2": "val2", "key3": "val3" },
    );
    const fields = decodeFields(encoded);
    expect(fields[3]!.length).toBe(3);

    const headers: Record<string, string> = {};
    for (const entry of fields[3]!) {
      const sub = decodeFields(entry.data as Buffer);
      const k = (sub[1]![0]!.data as Buffer).toString();
      const v = (sub[2]![0]!.data as Buffer).toString();
      headers[k] = v;
    }
    expect(headers).toEqual({ key1: "val1", key2: "val2", key3: "val3" });
  });
});
