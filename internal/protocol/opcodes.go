package protocol

// ProtocolVersion is the current wire protocol version.
const ProtocolVersion uint8 = 1

// OpCode represents a protocol operation code.
type OpCode uint8

// Client -> Server operations
const (
	OpPublish          OpCode = 0x01
	OpConsume          OpCode = 0x03
	OpAck              OpCode = 0x05
	OpNack             OpCode = 0x06
	OpExtendVisibility OpCode = 0x08
	OpSchemaRegister   OpCode = 0x10
	OpSchemaGet        OpCode = 0x11
	OpCall             OpCode = 0x20
)

// Server -> Client operations
const (
	OpPublishAck          OpCode = 0x02
	OpMessage             OpCode = 0x04
	OpError               OpCode = 0x07
	OpExtendVisibilityAck OpCode = 0x09
	OpSchemaResponse      OpCode = 0x12
	OpCallResponse        OpCode = 0x21
)

// Admin operations
const (
	OpSchemaList     OpCode = 0x30
	OpSchemaListResp OpCode = 0x31
	OpQueueList      OpCode = 0x32
	OpQueueListResp  OpCode = 0x33
)

// Auth operations
const (
	OpAuth         OpCode = 0x40
	OpAuthResponse OpCode = 0x41
)

var opCodeNames = map[OpCode]string{
	OpPublish:             "PUBLISH",
	OpPublishAck:          "PUBLISH_ACK",
	OpConsume:             "CONSUME",
	OpMessage:             "MESSAGE",
	OpAck:                 "ACK",
	OpNack:                "NACK",
	OpError:               "ERROR",
	OpExtendVisibility:    "EXTEND_VISIBILITY",
	OpExtendVisibilityAck: "EXTEND_VISIBILITY_ACK",
	OpSchemaRegister:      "SCHEMA_REGISTER",
	OpSchemaGet:           "SCHEMA_GET",
	OpSchemaResponse:      "SCHEMA_RESPONSE",
	OpCall:                "CALL",
	OpCallResponse:        "CALL_RESPONSE",
	OpSchemaList:          "SCHEMA_LIST",
	OpSchemaListResp:      "SCHEMA_LIST_RESP",
	OpQueueList:           "QUEUE_LIST",
	OpQueueListResp:       "QUEUE_LIST_RESP",
	OpAuth:                "AUTH",
	OpAuthResponse:        "AUTH_RESPONSE",
}

func (op OpCode) String() string {
	if name, ok := opCodeNames[op]; ok {
		return name
	}
	return "UNKNOWN"
}
