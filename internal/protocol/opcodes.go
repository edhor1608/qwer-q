package protocol

// ProtocolVersion is the current wire protocol version.
const ProtocolVersion uint8 = 1

// OpCode represents a protocol operation code.
type OpCode uint8

// Client -> Server operations
const (
	OpPublish        OpCode = 0x01
	OpConsume        OpCode = 0x03
	OpAck            OpCode = 0x05
	OpNack           OpCode = 0x06
	OpSchemaRegister OpCode = 0x10
	OpSchemaGet      OpCode = 0x11
	OpCall           OpCode = 0x20
)

// Server -> Client operations
const (
	OpPublishAck     OpCode = 0x02
	OpMessage        OpCode = 0x04
	OpError          OpCode = 0x07
	OpSchemaResponse OpCode = 0x12
	OpCallResponse   OpCode = 0x21
)

var opCodeNames = map[OpCode]string{
	OpPublish:        "PUBLISH",
	OpPublishAck:     "PUBLISH_ACK",
	OpConsume:        "CONSUME",
	OpMessage:        "MESSAGE",
	OpAck:            "ACK",
	OpNack:           "NACK",
	OpError:          "ERROR",
	OpSchemaRegister: "SCHEMA_REGISTER",
	OpSchemaGet:      "SCHEMA_GET",
	OpSchemaResponse: "SCHEMA_RESPONSE",
	OpCall:           "CALL",
	OpCallResponse:   "CALL_RESPONSE",
}

func (op OpCode) String() string {
	if name, ok := opCodeNames[op]; ok {
		return name
	}
	return "UNKNOWN"
}
