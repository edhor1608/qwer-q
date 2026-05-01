package client

import (
	"fmt"
	"sort"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// PublishProto marshals a protobuf message and publishes it to a queue.
func (c *Client) PublishProto(queue string, msg proto.Message) (*protocol.PublishResponse, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return c.Publish(queue, payload)
}

// ConsumeProto consumes a queue and decodes each message payload into a protobuf message.
// The handler receives both the original envelope and the decoded payload.
func (c *Client) ConsumeProto(queue string, prefetch uint32, newMessage func() proto.Message, handler func(*protocol.Message, proto.Message) error) error {
	return c.Consume(queue, prefetch, func(msg *protocol.Message) error {
		decoded := newMessage()
		if err := proto.Unmarshal(msg.Payload, decoded); err != nil {
			return err
		}
		return handler(msg, decoded)
	})
}

// SchemaRegisterMessage registers the protobuf schema for a generated message type.
func (c *Client) SchemaRegisterMessage(queue string, msg proto.Message) (*protocol.SchemaRegisterResponse, error) {
	descriptor, messageType, err := schemaDescriptorSet(msg)
	if err != nil {
		return nil, err
	}
	return c.SchemaRegister(queue, descriptor, messageType)
}

func schemaDescriptorSet(msg proto.Message) ([]byte, string, error) {
	if msg == nil {
		return nil, "", fmt.Errorf("nil protobuf message")
	}
	msgDesc := msg.ProtoReflect().Descriptor()
	if msgDesc == nil {
		return nil, "", fmt.Errorf("missing protobuf descriptor")
	}

	files := make(map[string]protoreflect.FileDescriptor)
	var collect func(protoreflect.FileDescriptor)
	collect = func(fd protoreflect.FileDescriptor) {
		key := fd.Path()
		if _, ok := files[key]; ok {
			return
		}
		files[key] = fd
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			collect(imports.Get(i).FileDescriptor)
		}
	}
	collect(msgDesc.ParentFile())

	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	fds := &descriptorpb.FileDescriptorSet{File: make([]*descriptorpb.FileDescriptorProto, 0, len(paths))}
	for _, path := range paths {
		fds.File = append(fds.File, protodesc.ToFileDescriptorProto(files[path]))
	}

	data, err := proto.Marshal(fds)
	if err != nil {
		return nil, "", err
	}
	return data, string(msgDesc.FullName()), nil
}
