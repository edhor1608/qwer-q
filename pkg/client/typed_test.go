package client

import (
	"strings"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func startTypedTestServer(t *testing.T) string {
	t.Helper()
	b := broker.NewBroker(broker.WithMemoryLimit(0))
	t.Cleanup(b.Close)

	srv := broker.NewServer(b)
	srv.SetSchemaMode(broker.SchemaModeStrict)
	go srv.ListenAndServe("127.0.0.1:0")
	t.Cleanup(func() { srv.Close() })
	srv.WaitReady()
	return srv.Addr().String()
}

func TestSchemaRegisterMessage(t *testing.T) {
	addr := startTypedTestServer(t)
	c, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	resp, err := c.SchemaRegisterMessage("typed-queue", &protocol.PublishRequest{})
	if err != nil {
		t.Fatalf("SchemaRegisterMessage failed: %v", err)
	}
	if resp.Version != 1 {
		t.Fatalf("version = %d, want 1", resp.Version)
	}
}

func TestPublishAndConsumeProto(t *testing.T) {
	addr := startTypedTestServer(t)
	pub, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	cons, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	queue := "typed-consume"
	if _, err := pub.SchemaRegisterMessage(queue, &protocol.PublishRequest{}); err != nil {
		t.Fatalf("SchemaRegisterMessage failed: %v", err)
	}

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- cons.ConsumeProto(queue, 1, func() proto.Message { return &protocol.PublishRequest{} }, func(msg *protocol.Message, decoded proto.Message) error {
			req := decoded.(*protocol.PublishRequest)
			if req.GetQueue() != "inner" {
				t.Errorf("decoded queue = %q, want %q", req.GetQueue(), "inner")
			}
			if err := cons.Ack(msg.MessageId); err != nil {
				t.Errorf("ack failed: %v", err)
			}
			close(done)
			_ = cons.Close()
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := pub.PublishProto(queue, &protocol.PublishRequest{Queue: "inner", Payload: []byte("hello")}); err != nil {
		t.Fatalf("PublishProto failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for typed consume")
	}

	if err := <-errCh; err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		t.Fatalf("ConsumeProto failed: %v", err)
	}
}
