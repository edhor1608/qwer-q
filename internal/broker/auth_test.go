package broker

import (
	"net"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func startTestServer(t *testing.T, authToken string) (*Server, string) {
	t.Helper()
	b := NewBroker()
	t.Cleanup(b.Close)

	srv := NewServer(b)
	if authToken != "" {
		srv.SetAuthToken(authToken)
	}
	go srv.ListenAndServe("127.0.0.1:0")
	t.Cleanup(func() { srv.Close() })

	srv.WaitReady()
	return srv, srv.Addr().String()
}

func dialTest(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func sendAuth(t *testing.T, conn net.Conn, token string) *protocol.AuthResponse {
	t.Helper()
	req := &protocol.AuthRequest{Token: token}
	data, _ := proto.Marshal(req)
	conn.Write(protocol.EncodeFrame(protocol.OpAuth, data))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read auth response: %v", err)
	}
	conn.SetReadDeadline(time.Time{})

	if frame.OpCode != protocol.OpAuthResponse {
		t.Fatalf("expected OpAuthResponse, got %v", frame.OpCode)
	}

	var resp protocol.AuthResponse
	if err := proto.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("failed to unmarshal auth response: %v", err)
	}
	return &resp
}

func TestAuthSuccess(t *testing.T) {
	_, addr := startTestServer(t, "secret-token")
	conn := dialTest(t, addr)

	resp := sendAuth(t, conn, "secret-token")
	if !resp.Success {
		t.Fatalf("expected auth success, got: %s", resp.Message)
	}

	// Should be able to use the connection normally after auth
	pubReq := &protocol.PublishRequest{Queue: "test", Payload: []byte("hello")}
	data, _ := proto.Marshal(pubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, data))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read publish ack: %v", err)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}
}

func TestAuthFailure(t *testing.T) {
	_, addr := startTestServer(t, "secret-token")
	conn := dialTest(t, addr)

	resp := sendAuth(t, conn, "wrong-token")
	if resp.Success {
		t.Fatal("expected auth failure")
	}
	if resp.Message != "invalid token" {
		t.Fatalf("expected 'invalid token', got: %s", resp.Message)
	}

	// Connection should be closed after failed auth — next read should fail
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := protocol.DecodeFrame(conn)
	if err == nil {
		t.Fatal("expected connection to be closed after auth failure")
	}
}

func TestAuthRequired_WrongOpcode(t *testing.T) {
	_, addr := startTestServer(t, "secret-token")
	conn := dialTest(t, addr)

	// Send a publish without authenticating first
	pubReq := &protocol.PublishRequest{Queue: "test", Payload: []byte("hello")}
	data, _ := proto.Marshal(pubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, data))

	// Should get an error response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read error response: %v", err)
	}
	if frame.OpCode != protocol.OpError {
		t.Fatalf("expected OpError, got %v", frame.OpCode)
	}

	var errResp protocol.ErrorResponse
	if err := proto.Unmarshal(frame.Payload, &errResp); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}
	if errResp.Code != 9 {
		t.Fatalf("expected error code 9, got %d", errResp.Code)
	}
	if errResp.Message != "authentication required" {
		t.Fatalf("expected 'authentication required', got: %s", errResp.Message)
	}

	// Connection should be closed
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = protocol.DecodeFrame(conn)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestAuthBypass_NoTokenConfigured(t *testing.T) {
	_, addr := startTestServer(t, "")
	conn := dialTest(t, addr)

	// Should work without auth when no token is configured
	pubReq := &protocol.PublishRequest{Queue: "test", Payload: []byte("hello")}
	data, _ := proto.Marshal(pubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, data))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck (auth bypass), got %v", frame.OpCode)
	}
}
