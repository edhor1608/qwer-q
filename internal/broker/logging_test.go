package broker

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLogging(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))
	SetLogger(testLogger)

	LogConnect("127.0.0.1:12345")
	output := buf.String()

	if !strings.Contains(output, "client connected") {
		t.Error("expected 'client connected' in log output")
	}
	if !strings.Contains(output, "127.0.0.1:12345") {
		t.Error("expected client address in log output")
	}
}

func TestLogPublish(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	SetLogger(testLogger)

	LogPublish("test-queue", "msg-123", "127.0.0.1:12345")
	output := buf.String()

	if !strings.Contains(output, "message published") {
		t.Error("expected 'message published' in log output")
	}
	if !strings.Contains(output, "test-queue") {
		t.Error("expected queue name in log output")
	}
	if !strings.Contains(output, "msg-123") {
		t.Error("expected message ID in log output")
	}
}

func TestLogger(t *testing.T) {
	l := Logger()
	if l == nil {
		t.Error("expected logger to be non-nil")
	}
}
