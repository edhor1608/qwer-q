package broker

import (
	"log/slog"
	"os"
)

var logger *slog.Logger

func init() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// SetLogger sets the global logger.
func SetLogger(l *slog.Logger) {
	logger = l
}

// Logger returns the global logger.
func Logger() *slog.Logger {
	return logger
}

// LogConnect logs a client connection.
func LogConnect(addr string) {
	logger.Info("client connected", "addr", addr)
}

// LogDisconnect logs a client disconnection.
func LogDisconnect(addr string) {
	logger.Info("client disconnected", "addr", addr)
}

// LogPublish logs a message publish.
func LogPublish(queue, messageID, clientAddr string) {
	logger.Debug("message published",
		"queue", queue,
		"message_id", messageID,
		"client", clientAddr,
	)
}

// LogConsume logs a message delivery.
func LogConsume(queue, messageID, clientAddr string) {
	logger.Debug("message delivered",
		"queue", queue,
		"message_id", messageID,
		"client", clientAddr,
	)
}

// LogAck logs a message acknowledgment.
func LogAck(queue, messageID, clientAddr string) {
	logger.Debug("message acked",
		"queue", queue,
		"message_id", messageID,
		"client", clientAddr,
	)
}

// LogNack logs a message negative acknowledgment.
func LogNack(queue, messageID, clientAddr string, requeue bool) {
	logger.Debug("message nacked",
		"queue", queue,
		"message_id", messageID,
		"client", clientAddr,
		"requeue", requeue,
	)
}

// LogError logs an error.
func LogError(msg string, err error, attrs ...any) {
	args := append([]any{"error", err}, attrs...)
	logger.Error(msg, args...)
}
