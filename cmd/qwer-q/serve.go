package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/protocol"
	"github.com/jonas/qwer-q/internal/storage"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the broker server",
	RunE:  runServe,
}

func init() {
	serveCmd.Flags().IntP("port", "p", 9876, "port to listen on")
	serveCmd.Flags().Int("metrics-port", 9877, "metrics server port")
	serveCmd.Flags().String("data-dir", "/data", "data directory for message persistence")
	serveCmd.Flags().String("max-message-size", "1MB", "maximum message payload size (e.g., 1MB, 512KB)")
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	maxMsgSize, _ := cmd.Flags().GetString("max-message-size")
	addr := fmt.Sprintf(":%d", port)
	metricsAddr := fmt.Sprintf(":%d", metricsPort)

	// Configure max message size
	if size, err := parseSize(maxMsgSize); err != nil {
		return fmt.Errorf("invalid max-message-size: %w", err)
	} else {
		protocol.SetMaxMessageSize(uint32(size))
	}

	var opts []broker.BrokerOption

	// Initialize persistent storage if data directory is specified
	if dataDir != "" {
		store, err := storage.NewBadgerStorage(dataDir)
		if err != nil {
			return fmt.Errorf("failed to open storage: %w", err)
		}
		opts = append(opts, broker.WithStorage(store))
	}

	b := broker.NewBroker(opts...)
	defer b.Close()

	// Load persisted messages from storage (only if storage is configured)
	if dataDir != "" {
		if err := b.LoadFromStorage(); err != nil {
			return fmt.Errorf("failed to load from storage: %w", err)
		}
	}

	srv := broker.NewServer(b)
	metricsSrv := broker.NewMetricsServer(metricsAddr)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		metricsSrv.Shutdown(ctx)
		srv.Close()
	}()

	// Start metrics server
	go metricsSrv.ListenAndServe()

	printBanner(addr, metricsAddr, version)

	return srv.ListenAndServe(addr)
}

func printBanner(brokerAddr, metricsAddr, ver string) {
	fmt.Printf(`
QWER-Q Message Queue v%s
Listening on %s (broker), %s (metrics)
Warning: Running without authentication - not for production

`, ver, brokerAddr, metricsAddr)
}

// parseSize parses a size string like "1MB", "512KB", "1024" into bytes.
func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	// Try parsing as plain number first
	var size int64
	var suffix string
	n, _ := fmt.Sscanf(s, "%d%s", &size, &suffix)
	if n == 0 {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	switch suffix {
	case "", "B", "b":
		// bytes, no change
	case "K", "k", "KB", "kb", "Kb", "kB":
		size *= 1024
	case "M", "m", "MB", "mb", "Mb", "mB":
		size *= 1024 * 1024
	case "G", "g", "GB", "gb", "Gb", "gB":
		size *= 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size suffix: %s", suffix)
	}

	return size, nil
}
