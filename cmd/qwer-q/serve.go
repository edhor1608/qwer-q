package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
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
	serveCmd.Flags().Duration("sync-interval", 100*time.Millisecond, "disk sync interval (0 = sync every write, higher = more throughput, less durability)")
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	syncInterval, _ := cmd.Flags().GetDuration("sync-interval")
	addr := fmt.Sprintf(":%d", port)
	metricsAddr := fmt.Sprintf(":%d", metricsPort)

	var opts []broker.BrokerOption

	// Initialize persistent storage if data directory is specified
	if dataDir != "" {
		store, err := storage.NewBadgerStorage(dataDir, storage.WithSyncInterval(syncInterval))
		if err != nil {
			return fmt.Errorf("failed to open storage: %w", err)
		}
		opts = append(opts, broker.WithStorage(store))
	}

	b := broker.NewBroker(opts...)
	defer b.Close()

	// Load persisted messages from storage
	if err := b.LoadFromStorage(); err != nil {
		return fmt.Errorf("failed to load from storage: %w", err)
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
