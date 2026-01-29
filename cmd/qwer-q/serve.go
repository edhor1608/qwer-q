package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
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
	serveCmd.Flags().String("data-dir", "", "data directory (not used yet)")
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	addr := fmt.Sprintf(":%d", port)
	metricsAddr := fmt.Sprintf(":%d", metricsPort)

	b := broker.NewBroker()
	defer b.Close()

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
