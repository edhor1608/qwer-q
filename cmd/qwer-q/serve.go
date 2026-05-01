package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jonas/qwer-q/internal/api"
	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/cluster"
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
	serveCmd.Flags().Duration("sync-interval", 100*time.Millisecond, "how often to fsync queue data (0 = every write)")
	serveCmd.Flags().Duration("batch-interval", 0, "write batch flush interval (e.g., 5ms). 0 = no batching")
	serveCmd.Flags().String("auth-token", "", "require clients to authenticate with this token (env: QWERQ_AUTH_TOKEN)")
	serveCmd.Flags().String("schema-mode", "permissive", "schema enforcement mode: permissive or strict (env: QWERQ_SCHEMA_MODE)")

	// Clustering flags
	serveCmd.Flags().String("cluster-node-id", "", "unique node ID for clustering (enables cluster mode)")
	serveCmd.Flags().String("cluster-bind", "0.0.0.0:9878", "address for Raft communication")
	serveCmd.Flags().String("cluster-advertise", "", "address advertised to peers (defaults to cluster-bind)")
	serveCmd.Flags().StringSlice("cluster-peers", nil, "initial cluster peers (format: id=host:port)")
	serveCmd.Flags().String("cluster-data-dir", "", "Raft data directory (defaults to data-dir/raft)")
	serveCmd.Flags().Bool("cluster-bootstrap", false, "bootstrap a new cluster (use on first start only)")

	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	maxMsgSize, _ := cmd.Flags().GetString("max-message-size")
	syncInterval, _ := cmd.Flags().GetDuration("sync-interval")
	batchInterval, _ := cmd.Flags().GetDuration("batch-interval")
	authToken, _ := cmd.Flags().GetString("auth-token")
	schemaModeStr, _ := cmd.Flags().GetString("schema-mode")
	if authToken == "" {
		authToken = os.Getenv("QWERQ_AUTH_TOKEN")
	}
	if !cmd.Flags().Changed("schema-mode") {
		if envMode := os.Getenv("QWERQ_SCHEMA_MODE"); envMode != "" {
			schemaModeStr = envMode
		}
	}

	schemaMode, err := parseSchemaMode(schemaModeStr)
	if err != nil {
		return err
	}
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
		var storageOpts []storage.StorageOption
		storageOpts = append(storageOpts, storage.WithSyncInterval(syncInterval))
		if batchInterval > 0 {
			storageOpts = append(storageOpts, storage.WithBatchInterval(batchInterval))
		}
		store, err := storage.NewBadgerStorage(dataDir, storageOpts...)
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
	if authToken != "" {
		srv.SetAuthToken(authToken)
	}
	srv.SetSchemaMode(schemaMode)
	metricsSrv := broker.NewMetricsServer(metricsAddr)

	// Register REST API on the same HTTP server as metrics
	apiHandler := api.New(b, srv.Registry())
	apiHandler.Register(metricsSrv.Mux())

	// Clustering setup (opt-in via --cluster-node-id)
	clusterNodeID, _ := cmd.Flags().GetString("cluster-node-id")
	var clusterNode *cluster.Node

	if clusterNodeID != "" {
		clusterBind, _ := cmd.Flags().GetString("cluster-bind")
		clusterAdvertise, _ := cmd.Flags().GetString("cluster-advertise")
		clusterPeers, _ := cmd.Flags().GetStringSlice("cluster-peers")
		clusterDataDir, _ := cmd.Flags().GetString("cluster-data-dir")
		clusterBootstrap, _ := cmd.Flags().GetBool("cluster-bootstrap")

		if clusterDataDir == "" {
			clusterDataDir = dataDir + "/raft"
		}

		clusterLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

		cfg := cluster.Config{
			NodeID:        clusterNodeID,
			BindAddr:      clusterBind,
			AdvertiseAddr: clusterAdvertise,
			DataDir:       clusterDataDir,
			Peers:         clusterPeers,
			Bootstrap:     clusterBootstrap,
		}

		var err error
		clusterNode, err = cluster.NewNode(cfg, b, clusterLogger)
		if err != nil {
			return fmt.Errorf("failed to start cluster node: %w", err)
		}
		srv.SetReplicator(clusterNode)

		fmt.Printf("Cluster mode: node=%s bind=%s peers=%s\n",
			clusterNodeID, clusterBind, strings.Join(clusterPeers, ","))
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		if clusterNode != nil {
			clusterNode.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		metricsSrv.Shutdown(ctx)
		srv.Close()
	}()

	// Start metrics server
	go metricsSrv.ListenAndServe()

	printBanner(addr, metricsAddr, version, authToken != "", schemaMode)

	return srv.ListenAndServe(addr)
}

func printBanner(brokerAddr, metricsAddr, ver string, authEnabled bool, schemaMode broker.SchemaMode) {
	authStatus := "WARNING: Running without authentication - not for production"
	if authEnabled {
		authStatus = "Authentication enabled"
	}
	fmt.Printf(`
QWER-Q Message Queue v%s
Listening on %s (broker), %s (metrics)
%s
Schema mode: %s

`, ver, brokerAddr, metricsAddr, authStatus, schemaMode)
}

// parseSize parses a size string like "1MB", "512KB", "1024" into bytes.
// Returns error for negative values or values exceeding uint32 max.
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

	// Reject negative values
	if size < 0 {
		return 0, fmt.Errorf("size cannot be negative: %s", s)
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

	// Check uint32 bounds (max ~4GB)
	const maxUint32 = 1<<32 - 1
	if size > maxUint32 {
		return 0, fmt.Errorf("size exceeds maximum (4GB): %s", s)
	}

	return size, nil
}

func parseSchemaMode(s string) (broker.SchemaMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", string(broker.SchemaModePermissive):
		return broker.SchemaModePermissive, nil
	case string(broker.SchemaModeStrict):
		return broker.SchemaModeStrict, nil
	default:
		return "", fmt.Errorf("invalid schema-mode %q (expected permissive or strict)", s)
	}
}
