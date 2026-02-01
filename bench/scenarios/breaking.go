package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// ResourceStats holds container resource usage
type ResourceStats struct {
	Timestamp   time.Time
	CPUPercent  float64
	MemoryMB    float64
	MemoryLimit float64
	NetworkRxMB float64
	NetworkTxMB float64
}

// BreakingPointResult holds results from breaking point tests
type BreakingPointResult struct {
	Queue            string
	MaxThroughput    float64 // msgs/sec before errors
	BreakingPoint    float64 // msgs/sec when errors start
	FirstErrorAt     int64   // message count when first error occurred
	TotalErrors      int64
	ErrorRate        float64 // errors per second
	AvgLatencyAtBreak time.Duration
	MaxMemoryMB      float64
	MaxCPUPercent    float64
	ResourceSamples  []ResourceStats
}

// FetchContainerStats gets resource usage from cAdvisor
func FetchContainerStats(containerName string) (*ResourceStats, error) {
	// cAdvisor API endpoint
	url := fmt.Sprintf("http://localhost:8080/api/v1.3/docker/%s", containerName)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	// Parse stats (simplified - real implementation would parse properly)
	stats := &ResourceStats{
		Timestamp: time.Now(),
	}

	return stats, nil
}

// RunBreakingPointTest finds the throughput where the system starts failing
func RunBreakingPointTest(ctx context.Context, adapter adapters.Adapter, containerName string) (*BreakingPointResult, error) {
	result := &BreakingPointResult{
		Queue:           adapter.Name(),
		ResourceSamples: make([]ResourceStats, 0),
	}

	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-breaking-%d", time.Now().UnixNano())
	payload := make([]byte, 1024)

	// Start with low rate and increase until errors
	rates := []int{1000, 5000, 10000, 20000, 50000, 100000, 200000, 500000}

	for _, targetRate := range rates {
		fmt.Printf("  Testing rate: %d msgs/sec...\n", targetRate)

		var published, errors atomic.Int64
		var firstError atomic.Int64
		firstError.Store(-1)

		testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

		var wg sync.WaitGroup

		// Consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			adapter.Consume(testCtx, queue, func(msg []byte) error {
				return nil
			})
		}()

		// Rate-limited producer
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := time.Second / time.Duration(targetRate)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-testCtx.Done():
					return
				case <-ticker.C:
					if err := adapter.Publish(testCtx, queue, payload); err != nil {
						errors.Add(1)
						if firstError.CompareAndSwap(-1, published.Load()) {
							// First error recorded
						}
					} else {
						published.Add(1)
					}
				}
			}
		}()

		wg.Wait()
		cancel()

		actualRate := float64(published.Load()) / 10.0
		errorCount := errors.Load()

		fmt.Printf("    Actual: %.0f msgs/sec, Errors: %d\n", actualRate, errorCount)

		if errorCount > 0 && result.BreakingPoint == 0 {
			result.BreakingPoint = actualRate
			result.FirstErrorAt = firstError.Load()
		}

		if errorCount == 0 {
			result.MaxThroughput = actualRate
		}

		result.TotalErrors += errorCount

		// If error rate > 10%, we've found the breaking point
		if errorCount > 0 && float64(errorCount)/float64(published.Load()) > 0.1 {
			fmt.Printf("    Breaking point reached (>10%% errors)\n")
			break
		}
	}

	return result, nil
}

// RunMemoryPressureTest tests behavior under memory constraints
func RunMemoryPressureTest(ctx context.Context, adapter adapters.Adapter, maxMessages int) (*harness.Result, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-memory-%d", time.Now().UnixNano())

	// Use larger messages to consume memory faster
	payload := make([]byte, 10*1024) // 10KB messages

	var published atomic.Int64
	var errors atomic.Int64
	samples := make([]sample, 0)
	var sampleMu sync.Mutex

	fmt.Printf("  Publishing %d x 10KB messages (target: %dMB)...\n", maxMessages, maxMessages*10/1024)

	// Publish without consuming to build up memory pressure
	start := time.Now()
	for i := 0; i < maxMessages; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			errors.Add(1)
			if errors.Load() > 100 {
				fmt.Printf("  Too many errors, stopping at %d messages\n", i)
				break
			}
		} else {
			published.Add(1)
		}

		// Sample every 1000 messages
		if i > 0 && i%1000 == 0 {
			sampleMu.Lock()
			samples = append(samples, sample{
				timestamp: time.Now(),
				published: published.Load(),
				consumed:  0,
			})
			sampleMu.Unlock()
		}

		// Check if context cancelled
		select {
		case <-ctx.Done():
			break
		default:
		}
	}

	duration := time.Since(start)

	return &harness.Result{
		Queue:     adapter.Name(),
		Published: published.Load(),
		Consumed:  0,
		Duration:  duration,
		PubErrors: errors.Load(),
		Samples:   convertSamples(samples),
	}, nil
}

// RunConnectionStormTest tests behavior with many rapid connections
func RunConnectionStormTest(ctx context.Context, adapterFactory func() adapters.Adapter, numConnections int) (*ConnectionStormResult, error) {
	result := &ConnectionStormResult{
		NumConnections: numConnections,
	}

	var successful, failed atomic.Int64
	var totalConnectTime atomic.Int64

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			adapter := adapterFactory()
			connectStart := time.Now()

			if err := adapter.Setup(ctx); err != nil {
				failed.Add(1)
				return
			}

			totalConnectTime.Add(time.Since(connectStart).Microseconds())
			successful.Add(1)

			// Keep connection briefly
			time.Sleep(100 * time.Millisecond)
			adapter.Teardown()
		}()

		// Stagger connections slightly
		time.Sleep(time.Millisecond)
	}

	wg.Wait()
	totalTime := time.Since(start)

	result.Successful = successful.Load()
	result.Failed = failed.Load()
	result.TotalTime = totalTime
	if successful.Load() > 0 {
		result.AvgConnectTime = time.Duration(totalConnectTime.Load()/successful.Load()) * time.Microsecond
	}

	return result, nil
}

// ConnectionStormResult holds connection storm test results
type ConnectionStormResult struct {
	NumConnections int
	Successful     int64
	Failed         int64
	TotalTime      time.Duration
	AvgConnectTime time.Duration
}

// RunCrashRecoveryTest tests recovery after simulated crash.
// For QWER-Q, this requires restarting the Docker container to test persistence.
func RunCrashRecoveryTest(ctx context.Context, adapter adapters.Adapter, messagesToPublish int) (*RecoveryResult, error) {
	result := &RecoveryResult{}

	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}

	// Use a fixed queue name so we can find it after restart
	queue := "bench-recovery-test"
	payload := make([]byte, 1024)

	// Publish messages
	fmt.Printf("  Publishing %d messages...\n", messagesToPublish)
	pubStart := time.Now()
	for i := 0; i < messagesToPublish; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			return nil, fmt.Errorf("publish failed at %d: %w", i, err)
		}
	}
	result.PublishTime = time.Since(pubStart)
	result.Published = int64(messagesToPublish)

	// Close connection before restart
	adapter.Teardown()

	// Simulate crash by restarting the container (only for QWER-Q)
	if adapter.Name() == "QWER-Q" {
		fmt.Printf("  Restarting container (simulating crash)...\n")
		restartContainer("bench-qwerq-1")
		time.Sleep(3 * time.Second) // Wait for restart
	} else {
		fmt.Printf("  Simulating crash (closing connection)...\n")
		time.Sleep(time.Second)
	}

	// Reconnect
	fmt.Printf("  Reconnecting...\n")
	reconnectStart := time.Now()
	if err := adapter.Setup(ctx); err != nil {
		return nil, fmt.Errorf("reconnect failed: %w", err)
	}
	result.ReconnectTime = time.Since(reconnectStart)

	// Try to consume the messages
	fmt.Printf("  Consuming messages...\n")
	var consumed atomic.Int64
	consumeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	consumeStart := time.Now()
	done := make(chan bool)

	go func() {
		adapter.Consume(consumeCtx, queue, func(msg []byte) error {
			if consumed.Add(1) >= int64(messagesToPublish) {
				cancel()
			}
			return nil
		})
		done <- true
	}()

	<-done
	result.ConsumeTime = time.Since(consumeStart)
	result.Consumed = consumed.Load()
	result.Recovered = consumed.Load() == int64(messagesToPublish)

	adapter.Teardown()
	return result, nil
}

func restartContainer(name string) {
	cmd := exec.Command("docker", "restart", name)
	cmd.Run()
}

// RecoveryResult holds crash recovery test results
type RecoveryResult struct {
	Published     int64
	Consumed      int64
	Recovered     bool
	PublishTime   time.Duration
	ReconnectTime time.Duration
	ConsumeTime   time.Duration
}

// PrintBreakingPointResults prints breaking point test results
func PrintBreakingPointResults(results map[string]*BreakingPointResult) {
	fmt.Println("\nBreaking Point Analysis:")
	fmt.Println("+-------------+--------------+--------------+--------------+-----------+")
	fmt.Println("| Queue       | Max Stable   | Breaking Pt  | First Error  | Errors    |")
	fmt.Println("+-------------+--------------+--------------+--------------+-----------+")
	for name, r := range results {
		fmt.Printf("| %-11s | %10.0f/s | %10.0f/s | %12d | %9d |\n",
			name, r.MaxThroughput, r.BreakingPoint, r.FirstErrorAt, r.TotalErrors)
	}
	fmt.Println("+-------------+--------------+--------------+--------------+-----------+")
}

// PrintConnectionStormResults prints connection storm results
func PrintConnectionStormResults(results map[string]*ConnectionStormResult) {
	fmt.Println("\nConnection Storm Test:")
	fmt.Println("+-------------+------------+------------+--------------+--------------+")
	fmt.Println("| Queue       | Attempted  | Successful | Failed       | Avg Connect  |")
	fmt.Println("+-------------+------------+------------+--------------+--------------+")
	for name, r := range results {
		fmt.Printf("| %-11s | %10d | %10d | %12d | %12s |\n",
			name, r.NumConnections, r.Successful, r.Failed, r.AvgConnectTime)
	}
	fmt.Println("+-------------+------------+------------+--------------+--------------+")
}

// PrintRecoveryResults prints crash recovery results
func PrintRecoveryResults(results map[string]*RecoveryResult) {
	fmt.Println("\nCrash Recovery Test:")
	fmt.Println("+-------------+------------+------------+-----------+--------------+--------------+")
	fmt.Println("| Queue       | Published  | Recovered  | Success   | Reconnect    | Consume Time |")
	fmt.Println("+-------------+------------+------------+-----------+--------------+--------------+")
	for name, r := range results {
		success := "NO"
		if r.Recovered {
			success = "YES"
		}
		fmt.Printf("| %-11s | %10d | %10d | %9s | %12s | %12s |\n",
			name, r.Published, r.Consumed, success, r.ReconnectTime, r.ConsumeTime)
	}
	fmt.Println("+-------------+------------+------------+-----------+--------------+--------------+")
}
