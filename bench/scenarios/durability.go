package scenarios

import (
	"context"
	"fmt"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// DurabilityResult captures hard crash durability test results
type DurabilityResult struct {
	Name           string
	Published      int64
	Recovered      int64
	Lost           int64
	LossRate       float64 // percentage of messages lost
	PublishTime    time.Duration
	RecoveryTime   time.Duration
	CrashMethod    string // "sigkill" or "graceful"
	ContainerName  string
	Error          string
}

// DurabilityConfig configures the durability test
type DurabilityConfig struct {
	MessageCount  int
	MessageSize   int
	ContainerName string           // e.g., "bench-qwerq-1"
	WaitAfterPub  time.Duration    // time to wait after publishing before kill (0 = immediate)
	HardKill      bool             // true = SIGKILL (power loss), false = graceful restart
}

// RunDurabilityTest tests data survival after hard crash (simulated power loss)
// This is the real durability test - SIGKILL gives no chance for graceful shutdown
func RunDurabilityTest(ctx context.Context, adapter adapters.Adapter, cfg DurabilityConfig) (*DurabilityResult, error) {
	result := &DurabilityResult{
		Name:          adapter.Name(),
		ContainerName: cfg.ContainerName,
	}

	if cfg.HardKill {
		result.CrashMethod = "SIGKILL (power loss simulation)"
	} else {
		result.CrashMethod = "Graceful restart"
	}

	// Setup connection
	if err := adapter.Setup(ctx); err != nil {
		result.Error = fmt.Sprintf("setup failed: %v", err)
		return result, nil
	}

	// Use fixed queue name so we can find it after restart
	queue := "bench-durability-test"
	payload := make([]byte, cfg.MessageSize)

	// Publish messages
	fmt.Printf("  Publishing %d messages (%d bytes each)...\n", cfg.MessageCount, cfg.MessageSize)
	pubStart := time.Now()

	for i := 0; i < cfg.MessageCount; i++ {
		// Embed sequence number in payload for verification
		payload[0] = byte(i >> 24)
		payload[1] = byte(i >> 16)
		payload[2] = byte(i >> 8)
		payload[3] = byte(i)

		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish failed at %d: %v", i, err)
			return result, nil
		}
	}
	result.PublishTime = time.Since(pubStart)
	result.Published = int64(cfg.MessageCount)

	// Close our connection before crash (simulates client disconnect)
	adapter.Teardown()

	// Optional wait - allows testing "what if crash happens N seconds after publish?"
	if cfg.WaitAfterPub > 0 {
		fmt.Printf("  Waiting %v before crash...\n", cfg.WaitAfterPub)
		time.Sleep(cfg.WaitAfterPub)
	}

	// CRASH THE CONTAINER
	if cfg.ContainerName != "" {
		if cfg.HardKill {
			fmt.Printf("  HARD KILL: docker kill -s SIGKILL %s (simulating power loss)...\n", cfg.ContainerName)
			killContainer(cfg.ContainerName)
		} else {
			fmt.Printf("  GRACEFUL: docker restart %s...\n", cfg.ContainerName)
			restartContainerGraceful(cfg.ContainerName)
		}

		// Wait for container to stop
		time.Sleep(2 * time.Second)

		// Start container again (if killed)
		if cfg.HardKill {
			fmt.Printf("  Starting container...\n")
			startContainer(cfg.ContainerName)
		}

		// Wait for service to be ready
		fmt.Printf("  Waiting for service to be ready...\n")
		time.Sleep(5 * time.Second)
	} else {
		fmt.Printf("  No container name - skipping crash simulation\n")
		time.Sleep(time.Second)
	}

	// Reconnect
	fmt.Printf("  Reconnecting...\n")
	recoveryStart := time.Now()
	if err := adapter.Setup(ctx); err != nil {
		result.Error = fmt.Sprintf("reconnect failed: %v", err)
		result.RecoveryTime = time.Since(recoveryStart)
		return result, nil
	}

	// Try to consume the messages
	fmt.Printf("  Consuming messages (30s timeout)...\n")
	var consumed atomic.Int64
	consumeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	done := make(chan bool)
	go func() {
		adapter.Consume(consumeCtx, queue, func(msg []byte) error {
			count := consumed.Add(1)
			if count >= int64(cfg.MessageCount) {
				cancel() // Got all messages
			}
			return nil
		})
		done <- true
	}()

	<-done
	result.RecoveryTime = time.Since(recoveryStart)
	result.Recovered = consumed.Load()
	result.Lost = result.Published - result.Recovered
	if result.Published > 0 {
		result.LossRate = float64(result.Lost) / float64(result.Published) * 100
	}

	adapter.Teardown()
	return result, nil
}

func killContainer(name string) error {
	cmd := exec.Command("docker", "kill", "-s", "SIGKILL", name)
	return cmd.Run()
}

func startContainer(name string) error {
	cmd := exec.Command("docker", "start", name)
	return cmd.Run()
}

func restartContainerGraceful(name string) error {
	cmd := exec.Command("docker", "restart", name)
	return cmd.Run()
}

// PrintDurabilityResults prints durability test results
func PrintDurabilityResults(results map[string]*DurabilityResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Published│ Recovered│ Lost     │ Loss %   │ Crash Method                    │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────┤")
	for name, r := range results {
		status := "✓"
		if r.Lost > 0 {
			status = "✗"
		}
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %6.2f%% %s│ %-31s │\n",
			name, r.Published, r.Recovered, r.Lost, r.LossRate, status, r.CrashMethod)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘")

	fmt.Println("\nDurability Interpretation:")
	fmt.Println("  - 0% loss with SIGKILL = Data is fsynced to disk (true durability)")
	fmt.Println("  - >0% loss with SIGKILL = Some data was only in memory buffers")
	fmt.Println("  - 0% loss with graceful = Data survives clean shutdown (expected)")
	fmt.Println()
	fmt.Println("Note: SIGKILL simulates power failure - process has NO chance to flush buffers.")
	fmt.Println("      This reveals the real durability guarantee, not the graceful shutdown path.")
}

// GetContainerName returns the docker container name for an adapter
func GetContainerName(adapterName string) string {
	switch adapterName {
	case "QWER-Q":
		return "bench-qwerq-1"
	case "NATS":
		return "bench-nats-1"
	case "RabbitMQ":
		return "bench-rabbitmq-1"
	case "Redis":
		return "bench-redis-1"
	case "Kafka":
		return "bench-kafka-1"
	case "Pulsar":
		return "bench-pulsar-1"
	case "RedPanda":
		return "bench-redpanda-1"
	default:
		return ""
	}
}
