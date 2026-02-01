package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/scenarios"
)

var (
	qwerqAddr       = flag.String("qwerq-addr", "localhost:9876", "QWER-Q server address")
	natsURL         = flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	rabbitmqURL     = flag.String("rabbitmq-url", "amqp://guest:guest@localhost:5672/", "RabbitMQ URL")
	redisAddr       = flag.String("redis-addr", "localhost:6379", "Redis address")
	kafkaBrokers    = flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
	pulsarURL       = flag.String("pulsar-url", "pulsar://localhost:6650", "Pulsar server URL")
	redpandaBrokers = flag.String("redpanda-brokers", "localhost:9093", "RedPanda broker addresses")
	queues          = flag.String("queues", "qwerq,nats,rabbitmq,kafka,redis,pulsar,redpanda", "Comma-separated list of queues")
	tests           = flag.String("tests", "all", "Tests: all, breaking, memory, connections, recovery, durability, ordering, exactlyonce, backpressure, redelivery, poison, fairness, network, ttl, largemsg, diskfull, reconnect, manyqueues")
	skipDocker      = flag.Bool("skip-docker", false, "Skip Docker setup (assume containers running)")
)

func main() {
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       QWER-Q Weakness Finding & Breaking Point Suite          ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Goal: Find weaknesses, breaking points, and failure modes")
	fmt.Println("All queues run in Docker with identical resource limits:")
	fmt.Println("  - CPU: 1 core")
	fmt.Println("  - Memory: 512MB")
	fmt.Println()

	ctx := context.Background()

	if !*skipDocker {
		fmt.Println("Starting Docker containers...")
		if err := startContainers(); err != nil {
			fmt.Printf("Failed to start containers: %v\n", err)
			fmt.Println("Run with --skip-docker if containers are already running")
			os.Exit(1)
		}
		fmt.Println("Waiting for services to be ready...")
		time.Sleep(10 * time.Second)
	}

	// Build adapter list
	queueList := strings.Split(*queues, ",")
	adapterMap := make(map[string]adapters.Adapter)
	for _, q := range queueList {
		q = strings.TrimSpace(q)
		switch q {
		case "qwerq":
			adapterMap[q] = adapters.NewQWERQAdapter(*qwerqAddr)
		case "nats":
			adapterMap[q] = adapters.NewNATSAdapter(*natsURL)
		case "rabbitmq":
			adapterMap[q] = adapters.NewRabbitMQAdapter(*rabbitmqURL)
		case "redis":
			adapterMap[q] = adapters.NewRedisAdapter(*redisAddr)
		case "kafka":
			adapterMap[q] = adapters.NewKafkaAdapter(*kafkaBrokers)
		case "pulsar":
			adapterMap[q] = adapters.NewPulsarAdapter(*pulsarURL)
		case "redpanda":
			adapterMap[q] = adapters.NewRedPandaAdapter(*redpandaBrokers)
		}
	}

	testList := strings.Split(*tests, ",")
	runAll := contains(testList, "all")

	// TEST 1: Breaking Point Analysis
	if runAll || contains(testList, "breaking") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Breaking Point Analysis")
		fmt.Println("Find the throughput where each queue starts failing")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.BreakingPointResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			containerName := fmt.Sprintf("bench-%s-1", name)
			result, err := scenarios.RunBreakingPointTest(ctx, adapter, containerName)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintBreakingPointResults(results)
	}

	// TEST 2: Memory Pressure
	if runAll || contains(testList, "memory") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Memory Pressure")
		fmt.Println("Publish 10KB messages until memory limit is reached")
		fmt.Println("Container limit: 512MB")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		// 512MB / 10KB = ~50,000 messages to fill memory
		maxMessages := 50000

		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunMemoryPressureTest(ctx, adapter, maxMessages)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			rate := float64(result.Published) / result.Duration.Seconds()
			fmt.Printf("  Published: %d, Rate: %.0f/s, Errors: %d\n",
				result.Published, rate, result.PubErrors)

			// Get container memory stats
			stats := getContainerStats(name)
			if stats != nil {
				fmt.Printf("  Memory: %.1fMB / %.1fMB (%.1f%%)\n",
					stats.MemoryMB, stats.MemoryLimitMB, stats.MemoryPercent)
			}
		}
	}

	// TEST 3: Connection Storm
	if runAll || contains(testList, "connections") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Connection Storm")
		fmt.Println("Rapidly create 100 concurrent connections")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.ConnectionStormResult)
		for name := range adapterMap {
			fmt.Printf("Testing %s...\n", name)

			var factory func() adapters.Adapter
			switch name {
			case "qwerq":
				factory = func() adapters.Adapter { return adapters.NewQWERQAdapter(*qwerqAddr) }
			case "nats":
				factory = func() adapters.Adapter { return adapters.NewNATSAdapter(*natsURL) }
			case "rabbitmq":
				factory = func() adapters.Adapter { return adapters.NewRabbitMQAdapter(*rabbitmqURL) }
			case "redis":
				factory = func() adapters.Adapter { return adapters.NewRedisAdapter(*redisAddr) }
			case "kafka":
				factory = func() adapters.Adapter { return adapters.NewKafkaAdapter(*kafkaBrokers) }
			case "pulsar":
				factory = func() adapters.Adapter { return adapters.NewPulsarAdapter(*pulsarURL) }
			case "redpanda":
				factory = func() adapters.Adapter { return adapters.NewRedPandaAdapter(*redpandaBrokers) }
			}

			result, err := scenarios.RunConnectionStormTest(ctx, factory, 100)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintConnectionStormResults(results)
	}

	// TEST 4: Crash Recovery
	if runAll || contains(testList, "recovery") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Crash Recovery")
		fmt.Println("Publish messages, simulate crash, verify data survives")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.RecoveryResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunCrashRecoveryTest(ctx, adapter, 10000)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintRecoveryResults(results)
	}

	// TEST 5: Durability (Hard Crash / Power Loss Simulation)
	if runAll || contains(testList, "durability") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Durability (Power Loss Simulation)")
		fmt.Println("SIGKILL container after publishing - no graceful shutdown")
		fmt.Println("This reveals TRUE durability: was data fsynced to disk?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.DurabilityResult)
		for name, adapter := range adapterMap {
			containerName := scenarios.GetContainerName(adapter.Name())
			if containerName == "" {
				fmt.Printf("Skipping %s (no container mapping)\n", name)
				continue
			}

			fmt.Printf("Testing %s...\n", name)
			cfg := scenarios.DurabilityConfig{
				MessageCount:  1000,
				MessageSize:   1024,
				ContainerName: containerName,
				WaitAfterPub:  0, // Immediate crash after publish
				HardKill:      true,
			}
			result, err := scenarios.RunDurabilityTest(ctx, adapter, cfg)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintDurabilityResults(results)
	}

	// TEST 6: Ordering Guarantees
	if runAll || contains(testList, "ordering") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Ordering Guarantees")
		fmt.Println("Verify FIFO ordering with sequence numbers")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.OrderingResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunOrderingTest(ctx, adapter, 10000, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintOrderingResults(results)
	}

	// TEST 6: Exactly-Once Semantics
	if runAll || contains(testList, "exactlyonce") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Exactly-Once Semantics")
		fmt.Println("Test deduplication behavior with repeated message IDs")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.ExactlyOnceResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunExactlyOnceTest(ctx, adapter, 1000, 3, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintExactlyOnceResults(results)
	}

	// TEST 7: Backpressure Behavior
	if runAll || contains(testList, "backpressure") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Backpressure Behavior")
		fmt.Println("Fast producer, slow consumer - observe system response")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.BackpressureResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunBackpressureTest(ctx, adapter, 30*time.Second, 10*time.Millisecond)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintBackpressureResults(results)
	}

	// TEST 8: Redelivery Timeout
	if runAll || contains(testList, "redelivery") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Redelivery Timeout")
		fmt.Println("Consumer holds message without ACK - does it get redelivered?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.RedeliveryResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunRedeliveryTest(ctx, adapter, 10, 5*time.Second, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintRedeliveryResults(results)
	}

	// TEST 9: Poison Message Handling
	if runAll || contains(testList, "poison") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Poison Message Handling")
		fmt.Println("Message always fails - infinite retry? DLQ? Queue blocked?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.PoisonResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunPoisonMessageTest(ctx, adapter, 10, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintPoisonResults(results)
	}

	// TEST 10: Multi-Consumer Fairness
	if runAll || contains(testList, "fairness") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Multi-Consumer Fairness")
		fmt.Println("Are messages distributed evenly among N consumers?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.FairnessResult)
		for name := range adapterMap {
			fmt.Printf("Testing %s...\n", name)

			var factory func() adapters.Adapter
			switch name {
			case "qwerq":
				factory = func() adapters.Adapter { return adapters.NewQWERQAdapter(*qwerqAddr) }
			case "nats":
				factory = func() adapters.Adapter { return adapters.NewNATSAdapter(*natsURL) }
			case "rabbitmq":
				factory = func() adapters.Adapter { return adapters.NewRabbitMQAdapter(*rabbitmqURL) }
			case "redis":
				factory = func() adapters.Adapter { return adapters.NewRedisAdapter(*redisAddr) }
			case "kafka":
				factory = func() adapters.Adapter { return adapters.NewKafkaAdapter(*kafkaBrokers) }
			case "pulsar":
				factory = func() adapters.Adapter { return adapters.NewPulsarAdapter(*pulsarURL) }
			case "redpanda":
				factory = func() adapters.Adapter { return adapters.NewRedPandaAdapter(*redpandaBrokers) }
			}

			if factory == nil {
				continue
			}

			result, err := scenarios.RunFairnessTest(ctx, factory, 5, 1000, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintFairnessResults(results)
	}

	// TEST 11: Network Disruption
	if runAll || contains(testList, "network") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Network Disruption")
		fmt.Println("Disconnect/reconnect container network - observe behavior")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.NetworkResult)
		for name, adapter := range adapterMap {
			containerName := scenarios.GetContainerName(adapter.Name())
			if containerName == "" {
				fmt.Printf("Skipping %s (no container mapping)\n", name)
				continue
			}

			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunNetworkTimeoutTest(ctx, adapter, containerName, "bench_default")
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintNetworkResults(results)
	}

	// TEST 12: Message TTL/Expiry
	if runAll || contains(testList, "ttl") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Message TTL/Expiry")
		fmt.Println("Do undelivered messages expire?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.TTLResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunTTLTest(ctx, adapter, 100, 10*time.Second, 15*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintTTLResults(results)
	}

	// TEST 13: Large Message Handling
	if runAll || contains(testList, "largemsg") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Large Message Handling")
		fmt.Println("Testing messages from 10KB to 10MB")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.LargeMsgResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunLargeMessageTest(ctx, adapter, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintLargeMsgResults(results)
	}

	// TEST 14: Disk Full Handling
	if runAll || contains(testList, "diskfull") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Disk Full Handling")
		fmt.Println("Publish large messages until storage fills - graceful error?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.DiskFullResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunDiskFullTest(ctx, adapter, 60*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintDiskFullResults(results)
	}

	// TEST 15: Broker Restart Reconnect
	if runAll || contains(testList, "reconnect") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Broker Restart Reconnect")
		fmt.Println("Restart broker - does client auto-reconnect?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.ReconnectResult)
		for name, adapter := range adapterMap {
			containerName := scenarios.GetContainerName(adapter.Name())
			if containerName == "" {
				fmt.Printf("Skipping %s (no container mapping)\n", name)
				continue
			}

			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunReconnectTest(ctx, adapter, containerName, 1000, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintReconnectResults(results)
	}

	// TEST 16: Many Queues Scalability
	if runAll || contains(testList, "manyqueues") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST: Many Queues Scalability")
		fmt.Println("Performance with 1/10/50/100 queues - throughput degradation?")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*scenarios.ManyQueuesResult)
		for name, adapter := range adapterMap {
			fmt.Printf("Testing %s...\n", name)
			result, err := scenarios.RunManyQueuesTest(ctx, adapter, 100, 60*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[name] = result
		}
		scenarios.PrintManyQueuesResults(results)
	}

	// Print container resource usage summary
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Container Resource Usage (via docker stats)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	printDockerStats()

	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Weakness Analysis Complete!")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func startContainers() error {
	cmd := exec.Command("docker", "compose", "-f", "bench/docker-compose.yml", "up", "-d", "--build")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if strings.TrimSpace(v) == item {
			return true
		}
	}
	return false
}

type ContainerStats struct {
	Name           string
	CPUPercent     float64
	MemoryMB       float64
	MemoryLimitMB  float64
	MemoryPercent  float64
	NetworkRxMB    float64
	NetworkTxMB    float64
}

func getContainerStats(name string) *ContainerStats {
	containerName := fmt.Sprintf("bench-%s-1", name)

	// Use docker stats API
	url := fmt.Sprintf("http://localhost:8080/api/v1.3/docker/%s", containerName)
	resp, err := http.Get(url)
	if err != nil {
		// Fallback to docker stats command
		return getContainerStatsFromDocker(containerName)
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return getContainerStatsFromDocker(containerName)
	}

	// Parse cAdvisor response (simplified)
	return &ContainerStats{Name: containerName}
}

func getContainerStatsFromDocker(containerName string) *ContainerStats {
	cmd := exec.Command("docker", "stats", containerName, "--no-stream", "--format",
		"{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}")
	output, err := cmd.Output()
	if err != nil {
		return nil
	}

	stats := &ContainerStats{Name: containerName}
	// Parse output (format: "0.50%	100MiB / 512MiB	19.53%")
	parts := strings.Split(strings.TrimSpace(string(output)), "\t")
	if len(parts) >= 3 {
		fmt.Sscanf(strings.TrimSuffix(parts[0], "%"), "%f", &stats.CPUPercent)
		fmt.Sscanf(strings.TrimSuffix(parts[2], "%"), "%f", &stats.MemoryPercent)

		memParts := strings.Split(parts[1], " / ")
		if len(memParts) == 2 {
			stats.MemoryMB = parseMemory(memParts[0])
			stats.MemoryLimitMB = parseMemory(memParts[1])
		}
	}
	return stats
}

func parseMemory(s string) float64 {
	s = strings.TrimSpace(s)
	var value float64
	if strings.HasSuffix(s, "GiB") {
		fmt.Sscanf(s, "%fGiB", &value)
		return value * 1024
	}
	if strings.HasSuffix(s, "MiB") {
		fmt.Sscanf(s, "%fMiB", &value)
		return value
	}
	if strings.HasSuffix(s, "KiB") {
		fmt.Sscanf(s, "%fKiB", &value)
		return value / 1024
	}
	return 0
}

func printDockerStats() {
	cmd := exec.Command("docker", "stats", "--no-stream", "--format",
		"table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}
