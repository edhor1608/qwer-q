package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
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
	queues          = flag.String("queues", "qwerq,nats,rabbitmq,redis,kafka,pulsar,redpanda", "Comma-separated list of queues to test")
	tests           = flag.String("tests", "all", "Tests to run: all, sustained, concurrency, sizes, depth, burst, lag")
	duration        = flag.Duration("duration", 60*time.Second, "Duration for sustained tests")
	messageSize     = flag.Int("message-size", 1024, "Message size in bytes")
)

func main() {
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║          QWER-Q Deep Stress Testing Suite                     ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx := context.Background()

	// Build adapter list
	queueList := strings.Split(*queues, ",")
	adapterList := make([]adapters.Adapter, 0)
	for _, q := range queueList {
		q = strings.TrimSpace(q)
		switch q {
		case "qwerq":
			adapterList = append(adapterList, adapters.NewQWERQAdapter(*qwerqAddr))
		case "nats":
			adapterList = append(adapterList, adapters.NewNATSAdapter(*natsURL))
		case "rabbitmq":
			adapterList = append(adapterList, adapters.NewRabbitMQAdapter(*rabbitmqURL))
		case "redis":
			adapterList = append(adapterList, adapters.NewRedisAdapter(*redisAddr))
		case "kafka":
			adapterList = append(adapterList, adapters.NewKafkaAdapter(*kafkaBrokers))
		case "pulsar":
			adapterList = append(adapterList, adapters.NewPulsarAdapter(*pulsarURL))
		case "redpanda":
			adapterList = append(adapterList, adapters.NewRedPandaAdapter(*redpandaBrokers))
		default:
			fmt.Printf("Unknown queue: %s\n", q)
			os.Exit(1)
		}
	}

	testList := strings.Split(*tests, ",")
	runAll := contains(testList, "all")

	// 1. Sustained Load Test (60s default)
	if runAll || contains(testList, "sustained") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Printf("TEST 1: Sustained Load (%s, %d byte messages)\n", *duration, *messageSize)
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("Testing sustained throughput over extended period...")
		fmt.Println()

		results := make([]*harness.Result, 0)
		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			cfg := scenarios.StressConfig{
				Duration:    *duration,
				MessageSize: *messageSize,
				Producers:   1,
				Consumers:   1,
			}
			result, err := scenarios.RunSustainedLoad(ctx, adapter, cfg)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results = append(results, result)
		}
		harness.PrintSustainedResults(results)
		harness.PrintMemoryOverTime(results)
	}

	// 2. High Concurrency Test
	if runAll || contains(testList, "concurrency") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST 2: High Concurrency (10 producers, 10 consumers)")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make([]*harness.Result, 0)
		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			cfg := scenarios.StressConfig{
				Duration:    30 * time.Second,
				MessageSize: *messageSize,
				Producers:   10,
				Consumers:   10,
			}
			result, err := scenarios.RunHighConcurrency(ctx, adapter, cfg)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results = append(results, result)
		}
		harness.PrintSustainedResults(results)
	}

	// 3. Message Size Impact
	if runAll || contains(testList, "sizes") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST 3: Message Size Impact (64B to 256KB)")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			results, err := scenarios.RunMessageSizes(ctx, adapter, 10*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			harness.PrintSizeResults(adapter.Name(), results)
		}
	}

	// 4. Queue Depth Test
	if runAll || contains(testList, "depth") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST 4: Queue Depth (pre-fill 500K messages, then drain)")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*harness.DepthResult)
		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			result, err := scenarios.RunQueueDepth(ctx, adapter, 500000, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[adapter.Name()] = result
		}
		harness.PrintDepthResults(results)
	}

	// 5. Burst Traffic Test
	if runAll || contains(testList, "burst") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST 5: Burst Traffic (1000 msgs every 100ms for 30s)")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*harness.BurstResult)
		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			result, err := scenarios.RunBurstTest(ctx, adapter, 1000, 100*time.Millisecond, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[adapter.Name()] = result
		}
		harness.PrintBurstResults(results)
	}

	// 6. Consumer Lag Test
	if runAll || contains(testList, "lag") {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("TEST 6: Consumer Lag (producer 1000/s, consumer with 10ms delay)")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println()

		results := make(map[string]*harness.LagResult)
		for _, adapter := range adapterList {
			fmt.Printf("Running %s...\n", adapter.Name())
			result, err := scenarios.RunConsumerLag(ctx, adapter, 1000, 10*time.Millisecond, 30*time.Second)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}
			results[adapter.Name()] = result
		}
		harness.PrintLagResults(results)
	}

	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Stress Testing Complete!")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if strings.TrimSpace(v) == item {
			return true
		}
	}
	return false
}
