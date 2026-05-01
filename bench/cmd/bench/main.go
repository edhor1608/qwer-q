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

func main() {
	var (
		queueFlag          = flag.String("queue", "", "Run only this queue (qwerq, nats, rabbitmq, redis, kafka, pulsar, redpanda)")
		scenarioFlag       = flag.String("scenario", "", "Run only this scenario (throughput, latency, queue-core, typed-queue, operator-core)")
		duration           = flag.Duration("duration", 10*time.Second, "Benchmark duration")
		messageSize        = flag.Int("message-size", 1024, "Message size in bytes")
		concurrency        = flag.Int("concurrency", 1, "Number of concurrent publishers")
		targetRate         = flag.Int("target-rate", 10000, "Target msgs/sec for latency test")
		orderingMessages   = flag.Int("ordering-messages", 1000, "Number of messages for queue-core ordering test")
		orderingTimeout    = flag.Duration("ordering-timeout", 10*time.Second, "Timeout for queue-core ordering test")
		redeliveryMessages = flag.Int("redelivery-messages", 1, "Number of messages for queue-core redelivery test")
		redeliveryHold     = flag.Duration("redelivery-hold", 2*time.Second, "How long the queue-core redelivery test holds a message before returning")
		redeliveryTimeout  = flag.Duration("redelivery-timeout", 6*time.Second, "Timeout for queue-core redelivery test")
		invalidAttempts    = flag.Int("invalid-attempts", 100, "Number of invalid publishes for typed-queue rejection test")
		depthPrefill       = flag.Int("depth-prefill", 10000, "Number of messages to prefill for operator-core depth test")
		depthDrainDuration = flag.Duration("depth-drain-duration", 10*time.Second, "Drain duration for operator-core depth test")
		durabilityMessages = flag.Int("durability-messages", 1000, "Number of messages for operator-core crash durability test")
		waitAfterPublish   = flag.Duration("wait-after-publish", 0, "How long operator-core waits after publish before hard-kill durability test")

		qwerqAddr       = flag.String("qwerq-addr", "localhost:9876", "QWER-Q server address")
		natsURL         = flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
		rabbitmqURL     = flag.String("rabbitmq-url", "amqp://guest:guest@localhost:5672/", "RabbitMQ URL")
		redisAddr       = flag.String("redis-addr", "localhost:6379", "Redis address")
		kafkaBrokers    = flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
		pulsarURL       = flag.String("pulsar-url", "pulsar://localhost:6650", "Pulsar server URL")
		redpandaBrokers = flag.String("redpanda-brokers", "localhost:9093", "RedPanda broker addresses")
	)
	flag.Parse()

	cfg := harness.Config{
		Duration:    *duration,
		MessageSize: *messageSize,
		Concurrency: *concurrency,
		TargetRate:  *targetRate,
	}

	allAdapters := map[string]adapters.Adapter{
		"qwerq":    adapters.NewQWERQAdapter(*qwerqAddr),
		"nats":     adapters.NewNATSAdapter(*natsURL),
		"rabbitmq": adapters.NewRabbitMQAdapter(*rabbitmqURL),
		"redis":    adapters.NewRedisAdapter(*redisAddr),
		"kafka":    adapters.NewKafkaAdapter(*kafkaBrokers),
		"pulsar":   adapters.NewPulsarAdapter(*pulsarURL),
		"redpanda": adapters.NewRedPandaAdapter(*redpandaBrokers),
	}

	// Filter adapters
	var selectedAdapters []adapters.Adapter
	if *queueFlag != "" {
		for _, q := range strings.Split(*queueFlag, ",") {
			q = strings.TrimSpace(strings.ToLower(q))
			a, ok := allAdapters[q]
			if !ok {
				fmt.Fprintf(os.Stderr, "Unknown queue: %s\n", q)
				os.Exit(1)
			}
			selectedAdapters = append(selectedAdapters, a)
		}
	} else {
		selectedAdapters = []adapters.Adapter{
			allAdapters["qwerq"],
			allAdapters["nats"],
			allAdapters["rabbitmq"],
			allAdapters["redis"],
			allAdapters["kafka"],
			allAdapters["pulsar"],
			allAdapters["redpanda"],
		}
	}

	fmt.Println("QWER-Q Benchmark Suite")
	fmt.Println("======================")

	ctx := context.Background()

	// Throughput test
	if *scenarioFlag == "" || *scenarioFlag == "throughput" {
		var results []harness.ThroughputResult
		for _, a := range selectedAdapters {
			fmt.Printf("Running throughput test for %s...\n", a.Name())
			results = append(results, scenarios.RunThroughput(ctx, a, cfg))
		}
		harness.PrintThroughputTable(cfg, results)
	}

	// Latency test
	if *scenarioFlag == "" || *scenarioFlag == "latency" {
		var results []harness.LatencyResult
		for _, a := range selectedAdapters {
			fmt.Printf("Running latency test for %s...\n", a.Name())
			results = append(results, scenarios.RunLatency(ctx, a, cfg))
		}
		harness.PrintLatencyTable(cfg, results)
	}

	// Queue-core product benchmark
	if *scenarioFlag == "queue-core" {
		queueCoreCfg := scenarios.QueueCoreConfig{
			Config:             cfg,
			OrderingMessages:   *orderingMessages,
			OrderingTimeout:    *orderingTimeout,
			RedeliveryMessages: *redeliveryMessages,
			RedeliveryHold:     *redeliveryHold,
			RedeliveryTimeout:  *redeliveryTimeout,
		}
		var results []harness.QueueCoreResult
		for _, a := range selectedAdapters {
			fmt.Printf("Running queue-core benchmark for %s...\n", a.Name())
			results = append(results, scenarios.RunQueueCore(ctx, a, queueCoreCfg))
		}
		harness.PrintQueueCoreTable(cfg, results)
	}

	if *scenarioFlag == "typed-queue" {
		typedCfg := scenarios.TypedQueueConfig{
			Config:          cfg,
			InvalidAttempts: *invalidAttempts,
		}
		var results []harness.TypedQueueResult
		for _, a := range selectedAdapters {
			fmt.Printf("Running typed-queue benchmark for %s...\n", a.Name())
			results = append(results, scenarios.RunTypedQueue(ctx, a, typedCfg))
		}
		harness.PrintTypedQueueTable(cfg, results)
	}

	if *scenarioFlag == "operator-core" {
		opCfg := scenarios.OperatorCoreConfig{
			MessageSize:        *messageSize,
			DepthPrefill:       *depthPrefill,
			DepthDrainDuration: *depthDrainDuration,
			DurabilityMessages: *durabilityMessages,
			WaitAfterPublish:   *waitAfterPublish,
		}
		var results []harness.OperatorCoreResult
		for _, a := range selectedAdapters {
			fmt.Printf("Running operator-core benchmark for %s...\n", a.Name())
			results = append(results, scenarios.RunOperatorCore(ctx, a, opCfg))
		}
		harness.PrintOperatorCoreTable(results)
	}
}
