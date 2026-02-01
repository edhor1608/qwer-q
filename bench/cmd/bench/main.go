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
		queueFlag    = flag.String("queue", "", "Run only this queue (qwerq, nats, rabbitmq, redis, kafka, pulsar, redpanda)")
		scenarioFlag = flag.String("scenario", "", "Run only this scenario (throughput, latency)")
		duration     = flag.Duration("duration", 10*time.Second, "Benchmark duration")
		messageSize  = flag.Int("message-size", 1024, "Message size in bytes")
		concurrency  = flag.Int("concurrency", 1, "Number of concurrent publishers")
		targetRate   = flag.Int("target-rate", 10000, "Target msgs/sec for latency test")

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
		a, ok := allAdapters[strings.ToLower(*queueFlag)]
		if !ok {
			fmt.Fprintf(os.Stderr, "Unknown queue: %s\n", *queueFlag)
			os.Exit(1)
		}
		selectedAdapters = []adapters.Adapter{a}
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
}
