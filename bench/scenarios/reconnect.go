package scenarios

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// ReconnectResult captures broker restart reconnection test results
type ReconnectResult struct {
	Name               string
	MessagesBeforeStop int64
	MessagesAfterStart int64
	TotalMessages      int64
	BrokerDowntime     time.Duration
	ReconnectTime      time.Duration // time from broker up to first message after
	AutoReconnect      bool          // did client reconnect automatically?
	MessagesContinued  bool          // did consumption resume?
	Behavior           string
	Error              string
}

// RunReconnectTest tests client auto-reconnection after broker restart
// Publishes messages, restarts broker container, checks if client reconnects
func RunReconnectTest(ctx context.Context, adapter adapters.Adapter, containerName string, messageCount int, timeout time.Duration) (*ReconnectResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-reconnect-%d", time.Now().UnixNano())
	payload := make([]byte, 256)

	result := &ReconnectResult{
		Name:          adapter.Name(),
		TotalMessages: int64(messageCount),
	}

	// Pre-publish all messages
	fmt.Printf("  Publishing %d messages...\n", messageCount)
	for i := 0; i < messageCount; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish error: %v", err)
			return result, nil
		}
	}

	// Start consuming in background
	var consumedBefore, consumedAfter atomic.Int64
	var brokerStopped, brokerStarted atomic.Bool
	var firstAfterRestart atomic.Int64

	consumeCtx, cancelConsume := context.WithTimeout(ctx, timeout)
	defer cancelConsume()

	restartTime := time.Now()
	var brokerUpTime time.Time

	go func() {
		adapter.Consume(consumeCtx, queue, func(msg []byte) error {
			if brokerStarted.Load() {
				if consumedAfter.Add(1) == 1 {
					// First message after restart - record reconnect time
					firstAfterRestart.Store(time.Since(brokerUpTime).Milliseconds())
				}
			} else if !brokerStopped.Load() {
				consumedBefore.Add(1)
			}
			return nil
		})
	}()

	// Let consumer start and process some messages
	time.Sleep(500 * time.Millisecond)

	// Restart the broker container
	fmt.Printf("  Restarting container '%s'...\n", containerName)
	brokerStopped.Store(true)
	restartTime = time.Now()

	restartContainer(containerName)

	brokerUpTime = time.Now()
	result.BrokerDowntime = brokerUpTime.Sub(restartTime)
	brokerStarted.Store(true)

	fmt.Printf("  Broker restarted (downtime: %v), waiting for reconnect...\n", result.BrokerDowntime.Round(time.Millisecond))

	// Wait for potential reconnection and message consumption
	time.Sleep(5 * time.Second)

	// Cancel and collect results
	cancelConsume()
	time.Sleep(100 * time.Millisecond)

	result.MessagesBeforeStop = consumedBefore.Load()
	result.MessagesAfterStart = consumedAfter.Load()

	if firstAfterRestart.Load() > 0 {
		result.ReconnectTime = time.Duration(firstAfterRestart.Load()) * time.Millisecond
	}

	result.AutoReconnect = result.MessagesAfterStart > 0
	result.MessagesContinued = result.MessagesAfterStart > 0

	// Determine behavior
	switch {
	case result.MessagesAfterStart > 0:
		result.Behavior = fmt.Sprintf("Auto-reconnect in %v", result.ReconnectTime.Round(time.Millisecond))
	case result.MessagesBeforeStop > 0:
		result.Behavior = "No auto-reconnect (consumed before only)"
	default:
		result.Behavior = "No messages consumed"
	}

	return result, nil
}

// PrintReconnectResults prints reconnection test results
func PrintReconnectResults(results map[string]*ReconnectResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Before   │ After    │ Downtime │ Reconn   │ Auto?    │ Behavior                        │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────┤")
	for name, r := range results {
		auto := "No"
		if r.AutoReconnect {
			auto = "Yes"
		}
		reconnect := "-"
		if r.ReconnectTime > 0 {
			reconnect = r.ReconnectTime.Round(time.Millisecond).String()
		}
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8s │ %8s │ %-8s │ %-31s │\n",
			name, r.MessagesBeforeStop, r.MessagesAfterStart,
			r.BrokerDowntime.Round(time.Millisecond).String(),
			reconnect, auto, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘")

	fmt.Println("\nReconnection Behavior:")
	fmt.Println("  - Auto-reconnect: Client library handles reconnection transparently")
	fmt.Println("  - No auto-reconnect: Application must detect and reconnect manually")
	fmt.Println("  - Best practice: Auto-reconnect with exponential backoff")
	fmt.Println("  - Note: Message loss during downtime depends on persistence settings")
}
