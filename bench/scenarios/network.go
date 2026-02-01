package scenarios

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// NetworkResult captures network disruption test results
type NetworkResult struct {
	Name               string
	ContainerName      string
	PublishedBefore    int64
	PublishedDuring    int64
	PublishedAfter     int64
	ErrorsDuring       int64
	ErrorsAfter        int64
	RecoveryTime       time.Duration
	Behavior           string
	Error              string
}

// RunNetworkTimeoutTest tests behavior when network connection is disrupted
// Uses docker network disconnect/connect to simulate network partition
func RunNetworkTimeoutTest(ctx context.Context, adapter adapters.Adapter, containerName string, networkName string) (*NetworkResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-network-%d", time.Now().UnixNano())
	payload := make([]byte, 256)

	result := &NetworkResult{
		Name:          adapter.Name(),
		ContainerName: containerName,
	}

	if containerName == "" {
		result.Error = "No container name provided"
		return result, nil
	}

	if networkName == "" {
		networkName = "bench_default"
	}

	// Phase 1: Publish before disruption
	fmt.Printf("  Phase 1: Publishing before disruption...\n")
	for i := 0; i < 100; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			break
		}
		result.PublishedBefore++
	}

	// Phase 2: Disconnect network and try to publish
	fmt.Printf("  Phase 2: Disconnecting network from %s...\n", containerName)
	if err := disconnectNetwork(containerName, networkName); err != nil {
		result.Error = fmt.Sprintf("disconnect failed: %v", err)
		return result, nil
	}

	// Give network disconnect time to take effect
	time.Sleep(time.Second)

	// Try to publish during disruption (should fail or timeout)
	fmt.Printf("  Publishing during disruption...\n")
	disruptCtx, disruptCancel := context.WithTimeout(ctx, 10*time.Second)
	for i := 0; i < 50; i++ {
		if err := adapter.Publish(disruptCtx, queue, payload); err != nil {
			result.ErrorsDuring++
		} else {
			result.PublishedDuring++
		}
		// Don't spam during failure
		if result.ErrorsDuring > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	disruptCancel()

	// Phase 3: Reconnect network
	fmt.Printf("  Phase 3: Reconnecting network...\n")
	reconnectStart := time.Now()
	if err := connectNetwork(containerName, networkName); err != nil {
		result.Error = fmt.Sprintf("reconnect failed: %v", err)
		return result, nil
	}

	// Wait for connection to be re-established
	time.Sleep(2 * time.Second)

	// Try to publish after recovery
	fmt.Printf("  Publishing after recovery...\n")
	recoverySuccess := false
	for i := 0; i < 100; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.ErrorsAfter++
		} else {
			if !recoverySuccess {
				result.RecoveryTime = time.Since(reconnectStart)
				recoverySuccess = true
			}
			result.PublishedAfter++
		}
	}

	// Determine behavior
	switch {
	case result.ErrorsDuring == 0:
		result.Behavior = "Buffered during disruption (no errors)"
	case result.ErrorsAfter == 0 && result.PublishedAfter > 0:
		result.Behavior = fmt.Sprintf("Recovered in %v", result.RecoveryTime.Round(time.Millisecond))
	case result.ErrorsAfter > 0 && result.PublishedAfter > 0:
		result.Behavior = "Partial recovery (some errors persist)"
	case result.PublishedAfter == 0:
		result.Behavior = "FAILED TO RECOVER - connection dead"
	default:
		result.Behavior = "Unknown behavior"
	}

	return result, nil
}

func disconnectNetwork(containerName, networkName string) error {
	cmd := exec.Command("docker", "network", "disconnect", networkName, containerName)
	return cmd.Run()
}

func connectNetwork(containerName, networkName string) error {
	cmd := exec.Command("docker", "network", "connect", networkName, containerName)
	return cmd.Run()
}

// PrintNetworkResults prints network disruption test results
func PrintNetworkResults(results map[string]*NetworkResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Before   │ During   │ Errors   │ After    │ Recovery │ Behavior                        │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────┤")
	for name, r := range results {
		recoveryStr := "-"
		if r.RecoveryTime > 0 {
			recoveryStr = r.RecoveryTime.Round(time.Millisecond).String()
		}
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %8d │ %8s │ %-31s │\n",
			name, r.PublishedBefore, r.PublishedDuring, r.ErrorsDuring, r.PublishedAfter, recoveryStr, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘")

	fmt.Println("\nNetwork Disruption Handling:")
	fmt.Println("  - 'Buffered': Client queued messages locally during disruption")
	fmt.Println("  - 'Recovered': Connection automatically restored after network returned")
	fmt.Println("  - 'FAILED TO RECOVER': Connection died and didn't reconnect (bad)")
}
