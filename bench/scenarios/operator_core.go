package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// OperatorCoreConfig configures the operator-efficiency benchmark layer.
type OperatorCoreConfig struct {
	MessageSize        int
	DepthPrefill       int
	DepthDrainDuration time.Duration
	DurabilityMessages int
	WaitAfterPublish   time.Duration
}

// RunOperatorCore runs the first operator-efficiency benchmark layer.
func RunOperatorCore(ctx context.Context, adapter adapters.Adapter, cfg OperatorCoreConfig) harness.OperatorCoreResult {
	result := harness.OperatorCoreResult{Name: adapter.Name()}

	depth, err := RunQueueDepth(ctx, adapter, cfg.DepthPrefill, cfg.DepthDrainDuration)
	if err != nil {
		result.Error = fmt.Errorf("queue depth: %w", err)
		return result
	}
	result.DrainRate = depth.ConsumeRate

	containerName := GetContainerName(adapter.Name())
	if containerName == "" {
		result.Notes = "no crash container mapping"
		return result
	}

	durability, err := RunDurabilityTest(ctx, adapter, DurabilityConfig{
		MessageCount:  cfg.DurabilityMessages,
		MessageSize:   cfg.MessageSize,
		ContainerName: containerName,
		WaitAfterPub:  cfg.WaitAfterPublish,
		HardKill:      true,
	})
	if err != nil {
		result.Error = fmt.Errorf("durability: %w", err)
		return result
	}
	if durability.Error != "" {
		result.Error = fmt.Errorf("durability: %s", durability.Error)
		return result
	}
	result.LossRate = durability.LossRate
	result.RecoveryTime = durability.RecoveryTime
	result.Notes = durability.CrashMethod
	return result
}
