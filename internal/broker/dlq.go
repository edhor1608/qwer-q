package broker

// FailurePolicy defines how to handle messages that exceed max retries.
type FailurePolicy string

const (
	// FailurePolicyDLQ moves failed messages to a dead letter queue.
	FailurePolicyDLQ FailurePolicy = "dlq"
	// FailurePolicyDrop discards failed messages.
	FailurePolicyDrop FailurePolicy = "drop"
	// FailurePolicyInfinite retries forever.
	FailurePolicyInfinite FailurePolicy = "infinite"
)

// DefaultMaxRetries is the default number of delivery attempts before DLQ.
const DefaultMaxRetries uint32 = 5

// DLQSuffix is appended to queue name for dead letter queue.
const DLQSuffix = ".dlq"

// DLQName returns the dead letter queue name for the given queue.
func DLQName(queue string) string {
	return queue + DLQSuffix
}

// IsDLQ returns true if the queue name is a dead letter queue.
func IsDLQ(queue string) bool {
	return len(queue) > len(DLQSuffix) && queue[len(queue)-len(DLQSuffix):] == DLQSuffix
}
