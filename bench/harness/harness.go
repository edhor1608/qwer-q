package harness

import (
	"fmt"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Config holds benchmark configuration.
type Config struct {
	Duration    time.Duration
	MessageSize int
	Concurrency int
	TargetRate  int // msgs/sec for latency test (0 = unlimited for throughput)
}

// DefaultConfig returns default benchmark configuration.
func DefaultConfig() Config {
	return Config{
		Duration:    10 * time.Second,
		MessageSize: 1024,
		Concurrency: 1,
		TargetRate:  10000,
	}
}

// ThroughputResult holds results from a throughput benchmark.
type ThroughputResult struct {
	Name       string
	Published  int64
	MsgsPerSec float64
	Error      error
}

// LatencyResult holds results from a latency benchmark.
type LatencyResult struct {
	Name  string
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
	Error error
}

// Histogram wraps hdrhistogram for latency recording.
type Histogram struct {
	hist *hdrhistogram.Histogram
}

// NewHistogram creates a new histogram for latency recording.
// Records values from 1 microsecond to 10 seconds with 3 significant figures.
func NewHistogram() *Histogram {
	return &Histogram{
		hist: hdrhistogram.New(1, 10_000_000, 3), // 1us to 10s in microseconds
	}
}

// Record records a latency value.
func (h *Histogram) Record(d time.Duration) {
	h.hist.RecordValue(d.Microseconds())
}

// Percentile returns the value at a given percentile.
func (h *Histogram) Percentile(p float64) time.Duration {
	return time.Duration(h.hist.ValueAtQuantile(p)) * time.Microsecond
}

// Max returns the maximum recorded value.
func (h *Histogram) Max() time.Duration {
	return time.Duration(h.hist.Max()) * time.Microsecond
}

// PrintThroughputTable prints throughput results as a formatted table.
func PrintThroughputTable(cfg Config, results []ThroughputResult) {
	fmt.Printf("\nScenario: Throughput (%s, %dB messages)\n", cfg.Duration, cfg.MessageSize)
	fmt.Println("+-------------+------------+------------+")
	fmt.Println("| Queue       | Published  | Msgs/sec   |")
	fmt.Println("+-------------+------------+------------+")
	for _, r := range results {
		if r.Error != nil {
			fmt.Printf("| %-11s | ERROR: %-19s |\n", r.Name, r.Error.Error())
		} else {
			fmt.Printf("| %-11s | %10s | %10s |\n", r.Name, formatNumber(r.Published), formatNumber(int64(r.MsgsPerSec)))
		}
	}
	fmt.Println("+-------------+------------+------------+")
}

// PrintLatencyTable prints latency results as a formatted table.
func PrintLatencyTable(cfg Config, results []LatencyResult) {
	fmt.Printf("\nScenario: Latency (%d msgs/sec, %dB messages)\n", cfg.TargetRate, cfg.MessageSize)
	fmt.Println("+-------------+---------+---------+---------+---------+")
	fmt.Println("| Queue       | p50     | p95     | p99     | max     |")
	fmt.Println("+-------------+---------+---------+---------+---------+")
	for _, r := range results {
		if r.Error != nil {
			fmt.Printf("| %-11s | ERROR: %-27s |\n", r.Name, r.Error.Error())
		} else {
			fmt.Printf("| %-11s | %7s | %7s | %7s | %7s |\n",
				r.Name, formatDuration(r.P50), formatDuration(r.P95), formatDuration(r.P99), formatDuration(r.Max))
		}
	}
	fmt.Println("+-------------+---------+---------+---------+---------+")
}

func formatNumber(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func formatDuration(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}

// Result holds comprehensive benchmark results
type Result struct {
	Queue     string
	Published int64
	Consumed  int64
	Duration  time.Duration
	PubErrors int64
	ConErrors int64
	Samples   []Sample
}

// Sample holds a point-in-time measurement
type Sample struct {
	Timestamp time.Time
	Published int64
	Consumed  int64
	MemAlloc  uint64
}

// SizeResult holds results for message size tests
type SizeResult struct {
	Size       int
	MsgsPerSec float64
	MBPerSec   float64
}

// DepthResult holds results for queue depth tests
type DepthResult struct {
	Prefilled   int
	Consumed    int64
	FillTime    time.Duration
	ConsumeTime time.Duration
	FillRate    float64
	ConsumeRate float64
}

// BurstResult holds results for burst tests
type BurstResult struct {
	BurstSize      int
	BurstInterval  time.Duration
	TotalBursts    int
	TotalPublished int64
	TotalConsumed  int64
	AvgBurstTime   time.Duration
}

// LagResult holds results for consumer lag tests
type LagResult struct {
	ProducerRate  int
	ConsumerDelay time.Duration
	Published     int64
	Consumed      int64
	MaxLag        int64
	FinalLag      int64
}

// PrintSustainedResults prints sustained load test results
func PrintSustainedResults(results []*Result) {
	fmt.Println("\n+-------------+------------+------------+------------+-----------+")
	fmt.Println("| Queue       | Published  | Consumed   | Pub/sec    | Errors    |")
	fmt.Println("+-------------+------------+------------+------------+-----------+")
	for _, r := range results {
		rate := float64(r.Published) / r.Duration.Seconds()
		fmt.Printf("| %-11s | %10s | %10s | %10s | %9d |\n",
			r.Queue, formatNumber(r.Published), formatNumber(r.Consumed), formatNumber(int64(rate)), r.PubErrors)
	}
	fmt.Println("+-------------+------------+------------+------------+-----------+")
}

// PrintSizeResults prints message size test results
func PrintSizeResults(queue string, results []SizeResult) {
	fmt.Printf("\nMessage Size Impact (%s):\n", queue)
	fmt.Println("+----------+------------+------------+")
	fmt.Println("| Size     | Msgs/sec   | MB/sec     |")
	fmt.Println("+----------+------------+------------+")
	for _, r := range results {
		fmt.Printf("| %8s | %10s | %10.1f |\n", formatBytes(r.Size), formatNumber(int64(r.MsgsPerSec)), r.MBPerSec)
	}
	fmt.Println("+----------+------------+------------+")
}

// PrintDepthResults prints queue depth test results
func PrintDepthResults(results map[string]*DepthResult) {
	fmt.Println("\nQueue Depth Test:")
	fmt.Println("+-------------+------------+------------+------------+------------+")
	fmt.Println("| Queue       | Prefilled  | Fill/sec   | Consumed   | Drain/sec  |")
	fmt.Println("+-------------+------------+------------+------------+------------+")
	for name, r := range results {
		fmt.Printf("| %-11s | %10s | %10s | %10s | %10s |\n",
			name, formatNumber(int64(r.Prefilled)), formatNumber(int64(r.FillRate)),
			formatNumber(r.Consumed), formatNumber(int64(r.ConsumeRate)))
	}
	fmt.Println("+-------------+------------+------------+------------+------------+")
}

// PrintBurstResults prints burst test results
func PrintBurstResults(results map[string]*BurstResult) {
	fmt.Println("\nBurst Test:")
	fmt.Println("+-------------+--------+------------+------------+-----------+")
	fmt.Println("| Queue       | Bursts | Published  | Consumed   | Avg Burst |")
	fmt.Println("+-------------+--------+------------+------------+-----------+")
	for name, r := range results {
		fmt.Printf("| %-11s | %6d | %10s | %10s | %9s |\n",
			name, r.TotalBursts, formatNumber(r.TotalPublished),
			formatNumber(r.TotalConsumed), formatDuration(r.AvgBurstTime))
	}
	fmt.Println("+-------------+--------+------------+------------+-----------+")
}

// PrintLagResults prints consumer lag test results
func PrintLagResults(results map[string]*LagResult) {
	fmt.Println("\nConsumer Lag Test:")
	fmt.Println("+-------------+------------+------------+------------+------------+")
	fmt.Println("| Queue       | Published  | Consumed   | Max Lag    | Final Lag  |")
	fmt.Println("+-------------+------------+------------+------------+------------+")
	for name, r := range results {
		fmt.Printf("| %-11s | %10s | %10s | %10s | %10s |\n",
			name, formatNumber(r.Published), formatNumber(r.Consumed),
			formatNumber(r.MaxLag), formatNumber(r.FinalLag))
	}
	fmt.Println("+-------------+------------+------------+------------+------------+")
}

// PrintMemoryOverTime prints memory usage over time
func PrintMemoryOverTime(results []*Result) {
	fmt.Println("\nMemory Usage Over Time:")
	for _, r := range results {
		if len(r.Samples) == 0 {
			continue
		}
		fmt.Printf("\n%s:\n", r.Queue)
		fmt.Println("  Time    | Published  | Consumed   | Memory    ")
		fmt.Println("  --------|------------|------------|----------")
		for i, s := range r.Samples {
			fmt.Printf("  %4ds   | %10s | %10s | %9s\n",
				i+1, formatNumber(s.Published), formatNumber(s.Consumed), formatBytes(int(s.MemAlloc)))
		}
	}
}

func formatBytes(b int) string {
	if b >= 1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	}
	if b >= 1024 {
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	}
	return fmt.Sprintf("%dB", b)
}
