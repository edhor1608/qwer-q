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
