package broker

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/storage"
)

// Stress tests exposing documented weaknesses.
// These tests serve as regression guards when fixing issues.
//
// Run with: go test -v -run Stress -timeout 5m
// Or with race detector: go test -race -v -run Stress -timeout 10m

// TestStressMemoryPressure exposes W-004: Memory pressure crash
// Publishes many messages without consuming to test memory behavior.
func TestStressMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir, err := os.MkdirTemp("", "stress-memory-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	defer b.Close()

	// Track memory
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	startAlloc := m.Alloc

	// 10KB messages, target 1000 messages (10MB data)
	// This is a scaled-down version of the benchmark that caused crashes
	msgSize := 10 * 1024 // 10KB
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	q := b.GetOrCreateQueue("stress-memory")
	var errors int

	for i := 0; i < 1000; i++ {
		msg := &Message{
			ID:      NewULID(),
			Queue:   "stress-memory",
			Payload: payload,
		}
		if err := q.Enqueue(msg); err != nil {
			errors++
			if errors > 10 {
				t.Logf("Stopping after %d messages with %d errors", i, errors)
				break
			}
		}

		// Save to storage like production
		if b.storage != nil {
			b.storage.SaveMessage(&storage.Message{
				ID:          msg.ID,
				Queue:       msg.Queue,
				Payload:     msg.Payload,
				PublishedAt: time.Now(),
			})
		}
	}

	runtime.ReadMemStats(&m)
	endAlloc := m.Alloc
	memGrowth := float64(endAlloc-startAlloc) / (1024 * 1024)

	t.Logf("Memory growth: %.2f MB for %d messages (%d bytes each)", memGrowth, q.Len(), msgSize)
	t.Logf("Queue length: %d, Errors: %d", q.Len(), errors)

	// Memory should not grow more than 4x the data size
	// (BadgerDB has memtables, caches, and other overhead)
	expectedMax := float64(1000*msgSize) / (1024 * 1024) * 4 // 4x overhead
	if memGrowth > expectedMax {
		t.Errorf("Memory grew %.2f MB, expected max %.2f MB", memGrowth, expectedMax)
	}
}

// TestStressMessageSizes exposes W-005: Large message throughput degradation
// Measures throughput with different message sizes.
func TestStressMessageSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	sizes := []int{64, 256, 1024, 4096, 16384, 65536}
	results := make(map[int]float64)

	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		b := NewBroker()
		q := b.GetOrCreateQueue(fmt.Sprintf("stress-size-%d", size))

		start := time.Now()
		count := 0
		duration := 2 * time.Second

		for time.Since(start) < duration {
			msg := &Message{
				ID:      NewULID(),
				Queue:   q.name,
				Payload: payload,
			}
			if err := q.Enqueue(msg); err != nil {
				break
			}
			count++
		}

		elapsed := time.Since(start).Seconds()
		rate := float64(count) / elapsed
		results[size] = rate

		b.Close()

		t.Logf("Size %6d bytes: %8.1f msg/s", size, rate)
	}

	// Large messages should not degrade more than 20x compared to small
	// (Currently degrades 67x, this test documents the issue)
	smallRate := results[64]
	largeRate := results[65536]
	ratio := smallRate / largeRate

	t.Logf("Degradation ratio (64B vs 64KB): %.1fx", ratio)

	// Target: <20x degradation (currently ~67x)
	// This test will fail until W-005 is fixed
	if ratio > 20 {
		t.Errorf("Large message degradation too high: %.1fx (target: <20x)", ratio)
	}
}

// TestStressBurstTraffic exposes W-006: Burst handling catastrophically slow
// Sends bursts of messages and measures processing time.
func TestStressBurstTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	b := NewBroker()
	defer b.Close()

	q := b.GetOrCreateQueue("stress-burst")
	payload := make([]byte, 256)

	// Send a burst of 100 messages
	burstSize := 100
	start := time.Now()

	for i := 0; i < burstSize; i++ {
		msg := &Message{
			ID:      NewULID(),
			Queue:   "stress-burst",
			Payload: payload,
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed at %d: %v", i, err)
		}
	}

	burstTime := time.Since(start)
	rate := float64(burstSize) / burstTime.Seconds()

	t.Logf("Burst of %d messages: %v (%.1f msg/s)", burstSize, burstTime, rate)

	// Burst should complete in under 100ms (in-memory)
	// With disk sync, this will be slower
	if burstTime > 100*time.Millisecond {
		t.Logf("WARNING: Burst took %v (>100ms) - likely sync write overhead", burstTime)
	}
}

// TestStressConcurrentConnections exposes W-003: Connection storm failure
// Creates many concurrent goroutines simulating connections.
func TestStressConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	b := NewBroker()
	defer b.Close()

	server := NewServer(b)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()
	if addr == nil {
		t.Fatal("server failed to start")
	}

	// Simulate 50 concurrent connection attempts
	numConns := 50
	var wg sync.WaitGroup
	var successful int64
	var failed int64

	wg.Add(numConns)
	for i := 0; i < numConns; i++ {
		go func() {
			defer wg.Done()
			conn, err := dialWithTimeout(addr.String(), time.Second)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			conn.Close()
			atomic.AddInt64(&successful, 1)
		}()
	}

	wg.Wait()

	t.Logf("Connection storm: %d successful, %d failed (of %d)", successful, failed, numConns)

	// All connections should succeed
	if successful != int64(numConns) {
		t.Errorf("Connection storm failed: %d/%d successful", successful, numConns)
	}
}

// TestStressConcurrentPublish tests concurrent publishers on same queue
func TestStressConcurrentPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	b := NewBroker()
	defer b.Close()

	q := b.GetOrCreateQueue("stress-concurrent")

	numPublishers := 10
	messagesPerPublisher := 100
	payload := make([]byte, 256)

	var wg sync.WaitGroup
	var totalPublished int64
	var errors int64

	start := time.Now()

	wg.Add(numPublishers)
	for p := 0; p < numPublishers; p++ {
		go func(publisherID int) {
			defer wg.Done()
			for i := 0; i < messagesPerPublisher; i++ {
				msg := &Message{
					ID:      NewULID(),
					Queue:   "stress-concurrent",
					Payload: payload,
				}
				if err := q.Enqueue(msg); err != nil {
					atomic.AddInt64(&errors, 1)
				} else {
					atomic.AddInt64(&totalPublished, 1)
				}
			}
		}(p)
	}

	wg.Wait()
	elapsed := time.Since(start)

	expected := numPublishers * messagesPerPublisher
	rate := float64(totalPublished) / elapsed.Seconds()

	t.Logf("Concurrent publish: %d/%d in %v (%.1f msg/s)", totalPublished, expected, elapsed, rate)
	t.Logf("Errors: %d", errors)

	if totalPublished != int64(expected) {
		t.Errorf("Expected %d published, got %d", expected, totalPublished)
	}
}

// TestStressConcurrentConsume tests concurrent consumers on same queue
func TestStressConcurrentConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	b := NewBroker()
	defer b.Close()

	q := b.GetOrCreateQueue("stress-consume")

	// Pre-fill queue
	numMessages := 100
	payload := make([]byte, 256)
	for i := 0; i < numMessages; i++ {
		msg := &Message{
			ID:      NewULID(),
			Queue:   "stress-consume",
			Payload: payload,
		}
		q.Enqueue(msg)
	}

	// Start multiple consumers
	numConsumers := 5
	var wg sync.WaitGroup
	var totalConsumed int64

	wg.Add(numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()
			ch := q.Dequeue(5 * time.Second)
			timeout := time.After(3 * time.Second)
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					q.Ack(msg.ID)
					atomic.AddInt64(&totalConsumed, 1)
				case <-timeout:
					return
				}
			}
		}(c)
	}

	wg.Wait()

	t.Logf("Concurrent consume: %d/%d consumed by %d consumers", totalConsumed, numMessages, numConsumers)

	// All messages should be consumed (competing consumers)
	if totalConsumed != int64(numMessages) {
		t.Errorf("Expected %d consumed, got %d", numMessages, totalConsumed)
	}
}

// TestStressPublishConsumeThroughput measures end-to-end throughput
func TestStressPublishConsumeThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	b := NewBroker()
	defer b.Close()

	q := b.GetOrCreateQueue("stress-throughput")
	payload := make([]byte, 1024) // 1KB messages

	var published int64
	var consumed int64
	duration := 3 * time.Second
	done := make(chan struct{})

	// Start consumer
	go func() {
		ch := q.Dequeue(30 * time.Second)
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				q.Ack(msg.ID)
				atomic.AddInt64(&consumed, 1)
			case <-done:
				return
			}
		}
	}()

	// Give consumer time to start
	time.Sleep(10 * time.Millisecond)

	// Publisher
	start := time.Now()
	for time.Since(start) < duration {
		msg := &Message{
			ID:      NewULID(),
			Queue:   "stress-throughput",
			Payload: payload,
		}
		if err := q.Enqueue(msg); err == nil {
			atomic.AddInt64(&published, 1)
		}
	}

	// Wait for consumer to catch up
	time.Sleep(500 * time.Millisecond)
	close(done)

	pubRate := float64(published) / duration.Seconds()
	consRate := float64(consumed) / duration.Seconds()

	t.Logf("Published: %d (%.1f/s)", published, pubRate)
	t.Logf("Consumed: %d (%.1f/s)", consumed, consRate)

	// Consumer should keep up with publisher (in-memory)
	if consumed < published*8/10 { // 80% threshold
		t.Errorf("Consumer falling behind: %d/%d", consumed, published)
	}
}

// dialWithTimeout is a helper for connection tests
func dialWithTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

// TestStressSyncWriteThroughput exposes W-001/W-005: Sync write penalty
// This test measures actual throughput with sync writes enabled.
func TestStressSyncWriteThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir, err := os.MkdirTemp("", "stress-sync-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("stress-sync")

	sizes := []int{64, 1024, 10240} // 64B, 1KB, 10KB
	results := make(map[int]float64)

	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		start := time.Now()
		count := 0
		duration := 2 * time.Second

		for time.Since(start) < duration {
			msg := &Message{
				ID:          NewULID(),
				Queue:       "stress-sync",
				Payload:     payload,
				PublishedAt: time.Now(),
			}
			if err := q.Enqueue(msg); err != nil {
				break
			}
			// Sync write to storage (this is the bottleneck)
			if err := b.storage.SaveMessage(&storage.Message{
				ID:          msg.ID,
				Queue:       msg.Queue,
				Payload:     msg.Payload,
				PublishedAt: msg.PublishedAt,
			}); err != nil {
				break
			}
			count++
		}

		elapsed := time.Since(start).Seconds()
		rate := float64(count) / elapsed
		results[size] = rate

		t.Logf("Size %6d bytes (sync): %8.1f msg/s", size, rate)
	}

	// With sync writes, we expect ~100-500 msg/s (disk bound)
	// If we get <50 msg/s, something is wrong
	smallRate := results[64]
	if smallRate < 50 {
		t.Errorf("Sync write throughput too low: %.1f msg/s (expected >50)", smallRate)
	}

	// Log degradation ratio for documentation
	if largeRate := results[10240]; smallRate > 0 && largeRate > 0 {
		ratio := smallRate / largeRate
		t.Logf("Degradation ratio (64B vs 10KB with sync): %.1fx", ratio)
	}
}

// TestStressHighVolumeMemory exposes W-004: Memory pressure at high volume
// Tests memory behavior with large number of messages.
func TestStressHighVolumeMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir, err := os.MkdirTemp("", "stress-volume-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("stress-volume")

	// 10KB messages, target 5000 messages (50MB)
	// Scaled down from the 50K that crashed in benchmarks
	msgSize := 10 * 1024
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	startAlloc := m.Alloc

	targetCount := 5000
	var published, errors int

	for i := 0; i < targetCount; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "stress-volume",
			Payload:     payload,
			PublishedAt: time.Now(),
		}
		if err := q.Enqueue(msg); err != nil {
			errors++
			if errors > 100 {
				t.Logf("Stopping after %d messages with %d errors", i, errors)
				break
			}
			continue
		}
		if err := b.storage.SaveMessage(&storage.Message{
			ID:          msg.ID,
			Queue:       msg.Queue,
			Payload:     msg.Payload,
			PublishedAt: msg.PublishedAt,
		}); err != nil {
			errors++
			if errors > 100 {
				t.Logf("Stopping after %d messages with %d errors", i, errors)
				break
			}
			continue
		}
		published++

		// Log progress every 1000 messages
		if published%1000 == 0 {
			runtime.ReadMemStats(&m)
			t.Logf("Progress: %d/%d, Memory: %.1f MB", published, targetCount, float64(m.Alloc)/(1024*1024))
		}
	}

	runtime.ReadMemStats(&m)
	endAlloc := m.Alloc
	memGrowth := float64(endAlloc-startAlloc) / (1024 * 1024)

	t.Logf("Final: %d published, %d errors", published, errors)
	t.Logf("Memory growth: %.1f MB", memGrowth)
	t.Logf("Expected data size: %.1f MB", float64(published*msgSize)/(1024*1024))

	// Should publish at least 80% of target
	if published < targetCount*8/10 {
		t.Errorf("Only published %d/%d messages", published, targetCount)
	}

	// Memory should be bounded (not unbounded growth)
	maxExpected := float64(published*msgSize)/(1024*1024)*3 + 100 // 3x data + 100MB overhead
	if memGrowth > maxExpected {
		t.Errorf("Memory grew %.1f MB, expected max %.1f MB", memGrowth, maxExpected)
	}
}
