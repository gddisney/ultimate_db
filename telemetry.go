package ultimate_db

import (
	"sync/atomic"
	"time"
)

// AtomicMetrics implements the EngineMetrics interface using high-performance,
// lock-free atomic operations suitable for concurrent production workloads.
type AtomicMetrics struct {
	bufferPoolHits    atomic.Uint64
	bufferPoolMisses  atomic.Uint64
	walFlushLatencies atomic.Uint64 // Cumulative duration in nanoseconds
	walFlushCount     atomic.Uint64
	compactionTime    atomic.Uint64 // Cumulative duration in nanoseconds
	compactionCount   atomic.Uint64
	activeTxns        atomic.Int64
}

// NewAtomicMetrics initializes an empty metrics container.
func NewAtomicMetrics() *AtomicMetrics {
	return &AtomicMetrics{}
}

// ============================================================================
// 1. EngineMetrics Interface Implementation
// ============================================================================

func (m *AtomicMetrics) IncrBufferPoolHit() {
	m.bufferPoolHits.Add(1)
}

func (m *AtomicMetrics) IncrBufferPoolMiss() {
	m.bufferPoolMisses.Add(1)
}

func (m *AtomicMetrics) RecordWalFlushLatency(d time.Duration) {
	m.walFlushLatencies.Add(uint64(d.Nanoseconds()))
	m.walFlushCount.Add(1)
}

func (m *AtomicMetrics) RecordPageCompactionTime(d time.Duration) {
	m.compactionTime.Add(uint64(d.Nanoseconds()))
	m.compactionCount.Add(1)
}

func (m *AtomicMetrics) SetActiveTransactions(count int64) {
	m.activeTxns.Store(count)
}

// ============================================================================
// 2. Snapshot Telemetry Exporters (For API / Prometheus Endpoints)
// ============================================================================

// Snapshot represents a static copy of system vital signs captured at a specific point in time.
type EngineMetricsSnapshot struct {
	BufferPoolHits       uint64
	BufferPoolMisses     uint64
	BufferPoolHitRatio   float64
	AvgWalFlushLatency   time.Duration
	AvgCompactionLatency time.Duration
	ActiveTransactions   int64
}

// Capture returns a point-in-time snapshot of the database diagnostics.
func (m *AtomicMetrics) Capture() EngineMetricsSnapshot {
	hits := m.bufferPoolHits.Load()
	misses := m.bufferPoolMisses.Load()
	walTime := m.walFlushLatencies.Load()
	walCount := m.walFlushCount.Load()
	compactTime := m.compactionTime.Load()
	compactCount := m.compactionCount.Load()

	var hitRatio float64
	if total := hits + misses; total > 0 {
		hitRatio = float64(hits) / float64(total)
	}

	var avgWal time.Duration
	if walCount > 0 {
		avgWal = time.Duration(walTime / walCount)
	}

	var avgCompact time.Duration
	if compactCount > 0 {
		avgCompact = time.Duration(compactTime / compactCount)
	}

	return EngineMetricsSnapshot{
		BufferPoolHits:       hits,
		BufferPoolMisses:     misses,
		BufferPoolHitRatio:   hitRatio,
		AvgWalFlushLatency:   avgWal,
		AvgCompactionLatency: avgCompact,
		ActiveTransactions:   m.activeTxns.Load(),
	}
}
