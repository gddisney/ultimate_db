
# ultimate_db

`ultimate_db` is a high-performance, production-ready embedded database engine written in Go. It uniquely combines transactional Multi-Version Concurrency Control (**MVCC**), a custom multi-pass entropy compression layer (**"Middle-Out"**), an auto-checkpointing Write-Ahead Log (**WAL**), a latch-crabbing **B+ Tree** optimized with Optimistic Concurrency Control, and a fully featured Boolean Abstract Syntax Tree (**AST**) inverted search index.

Designed for resource-constrained environments, high-concurrency microservices, and specialized low-latency caching, the engine guarantees zero-allocation data hot paths and robust crash resilience.

---

## Architecture Overview

The core architecture is built around four decoupled, cooperating layers operating over a unified slotted-page buffer pool:

1. **Storage Layer:** Implements a strict 32KB slotted-page framework (`PageSize = 32768`) featuring dynamic on-page compaction, dead transaction vacuuming, and automated TTL pruning.
2. **Durability Engine (WAL & Checkpointer):** A lock-free single-goroutine buffer drain loop combines concurrent sequential updates down into single OS flushing frames via group commits. A background worker periodically orchestrates full-pool flushes and log truncations to bound crash-recovery times.
3. **Indexing Tier (B+ Tree with OCC):** Standard lock-coupling/latch-crabbing is augmented by an Optimistic Concurrency Control (OCC) pass. Traversals default to lightweight Read-Locks, upgrading to exclusive Write-Locks only at the leaf layer if headspace permits, eliminating parent-index contention and resource starvation deadlocks.
4. **Compression Pipeline (Middle-Out):** A zero-allocation dual-pass compression topology leveraging `sync.Pool`. Pass 1 executes token-escaped run-length encoding (RLE), and Pass 2 compresses high-density byte paths using statically compiled lookahead forest density matrices.

---

## Features

* **MVCC Transactions & Active GC:** Point-in-time isolation reads alongside non-blocking writes. Expired transactional records and stale TTL boundaries are automatically collected during runtime page compaction.
* **Optimistic Latch-Crabbing:** Highly concurrent index traversals with zero-overhead lookups, dropping parent locks aggressively to maximize vertical scale.
* **Zero-Allocation Data Hot Paths:** Reusable internal scratch buffers neutralize Go's runtime GC pressure under write-heavy loads.
* **Boolean Query AST Parser:** A recursive-descent query processor with built-in stack depth protection (`MaxQueryDepth = 50`) translating logical text inputs (`AND`, `OR`, `NOT`, brackets) into set-mathematics evaluation pipelines.
* **Redis-compatible Hash Primitives:** Out-of-the-box support for nested structural data layout bindings via composite key routing (`HSet`).

---

## Component Layout

Your repository directory should be structured as follows:

```text
ultimate_db/
├── common.go         # Shared configuration constants, registers, and forest tables
├── encoder.go        # Dual-pass compression pipelines (RLE + Huffman Bit-Packing)
├── decoder.go        # Zero-allocation inverse decompression routing matrices
├── ultimate_db.go    # Core transactional storage engine, B+ Tree, Indexer, Parser, & Checkpointer
└── README.md         # Module documentation

```

---

## Installation

Ensure your local environment has Go 1.18+ configured. Copy the codebase into your package structure and import it directly:

```go
import "your-project/ultimate_db"

```

---

## Quick Start & Usage Examples

### 1. Initialize the Engine

Setting up the storage subsystem requires configuring a `DiskManager`, initializing a shared `BufferPool`, and linking an active group-committing `BatchingWAL`:

```go
package main

import (
	"log"
	"time"
	"your-project/ultimate_db"
)

func main() {
	// Initialize the physical layer
	disk, err := ultimate_db.NewDiskManager("production.db")
	if err != nil {
		log.Fatalf("failed to open database file: %v", err)
	}

	// Initialize buffer pool (e.g., 1024 cached memory frames)
	pool := ultimate_db.NewBufferPool(disk, 1024)

	// Spin up the Write-Ahead Log
	wal, err := ultimate_db.NewBatchingWAL("production.wal")
	if err != nil {
		log.Fatalf("failed to initialize WAL: %v", err)
	}

	// Create and boot the primary database core
	db := ultimate_db.NewDB(pool, wal)
	defer db.Close()

	// Optional: Execute crash recovery on startup
	if err := ultimate_db.RecoverDB("production.wal", db); err != nil {
		log.Fatalf("database recovery failed: %v", err)
	}
}

```

### 2. Basic MVCC & Middle-Out Compressed Operations

```go
// Begin a transaction block
txnID := db.BeginTxn()
pageID := ultimate_db.PageID(0)

key := []byte("user:session:1002")
value := []byte(`{"user_id":1002,"role":"admin","status":"authenticated"}`)

// Write using transparent Middle-Out compression with a 1-hour TTL
err := db.WriteCompressed(pageID, txnID, key, value, 1*time.Hour)
if err != nil {
	log.Printf("write failed: %v", err)
}

// Read the compressed record back safely within transactional bounds
readTxn := db.BeginTxn()
data, err := db.ReadCompressed(pageID, readTxn, key)
if err != nil {
	log.Printf("read failed: %v", err)
}
log.Printf("Retrieved Session Data: %s", string(data))

```

### 3. High-Concurrency B+ Tree Key-Value Indexing

```go
// Root page mapping for a newly initialized index
rootPageID := ultimate_db.PageID(1)
btree := ultimate_db.NewBTree(pool, rootPageID)

// Thread-safe concurrent insertions utilizing OCC lock-coupling
go func() {
	err := btree.Insert([]byte("alpha_key"), []byte("payload_data_a"))
	if err != nil {
		log.Printf("B+ Tree insertion failed: %v", err)
	}
}()

// Lazy sequential range scanning via structural Cursors
cursor, err := ultimate_db.NewBTreeCursor(btree)
if err != nil {
	log.Fatalf("failed to compile iterator cursor: %v", err)
}
defer cursor.Close()

for {
	k, v, err := cursor.Next()
	if err != nil {
		break // Yields io.EOF when index boundaries are reached
	}
	log.Printf("Cursor Found Index entry Key: %s -> Val: %s", string(k), string(v))
}

```

### 4. Full-Text Search Segment Evaluation

```go
// Spin up an in-memory postings matrix segment
memIndex := ultimate_db.NewMemIndex()
memIndex.Add(1, "The quick brown fox jumps over the lazy dog")
memIndex.Add(2, "Structured OIDC database records and telemetry logs")

// Compile and serialize memory matrices into optimized binary layouts
err = memIndex.WriteSegment("search_segment.idx")

// Map segment bytes directly for processing queries
segmentData, _ := os.ReadFile("search_segment.idx")
searcher := &ultimate_db.SegmentSearcher{Data: segmentData}

// Run a boolean expression through the stack-protected recursive descent parser
results, err := searcher.Search("(database AND records) OR (fox NOT dog)")
if err != nil {
	log.Printf("Parsing breakdown: %v", err)
}
log.Printf("Matching Document IDs: %v", results) // Yields matching arrays linearly

```

---

## Production Security & Safety Considerations

* **Lock Escalation Resiliency:** While the engine applies Optimistic Concurrency Control loops to scale read workloads, lock-upgrades are non-atomic in native Go sync maps. High-contention collisions trigger automated internal rollbacks to pessimistic latch-crabbing modes gracefully.
* **Parser Boundaries:** The text translation stack drops processing evaluations if deep nesting attempts to exceed `MaxQueryDepth = 50`. Do not modify this limit without verifying operating system stack configurations.
* **Log Rotation:** The continuous background checkpointer loop targets automated cleanups every 5 minutes by default. For micro-burst environments, adjust the ticker speed within `startCheckpointer()` inside `ultimate_db.go` to match available underlying physical hardware I/O constraints.

---

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
