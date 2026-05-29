# ultimate_db (v2.0)

A high-performance, embedded transactional storage engine engineered in Go. `ultimate_db` features a bimodal architecture pairing a slot-page B+ Tree storage engine with a lock-free Optimistic Concurrency Control (OCC) fast memory cache. It is designed specifically to back high-velocity, distributed zero-trust mesh layers and real-time security analytics engines with near-flat operating overhead.

## Core Architecture

`ultimate_db` bridges raw file-system page frames with a type-safe object-relational abstraction layer.

```
       [ Client / API Applications ]
                     |
                     v
       [ GlobalCacheStore (MVCC/OCC) ]  <-- Fast Path (Microseconds)
                     |
         (Cache Miss / Commits)
                     v
       [ ultimate_db.DB Core Instance ]
         /           |            \
        v            v             v
 [Buffer Pool] [MemIndex / B+] [Batching WAL]
        |            |             |
        +------------+-------------+
                     |
                     v
           [ OS Block Device ]          <-- Durable Path (Slotted Pages)

```

* **Storage Primitives (`ultimate_db.DB`)**: Manages durable slot-allocated page boundaries on disk via a buffer pool and a low-latency Write-Ahead Log (WAL).
* **Memory Subsystem (`GlobalCacheStore`)**: A high-speed, thread-safe, multi-version memory ring that intercepts operations on hot paths to eliminate synchronous disk I/O penalties during validation loops.

---

## 1. Storage API Reference

### Engine Lifecycle

#### `NewDB`

Instantiates a long-running instance of the core engine tracking structural storage blocks.

```go
func NewDB(
    bp *ultimate_db.BufferPool, 
    wal *ultimate_db.BatchingWAL, 
    metrics ultimate_db.EngineMetrics
) *ultimate_db.DB

```

#### `Close`

Flushes outstanding dirty frames, closes the WAL file descriptors, and terminates block device holds gracefully.

```go
func (db *DB) Close() error

```

### Transaction Management

The engine runs fully isolated read/write cycles across snapshot environments via deterministic logical clock barriers.

```go
// BeginTxn creates a unique MVCC tracking transaction identifier
func (db *DB) BeginTxn() uint64

// CommitTxn seals modifications made within the tracking context and releases log holds
func (db *DB) CommitTxn(txnID uint64)

```

### Page Level I/O Operators

#### `Read`

Pulls a discrete byte value chain out of a specific slotted page record index tracking slots.

```go
func (db *DB) Read(pageID ultimate_db.PageID, txnID uint64, key []byte) ([]byte, error) {
    // If the record does not exist or has an active tombstone, it returns an error
}

```

#### `Write`

Persists a byte sequence slice to a specific slotted page structure, immediately committing a record to the WAL.

```go
func (db *DB) Write(pageID ultimate_db.PageID, txnID uint64, key []byte, value []byte, ttl time.Duration) error {
    // Setting value to an empty slice []byte{} writes an explicit tombstone (deletion marker)
}

```

#### `ScanCompressed`

Iterates linearly through all non-tombstoned page keys in sorted key order using a fast traversal predicate callback.

```go
func (db *DB) ScanCompressed(
    pageID ultimate_db.PageID, 
    txnID uint64, 
    startPrefix []byte, 
    callback func(key, value []byte) bool,
) error

```

---

## 2. Global MVCC Memory Cache (`GlobalCacheStore`)

The `GlobalCacheStore` is a lock-free, global operational interface optimized for sub-millisecond execution loops (e.g., policy assertions, token checking, and identity lookup loops).

### `BeginOCC`

Starts an Optimistic Concurrency Control snapshot observation window, returning a transaction sequencing ID.

```go
func (g *GlobalCacheStore) BeginOCC() uint64

```

### `Read`

Queries the volatile cache layer instantly using non-blocking read paths.

```go
func (g *GlobalCacheStore) Read(txID uint64, key string) ([]byte, error) {
    // Returns a cache miss error if the key is not in volatile memory
}

```

### `ValidateAndCommit`

Atomically writes a key-value map into the global cache ring. If an epoch collision or a version tracking desynchronization occurs during execution, the operation aborts to safeguard thread safety.

```go
func (g *GlobalCacheStore) ValidateAndCommit(txID uint64, writeSet map[string][]byte, ttl time.Duration) error

```

---

## 3. The ORM Layer

The `ultimate_db.ORM` subsystem uses Go reflection mapping primitives (`reflect.Value`) to decode slot byte values straight into type-safe structured data fields.

### `NewORM`

Bridges raw transactional byte writes with object definitions.

```go
func NewORM(db *ultimate_db.DB, index *ultimate_db.MemIndex, schema *ultimate_db.Schema, walPath string) *ultimate_db.ORM

```

### `Insert`

Translates a struct instance into a structured JSON string sequence and stores it sequentially inside the engine using the struct's primary key element.

```go
func (o *ORM) Insert(record interface{}) error {
    // Emits records straight into underlying storage layouts mapped to standard collection frames
}

```

### `Find`

Retrieves a record by its unique primary key and unmarshals the payload directly into a reference object.

```go
func (o *ORM) Find(id uint64, dest interface{}) error {
    // Returns an evaluation error if the structure schemas do not match target properties
}

```

---

## 4. Query Language & AST Engine Reference

`ultimate_db` features a powerful boolean Abstract Syntax Tree (AST) query parser designed to compile complex full-text and rule-evaluation queries into structured execution plans.

### Abstract Syntax Tree Types

Queries are parsed into type-safe node definitions implementing the `ultimate_db.Query` interface:

* **`TermQuery`**: Re-evaluates target terms against index postings maps.
```go
type TermQuery struct { Term string }

```


* **`AndQuery`**: Intersection node requiring both child structures to match.
```go
type AndQuery struct { Left, Right ultimate_db.Query }

```


* **`OrQuery`**: Union node returning true if either child structure matches.
```go
type OrQuery struct { Left, Right ultimate_db.Query }

```


* **`NotQuery`**: Exclusion modifier dropping matching parameters from lookup branches.
```go
type NotQuery struct { Left, Right ultimate_db.Query }

```



### `ParseQuery`

Compiles an infix free-text or structured boolean string query into a composite `Query` AST node tree.

```go
func ParseQuery(query string) (ultimate_db.Query, error)

```

### Query Language Syntax Specification

The query language handles alphanumeric values, multi-word matching combinations, and parenthetical evaluation groupings.

#### Simple Term Query

Finds all documents containing the analyzed string primitive.

```text
vulnerabilities

```

#### Implicit Conjunction (AND)

Space-separated tokens automatically compile into an implicit `AndQuery` chain wrapper.

```text
"Adversarial AI" vulnerabilities

```

#### Explicit Logical Operators

Supports boolean expressions (`AND`, `OR`, `NOT`) to explicitly manipulate match evaluation groupings.

```text
audit AND tracking NOT replica

```

#### Complex Parenthetical Evaluation

Group expressions with parentheses to override operator precedence rules during compilation.

```text
(identity OR session) AND "TPM hardware" NOT replay

```

---

## 5. Quick-Start Ingestion Example

This example demonstrates how to orchestrate `ultimate_db` components to securely ingest multi-word event logs using an inverted indexing chunk allocation layout (`orchid_sync`).

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gddisney/ultimate_db"
	"github.com/gddisney/orchid_sync"
)

func main() {
	// 1. Initialize physical device structures
	device, err := ultimate_db.NewOSFileDevice("production.db")
	if err != nil {
		log.Fatalf("Failed to attach block device storage: %v", err)
	}

	disk := ultimate_db.NewDiskManager(device)
	evictor := ultimate_db.NewLRUEvictionPolicy()
	metrics := ultimate_db.NewAtomicMetrics()
	bp := ultimate_db.NewBufferPool(disk, 1024, evictor, metrics)

	wal, err := ultimate_db.NewBatchingWAL("production.wal")
	if err != nil {
		log.Fatalf("Failed to activate Write-Ahead Logging: %v", err)
	}

	// 2. Initialize the core database engine
	db := ultimate_db.NewDB(bp, wal, metrics)
	defer db.Close()

	// 3. Bind the NLP search analyzer pipeline
	analyzer := orchid_sync.NewAnalyzer()
	indexer := orchid_sync.NewIndexer(db, analyzer)

	// 4. Ingest and index telemetry records atomically
	docID := "audit-log-89912"
	telemetryText := "Adversarial AI telemetry assertion threat verified for TPM hardware keys"

	err = indexer.AddDocument(docID, telemetryText)
	if err != nil {
		log.Fatalf("Ingestion pipeline failed on document mapping: %v", err)
	}
	fmt.Printf("Successfully committed document [%s] to durable B+ Tree pages.\n", docID)
}

```
