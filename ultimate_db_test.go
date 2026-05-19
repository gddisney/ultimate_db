package ultimate_db

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// ==========================================
// 1. CORE LIFECYCLE & MVCC TESTS
// ==========================================

func TestUltimateDB_CoreLifecycle(t *testing.T) {
	dbPath := "test_identity.db"
	walPath := "test_identity.wal"
	defer os.Remove(dbPath)
	defer os.Remove(walPath)

	// Initialize Engine
	dm, _ := NewDiskManager(dbPath)
	bp := NewBufferPool(dm, 64)
	wal, _ := NewBatchingWAL(walPath)
	db := NewDB(bp, wal)
	defer db.Close()

	// Ensure we have an initial page
	pageID := PageID(0)
	if _, err := bp.FetchPage(pageID); err != nil {
		bp.NewPage()
	} else {
		bp.UnpinPage(pageID, false)
	}

	t.Run("Compression Stability", func(t *testing.T) {
		key := []byte("user:1234")
		value := []byte("SESSION_DATA:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_SECRET_JSON_DATA_12345")

		txn := db.BeginTxn()
		err := db.WriteCompressed(pageID, txn, key, value, 0)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		db.CommitTxn(txn)

		readTxn := db.BeginTxn()
		readVal, err := db.ReadCompressed(pageID, readTxn, key)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if !bytes.Equal(value, readVal) {
			t.Errorf("Data corruption! Expected %s, got %s", value, readVal)
		}
	})

	t.Run("Concurrent Transaction Isolation", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 10
		
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				txn := db.BeginTxn()
				key := []byte(fmt.Sprintf("key-%d", id))
				val := []byte(fmt.Sprintf("value-%d", id))
				
				_ = db.Write(pageID, txn, key, val, 0)
				db.CommitTxn(txn)
			}(i)
		}
		wg.Wait()

		txn := db.BeginTxn()
		for i := 0; i < iterations; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			expected := []byte(fmt.Sprintf("value-%d", i))
			val, err := db.Read(pageID, txn, key)
			if err != nil || !bytes.Equal(val, expected) {
				t.Errorf("Isolation failure for %s", key)
			}
		}
	})

	t.Run("TTL Expiration Logic", func(t *testing.T) {
		txn := db.BeginTxn()
		key := []byte("temporary_key")
		val := []byte("volatile_data")
		
		_ = db.Write(pageID, txn, key, val, 1*time.Second)
		db.CommitTxn(txn)
		
		readTxn1 := db.BeginTxn()
		res, _ := db.Read(pageID, readTxn1, key)
		if len(res) == 0 {
			t.Error("Immediate read failed before TTL expired")
		}

		// Wait for expiration
		time.Sleep(1100 * time.Millisecond)
		
		readTxn2 := db.BeginTxn()
		res, _ = db.Read(pageID, readTxn2, key)
		if len(res) != 0 {
			t.Error("Data persisted beyond TTL expiration")
		}
	})
}

// ==========================================
// 2. COMPRESSION UNIT TESTS
// ==========================================

func TestCompressionRange(t *testing.T) {
	testCases := []string{
		"short",
		"longer string with symbols !@#$%^&*()",
		"{\"menu\": {\"id\": \"file\", \"value\": \"File\", \"popup\": {\"menuitem\": [{\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"}]}}}",
		string(make([]byte, 1000)),
	}

	for _, tc := range testCases {
		src := []byte(tc)
		dst := make([]byte, MaxBlockSize*2)
		
		compLen, err := Compress(src, dst)
		if err != nil {
			t.Fatalf("Compression error for string '%s': %v", tc, err)
		}

		decompressed := make([]byte, len(src))
		_, err = Decompress(dst[:compLen], decompressed)
		if err != nil {
			t.Fatalf("Decompression error for string '%s': %v", tc, err)
		}

		if !bytes.Equal(src, decompressed) {
			t.Errorf("Mismatch for string: %s", tc)
		}
	}
}

// ==========================================
// 3. B+ TREE INDEX TESTS
// ==========================================

func TestBTree_InsertAndScan(t *testing.T) {
	dbPath := "test_btree.db"
	defer os.Remove(dbPath)

	dm, _ := NewDiskManager(dbPath)
	bp := NewBufferPool(dm, 64)

	// Initialize root page
	rootPage, _ := bp.NewPage()
	rootPage.Latch.Lock()
	node := &BTreePage{rootPage}
	node.BTreeInit()
	node.SetPageType(PageTypeLeaf)
	rootPage.Latch.Unlock()
	bp.UnpinPage(rootPage.ID, true)

	btree := NewBTree(bp, rootPage.ID)

	// Test Insertions
	insertions := map[string]string{
		"apple":   "red",
		"apricot": "orange",
		"banana":  "yellow",
		"grape":   "purple",
	}

	for k, v := range insertions {
		if err := btree.Insert([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to insert %s: %v", k, err)
		}
	}

	// FIX: Changed prefix from "app" to "ap" so it correctly catches "apricot"
	keys, vals, err := btree.Scan("ap")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("Expected 2 results for prefix 'ap', got %d", len(keys))
	}

	// BTree should return results sorted by key
	if string(keys[0]) != "apple" || string(vals[0]) != "red" {
		t.Errorf("Unexpected first scan result: %s -> %s", keys[0], vals[0])
	}
	if string(keys[1]) != "apricot" || string(vals[1]) != "orange" {
		t.Errorf("Unexpected second scan result: %s -> %s", keys[1], vals[1])
	}
}

// ==========================================
// 4. INVERTED INDEX & AST SEARCH TESTS
// ==========================================

func TestSearch_InvertedIndex(t *testing.T) {
	segPath := "test_segment.idx"
	defer os.Remove(segPath)

	// 1. Build Index
	memIndex := NewMemIndex()
	memIndex.Add(1, "The quick brown fox jumps over the lazy dog")
	memIndex.Add(2, "Structured OIDC database records and telemetry logs")
	memIndex.Add(3, "A brown dog barks loudly")
	memIndex.Add(4, "The fox is fast but the dog is lazy")

	err := memIndex.WriteSegment(segPath)
	if err != nil {
		t.Fatalf("Failed to write segment: %v", err)
	}

	// 2. Load Segment
	segmentData, _ := os.ReadFile(segPath)
	searcher := &SegmentSearcher{Data: segmentData}

	// 3. Test Queries
	testCases := []struct {
		query    string
		expected []uint64
	}{
		{"database", []uint64{2}},
		{"brown AND dog", []uint64{1, 3}},
		{"fox OR database", []uint64{1, 2, 4}},
		{"(fox AND dog) NOT lazy", []uint64{}},           
		{"(brown OR records) NOT jumps", []uint64{2, 3}}, // FIX: Removed AND before NOT
	}

	for _, tc := range testCases {
		results, err := searcher.Search(tc.query)
		if err != nil {
			t.Fatalf("Query failed for '%s': %v", tc.query, err)
		}

		// Sort results for consistent comparison
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		
		if len(results) != len(tc.expected) {
			t.Errorf("Query '%s' length mismatch. Expected %v, got %v", tc.query, tc.expected, results)
			continue
		}
		for i := range results {
			if results[i] != tc.expected[i] {
				t.Errorf("Query '%s' mismatch. Expected %v, got %v", tc.query, tc.expected, results)
				break
			}
		}
	}
}

// ==========================================
// 5. WAL CRASH RECOVERY TESTS
// ==========================================

func TestWAL_Recovery(t *testing.T) {
	dbPath := "test_recovery.db"
	walPath := "test_recovery.wal"
	defer os.Remove(dbPath)
	defer os.Remove(walPath)

	// 1. Setup DB and write data
	dm, _ := NewDiskManager(dbPath)
	bp := NewBufferPool(dm, 64)
	wal, _ := NewBatchingWAL(walPath)
	db := NewDB(bp, wal)

	pageID := PageID(0)
	bp.NewPage() // Create page 0
	bp.UnpinPage(pageID, false)

	txn := db.BeginTxn()
	key1, val1 := []byte("recover_k1"), []byte("data1")
	key2, val2 := []byte("recover_k2"), []byte("data2")

	db.Write(pageID, txn, key1, val1, 0)
	db.Write(pageID, txn, key2, val2, 0)
	db.CommitTxn(txn)

	// Simulate a crash by abruptly closing without a proper checkpoint flush
	db.wal.Close() 

	// 2. Re-initialize and Recover
	dm2, _ := NewDiskManager(dbPath)
	bp2 := NewBufferPool(dm2, 64)
	wal2, _ := NewBatchingWAL(walPath)
	db2 := NewDB(bp2, wal2)
	defer db2.Close()

	if err := RecoverDB(walPath, db2); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// 3. Verify Data Survived
	readTxn := db2.BeginTxn()
	
	res1, err := db2.Read(pageID, readTxn, key1)
	if err != nil || !bytes.Equal(res1, val1) {
		t.Errorf("Failed to recover key1. Expected %s, got %s", val1, res1)
	}

	res2, err := db2.Read(pageID, readTxn, key2)
	if err != nil || !bytes.Equal(res2, val2) {
		t.Errorf("Failed to recover key2. Expected %s, got %s", val2, res2)
	}
}
