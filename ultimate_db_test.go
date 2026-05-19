package ultimate_db

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestUltimateDB_CoreLifecycle(t *testing.T) {
	dbPath := "test_identity.db"
	walPath := "test_identity.wal"
	defer os.Remove(dbPath)
	defer os.Remove(walPath)

	// 1. Initialize Engine
	dm, _ := NewDiskManager(dbPath)
	bp := NewBufferPool(dm, 64)
	wal, _ := NewBatchingWAL(walPath)
	db := NewDB(bp, wal)

	// Ensure we have an initial page
	pageID := PageID(0)
	if _, err := bp.FetchPage(pageID); err != nil {
		bp.NewPage()
	} else {
		bp.UnpinPage(pageID, false)
	}

	t.Run("Compression Stability", func(t *testing.T) {
		key := []byte("user:1234")
		// Highly redundant data to test LRE (Run Length Encoding) + Huffman
		value := []byte("SESSION_DATA:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_SECRET_JSON_DATA_12345")

		txn := db.BeginTxn()
		err := db.Write(pageID, txn, key, value, 0)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		readVal, err := db.Read(pageID, txn, key)
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
		
		// Test multiple writers to different keys on the same page
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				txn := db.BeginTxn()
				key := []byte(fmt.Sprintf("key-%d", id))
				val := []byte(fmt.Sprintf("value-%d", id))
				
				_ = db.Write(pageID, txn, key, val, 0)
			}(i)
		}
		wg.Wait()

		// Verify all data exists
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
		
		// Set a 1-second TTL
		_ = db.Write(pageID, txn, key, val, 1*time.Second)
		
		// Immediate read should work
		res, _ := db.Read(pageID, txn, key)
		if len(res) == 0 {
			t.Error("Immediate read failed before TTL expired")
		}

		// Wait for expiration
		time.Sleep(1100 * time.Millisecond)
		
		res, _ = db.Read(pageID, txn, key)
		if len(res) != 0 {
			t.Error("Data persisted beyond TTL expiration")
		}
	})
}

func TestCompressionRange(t *testing.T) {
	// Tests the dynamic common.go Huffman tables directly
	testCases := []string{
		"short",
		"longer string with symbols !@#$%^&*()",
		"{\"menu\": {\"id\": \"file\", \"value\": \"File\", \"popup\": {\"menuitem\": [{\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"}]}}}",
		string(make([]byte, 1000)), // Test large null-byte blocks
	}

	for _, tc := range testCases {
		src := []byte(tc)
		dst := make([]byte, MaxBlockSize*2)
		
		compLen, err := Compress(src, dst)
		if err != nil {
			t.Fatalf("Compression error: %v", err)
		}

		decompressed := make([]byte, len(src))
		_, err = Decompress(dst[:compLen], decompressed)
		if err != nil {
			t.Fatalf("Decompression error: %v", err)
		}

		if !bytes.Equal(src, decompressed) {
			t.Errorf("Mismatch for string: %s", tc)
		}
	}
}
