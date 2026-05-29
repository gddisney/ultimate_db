package ultimate_db

import (
	"bytes"
	"os"
	"testing"
)

func TestDB_WriteRead(t *testing.T) {
	dbPath, walPath := "test.db", "test.wal"
	defer os.Remove(dbPath); defer os.Remove(walPath)

	device, err := NewOSFileDevice(dbPath)
	if err != nil { t.Fatalf("Failed to open VFS device: %v", err) }
	
	disk := NewDiskManager(device)
	evictor := NewLRUEvictionPolicy()
	metrics := NewAtomicMetrics()
	
	bp := NewBufferPool(disk, 10, evictor, metrics)
	wal, _ := NewBatchingWAL(walPath)
	defer wal.Close()

	db := NewDB(bp, wal, metrics)
	defer db.Close()

	// Allocate and format a valid page template 
	rootPage, err := bp.NewPage()
	if err != nil { t.Fatalf("Failed to allocate slotted test block: %v", err) }
	bp.UnpinPage(rootPage.ID, true)

	txnID := db.BeginTxn()
	key, value := []byte("k1"), []byte("v1")
	err = db.Write(rootPage.ID, txnID, key, value, 0)
	if err != nil { t.Fatalf("Write failed: %v", err) }
	db.CommitTxn(txnID)

	readTxn := db.BeginTxn()
	got, err := db.Read(rootPage.ID, readTxn, key)
	if err != nil { t.Fatalf("Read failed: %v", err) }
	if !bytes.Equal(got, value) { t.Errorf("Expected %s, got %s", value, got) }
	db.CommitTxn(readTxn)
}
