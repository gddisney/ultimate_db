package ultimate_db

import (
	"fmt"
	"os"
	"testing"
)

// Define a production-representative struct for ORM validation.
// The ORM layer requires a public 'ID' field of type uint64.
type UserProfile struct {
	ID        uint64 `json:"id"`
	Username  string `json:"username"`
	Role      string `json:"role"`
	IsActive  bool   `json:"is_active"`
}

func TestFullDatabasePipeline(t *testing.T) {
	dbPath, walPath := "integration.db", "integration.wal"
	segPath := "integration.seg"
	os.Remove(dbPath); os.Remove(walPath); os.Remove(segPath)
	defer os.Remove(dbPath); defer os.Remove(walPath); defer os.Remove(segPath)

	// 1. Initialize Subsystems
	device, err := NewOSFileDevice(dbPath)
	if err != nil { t.Fatalf("VFS Device error: %v", err) }
	
	disk := NewDiskManager(device)
	evictor := NewLRUEvictionPolicy()
	metrics := NewAtomicMetrics()

	bp := NewBufferPool(disk, 10, evictor, metrics)
	wal, err := NewBatchingWAL(walPath)
	if err != nil { t.Fatalf("WAL error: %v", err) }
	defer wal.Close()

	db := NewDB(bp, wal, metrics)
	defer db.Close()

	rootPage, err := bp.NewPage()
	if err != nil { t.Fatalf("Failed to allocate slotted root page: %v", err) }
	bp.UnpinPage(rootPage.ID, true)

	index := NewMemIndex()

	// 2. Ingest Data via UQL CRUD Statements
	docs := map[uint64]string{
		1: "cybersecurity research adversarial ai",
		2: "cryptography modular point transformer",
		3: "adversarial ai ethical inversion",
	}

	for id, text := range docs {
		insertQuery := fmt.Sprintf("INSERT INTO docs VALUES (%d, '%s')", id, text)
		stmt, err := ParseUQL(insertQuery)
		if err != nil { t.Fatalf("Failed to parse insert query for doc %d: %v", id, err) }
		
		_, err = stmt.Execute(db, index, nil, nil, nil, walPath)
		if err != nil { t.Fatalf("Failed to execute insert for doc %d: %v", id, err) }
	}

	db.bp.FlushAll()
	db.wal.Checkpoint()

	// 3. Verify direct engine Read operations using the normalized string key format
	txn := db.BeginTxn()
	targetKey := []byte(fmt.Sprintf("%d", 1))
	got, err := db.Read(rootPage.ID, txn, targetKey)
	if err != nil {
		t.Fatalf("Slotted page read error: %v", err)
	}
	if string(got) != docs[1] {
		t.Fatalf("Slotted page read data mismatch. Expected '%s', got '%s'", docs[1], string(got))
	}
	db.CommitTxn(txn)

	// 4. Index Segmentation & Search
	index.WriteSegment(segPath)
	data, err := os.ReadFile(segPath)
	if err != nil { t.Fatalf("Failed to read segment: %v", err) }
	searcher := &SegmentSearcher{Data: data}

	query := "SELECT * FROM docs WHERE adversarial AND ai"
	stmt, err := ParseUQL(query)
	if err != nil { t.Fatalf("UQL Parse failed: %v", err) }

	results, err := stmt.Execute(db, index, searcher, nil, nil, walPath)
	if err != nil { t.Fatalf("UQL Execution failed: %v", err) }

	if len(results) != 2 {
		t.Errorf("Expected 2 results from UQL query, got %d", len(results))
	}
}

func TestORMLifecycle(t *testing.T) {
	dbPath, walPath := "orm_test.db", "orm_test.wal"
	os.Remove(dbPath); os.Remove(walPath)
	defer os.Remove(dbPath); defer os.Remove(walPath)

	device, err := NewOSFileDevice(dbPath)
	if err != nil { t.Fatalf("VFS Device error: %v", err) }
	
	disk := NewDiskManager(device)
	evictor := NewLRUEvictionPolicy()
	metrics := NewAtomicMetrics()
	bp := NewBufferPool(disk, 10, evictor, metrics)
	
	wal, err := NewBatchingWAL(walPath)
	if err != nil { t.Fatalf("WAL error: %v", err) }
	defer wal.Close()

	db := NewDB(bp, wal, metrics)
	defer db.Close()

	// Format Page 0 to handle initial slotted records cleanly
	rootPage, err := bp.NewPage()
	if err != nil { t.Fatalf("Failed to allocate slotted page: %v", err) }
	bp.UnpinPage(rootPage.ID, true)

	index := NewMemIndex()
	
	// 1. Initialize ORM Wrapper Instance
	orm := NewORM(db, index, nil, walPath)

	user := UserProfile{
		ID:       42,
		Username: "greg_disney",
		Role:     "Security Researcher",
		IsActive: true,
	}

	// 2. Validate ORM Insert Execution (Translates struct properties to UQL strings internally)
	err = orm.Insert(user)
	if err != nil {
		t.Fatalf("ORM Insert operation failed: %v", err)
	}

	// 3. Validate ORM Find (Hydrates an uninitialized destination struct address via key lookups)
	var fetchedUser UserProfile
	err = orm.Find(42, &fetchedUser)
	if err != nil {
		t.Fatalf("ORM Find operation failed: %v", err)
	}

	if fetchedUser.Username != user.Username || fetchedUser.Role != user.Role || fetchedUser.IsActive != user.IsActive {
		t.Errorf("ORM data validation mismatch. Ingested %+v, Fetched %+v", user, fetchedUser)
	}

	// 4. Validate ORM Update (Enforces payload mutations directly over slot directories)
	fetchedUser.Role = "Principal AI Red Teamer"
	fetchedUser.IsActive = false
	
	err = orm.Update(fetchedUser)
	if err != nil {
		t.Fatalf("ORM Update operation failed: %v", err)
	}

	var updatedUser UserProfile
	err = orm.Find(42, &updatedUser)
	if err != nil {
		t.Fatalf("ORM Refind after update failed: %v", err)
	}

	if updatedUser.Role != "Principal AI Red Teamer" || updatedUser.IsActive {
		t.Errorf("ORM Update failed to structurally commit mutated values: %+v", updatedUser)
	}

	// 5. Validate ORM Delete (Appends a high-speed tombstone marker sequence)
	err = orm.Delete(updatedUser)
	if err != nil {
		t.Fatalf("ORM Delete operation failed: %v", err)
	}

	var deadUser UserProfile
	err = orm.Find(42, &deadUser)
	if err == nil {
		t.Errorf("ORM Delete failure: record matching ID 42 should be unreadable or expired within slotted scope")
	}
}
