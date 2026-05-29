package ultimate_db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ORM provides a type-safe data mapping layer on top of ultimate_db.
type ORM struct {
	db       *DB
	index    *MemIndex
	searcher *SegmentSearcher
	walPath  string
}

// NewORM instantiates a clean object mapping wrapper.
func NewORM(db *DB, index *MemIndex, searcher *SegmentSearcher, walPath string) *ORM {
	return &ORM{
		db:       db,
		index:    index,
		searcher: searcher,
		walPath:  walPath,
	}
}

// parseModel inspects struct elements via reflection to extract identifiers and schemas.
func (o *ORM) parseModel(model interface{}) (uint64, string, []byte, error) {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return 0, "", nil, errors.New("provided model payload must be a struct or pointer to a struct")
	}

	t := val.Type()
	// Automatically generate a collection target descriptor (e.g., User struct maps to "users")
	tableName := strings.ToLower(t.Name()) + "s"

	idField := val.FieldByName("ID")
	if !idField.IsValid() {
		return 0, "", nil, errors.New("target model struct must contain a public 'ID' field")
	}
	if idField.Kind() != reflect.Uint64 {
		return 0, "", nil, errors.New("the model 'ID' field tracking identifier must be a uint64 primitive")
	}

	id := idField.Uint()

	// Serialize the Go struct directly into a JSON document string representation
	payload, err := json.Marshal(model)
	if err != nil {
		return 0, "", nil, fmt.Errorf("failed to serialize model properties to JSON: %w", err)
	}

	return id, tableName, payload, nil
}

// Insert routes an object through a synthesized UQL command to verify query logging behavior.
func (o *ORM) Insert(model interface{}) error {
	id, tableName, payload, err := o.parseModel(model)
	if err != nil {
		return err
	}

	// Escape single quotes inside the serialized JSON data payload to ensure safe syntax parsing boundaries
	escapedPayload := strings.ReplaceAll(string(payload), "'", "''")
	query := fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s')", tableName, id, escapedPayload)

	stmt, err := ParseUQL(query)
	if err != nil {
		return fmt.Errorf("orm pipeline compilation aborted: %w", err)
	}

	// Execute through full pipeline mechanics to cleanly populate active page frames and indices
	_, err = stmt.Execute(o.db, o.index, o.searcher, nil, nil, o.walPath)
	return err
}

// Find performs an ID-bound direct lookup, mapping raw bytes back to structured runtime objects.
func (o *ORM) Find(id uint64, out interface{}) error {
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr || outVal.Elem().Kind() != reflect.Struct {
		return errors.New("destination output parameter must be an initialized pointer to a struct type")
	}

	txn := o.db.BeginTxn()
	defer o.db.CommitTxn(txn)

	// Format matching string key primitives
	key := []byte(fmt.Sprintf("%d", id))
	rawBytes, err := o.db.Read(PageID(0), txn, key)
	if err != nil {
		return fmt.Errorf("orm record query failed: %w", err)
	}

	// Inflate JSON text data back into the targeted output structure address
	err = json.Unmarshal(rawBytes, out)
	if err != nil {
		return fmt.Errorf("orm failed to unmarshal matching physical row contents: %w", err)
	}

	return nil
}

// Update enforces atomicity by writing revised data states over structural slots directly.
func (o *ORM) Update(model interface{}) error {
	id, _, payload, err := o.parseModel(model)
	if err != nil {
		return err
	}

	txn := o.db.BeginTxn()
	key := []byte(fmt.Sprintf("%d", id))
	err = o.db.Write(PageID(0), txn, key, payload, 0)
	o.db.CommitTxn(txn)

	if err == nil && o.index != nil {
		o.index.Add(id, string(payload))
	}
	return err
}

// Delete writes an immediate tombstong vector expiration marker across targeted keys.
func (o *ORM) Delete(model interface{}) error {
	id, _, _, err := o.parseModel(model)
	if err != nil {
		return err
	}

	txn := o.db.BeginTxn()
	key := []byte(fmt.Sprintf("%d", id))
	err = o.db.Write(PageID(0), txn, key, nil, time.Nanosecond)
	o.db.CommitTxn(txn)

	return err
}
