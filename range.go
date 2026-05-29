package ultimate_db

import (
	"bytes"
	"fmt"
)

// RangeCursor handles safe, page-crossing sequential iteration tracking
type RangeCursor struct {
	db        *DB
	txnID     uint64
	pageID    PageID
	startKey  []byte
	endKey    []byte
	currentKey []byte
	exhausted bool
}

// NewRangeCursor initializes a bounded iteration scanner over a specific page matrix
func (db *DB) NewRangeCursor(pageID PageID, txnID uint64, startKey, endKey []byte) *RangeCursor {
	return &RangeCursor{
		db:       db,
		txnID:    txnID,
		pageID:   pageID,
		startKey: startKey,
		endKey:   endKey,
	}
}

// Next advances the cursor to the next valid slot record, seamlessly stepping across underlying physical splits
func (rc *RangeCursor) Next() ([]byte, []byte, error) {
	if rc.exhausted {
		return nil, nil, nil
	}

	var foundKey, foundValue []byte
	var stepErr error

	// Scan compressed pages sequentially starting from our last tracked key boundary
	stepErr = rc.db.ScanCompressed(rc.pageID, rc.txnID, rc.startKey, func(key, value []byte) bool {
		// If we've already evaluated this key in a prior Next() cycle, keep moving forward
		if rc.currentKey != nil && bytes.Compare(key, rc.currentKey) <= 0 {
			return true
		}

		// Ensure we haven't broken past our upper range boundary constraint
		if rc.endKey != nil && bytes.Compare(key, rc.endKey) > 0 {
			rc.exhausted = true
			return false // Abort scan loop
		}

		// Capture the isolated key/value slice values
		foundKey = append([]byte(nil), key...)
		foundValue = append([]byte(nil), value...)
		return false // Break out of this page iteration to hand control back to caller
	})

	if stepErr != nil {
		return nil, nil, stepErr
	}

	if foundKey == nil {
		rc.exhausted = true
		return nil, nil, nil
	}

	// Update our state machine tracking registers to avoid infinite processing cycles
	rc.currentKey = foundKey
	rc.startKey = foundKey

	return foundKey, foundValue, nil
}
