package ultimate_db

import (
        "time"
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

// ============================================================================
// 1. Production ARIES Log Record Structure
// ============================================================================

// WalEntry represents a production-grade ARIES log record frame.
// 
// Physical Record Layout on disk:
// [0:4]   CRC32 Checksum (Validates whole frame payload)
// [4:12]  LSN (Log Sequence Number - structural byte offset)
// [12:20] TxnID (Transaction Identifier)
// [20:28] PrevLSN (Backward pointer to this transaction's prior log record)
// [28:29] LogType (Begin, Update, Commit, Abort, CLR, Checkpoint)
// [29:37] PageID (Target modification block address)
// [37:41] Key Length (kLen)
// [41:45] Old Value Length (undoLen - Before-Image length)
// [45:49] New Value Length (redoLen - After-Image length)
// [...:]  Variable Payload Binary Vector: [Key] + [Old Value] + [New Value]
type walEntry struct {
	lsn       LogSequenceNumber
	txnID     uint64
	prevLSN   LogSequenceNumber
	logType   uint8
	pageID    PageID
	key       []byte
	oldValue  []byte // Before-Image (Required for transaction Undo/Rollback)
	newValue  []byte // After-Image (Required for transaction Redo/Crash safety)
	errCh     chan error
}

type truncateReq struct {
	errCh chan error
}

// ============================================================================
// 2. Group Commit Write-Ahead Log Engine
// ============================================================================

type BatchingWAL struct {
	file         *os.File
	path         string
	queue        chan interface{}
	quit         chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	currentLSN   uint64 // Monotonically tracked internal byte offset marker
	txnPrevLSN   map[uint64]LogSequenceNumber
	lsnMu        sync.Mutex
}

func NewBatchingWAL(path string) (*BatchingWAL, error) {
	// Open using standard O_APPEND rules to protect streaming consistency boundaries
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	w := &BatchingWAL{
		file:       f,
		path:       path,
		queue:      make(chan interface{}, 4096),
		quit:       make(chan struct{}),
		currentLSN: uint64(stat.Size()),
		txnPrevLSN: make(map[uint64]LogSequenceNumber),
	}
	
	w.wg.Add(1)
	go w.flusher()
	return w, nil
}

// Append puts an operational update update vector into the group commit flusher loop.
func (w *BatchingWAL) Append(txnID uint64, logType uint8, pageID PageID, key, oldValue, newValue []byte) (LogSequenceNumber, error) {
	w.lsnMu.Lock()
	
	// Track the LSN backward chain pointer for this specific transaction
	pLSN := w.txnPrevLSN[txnID]
	
	errCh := make(chan error, 1)
	entry := walEntry{
		txnID:    txnID,
		prevLSN:  pLSN,
		logType:  logType,
		pageID:   pageID,
		key:      key,
		oldValue: oldValue,
		newValue: newValue,
		errCh:    errCh,
	}

	// Statically project the size of the record to compute its active LSN footprint before flushing
	recordSize := uint64(49 + len(key) + len(oldValue) + len(newValue))
	assignedLSN := LogSequenceNumber(w.currentLSN)
	entry.lsn = assignedLSN
	
	// Advance global LSN positioning vectors
	w.currentLSN += recordSize
	
	if logType == LogTypeCommit || logType == LogTypeAbort {
		delete(w.txnPrevLSN, txnID) // Tear down tracking table states on completion context
	} else {
		w.txnPrevLSN[txnID] = assignedLSN // Shift forward target pointers
	}
	w.lsnMu.Unlock()

	w.queue <- entry
	return assignedLSN, <-errCh
}

func (w *BatchingWAL) Checkpoint() error {
	errCh := make(chan error, 1)
	w.queue <- truncateReq{errCh: errCh}
	return <-errCh
}

// ============================================================================
// 3. Concurrent Pipeline Processing (Flusher Loop)
// ============================================================================

func (w *BatchingWAL) flusher() {
	defer w.wg.Done()
	bw := bufio.NewWriterSize(w.file, 64*1024) // 64KB optimized pipeline streaming frame buffer
	var pending []walEntry

	for {
		select {
		case msg := <-w.queue:
			switch req := msg.(type) {
			case walEntry:
				pending = append(pending, req)
				
				// Group Commit Drain Loop: Pack up to 1000 concurrent records into a single disk sync batch
			drainLoop:
				for len(pending) < 1000 {
					select {
					case nextMsg := <-w.queue:
						if e, ok := nextMsg.(walEntry); ok {
							pending = append(pending, e)
						} else {
							w.queue <- nextMsg
							break drainLoop
						}
					default:
						break drainLoop
					}
				}

				start := time.Now()
				for _, entry := range pending {
					w.writeEntry(bw, entry)
				}

				err := bw.Flush()
				if err == nil {
					err = w.file.Sync() // Strict kernel sync barrier boundary (fsync)
				}

				// Wake up and notify waiting transaction worker channels
				for _, entry := range pending {
					entry.errCh <- err
				}
				
				// Zero allocation slicing resets internal state vectors cleanly
				pending = pending[:0]
				_ = start // Retain hooks for performance tracing meters if necessary

			case truncateReq:
				bw.Flush()
				w.file.Sync()
				w.file.Close()
				
				// Physical log truncation must be safely coordinated to prevent active log data drops
				os.Truncate(w.path, 0)
				w.file, _ = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				bw.Reset(w.file)
				
				w.lsnMu.Lock()
				w.currentLSN = 0
				w.lsnMu.Unlock()
				req.errCh <- nil
			}
		case <-w.quit:
			return
		}
	}
}

func (w *BatchingWAL) writeEntry(bw *bufio.Writer, req walEntry) {
	buf := make([]byte, 49)
	
	binary.LittleEndian.PutUint64(buf[4:12], uint64(req.lsn))
	binary.LittleEndian.PutUint64(buf[12:20], req.txnID)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(req.prevLSN))
	buf[28] = req.logType
	binary.LittleEndian.PutUint64(buf[29:37], uint64(req.pageID))
	binary.LittleEndian.PutUint32(buf[37:41], uint32(len(req.key)))
	binary.LittleEndian.PutUint32(buf[41:45], uint32(len(req.oldValue)))
	binary.LittleEndian.PutUint32(buf[45:49], uint32(len(req.newValue)))

	// IEEE CRC32 Checksum covers the frame header and all subsequent binary images
	hash := crc32.NewIEEE()
	hash.Write(buf[4:49])
	hash.Write(req.key)
	hash.Write(req.oldValue)
	hash.Write(req.newValue)
	binary.LittleEndian.PutUint32(buf[0:4], hash.Sum32())

	bw.Write(buf)
	bw.Write(req.key)
	bw.Write(req.oldValue)
	bw.Write(req.newValue)
}

func (w *BatchingWAL) Close() error {
	w.closeOnce.Do(func() {
		close(w.quit)
		w.wg.Wait()
	})
	return w.file.Close()
}
