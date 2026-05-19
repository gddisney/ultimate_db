package ultimate_db

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const PageSize = 32768
const PageHeaderSize = 8
const RecordHeaderSize = 24

type PageID uint64

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type Page struct {
	ID         PageID
	Data       [PageSize]byte
	PinCount   atomic.Int32
	IsDirty    bool
	Latch      sync.RWMutex
	MemVersion uint64 // Added for OCC validation
}

func (p *Page) Init() {
	binary.LittleEndian.PutUint32(p.Data[0:4], uint32(PageHeaderSize))
	binary.LittleEndian.PutUint32(p.Data[4:8], 0)
}
func (p *Page) GetFreeSpaceOffset() uint32 {
	return binary.LittleEndian.Uint32(p.Data[0:4])
}
func (p *Page) SetFreeSpaceOffset(offset uint32) {
	binary.LittleEndian.PutUint32(p.Data[0:4], offset)
}

type DiskManager struct {
	file *os.File
}

func NewDiskManager(dbPath string) (*DiskManager, error) {
	file, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &DiskManager{file: file}, nil
}

func (d *DiskManager) ReadPage(id PageID, data *[PageSize]byte) error {
	_, err := d.file.ReadAt(data[:], int64(id)*PageSize)
	return err
}

func (d *DiskManager) WritePage(id PageID, data *[PageSize]byte) error {
	_, err := d.file.WriteAt(data[:], int64(id)*PageSize)
	return err
}

type BufferPool struct {
	disk      *DiskManager
	frames    []*Page
	pageTable map[PageID]*list.Element
	lru       *list.List
	freeList  []int
	mu        sync.Mutex
}

func NewBufferPool(disk *DiskManager, poolSize int) *BufferPool {
	bp := &BufferPool{
		disk:      disk,
		frames:    make([]*Page, poolSize),
		pageTable: make(map[PageID]*list.Element),
		lru:       list.New(),
		freeList:  make([]int, poolSize),
	}
	for i := 0; i < poolSize; i++ {
		bp.frames[i] = &Page{}
		bp.freeList[i] = i
	}
	return bp
}

func (bp *BufferPool) FetchPage(id PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if elem, exists := bp.pageTable[id]; exists {
		bp.lru.MoveToFront(elem)
		frameIdx := elem.Value.(int)
		page := bp.frames[frameIdx]
		page.PinCount.Add(1)
		return page, nil
	}

	frameIdx, err := bp.getAvailableFrame()
	if err != nil {
		return nil, err
	}

	page := bp.frames[frameIdx]
	page.ID = id
	page.PinCount.Store(1)
	page.IsDirty = false

	if err := bp.disk.ReadPage(id, &page.Data); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	elem := bp.lru.PushFront(frameIdx)
	bp.pageTable[id] = elem
	return page, nil
}

func (bp *BufferPool) UnpinPage(id PageID, isDirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if elem, exists := bp.pageTable[id]; exists {
		frameIdx := elem.Value.(int)
		page := bp.frames[frameIdx]
		page.PinCount.Add(-1)
		if isDirty {
			page.IsDirty = true
		}
	}
}

func (bp *BufferPool) getAvailableFrame() (int, error) {
	if len(bp.freeList) > 0 {
		idx := bp.freeList[0]
		bp.freeList = bp.freeList[1:]
		return idx, nil
	}

	for e := bp.lru.Back(); e != nil; e = e.Prev() {
		frameIdx := e.Value.(int)
		page := bp.frames[frameIdx]
		
		if page.PinCount.Load() == 0 {
			if page.IsDirty {
				if err := bp.disk.WritePage(page.ID, &page.Data); err != nil {
					return -1, err
				}
			}
			
			delete(bp.pageTable, page.ID)
			bp.lru.Remove(e)
			return frameIdx, nil
		}
	}
	return -1, errors.New("buffer pool exhausted")
}

func (bp *BufferPool) NewPage() (*Page, error) {
	bp.mu.Lock()
	stat, err := bp.disk.file.Stat()
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}
	newID := PageID(stat.Size() / PageSize)
	empty := [PageSize]byte{}
	if err := bp.disk.WritePage(newID, &empty); err != nil {
		bp.mu.Unlock()
		return nil, err
	}
	bp.mu.Unlock()
	return bp.FetchPage(newID)
}

func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	var dirtyPages []*Page

	// 1. Gather and pin dirty pages fast to avoid blocking the pool
	for _, page := range bp.frames {
		if page.IsDirty {
			page.PinCount.Add(1) // Pin it so eviction ignores it during flush
			dirtyPages = append(dirtyPages, page)
		}
	}
	bp.mu.Unlock() // Let the database continue working!

	var firstErr error

	// 2. Flush to disk without holding the global lock
	for _, page := range dirtyPages {
		page.Latch.RLock() // Prevent partial writes being flushed
		err := bp.disk.WritePage(page.ID, &page.Data)
		page.Latch.RUnlock()

		bp.mu.Lock()
		if err == nil {
			page.IsDirty = false
		} else if firstErr == nil {
			firstErr = err
		}
		page.PinCount.Add(-1) // Unpin
		bp.mu.Unlock()
	}

	return firstErr
}

// ==========================================
// 3. GROUP COMMIT WRITE-AHEAD LOG & CHECKPOINTING
// ==========================================

type walEntry struct {
	txnID     uint64
	expiresAt int64
	id        PageID
	key       []byte
	value     []byte
	errCh     chan error
}

type truncateReq struct {
	errCh chan error
}

type BatchingWAL struct {
	file         *os.File
	path         string
	queue        chan interface{} // Handles both walEntry and truncateReq
	quit         chan struct{}
	wg           sync.WaitGroup
}

func NewBatchingWAL(path string) (*BatchingWAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	w := &BatchingWAL{
		file:  f,
		path:  path,
		queue: make(chan interface{}, 4096),
		quit:  make(chan struct{}),
	}
	w.wg.Add(1)
	go w.flusher()
	return w, nil
}

func (w *BatchingWAL) Append(txnID uint64, expiresAt int64, id PageID, key, value []byte) error {
	errCh := make(chan error, 1)
	w.queue <- walEntry{txnID: txnID, expiresAt: expiresAt, id: id, key: key, value: value, errCh: errCh}
	return <-errCh
}

func (w *BatchingWAL) Checkpoint() error {
	errCh := make(chan error, 1)
	w.queue <- truncateReq{errCh: errCh}
	return <-errCh
}

func (w *BatchingWAL) flusher() {
	defer w.wg.Done()
	bw := bufio.NewWriterSize(w.file, 32*1024)
	var pending []walEntry

	for {
		select {
		case msg := <-w.queue:
			switch req := msg.(type) {
			case walEntry:
				pending = append(pending, req)
			drainLoop:
				for len(pending) < 1000 {
					select {
					case nextMsg := <-w.queue:
						if e, ok := nextMsg.(walEntry); ok {
							pending = append(pending, e)
						} else {
							// Push non-entry messages back and break drain
							w.queue <- nextMsg
							break drainLoop
						}
					default:
						break drainLoop
					}
				}

				for _, entry := range pending {
					w.writeEntry(bw, entry)
				}

				err := bw.Flush()
				if err == nil {
					err = w.file.Sync()
				}

				for _, entry := range pending {
					entry.errCh <- err
				}
				pending = pending[:0]

			case truncateReq:
				// Execute Log Truncation
				bw.Flush()
				w.file.Sync()
				w.file.Close()
				err := os.Truncate(w.path, 0)
				
				w.file, err = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				bw.Reset(w.file)
				req.errCh <- err
			}

		case <-w.quit:
			return
		}
	}
}

func (w *BatchingWAL) writeEntry(bw *bufio.Writer, req walEntry) {
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint64(buf[4:12], req.txnID)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(req.expiresAt))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(req.id))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(len(req.key)))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(len(req.value)))

	hash := crc32.NewIEEE()
	hash.Write(buf[4:36])
	hash.Write(req.key)
	hash.Write(req.value)
	binary.LittleEndian.PutUint32(buf[0:4], hash.Sum32())

	bw.Write(buf)
	bw.Write(req.key)
	bw.Write(req.value)
}

func (w *BatchingWAL) Close() error {
	close(w.quit)
	w.wg.Wait()
	return w.file.Close()
}

// ==========================================
// 4. CORE ENGINE (MVCC + TTL ACTIVE GC)
// ==========================================

type DB struct {
	bp         *BufferPool
	wal        *BatchingWAL
	nextTxnID  atomic.Uint64
	activeTxns sync.Map
	quit       chan struct{}
	wg         sync.WaitGroup
}

func NewDB(bp *BufferPool, wal *BatchingWAL) *DB {
	db := &DB{
		bp:   bp,
		wal:  wal,
		quit: make(chan struct{}),
	}
	db.startCheckpointer()
	return db
}

func (db *DB) startCheckpointer() {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 1. Flush all dirty pages to the permanent DB file
				db.bp.FlushAll()
				// 2. Since pages are synced, WAL can be safely cleared
				db.wal.Checkpoint()
			case <-db.quit:
				return
			}
		}
	}()
}

func (db *DB) BeginTxn() uint64 {
	id := db.nextTxnID.Add(1)
	db.activeTxns.Store(id, true) // Mark as active/uncommitted
	return id
}

func (db *DB) CommitTxn(txnID uint64) {
	db.activeTxns.Delete(txnID) // Mark as committed globally
}

func (db *DB) compactPage(page *Page) {
	currentOffset := uint32(PageHeaderSize)
	freeOffset := page.GetFreeSpaceOffset()
	now := time.Now().UnixNano()

	type record struct {
		txnID     uint64
		expiresAt uint64
		key       []byte
		val       []byte
	}
	latestRecords := make(map[string]record)

	for currentOffset < freeOffset {
		recordSlice := page.Data[currentOffset:]

		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := binary.LittleEndian.Uint64(recordSlice[8:16])
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])

		k := recordSlice[24 : 24+keyLen]
		v := recordSlice[24+keyLen : 24+keyLen+valLen]

		existing, exists := latestRecords[string(k)]
		if !exists || recordTxnID > existing.txnID {
			if int64(expiresAt) > now {
				keyCopy := make([]byte, keyLen)
				copy(keyCopy, k)
				valCopy := make([]byte, valLen)
				copy(valCopy, v)
				latestRecords[string(k)] = record{
					txnID:     recordTxnID,
					expiresAt: expiresAt,
					key:       keyCopy,
					val:       valCopy,
				}
			} else if exists {
				delete(latestRecords, string(k))
			}
		}
		currentOffset += RecordHeaderSize + keyLen + valLen
	}

	page.Init()
	newOffset := uint32(PageHeaderSize)

	for _, rec := range latestRecords {
		targetSlice := page.Data[newOffset:]
		binary.LittleEndian.PutUint64(targetSlice[0:8], rec.txnID)
		binary.LittleEndian.PutUint64(targetSlice[8:16], rec.expiresAt)
		binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(rec.key)))
		binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(rec.val)))
		
		copy(targetSlice[24:24+len(rec.key)], rec.key)
		copy(targetSlice[24+len(rec.key):24+len(rec.key)+len(rec.val)], rec.val)
		
		newOffset += RecordHeaderSize + uint32(len(rec.key)) + uint32(len(rec.val))
	}

	page.SetFreeSpaceOffset(newOffset)
}

func (db *DB) Write(pageID PageID, txnID uint64, key, value []byte, ttl time.Duration) error {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	} else {
		expiresAt = math.MaxInt64
	}

	if err := db.wal.Append(txnID, expiresAt, pageID, key, value); err != nil {
		return err
	}

	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer db.bp.UnpinPage(pageID, true)

	page.Latch.Lock()
	defer page.Latch.Unlock()

	freeOffset := page.GetFreeSpaceOffset()
	if freeOffset == 0 {
		page.Init()
		freeOffset = page.GetFreeSpaceOffset()
	}

	recordSize := RecordHeaderSize + len(key) + len(value)
	
	if freeOffset+uint32(recordSize) > PageSize {
		db.compactPage(page)
		freeOffset = page.GetFreeSpaceOffset()
	}

	if freeOffset+uint32(recordSize) > PageSize {
		return errors.New("page overflow")
	}

	targetSlice := page.Data[freeOffset : freeOffset+uint32(recordSize)]
	binary.LittleEndian.PutUint64(targetSlice[0:8], txnID)
	binary.LittleEndian.PutUint64(targetSlice[8:16], uint64(expiresAt))
	binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(key)))
	binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(value)))
	copy(targetSlice[24:24+len(key)], key)
	copy(targetSlice[24+len(key):], value)

	page.SetFreeSpaceOffset(freeOffset + uint32(recordSize))
	return nil
}

func (db *DB) Read(pageID PageID, readTxnID uint64, key []byte) ([]byte, error) {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	defer db.bp.UnpinPage(pageID, false)

	page.Latch.RLock()
	defer page.Latch.RUnlock()

	freeOffset := page.GetFreeSpaceOffset()
	if freeOffset == 0 {
		return nil, errors.New("key not found")
	}

	currentOffset := uint32(PageHeaderSize)
	var latestValue []byte
	var highestTxnID uint64 = 0
	now := time.Now().UnixNano()

	for currentOffset < freeOffset {
		recordSlice := page.Data[currentOffset:]

		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := int64(binary.LittleEndian.Uint64(recordSlice[8:16]))
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])

		recordKey := recordSlice[24 : 24+keyLen]
		recordVal := recordSlice[24+keyLen : 24+keyLen+valLen]

		// ISOLATION CHECK: Is this record committed, or is it our own uncommitted write?
		_, isActive := db.activeTxns.Load(recordTxnID)
		isCommitted := !isActive || recordTxnID == readTxnID

		if string(recordKey) == string(key) && recordTxnID <= readTxnID && recordTxnID >= highestTxnID && isCommitted {
			if expiresAt > now {
				highestTxnID = recordTxnID
				latestValue = make([]byte, valLen)
				copy(latestValue, recordVal)
			} else {
				highestTxnID = recordTxnID 
				latestValue = nil 
			}
		}
		currentOffset += RecordHeaderSize + keyLen + valLen
	}

	if latestValue != nil {
		return latestValue, nil
	}
	return nil, errors.New("key not found or expired")
}

func (db *DB) Scan(pageID PageID, readTxnID uint64, prefix []byte, iter func(key, value []byte) bool) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer db.bp.UnpinPage(pageID, false)

	page.Latch.RLock()
	defer page.Latch.RUnlock()

	freeOffset := page.GetFreeSpaceOffset()
	if freeOffset == 0 || freeOffset <= uint32(PageHeaderSize) {
		return nil 
	}

	type scanRecord struct {
		txnID uint64
		val   []byte
	}
	
	latest := make(map[string]scanRecord)
	now := time.Now().UnixNano()
	currentOffset := uint32(PageHeaderSize)

	for currentOffset < freeOffset {
		recordSlice := page.Data[currentOffset:]

		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := int64(binary.LittleEndian.Uint64(recordSlice[8:16]))
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])

		recordKey := recordSlice[24 : 24+keyLen]
		recordVal := recordSlice[24+keyLen : 24+keyLen+valLen]

		// ISOLATION CHECK: Is this record committed, or is it our own uncommitted write?
		_, isActive := db.activeTxns.Load(recordTxnID)
		isCommitted := !isActive || recordTxnID == readTxnID

		if bytes.HasPrefix(recordKey, prefix) && recordTxnID <= readTxnID && isCommitted {
			existing, exists := latest[string(recordKey)]
			
			if !exists || recordTxnID >= existing.txnID {
				if expiresAt > now {
					valCopy := make([]byte, valLen)
					copy(valCopy, recordVal)
					latest[string(recordKey)] = scanRecord{
						txnID: recordTxnID,
						val:   valCopy,
					}
				} else {
					delete(latest, string(recordKey))
				}
			}
		}
		currentOffset += RecordHeaderSize + keyLen + valLen
	}

	for k, rec := range latest {
		if !iter([]byte(k), rec.val) {
			break
		}
	}

	return nil
}

func (db *DB) WriteCompressed(pageID PageID, txnID uint64, key, value []byte, ttl time.Duration) error {
	compressedVal := make([]byte, MaxBlockSize)
	compLen, err := Compress(value, compressedVal)
	if err != nil {
		return fmt.Errorf("middle-out compression failed: %w", err)
	}
	return db.Write(pageID, txnID, key, compressedVal[:compLen], ttl)
}

func (db *DB) ReadCompressed(pageID PageID, readTxnID uint64, key []byte) ([]byte, error) {
	compressedVal, err := db.Read(pageID, readTxnID, key)
	if err != nil {
		return nil, err
	}
	if len(compressedVal) < 6 {
		return nil, errors.New("corrupted payload: missing middle-out magic header")
	}
	headerSign := uint16(compressedVal[0])<<8 | uint16(compressedVal[1])
	if headerSign != MagicHeader {
		return nil, errors.New("corrupted payload: invalid signature, data may not be compressed")
	}
	originalRawLen := int(compressedVal[2])<<8 | int(compressedVal[3])
	decompressedVal := make([]byte, originalRawLen)
	_, err = Decompress(compressedVal, decompressedVal)
	if err != nil {
		return nil, fmt.Errorf("middle-out decompression failed: %w", err)
	}
	return decompressedVal, nil
}

func (db *DB) ScanCompressed(pageID PageID, readTxnID uint64, prefix []byte, iter func(key, value []byte) bool) error {
	decompressingIter := func(key, compressedVal []byte) bool {
		if len(compressedVal) < 6 {
			return iter(key, compressedVal) 
		}
		originalRawLen := int(compressedVal[2])<<8 | int(compressedVal[3])
		decompressedVal := make([]byte, originalRawLen)
		_, err := Decompress(compressedVal, decompressedVal)
		if err != nil {
			return false
		}
		return iter(key, decompressedVal)
	}
	return db.Scan(pageID, readTxnID, prefix, decompressingIter)
}

func (db *DB) restoreWrite(txnID uint64, expiresAt int64, pageID PageID, key, value []byte) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer db.bp.UnpinPage(pageID, true)

	page.Latch.Lock()
	defer page.Latch.Unlock()

	freeOffset := page.GetFreeSpaceOffset()
	if freeOffset == 0 {
		page.Init()
		freeOffset = page.GetFreeSpaceOffset()
	}

	recordSize := RecordHeaderSize + len(key) + len(value)
	
	if freeOffset+uint32(recordSize) > PageSize {
		db.compactPage(page)
		freeOffset = page.GetFreeSpaceOffset()
	}
	
	if freeOffset+uint32(recordSize) > PageSize {
		return errors.New("page overflow during recovery")
	}

	targetSlice := page.Data[freeOffset : freeOffset+uint32(recordSize)]
	binary.LittleEndian.PutUint64(targetSlice[0:8], txnID)
	binary.LittleEndian.PutUint64(targetSlice[8:16], uint64(expiresAt))
	binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(key)))
	binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(value)))
	copy(targetSlice[24:24+len(key)], key)
	copy(targetSlice[24+len(key):], value)

	page.SetFreeSpaceOffset(freeOffset + uint32(recordSize))
	return nil
}

func (db *DB) Close() error {
	close(db.quit)
	db.wg.Wait()
	if err := db.bp.FlushAll(); err != nil {
		return err
	}
	return db.wal.Close()
}

func RecoverDB(walPath string, db *DB) error {
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil 
		}
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	headerBuf := make([]byte, 36)
	var maxTxnID uint64

	for {
		_, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.New("corrupted WAL: unexpected EOF")
		}

		expectedCRC := binary.LittleEndian.Uint32(headerBuf[0:4])
		txnID := binary.LittleEndian.Uint64(headerBuf[4:12])
		expiresAt := int64(binary.LittleEndian.Uint64(headerBuf[12:20]))
		pageID := PageID(binary.LittleEndian.Uint64(headerBuf[20:28]))
		keyLen := binary.LittleEndian.Uint32(headerBuf[28:32])
		valLen := binary.LittleEndian.Uint32(headerBuf[32:36])

		if keyLen == 0 {
			return errors.New("corrupted WAL: missing key")
		}
		if keyLen > 1<<20 || valLen > 1<<28 {
			return errors.New("corrupted WAL: limits exceeded")
		}

		payload := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return errors.New("corrupted WAL: missing payload")
		}

		hash := crc32.NewIEEE()
		hash.Write(headerBuf[4:36])
		hash.Write(payload)
		if hash.Sum32() != expectedCRC {
			return errors.New("corrupted WAL: CRC mismatch")
		}

		key := payload[:keyLen]
		val := payload[keyLen:]

		if err := db.restoreWrite(txnID, expiresAt, pageID, key, val); err != nil {
			return err
		}

		if txnID > maxTxnID {
			maxTxnID = txnID
		}
	}

	db.nextTxnID.Store(maxTxnID)
	return nil
}

const PrefixHash = "H:"

func (db *DB) HSet(pageID PageID, txnID uint64, hashKey, field, value []byte, ttl time.Duration) error {
	compositeKey := make([]byte, 0, len(PrefixHash)+len(hashKey)+1+len(field))
	compositeKey = append(compositeKey, []byte(PrefixHash)...)
	compositeKey = append(compositeKey, hashKey...)
	compositeKey = append(compositeKey, ':')
	compositeKey = append(compositeKey, field...)

	return db.Write(pageID, txnID, compositeKey, value, ttl)
}

// ==========================================
// 7. B+ TREE & CURSORS
// ==========================================

const (
	PageTypeInternal uint16 = 1
	PageTypeLeaf     uint16 = 2
	BTreeHeaderSize  uint32 = 24
)

var ErrPageFull = errors.New("page requires splitting")

type BTreePage struct{ *Page }

func (p *BTreePage) BTreeInit() {
	for i := 0; i < int(BTreeHeaderSize); i++ {
		p.Data[i] = 0
	}
}

func (p *BTreePage) PageType() uint16             { return binary.LittleEndian.Uint16(p.Data[0:2]) }
func (p *BTreePage) SetPageType(t uint16)         { binary.LittleEndian.PutUint16(p.Data[0:2], t) }
func (p *BTreePage) NumCells() uint16             { return binary.LittleEndian.Uint16(p.Data[2:4]) }
func (p *BTreePage) SetNumCells(n uint16)         { binary.LittleEndian.PutUint16(p.Data[2:4], n) }
func (p *BTreePage) NextLeafID() PageID           { return PageID(binary.LittleEndian.Uint64(p.Data[4:12])) }
func (p *BTreePage) SetNextLeafID(id PageID)      { binary.LittleEndian.PutUint64(p.Data[4:12], uint64(id)) }
func (p *BTreePage) ParentID() PageID             { return PageID(binary.LittleEndian.Uint64(p.Data[12:20])) }
func (p *BTreePage) SetParentID(id PageID)        { binary.LittleEndian.PutUint64(p.Data[12:20], uint64(id)) }
func (p *BTreePage) RightmostChildID() PageID     { return PageID(binary.LittleEndian.Uint32(p.Data[20:24])) }
func (p *BTreePage) SetRightmostChildID(id PageID){ binary.LittleEndian.PutUint32(p.Data[20:24], uint32(id)) }
func (p *BTreePage) IsSafeForInsert(requiredBytes uint32) bool {
	numCells := p.NumCells()
	offset := BTreeHeaderSize

	for i := uint16(0); i < numCells; i++ {
		if p.PageType() == PageTypeLeaf {
			kLen := binary.LittleEndian.Uint16(p.Data[offset : offset+2])
			vLen := binary.LittleEndian.Uint16(p.Data[offset+2 : offset+4])
			offset += 4 + uint32(kLen) + uint32(vLen)
		} else {
			kLen := binary.LittleEndian.Uint16(p.Data[offset : offset+2])
			offset += 10 + uint32(kLen)
		}
	}
	return (offset + requiredBytes) < PageSize
}

type BTree struct {
	bp     *BufferPool
	rootID PageID
}

func NewBTree(bp *BufferPool, rootID PageID) *BTree {
	return &BTree{bp: bp, rootID: rootID}
}

type BTreeCursor struct {
	tree     *BTree
	currNode *BTreePage
	cellIdx  uint16
	offset   uint32
	isEOF    bool
}

func NewBTreeCursor(tree *BTree) (*BTreeCursor, error) {
	currID := tree.rootID
	for {
		raw, err := tree.bp.FetchPage(currID)
		if err != nil {
			return nil, err
		}
		raw.Latch.RLock()
		node := &BTreePage{raw}

		if node.PageType() == PageTypeLeaf {
			return &BTreeCursor{
				tree:     tree,
				currNode: node,
				cellIdx:  0,
				offset:   BTreeHeaderSize,
				isEOF:    node.NumCells() == 0,
			}, nil
		}

		childID := PageID(binary.LittleEndian.Uint64(node.Data[BTreeHeaderSize+2 : BTreeHeaderSize+10]))
		node.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		currID = childID
	}
}

func (c *BTreeCursor) Next() ([]byte, []byte, error) {
	if c.isEOF {
		return nil, nil, io.EOF
	}

	kLen := binary.LittleEndian.Uint16(c.currNode.Data[c.offset : c.offset+2])
	vLen := binary.LittleEndian.Uint16(c.currNode.Data[c.offset+2 : c.offset+4])
	key := make([]byte, kLen)
	val := make([]byte, vLen)
	copy(key, c.currNode.Data[c.offset+4 : c.offset+4+uint32(kLen)])
	copy(val, c.currNode.Data[c.offset+4+uint32(kLen) : c.offset+4+uint32(kLen)+uint32(vLen)])

	c.cellIdx++
	c.offset += 4 + uint32(kLen) + uint32(vLen)

	if c.cellIdx >= c.currNode.NumCells() {
		nextLeafID := c.currNode.NextLeafID()
		c.currNode.Latch.RUnlock()
		c.tree.bp.UnpinPage(c.currNode.ID, false)

		if nextLeafID == 0 {
			c.isEOF = true
			c.currNode = nil
		} else {
			nextRaw, err := c.tree.bp.FetchPage(nextLeafID)
			if err != nil {
				return nil, nil, err
			}
			nextRaw.Latch.RLock()
			c.currNode = &BTreePage{nextRaw}
			c.cellIdx = 0
			c.offset = BTreeHeaderSize
		}
	}
	return key, val, nil
}

func (c *BTreeCursor) Close() {
	if c.currNode != nil {
		c.currNode.Latch.RUnlock()
		c.tree.bp.UnpinPage(c.currNode.ID, false)
		c.currNode = nil
		c.isEOF = true
	}
}

type JoinResult struct {
	Key       []byte
	LeftValue []byte
	RightValue []byte
}

func SortMergeJoin(leftTree, rightTree *BTree) ([]JoinResult, error) {
	leftCursor, err := NewBTreeCursor(leftTree)
	if err != nil {
		return nil, err
	}
	defer leftCursor.Close()

	rightCursor, err := NewBTreeCursor(rightTree)
	if err != nil {
		return nil, err
	}
	defer rightCursor.Close()

	var results []JoinResult
	lKey, lVal, lErr := leftCursor.Next()
	rKey, rVal, rErr := rightCursor.Next()

	for lErr == nil && rErr == nil {
		cmp := bytes.Compare(lKey, rKey)
		if cmp == 0 {
			results = append(results, JoinResult{Key: lKey, LeftValue: lVal, RightValue: rVal})
			lKey, lVal, lErr = leftCursor.Next()
			rKey, rVal, rErr = rightCursor.Next()
		} else if cmp < 0 {
			lKey, lVal, lErr = leftCursor.Next()
		} else {
			rKey, rVal, rErr = rightCursor.Next()
		}
	}

	if lErr != nil && lErr != io.EOF {
		return nil, lErr
	}
	if rErr != nil && rErr != io.EOF {
		return nil, rErr
	}
	return results, nil
}

func (tree *BTree) Scan(prefix string) ([][]byte, [][]byte, error) {
	var keys, values [][]byte
	currNode, err := tree.FindLeaf([]byte(prefix))
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currNode.ID, false)
	}()

	for {
		numCells := currNode.NumCells()
		offset := uint32(BTreeHeaderSize)

		for i := uint16(0); i < numCells; i++ {
			kLen := binary.LittleEndian.Uint16(currNode.Data[offset : offset+2])
			vLen := binary.LittleEndian.Uint16(currNode.Data[offset+2 : offset+4])
			key := currNode.Data[offset+4 : offset+4+uint32(kLen)]
			val := currNode.Data[offset+4+uint32(kLen) : offset+4+uint32(kLen)+uint32(vLen)]

			if strings.HasPrefix(string(key), prefix) {
				keyCopy, valCopy := make([]byte, len(key)), make([]byte, len(val))
				copy(keyCopy, key)
				copy(valCopy, val)
				keys = append(keys, keyCopy)
				values = append(values, valCopy)
			}
			offset += 4 + uint32(kLen) + uint32(vLen)
		}

		nextID := currNode.NextLeafID()
		if nextID == 0 {
			break
		}

		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currNode.ID, false)
		rawPage, err := tree.bp.FetchPage(nextID)
		if err != nil {
			return keys, values, err
		}
		rawPage.Latch.RLock()
		currNode = &BTreePage{rawPage}
	}
	return keys, values, nil
}

func (tree *BTree) FindLeaf(key []byte) (*BTreePage, error) {
	currID := tree.rootID
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return nil, err
	}
	currRaw.Latch.RLock()
	currNode := &BTreePage{currRaw}

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			currNode.Latch.RUnlock()
			tree.bp.UnpinPage(currID, false)
			return nil, err
		}
		childRaw.Latch.RLock()
		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		currID = childID
		currNode = &BTreePage{childRaw}
	}
	return currNode, nil
}

func (tree *BTree) Insert(key, value []byte) error {
	reqBytes := uint32(4 + len(key) + len(value))

	// OPTIMISTIC PASS: Traverse down holding only Read-Locks
	currID := tree.rootID
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return err
	}
	
	currRaw.Latch.RLock()
	currNode := &BTreePage{currRaw}

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			currRaw.Latch.RUnlock()
			tree.bp.UnpinPage(currID, false)
			return err
		}
		childRaw.Latch.RLock()
		currRaw.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		
		currID = childID
		currRaw = childRaw
		currNode = &BTreePage{currRaw}
	}

	// We are at the leaf with a Read Lock. Is it safe to insert?
	if currNode.IsSafeForInsert(reqBytes) {
		// Capture version before releasing the Read Lock
		version := currRaw.MemVersion 

		currRaw.Latch.RUnlock()
		currRaw.Latch.Lock()

		// OCC VALIDATION: Did another thread mutate this node while we escalated?
		if currRaw.MemVersion != version {
			currRaw.Latch.Unlock()
			tree.bp.UnpinPage(currID, false)
			return tree.pessimisticInsert(key, value) // Fallback safely
		}

		if currNode.IsSafeForInsert(reqBytes) {
			err := tree.insertIntoLeaf(currNode, key, value)
			currRaw.Latch.Unlock()
			tree.bp.UnpinPage(currID, true)
			return err
		}

		currRaw.Latch.Unlock()
	} else {
		currRaw.Latch.RUnlock()
	}

	tree.bp.UnpinPage(currID, false)

	// PESSIMISTIC PASS: Fallback to full Latch-Crabbing for structural splits
	return tree.pessimisticInsert(key, value)
}

func (tree *BTree) pessimisticInsert(key, value []byte) error {
	currID := tree.rootID
	var lockedAncestors []*Page 
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return err
	}
	currRaw.Latch.Lock()
	lockedAncestors = append(lockedAncestors, currRaw)
	currNode := &BTreePage{currRaw}

	reqBytes := uint32(4 + len(key) + len(value))

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			tree.releaseAncestors(lockedAncestors)
			return err
		}
		childRaw.Latch.Lock()
		childNode := &BTreePage{childRaw}

		if childNode.IsSafeForInsert(reqBytes) {
			tree.releaseAncestors(lockedAncestors)
			lockedAncestors = []*Page{}
		}

		lockedAncestors = append(lockedAncestors, childRaw)
		currID = childID
		currNode = childNode
	}

	err = tree.insertIntoLeaf(currNode, key, value)
	if err != nil && errors.Is(err, ErrPageFull) {
		err = tree.SplitLeaf(currNode, lockedAncestors)
	}

	tree.releaseAncestors(lockedAncestors)
	return err
}

func (tree *BTree) releaseAncestors(ancestors []*Page) {
	for i := len(ancestors) - 1; i >= 0; i-- {
		page := ancestors[i]
		page.Latch.Unlock()
		tree.bp.UnpinPage(page.ID, true) 
	}
}

// ==========================================
// 8. B+ TREE NODE SPLITTING
// ==========================================

func (tree *BTree) SplitLeaf(node *BTreePage, lockedAncestors []*Page) error {
	newRawPage, err := tree.bp.NewPage()
	if err != nil {
		return err
	}
	newRawPage.Latch.Lock()
	defer newRawPage.Latch.Unlock()
	defer tree.bp.UnpinPage(newRawPage.ID, true)

	newLeaf := &BTreePage{newRawPage}
	newLeaf.BTreeInit()
	newLeaf.SetPageType(PageTypeLeaf)

	newLeaf.SetNextLeafID(node.NextLeafID())
	node.SetNextLeafID(newLeaf.ID)
	newLeaf.SetParentID(node.ParentID())

	numCells := node.NumCells()
	midPoint := numCells / 2

	offset := BTreeHeaderSize
	var midKey []byte

	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		vLen := binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4])

		if i == midPoint {
			midKey = make([]byte, kLen)
			copy(midKey, node.Data[offset+4 : offset+4+uint32(kLen)])
			break
		}
		offset += 4 + uint32(kLen) + uint32(vLen)
	}

	bytesToMove := PageSize - offset
	copy(newLeaf.Data[BTreeHeaderSize:], node.Data[offset:offset+bytesToMove])

	newLeaf.SetNumCells(numCells - midPoint)
	node.SetNumCells(midPoint)

	for i := offset; i < PageSize; i++ {
		node.Data[i] = 0
	}

	node.MemVersion++
	newLeaf.MemVersion++

	return tree.promoteToParent(node.ID, newLeaf.ID, midKey, lockedAncestors)
}

func (tree *BTree) SplitInternalNode(node *BTreePage, lockedAncestors []*Page) error {
	newRawPage, err := tree.bp.NewPage()
	if err != nil {
		return err
	}
	newRawPage.Latch.Lock()
	defer newRawPage.Latch.Unlock()
	defer tree.bp.UnpinPage(newRawPage.ID, true)

	newInternal := &BTreePage{newRawPage}
	newInternal.BTreeInit()
	newInternal.SetPageType(PageTypeInternal)
	newInternal.SetParentID(node.ParentID())

	numCells := node.NumCells()
	midPoint := numCells / 2

	offset := BTreeHeaderSize
	var pivotKey []byte
	var midCellLeftChild PageID
	var midCellEnd uint32

	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		if i == midPoint {
			midCellLeftChild = PageID(binary.LittleEndian.Uint64(node.Data[offset+2 : offset+10]))
			pivotKey = make([]byte, kLen)
			copy(pivotKey, node.Data[offset+10 : offset+10+uint32(kLen)])
			midCellEnd = offset + 10 + uint32(kLen)
			break
		}
		offset += 10 + uint32(kLen)
	}

	newInternal.SetRightmostChildID(node.RightmostChildID())
	node.SetRightmostChildID(midCellLeftChild)

	bytesToMove := PageSize - midCellEnd
	copy(newInternal.Data[BTreeHeaderSize:], node.Data[midCellEnd : midCellEnd+bytesToMove])

	newInternal.SetNumCells(numCells - midPoint - 1)
	node.SetNumCells(midPoint)

	for i := offset; i < PageSize; i++ {
		node.Data[i] = 0
	}

	node.MemVersion++
	newInternal.MemVersion++

	return tree.promoteToParent(node.ID, newInternal.ID, pivotKey, lockedAncestors)
}

func (tree *BTree) promoteToParent(leftChildID, rightChildID PageID, pivotKey []byte, lockedAncestors []*Page) error {
	if leftChildID == tree.rootID {
		newRootRaw, err := tree.bp.NewPage()
		if err != nil {
			return err
		}
		newRootRaw.Latch.Lock()
		defer newRootRaw.Latch.Unlock()
		defer tree.bp.UnpinPage(newRootRaw.ID, true)

		newRoot := &BTreePage{newRootRaw}
		newRoot.BTreeInit()
		newRoot.SetPageType(PageTypeInternal)
		newRoot.SetNumCells(1)

		offset := BTreeHeaderSize
		binary.LittleEndian.PutUint16(newRoot.Data[offset:offset+2], uint16(len(pivotKey)))
		binary.LittleEndian.PutUint64(newRoot.Data[offset+2:offset+10], uint64(leftChildID))
		copy(newRoot.Data[offset+10:], pivotKey)

		newRoot.SetRightmostChildID(rightChildID)
		newRoot.MemVersion++
		tree.rootID = newRoot.ID
		return nil
	}

	parentRaw := lockedAncestors[len(lockedAncestors)-2]
	parentNode := &BTreePage{parentRaw}
	err := tree.insertIntoInternal(parentNode, pivotKey, leftChildID, rightChildID)

	if err != nil && errors.Is(err, ErrPageFull) {
		return tree.SplitInternalNode(parentNode, lockedAncestors[:len(lockedAncestors)-1])
	}
	return err
}

// ==========================================
// 9. B+ TREE BYTE MANIPULATION
// ==========================================

func (tree *BTree) findChildInInternalNode(node *BTreePage, searchKey []byte) PageID {
	numCells := node.NumCells()
	offset := BTreeHeaderSize
	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		childID := PageID(binary.LittleEndian.Uint64(node.Data[offset+2 : offset+10]))
		cellKey := node.Data[offset+10 : offset+10+uint32(kLen)]

		if bytes.Compare(searchKey, cellKey) < 0 {
			return childID
		}
		offset += 10 + uint32(kLen)
	}
	return node.RightmostChildID()
}

func (tree *BTree) insertIntoLeaf(node *BTreePage, newKey, newVal []byte) error {
	reqBytes := uint32(4 + len(newKey) + len(newVal))
	if !node.IsSafeForInsert(reqBytes) {
		return ErrPageFull
	}

	numCells := node.NumCells()
	offset := BTreeHeaderSize
	insertOffset := uint32(0)
	foundInsertPoint := false

	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		vLen := binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4])
		cellKey := node.Data[offset+4 : offset+4+uint32(kLen)]

		if !foundInsertPoint && bytes.Compare(newKey, cellKey) < 0 {
			insertOffset = offset
			foundInsertPoint = true
		}
		offset += 4 + uint32(kLen) + uint32(vLen)
	}

	if !foundInsertPoint {
		insertOffset = offset
	}

	if insertOffset < offset {
		bytesToShift := offset - insertOffset
		copy(node.Data[insertOffset+reqBytes : insertOffset+reqBytes+bytesToShift], node.Data[insertOffset:offset])
	}

	binary.LittleEndian.PutUint16(node.Data[insertOffset:insertOffset+2], uint16(len(newKey)))
	binary.LittleEndian.PutUint16(node.Data[insertOffset+2:insertOffset+4], uint16(len(newVal)))

	keyStart := insertOffset + 4
	valStart := keyStart + uint32(len(newKey))

	copy(node.Data[keyStart:valStart], newKey)
	copy(node.Data[valStart:valStart+uint32(len(newVal))], newVal)

	node.SetNumCells(numCells + 1)
	node.MemVersion++
	return nil
}

func (tree *BTree) insertIntoInternal(node *BTreePage, pivotKey []byte, leftChildID, rightChildID PageID) error {
	reqBytes := uint32(10 + len(pivotKey))
	if !node.IsSafeForInsert(reqBytes) {
		return ErrPageFull
	}

	numCells := node.NumCells()
	offset := BTreeHeaderSize
	insertOffset := uint32(0)
	foundInsertPoint := false

	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		cellKey := node.Data[offset+10 : offset+10+uint32(kLen)]

		if !foundInsertPoint && bytes.Compare(pivotKey, cellKey) < 0 {
			insertOffset = offset
			foundInsertPoint = true
		}
		offset += 10 + uint32(kLen)
	}

	if !foundInsertPoint {
		insertOffset = offset
		node.SetRightmostChildID(rightChildID)
	} else {
		bytesToShift := offset - insertOffset
		copy(node.Data[insertOffset+reqBytes : insertOffset+reqBytes+bytesToShift], node.Data[insertOffset:offset])
		pushedCellLeftChildOffset := insertOffset + reqBytes + 2
		binary.LittleEndian.PutUint64(node.Data[pushedCellLeftChildOffset : pushedCellLeftChildOffset+8], uint64(rightChildID))
	}

	binary.LittleEndian.PutUint16(node.Data[insertOffset : insertOffset+2], uint16(len(pivotKey)))
	binary.LittleEndian.PutUint64(node.Data[insertOffset+2 : insertOffset+10], uint64(leftChildID))
	copy(node.Data[insertOffset+10 : insertOffset+10+uint32(len(pivotKey))], pivotKey)

	node.SetNumCells(numCells + 1)
	node.MemVersion++
	return nil
}

// ==========================================
// 10. INVERTED INDEX & SEGMENT SEARCH
// ==========================================

func Tokenize(text string) []string {
	clean := func(c rune) bool {
		return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
	}
	return strings.FieldsFunc(strings.ToLower(text), clean)
}

type MemIndex struct {
	mu        sync.RWMutex
	postings  map[string][]uint64
	docsCount uint64
}

func NewMemIndex() *MemIndex {
	return &MemIndex{
		postings: make(map[string][]uint64),
	}
}

func (m *MemIndex) Add(docID uint64, text string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := Tokenize(text)
	seen := make(map[string]bool)
	for _, t := range tokens {
		if !seen[t] {
			m.postings[t] = append(m.postings[t], docID)
			seen[t] = true
		}
	}
	m.docsCount++
}

func (m *MemIndex) WriteSegment(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	terms := make([]string, 0, len(m.postings))
	for t := range m.postings {
		terms = append(terms, t)
	}
	sort.Strings(terms)

	varintBuf := make([]byte, binary.MaxVarintLen64)
	var termOffsets []uint64
	currentOffset := uint64(0)

	for _, t := range terms {
		termOffsets = append(termOffsets, currentOffset)
		postings := m.postings[t]
		sort.Slice(postings, func(i, j int) bool { return postings[i] < postings[j] })

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint32(len(postings)))

		var lastID uint64
		for _, id := range postings {
			delta := id - lastID
			n := binary.PutUvarint(varintBuf, delta)
			buf.Write(varintBuf[:n])
			lastID = id
		}
		
		written, _ := f.Write(buf.Bytes())
		currentOffset += uint64(written)
	}

	dictOffset := currentOffset
	for i, t := range terms {
		binary.Write(f, binary.LittleEndian, uint32(len(t)))
		f.WriteString(t)
		binary.Write(f, binary.LittleEndian, termOffsets[i])
	}

	return binary.Write(f, binary.LittleEndian, dictOffset)
}

type SegmentSearcher struct {
	Data []byte
	dict map[string]uint64
	once sync.Once
}

func (s *SegmentSearcher) initDict() {
	s.dict = make(map[string]uint64)
	if len(s.Data) < 8 {
		return
	}

	dictOffset := binary.LittleEndian.Uint64(s.Data[len(s.Data)-8:])
	reader := bytes.NewReader(s.Data[dictOffset : len(s.Data)-8])
	for {
		var termLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &termLen); err != nil {
			break
		}

		termBuf := make([]byte, termLen)
		reader.Read(termBuf)
		term := string(termBuf)

		var offset uint64
		binary.Read(reader, binary.LittleEndian, &offset)
		s.dict[term] = offset
	}
}

func (s *SegmentSearcher) FetchPostings(target string) []uint64 {
	s.once.Do(s.initDict)

	offset, exists := s.dict[target]
	if !exists {
		return []uint64{}
	}

	reader := bytes.NewReader(s.Data[offset:])
	var postCount uint32
	binary.Read(reader, binary.LittleEndian, &postCount)

	results := make([]uint64, postCount)
	var lastID uint64
	for i := uint32(0); i < postCount; i++ {
		delta, _ := binary.ReadUvarint(reader)
		id := lastID + delta
		results[i] = id
		lastID = id
	}
	return results
}

func (s *SegmentSearcher) Search(queryString string) ([]uint64, error) {
	ast, err := ParseQuery(queryString)
	if err != nil {
		return nil, err
	}
	return ast.Execute(s), nil
}

// ==========================================
// 11. BOOLEAN QUERY AST & SET MATH
// ==========================================

func Intersect(a, b []uint64) []uint64 {
	res := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			res = append(res, a[i])
			i++
			j++
		}
	}
	return res
}

func Union(a, b []uint64) []uint64 {
	res := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			res = append(res, a[i])
			i++
		} else if a[i] > b[j] {
			res = append(res, b[j])
			j++
		} else {
			res = append(res, a[i])
			i++
			j++
		}
	}
	res = append(res, a[i:]...)
	res = append(res, b[j:]...)
	return res
}

func Difference(a, b []uint64) []uint64 {
	res := make([]uint64, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			res = append(res, a[i])
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			i++
			j++
		}
	}
	res = append(res, a[i:]...)
	return res
}

type Query interface { Execute(s *SegmentSearcher) []uint64 }

type TermQuery struct{ Term string }
func (q *TermQuery) Execute(s *SegmentSearcher) []uint64 { return s.FetchPostings(q.Term) }

type AndQuery struct{ Left, Right Query }
func (q *AndQuery) Execute(s *SegmentSearcher) []uint64 { return Intersect(q.Left.Execute(s), q.Right.Execute(s)) }

type OrQuery struct{ Left, Right Query }
func (q *OrQuery) Execute(s *SegmentSearcher) []uint64 { return Union(q.Left.Execute(s), q.Right.Execute(s)) }

type NotQuery struct{ Left, Right Query }
func (q *NotQuery) Execute(s *SegmentSearcher) []uint64 { return Difference(q.Left.Execute(s), q.Right.Execute(s)) }

// ==========================================
// 12. QUERY PARSER
// ==========================================

const MaxQueryDepth = 50

type Parser struct {
	tokens []string
	pos    int
	depth  int
}

func ParseQuery(input string) (Query, error) {
	input = strings.ReplaceAll(input, "(", " ( ")
	input = strings.ReplaceAll(input, ")", " ) ")
	p := &Parser{tokens: strings.Fields(input)}
	return p.parseExpression()
}

func (p *Parser) current() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	return p.tokens[p.pos]
}

func (p *Parser) consume() string {
	token := p.current()
	p.pos++
	return token
}

func (p *Parser) parseExpression() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth {
		return nil, errors.New("query exceeded maximum nesting depth")
	}
	defer func() { p.depth-- }()

	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.current() == "OR" {
		p.consume()
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		left = &OrQuery{Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseTerm() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth {
		return nil, errors.New("query exceeded maximum nesting depth")
	}
	defer func() { p.depth-- }()

	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.current() == "AND" || p.current() == "NOT" {
		op := p.consume()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}

		if op == "AND" {
			left = &AndQuery{Left: left, Right: right}
		} else {
			left = &NotQuery{Left: left, Right: right}
		}
	}
	return left, nil
}

func (p *Parser) parseFactor() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth {
		return nil, errors.New("query exceeded maximum nesting depth")
	}
	defer func() { p.depth-- }()

	token := p.current()

	if token == "(" {
		p.consume()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.current() != ")" {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.consume()
		return expr, nil
	}

	if token == "" || token == ")" || token == "AND" || token == "OR" || token == "NOT" {
		return nil, fmt.Errorf("unexpected token: %s", token)
	}

	p.consume()
	cleaned := Tokenize(token)
	
	if len(cleaned) == 0 {
		return &TermQuery{Term: ""}, nil
	}
	return &TermQuery{Term: cleaned[0]}, nil
}
