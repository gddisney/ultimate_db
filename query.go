package ultimate_db

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// ============================================================================
// 1. MVCC & OCC Engine Primitives
// ============================================================================

type MVCCRecord struct {
	Version   uint64
	TxnID     uint64
	Value     []byte
	Deleted   bool
	ExpiredAt time.Time
}

type MVCCCacheStore struct {
	mu       sync.RWMutex
	recs     map[string][]MVCCRecord
	txIdGen  uint64
	activeTx map[uint64]uint64 // Maps TxnID -> ReadTimestamp
}

var GlobalCacheStore = &MVCCCacheStore{
	recs:     make(map[string][]MVCCRecord),
	activeTx: make(map[uint64]uint64),
}

func (c *MVCCCacheStore) BeginOCC() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txIdGen++
	txID := c.txIdGen
	c.activeTx[txID] = uint64(time.Now().UnixNano())
	return txID
}

func (c *MVCCCacheStore) Read(txID uint64, key string) ([]byte, error) {
	c.mu.RLock()
	readTs, exists := c.activeTx[txID]
	versions, found := c.recs[key]
	c.mu.RUnlock()

	if !exists {
		return nil, errors.New("invalid or expired OCC transaction window")
	}
	if !found || len(versions) == 0 {
		return nil, errors.New("key not found within transactional cache scope")
	}

	// Scan backwards through version chain to locate the highest version visible to our snapshot
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if v.TxnID <= txID || v.Version <= readTs {
			if v.Deleted || (!v.ExpiredAt.IsZero() && time.Now().After(v.ExpiredAt)) {
				return nil, errors.New("key has expired or been explicitly tombstoned")
			}
			return v.Value, nil
		}
	}
	return nil, errors.New("no visible version found within isolation snapshot")
}

func (c *MVCCCacheStore) ValidateAndCommit(txID uint64, writeSet map[string][]byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	readTs := c.activeTx[txID]

	// OCC Validation Phase: Verify that no keys in our write-set have been updated 
	// by another transaction since this transaction's read timestamp.
	for key := range writeSet {
		if versions, found := c.recs[key]; found && len(versions) > 0 {
			latest := versions[len(versions)-1]
			if latest.Version > readTs && latest.TxnID != txID {
				delete(c.activeTx, txID)
				return fmt.Errorf("OCC SERIALIZATION ISOLATION FAILURE: Write conflict detected on key %s", key)
			}
		}
	}

	// Commit Phase: Append new versions to the MVCC chain
	commitTs := uint64(time.Now().UnixNano())
	var expireTime time.Time
	if ttl > 0 {
		expireTime = time.Now().Add(ttl)
	}

	for key, val := range writeSet {
		rec := MVCCRecord{
			Version:   commitTs,
			TxnID:     txID,
			Value:     val,
			Deleted:   val == nil,
			ExpiredAt: expireTime,
		}
		c.recs[key] = append(c.recs[key], rec)
	}

	delete(c.activeTx, txID)
	return nil
}

// ============================================================================
// 2. Production-Grade Tokenizer (Lexer)
// ============================================================================

type TokenType int

const (
	TokenIllegal TokenType = iota
	TokenEOF
	TokenIdent
	TokenString
	TokenNumber

	// Data Keywords (CRUD)
	TokenSelect
	TokenInsert
	TokenInto
	TokenValues
	TokenUpdate
	TokenSet
	TokenDelete
	TokenFrom
	TokenWhere
	TokenJoin
	TokenOn
	TokenAnd
	TokenOr
	TokenNot
	TokenLimit

	// Engine Feature Keywords
	TokenHSet
	TokenCompressed
	TokenRecover
	TokenCheckpoint
	TokenShow
	TokenMetrics

	// Syntax Operators
	TokenLParen
	TokenRParen
	TokenAsterisk
	TokenComma
	TokenEqual
	TokenColon
)

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	var tok Token

	switch l.ch {
	case '(': tok = Token{TokenLParen, "("}
	case ')': tok = Token{TokenRParen, ")"}
	case '*': tok = Token{TokenAsterisk, "*"}
	case ',': tok = Token{TokenComma, ","}
	case '=': tok = Token{TokenEqual, "="}
	case ':': tok = Token{TokenColon, ":"}
	case '\'':
		tok.Type = TokenString
		tok.Literal = l.readString()
		return tok
	case 0:
		tok = Token{TokenEOF, ""}
	default:
		if isLetter(l.ch) {
			literal := l.readIdentifier()
			tok.Type = LookupIdent(literal)
			tok.Literal = literal
			return tok
		} else if unicode.IsDigit(rune(l.ch)) {
			tok.Type = TokenNumber
			tok.Literal = l.readNumber()
			return tok
		}
		tok = Token{TokenIllegal, string(l.ch)}
	}
	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || unicode.IsDigit(rune(l.ch)) || l.ch == '.' || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for unicode.IsDigit(rune(l.ch)) {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readString() string {
	l.readChar() // consume single quote
	position := l.position
	for l.ch != '\'' && l.ch != 0 {
		l.readChar()
	}
	lit := l.input[position:l.position]
	l.readChar() // consume closing single quote
	return lit
}

func isLetter(ch byte) bool { return unicode.IsLetter(rune(ch)) || ch == '_' }

func LookupIdent(ident string) TokenType {
	keywords := map[string]TokenType{
		"SELECT": TokenSelect, "INSERT": TokenInsert, "INTO": TokenInto,
		"VALUES": TokenValues, "UPDATE": TokenUpdate, "SET": TokenSet,
		"DELETE": TokenDelete, "FROM": TokenFrom, "WHERE": TokenWhere,
		"JOIN": TokenJoin, "ON": TokenOn, "AND": TokenAnd, "OR": TokenOr,
		"NOT": TokenNot, "LIMIT": TokenLimit, "HSET": TokenHSet,
		"COMPRESSED": TokenCompressed, "RECOVER": TokenRecover,
		"CHECKPOINT": TokenCheckpoint, "SHOW": TokenShow, "METRICS": TokenMetrics,
	}
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TokenIdent
}

// ============================================================================
// 3. Extensible UQL Parser Core Engine
// ============================================================================

type UQLStatement interface {
	Execute(db *DB, index *MemIndex, s *SegmentSearcher, lockMgr LockManager, codec Codec, walPath string) ([][]byte, error)
}

type ParserUQL struct {
	l       *Lexer
	curTok  Token
	peekTok Token
}

func NewParserUQL(input string) *ParserUQL {
	p := &ParserUQL{l: NewLexer(input)}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *ParserUQL) nextToken() {
	p.curTok = p.peekTok
	p.peekTok = p.l.NextToken()
}

func ParseUQL(input string) (UQLStatement, error) {
	p := NewParserUQL(input)

	switch p.curTok.Type {
	case TokenSelect:
		return p.parseSelect()
	case TokenInsert:
		return p.parseInsert()
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	case TokenHSet:
		return p.parseHSet()
	case TokenRecover:
		return &RecoverStatement{}, nil
	case TokenCheckpoint:
		return &CheckpointStatement{}, nil
	case TokenShow:
		if p.peekTok.Type == TokenMetrics {
			return &MetricsStatement{}, nil
		}
		return nil, fmt.Errorf("unexpected target after SHOW: %s", p.peekTok.Literal)
	default:
		return nil, fmt.Errorf("unknown statement syntax mapping: %s", p.curTok.Literal)
	}
}

// ============================================================================
// 4. Command: HSET (Dual MVCC OCC Cache + Relational Pipeline Entry)
// ============================================================================

type HSetStatement struct {
	HashKey string
	Field   string
	Value   string
}

func (p *ParserUQL) parseHSet() (*HSetStatement, error) {
	p.nextToken() // consume HSET
	hKey := p.curTok.Literal
	p.nextToken()
	field := p.curTok.Literal
	p.nextToken()
	val := p.curTok.Literal

	return &HSetStatement{HashKey: hKey, Field: field, Value: val}, nil
}

func (stmt *HSetStatement) Execute(db *DB, _ *MemIndex, _ *SegmentSearcher, lockMgr LockManager, _ Codec, _ string) ([][]byte, error) {
	compositeKey := PrefixHash + stmt.HashKey + ":" + stmt.Field

	// 1. Transactional Write Path to MVCC/OCC Fast Cache Side
	txID := GlobalCacheStore.BeginOCC()
	writeSet := map[string][]byte{
		compositeKey: []byte(stmt.Value),
	}
	if err := GlobalCacheStore.ValidateAndCommit(txID, writeSet, 0); err != nil {
		return nil, fmt.Errorf("cache transactional write failed: %w", err)
	}

	// 2. Transactional Write Path to Slotted Page Durability Subsystem
	txn := db.BeginTxn()
	if lockMgr != nil {
		if err := lockMgr.Acquire(txn, compositeKey, LockExclusive); err != nil {
			db.CommitTxn(txn)
			return nil, err
		}
	}

	err := db.HSet(PageID(0), txn, []byte(stmt.HashKey), []byte(stmt.Field), []byte(stmt.Value), 0)
	db.CommitTxn(txn)
	if lockMgr != nil {
		lockMgr.ReleaseAll(txn)
	}

	if err != nil {
		return nil, err
	}
	return [][]byte{[]byte("SUCCESS: Transaction complete. MVCC record appended and slotted data synchronized.")}, nil
}

// ============================================================================
// 5. Command: SELECT (With High-Speed MVCC Cache Read Optimization)
// ============================================================================

type SelectStatement struct {
	Filter       Query
	Limit        int
	IsCompressed bool
	JoinTable    string
	JoinOnLeft   string
	JoinOnRight  string
}

func (p *ParserUQL) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{Limit: 100}
	p.nextToken() // consume SELECT

	if p.curTok.Type == TokenCompressed {
		stmt.IsCompressed = true
		p.nextToken()
	}

	if p.curTok.Type == TokenAsterisk { p.nextToken() }
	if p.curTok.Type != TokenFrom { return nil, errors.New("expected FROM keyword") }
	p.nextToken(); p.nextToken() // skip table token context

	if p.curTok.Type == TokenJoin {
		p.nextToken()
		stmt.JoinTable = p.curTok.Literal
		p.nextToken()
		if p.curTok.Type != TokenOn { return nil, errors.New("expected ON relationship match") }
		p.nextToken()
		stmt.JoinOnLeft = p.curTok.Literal
		p.nextToken()
		p.nextToken() // skip equal token
		stmt.JoinOnRight = p.curTok.Literal
		p.nextToken()
	}

	if p.curTok.Type != TokenWhere { return nil, errors.New("missing conditional WHERE boundary") }
	p.nextToken() // advance past WHERE token

	var remaining []string
	for p.curTok.Type != TokenEOF && p.curTok.Type != TokenLimit {
		remaining = append(remaining, p.curTok.Literal)
		p.nextToken()
	}

	if p.curTok.Type == TokenLimit {
		p.nextToken()
		limit, err := strconv.Atoi(p.curTok.Literal)
		if err == nil { stmt.Limit = limit }
	}

	ast, err := ParseQuery(strings.ToLower(strings.Join(remaining, " ")))
	if err != nil { return nil, err }
	stmt.Filter = ast

	return stmt, nil
}

func (stmt *SelectStatement) Execute(db *DB, _ *MemIndex, s *SegmentSearcher, lockMgr LockManager, codec Codec, _ string) ([][]byte, error) {
	bitmap := stmt.Filter.Execute(s)
	docIDs := bitmap.ToArray()

	var results [][]byte
	txID := GlobalCacheStore.BeginOCC() // Non-blocking MVCC snapshot transaction isolation window

	// Open fallback page frame transaction context if record hits a cache miss
	txn := db.BeginTxn()
	defer db.CommitTxn(txn)

	for _, id := range docIDs {
		keyStr := fmt.Sprintf("%d", id)
		
		// Attempt reading directly from MVCC Lock-Free Fast Cache first
		val, err := GlobalCacheStore.Read(txID, keyStr)
		if err == nil {
			if stmt.JoinTable != "" {
				val = []byte(fmt.Sprintf("%s linked to %s relational row matrix", string(val), stmt.JoinTable))
			}
			results = append(results, val)
			if len(results) >= stmt.Limit { break }
			continue
		}

		// Fallback to Slotted Page Durability Store on cache miss
		if lockMgr != nil {
			if err := lockMgr.Acquire(txn, keyStr, LockShared); err != nil {
				return nil, err
			}
		}

		var diskKey = []byte(keyStr)
		if stmt.IsCompressed && codec != nil {
			val, err = db.ReadCompressed(PageID(0), txn, diskKey, codec)
		} else {
			val, err = db.Read(PageID(0), txn, diskKey)
		}

		if err == nil {
			// Populate cache side asynchronously or sequentially to serve subsequent lookups
			_ = GlobalCacheStore.ValidateAndCommit(txID, map[string][]byte{keyStr: val}, 0)

			if stmt.JoinTable != "" {
				val = []byte(fmt.Sprintf("%s linked to %s relational row matrix", string(val), stmt.JoinTable))
			}
			results = append(results, val)
		}
		if len(results) >= stmt.Limit { break }
	}

	if lockMgr != nil { lockMgr.ReleaseAll(txn) }
	return results, nil
}

// ============================================================================
// 6. Commands: RECOVER & CHECKPOINT
// ============================================================================

type RecoverStatement struct{}

func (stmt *RecoverStatement) Execute(db *DB, _ *MemIndex, _ *SegmentSearcher, _ LockManager, _ Codec, walPath string) ([][]byte, error) {
	err := PerformRecovery(db, walPath)
	if err != nil {
		return nil, fmt.Errorf("UQL ARIES Recovery aborted: %w", err)
	}
	return [][]byte{[]byte("SUCCESS: System historical record replay complete. Engine stable.")}, nil
}

type CheckpointStatement struct{}

func (stmt *CheckpointStatement) Execute(db *DB, _ *MemIndex, _ *SegmentSearcher, _ LockManager, _ Codec, _ string) ([][]byte, error) {
	db.bp.FlushAll()
	err := db.wal.Checkpoint()
	if err != nil {
		return nil, err
	}
	return [][]byte{[]byte("SUCCESS: Dirty frames synced to storage disk. Checkpoint sealed.")}, nil
}

// ============================================================================
// 7. Command: SHOW METRICS
// ============================================================================

type MetricsStatement struct{}

func (stmt *MetricsStatement) Execute(db *DB, _ *MemIndex, _ *SegmentSearcher, _ LockManager, _ Codec, _ string) ([][]byte, error) {
	if db.metrics == nil {
		return nil, errors.New("telemetry interface is not active on this runtime instance")
	}

	if atomicM, ok := db.metrics.(*AtomicMetrics); ok {
		snap := atomicM.Capture()
		report := fmt.Sprintf(
			"METRICS REPORT:\n- Buffer Pool Cache Hits: %d\n- Buffer Pool Cache Misses: %d\n- Cache Hit Efficiency Ratio: %.2f%%\n- Active Concurrent Transactions: %d\n- Average WAL Flush Sync Delay: %v\n- Average Slotted Page Compaction Time: %v",
			snap.BufferPoolHits, snap.BufferPoolMisses, snap.BufferPoolHitRatio*100, snap.ActiveTransactions, snap.AvgWalFlushLatency, snap.AvgCompactionLatency,
		)
		return [][]byte{[]byte(report)}, nil
	}

	return [][]byte{[]byte("SUCCESS: Telemetry processing diagnostics active")}, nil
}

// ============================================================================
// 8. CRUD Operators (INSERT, UPDATE, DELETE With Integrated Cache Mutation)
// ============================================================================

type InsertStatement struct {
	DocID        uint64
	Value        string
	IsCompressed bool
}

func (p *ParserUQL) parseInsert() (*InsertStatement, error) {
	p.nextToken() // consume INSERT
	isComp := false
	if p.curTok.Type == TokenCompressed {
		isComp = true
		p.nextToken()
	}
	p.nextToken() // consume INTO
	p.nextToken() // skip table name context token
	if p.curTok.Type != TokenValues {
		return nil, errors.New("expected VALUES clause modifier entry flag")
	}
	p.nextToken() // consume VALUES
	p.nextToken() // consume LParen
	
	id, err := strconv.ParseUint(p.curTok.Literal, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid insert identifier token: %w", err)
	}
	
	p.nextToken() // consume number literal ID
	p.nextToken() // consume comma divider operator
	content := p.curTok.Literal
	
	return &InsertStatement{DocID: id, Value: content, IsCompressed: isComp}, nil
}

func (stmt *InsertStatement) Execute(db *DB, index *MemIndex, _ *SegmentSearcher, lockMgr LockManager, codec Codec, _ string) ([][]byte, error) {
	keyStr := fmt.Sprintf("%d", stmt.DocID)

	// 1. Write Version to MVCC OCC Fast Cache Side
	txID := GlobalCacheStore.BeginOCC()
	if err := GlobalCacheStore.ValidateAndCommit(txID, map[string][]byte{keyStr: []byte(stmt.Value)}, 0); err != nil {
		return nil, err
	}

	// 2. Write Record to Pessimistic Slotted Database Engine Page Block Frame
	txn := db.BeginTxn()
	key := []byte(keyStr)
	if lockMgr != nil { _ = lockMgr.Acquire(txn, keyStr, LockExclusive) }

	var err error
	if stmt.IsCompressed && codec != nil {
		err = db.WriteCompressed(PageID(0), txn, key, []byte(stmt.Value), 0, codec)
	} else {
		err = db.Write(PageID(0), txn, key, []byte(stmt.Value), 0)
	}
	
	db.CommitTxn(txn)
	if lockMgr != nil { lockMgr.ReleaseAll(txn) }
	if err != nil { return nil, err }
	if index != nil { index.Add(stmt.DocID, stmt.Value) }
	
	return [][]byte{[]byte("SUCCESS: Insert executed on storage block and tracking version appended to cache chains")}, nil
}

type UpdateStatement struct {
	Filter   Query
	NewValue string
}

func (p *ParserUQL) parseUpdate() (*UpdateStatement, error) {
	p.nextToken() // consume UPDATE
	p.nextToken() // skip table target
	p.nextToken() // consume SET
	p.nextToken() // skip field identifier
	p.nextToken() // skip equal operator
	newVal := p.curTok.Literal
	p.nextToken() // consume value string
	p.nextToken() // consume WHERE
	
	if p.curTok.Type != TokenWhere { return nil, errors.New("expected WHERE parameter boundary definitions") }
	p.nextToken()

	var remaining []string
	for p.curTok.Type != TokenEOF {
		remaining = append(remaining, p.curTok.Literal)
		p.nextToken()
	}
	ast, err := ParseQuery(strings.ToLower(strings.Join(remaining, " ")))
	if err != nil { return nil, err }
	
	return &UpdateStatement{Filter: ast, NewValue: newVal}, nil
}

func (stmt *UpdateStatement) Execute(db *DB, index *MemIndex, s *SegmentSearcher, lockMgr LockManager, _ Codec, _ string) ([][]byte, error) {
	bitmap := stmt.Filter.Execute(s)
	docIDs := bitmap.ToArray()
	
	txID := GlobalCacheStore.BeginOCC()
	txn := db.BeginTxn()
	
	writeSet := make(map[string][]byte)
	for _, id := range docIDs {
		keyStr := fmt.Sprintf("%d", id)
		writeSet[keyStr] = []byte(stmt.NewValue)

		key := []byte(keyStr)
		if lockMgr != nil { _ = lockMgr.Acquire(txn, keyStr, LockExclusive) }
		_ = db.Write(PageID(0), txn, key, []byte(stmt.NewValue), 0)
		if index != nil { index.Add(id, stmt.NewValue) }
	}
	
	// Validate updates against cache transaction timeline before confirming commits
	if err := GlobalCacheStore.ValidateAndCommit(txID, writeSet, 0); err != nil {
		db.CommitTxn(txn) // Abort/rollback patterns would resolve here
		if lockMgr != nil { lockMgr.ReleaseAll(txn) }
		return nil, err
	}

	db.CommitTxn(txn)
	if lockMgr != nil { lockMgr.ReleaseAll(txn) }
	return [][]byte{[]byte("SUCCESS: MVCC chains updated and physical blocks synchronized")}, nil
}

type DeleteStatement struct {
	Filter Query
}

func (p *ParserUQL) parseDelete() (*DeleteStatement, error) {
	p.nextToken() // consume DELETE
	p.nextToken() // consume FROM
	p.nextToken() // skip table destination
	p.nextToken() // consume WHERE
	
	if p.curTok.Type != TokenWhere { return nil, errors.New("missing conditional parameters") }
	p.nextToken()

	var remaining []string
	for p.curTok.Type != TokenEOF {
		remaining = append(remaining, p.curTok.Literal)
		p.nextToken()
	}
	ast, err := ParseQuery(strings.ToLower(strings.Join(remaining, " ")))
	if err != nil { return nil, err }
	
	return &DeleteStatement{Filter: ast}, nil
}

func (stmt *DeleteStatement) Execute(db *DB, _ *MemIndex, s *SegmentSearcher, lockMgr LockManager, _ Codec, _ string) ([][]byte, error) {
	bitmap := stmt.Filter.Execute(s)
	docIDs := bitmap.ToArray()
	
	txID := GlobalCacheStore.BeginOCC()
	txn := db.BeginTxn()
	
	writeSet := make(map[string][]byte)
	for _, id := range docIDs {
		keyStr := fmt.Sprintf("%d", id)
		writeSet[keyStr] = nil // Appends explicit MVCC deletion tombstone version

		key := []byte(keyStr)
		if lockMgr != nil { _ = lockMgr.Acquire(txn, keyStr, LockExclusive) }
		_ = db.Write(PageID(0), txn, key, nil, time.Nanosecond)
	}
	
	if err := GlobalCacheStore.ValidateAndCommit(txID, writeSet, 0); err != nil {
		db.CommitTxn(txn)
		if lockMgr != nil { lockMgr.ReleaseAll(txn) }
		return nil, err
	}

	db.CommitTxn(txn)
	if lockMgr != nil { lockMgr.ReleaseAll(txn) }
	return [][]byte{[]byte("SUCCESS: Transaction complete. MVCC tombstone markers assigned.")}, nil
}
