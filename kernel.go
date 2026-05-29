package ultimate_db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Internal Registry Reservations
const (
	IndexPageID         PageID = 10
	MetadataPageID      PageID = 11
	PostingsChunkSize          = 256
	DefaultVirtualNodes        = 64
)

// -----------------------------------------------------------------------------
// Core Interfaces (Breaking the Circular Dependency)
// -----------------------------------------------------------------------------

// NetworkTransport abstracts the peer-to-peer overlay plane
type NetworkTransport interface {
	BroadcastQuery(ctx context.Context, queryPayload []byte) ([][]byte, error)
	GetLocalNodeID() string
}

// AuditInterceptor abstracts inline permission and logging checks
type AuditInterceptor interface {
	VerifyAccess(subject []byte, action, resource string) bool
	LogAudit(actor, action, msg string)
}

// -----------------------------------------------------------------------------
// Inverted Index Models
// -----------------------------------------------------------------------------

type Posting struct {
	DocID string  `json:"doc_id"`
	TF    float64 `json:"tf"`
}

type PostingsChunk struct {
	ChunkID  uint32    `json:"chunk_id"`
	Postings []Posting `json:"postings"`
}

type EngineState struct {
	TotalDocs int     `json:"total_docs"`
	AvgDocLen float64 `json:"avg_doc_len"`
}

type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

type ClusterQuery struct {
	QueryID   string `json:"query_id"`
	QueryText string `json:"query_text"`
	Limit     int    `json:"limit"`
}

type RoutingEntry struct {
	ID      string
	Address string
	Healthy bool
}

// -----------------------------------------------------------------------------
// Integrated Engine Handler
// -----------------------------------------------------------------------------

type IntegratedEngine struct {
	DB          *DB
	Transport   NetworkTransport // Injected P2P capabilities
	Interceptor AuditInterceptor // Injected PBAC/ABAC guardrails
	Analyzer    *InternalAnalyzer

	// Sharding State
	vNodes       int
	ring         []uint32
	nodeMap      map[uint32]string
	routingTable map[string]*RoutingEntry

	// Metrics
	TotalDocs int
	AvgDocLen float64
	k1        float64
	b         float64
	mu        sync.RWMutex
}

func NewIntegratedEngine(db *DB, transport NetworkTransport, interceptor AuditInterceptor) (*IntegratedEngine, error) {
	ie := &IntegratedEngine{
		DB:           db,
		Transport:    transport,
		Interceptor:  interceptor,
		Analyzer:     NewInternalAnalyzer(),
		vNodes:       DefaultVirtualNodes,
		nodeMap:      make(map[uint32]string),
		routingTable: make(map[string]*RoutingEntry),
		k1:           1.2,
		b:            0.75,
	}

	// Recover engine state from page frames
	txn := db.BeginTxn()
	stateBytes, err := db.Read(MetadataPageID, txn, []byte("bm25_state"))
	db.CommitTxn(txn)

	if err == nil && len(stateBytes) > 0 {
		var state EngineState
		if err := json.Unmarshal(stateBytes, &state); err == nil {
			ie.TotalDocs = state.TotalDocs
			ie.AvgDocLen = state.AvgDocLen
		}
	}

	return ie, nil
}

// AddClusterNode maps network coordinates onto the internal hash ring
func (ie *IntegratedEngine) AddClusterNode(nodeID, address string) {
	ie.mu.Lock()
	defer ie.mu.Unlock()

	ie.routingTable[nodeID] = &RoutingEntry{
		ID:      nodeID,
		Address: address,
		Healthy: true,
	}

	hasher := sha256.New()
	for i := 0; i < ie.vNodes; i++ {
		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%s#%d", nodeID, i)))
		hk := binary.BigEndian.Uint32(hasher.Sum(nil)[0:4])
		ie.ring = append(ie.ring, hk)
		ie.nodeMap[hk] = nodeID
	}
	sort.Slice(ie.ring, func(i, j int) bool { return ie.ring[i] < ie.ring[j] })
}

func (ie *IntegratedEngine) LocalSearch(queryText string, limit int) ([]SearchResult, error) {
	ie.mu.RLock()
	totalDocs := ie.TotalDocs
	avgDocLen := ie.AvgDocLen
	ie.mu.RUnlock()

	ast, err := ParseQuery(queryText)
	if err != nil {
		return nil, err
	}

	txn := ie.DB.BeginTxn()
	defer ie.DB.CommitTxn(txn)

	validDocs := ie.evaluateAST(ast, txn)
	if len(validDocs) == 0 {
		return nil, nil
	}

	tokens := ie.Analyzer.Tokenize(queryText)
	docScores := make(map[string]float64)

	for _, token := range tokens {
		startRange := []byte(fmt.Sprintf("term:%s:chunk:0", token))
		endRange := []byte(fmt.Sprintf("term:%s:chunk:\xff", token))

		cursor := ie.DB.NewRangeCursor(IndexPageID, txn, startRange, endRange)
		var postingsCount int
		var matches []Posting

		for {
			_, valBytes, err := cursor.Next()
			if err != nil || valBytes == nil {
				break
			}
			var chunk PostingsChunk
			if json.Unmarshal(valBytes, &chunk) == nil {
				postingsCount += len(chunk.Postings)
				matches = append(matches, chunk.Postings...)
			}
		}

		if postingsCount == 0 {
			continue
		}

		numerator := float64(totalDocs-postingsCount) + 0.5
		denominator := float64(postingsCount) + 0.5
		idf := math.Log(1.0 + (numerator / denominator))

		for _, p := range matches {
			if !validDocs[p.DocID] {
				continue
			}
			lNorm := 1.0 - ie.b + ie.b*(avgDocLen/avgDocLen)
			tfNorm := (p.TF * (ie.k1 + 1.0)) / (p.TF + ie.k1*lNorm)
			docScores[p.DocID] += idf * tfNorm
		}
	}

	var results []SearchResult
	for id, score := range docScores {
		results = append(results, SearchResult{DocID: id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

func (ie *IntegratedEngine) evaluateAST(q Query, txn uint64) map[string]bool {
	res := make(map[string]bool)
	switch v := q.(type) {
	case *TermQuery:
		startRange := []byte(fmt.Sprintf("term:%s:chunk:0", v.Term))
		endRange := []byte(fmt.Sprintf("term:%s:chunk:\xff", v.Term))
		cursor := ie.DB.NewRangeCursor(IndexPageID, txn, startRange, endRange)
		for {
			_, valBytes, err := cursor.Next()
			if err != nil || valBytes == nil {
				break
			}
			var chunk PostingsChunk
			if json.Unmarshal(valBytes, &chunk) == nil {
				for _, p := range chunk.Postings {
					res[p.DocID] = true
				}
			}
		}
	case *AndQuery:
		left := ie.evaluateAST(v.Left, txn)
		right := ie.evaluateAST(v.Right, txn)
		for k := range left {
			if right[k] { res[k] = true }
		}
	case *OrQuery:
		left := ie.evaluateAST(v.Left, txn)
		right := ie.evaluateAST(v.Right, txn)
		for k := range left { res[k] = true }
		for k := range right { res[k] = true }
	}
	return res
}

func (ie *IntegratedEngine) ScatterGather(ctx context.Context, subjectID []byte, queryText string, limit int) ([]SearchResult, error) {
	// 1. Core Interception: Use the abstract interface to perform authentication checks inline
	if ie.Interceptor != nil {
		if !ie.Interceptor.VerifyAccess(subjectID, "SEARCH", "audit_logs") {
			if ie.Interceptor != nil {
				ie.Interceptor.LogAudit(string(subjectID), "SEARCH_DENIED", "ABAC validation failure on distributed search request")
			}
			return nil, fmt.Errorf("security policy validation failure: search access denied")
		}
	}

	localHits, err := ie.LocalSearch(queryText, limit)
	if err != nil {
		return nil, err
	}

	// 2. Network Fan-Out via Injected Transport Layer
	if ie.Transport == nil {
		return localHits, nil
	}

	payload, _ := json.Marshal(ClusterQuery{
		QueryID:   fmt.Sprintf("q_%d", time.Now().UnixNano()),
		QueryText: queryText,
		Limit:     limit,
	})

	remoteResponses, err := ie.Transport.BroadcastQuery(ctx, payload)
	var globalHits []SearchResult
	globalHits = append(globalHits, localHits...)

	if err == nil {
		for _, respBytes := range remoteResponses {
			var peerHits []SearchResult
			if json.Unmarshal(respBytes, &peerHits) == nil {
				globalHits = append(globalHits, peerHits...)
			}
		}
	}

	sort.Slice(globalHits, func(i, j int) bool { return globalHits[i].Score > globalHits[j].Score })
	if limit > 0 && len(globalHits) > limit {
		globalHits = globalHits[:limit]
	}

	return globalHits, nil
}
