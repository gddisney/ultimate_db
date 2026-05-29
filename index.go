package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"strings"
	"sync"
)

// ============================================================================
// 1. Full-Text Tokenizer Strategy
// ============================================================================

// Tokenize acts as the core parsing utility for data indexing and factor isolation.
func Tokenize(text string) []string {
	clean := func(c rune) bool {
		return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
	}
	return strings.FieldsFunc(strings.ToLower(text), clean)
}

// ============================================================================
// 2. Thread-Safe Concurrent Memory Index
// ============================================================================

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

// Add splits inbound text fields and appends unique Document IDs to the postings list.
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

// WriteSegment serializes the postings map into a performance-sorted binary disk segment.
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

	// 1. Write the delta-encoded posting lists
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

	// 2. Write the term dictionary footer block
	dictOffset := currentOffset
	for i, t := range terms {
		binary.Write(f, binary.LittleEndian, uint32(len(t)))
		f.WriteString(t)
		binary.Write(f, binary.LittleEndian, termOffsets[i])
	}

	// 3. Seal the segment with an 8-byte dictionary pointer address
	return binary.Write(f, binary.LittleEndian, dictOffset)
}

// ============================================================================
// 3. Segment Searcher (Dictionary Parse & Bitmaps Execution)
// ============================================================================

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

	// Read the final 8 bytes to extract the vocabulary dictionary's beginning offset boundary
	dictOffset := binary.LittleEndian.Uint64(s.Data[len(s.Data)-8:])
	if dictOffset >= uint64(len(s.Data)-8) {
		return
	}
	
	reader := bytes.NewReader(s.Data[dictOffset : len(s.Data)-8])
	for {
		var termLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &termLen); err != nil {
			break
		}

		termBuf := make([]byte, termLen)
		if n, _ := reader.Read(termBuf); uint32(n) != termLen {
			break
		}
		term := string(termBuf)

		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			break
		}
		s.dict[term] = offset
	}
}

// FetchPostings extracts a raw list of sorted Document IDs matching a targeted vocabulary term string.
func (s *SegmentSearcher) FetchPostings(target string) []uint64 {
	s.once.Do(s.initDict)

	offset, exists := s.dict[target]
	if !exists {
		return []uint64{}
	}

	reader := bytes.NewReader(s.Data[offset:])
	var postCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &postCount); err != nil {
		return []uint64{}
	}

	results := make([]uint64, postCount)
	var lastID uint64
	for i := uint32(0); i < postCount; i++ {
		delta, err := binary.ReadUvarint(reader)
		if err != nil {
			return results[:i]
		}
		id := lastID + delta
		results[i] = id
		lastID = id
	}
	return results
}

// Search coordinates query resolution by running the AST execution logic directly.
func (s *SegmentSearcher) Search(queryString string) (*RoaringBitmap, error) {
	ast, err := ParseQuery(queryString)
	if err != nil {
		return nil, err
	}
	// Execution yields highly optimized compressed Roaring Bitmap chunks instead of raw arrays
	return ast.Execute(s), nil
}
