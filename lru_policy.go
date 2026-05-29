package ultimate_db

import (
	"container/list"
	"sync"
)

// LRUEvictionPolicy implements the EvictionPolicy interface using a standard
// Least Recently Used (LRU) algorithm cache layout.
type LRUEvictionPolicy struct {
	mu        sync.Mutex
	lruList   *list.List
	pageMap   map[PageID]*list.Element
}

// NewLRUEvictionPolicy initializes an empty LRU policy tracking space.
func NewLRUEvictionPolicy() *LRUEvictionPolicy {
	return &LRUEvictionPolicy{
		lruList: list.New(),
		pageMap: make(map[PageID]*list.Element),
	}
}

// RecordAccess promotes a page to the front of the tracking list,
// marking it as the most recently used element.
func (p *LRUEvictionPolicy) RecordAccess(id PageID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, exists := p.pageMap[id]; exists {
		p.lruList.MoveToFront(elem)
		return
	}

	elem := p.lruList.PushFront(id)
	p.pageMap[id] = elem
}

// Evict identifies and extracts the oldest unpinned page from the back of the
// tracking history list to be recycled by the buffer pool.
func (p *LRUEvictionPolicy) Evict() (PageID, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Pick the tail element as the primary eviction candidate
	elem := p.lruList.Back()
	if elem == nil {
		return 0, false
	}

	id := elem.Value.(PageID)
	p.lruList.Remove(elem)
	delete(p.pageMap, id)
	return id, true
}

// Remove explicitly extracts a page identifier from the eviction history list.
// This is critical when files are truncated, tables dropped, or pages forced out.
func (p *LRUEvictionPolicy) Remove(id PageID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, exists := p.pageMap[id]; exists {
		p.lruList.Remove(elem)
		delete(p.pageMap, id)
	}
}
