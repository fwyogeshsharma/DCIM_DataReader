package ingest

import (
	"container/list"
	"sync"
	"time"
)

// LRU is a fixed-capacity thread-safe LRU cache for string→string lookups with
// per-entry TTL. Used to avoid hammering Postgres with DeviceIDBySource /
// InterfaceID queries on every metric packet.
//
// TTL exists because the resolved UUID is only stable for the lifetime of the
// underlying row. If an operator wipes the devices table externally
// (TRUNCATE / DROP / restore from backup), cached UUIDs become dangling — the
// downstream FK INSERT on `interfaces.device_id` then fails. With TTL the
// stale entry expires within ttl, the next packet refetches from DB, and
// writes recover automatically.
type LRU struct {
	mu  sync.Mutex
	cap int
	ttl time.Duration
	ll  *list.List
	idx map[string]*list.Element
}

type lruEntry struct {
	key       string
	val       string
	expiresAt time.Time // zero → no expiry
}

// NewLRU creates a cache with the given capacity and no TTL.
func NewLRU(capacity int) *LRU {
	return NewLRUWithTTL(capacity, 0)
}

// NewLRUWithTTL creates a cache with the given capacity and per-entry TTL.
// ttl <= 0 disables expiry (entries live until evicted by capacity pressure).
func NewLRUWithTTL(capacity int, ttl time.Duration) *LRU {
	if capacity <= 0 {
		capacity = 1024
	}
	return &LRU{
		cap: capacity,
		ttl: ttl,
		ll:  list.New(),
		idx: make(map[string]*list.Element, capacity),
	}
}

func (c *LRU) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.idx[key]
	if !ok {
		return "", false
	}
	entry := e.Value.(*lruEntry)
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		// Expired — drop and report a miss so the caller refetches from DB.
		c.ll.Remove(e)
		delete(c.idx, key)
		return "", false
	}
	c.ll.MoveToFront(e)
	return entry.val, true
}

func (c *LRU) Put(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var exp time.Time
	if c.ttl > 0 {
		exp = time.Now().Add(c.ttl)
	}
	if e, ok := c.idx[key]; ok {
		entry := e.Value.(*lruEntry)
		entry.val = val
		entry.expiresAt = exp
		c.ll.MoveToFront(e)
		return
	}
	e := c.ll.PushFront(&lruEntry{key: key, val: val, expiresAt: exp})
	c.idx[key] = e
	if c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.idx, oldest.Value.(*lruEntry).key)
		}
	}
}

func (c *LRU) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.idx[key]; ok {
		c.ll.Remove(e)
		delete(c.idx, key)
	}
}

// Flush drops all entries. Called by the admin endpoint after an external
// truncate/restore so writes recover without waiting for TTL expiry.
func (c *LRU) Flush() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.ll.Len()
	c.ll.Init()
	c.idx = make(map[string]*list.Element, c.cap)
	return n
}

// Len returns the current entry count (for diagnostics).
func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
