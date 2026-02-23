// Package cache provides a thread-safe in-memory LRU object cache with an
// optional per-entry TTL and a byte-budget eviction policy.
//
// The cache is intended as a read-through layer in front of remote site reads.
// Callers populate it on Get-cache-miss and invalidate it on Put/Delete so
// subsequent reads serve locally without a network round-trip.
//
// Eviction order is least-recently-used: the entry that has not been accessed
// for the longest time is removed first when the byte budget is exceeded.
// When a TTL is configured, expired entries are treated as cache misses on
// Get; they are lazily removed from the index at that point rather than by a
// background sweep.
package cache

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

// Config holds cache configuration.
type Config struct {
	// MaxBytes is the maximum total number of bytes the cache may hold.
	// Entries are evicted LRU when inserting a value would exceed the budget.
	// A value of 0 disables the byte budget (unlimited).
	MaxBytes int64

	// TTL is the maximum age of a cached entry.  Entries older than TTL are
	// treated as misses on Get and lazily removed.
	// A value of 0 disables TTL (entries never expire).
	TTL time.Duration
}

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Hits      int64 // successful Get lookups
	Misses    int64 // Get lookups that returned no data
	Evictions int64 // entries removed to satisfy the byte budget
	Bytes     int64 // current number of bytes stored
}

// entry is one cached value stored in the list.
type entry struct {
	key       string
	data      []byte
	expiresAt time.Time // zero means no expiry
}

// Cache is a thread-safe in-memory LRU cache with byte-budget eviction.
//
// The zero value is not usable; construct with New.
type Cache struct {
	mu       sync.Mutex
	cfg      Config
	list     *list.List               // front = MRU, back = LRU
	index    map[string]*list.Element // key → list element
	hits     int64
	misses   int64
	evicted  int64
	curBytes int64
}

// New creates a Cache with the supplied Config.
func New(cfg Config) *Cache {
	return &Cache{
		cfg:   cfg,
		list:  list.New(),
		index: make(map[string]*list.Element),
	}
}

// Get looks up key in the cache.
//
// Returns (data, true) on a hit, or (nil, false) on a miss or expired entry.
// A hit moves the entry to the front of the LRU list.
func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.index[key]
	if !ok {
		c.misses++
		return nil, false
	}

	e := el.Value.(*entry)

	// Treat expired entries as misses; remove lazily.
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		c.removeElement(el)
		c.misses++
		return nil, false
	}

	c.list.MoveToFront(el)
	c.hits++
	cp := make([]byte, len(e.data))
	copy(cp, e.data)
	return cp, true
}

// Put inserts or replaces the value for key.
//
// If inserting the value would exceed MaxBytes, the least-recently-used
// entries are evicted until there is room (or MaxBytes is 0, meaning unlimited).
// Replacing an existing entry removes the old byte count before inserting.
// If the value is larger than MaxBytes it is silently dropped — an entry that
// can never fit would overflow the budget and never age out naturally.
func (c *Cache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newSize := int64(len(data))

	// Drop entries that can never fit within the byte budget.
	if c.cfg.MaxBytes > 0 && newSize > c.cfg.MaxBytes {
		return
	}

	// Replace existing entry: remove old bytes first.
	if el, ok := c.index[key]; ok {
		c.removeElement(el)
	}

	// Evict LRU entries until there is room for the new value.
	if c.cfg.MaxBytes > 0 {
		for c.curBytes+newSize > c.cfg.MaxBytes && c.list.Len() > 0 {
			c.evictLRU()
		}
	}

	e := &entry{
		key:  key,
		data: make([]byte, len(data)),
	}
	copy(e.data, data)
	if c.cfg.TTL > 0 {
		e.expiresAt = time.Now().Add(c.cfg.TTL)
	}

	el := c.list.PushFront(e)
	c.index[key] = el
	c.curBytes += newSize
}

// Delete removes the entry for key if it exists.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.index[key]; ok {
		c.removeElement(el)
	}
}

// Invalidate removes all entries whose key begins with prefix.
// Pass an empty prefix to remove all entries.
func (c *Cache) Invalidate(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if prefix == "" {
		// Fast path: clear everything.
		c.list.Init()
		c.index = make(map[string]*list.Element)
		c.curBytes = 0
		return
	}

	// Two-pass: collect first, then remove.  Modifying a map during range
	// can skip entries; collecting keys avoids that undefined behaviour.
	var toRemove []*list.Element
	for key, el := range c.index {
		if strings.HasPrefix(key, prefix) {
			toRemove = append(toRemove, el)
		}
	}
	for _, el := range toRemove {
		c.removeElement(el)
	}
}

// Stats returns a point-in-time snapshot of cache statistics.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evicted,
		Bytes:     c.curBytes,
	}
}

// Len returns the number of entries currently in the cache.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.index)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// removeElement removes el from the list and index, updating curBytes.
// Caller must hold c.mu.
func (c *Cache) removeElement(el *list.Element) {
	e := el.Value.(*entry)
	c.list.Remove(el)
	delete(c.index, e.key)
	c.curBytes -= int64(len(e.data))
}

// evictLRU removes the least-recently-used element from the cache.
// Caller must hold c.mu.
func (c *Cache) evictLRU() {
	el := c.list.Back()
	if el == nil {
		return
	}
	c.removeElement(el)
	c.evicted++
}
