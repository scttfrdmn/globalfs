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
//
// # Invalidation generation
//
// A read-through fill is not atomic with the read it caches: the caller takes a
// miss, fetches from a remote site, and inserts the bytes some time later.  A
// write or delete that lands in that window used to be lost — [Delete] removed
// an entry that was not there yet, and the in-flight fill then inserted the
// pre-write bytes, which had no expiry and so were served until LRU pressure
// happened to evict them (#89, #90).
//
// Every invalidation therefore bumps a counter.  Callers doing a read-through
// fill read it with [Cache.Generation] *before* the remote read and insert with
// [Cache.PutIfUnchanged], which drops the value if the counter moved.  The
// counter is global rather than per-key: an unrelated key's invalidation costs a
// spurious skip, and a skipped fill is only a cache miss, which is always safe.
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
	// gen counts invalidations (Delete and Invalidate).  It is the whole of the
	// generation mechanism described in the package doc: it never decreases, so
	// "unchanged" is a plain equality test and there is nothing to garbage
	// collect when a key stops being read (#89, #90).
	gen uint64
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
	c.putLocked(key, data)
}

// PutAndRecordEvictions inserts or replaces the value for key, exactly like
// [Put], and additionally returns the number of entries that were evicted to
// make room for the new value.  Callers that track eviction metrics should
// prefer this method over a separate [Stats] call to avoid a TOCTOU race when
// concurrent goroutines are also inserting values.
func (c *Cache) PutAndRecordEvictions(key string, data []byte) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putLocked(key, data)
}

// Generation returns the current invalidation counter.
//
// Pair it with [PutIfUnchanged] to make a read-through fill safe against a
// concurrent write or delete; see the package doc for why the counter is global.
func (c *Cache) Generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// PutIfUnchanged inserts or replaces the value for key only if no invalidation
// has happened since gen was read, and reports whether it stored the value along
// with the number of entries evicted to make room (as [PutAndRecordEvictions]
// does).
//
// stored=false means an invalidation raced the fill and the value the caller
// holds may predate it, so caching it could serve overwritten or deleted bytes
// indefinitely — the cache has no expiry unless a TTL is configured (#89, #90).
// Dropping it costs one cache miss on the next read.
//
// The comparison and the insert happen under the same lock acquisition, which is
// what makes this a fix rather than a narrower window: an invalidation cannot
// land between the caller's check and its write.
func (c *Cache) PutIfUnchanged(key string, data []byte, gen uint64) (evicted int64, stored bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gen != gen {
		return 0, false
	}
	return c.putLocked(key, data), true
}

// Delete removes the entry for key if it exists.
//
// The invalidation generation is bumped whether or not the key was present: the
// case that matters is precisely the one where it is absent because a
// read-through fill for it is still in flight (#89, #90).
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gen++
	if el, ok := c.index[key]; ok {
		c.removeElement(el)
	}
}

// Invalidate removes all entries whose key begins with prefix.
// Pass an empty prefix to remove all entries.
//
// Bumps the invalidation generation, so in-flight read-through fills for any key
// are dropped rather than reinstating a value this call was meant to remove.
func (c *Cache) Invalidate(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gen++

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

// putLocked inserts or replaces the value for key and returns the number of
// entries evicted to make room.  Caller must hold c.mu.
//
// The three exported insert paths share this so their eviction, byte-accounting,
// and TTL behaviour cannot drift apart; PutAndRecordEvictions was previously a
// hand-copied duplicate of Put.
func (c *Cache) putLocked(key string, data []byte) int64 {
	newSize := int64(len(data))

	// Drop entries that can never fit within the byte budget.
	if c.cfg.MaxBytes > 0 && newSize > c.cfg.MaxBytes {
		return 0
	}

	// Replace existing entry: remove old bytes first.
	if el, ok := c.index[key]; ok {
		c.removeElement(el)
	}

	// Evict LRU entries until there is room for the new value.
	var evicted int64
	if c.cfg.MaxBytes > 0 {
		for c.curBytes+newSize > c.cfg.MaxBytes && c.list.Len() > 0 {
			c.evictLRU()
			evicted++
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
	return evicted
}

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
