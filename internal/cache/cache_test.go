package cache

import (
	"fmt"
	"testing"
	"time"
)

// ── Get / Put basics ──────────────────────────────────────────────────────────

func TestCache_GetMissOnEmpty(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	_, ok := c.Get("missing")
	if ok {
		t.Error("expected miss on empty cache")
	}
}

func TestCache_PutThenGet(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("k", []byte("hello"))
	data, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", data, "hello")
	}
}

func TestCache_GetReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("k", []byte("original"))
	data, _ := c.Get("k")
	data[0] = 'X'
	// Second get should return the original, not the mutated copy.
	data2, _ := c.Get("k")
	if string(data2) != "original" {
		t.Errorf("cache returned mutated data: %q", data2)
	}
}

func TestCache_PutReplace(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 100})
	c.Put("k", []byte("first"))
	c.Put("k", []byte("second"))
	data, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit after replace")
	}
	if string(data) != "second" {
		t.Errorf("got %q, want %q", data, "second")
	}
	// Byte count should reflect only the replacement, not the original.
	stats := c.Stats()
	if stats.Bytes != int64(len("second")) {
		t.Errorf("bytes: got %d, want %d", stats.Bytes, len("second"))
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func TestCache_Delete(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("k", []byte("v"))
	c.Delete("k")
	_, ok := c.Get("k")
	if ok {
		t.Error("expected miss after delete")
	}
}

func TestCache_DeleteMissingIsNoop(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Delete("no-such-key") // must not panic
}

// ── Invalidate ────────────────────────────────────────────────────────────────

func TestCache_InvalidatePrefix(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("a/1", []byte("1"))
	c.Put("a/2", []byte("2"))
	c.Put("b/1", []byte("3"))
	c.Invalidate("a/")
	if _, ok := c.Get("a/1"); ok {
		t.Error("a/1 should be invalidated")
	}
	if _, ok := c.Get("a/2"); ok {
		t.Error("a/2 should be invalidated")
	}
	if _, ok := c.Get("b/1"); !ok {
		t.Error("b/1 should still be cached")
	}
}

func TestCache_InvalidateAll(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("x", []byte("x"))
	c.Put("y", []byte("y"))
	c.Invalidate("")
	if c.Len() != 0 {
		t.Errorf("expected empty cache after Invalidate(\"\"), got %d entries", c.Len())
	}
	stats := c.Stats()
	if stats.Bytes != 0 {
		t.Errorf("bytes should be 0 after full invalidation, got %d", stats.Bytes)
	}
}

// TestCache_Invalidate_AllMatchingKeysRemoved verifies that all entries whose
// key starts with the prefix are removed, not just some of them.  This is the
// regression test for the map-modification-during-range bug (#45).
func TestCache_Invalidate_AllMatchingKeysRemoved(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	const n = 20
	for i := 0; i < n; i++ {
		c.Put(fmt.Sprintf("pfx/%d", i), []byte("v"))
	}
	c.Put("other", []byte("keep"))
	c.Invalidate("pfx/")

	if c.Len() != 1 {
		t.Errorf("expected 1 entry after Invalidate(\"pfx/\"), got %d", c.Len())
	}
	if _, ok := c.Get("other"); !ok {
		t.Error("non-matching key should survive Invalidate")
	}
	for i := 0; i < n; i++ {
		if _, ok := c.Get(fmt.Sprintf("pfx/%d", i)); ok {
			t.Errorf("pfx/%d should have been invalidated", i)
		}
	}
}

// ── LRU eviction ─────────────────────────────────────────────────────────────

func TestCache_EvictsLRUWhenBudgetExceeded(t *testing.T) {
	t.Parallel()
	// Budget = 10 bytes; each value is 5 bytes.
	c := New(Config{MaxBytes: 10})
	c.Put("a", []byte("AAAAA")) // 5 bytes; total=5
	c.Put("b", []byte("BBBBB")) // 5 bytes; total=10
	c.Put("c", []byte("CCCCC")) // 5 bytes; must evict "a" (LRU)

	if _, ok := c.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("b should still be cached")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be cached")
	}
	stats := c.Stats()
	if stats.Evictions != 1 {
		t.Errorf("evictions: got %d, want 1", stats.Evictions)
	}
}

func TestCache_AccessPromotesToFront(t *testing.T) {
	t.Parallel()
	// Budget = 10 bytes.  Access "a" to promote it, so "b" becomes LRU.
	c := New(Config{MaxBytes: 10})
	c.Put("a", []byte("AAAAA")) // inserted first → LRU candidate
	c.Put("b", []byte("BBBBB"))
	c.Get("a") // promote "a" → now "b" is LRU
	c.Put("c", []byte("CCCCC")) // must evict "b"

	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted after a was accessed")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should still be cached")
	}
}

func TestCache_ZeroMaxBytesIsUnlimited(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 0})
	for i := 0; i < 1000; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 1024))
	}
	if c.Len() != 1000 {
		t.Errorf("expected 1000 entries, got %d", c.Len())
	}
}

func TestCache_EntryLargerThanBudget(t *testing.T) {
	t.Parallel()
	// Entry is 20 bytes but budget is 10.  The entry can never fit within the
	// budget so Put must drop it silently rather than inserting an entry that
	// permanently exceeds the limit and can never age out.
	c := New(Config{MaxBytes: 10})
	c.Put("big", make([]byte, 20))
	if _, ok := c.Get("big"); ok {
		t.Error("oversized entry should be dropped, not inserted")
	}
	if c.Len() != 0 {
		t.Errorf("expected empty cache after dropping oversized entry, got %d entries", c.Len())
	}
	if c.Stats().Bytes != 0 {
		t.Errorf("expected 0 bytes after dropping oversized entry, got %d", c.Stats().Bytes)
	}
}

// ── TTL expiry ────────────────────────────────────────────────────────────────

func TestCache_TTLExpiry(t *testing.T) {
	t.Parallel()
	c := New(Config{TTL: 50 * time.Millisecond})
	c.Put("k", []byte("v"))
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before TTL expiry")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("expected miss after TTL expiry")
	}
	// Entry should be removed from the index on expiry.
	if c.Len() != 0 {
		t.Errorf("expected 0 entries after expiry, got %d", c.Len())
	}
}

func TestCache_ZeroTTLNoExpiry(t *testing.T) {
	t.Parallel()
	c := New(Config{TTL: 0})
	c.Put("k", []byte("v"))
	time.Sleep(5 * time.Millisecond) // small sleep; entry should still be there
	if _, ok := c.Get("k"); !ok {
		t.Error("expected hit when TTL=0")
	}
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func TestCache_Stats(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 10})
	c.Put("a", []byte("AAAAA"))
	c.Put("b", []byte("BBBBB"))
	c.Get("a") // hit
	c.Get("x") // miss
	c.Put("c", []byte("CCCCC")) // evicts "a" (accessed after "b", but "b" is older — actually "b" is LRU after we accessed "a")

	s := c.Stats()
	if s.Hits != 1 {
		t.Errorf("hits: got %d, want 1", s.Hits)
	}
	if s.Misses != 1 {
		t.Errorf("misses: got %d, want 1", s.Misses)
	}
	if s.Evictions != 1 {
		t.Errorf("evictions: got %d, want 1", s.Evictions)
	}
	if s.Bytes != 10 {
		t.Errorf("bytes: got %d, want 10", s.Bytes)
	}
}

func TestCache_BytesDecrementOnDelete(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	c.Put("k", []byte("hello"))
	if c.Stats().Bytes != 5 {
		t.Fatalf("bytes before delete: got %d, want 5", c.Stats().Bytes)
	}
	c.Delete("k")
	if c.Stats().Bytes != 0 {
		t.Errorf("bytes after delete: got %d, want 0", c.Stats().Bytes)
	}
}

// ── Len ───────────────────────────────────────────────────────────────────────

func TestCache_Len(t *testing.T) {
	t.Parallel()
	c := New(Config{})
	if c.Len() != 0 {
		t.Fatalf("initial len: got %d, want 0", c.Len())
	}
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	if c.Len() != 2 {
		t.Errorf("after 2 puts: got %d, want 2", c.Len())
	}
	c.Delete("a")
	if c.Len() != 1 {
		t.Errorf("after delete: got %d, want 1", c.Len())
	}
}

// ── Concurrency ───────────────────────────────────────────────────────────────

func TestCache_ConcurrentSafe(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 1024})
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("k%d", (i*100+j)%20)
				c.Put(key, []byte("value"))
				c.Get(key)
				c.Delete(key)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// ── PutAndRecordEvictions ─────────────────────────────────────────────────────

// TestCache_PutAndRecordEvictions_NoEviction verifies that zero is returned
// when the new entry fits without displacing anything.
func TestCache_PutAndRecordEvictions_NoEviction(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 10})
	evicted := c.PutAndRecordEvictions("k", []byte("hello")) // 5 bytes; fits
	if evicted != 0 {
		t.Errorf("expected 0 evictions, got %d", evicted)
	}
}

// TestCache_PutAndRecordEvictions_OneEviction verifies that the exact number
// of evictions caused by a Put is returned (#54).
func TestCache_PutAndRecordEvictions_OneEviction(t *testing.T) {
	t.Parallel()
	// Budget = 10 bytes; two 5-byte entries fill it exactly.
	c := New(Config{MaxBytes: 10})
	c.Put("a", []byte("AAAAA"))
	c.Put("b", []byte("BBBBB"))
	// Third 5-byte entry must evict exactly one LRU entry.
	evicted := c.PutAndRecordEvictions("c", []byte("CCCCC"))
	if evicted != 1 {
		t.Errorf("expected 1 eviction, got %d", evicted)
	}
	// Verify cache state: "a" should be gone, "b" and "c" present.
	if _, ok := c.Get("a"); ok {
		t.Error("a should have been evicted")
	}
}

// TestCache_PutAndRecordEvictions_OversizedDropped verifies that zero is
// returned when the value exceeds MaxBytes and is silently dropped.
func TestCache_PutAndRecordEvictions_OversizedDropped(t *testing.T) {
	t.Parallel()
	c := New(Config{MaxBytes: 5})
	evicted := c.PutAndRecordEvictions("big", make([]byte, 10))
	if evicted != 0 {
		t.Errorf("oversized drop should return 0 evictions, got %d", evicted)
	}
	if _, ok := c.Get("big"); ok {
		t.Error("oversized entry should not be cached")
	}
}

// TestCache_PutAndRecordEvictions_ConsistentWithPut verifies that
// PutAndRecordEvictions produces the same cache state as Put.
func TestCache_PutAndRecordEvictions_ConsistentWithPut(t *testing.T) {
	t.Parallel()
	// Two caches with same config; use Put in one and PutAndRecordEvictions
	// in the other; end state must be identical.
	c1 := New(Config{MaxBytes: 10})
	c2 := New(Config{MaxBytes: 10})
	for _, key := range []string{"a", "b", "c"} {
		c1.Put(key, []byte("AAAAA"))
		c2.PutAndRecordEvictions(key, []byte("AAAAA"))
	}
	if c1.Stats().Bytes != c2.Stats().Bytes {
		t.Errorf("bytes differ: c1=%d c2=%d", c1.Stats().Bytes, c2.Stats().Bytes)
	}
	if c1.Len() != c2.Len() {
		t.Errorf("len differs: c1=%d c2=%d", c1.Len(), c2.Len())
	}
}
