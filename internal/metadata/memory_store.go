package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// MemoryStore is a thread-safe in-process implementation of [Store].
//
// State is not persisted across process restarts.  MemoryStore is suitable
// for unit tests and single-node deployments that do not require durability.
type MemoryStore struct {
	mu       sync.RWMutex
	sites    map[string]*SiteRecord
	jobs     map[string]*ReplicationJob
	watchers []*memWatcher
}

// memWatcher holds a single Watch subscription.
type memWatcher struct {
	prefix string
	ch     chan WatchEvent
}

// NewMemoryStore creates an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sites: make(map[string]*SiteRecord),
		jobs:  make(map[string]*ReplicationJob),
	}
}

// ── Site registry ──────────────────────────────────────────────────────────────

// PutSite creates or updates the record for site.Name.
func (m *MemoryStore) PutSite(_ context.Context, site *SiteRecord) error {
	cp := *site
	data, err := json.Marshal(&cp)
	if err != nil {
		slog.Error("metadata: failed to marshal SiteRecord for watcher notification", "site", site.Name, "err", err)
		// data is nil; watchers receive a WatchEvent with nil Value.
	}
	m.mu.Lock()
	m.sites[site.Name] = &cp
	m.mu.Unlock()
	// Notify outside the lock so watcher consumers can call store methods
	// without risk of deadlock, and so the lock is not held during channel I/O.
	m.notify("sites/"+site.Name, WatchEventPut, data)
	return nil
}

// GetSite returns the record for the named site, or an error if not found.
func (m *MemoryStore) GetSite(_ context.Context, name string) (*SiteRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sites[name]
	if !ok {
		return nil, fmt.Errorf("metadata: site %q not found", name)
	}
	cp := *s
	return &cp, nil
}

// ListSites returns all stored site records in unspecified order.
func (m *MemoryStore) ListSites(_ context.Context) ([]*SiteRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*SiteRecord, 0, len(m.sites))
	for _, s := range m.sites {
		cp := *s
		result = append(result, &cp)
	}
	return result, nil
}

// DeleteSite removes the record for the named site.
func (m *MemoryStore) DeleteSite(_ context.Context, name string) error {
	m.mu.Lock()
	delete(m.sites, name)
	m.mu.Unlock()
	m.notify("sites/"+name, WatchEventDelete, nil)
	return nil
}

// ── Replication queue ──────────────────────────────────────────────────────────

// PutReplicationJob creates or replaces the job record.
func (m *MemoryStore) PutReplicationJob(_ context.Context, job *ReplicationJob) error {
	cp := *job
	data, err := json.Marshal(&cp)
	if err != nil {
		slog.Error("metadata: failed to marshal ReplicationJob for watcher notification", "id", job.ID, "err", err)
		// data is nil; watchers receive a WatchEvent with nil Value.
	}
	m.mu.Lock()
	m.jobs[job.ID] = &cp
	m.mu.Unlock()
	m.notify("jobs/"+job.ID, WatchEventPut, data)
	return nil
}

// GetPendingJobs returns all stored replication jobs in unspecified order.
func (m *MemoryStore) GetPendingJobs(_ context.Context) ([]*ReplicationJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*ReplicationJob, 0, len(m.jobs))
	for _, j := range m.jobs {
		cp := *j
		result = append(result, &cp)
	}
	return result, nil
}

// DeleteJob removes the job with the given ID.
func (m *MemoryStore) DeleteJob(_ context.Context, id string) error {
	m.mu.Lock()
	delete(m.jobs, id)
	m.mu.Unlock()
	m.notify("jobs/"+id, WatchEventDelete, nil)
	return nil
}

// ── Watch ──────────────────────────────────────────────────────────────────────

// Watch returns a buffered channel of WatchEvents for keys matching prefix.
// The channel is closed when ctx is cancelled.
func (m *MemoryStore) Watch(ctx context.Context, prefix string) (<-chan WatchEvent, error) {
	ch := make(chan WatchEvent, 64)
	w := &memWatcher{prefix: prefix, ch: ch}

	m.mu.Lock()
	m.watchers = append(m.watchers, w)
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		// Remove the watcher under the lock so future notify calls will not
		// include it in their snapshot.  Close the channel after releasing the
		// lock to avoid a send-on-closed-channel panic in concurrent notifies
		// that may have already snapshotted this watcher.
		m.mu.Lock()
		for i, existing := range m.watchers {
			if existing == w {
				m.watchers = append(m.watchers[:i], m.watchers[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		close(ch)
	}()

	return ch, nil
}

// ── Lifecycle ──────────────────────────────────────────────────────────────────

// Close is a no-op for MemoryStore; it satisfies the [Store] interface.
func (m *MemoryStore) Close() error { return nil }

// ── Internal ───────────────────────────────────────────────────────────────────

// notify fans out a WatchEvent to all watchers whose prefix matches key.
// Must NOT be called with m.mu held — it acquires its own RLock to snapshot
// the watchers list and then sends outside the lock so that:
//  1. Watcher consumers may call store methods without deadlocking.
//  2. Channel I/O does not delay other writers.
//
// A per-send recover guards against the rare race where a watcher is removed
// and its channel is closed between the snapshot and the send.
func (m *MemoryStore) notify(key string, evType WatchEventType, value []byte) {
	ev := WatchEvent{Type: evType, Key: key, Value: value}

	m.mu.RLock()
	snapshot := make([]*memWatcher, len(m.watchers))
	copy(snapshot, m.watchers)
	m.mu.RUnlock()

	for _, w := range snapshot {
		if strings.HasPrefix(key, w.prefix) {
			safeWatchSend(w.ch, ev)
		}
	}
}

// safeWatchSend performs a non-blocking send on ch, recovering from a panic
// in the event the channel was closed between the watchers snapshot and the
// send (a narrow but theoretically possible race).
func safeWatchSend(ch chan WatchEvent, ev WatchEvent) {
	defer func() { recover() }() //nolint:errcheck
	select {
	case ch <- ev:
	default:
		// Drop event if subscriber is not keeping up.
	}
}
