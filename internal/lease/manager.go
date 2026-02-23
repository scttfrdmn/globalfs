// Package lease implements a distributed lease manager backed by etcd.
//
// A lease is a time-limited ownership claim on a named resource.  When
// multiple coordinator instances compete for the same lease, only the winner
// is the active leader and processes replication jobs.
//
// Two constructors are provided:
//
//   - [NewManager]: production, backed by etcd via [metadata.EtcdStore].
//   - [NewMemoryManager]: in-process, zero-dependency, for unit tests.
//   - [NewMemoryManagerPair]: returns two managers sharing a backend, for
//     multi-node unit tests.
package lease

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/scttfrdmn/globalfs/internal/metadata"
)

// ── Lease ─────────────────────────────────────────────────────────────────────

// Lease represents a successfully acquired distributed lease.
type Lease struct {
	// Resource is the name of the resource this lease protects.
	Resource string

	nodeID  string
	leaseID int64
	mgr     *Manager
}

// KeepAlive starts background lease renewal.  It returns a channel that is
// closed when the lease is lost — because the context was cancelled, the TTL
// expired, or Release was called.
//
// KeepAlive should be called once immediately after [Manager.Acquire] or
// [Manager.TryAcquire].
func (l *Lease) KeepAlive(ctx context.Context) <-chan struct{} {
	return l.mgr.backend.keepAlive(ctx, l.leaseID)
}

// Release surrenders the lease immediately so another node can acquire it.
// Any channel returned by KeepAlive will be closed shortly after Release.
func (l *Lease) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return l.mgr.backend.revoke(ctx, l.leaseID)
}

// ── Manager ───────────────────────────────────────────────────────────────────

// Manager provides distributed lease acquisition backed by an etcd cluster
// (or an in-memory simulation for tests).  Manager is safe for concurrent use.
type Manager struct {
	nodeID  string
	prefix  string // key prefix (e.g. "/globalfs/")
	backend leaseBackend
}

// NewManager returns a Manager backed by etcd via the given EtcdStore.
// nodeID uniquely identifies this coordinator instance (e.g. hostname:port).
func NewManager(nodeID string, store *metadata.EtcdStore) *Manager {
	return &Manager{
		nodeID:  nodeID,
		prefix:  store.Prefix(),
		backend: newEtcdBackend(store.Client()),
	}
}

// NewMemoryManager returns an in-process Manager suitable for unit tests.
// Leases are not persisted and have no real TTL semantics.
func NewMemoryManager(nodeID string) *Manager {
	return &Manager{
		nodeID:  nodeID,
		prefix:  "/test/",
		backend: newMemBackend(),
	}
}

// NewMemoryManagerPair returns two Managers sharing the same in-memory
// backend.  Use this to simulate leader-election between two "nodes" in
// unit tests.
func NewMemoryManagerPair(nodeID1, nodeID2 string) (*Manager, *Manager) {
	b := newMemBackend()
	m1 := &Manager{nodeID: nodeID1, prefix: "/test/", backend: b}
	m2 := &Manager{nodeID: nodeID2, prefix: "/test/", backend: b}
	return m1, m2
}

// resourceKey maps a resource name to its etcd key.
func (m *Manager) resourceKey(resource string) string {
	return m.prefix + "leases/" + resource
}

// Acquire attempts to obtain the lease for resource with the given TTL.
// If the lease is already held by another node, Acquire blocks until the
// current holder releases it or ctx expires.
//
// For a non-blocking variant, use [TryAcquire].
func (m *Manager) Acquire(ctx context.Context, resource string, ttl time.Duration) (*Lease, error) {
	ttlSec := durationToSeconds(ttl)
	key := m.resourceKey(resource)

	for {
		leaseID, err := m.backend.grant(ctx, ttlSec)
		if err != nil {
			return nil, fmt.Errorf("lease: grant for %q: %w", resource, err)
		}

		ok, err := m.backend.txnPutIfNotExists(ctx, key, m.nodeID, leaseID)
		if err != nil {
			_ = m.backend.revoke(ctx, leaseID)
			return nil, fmt.Errorf("lease: acquire %q: %w", resource, err)
		}
		if ok {
			return &Lease{Resource: resource, nodeID: m.nodeID, leaseID: leaseID, mgr: m}, nil
		}
		_ = m.backend.revoke(ctx, leaseID)

		// Lease is held; wait briefly then retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TryAcquire attempts to obtain the lease for resource without blocking.
// Returns (lease, true, nil) on success, or (nil, false, nil) if currently
// held by another node.
func (m *Manager) TryAcquire(ctx context.Context, resource string, ttl time.Duration) (*Lease, bool, error) {
	ttlSec := durationToSeconds(ttl)
	key := m.resourceKey(resource)

	leaseID, err := m.backend.grant(ctx, ttlSec)
	if err != nil {
		return nil, false, fmt.Errorf("lease: grant for %q: %w", resource, err)
	}

	ok, err := m.backend.txnPutIfNotExists(ctx, key, m.nodeID, leaseID)
	if err != nil {
		_ = m.backend.revoke(ctx, leaseID)
		return nil, false, fmt.Errorf("lease: try-acquire %q: %w", resource, err)
	}
	if !ok {
		_ = m.backend.revoke(ctx, leaseID)
		return nil, false, nil
	}

	return &Lease{Resource: resource, nodeID: m.nodeID, leaseID: leaseID, mgr: m}, true, nil
}

// IsLeader reports whether this Manager instance currently holds the lease
// for the named resource.
func (m *Manager) IsLeader(ctx context.Context, resource string) (bool, error) {
	key := m.resourceKey(resource)
	holder, exists, err := m.backend.get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("lease: check leader for %q: %w", resource, err)
	}
	return exists && holder == m.nodeID, nil
}

// durationToSeconds converts d to whole seconds, clamped to a minimum of 1.
func durationToSeconds(d time.Duration) int64 {
	sec := int64(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	return sec
}

// ── leaseBackend ──────────────────────────────────────────────────────────────

// leaseBackend abstracts over etcd and the in-memory test double.
type leaseBackend interface {
	// grant creates a lease with the given TTL (seconds).
	grant(ctx context.Context, ttlSeconds int64) (leaseID int64, err error)

	// revoke cancels a lease immediately.
	revoke(ctx context.Context, leaseID int64) error

	// keepAlive starts periodic lease renewal.  The returned channel is closed
	// when the lease is lost: context cancelled, TTL expired, or revoked.
	keepAlive(ctx context.Context, leaseID int64) <-chan struct{}

	// txnPutIfNotExists atomically stores key=value with the given lease only
	// if key does not already exist.  Returns true if the write succeeded.
	txnPutIfNotExists(ctx context.Context, key, value string, leaseID int64) (bool, error)

	// get returns the current value for key and whether it exists.
	get(ctx context.Context, key string) (value string, exists bool, err error)

	// del deletes key unconditionally.
	del(ctx context.Context, key string) error
}

// ── etcd backend ──────────────────────────────────────────────────────────────

type etcdBackend struct {
	client *clientv3.Client
}

func newEtcdBackend(client *clientv3.Client) *etcdBackend {
	return &etcdBackend{client: client}
}

func (b *etcdBackend) grant(ctx context.Context, ttlSec int64) (int64, error) {
	resp, err := b.client.Grant(ctx, ttlSec)
	if err != nil {
		return 0, err
	}
	return int64(resp.ID), nil
}

func (b *etcdBackend) revoke(ctx context.Context, leaseID int64) error {
	_, err := b.client.Revoke(ctx, clientv3.LeaseID(leaseID))
	return err
}

func (b *etcdBackend) keepAlive(ctx context.Context, leaseID int64) <-chan struct{} {
	lost := make(chan struct{})
	kaCh, err := b.client.KeepAlive(ctx, clientv3.LeaseID(leaseID))
	if err != nil {
		close(lost)
		return lost
	}
	go func() {
		defer close(lost)
		for {
			select {
			case _, ok := <-kaCh:
				if !ok {
					return // etcd closed the keepalive channel
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return lost
}

func (b *etcdBackend) txnPutIfNotExists(ctx context.Context, key, value string, leaseID int64) (bool, error) {
	resp, err := b.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(clientv3.LeaseID(leaseID)))).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (b *etcdBackend) get(ctx context.Context, key string) (string, bool, error) {
	resp, err := b.client.Get(ctx, key)
	if err != nil {
		return "", false, err
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	return string(resp.Kvs[0].Value), true, nil
}

func (b *etcdBackend) del(ctx context.Context, key string) error {
	_, err := b.client.Delete(ctx, key)
	return err
}

// ── in-memory backend ─────────────────────────────────────────────────────────

type memLease struct {
	id   int64
	lost chan struct{}
	once sync.Once
}

func (l *memLease) close() {
	l.once.Do(func() { close(l.lost) })
}

type memBackend struct {
	mu        sync.Mutex
	nextID    int64
	leases    map[int64]*memLease
	keys      map[string]int64  // key → leaseID
	keyValues map[string]string // key → holder nodeID
}

func newMemBackend() *memBackend {
	return &memBackend{
		nextID:    1,
		leases:    make(map[int64]*memLease),
		keys:      make(map[string]int64),
		keyValues: make(map[string]string),
	}
}

func (b *memBackend) grant(_ context.Context, _ int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextID
	b.nextID++
	b.leases[id] = &memLease{id: id, lost: make(chan struct{})}
	return id, nil
}

func (b *memBackend) revoke(_ context.Context, leaseID int64) error {
	b.mu.Lock()
	l, ok := b.leases[leaseID]
	if !ok {
		b.mu.Unlock()
		return nil // already revoked
	}
	for k, id := range b.keys {
		if id == leaseID {
			delete(b.keys, k)
			delete(b.keyValues, k)
		}
	}
	delete(b.leases, leaseID)
	b.mu.Unlock()

	l.close() // close outside the lock to avoid potential deadlock in close callbacks
	return nil
}

func (b *memBackend) keepAlive(ctx context.Context, leaseID int64) <-chan struct{} {
	b.mu.Lock()
	l, ok := b.leases[leaseID]
	b.mu.Unlock()

	if !ok {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	// When the context is cancelled, revoke the lease (simulates keepalive stopping).
	go func() {
		select {
		case <-ctx.Done():
			// Use a bounded context so the goroutine cannot block indefinitely
			// if revoke ever contends on the mutex or gains a real network call.
			revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer revokeCancel()
			_ = b.revoke(revokeCtx, leaseID)
		case <-l.lost:
			// Already gone; goroutine exits cleanly.
		}
	}()

	return l.lost
}

func (b *memBackend) txnPutIfNotExists(_ context.Context, key, value string, leaseID int64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If key exists and its lease is still valid, the slot is taken.
	if existingID, exists := b.keys[key]; exists {
		if _, leaseValid := b.leases[existingID]; leaseValid {
			return false, nil
		}
		// Stale lease: clean up and allow the put.
		delete(b.keys, key)
		delete(b.keyValues, key)
	}

	if _, ok := b.leases[leaseID]; !ok {
		return false, fmt.Errorf("lease: grant %d not found or expired", leaseID)
	}

	b.keys[key] = leaseID
	b.keyValues[key] = value
	return true, nil
}

func (b *memBackend) get(_ context.Context, key string) (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	existingID, exists := b.keys[key]
	if !exists {
		return "", false, nil
	}
	if _, leaseValid := b.leases[existingID]; !leaseValid {
		return "", false, nil // stale
	}
	return b.keyValues[key], true, nil
}

func (b *memBackend) del(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.keys, key)
	delete(b.keyValues, key)
	return nil
}
