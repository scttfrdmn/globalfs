// Package coordinator routes object operations across a prioritized set of
// SiteMounts.
//
// # Routing
//
// When a [*policy.Engine] is registered via SetPolicy, every operation
// delegates site selection and ordering to the engine.  If no policy is set
// (the default), the engine behaves as an empty rule set: sites are ordered
// primary → backup → burst.
//
//   - Get/Head: tries sites in the routed order, returns the first success.
//   - Put: writes synchronously to primary-role sites in the routed set;
//     asynchronously replicates to non-primary sites via a bounded queue.
//   - Delete: synchronous on primary-role sites (errors returned);
//     best-effort on non-primaries (errors logged).
//   - List: delegates to the embedded Namespace (priority-merge, no policy).
//
// # Lifecycle
//
// Call Start to begin background replication processing, then Stop (or Close)
// when done.  Coordinator is safe for concurrent use.
package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/policy"
	"github.com/scttfrdmn/globalfs/pkg/namespace"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// replicaTask holds a pending background replication job.
type replicaTask struct {
	key  string
	data []byte
	dest *site.SiteMount
}

const defaultReplicaQueueDepth = 256

// Coordinator routes object operations across a prioritized set of SiteMounts.
type Coordinator struct {
	mu     sync.RWMutex
	sites  []*site.SiteMount
	ns     *namespace.Namespace
	policy *policy.Engine // never nil; default = empty engine

	// replicaCh carries async replication tasks for non-primary sites.
	replicaCh chan replicaTask

	wg   sync.WaitGroup
	done chan struct{}
	once sync.Once
}

// New creates a Coordinator from an ordered list of SiteMounts.
//
// Sites listed earlier have higher priority for reads.  Call Start to enable
// background replication; without it Put operations still write synchronously
// to primaries but non-primary sites never receive async copies.
func New(sites ...*site.SiteMount) *Coordinator {
	cp := make([]*site.SiteMount, len(sites))
	copy(cp, sites)
	return &Coordinator{
		sites:     cp,
		ns:        namespace.New(cp...),
		policy:    policy.New(), // empty engine → default role ordering
		replicaCh: make(chan replicaTask, defaultReplicaQueueDepth),
		done:      make(chan struct{}),
	}
}

// SetPolicy registers a policy engine with the coordinator.
//
// Subsequent routing decisions (Get, Put, Delete, Head) will be delegated to
// the engine.  Pass nil to revert to the default empty engine (role ordering).
func (c *Coordinator) SetPolicy(e *policy.Engine) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e == nil {
		c.policy = policy.New()
	} else {
		c.policy = e
	}
}

// Start launches the background replication worker.
// It is safe to call Start multiple times; only the first call has effect.
func (c *Coordinator) Start(ctx context.Context) {
	c.once.Do(func() {
		c.wg.Add(1)
		go c.replicationWorker(ctx)
	})
}

// Stop signals the background replication worker to stop and waits for it to
// drain.  Calling Stop before Start is safe.
func (c *Coordinator) Stop() {
	c.once.Do(func() {}) // consume once so Start is a no-op if called later
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	c.wg.Wait()
}

// Close stops background work and closes all sites.
func (c *Coordinator) Close() error {
	c.Stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ns.Close()
}

// AddSite appends a site at the lowest priority.
func (c *Coordinator) AddSite(s *site.SiteMount) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sites = append(c.sites, s)
	c.ns.AddSite(s)
}

// RemoveSite removes the site with the given name.
// If no site with that name exists, this is a no-op.
func (c *Coordinator) RemoveSite(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	filtered := make([]*site.SiteMount, 0, len(c.sites))
	for _, s := range c.sites {
		if s.Name() != name {
			filtered = append(filtered, s)
		}
	}
	c.sites = filtered
	c.ns = namespace.New(c.sites...)
}

// Sites returns a snapshot of the current site list (highest priority first).
func (c *Coordinator) Sites() []*site.SiteMount {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make([]*site.SiteMount, len(c.sites))
	copy(cp, c.sites)
	return cp
}

// Health returns a per-site health report.
// A nil error means the site is healthy; checks run concurrently.
func (c *Coordinator) Health(ctx context.Context) map[string]error {
	c.mu.RLock()
	snapshot := c.snapshotSites()
	c.mu.RUnlock()

	result := make(map[string]error, len(snapshot))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, s := range snapshot {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Health(ctx)
			mu.Lock()
			result[s.Name()] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result
}

// Get fetches the full content of the object at key.
//
// The policy engine determines site order; sites are tried in that order and
// the first successful read is returned.
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	snapshot, pol := c.snapshotSites(), c.policy
	c.mu.RUnlock()

	ordered, err := pol.Route(ctx, policy.OperationRead, key, snapshot)
	if err != nil {
		return nil, fmt.Errorf("coordinator: Get %q: policy error: %w", key, err)
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("coordinator: Get %q: no sites available", key)
	}

	var lastErr error
	for _, s := range ordered {
		data, err := s.Get(ctx, key, 0, 0)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("coordinator: Get %q failed on all sites: %w", key, lastErr)
}

// Put writes data to the primary-role sites in the policy-routed set
// synchronously, and enqueues async replication to non-primary sites.
//
// Returns once all primary sites have acknowledged the write.  If any primary
// write fails, Put returns immediately with that error.
func (c *Coordinator) Put(ctx context.Context, key string, data []byte) error {
	c.mu.RLock()
	snapshot, pol := c.snapshotSites(), c.policy
	c.mu.RUnlock()

	routed, err := pol.Route(ctx, policy.OperationWrite, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Put %q: policy error: %w", key, err)
	}

	primaries, others := partitionByRole(routed)

	for _, s := range primaries {
		if err := s.Put(ctx, key, data); err != nil {
			return fmt.Errorf("coordinator: Put %q to primary %q: %w", key, s.Name(), err)
		}
	}

	for _, s := range others {
		task := replicaTask{key: key, data: data, dest: s}
		select {
		case c.replicaCh <- task:
		default:
			log.Printf("coordinator: replication queue full; dropping async copy of %q to %q",
				key, s.Name())
		}
	}
	return nil
}

// Delete removes the object at key from sites in the policy-routed set.
//
// Primary site deletes are synchronous and return errors on failure.
// Non-primary deletes are best-effort: errors are logged but not returned.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	c.mu.RLock()
	snapshot, pol := c.snapshotSites(), c.policy
	c.mu.RUnlock()

	routed, err := pol.Route(ctx, policy.OperationDelete, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Delete %q: policy error: %w", key, err)
	}

	primaries, others := partitionByRole(routed)

	for _, s := range primaries {
		if err := s.Delete(ctx, key); err != nil {
			return fmt.Errorf("coordinator: Delete %q from primary %q: %w", key, s.Name(), err)
		}
	}
	for _, s := range others {
		if err := s.Delete(ctx, key); err != nil {
			log.Printf("coordinator: Delete %q from non-primary %q: %v", key, s.Name(), err)
		}
	}
	return nil
}

// List returns up to limit objects under prefix, merged across all sites.
// Delegates to the embedded Namespace (highest-priority site wins on conflicts).
// Pass limit ≤ 0 to retrieve all matching objects.
func (c *Coordinator) List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	return c.ns.List(ctx, prefix, limit)
}

// Head returns metadata for the object at key.
// Sites are checked in policy-routed order; the first hit is returned.
func (c *Coordinator) Head(ctx context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	c.mu.RLock()
	snapshot, pol := c.snapshotSites(), c.policy
	c.mu.RUnlock()

	ordered, err := pol.Route(ctx, policy.OperationRead, key, snapshot)
	if err != nil {
		return nil, fmt.Errorf("coordinator: Head %q: policy error: %w", key, err)
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("coordinator: Head %q: no sites available", key)
	}

	var lastErr error
	for _, s := range ordered {
		info, err := s.Head(ctx, key)
		if err == nil {
			return info, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("coordinator: Head %q failed on all sites: %w", key, lastErr)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// snapshotSites returns a copy of c.sites. Caller must hold at least RLock.
func (c *Coordinator) snapshotSites() []*site.SiteMount {
	cp := make([]*site.SiteMount, len(c.sites))
	copy(cp, c.sites)
	return cp
}

// partitionByRole splits sites into primary-role and non-primary slices,
// preserving the relative order within each group.
func partitionByRole(sites []*site.SiteMount) (primaries, others []*site.SiteMount) {
	for _, s := range sites {
		if s.Role() == types.SiteRolePrimary {
			primaries = append(primaries, s)
		} else {
			others = append(others, s)
		}
	}
	return
}

// replicationWorker drains replicaCh until ctx is cancelled or Stop is called.
func (c *Coordinator) replicationWorker(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			return
		case task := <-c.replicaCh:
			if err := task.dest.Put(ctx, task.key, task.data); err != nil {
				log.Printf("coordinator: async replication of %q to %q failed: %v",
					task.key, task.dest.Name(), err)
			}
		}
	}
}
