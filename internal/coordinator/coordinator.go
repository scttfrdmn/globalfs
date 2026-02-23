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
//     asynchronously replicates to non-primary sites via the replication.Worker.
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
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/cache"
	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/lease"
	"github.com/scttfrdmn/globalfs/internal/metadata"
	"github.com/scttfrdmn/globalfs/internal/metrics"
	"github.com/scttfrdmn/globalfs/internal/policy"
	"github.com/scttfrdmn/globalfs/internal/replication"
	"github.com/scttfrdmn/globalfs/internal/retry"
	"github.com/scttfrdmn/globalfs/pkg/namespace"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// Coordinator routes object operations across a prioritized set of SiteMounts.
type Coordinator struct {
	mu           sync.RWMutex
	sites        []*site.SiteMount
	ns           *namespace.Namespace
	policy       *policy.Engine  // never nil; default = empty engine
	worker       *replication.Worker
	store        metadata.Store   // optional; nil means no persistence
	m            *metrics.Metrics // optional; nil means no instrumentation
	storeCancel  context.CancelFunc
	storeWg      sync.WaitGroup
	startOnce    sync.Once          // ensures Start() body runs exactly once
	leaseManager *lease.Manager     // optional; nil means single-node mode
	leaderLease  *lease.Lease       // non-nil when this instance is the leader
	leaderCancel context.CancelFunc // cancels the leaderCtx

	// Background health polling.
	healthPollInterval time.Duration   // 0 → use defaultHealthPollInterval
	healthCacheMu      sync.RWMutex
	healthCache        map[string]error // nil = not yet polled
	healthCheckedAt    time.Time

	// Circuit breaker (optional).
	cb *circuitbreaker.Breaker // nil = disabled

	// Per-site retry (optional).
	retryConfig *retry.Config // nil = single attempt (no retry)

	// Read-through object cache (optional).
	objCache *cache.Cache // nil = disabled
}

// defaultHealthPollInterval is the cadence for background site health checks
// when no explicit interval has been set via SetHealthPollInterval.
const defaultHealthPollInterval = 30 * time.Second

// New creates a Coordinator from an ordered list of SiteMounts.
//
// Sites listed earlier have higher priority for reads.  Call Start to enable
// background replication; without it Put operations still write synchronously
// to primaries but non-primary sites never receive async copies.
func New(sites ...*site.SiteMount) *Coordinator {
	cp := make([]*site.SiteMount, len(sites))
	copy(cp, sites)
	return &Coordinator{
		sites:  cp,
		ns:     namespace.New(cp...),
		policy: policy.New(), // empty engine → default role ordering
		worker: replication.NewWorker(0),
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

// SetStore registers a metadata store for persistence.
//
// When set, replication jobs are persisted before they are enqueued so they
// survive coordinator restarts.  Completed and failed jobs are deleted from
// the store.  SetStore must be called before Start to enable job recovery.
func (c *Coordinator) SetStore(s metadata.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = s
}

// SetLeaseManager registers a distributed lease manager.
//
// When set, Start attempts to acquire the "coordinator/leader" lease before
// launching the replication worker.  If this instance is not the leader, the
// worker is not started and the coordinator operates in standby mode (writes
// still reach primary sites synchronously, but async replication is skipped).
//
// SetLeaseManager must be called before Start.
func (c *Coordinator) SetLeaseManager(mgr *lease.Manager) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseManager = mgr
}

// SetMetrics registers a Metrics instance with the coordinator.
// When set, site-count and replication event metrics are emitted automatically.
// SetMetrics must be called before Start to instrument replication events.
func (c *Coordinator) SetMetrics(m *metrics.Metrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = m
}

// SetHealthPollInterval sets the interval between background site health
// checks.  The default is 30 seconds.  Pass 0 to use the default.
// Must be called before Start.
func (c *Coordinator) SetHealthPollInterval(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healthPollInterval = d
}

// SetCircuitBreaker registers a circuit breaker with the coordinator.
//
// When set, sites whose circuit is open are skipped during read routing (Get,
// Head).  Write operations (Put, Delete) always target their designated sites
// but record success/failure to keep the breaker state current.
//
// If all circuits are open the breaker is bypassed so callers are never
// completely blocked by a stale circuit state.  Pass nil to disable circuit
// breaking.  May be called at any time; safe for concurrent use.
func (c *Coordinator) SetCircuitBreaker(cb *circuitbreaker.Breaker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cb = cb
}

// SetRetryConfig registers a per-site retry configuration with the coordinator.
//
// When set, each site attempt in Get and Head is retried up to
// cfg.MaxAttempts times (with exponential backoff) before the coordinator
// records a failure in the circuit breaker and moves to the next site.
//
// This is intentionally not applied to Put or Delete: primary writes are
// fail-fast by design to prevent double-write confusion.
//
// Pass nil to disable retries (the default: each site is tried once).
// May be called at any time; safe for concurrent use.
func (c *Coordinator) SetRetryConfig(cfg *retry.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retryConfig = cfg
}

// SetCache registers an in-memory LRU cache with the coordinator.
//
// When set, Get serves reads from the cache when available (read-through).
// Put and Delete invalidate the affected key so stale data is never returned.
//
// Pass nil to disable caching.  May be called at any time; safe for
// concurrent use.
func (c *Coordinator) SetCache(oc *cache.Cache) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objCache = oc
}

// HealthStatus returns the most recent cached health report and the time it
// was collected.  Returns (nil, zero) if no background poll has run yet.
// The returned map is a copy; it is safe to read without holding any lock.
func (c *Coordinator) HealthStatus() (map[string]error, time.Time) {
	c.healthCacheMu.RLock()
	defer c.healthCacheMu.RUnlock()
	if c.healthCache == nil {
		return nil, time.Time{}
	}
	cp := make(map[string]error, len(c.healthCache))
	for k, v := range c.healthCache {
		cp[k] = v
	}
	return cp, c.healthCheckedAt
}

// runHealthPoll performs one health check of all sites and stores the result
// in the cache.  It is called from the background polling goroutine.
func (c *Coordinator) runHealthPoll(ctx context.Context) {
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	report := c.Health(pollCtx)
	now := time.Now()
	c.healthCacheMu.Lock()
	c.healthCache = report
	c.healthCheckedAt = now
	c.healthCacheMu.Unlock()
}

// Start launches the background replication worker.
// It is safe to call Start multiple times; only the first call has effect.
//
// If a LeaseManager has been registered via SetLeaseManager, Start attempts to
// acquire the "coordinator/leader" lease.  Only the leader starts the worker;
// if another instance already holds the lease this coordinator enters standby
// mode and Start returns without launching any background goroutines.
//
// If a Store has been registered via SetStore, Start also recovers any pending
// jobs from the previous run and begins draining worker events to keep the
// store in sync.
func (c *Coordinator) Start(ctx context.Context) {
	c.startOnce.Do(func() { c.start(ctx) })
}

// start is the internal implementation of Start; called exactly once via startOnce.
func (c *Coordinator) start(ctx context.Context) {
	c.mu.Lock()
	mgr := c.leaseManager
	store := c.store
	c.mu.Unlock()

	// workerCtx is cancelled when the lease is lost (if a lease manager is set).
	workerCtx := ctx

	if mgr != nil {
		l, acquired, err := mgr.TryAcquire(ctx, "coordinator/leader", 15*time.Second)
		if err != nil {
			log.Printf("coordinator: acquire leader lease: %v; running in standby mode", err)
			return
		}
		if !acquired {
			log.Printf("coordinator: another instance holds the leader lease; running in standby mode")
			return
		}
		log.Printf("coordinator: acquired leader lease")

		leaderCtx, leaderCancel := context.WithCancel(ctx)
		lostCh := l.KeepAlive(leaderCtx)

		c.mu.Lock()
		c.leaderLease = l
		c.leaderCancel = leaderCancel
		c.mu.Unlock()

		// Transition to standby when the lease is lost.
		go func() {
			defer leaderCancel()
			select {
			case <-lostCh:
				log.Printf("coordinator: lost leader lease; transitioning to standby mode")
			case <-leaderCtx.Done():
			}
		}()

		workerCtx = leaderCtx
	}

	// Always drain worker events — needed for metrics even when store is nil.
	// drainCancel is stored so Stop() can terminate this goroutine.
	drainCtx, drainCancel := context.WithCancel(workerCtx)
	c.mu.Lock()
	c.storeCancel = drainCancel
	c.mu.Unlock()

	if store != nil {
		c.recoverPendingJobs(workerCtx, store)
	}
	c.storeWg.Add(1)
	go func() {
		defer c.storeWg.Done()
		c.drainWorkerEvents(drainCtx, store)
	}()

	// Launch background health polling goroutine.
	// Uses drainCtx so it stops when Stop() is called (via storeCancel).
	pollInterval := c.healthPollInterval
	if pollInterval <= 0 {
		pollInterval = defaultHealthPollInterval
	}
	c.storeWg.Add(1)
	go func() {
		defer c.storeWg.Done()
		c.runHealthPoll(drainCtx) // immediate first check
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.runHealthPoll(drainCtx)
			case <-drainCtx.Done():
				return
			}
		}
	}()

	c.worker.Start(workerCtx)
}

// Stop signals the background replication worker to stop and waits for it to
// finish the current job.  If a leader lease is held it is released so that a
// standby coordinator can take over immediately.  Calling Stop before Start is
// safe.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	storeCancel := c.storeCancel
	leaderCancel := c.leaderCancel
	l := c.leaderLease
	c.mu.Unlock()

	// Cancel both contexts: stops the drain goroutine and keepalive goroutine.
	if leaderCancel != nil {
		leaderCancel()
	}
	if storeCancel != nil {
		storeCancel()
	}
	c.storeWg.Wait()
	c.worker.Stop()

	// Release the leader lease last so a standby can take over quickly.
	if l != nil {
		if err := l.Release(); err != nil {
			log.Printf("coordinator: release leader lease: %v", err)
		}
	}
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
	c.m.SetSiteCount(len(c.sites))
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
	c.m.SetSiteCount(len(c.sites))
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
// When a cache is registered via SetCache, Get returns the cached value
// immediately on a hit without contacting any site.  On a miss the data is
// fetched from a site and stored in the cache before being returned.
//
// The policy engine determines site order; healthy sites (from the background
// health cache) are then promoted to the front of that list so degraded sites
// are only tried as a fallback.  The first successful read is returned.
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	snapshot, pol, cb, retryCfg, oc, m := c.snapshotSites(), c.policy, c.cb, c.retryConfig, c.objCache, c.m
	c.mu.RUnlock()

	// Cache read-through: serve from cache when available.
	if oc != nil {
		if cached, ok := oc.Get(key); ok {
			m.RecordCacheHit()
			m.SetCacheBytes(oc.Stats().Bytes)
			return cached, nil
		}
		m.RecordCacheMiss()
	}

	ordered, err := pol.Route(ctx, policy.OperationRead, key, snapshot)
	if err != nil {
		return nil, fmt.Errorf("coordinator: Get %q: policy error: %w", key, err)
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("coordinator: Get %q: no sites available", key)
	}

	healthReport, _ := c.HealthStatus()
	ordered = preferHealthySites(ordered, healthReport)
	ordered = filterByCircuitBreaker(cb, ordered)

	var lastErr error
	for _, s := range ordered {
		var data []byte
		siteErr := doWithRetry(ctx, retryCfg, func() error {
			var err error
			data, err = s.Get(ctx, key, 0, 0)
			return err
		})
		if cb != nil {
			if siteErr == nil {
				cb.RecordSuccess(s.Name())
			} else {
				cb.RecordFailure(s.Name())
			}
		}
		if siteErr == nil {
			// Populate cache on successful site fetch.
			if oc != nil {
				prevEvictions := oc.Stats().Evictions
				oc.Put(key, data)
				newStats := oc.Stats()
				m.SetCacheBytes(newStats.Bytes)
				for i := int64(0); i < newStats.Evictions-prevEvictions; i++ {
					m.RecordCacheEviction()
				}
			}
			return data, nil
		}
		lastErr = siteErr
	}
	return nil, fmt.Errorf("coordinator: Get %q failed on all sites: %w", key, lastErr)
}

// Put writes data to the primary-role sites in the policy-routed set
// synchronously, and enqueues async replication to non-primary sites via the
// replication.Worker.
//
// Returns once all primary sites have acknowledged the write.  If any primary
// write fails, Put returns immediately with that error.
//
// If the policy routes a write to a set with no primaries (e.g. a burst-only
// rule), the first non-primary site is promoted to the synchronous write
// target so data is durably stored before Put returns.
func (c *Coordinator) Put(ctx context.Context, key string, data []byte) error {
	c.mu.RLock()
	snapshot, pol, store, cb, oc, m := c.snapshotSites(), c.policy, c.store, c.cb, c.objCache, c.m
	c.mu.RUnlock()

	routed, err := pol.Route(ctx, policy.OperationWrite, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Put %q: policy error: %w", key, err)
	}

	primaries, others := partitionByRole(routed)

	// If the routed set has no primaries (e.g. a burst-only policy rule),
	// promote the first site to a synchronous write target so the data is
	// persisted before Put returns.
	if len(primaries) == 0 && len(others) > 0 {
		primaries = others[:1]
		others = others[1:]
	}

	for _, s := range primaries {
		if err := s.Put(ctx, key, data); err != nil {
			if cb != nil {
				cb.RecordFailure(s.Name())
			}
			return fmt.Errorf("coordinator: Put %q to %q: %w", key, s.Name(), err)
		}
		if cb != nil {
			cb.RecordSuccess(s.Name())
		}
	}

	// Enqueue async replication to remaining sites using the first primary
	// (or promoted site) as the GET source.
	if len(primaries) > 0 {
		src := primaries[0]
		for _, s := range others {
			c.worker.Enqueue(replication.ReplicationJob{
				SourceSite: src,
				DestSite:   s,
				Key:        key,
				Size:       int64(len(data)),
			})
			if store != nil {
				metaJob := &metadata.ReplicationJob{
					ID:         makeJobID(src.Name(), s.Name(), key),
					SourceSite: src.Name(),
					DestSite:   s.Name(),
					Key:        key,
					Size:       int64(len(data)),
					CreatedAt:  time.Now(),
				}
				if persistErr := store.PutReplicationJob(ctx, metaJob); persistErr != nil {
					log.Printf("coordinator: persist job %q: %v", metaJob.ID, persistErr)
				}
			}
		}
	}

	// Invalidate the cache so the next Get fetches the freshly-written value.
	if oc != nil {
		oc.Delete(key)
		m.SetCacheBytes(oc.Stats().Bytes)
	}
	return nil
}

// Delete removes the object at key from sites in the policy-routed set.
//
// Primary site deletes are synchronous and return errors on failure.
// Non-primary deletes are best-effort: errors are logged but not returned.
// The cache entry for key is invalidated regardless of per-site outcome.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	c.mu.RLock()
	snapshot, pol, cb, oc, m := c.snapshotSites(), c.policy, c.cb, c.objCache, c.m
	c.mu.RUnlock()

	routed, err := pol.Route(ctx, policy.OperationDelete, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Delete %q: policy error: %w", key, err)
	}

	primaries, others := partitionByRole(routed)

	for _, s := range primaries {
		if err := s.Delete(ctx, key); err != nil {
			if cb != nil {
				cb.RecordFailure(s.Name())
			}
			return fmt.Errorf("coordinator: Delete %q from primary %q: %w", key, s.Name(), err)
		}
		if cb != nil {
			cb.RecordSuccess(s.Name())
		}
	}
	for _, s := range others {
		if err := s.Delete(ctx, key); err != nil {
			if cb != nil {
				cb.RecordFailure(s.Name())
			}
			log.Printf("coordinator: Delete %q from non-primary %q: %v", key, s.Name(), err)
		} else if cb != nil {
			cb.RecordSuccess(s.Name())
		}
	}

	// Invalidate the cache entry whether or not site deletes succeeded.
	if oc != nil {
		oc.Delete(key)
		m.SetCacheBytes(oc.Stats().Bytes)
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
// Sites are checked in policy-routed order with healthy sites promoted to the
// front (same health-aware reordering as Get).  The first hit is returned.
func (c *Coordinator) Head(ctx context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	c.mu.RLock()
	snapshot, pol, cb, retryCfg := c.snapshotSites(), c.policy, c.cb, c.retryConfig
	c.mu.RUnlock()

	ordered, err := pol.Route(ctx, policy.OperationRead, key, snapshot)
	if err != nil {
		return nil, fmt.Errorf("coordinator: Head %q: policy error: %w", key, err)
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("coordinator: Head %q: no sites available", key)
	}

	healthReport, _ := c.HealthStatus()
	ordered = preferHealthySites(ordered, healthReport)
	ordered = filterByCircuitBreaker(cb, ordered)

	var lastErr error
	for _, s := range ordered {
		var info *objectfstypes.ObjectInfo
		siteErr := doWithRetry(ctx, retryCfg, func() error {
			var err error
			info, err = s.Head(ctx, key)
			return err
		})
		if cb != nil {
			if siteErr == nil {
				cb.RecordSuccess(s.Name())
			} else {
				cb.RecordFailure(s.Name())
			}
		}
		if siteErr == nil {
			return info, nil
		}
		lastErr = siteErr
	}
	return nil, fmt.Errorf("coordinator: Head %q failed on all sites: %w", key, lastErr)
}

// ── Site information ──────────────────────────────────────────────────────────

// SiteInfo is a read-only snapshot of a site's name, role, and health.
type SiteInfo struct {
	Name         string         `json:"name"`
	Role         types.SiteRole `json:"role"`
	Healthy      bool           `json:"healthy"`
	Error        string         `json:"error,omitempty"`
	CircuitState string         `json:"circuit_state,omitempty"`
}

// SiteInfos returns a health-annotated snapshot of all registered sites.
// Health checks run concurrently; the call blocks until all complete.
// When a circuit breaker is configured, each SiteInfo also carries
// the current CircuitState for that site.
func (c *Coordinator) SiteInfos(ctx context.Context) []SiteInfo {
	c.mu.RLock()
	snapshot := c.snapshotSites()
	cb := c.cb
	c.mu.RUnlock()

	report := c.Health(ctx)

	infos := make([]SiteInfo, len(snapshot))
	for i, s := range snapshot {
		info := SiteInfo{Name: s.Name(), Role: s.Role(), Healthy: true}
		if err := report[s.Name()]; err != nil {
			info.Healthy = false
			info.Error = err.Error()
		}
		if cb != nil {
			info.CircuitState = cb.State(s.Name()).String()
		}
		infos[i] = info
	}
	return infos
}

// Replicate enqueues a direct replication of key from fromSite to toSite,
// bypassing the policy engine.  Both site names must be registered.
// The job is processed asynchronously by the background worker.
func (c *Coordinator) Replicate(ctx context.Context, key, fromSite, toSite string) error {
	c.mu.RLock()
	snapshot := c.snapshotSites()
	c.mu.RUnlock()

	var src, dst *site.SiteMount
	for _, s := range snapshot {
		switch s.Name() {
		case fromSite:
			src = s
		case toSite:
			dst = s
		}
	}
	if src == nil {
		return fmt.Errorf("coordinator: replicate: source site %q not found", fromSite)
	}
	if dst == nil {
		return fmt.Errorf("coordinator: replicate: destination site %q not found", toSite)
	}

	c.worker.Enqueue(replication.ReplicationJob{
		SourceSite: src,
		DestSite:   dst,
		Key:        key,
	})
	return nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// ReplicationQueueDepth returns the number of replication jobs currently
// waiting in the worker queue.
func (c *Coordinator) ReplicationQueueDepth() int {
	return c.worker.QueueDepth()
}

// IsLeader reports whether this coordinator instance is currently the active
// leader.  In single-node deployments (no lease manager configured) the
// coordinator is always the leader.
func (c *Coordinator) IsLeader() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leaseManager == nil || c.leaderLease != nil
}

// snapshotSites returns a copy of c.sites. Caller must hold at least RLock.
func (c *Coordinator) snapshotSites() []*site.SiteMount {
	cp := make([]*site.SiteMount, len(c.sites))
	copy(cp, c.sites)
	return cp
}

// preferHealthySites returns a reordered copy of sites where sites with a nil
// health-cache entry (healthy) appear before sites with a non-nil entry
// (degraded), preserving relative order within each group.
//
// When report is nil (cache not yet populated) the original slice is returned
// unchanged.  Degraded sites are placed last but never omitted, so they
// remain available as a fallback if the cache is stale or a site partially
// recovers.
func preferHealthySites(sites []*site.SiteMount, report map[string]error) []*site.SiteMount {
	if report == nil {
		return sites
	}
	healthy := make([]*site.SiteMount, 0, len(sites))
	degraded := make([]*site.SiteMount, 0)
	for _, s := range sites {
		if report[s.Name()] == nil {
			healthy = append(healthy, s)
		} else {
			degraded = append(degraded, s)
		}
	}
	if len(degraded) == 0 {
		return healthy
	}
	return append(healthy, degraded...)
}

// doWithRetry calls fn once when cfg is nil, or delegates to retry.Do when a
// retry configuration is set.  This keeps the call sites readable and avoids
// a nil-pointer dereference on the config.
func doWithRetry(ctx context.Context, cfg *retry.Config, fn func() error) error {
	if cfg == nil {
		return fn()
	}
	return retry.Do(ctx, *cfg, fn)
}

// filterByCircuitBreaker returns a filtered subset of sites whose circuits
// are not open.  For HalfOpen sites, Allow is called which marks them as
// probing so only one probe is in flight at a time.
//
// If cb is nil, or if every site's circuit is open, the original slice is
// returned unchanged so callers are never completely blocked by stale state.
func filterByCircuitBreaker(cb *circuitbreaker.Breaker, sites []*site.SiteMount) []*site.SiteMount {
	if cb == nil {
		return sites
	}
	allowed := make([]*site.SiteMount, 0, len(sites))
	for _, s := range sites {
		if cb.Allow(s.Name()) {
			allowed = append(allowed, s)
		}
	}
	if len(allowed) == 0 {
		// All circuits open — fall back to all sites to avoid blocking callers.
		return sites
	}
	return allowed
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

// makeJobID returns a deterministic store key for a pending replication job.
func makeJobID(sourceSite, destSite, key string) string {
	return sourceSite + ":" + destSite + ":" + key
}

// recoverPendingJobs reads all pending replication jobs from the store and
// re-enqueues them.  Called at Start time when a store is configured.
func (c *Coordinator) recoverPendingJobs(ctx context.Context, store metadata.Store) {
	jobs, err := store.GetPendingJobs(ctx)
	if err != nil {
		log.Printf("coordinator: recover pending jobs: %v", err)
		return
	}

	c.mu.RLock()
	siteMap := make(map[string]*site.SiteMount, len(c.sites))
	for _, s := range c.sites {
		siteMap[s.Name()] = s
	}
	c.mu.RUnlock()

	for _, j := range jobs {
		src, srcOK := siteMap[j.SourceSite]
		dst, dstOK := siteMap[j.DestSite]
		if !srcOK || !dstOK {
			log.Printf("coordinator: skip recovered job %q (site missing)", j.ID)
			continue
		}
		c.worker.Enqueue(replication.ReplicationJob{
			SourceSite: src,
			DestSite:   dst,
			Key:        j.Key,
			Size:       j.Size,
		})
	}
}

// drainWorkerEvents processes replication job events.
// It removes completed/failed jobs from the store (when set) and updates metrics.
// Runs in a goroutine until ctx is cancelled.
func (c *Coordinator) drainWorkerEvents(ctx context.Context, store metadata.Store) {
	for {
		select {
		case ev, ok := <-c.worker.Events():
			if !ok {
				return
			}
			if ev.Type == replication.EventCompleted || ev.Type == replication.EventFailed {
				if store != nil {
					id := makeJobID(ev.Job.SourceSite.Name(), ev.Job.DestSite.Name(), ev.Job.Key)
					if err := store.DeleteJob(ctx, id); err != nil {
						log.Printf("coordinator: delete job %q from store: %v", id, err)
					}
				}
				c.m.RecordReplication(string(ev.Type))
				c.m.SetReplicationQueueDepth(c.worker.QueueDepth())
			}
		case <-ctx.Done():
			return
		}
	}
}
