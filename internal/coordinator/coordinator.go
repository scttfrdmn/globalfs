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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	objectfserrors "github.com/scttfrdmn/objectfs/pkg/errors"
	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"
	objectfssdk "github.com/scttfrdmn/objectfs/sdks/go/objectfs"

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
	policy       *policy.Engine // never nil; default = empty engine
	worker       *replication.Worker
	store        metadata.Store   // optional; nil means no persistence
	m            *metrics.Metrics // optional; nil means no instrumentation
	storeCancel  context.CancelFunc
	storeWg      sync.WaitGroup
	startOnce    sync.Once          // ensures Start() body runs exactly once
	leaseManager *lease.Manager     // optional; nil means single-node mode
	leaderLease  *lease.Lease       // non-nil when this instance is the leader
	leaderCancel context.CancelFunc // cancels the leaderCtx
	leaseTTL     time.Duration      // 0 → defaultLeaseTTL

	// Background health polling.
	healthPollInterval time.Duration // 0 → use defaultHealthPollInterval
	healthCacheMu      sync.RWMutex
	healthCache        map[string]error // nil = not yet polled
	healthCheckedAt    time.Time

	// Circuit breaker (optional).
	cb *circuitbreaker.Breaker // nil = disabled

	// Per-site retry (optional).
	retryConfig *retry.Config // nil = single attempt (no retry)

	// Read-through object cache (optional).
	objCache *cache.Cache // nil = disabled

	// enqueueBackpressure bounds how long Put waits for replication queue room.
	// 0 → defaultEnqueueBackpressure.  Overridden in tests to keep them fast.
	enqueueBackpressure time.Duration
}

// defaultHealthPollInterval is the cadence for background site health checks
// when no explicit interval has been set via SetHealthPollInterval.
const defaultHealthPollInterval = 30 * time.Second

// defaultLeaseTTL is the leader-lease TTL used when none is configured.
const defaultLeaseTTL = 15 * time.Second

// defaultHealthTimeout is the maximum duration Health waits for all goroutines
// when the caller supplies a context without a deadline.
const defaultHealthTimeout = 30 * time.Second

// ErrNotFound reports that an object does not exist at any of the routed sites.
//
// Get and Head return an error wrapping this sentinel when every site they
// tried answered "no such key", as opposed to failing to answer at all.  The
// distinction matters to callers: "absent" is a normal answer that deserves a
// 404, while "every site errored" is a 502.  Before this existed the two were
// indistinguishable (#77).
//
// It wraps [objectfssdk.ErrNotFound], so both of these hold for the error
// returned by Get:
//
//	errors.Is(err, coordinator.ErrNotFound)
//	errors.Is(err, objectfssdk.ErrNotFound)
var ErrNotFound = fmt.Errorf("object not found at any site: %w", objectfssdk.ErrNotFound)

// ErrReplicationNotQueued reports that a Put stored the data on its primary
// sites but could not queue replication to one or more secondaries, because the
// replication queue was still full after the backpressure budget elapsed.
//
// A Put that returns an error wrapping this sentinel is a *partial success* and
// must not be read as a failed write:
//
//   - The bytes are durably stored on every primary in the routed set.  A
//     synchronous write failure returns a different error and never reaches
//     this point.
//   - Secondary sites named in the message do not have the data and no
//     background work will deliver it, unless a metadata store is configured —
//     in which case the persisted job is recovered on the next start.
//
// Retrying the identical Put is safe and is the intended response: the primary
// write is idempotent and the coordinator-level dedup skips destinations that
// already hold the content.
//
// The returned error also wraps the underlying cause — the worker's queue-full
// error, or the context error if the caller's context ended while waiting for
// room — so errors.Is finds both this sentinel and the reason.
//
// Before this existed, a full queue was logged at warn level and Put returned
// nil, so callers were told a write was replicated when it was not — 31 of 40
// Puts were dropped at the shipped defaults with every one reporting success
// (#79).
var ErrReplicationNotQueued = errors.New("replication not queued")

// defaultEnqueueBackpressure is how long Put will wait for room in the
// replication queue before giving up and reporting ErrReplicationNotQueued.
//
// It exists because the queue is much smaller than it looks and the worker is
// serial: the shipped daemon passes Performance.MaxConcurrentTransfers (default
// 8) to SetWorkerQueueDepth, so a burst of writes fills it almost immediately
// even though each job takes only as long as one GET plus one PUT.  Waiting
// briefly converts a burst into a queue rather than into data loss.
//
// The budget is deliberately short and deliberately finite.  Unbounded blocking
// would make the async path synchronous under load and let one unreachable
// destination stall every writer; returning immediately, as the code did before,
// loses the write.  A couple of seconds spans a burst without hiding a genuine
// backlog, which is what the error and the dropped counter are for.
const defaultEnqueueBackpressure = 2 * time.Second

// enqueueRetryInterval is the poll interval used while waiting for queue room.
// The worker exposes no "room available" signal, so this polls Enqueue.
const enqueueRetryInterval = 5 * time.Millisecond

// isSiteFailure reports whether err is evidence that the site itself is unwell,
// as opposed to an ordinary answer to an ordinary request.
//
// The circuit breaker needs this distinction and cannot make it from the fact
// that an error occurred.  A missing key means the site is up, reachable,
// authenticating and answering correctly — counting it as a failure is how five
// lookups of absent keys ejected a healthy site from routing for the whole
// cooldown (#77).
//
// The authority is objectfs's [objectfserrors.IsServiceFailure], the same
// function objectfs's own health tracker and circuit breaker consult, so
// GlobalFS cannot drift from the classification objectfs makes one layer down.
// It matches on the error *code*, so a backend error wrapped several times over
// still resolves.
//
// An error carrying no objectfs code counts as a failure.  That direction is
// deliberate and matches objectfs: the failure mode of misclassifying something
// unknown is a breaker that opens too eagerly and recovers on its cooldown, not
// one that never notices an outage.
func isSiteFailure(err error) bool {
	if err == nil {
		return false
	}

	// A caller who withdrew the request says nothing about the site.  objectfs
	// classifies this as ErrCodeOperationCanceled (not a failure), but GlobalFS
	// produces the bare context error itself — retry.Do returns ctx.Err()
	// directly — so it arrives without an objectfs code and has to be named here.
	if errors.Is(err, context.Canceled) {
		return false
	}

	var objErr *objectfserrors.ObjectFSError
	if errors.As(err, &objErr) {
		return objectfserrors.IsServiceFailure(objErr.Code)
	}
	return true
}

// recordSiteResult updates the circuit breaker for one site attempt.
//
// A non-failure error records a *success*: the site was reached and it answered,
// which is exactly what the breaker is trying to measure.  This mirrors
// objectfs's defaultIsSuccessful.  No-op when cb is nil.
func recordSiteResult(cb *circuitbreaker.Breaker, name string, err error) {
	if cb == nil {
		return
	}
	if isSiteFailure(err) {
		cb.RecordFailure(name)
		return
	}
	cb.RecordSuccess(name)
}

// ── Metrics helpers ───────────────────────────────────────────────────────────
// These wrappers centralise the nil check so call sites stay clean.

func (c *Coordinator) metricsCacheHit() {
	if c.m != nil {
		c.m.RecordCacheHit()
	}
}
func (c *Coordinator) metricsCacheMiss() {
	if c.m != nil {
		c.m.RecordCacheMiss()
	}
}
func (c *Coordinator) metricsCacheEviction() {
	if c.m != nil {
		c.m.RecordCacheEviction()
	}
}
func (c *Coordinator) metricsCacheBytes(n int64) {
	if c.m != nil {
		c.m.SetCacheBytes(n)
	}
}
func (c *Coordinator) metricsSiteCount(n int) {
	if c.m != nil {
		c.m.SetSiteCount(n)
	}
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

// SetLeaseTTL sets the TTL used when acquiring the distributed leader lease.
// When 0 (the default), defaultLeaseTTL (15 s) is used.
// Must be called before Start.
func (c *Coordinator) SetLeaseTTL(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseTTL = d
}

// SetWorkerQueueDepth configures the replication worker queue depth.
// Must be called before Start; has no effect if Start has already been called.
// Pass 0 to retain the existing depth (default 512).
//
// Note that this is a *queue depth*, not a concurrency limit — the worker is a
// single serial goroutine.  The shipped daemon passes
// Performance.MaxConcurrentTransfers here, which conflates the two and is why
// the effective queue is 8 rather than 512 at the defaults.  Renaming that
// setting is a config change and belongs elsewhere; Put now applies
// backpressure rather than dropping writes, so the small depth costs latency
// under a burst instead of data (#79).
func (c *Coordinator) SetWorkerQueueDepth(depth int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if depth > 0 {
		c.worker = replication.NewWorker(depth)
	}
}

// SetEnqueueBackpressure bounds how long Put waits for room in the replication
// queue before returning an error wrapping ErrReplicationNotQueued.
//
// Pass 0 to use defaultEnqueueBackpressure (2 s).  A negative value disables
// waiting entirely: Put attempts one Enqueue and reports the drop immediately.
// May be called at any time; safe for concurrent use.
func (c *Coordinator) SetEnqueueBackpressure(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enqueueBackpressure = d
}

// SetCircuitBreaker registers a circuit breaker with the coordinator.
//
// When set, sites whose circuit is open are skipped during read routing (Get,
// Head).  Write operations (Put, Delete) always target their designated sites
// but record success/failure to keep the breaker state current.
//
// Only errors that are evidence the site itself is unwell count as failures;
// see isSiteFailure.  A missing key is an ordinary answer and records a success.
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
	leaseTTL := c.leaseTTL
	pollInterval := c.healthPollInterval
	c.mu.Unlock()

	// workerCtx is cancelled when the lease is lost (if a lease manager is set).
	workerCtx := ctx

	if mgr != nil {
		if leaseTTL <= 0 {
			leaseTTL = defaultLeaseTTL
		}
		l, acquired, err := mgr.TryAcquire(ctx, "coordinator/leader", leaseTTL)
		if err != nil {
			slog.Warn("coordinator: acquire leader lease; running in standby mode", "error", err)
			return
		}
		if !acquired {
			slog.Info("coordinator: another instance holds the leader lease; running in standby mode")
			return
		}
		slog.Info("coordinator: acquired leader lease")

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
				slog.Warn("coordinator: lost leader lease; transitioning to standby mode")
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
	// pollInterval was read from c.healthPollInterval under c.mu at the top.
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
//
// # Teardown order
//
// The producer is stopped before its consumer.  worker.Stop waits for the
// in-flight transfer to finish, and that transfer emits a terminal event; if the
// drain goroutine is torn down first there is nobody left to read it, and the
// job stays in the store as a phantom while its content hash is lost (#78).
func (c *Coordinator) Stop() {
	c.mu.Lock()
	storeCancel := c.storeCancel
	leaderCancel := c.leaderCancel
	l := c.leaderLease
	store := c.store
	c.mu.Unlock()

	// 1. Stop the producer.  Returns once the in-flight job has settled, so
	//    every terminal event it will ever emit is buffered by the time this
	//    returns.  The events channel is buffered to the queue depth, so emit
	//    cannot block even with no reader currently scheduled.
	c.worker.Stop()

	// 2. Now the consumer.  Cancelling these stops the drain goroutine, the
	//    health poller, and the lease keepalive.
	//
	//    Note for #83: leaderCancel used to run first, and because leaderCtx is
	//    the worker's ctx that gave the in-flight transfer a cancellation path.
	//    It no longer does — deliberately, since aborting the transfer is the
	//    opposite of letting it settle.  Bounding a transfer against an
	//    unresponsive site is #83's job and wants a real timeout; it was never
	//    covered in the no-lease-manager case anyway, which is every shipped
	//    deployment.  cmd/coordinator cancels the root context before calling
	//    Close, so the transfer ctx is already done by the time Stop is entered.
	if leaderCancel != nil {
		leaderCancel()
	}
	if storeCancel != nil {
		storeCancel()
	}
	c.storeWg.Wait()

	// 3. Final flush on this goroutine.  storeWg.Wait above guarantees the drain
	//    goroutine has returned, so there is no concurrent reader.
	//
	//    Step 1 plus the drain's own flush covers the common case, but not the
	//    one the shipped daemon actually produces: cmd/coordinator cancels the
	//    root context and *then* calls Close, so the drain can already have
	//    exited on ctx.Done before Stop is ever entered — and the same is true
	//    after a lost leader lease, which cancels leaderCtx with no Stop
	//    involved.  Flushing here makes the guarantee independent of who
	//    cancelled what first, which is worth more than the one non-blocking
	//    channel read it costs when the buffer is empty.
	c.flushWorkerEvents(store)

	// Release the leader lease last so a standby can take over quickly.
	if l != nil {
		if err := l.Release(); err != nil {
			slog.Warn("coordinator: release leader lease", "error", err)
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

// AddSite appends a site at the lowest priority, without checking whether its
// name is already taken.
//
// Prefer [Coordinator.AddSiteUnique] anywhere the name comes from outside the
// process.  Site names are the only handle every other operation has on a site —
// RemoveSite, Replicate and the health report all address sites by name — so two
// sites sharing one is an ambiguity none of them can resolve (#80).  Config
// loading rejects duplicates; the HTTP API historically did not.
func (c *Coordinator) AddSite(s *site.SiteMount) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sites = append(c.sites, s)
	c.ns.AddSite(s)
	c.metricsSiteCount(len(c.sites))
}

// ErrDuplicateSite reports that a site with the requested name is already
// registered.  An HTTP layer should map it to 409 Conflict.
var ErrDuplicateSite = errors.New("site name already registered")

// AddSiteUnique appends a site at the lowest priority, or returns an error
// wrapping [ErrDuplicateSite] if a site with the same name is already
// registered.  The site is left untouched — not closed — when rejected, since
// the caller constructed it and is better placed to decide.
//
// The check and the append happen under one lock hold, so a caller cannot be
// beaten to the name between deciding and acting; the same reason RemoveSite
// reports a bool instead of exposing an existence check (#58).
func (c *Coordinator) AddSiteUnique(s *site.SiteMount) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, existing := range c.sites {
		if existing.Name() == s.Name() {
			return fmt.Errorf("coordinator: add site %q: %w", s.Name(), ErrDuplicateSite)
		}
	}
	c.sites = append(c.sites, s)
	c.ns.AddSite(s)
	c.metricsSiteCount(len(c.sites))
	return nil
}

// RemoveSite removes the named site, closes it, and reports whether it was
// found.  If no site has that name this is a no-op returning false.
//
// Returning a bool allows callers to distinguish "not found" from "removed"
// atomically, eliminating the TOCTOU race in the HTTP handler (#58).
//
// Exactly one site is removed per call, even if several share the name: the
// highest-priority match goes and the rest stay, so a second call removes the
// next one.  The loop used to run to completion, keeping only the *last* match
// while filtering out *all* of them — one DELETE emptied the whole set and closed
// one site, leaking the S3 connection pools of the others and reintroducing the
// leak #62 fixed by another route (#80).  Duplicate names should not exist
// (AddSiteUnique and config validation both refuse them), but this is the
// function whose contract that leak fix rests on, so it does not assume it.
func (c *Coordinator) RemoveSite(name string) bool {
	var removed *site.SiteMount
	c.mu.Lock()
	for i, s := range c.sites {
		if s.Name() != name {
			continue
		}
		removed = s
		remaining := make([]*site.SiteMount, 0, len(c.sites)-1)
		remaining = append(remaining, c.sites[:i]...)
		remaining = append(remaining, c.sites[i+1:]...)
		c.sites = remaining
		break
	}
	if removed == nil {
		c.mu.Unlock()
		return false
	}
	c.ns = namespace.New(c.sites...)
	c.metricsSiteCount(len(c.sites))
	c.mu.Unlock()

	// Close outside the lock: Close talks to the network and can block, and
	// holding c.mu across it stalls every read and write on the coordinator.
	// Keep it that way (#95 exists because Close does not).
	if err := removed.Close(); err != nil {
		slog.Warn("coordinator: RemoveSite close", "site", name, "error", err)
	}
	return true
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
//
// If ctx carries no deadline, a defaultHealthTimeout deadline is imposed so
// that per-site goroutines cannot block indefinitely on unreachable sites.
func (c *Coordinator) Health(ctx context.Context) map[string]error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultHealthTimeout)
		defer cancel()
	}

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
//
// When every site tried answered "no such key", the returned error wraps
// [ErrNotFound].  Callers should test for that before treating a failure as an
// outage; a missing object is an answer, not a failure (#77).
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	snapshot, pol, cb, retryCfg, oc := c.snapshotSites(), c.policy, c.cb, c.retryConfig, c.objCache
	c.mu.RUnlock()

	// Cache read-through: serve from cache when available.
	if oc != nil {
		if cached, ok := oc.Get(key); ok {
			c.metricsCacheHit()
			c.metricsCacheBytes(oc.Stats().Bytes)
			return cached, nil
		}
		c.metricsCacheMiss()
	}

	ordered, err := pol.Route(policy.OperationRead, key, snapshot)
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
	// allNotFound stays true while every site tried has answered "no such key".
	// It distinguishes "the object does not exist" from "no site could answer",
	// which the API layer needs to tell a 404 from a 502 (#77).
	allNotFound := true
	for _, s := range ordered {
		var data []byte
		siteErr := doWithRetry(ctx, retryCfg, func() error {
			var err error
			data, err = s.Get(ctx, key, 0, 0)
			return err
		})
		recordSiteResult(cb, s.Name(), siteErr)
		if siteErr != nil && !errors.Is(siteErr, objectfssdk.ErrNotFound) {
			allNotFound = false
		}
		if siteErr == nil {
			// Populate cache on successful site fetch.
			if oc != nil {
				evicted := oc.PutAndRecordEvictions(key, data)
				c.metricsCacheBytes(oc.Stats().Bytes)
				for i := int64(0); i < evicted; i++ {
					c.metricsCacheEviction()
				}
			}
			return data, nil
		}
		lastErr = siteErr
	}
	if allNotFound {
		return nil, fmt.Errorf("coordinator: Get %q: %w", key, ErrNotFound)
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
//
// # Replication backpressure
//
// If the replication queue is full, Put waits up to the backpressure budget
// (see SetEnqueueBackpressure) for room.  If it is still full, Put returns an
// error wrapping [ErrReplicationNotQueued] — a *partial success*: the data is
// durably stored on the primaries but the named secondaries did not get a job.
// Retrying the same Put is safe and is the intended response.  Callers that only
// care about the primary write should test for that sentinel rather than
// treating the error as a failed write (#79).
//
// An HTTP layer in front of this should map ErrReplicationNotQueued to 202
// Accepted (the object exists; replication is incomplete), not to 5xx: the
// object is retrievable immediately afterwards.  cmd/coordinator currently maps
// every Put error to 502, which is wrong for this case and is tracked separately.
func (c *Coordinator) Put(ctx context.Context, key string, data []byte) error {
	c.mu.RLock()
	snapshot, pol, store, cb, oc := c.snapshotSites(), c.policy, c.store, c.cb, c.objCache
	m, backpressure := c.m, c.enqueueBackpressure
	c.mu.RUnlock()

	routed, err := pol.Route(policy.OperationWrite, key, snapshot)
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
			recordSiteResult(cb, s.Name(), err)
			return fmt.Errorf("coordinator: Put %q to %q: %w", key, s.Name(), err)
		}
		recordSiteResult(cb, s.Name(), nil)
	}

	// Enqueue async replication to remaining sites using the first primary
	// (or promoted site) as the GET source.
	//
	// Persist to the store BEFORE enqueueing so that the job is durable before
	// the worker can complete it.  If the order were reversed, a fast worker
	// could complete the job and drainWorkerEvents could call DeleteJob before
	// PutReplicationJob runs, leaving a phantom entry in the store.
	// notQueued collects destinations whose replication job could not be queued,
	// so Put can report them all rather than only the first or none at all.
	// notQueuedCause keeps the first underlying reason (queue full, or the
	// context error) so the caller can distinguish "we gave up waiting" from
	// "you cancelled" with errors.Is.
	var notQueued []string
	var notQueuedCause error
	if len(primaries) > 0 {
		src := primaries[0]
		// Compute content hash once for coordinator-level dedup below.
		rawHash := sha256.Sum256(data)
		contentHash := hex.EncodeToString(rawHash[:])
		for _, s := range others {
			// Coordinator-level dedup: skip enqueue if the destination already
			// holds the exact same content, as recorded by a previous transfer.
			if store != nil {
				if rec, _ := store.GetReplicatedObject(ctx, s.Name(), key); rec != nil {
					if rec.ContentHash == contentHash {
						slog.Debug("coordinator: skipping replication, dest already has content",
							"key", key, "dest", s.Name())
						continue
					}
				}
			}
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
					slog.Error("coordinator: persist job; skipping enqueue to preserve durability guarantee", "job_id", metaJob.ID, "error", persistErr)
					continue
				}
			}
			if enqErr := c.enqueueWithBackpressure(ctx, backpressure, replication.ReplicationJob{
				SourceSite: src,
				DestSite:   s,
				Key:        key,
				Size:       int64(len(data)),
			}); enqErr != nil {
				// The job stays in the store when one is configured, so a restart
				// recovers it.  With no store — every shipped deployment today —
				// the write is simply not replicated, which is why this is an
				// error to the caller and a counter rather than a log line.
				m.RecordReplicationDropped()
				slog.Error("coordinator: replication queue full; write not replicated",
					"key", key, "dest", s.Name(), "queue_depth", c.worker.QueueDepth(), "error", enqErr)
				notQueued = append(notQueued, s.Name())
				if notQueuedCause == nil {
					notQueuedCause = enqErr
				}
				// A dead context will not recover for any later destination, and
				// polling each one for the full budget would multiply the delay.
				if ctx.Err() != nil {
					break
				}
			}
		}
	}

	// Invalidate the cache so the next Get fetches the freshly-written value.
	// Done before the partial-success return: the primary write happened, so a
	// cached copy of the old value is stale either way.
	if oc != nil {
		oc.Delete(key)
		c.metricsCacheBytes(oc.Stats().Bytes)
	}

	if len(notQueued) > 0 {
		return fmt.Errorf("coordinator: Put %q: stored on primaries but %w for %v: %w",
			key, ErrReplicationNotQueued, notQueued, notQueuedCause)
	}
	return nil
}

// enqueueWithBackpressure submits job to the replication worker, waiting up to
// budget for queue room.  A budget of 0 means defaultEnqueueBackpressure; a
// negative budget means a single attempt with no wait.
//
// Returns nil once the job is queued, or the worker's queue-full error if the
// budget elapses.  A cancelled ctx aborts the wait and returns ctx.Err().
func (c *Coordinator) enqueueWithBackpressure(ctx context.Context, budget time.Duration, job replication.ReplicationJob) error {
	err := c.worker.Enqueue(job)
	if err == nil || budget < 0 {
		return err
	}
	if budget == 0 {
		budget = defaultEnqueueBackpressure
	}

	// The queue is full.  Poll for room: the worker exposes no readiness signal,
	// and a job leaves the queue as soon as the serial worker picks it up, so the
	// wait is usually one transfer long rather than the whole budget.
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	ticker := time.NewTicker(enqueueRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return err // the last queue-full error
		case <-ticker.C:
			if err = c.worker.Enqueue(job); err == nil {
				return nil
			}
		}
	}
}

// Delete removes the object at key from sites in the policy-routed set.
//
// Primary site deletes are synchronous and return errors on failure.
// Non-primary deletes are best-effort: errors are logged but not returned.
// The cache entry for key is invalidated regardless of per-site outcome.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	c.mu.RLock()
	snapshot, pol, cb, oc := c.snapshotSites(), c.policy, c.cb, c.objCache
	c.mu.RUnlock()

	routed, err := pol.Route(policy.OperationDelete, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Delete %q: policy error: %w", key, err)
	}

	primaries, others := partitionByRole(routed)

	for _, s := range primaries {
		if err := s.Delete(ctx, key); err != nil {
			recordSiteResult(cb, s.Name(), err)
			return fmt.Errorf("coordinator: Delete %q from primary %q: %w", key, s.Name(), err)
		}
		recordSiteResult(cb, s.Name(), nil)
	}
	for _, s := range others {
		err := s.Delete(ctx, key)
		recordSiteResult(cb, s.Name(), err)
		if err != nil {
			slog.Warn("coordinator: Delete from non-primary site", "key", key, "site", s.Name(), "error", err)
		}
	}

	// Invalidate the cache entry whether or not site deletes succeeded.
	if oc != nil {
		oc.Delete(key)
		c.metricsCacheBytes(oc.Stats().Bytes)
	}
	return nil
}

// List returns up to limit objects under prefix, merged across policy-routed sites.
//
// The policy engine is consulted using the prefix as the key and
// OperationRead as the operation type.  Healthy sites are promoted and open
// circuit breakers are filtered before the Namespace merge so that routing
// rules, health state, and breaker state all apply consistently to List —
// the same way they do for Get and Head.
// Pass limit ≤ 0 to retrieve all matching objects.
func (c *Coordinator) List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	c.mu.RLock()
	snapshot, pol, cb := c.snapshotSites(), c.policy, c.cb
	c.mu.RUnlock()

	// Apply policy routing using the prefix as the key.
	// List is treated as a read operation for routing purposes.
	routed, err := pol.Route(policy.OperationRead, prefix, snapshot)
	if err != nil {
		return nil, fmt.Errorf("coordinator: List %q: policy error: %w", prefix, err)
	}

	healthReport, _ := c.HealthStatus()
	routed = preferHealthySites(routed, healthReport)
	routed = filterByCircuitBreaker(cb, routed)

	// Merge from the policy-selected sites only.
	ns := namespace.New(routed...)
	return ns.List(ctx, prefix, limit)
}

// Head returns metadata for the object at key.
// Sites are checked in policy-routed order with healthy sites promoted to the
// front (same health-aware reordering as Get).  The first hit is returned.
//
// As with Get, the returned error wraps [ErrNotFound] when every site answered
// "no such key" rather than failing to answer.
func (c *Coordinator) Head(ctx context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	c.mu.RLock()
	snapshot, pol, cb, retryCfg := c.snapshotSites(), c.policy, c.cb, c.retryConfig
	c.mu.RUnlock()

	ordered, err := pol.Route(policy.OperationRead, key, snapshot)
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
	allNotFound := true
	for _, s := range ordered {
		var info *objectfstypes.ObjectInfo
		siteErr := doWithRetry(ctx, retryCfg, func() error {
			var err error
			info, err = s.Head(ctx, key)
			return err
		})
		recordSiteResult(cb, s.Name(), siteErr)
		if siteErr != nil && !errors.Is(siteErr, objectfssdk.ErrNotFound) {
			allNotFound = false
		}
		if siteErr == nil {
			return info, nil
		}
		lastErr = siteErr
	}
	if allNotFound {
		return nil, fmt.Errorf("coordinator: Head %q: %w", key, ErrNotFound)
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

	if err := c.worker.Enqueue(replication.ReplicationJob{
		SourceSite: src,
		DestSite:   dst,
		Key:        key,
	}); err != nil {
		return fmt.Errorf("coordinator: replicate: %w", err)
	}
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
//
// A "no such key" answer is never retried.  It is not transient: the site
// answered, and it will answer the same way three times.  Retrying cost three
// round trips and two backoff sleeps per absent key before #77 was fixed.
//
// Other error classes are still retried even when isSiteFailure says they are
// not the site's fault.  Retryable and service-failure are separate questions
// — objectfs makes the same split — and only not-found is unambiguously
// pointless to repeat.
func doWithRetry(ctx context.Context, cfg *retry.Config, fn func() error) error {
	if cfg == nil {
		return fn()
	}
	// retry.Do has no early-exit hook, so a terminal error is stashed here and
	// the wrapper reports nil to break the loop.  The stashed error is what the
	// caller sees, so the "success" is never observable outside this function.
	var terminal error
	err := retry.Do(ctx, *cfg, func() error {
		fnErr := fn()
		if fnErr != nil && errors.Is(fnErr, objectfssdk.ErrNotFound) {
			terminal = fnErr
			return nil
		}
		return fnErr
	})
	if terminal != nil {
		return terminal
	}
	return err
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
		slog.Error("coordinator: recover pending jobs", "error", err)
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
			slog.Warn("coordinator: skip recovered job (site missing)", "job_id", j.ID)
			continue
		}
		if err := c.worker.Enqueue(replication.ReplicationJob{
			SourceSite: src,
			DestSite:   dst,
			Key:        j.Key,
			Size:       j.Size,
		}); err != nil {
			slog.Warn("coordinator: recover job enqueue", "job_id", j.ID, "error", err)
		}
	}
}

// drainWorkerEvents processes replication job events.
// It removes completed/failed jobs from the store (when set) and updates metrics.
// Runs in a goroutine until ctx is cancelled.
//
// On cancellation it flushes whatever is already buffered before returning.
// Exiting on the first ctx.Done would discard events the worker has already
// emitted, which is the same loss #78 describes in a narrower window: the
// buffered EventCompleted of a transfer that finished a moment before shutdown.
func (c *Coordinator) drainWorkerEvents(ctx context.Context, store metadata.Store) {
	for {
		select {
		case ev, ok := <-c.worker.Events():
			if !ok {
				return
			}
			c.handleWorkerEvent(ev, store)
		case <-ctx.Done():
			c.flushWorkerEvents(store)
			return
		}
	}
}

// flushWorkerEvents processes every event currently buffered in the worker's
// events channel and returns as soon as it is empty.  It never blocks waiting
// for an event that has not been emitted yet.
//
// Callers must ensure no other goroutine is reading Events() concurrently, or
// the two will split the buffer between them.
func (c *Coordinator) flushWorkerEvents(store metadata.Store) {
	for {
		select {
		case ev, ok := <-c.worker.Events():
			if !ok {
				return
			}
			c.handleWorkerEvent(ev, store)
		default:
			return
		}
	}
}

// handleWorkerEvent applies one replication event to the store and to metrics.
// Non-terminal events (EventStarted) are ignored.
func (c *Coordinator) handleWorkerEvent(ev replication.ReplicationEvent, store metadata.Store) {
	if ev.Type != replication.EventCompleted && ev.Type != replication.EventFailed {
		return
	}

	if store != nil {
		id := makeJobID(ev.Job.SourceSite.Name(), ev.Job.DestSite.Name(), ev.Job.Key)
		// Use a fresh context: the drain's ctx may already be cancelled if
		// Stop() fired while this event was being processed.  A cancelled ctx
		// would leave the job in the store and cause it to be re-enqueued on
		// the next restart (#61).
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.DeleteJob(deleteCtx, id)
		deleteCancel()
		if err != nil {
			slog.Warn("coordinator: delete job from store", "job_id", id, "error", err)
		}
	}
	// Record content hash on successful transfer for future coordinator-level dedup.
	if store != nil && ev.Type == replication.EventCompleted && ev.ContentHash != "" {
		recCtx, recCancel := context.WithTimeout(context.Background(), 5*time.Second)
		recErr := store.PutReplicatedObject(recCtx, &metadata.ReplicatedObject{
			Site:         ev.Job.DestSite.Name(),
			Key:          ev.Job.Key,
			ContentHash:  ev.ContentHash,
			ReplicatedAt: time.Now(),
		})
		recCancel()
		if recErr != nil {
			slog.Warn("coordinator: record replicated object", "key", ev.Job.Key, "error", recErr)
		}
	}

	c.mu.RLock()
	m := c.m
	c.mu.RUnlock()
	if m != nil {
		m.RecordReplication(string(ev.Type))
		m.SetReplicationQueueDepth(c.worker.QueueDepth())
	}
}
