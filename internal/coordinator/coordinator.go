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
//   - Delete: synchronous on every routed site, primaries first; returns nil
//     only if the object is gone everywhere, otherwise an error naming the sites
//     that still hold it (#87).
//   - List: delegates to the embedded Namespace (priority-merge, no policy).
//
// # Lifecycle
//
// A Coordinator is **single-use**.  It moves through three states, in one
// direction only, and never returns to an earlier one:
//
//	created --Start--> running --Stop--> stopped
//	created ---------------Stop--------> stopped
//
// The rules that follow from that, all enforced rather than merely documented:
//
//   - **Start is idempotent and reports failure.**  It returns nil on the call
//     that starts the coordinator (or that enters standby, which is a successful
//     start with no worker), and nil again for every later call on a running
//     coordinator.  It returns an error wrapping [ErrStopped] on a stopped one.
//   - **Stop before Start is legal and terminal.**  It is a no-op on goroutines
//     that do not exist, but it *does* move the coordinator to stopped, so a
//     later Start fails loudly instead of running with replication silently off.
//     Callers that stop defensively during a failed boot get an error they can
//     act on rather than a healthy-looking coordinator that replicates nothing
//     (#84).
//   - **Stop-then-Start is illegal**, and the error says so.  Restarting would
//     mean rebuilding the worker, its queue, its done channel and every
//     in-flight job's context; a fresh [New] is the supported way to get a
//     working coordinator, and it costs nothing but the site list.
//   - **Start and Stop are ordered with respect to each other.**  Both transition
//     the same `state` field under `c.mu`, so a Stop racing a Start either loses
//     (Start wins, Stop then tears down what Start built) or wins (Start is
//     refused and launches nothing).  It cannot land in between and leave
//     goroutines nobody can cancel, which is what a nil `storeCancel` plus a
//     zero `storeWg` used to allow — 59 of 60 races leaked a drain goroutine
//     (#82).
//   - **Configuration is frozen at Start.**  Every Set* method that Start reads
//     once — SetStore, SetMetrics, SetWorkerQueueDepth, SetLeaseManager,
//     SetLeaseTTL, SetHealthPollInterval — returns an error wrapping
//     [ErrStarted] if called after Start, instead of mutating state the running
//     goroutines have already copied (#85, #86).  The genuinely dynamic
//     knobs — SetPolicy, SetCache, SetCircuitBreaker, SetRetryConfig,
//     SetEnqueueBackpressure — stay callable at any time and return nothing.
//
// Stop and Close wait for the in-flight replication transfer to settle, which is
// unbounded if the destination site is unresponsive.  Use [Coordinator.StopContext]
// or [Coordinator.CloseContext] to bound that wait; a caller that must terminate
// (a SIGTERM handler) should always use the bounded form (#83).
//
// Coordinator is safe for concurrent use.
package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
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
	leaseManager *lease.Manager     // optional; nil means single-node mode
	leaderLease  *lease.Lease       // non-nil when this instance is the leader
	leaderCancel context.CancelFunc // cancels the leaderCtx
	leaseTTL     time.Duration      // 0 → defaultLeaseTTL

	// state is the coordinator's position in its one-directional lifecycle; see
	// the package Lifecycle doc.  It replaced a sync.Once plus the nil-ness of
	// storeCancel and the zero-ness of a shared WaitGroup, which between them
	// could not order Start against Stop at all (#82, #84, #85).  Guarded by
	// c.mu, the same lock every Set* method takes, so "has Start read this yet?"
	// is one question with one answer rather than a per-field guess.
	state coordState

	// lifecycleMu serializes a whole Start against a whole Stop.
	//
	// c.mu cannot do this job: Start must release it across the lease
	// acquisition and across recoverPendingJobs, both of which do I/O, and it is
	// also the lock every in-flight Get and Put takes — holding it for the
	// duration of a shutdown is exactly the availability problem #95 is about.
	// A separate lifecycle lock keeps Start and Stop mutually exclusive without
	// making either of them exclude ordinary traffic.
	//
	// Ordering rule: acquire lifecycleMu before c.mu, never the reverse.
	lifecycleMu sync.Mutex

	// drainWg tracks the worker-event drain goroutine; healthWg tracks the health
	// poller.  Separate because Stop has to distinguish them: the final event
	// flush is safe only once the drain has returned, and a health poller wedged
	// in a probe that ignores its context must not be able to veto that flush.
	// One shared WaitGroup made a stuck probe cost buffered replication events.
	drainWg  sync.WaitGroup
	healthWg sync.WaitGroup

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

// coordState is the coordinator's position in its one-directional lifecycle.
// See the package Lifecycle doc for the contract it enforces.
type coordState int

const (
	// coordCreated is a Coordinator that has never been started.
	coordCreated coordState = iota
	// coordRunning is a Coordinator whose Start has been accepted.  Standby
	// counts: the lease was lost or held elsewhere, so no worker runs, but the
	// coordinator started successfully and Stop still has work to undo.
	coordRunning
	// coordStopped is terminal.  Start on it returns ErrStopped.
	coordStopped
)

func (s coordState) String() string {
	switch s {
	case coordCreated:
		return "created"
	case coordRunning:
		return "running"
	case coordStopped:
		return "stopped"
	default:
		return fmt.Sprintf("coordState(%d)", int(s))
	}
}

// ErrStarted reports that a configuration call arrived after Start, at which
// point the value it sets has already been read and copied by the background
// goroutines.  Such a call is a no-op, logged at Error; the sentinel exists so a
// caller that wants to check first can, and so the message has one spelling.
var ErrStarted = errors.New("coordinator already started")

// ErrStopped reports that Start was called on a stopped Coordinator.
//
// A Coordinator is single-use: Stop is terminal even when it ran before Start
// ever did.  A caller that stops defensively during a failed boot and then
// starts must construct a new Coordinator with [New]; the alternative — a Start
// that appears to succeed while replication is off for the process lifetime — is
// the failure #84 describes, and this error is what makes it visible.
var ErrStopped = errors.New("coordinator stopped; a Coordinator is single-use")

// defaultStopTimeout bounds Stop and Close when the caller supplies no deadline
// of their own.
//
// Unbounded was the previous behaviour and it is how a single unresponsive S3
// endpoint kept the process alive past its SIGTERM grace period until the
// orchestrator SIGKILLed it, discarding the in-memory replication queue (#83).
// 30 s is chosen to sit inside a typical 60 s terminationGracePeriodSeconds with
// room for the HTTP drain that precedes it, and to be long enough that a
// genuinely slow-but-progressing transfer finishes rather than being abandoned.
// Callers that know their own budget should pass it via StopContext/CloseContext.
const defaultStopTimeout = 30 * time.Second

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
//
// Every *metrics.Metrics method is nil-safe on the receiver, so these wrappers
// exist purely to funnel every read of c.m through metrics(), which does it under
// the lock.  Reading the field directly was the race in #86: SetMetrics writes it
// under c.mu, and an unsynchronized read of an interface-shaped field can observe
// a non-nil type word with a stale data word, so the nil check passes and the call
// dereferences garbage.  The nil checks the wrappers used to perform are gone
// because they were never the load-bearing part — Metrics does them itself.

// metrics returns the registered Metrics, or nil.  The nil is safe to call
// methods on; every *metrics.Metrics method returns early on a nil receiver.
func (c *Coordinator) metrics() *metrics.Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m
}

func (c *Coordinator) metricsCacheHit()          { c.metrics().RecordCacheHit() }
func (c *Coordinator) metricsCacheMiss()         { c.metrics().RecordCacheMiss() }
func (c *Coordinator) metricsCacheEviction()     { c.metrics().RecordCacheEviction() }
func (c *Coordinator) metricsCacheBytes(n int64) { c.metrics().SetCacheBytes(n) }

// metricsSiteCountLocked records the site count.  Unlike its siblings this one is
// called from AddSite/RemoveSite with c.mu already held for writing, so it reads
// the field directly — taking RLock here would deadlock on Go's non-reentrant
// RWMutex.
func (c *Coordinator) metricsSiteCountLocked(n int) { c.m.SetSiteCount(n) }

// workerRef returns the replication worker under the read lock.
//
// SetWorkerQueueDepth writes c.worker under c.mu; Put, Replicate,
// ReplicationQueueDepth and the event drain all used to read it without any lock,
// which is a pointer race regardless of how benign the timing looks — the second
// half of #85, and confirmed by -race on the exported
// ReplicationQueueDepth/SetWorkerQueueDepth pair alone.  Gating the setter on
// started-state narrows the window but does not close it: two goroutines can still
// configure and read a *created* coordinator concurrently.
//
// The pointer is stable for the whole running lifetime, so callers may hold the
// returned value across a long operation; the lock is only needed to read it.
func (c *Coordinator) workerRef() *replication.Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.worker
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

// setBeforeStart applies fn under c.mu, but only while the coordinator is still
// in the created state.
//
// It is the single gate behind every configuration setter whose value Start reads
// once and hands to a goroutine.  Mutating such a field afterwards cannot take
// effect — the goroutine already has its copy — so the honest outcomes are an
// error and a no-op, not a silent write to a field nobody will read again.  What
// it replaces is a set of methods that documented one behaviour ("no effect after
// Start") and implemented another, of which SetWorkerQueueDepth was the
// destructive case: it replaced a *running* worker, leaking its goroutine and
// leaving the new one unstarted, so every subsequent Enqueue filled a queue
// nobody drained while Put kept returning nil (#85).
//
// The error is logged here as well as returned.  Every shipped call site is in
// cmd/coordinator before Start, where this returns nil; a call that arrives after
// Start is a caller bug, and the log is what makes it visible even to a caller
// that discards the error.
func (c *Coordinator) setBeforeStart(name string, fn func()) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case coordRunning, coordStopped:
		err := fmt.Errorf("coordinator: %s after Start (state=%s): %w", name, c.state, ErrStarted)
		slog.Error("coordinator: configuration call ignored", "call", name, "state", c.state.String())
		return err
	}
	fn()
	return nil
}

// SetStore registers a metadata store for persistence.
//
// When set, replication jobs are persisted before they are enqueued so they
// survive coordinator restarts.  Completed and failed jobs are deleted from
// the store.
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.
func (c *Coordinator) SetStore(s metadata.Store) error {
	return c.setBeforeStart("SetStore", func() { c.store = s })
}

// SetLeaseManager registers a distributed lease manager.
//
// When set, Start attempts to acquire the "coordinator/leader" lease before
// launching the replication worker.  If this instance is not the leader, the
// worker is not started and the coordinator operates in standby mode (writes
// still reach primary sites synchronously, but async replication is skipped).
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.
func (c *Coordinator) SetLeaseManager(mgr *lease.Manager) error {
	return c.setBeforeStart("SetLeaseManager", func() { c.leaseManager = mgr })
}

// SetMetrics registers a Metrics instance with the coordinator.
// When set, site-count and replication event metrics are emitted automatically.
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.  Freezing it at Start is what closes the #86 race:
// every read now goes through c.metrics() under the lock, and the field stops
// changing at the moment concurrent readers appear.
func (c *Coordinator) SetMetrics(m *metrics.Metrics) error {
	return c.setBeforeStart("SetMetrics", func() { c.m = m })
}

// SetHealthPollInterval sets the interval between background site health
// checks.  The default is 30 seconds.  Pass 0 to use the default.
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.
func (c *Coordinator) SetHealthPollInterval(d time.Duration) error {
	return c.setBeforeStart("SetHealthPollInterval", func() { c.healthPollInterval = d })
}

// SetLeaseTTL sets the TTL used when acquiring the distributed leader lease.
// When 0 (the default), defaultLeaseTTL (15 s) is used.
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.
func (c *Coordinator) SetLeaseTTL(d time.Duration) error {
	return c.setBeforeStart("SetLeaseTTL", func() { c.leaseTTL = d })
}

// SetWorkerQueueDepth configures the replication worker queue depth.
// Pass 0 to retain the existing depth (default 512).
//
// Must be called before Start; returns an error wrapping [ErrStarted] otherwise,
// having changed nothing.  This is the setter #85 is about: it used to replace
// c.worker unconditionally, so calling it on a running coordinator orphaned the
// goroutine that was draining the queue and installed a fresh worker that nobody
// would ever Start.  Replication stopped permanently and Put went on returning
// nil.  Erroring is the right resolution rather than making the depth genuinely
// reconfigurable: the queue's capacity is the buffer of a channel created by
// NewWorker, growing it means a new channel, and a new channel means either
// discarding the jobs in the old one or draining two queues with one worker.
// The one shipped caller (cmd/coordinator) sets it once at boot.
//
// Note that this is a *queue depth*, not a concurrency limit — the worker is a
// single serial goroutine.  The shipped daemon passes
// Performance.MaxConcurrentTransfers here, which conflates the two and is why
// the effective queue is 8 rather than 512 at the defaults.  Renaming that
// setting is a config change and belongs elsewhere; Put now applies
// backpressure rather than dropping writes, so the small depth costs latency
// under a burst instead of data (#79).
func (c *Coordinator) SetWorkerQueueDepth(depth int) error {
	return c.setBeforeStart("SetWorkerQueueDepth", func() {
		if depth > 0 {
			c.worker = replication.NewWorker(depth)
		}
	})
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

// Start launches the background replication worker and the health poller.
//
// It returns nil on the call that starts the coordinator and nil again for every
// later call on a running one, so Start is idempotent for callers that do not
// coordinate among themselves.  It returns an error wrapping [ErrStopped] if the
// coordinator has been stopped — including stopped before it was ever started.
// A Coordinator is single-use; see the package Lifecycle doc.
//
// Start used to return nothing and to be guarded by a sync.Once, which made
// "started" and "refused" indistinguishable.  The Once had no relationship to
// Stop's state at all: Stop-then-Start launched the drain and the health poller
// while silently leaving the worker dead, so replication was off for the process
// lifetime with the coordinator reporting healthy and Put returning success
// (#84).  Returning an error is what lets a caller notice.
//
// If a LeaseManager has been registered via SetLeaseManager, Start attempts to
// acquire the "coordinator/leader" lease.  Only the leader starts the worker; if
// another instance already holds the lease this coordinator enters standby mode.
// Standby is a *successful* start — the coordinator is running, it just has no
// worker — so Start returns nil and Stop still has the drain and poller to undo.
//
// If a Store has been registered via SetStore, Start also recovers any pending
// jobs from the previous run and begins draining worker events to keep the store
// in sync.
func (c *Coordinator) Start(ctx context.Context) error {
	// lifecycleMu serializes Start against Stop for their whole duration, which
	// is the invariant #82 was missing.  c.mu alone cannot provide it: Start has
	// to release c.mu across the lease acquisition (a network call) and across
	// recoverPendingJobs, and a Stop that slipped into that window saw
	// storeCancel still nil and storeWg still zero, concluded there was nothing
	// to stop, and returned — after which Start launched a drain goroutine and a
	// health poller under a context nothing would ever cancel.  59 of 60 races
	// leaked a pair.
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	switch c.state {
	case coordRunning:
		c.mu.Unlock()
		return nil
	case coordStopped:
		c.mu.Unlock()
		return fmt.Errorf("coordinator: Start: %w", ErrStopped)
	}
	// Claim the running state and freeze the configuration in one lock hold, so
	// no Set* call can land between the check and the read.
	c.state = coordRunning
	cfg := startConfig{
		mgr:          c.leaseManager,
		store:        c.store,
		leaseTTL:     c.leaseTTL,
		pollInterval: c.healthPollInterval,
		worker:       c.worker,
	}
	c.mu.Unlock()

	c.launch(ctx, cfg)
	return nil
}

// startConfig is the snapshot of configuration Start takes under c.mu and hands
// to launch.  Grouping it makes the freeze-at-Start rule visible: these fields
// are read once, here, and every Set* that writes them refuses afterwards.
type startConfig struct {
	mgr          *lease.Manager
	store        metadata.Store
	leaseTTL     time.Duration
	pollInterval time.Duration
	worker       *replication.Worker
}

// launch does the work of Start.  Callers must hold c.lifecycleMu.
func (c *Coordinator) launch(ctx context.Context, cfg startConfig) {
	// workerCtx is cancelled when the lease is lost (if a lease manager is set).
	workerCtx := ctx

	if cfg.mgr != nil {
		leaseTTL := cfg.leaseTTL
		if leaseTTL <= 0 {
			leaseTTL = defaultLeaseTTL
		}
		l, acquired, err := cfg.mgr.TryAcquire(ctx, "coordinator/leader", leaseTTL)
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

	if cfg.store != nil {
		c.recoverPendingJobs(workerCtx, cfg.store)
	}

	// The drain and the health poller get separate WaitGroups because Stop needs
	// to know which of them finished.  The final flush is only safe once the
	// drain has returned — two readers would split the event buffer between them
	// (#78) — and one WaitGroup for both means a health poller stuck in an
	// unresponsive site's probe makes Stop unable to tell whether flushing is
	// safe, so it would have to skip a flush it could have done.
	c.drainWg.Add(1)
	go func() {
		defer c.drainWg.Done()
		c.drainWorkerEvents(drainCtx, cfg.store)
	}()

	// Launch background health polling goroutine.
	// Uses drainCtx so it stops when Stop() is called (via storeCancel).
	pollInterval := cfg.pollInterval
	if pollInterval <= 0 {
		pollInterval = defaultHealthPollInterval
	}
	c.healthWg.Add(1)
	go func() {
		defer c.healthWg.Done()
		c.runHealthPollLoop(drainCtx, pollInterval)
	}()

	cfg.worker.Start(workerCtx)
}

// runHealthPollLoop polls site health until ctx is cancelled.
//
// It is a named method rather than the closure it used to be so that it appears
// as a stable frame in a goroutine dump: the leak assertions for #82 identify the
// coordinator's background goroutines by function name, which is the only way to
// tell them apart from those of other tests running in the same process.
func (c *Coordinator) runHealthPollLoop(ctx context.Context, interval time.Duration) {
	c.runHealthPoll(ctx) // immediate first check
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.runHealthPoll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// Stop signals the background replication worker to stop and waits for it to
// finish the current job.  If a leader lease is held it is released so that a
// standby coordinator can take over immediately.
//
// Stop before Start is legal and is terminal: nothing is torn down because
// nothing was built, but the coordinator moves to stopped and a later Start
// returns [ErrStopped].  Calling Stop more than once is safe.
//
// Stop bounds itself at defaultStopTimeout (30 s).  Use [Coordinator.StopContext]
// to supply your own budget.  Unbounded was the old behaviour, and it is how one
// unresponsive S3 endpoint kept a SIGTERMed process alive until the orchestrator
// SIGKILLed it (#83).
//
// # Teardown order
//
// The producer is stopped before its consumer.  worker.Stop waits for the
// in-flight transfer to finish, and that transfer emits a terminal event; if the
// drain goroutine is torn down first there is nobody left to read it, and the
// job stays in the store as a phantom while its content hash is lost (#78).
func (c *Coordinator) Stop() {
	if err := c.StopContext(context.Background()); err != nil {
		slog.Error("coordinator: Stop did not complete within the default budget",
			"timeout", defaultStopTimeout, "error", err)
	}
}

// StopContext is Stop with a caller-supplied deadline.  It returns nil when
// shutdown completed, or an error wrapping ctx.Err() when the budget elapsed
// first.
//
// If ctx carries no deadline, defaultStopTimeout is imposed — the same treatment
// [Coordinator.Health] gives its own context, and for the same reason: an
// unbounded wait on a remote endpoint is a liveness bug, not a configuration
// choice.
//
// # What a non-nil error means
//
// The coordinator *is* stopped either way: the state transition, the worker's
// stop signal and both context cancellations all happen before anything is
// waited on, so no new work starts.  What the error reports is that shutdown gave
// up *observing* the finish, so one or more of these may still be true:
//
//   - a replication transfer is still parked inside a site's PUT, and its
//     terminal event will be emitted with nobody left to read it (so a persisted
//     job may be re-enqueued on the next start — which is correct, if wasteful);
//   - a health probe is still inside the objectfs connection pool's own 30 s
//     wait, which is not context-aware.
//
// Those goroutines are abandoned, not cancelled.  Neither transfer nor probe
// becomes interruptible because shutdown would like it to be, so the choice is a
// leaked goroutine in a process that is terminating, or a process that never
// terminates.  This picks the first, and returns the error so the caller can log
// it and set a non-zero exit code (#69's reasoning applied to #83).
func (c *Coordinator) StopContext(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultStopTimeout)
		defer cancel()
	}

	// Held for the whole of Stop, so a concurrent Start either completes before
	// this begins (and is torn down) or is refused after it (#82).
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	c.state = coordStopped
	storeCancel := c.storeCancel
	leaderCancel := c.leaderCancel
	l := c.leaderLease
	c.leaderLease = nil // so a second Stop does not double-release
	store := c.store
	worker := c.worker
	c.mu.Unlock()

	var errs []error

	// 1. Stop the producer.  Returns once the in-flight job has settled, so
	//    every terminal event it will ever emit is buffered by the time this
	//    returns.  The events channel is buffered to the queue depth, so emit
	//    cannot block even with no reader currently scheduled.
	if err := worker.StopContext(ctx); err != nil {
		errs = append(errs, fmt.Errorf("replication worker did not settle: %w", err))
	}

	// 2. Now the consumer.  Cancelling these stops the drain goroutine, the
	//    health poller, and the lease keepalive.
	//
	//    leaderCancel deliberately does not run first.  leaderCtx is the worker's
	//    ctx, so cancelling it before step 1 would abort the in-flight transfer
	//    instead of letting it settle, which is the opposite of what #78 needs.
	//    It was also never a bound in the no-lease-manager case, which is every
	//    shipped deployment; the bound is now the ctx above.
	if leaderCancel != nil {
		leaderCancel()
	}
	if storeCancel != nil {
		storeCancel()
	}

	// 3. Wait for the drain, then flush on this goroutine.  The wait is what
	//    makes the flush safe: flushWorkerEvents and a live drain would split the
	//    event buffer between them.  If the wait times out the flush is skipped
	//    rather than raced.
	//
	//    Step 1 plus the drain's own flush covers the common case, but not the
	//    one the shipped daemon actually produces: cmd/coordinator cancels the
	//    root context and *then* calls Close, so the drain can already have
	//    exited on ctx.Done before Stop is ever entered — and the same is true
	//    after a lost leader lease, which cancels leaderCtx with no Stop
	//    involved.  Flushing here makes the guarantee independent of who
	//    cancelled what first, which is worth more than the one non-blocking
	//    channel read it costs when the buffer is empty (#78).
	if err := waitBounded(ctx, &c.drainWg); err != nil {
		errs = append(errs, fmt.Errorf("event drain did not exit, skipping final flush: %w", err))
	} else {
		c.flushWorkerEvents(store)
	}

	// 4. The health poller.  Waited on separately because it is the goroutine
	//    most likely to be stuck — objectfs's connection pool ignores the probe
	//    context and waits up to 30 s of its own (#83) — and letting that block
	//    the flush decision above would cost data for no reason.
	if err := waitBounded(ctx, &c.healthWg); err != nil {
		errs = append(errs, fmt.Errorf("health poller did not exit: %w", err))
	}

	// Release the leader lease last so a standby can take over quickly.
	if l != nil {
		if err := l.Release(); err != nil {
			slog.Warn("coordinator: release leader lease", "error", err)
			errs = append(errs, fmt.Errorf("release leader lease: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("coordinator: Stop: %w", errors.Join(errs...))
	}
	return nil
}

// waitBounded waits for wg, giving up when ctx ends.
//
// sync.WaitGroup has no context-aware Wait, so the wait runs on its own
// goroutine.  That goroutine outlives a timed-out call for exactly as long as the
// work it is waiting on does; see StopContext for why abandoning it is the right
// trade.  Every Add on these WaitGroups happens inside Start, which holds
// c.lifecycleMu, so the Add-before-Wait requirement holds by construction.
func waitBounded(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops background work and closes all sites.
//
// It bounds itself at defaultStopTimeout (30 s); use [Coordinator.CloseContext]
// to supply your own budget.  Close is idempotent: the second call finds no sites
// and returns whatever the stop reported.
func (c *Coordinator) Close() error {
	return c.CloseContext(context.Background())
}

// CloseContext is Close with a caller-supplied deadline.  It stops background
// work and then closes every site, returning the joined errors of both halves.
//
// # Why the sites are closed outside c.mu
//
// A site's Close is a network operation — connection-pool drain, in-flight
// request completion — with no bound on how long it takes.  Holding c.mu across
// it blocked every method that touches the site set, including Sites() and the
// /health handler behind it, for the full duration: measured at over two seconds
// against one slow site, which an orchestrator polling health during a rolling
// restart reads as a hung process rather than a draining one (#95).
//
// So this follows the pattern [Coordinator.RemoveSite] already uses: snapshot
// under the lock, clear the set inside the same critical section so a concurrent
// request sees an empty site list rather than a half-closed one, and do the
// teardown after releasing.  Clearing inside the critical section is also what
// keeps Close idempotent.
//
// The closes run concurrently and are bounded by ctx for the same reason the
// worker wait is: one unresponsive endpoint must not be able to prevent the
// process from terminating (#83).  A close that outruns the budget is abandoned
// and reported.
func (c *Coordinator) CloseContext(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultStopTimeout)
		defer cancel()
	}

	var errs []error
	if err := c.StopContext(ctx); err != nil {
		errs = append(errs, err)
	}

	c.mu.Lock()
	sites := c.snapshotSites()
	c.sites = nil
	// c.ns is replaced rather than closed: it holds the same *SiteMount pointers
	// as c.sites, so closing both would close every site twice.  An empty
	// Namespace keeps every c.ns.* call site nil-safe.
	c.ns = namespace.New()
	c.mu.Unlock()

	if err := closeSites(ctx, sites); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// closeSites closes every site concurrently and waits up to ctx for them.
// Returns the joined per-site errors, plus ctx.Err() if the budget elapsed with
// closes still outstanding.
func closeSites(ctx context.Context, sites []*site.SiteMount) error {
	if len(sites) == 0 {
		return nil
	}

	// Buffered to len(sites) so a close that finishes after the deadline can still
	// deliver its result and exit instead of blocking on the send forever.
	results := make(chan error, len(sites))
	for _, s := range sites {
		go func() {
			if err := s.Close(); err != nil {
				results <- fmt.Errorf("close site %q: %w", s.Name(), err)
				return
			}
			results <- nil
		}()
	}

	var errs []error
	for i := 0; i < len(sites); i++ {
		select {
		case err := <-results:
			if err != nil {
				errs = append(errs, err)
			}
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("%d of %d site close(s) did not finish: %w",
				len(sites)-i, len(sites), ctx.Err()))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
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
	c.metricsSiteCountLocked(len(c.sites))
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
	c.metricsSiteCountLocked(len(c.sites))
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
	c.metricsSiteCountLocked(len(c.sites))
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

// ErrHealthTimeout marks a site whose health probe had not answered when Health's
// context expired.  It means "unknown", not "unhealthy" — but for routing and for
// /health the two are treated the same, which is the safe direction.
var ErrHealthTimeout = errors.New("health check did not answer before the deadline")

// Health returns a per-site health report.
// A nil error means the site is healthy; checks run concurrently.
//
// If ctx carries no deadline, defaultHealthTimeout is imposed.
//
// # The report is bounded, the probes are not
//
// Health returns when every probe has answered *or* when ctx expires, whichever
// comes first, and a site that has not answered by then is reported with
// [ErrHealthTimeout].  It used to wait unconditionally for every goroutine, which
// made the deadline a request rather than a guarantee: a probe that ignores its
// context pinned the wait open, and because the background poller runs under the
// same call, that pinned Stop open too — one unresponsive endpoint and SIGTERM
// never completed (#83).
//
// The probes do ignore their context, which is why this matters rather than being
// belt-and-braces.  SiteMount.Health calls objectfs's Client.Health, which reaches
// Backend.HealthCheck → ClientManager.HealthCheck, and that acquires a pooled S3
// client via ConnectionPool.Get() *before* it makes the ctx-aware HeadBucket call.
// Get is hard-coded to GetWithTimeout(30 * time.Second) and takes no context at
// all, so a saturated pool blocks the probe for up to 30 s no matter what deadline
// the caller set.  Verified by reading objectfs v0.12.0: internal/storage/s3/
// client.go:239-256 and pool.go:111-201.
//
// A timed-out probe is therefore abandoned, not cancelled.  It writes its result
// into a channel this function has already stopped reading; the channel is
// buffered to the site count so the write cannot block, and the goroutine exits on
// its own once the underlying call returns.
func (c *Coordinator) Health(ctx context.Context) map[string]error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultHealthTimeout)
		defer cancel()
	}

	c.mu.RLock()
	snapshot := c.snapshotSites()
	c.mu.RUnlock()

	type probeResult struct {
		name string
		err  error
	}
	// Buffered to len(snapshot): an abandoned probe must be able to complete its
	// send and exit rather than leaking on a blocked channel write forever.
	results := make(chan probeResult, len(snapshot))
	for _, s := range snapshot {
		go func() {
			results <- probeResult{name: s.Name(), err: s.Health(ctx)}
		}()
	}

	result := make(map[string]error, len(snapshot))
	for i := 0; i < len(snapshot); i++ {
		select {
		case r := <-results:
			result[r.name] = r.err
		case <-ctx.Done():
			// Fill in every site that has not answered.  Reporting them as
			// timed-out rather than omitting them keeps the report's key set equal
			// to the site set, which SiteInfos and preferHealthySites both assume.
			for _, s := range snapshot {
				if _, ok := result[s.Name()]; !ok {
					result[s.Name()] = fmt.Errorf("%w: %w", ErrHealthTimeout, ctx.Err())
				}
			}
			return result
		}
	}
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
	//
	// cacheGen is the invalidation generation observed *before* the site read, and
	// is what the fill below is conditioned on.  It has to be captured here, not
	// next to the fill: the whole point is to notice a Put or Delete that landed
	// while this goroutine was blocked in s.Get (#89, #90).
	var cacheGen uint64
	if oc != nil {
		if cached, ok := oc.Get(key); ok {
			c.metricsCacheHit()
			c.metricsCacheBytes(oc.Stats().Bytes)
			return cached, nil
		}
		c.metricsCacheMiss()
		cacheGen = oc.Generation()
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
	ordered, cbBypass := filterByCircuitBreaker(cb, ordered)

	var lastErr error
	// allNotFound stays true while every site tried has answered "no such key".
	// It distinguishes "the object does not exist" from "no site could answer",
	// which the API layer needs to tell a 404 from a 502 (#77).
	allNotFound := true
	for s := range attemptableSites(cb, ordered, cbBypass) {
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
			// Populate the cache on a successful site fetch — but only if nothing
			// was invalidated while the read was in flight.  A Put or Delete that
			// committed at the sites and then cleared the cache found nothing to
			// clear, because this entry did not exist yet, so an unconditional fill
			// reinstated bytes the caller had already overwritten or erased, with no
			// expiry to bound how long they were served (#89, #90).  Skipping the
			// fill costs one cache miss on the next read, which is always safe.
			if oc != nil {
				evicted, stored := oc.PutIfUnchanged(key, data, cacheGen)
				if stored {
					c.metricsCacheBytes(oc.Stats().Bytes)
					for i := int64(0); i < evicted; i++ {
						c.metricsCacheEviction()
					}
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
// object is retrievable immediately afterwards.  cmd/coordinator does this, and
// also sets X-GlobalFS-Replication: pending so a caller can tell the two kinds
// of success apart without parsing a body (#130).
//
// # Cache invalidation
//
// The cached entry for key is invalidated on every path out of Put, not only the
// successful one.  Primaries are written sequentially and the first failure
// returns immediately, so a Put that reports an error may still have mutated an
// earlier primary — and the cache would otherwise go on serving the pre-Put
// value, with the default TTL of 0, for as long as the process ran (#91).
func (c *Coordinator) Put(ctx context.Context, key string, data []byte) error {
	c.mu.RLock()
	snapshot, pol, store, cb, oc := c.snapshotSites(), c.policy, c.store, c.cb, c.objCache
	m, backpressure, worker := c.m, c.enqueueBackpressure, c.worker
	c.mu.RUnlock()

	routed, err := pol.Route(policy.OperationWrite, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Put %q: policy error: %w", key, err)
	}

	// partitionForWrite promotes the first non-primary when the routed set has no
	// primaries (e.g. a burst-only policy rule), so the data is persisted before
	// Put returns.  Delete uses the same helper (#88).
	primaries, others := partitionForWrite(routed)

	// Invalidate the cache on every path out of Put, not only the success path.
	//
	// Primaries are written sequentially and the first failure returns
	// immediately, so a Put that reports an error may still have mutated an
	// earlier primary.  When the invalidation lived at the end of the function
	// the error path skipped it and readers kept being served the pre-Put value
	// from cache — with the default TTL of 0, for as long as the process ran
	// (#91).  A defer taken here, before the first site is touched, covers the
	// early returns below by construction rather than by remembering to.
	//
	// Invalidating more than strictly necessary is the safe direction: a
	// spurious invalidation costs one cache miss, a missed one serves stale data
	// indefinitely.
	if oc != nil {
		defer func() {
			oc.Delete(key)
			c.metricsCacheBytes(oc.Stats().Bytes)
		}()
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
			if enqErr := enqueueWithBackpressure(ctx, worker, backpressure, replication.ReplicationJob{
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
					"key", key, "dest", s.Name(), "queue_depth", worker.QueueDepth(), "error", enqErr)
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

	if len(notQueued) > 0 {
		return fmt.Errorf("coordinator: Put %q: stored on primaries but %w for %v: %w",
			key, ErrReplicationNotQueued, notQueued, notQueuedCause)
	}
	return nil
}

// enqueueWithBackpressure submits job to worker, waiting up to budget for queue
// room.  A budget of 0 means defaultEnqueueBackpressure; a negative budget means
// a single attempt with no wait.
//
// Returns nil once the job is queued, or the worker's queue-full error if the
// budget elapses.  A cancelled ctx aborts the wait and returns ctx.Err().
//
// The worker is a parameter rather than read from the receiver so that the caller
// reads c.worker once, under the lock, and the whole backpressure loop then works
// against that one worker.  Re-reading the field per retry would have been the
// pointer race in #85 with extra steps.
func enqueueWithBackpressure(ctx context.Context, worker *replication.Worker, budget time.Duration, job replication.ReplicationJob) error {
	err := worker.Enqueue(job)
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
			if err = worker.Enqueue(job); err == nil {
				return nil
			}
		}
	}
}

// ErrDeleteIncomplete reports that a Delete removed the object from some of the
// routed sites but not all of them, so the object is still readable.
//
// It is the delete-side counterpart of [ErrReplicationNotQueued]: a partial
// outcome that neither "succeeded" nor "failed" describes honestly.  The error
// message names every site that may still hold the object, and the underlying
// cause of the first such failure is wrapped, so errors.Is finds both this
// sentinel and the reason.
//
// Retrying the identical Delete is safe and is the intended response.  Delete is
// idempotent at every site: a site that no longer has the key answers "no such
// key", which Delete counts as already-deleted rather than as a failure, so a
// retry converges on nil once the unwell sites recover.
//
// Nothing in GlobalFS retries on the caller's behalf.  There is no tombstone and
// no queued delete job — the durable job machinery would need the metadata store,
// which no shipped deployment wires — so an error wrapping this sentinel is the
// only signal that exists, and a caller that discards it has an object it
// believes is erased and that [Coordinator.Get] will still serve from the
// surviving replica (#87).
var ErrDeleteIncomplete = errors.New("delete incomplete; object still present at some sites")

// Delete removes the object at key from every site in the policy-routed set.
//
// Delete is all-or-report: it returns nil only when every routed site confirmed
// the object is gone.  If any site failed to delete it, Delete returns an error
// wrapping [ErrDeleteIncomplete] that names the sites which may still hold the
// object, and increments globalfs_delete_incomplete_total.  A site that answers
// "no such key" counts as gone, not as a failure, so repeating a Delete is safe.
//
// Every routed site is attempted even after one fails, primaries first.  The
// previous behaviour returned on the first primary error, which left the replicas
// untouched, and logged-and-discarded every non-primary error, which meant a
// delete that removed nothing from a burst site still reported success while Get
// went on serving the surviving copy (#87, #88).  Removing the object from as
// many sites as possible and then naming the rest is strictly better for a caller
// under an erasure obligation than stopping at the first refusal.
//
// The cache entry for key is invalidated on every path, including the incomplete
// one: the object was removed somewhere, so the cached copy is wrong either way.
//
// An HTTP layer in front of this may want to distinguish the partial case (the
// object exists at a named subset of sites, and retrying is the fix) from a
// delete that achieved nothing.  cmd/coordinator currently maps every Delete
// error to 502, which is defensible for both but tells the client less than it
// could; refining it is tracked separately.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	c.mu.RLock()
	snapshot, pol, cb, oc := c.snapshotSites(), c.policy, c.cb, c.objCache
	c.mu.RUnlock()

	routed, err := pol.Route(policy.OperationDelete, key, snapshot)
	if err != nil {
		return fmt.Errorf("coordinator: Delete %q: policy error: %w", key, err)
	}

	// The promotion in partitionForWrite is what makes a burst-only delete rule
	// report anything at all: without it every site is a non-primary and, before
	// this, every non-primary error was discarded (#88).  It matters less now that
	// both loops report, but Put and Delete sharing one partition keeps the two
	// from drifting apart again.
	primaries, others := partitionForWrite(routed)

	// Invalidate on every path out of Delete, for the same reason Put does (#91).
	if oc != nil {
		defer func() {
			oc.Delete(key)
			c.metricsCacheBytes(oc.Stats().Bytes)
		}()
	}

	// Primaries first, then the rest, in one loop so both get identical
	// treatment.  Built into a fresh slice rather than appending others onto
	// primaries: after the promotion above, primaries aliases others' backing
	// array, and appending to it would write through into the slice being
	// appended from.
	targets := make([]*site.SiteMount, 0, len(primaries)+len(others))
	targets = append(targets, primaries...)
	targets = append(targets, others...)

	// remaining collects the sites that may still hold the object, so the error
	// names all of them rather than only the first.  cause keeps the first
	// underlying error for errors.Is.
	var remaining []string
	var cause error
	for _, s := range targets {
		siteErr := s.Delete(ctx, key)
		recordSiteResult(cb, s.Name(), siteErr)
		if !objectStillPresentAfterDelete(siteErr) {
			continue
		}
		remaining = append(remaining, s.Name())
		if cause == nil {
			cause = siteErr
		}
		slog.Error("coordinator: Delete failed; the object is still readable at this site",
			"key", key, "site", s.Name(), "role", s.Role(), "error", siteErr)
	}

	if len(remaining) > 0 {
		// Counter as well as log: a delete that did not happen everywhere is an
		// integrity event that has to stay visible after the log line scrolls,
		// and for a deployment under a retention obligation it is the only
		// machine-readable record that the API reported an erasure it did not
		// perform (#87).
		c.metrics().RecordDeleteIncomplete()
		return fmt.Errorf("coordinator: Delete %q: %w: %v: %w",
			key, ErrDeleteIncomplete, remaining, cause)
	}
	return nil
}

// objectStillPresentAfterDelete reports whether a site's Delete error leaves the
// object readable at that site.
//
// A "no such key" answer does not: the site was reached, it answered, and its
// answer is that the object is absent — which is the state Delete wanted.  Any
// other error is treated as "it may still be there", because the coordinator
// cannot tell a refused delete from a delete that happened and failed to
// acknowledge, and assuming the object survived is the direction that reports a
// problem instead of hiding one.
//
// This is the same classification isSiteFailure makes for the circuit breaker
// (#77) and it has to be made twice, for different questions: the breaker asks
// whether the site is unwell, Delete asks whether the object is gone.  A backend
// that reports not-found on a repeat delete would otherwise make every retry of
// an already-completed Delete look incomplete forever.
func objectStillPresentAfterDelete(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, objectfssdk.ErrNotFound)
}

// List returns up to limit objects under prefix, merged across policy-routed sites.
//
// The policy engine is consulted using the prefix as the key and
// OperationRead as the operation type.  Healthy sites are promoted and open
// circuit breakers are filtered before the Namespace merge so that routing
// rules, health state, and breaker state all apply consistently to List —
// the same way they do for Get and Head.
// Pass limit ≤ 0 to retrieve all matching objects.
//
// # Circuit breaker
//
// List reads breaker state but never acquires a probe permit, and records no
// outcomes.  It is a pure consumer of the state Get, Head, Put and Delete
// maintain.  That is deliberate rather than an omission (#94):
//
//   - It cannot leak permits, which is what it used to do worst of all the read
//     paths — it asked Allow for every candidate and then handed the whole set to
//     the Namespace merge, recording nothing for any of them.
//   - It could not reliably record them either.  [namespace.Namespace.List] folds
//     per-site failures into one joined error for the merge, so List cannot
//     attribute an outcome back to the site it came from without reimplementing
//     the merge.
//   - A HalfOpen site therefore stays listable while its probe is outstanding.
//     Including it can only add keys to a merged listing that already tolerates
//     unreachable sites, whereas excluding it silently truncates the namespace —
//     the failure mode List must not have.
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
	routed, _ = filterByCircuitBreaker(cb, routed)

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
	ordered, cbBypass := filterByCircuitBreaker(cb, ordered)

	var lastErr error
	allNotFound := true
	// Lazy permit acquisition, paired one-to-one with recordSiteResult below.  Head
	// has the same first-hit-wins shape as Get and leaked probe permits the same
	// way (#94).
	for s := range attemptableSites(cb, ordered, cbBypass) {
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

	if err := c.workerRef().Enqueue(replication.ReplicationJob{
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
	return c.workerRef().QueueDepth()
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
// are not open, and whether the breaker is being bypassed for that subset.
//
// bypass is true when cb is nil, or when every site's circuit is open — in which
// case the original slice is returned unchanged so callers are never completely
// blocked by stale state.  That fallback is load-bearing: without it a window in
// which every site had failed once would be a total read outage no recovery could
// clear, because a site that is never tried never records the success that would
// close its circuit.  Callers must not ask the breaker for permission when bypass
// is set; the sites in the returned slice have already been judged unusable and
// asking would refuse every one of them.
//
// The membership test is [circuitbreaker.Breaker.State], not Allow.  Allow is not
// a predicate — on a HalfOpen circuit it *takes* the single probe permit, which
// only a recorded outcome releases.  Asking it about every candidate while the
// read paths use just the first site that answers leaked one permit per unused
// site and ejected those sites from routing for the process lifetime, reporting
// HalfOpen so they did not even look tripped (#94).  State answers the same
// routing question and takes nothing; the permit is now acquired per attempt, in
// the read loops, where it is paired with a recordSiteResult that releases it.
func filterByCircuitBreaker(cb *circuitbreaker.Breaker, sites []*site.SiteMount) (allowed []*site.SiteMount, bypass bool) {
	if cb == nil {
		return sites, true
	}
	allowed = make([]*site.SiteMount, 0, len(sites))
	for _, s := range sites {
		if cb.State(s.Name()) != circuitbreaker.StateOpen {
			allowed = append(allowed, s)
		}
	}
	if len(allowed) == 0 {
		// All circuits open — fall back to all sites to avoid blocking callers.
		return sites, true
	}
	return allowed, false
}

// attemptableSites yields the sites a first-hit-wins read may try, in order,
// having acquired each one's circuit-breaker probe permit as it is yielded.
//
// The caller must call [recordSiteResult] exactly once for every site it
// receives, and must not skip a yielded site for any other reason.  That pairing
// is what releases the permit and is the whole of the fix for #94: acquiring
// permits for the whole candidate list up-front, as filterByCircuitBreaker used
// to, stranded one for every site the read did not reach — and a stranded
// HalfOpen permit is never released, so the site was excluded from routing for
// the process lifetime while reporting HalfOpen rather than Open.
//
// Breaking out of the range (which every successful read does) simply stops the
// iteration; no permit is taken for a site that is never yielded.
//
// bypass comes from filterByCircuitBreaker and means "do not consult the
// breaker": either there is none, or every circuit was open and the caller is
// deliberately ignoring them.
//
// If the breaker refuses every candidate, the sites are yielded a second time
// with no permit required.  This is the lazy equivalent of
// filterByCircuitBreaker's all-open fallback, and it is needed because the two
// decisions are no longer simultaneous: a site that was non-Open when the
// candidate list was built can refuse the attempt a moment later, and without the
// second pass a read that reached no site at all would report the object as
// absent — a 404 for data that exists.  Recording an outcome for a site whose
// permit we did not hold can release a concurrent probe early, which the old
// all-open fallback also did; the outcome is real evidence about the site either
// way, and the alternative is worse.
func attemptableSites(cb *circuitbreaker.Breaker, sites []*site.SiteMount, bypass bool) iter.Seq[*site.SiteMount] {
	return func(yield func(*site.SiteMount) bool) {
		if bypass {
			for _, s := range sites {
				if !yield(s) {
					return
				}
			}
			return
		}

		attempted := 0
		for _, s := range sites {
			if !cb.Allow(s.Name()) {
				continue
			}
			attempted++
			if !yield(s) {
				return
			}
		}
		if attempted > 0 {
			return
		}
		for _, s := range sites {
			if !yield(s) {
				return
			}
		}
	}
}

// partitionForWrite splits routed into the sites a mutation must be applied to
// synchronously — and whose errors therefore reach the caller — and the sites it
// is applied to asynchronously (Put) or after the synchronous ones (Delete).
//
// It is partitionByRole plus one rule: if the routed set contains no primary at
// all, the first non-primary is promoted into primaries.  A policy rule whose
// TargetRoles is [burst] produces exactly that set, and without the promotion
// there is no synchronous target, so the caller cannot learn whether the
// mutation happened.  For Put that meant data that was never durably stored
// anywhere before Put returned nil; for Delete it meant nil under *every*
// outcome, because every site landed in the loop that logged its errors and
// discarded them (#88).
//
// Put and Delete share this instead of each inlining it because they already
// drifted apart once: Put grew the promotion with a comment explaining why it
// was necessary, Delete never did, and neither call site shows the omission on
// its own.
func partitionForWrite(routed []*site.SiteMount) (primaries, others []*site.SiteMount) {
	primaries, others = partitionByRole(routed)
	if len(primaries) == 0 && len(others) > 0 {
		primaries = others[:1]
		others = others[1:]
	}
	return primaries, others
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
	worker := c.workerRef()
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
		if err := worker.Enqueue(replication.ReplicationJob{
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
	events := c.workerRef().Events()
	for {
		select {
		case ev, ok := <-events:
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
	events := c.workerRef().Events()
	for {
		select {
		case ev, ok := <-events:
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

	// Both reads go through the locked accessors: c.m is written by SetMetrics
	// under c.mu (#86) and c.worker by SetWorkerQueueDepth under c.mu (#85), and
	// this runs on the drain goroutine concurrently with both.
	m := c.metrics()
	m.RecordReplication(string(ev.Type))
	m.SetReplicationQueueDepth(c.workerRef().QueueDepth())
}
