// Package replication provides a bounded, retriable worker that moves object
// data between SiteMounts.
//
// # Transfer
//
// The v0.1.0 transfer implementation is a simple GET → PUT over the SiteMount
// interface.  Future versions will replace this with CargoShip's streaming
// archive pipeline (tar.zst) for higher-throughput, compressed inter-site
// transfers.
//
// # Lifecycle
//
// A Worker is single-use.  It moves through exactly three states, in one
// direction only:
//
//	created --Start--> running --Stop--> stopped
//	created ---------------Stop--------> stopped
//
// Create a Worker with NewWorker, call Start to begin processing, then Stop
// when done.  Enqueue is safe to call before Start; jobs accumulate in the
// bounded queue.  Stop waits for the currently executing job to finish, and
// StopContext bounds that wait.
//
// A stopped Worker cannot be restarted: Start on it is a no-op logged at Error,
// and that includes a Worker stopped before it was ever started.  The single-use
// rule is deliberate rather than an artefact — the done channel, the WaitGroup
// and the job context are all one-shot — and it is the same rule the coordinator
// enforces one level up, so a stopped Worker is never reached through it.
//
// Before this was a state machine the rule was expressed as two sync.Onces, and
// Stop burned Start's once with `w.once.Do(func(){})`.  That made "stopped after
// running" and "stopped before ever running" indistinguishable to the caller and
// to the doc comment, which claimed Stop-before-Start was safe while it in fact
// disabled the worker for the process lifetime with no way to observe it (#84).
//
// # Events
//
// For each job the worker emits an EventStarted before the first attempt and
// either EventCompleted or EventFailed when the job settles.  Drain the
// channel returned by Events to observe progress.  The channel is buffered to
// the same depth as the work queue; if it fills, events are dropped (logged)
// rather than blocking the worker.
//
// # Panic containment
//
// The worker goroutine is a long-lived part of the coordinator daemon, so a
// panic in it would terminate the whole process.  Each job is therefore run
// under a recover: the panic value and stack are logged at Error and the job
// settles as EventFailed, keeping the failure scoped to the object that caused
// it.  This is containment, not suppression — a recovered panic is still a bug
// and is logged as loudly as the process can log it.
package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/site"
)

// EventType classifies the lifecycle phase reported in a ReplicationEvent.
type EventType string

const (
	// EventStarted is emitted once before the first transfer attempt.
	EventStarted EventType = "started"
	// EventCompleted is emitted when the transfer succeeds.
	EventCompleted EventType = "completed"
	// EventFailed is emitted when all retry attempts are exhausted.
	EventFailed EventType = "failed"
)

// ReplicationJob describes a single object transfer between two SiteMounts.
type ReplicationJob struct {
	// SourceSite is the SiteMount to GET the object from.
	SourceSite *site.SiteMount

	// DestSite is the SiteMount to PUT the object to.
	DestSite *site.SiteMount

	// Key is the object key.
	Key string

	// Size is the expected object size in bytes (0 = unknown; informational only).
	Size int64
}

// ReplicationEvent is emitted for each job lifecycle transition.
type ReplicationEvent struct {
	Job         ReplicationJob
	Type        EventType
	Attempt     int
	Err         error  // non-nil only for EventFailed
	ContentHash string // SHA-256 hex; set on EventCompleted, empty otherwise
}

const (
	// DefaultQueueDepth is the default bounded queue size.
	DefaultQueueDepth = 512

	// MaxRetries is the maximum number of transfer attempts per job.
	MaxRetries = 3

	defaultBaseBackoff = 100 * time.Millisecond
)

// workerState is the Worker's position in its one-directional lifecycle.
type workerState int

const (
	// workerCreated is a Worker that has never run.  Start moves it to
	// workerRunning; Stop moves it straight to workerStopped.
	workerCreated workerState = iota
	// workerRunning is a Worker whose run goroutine is live.
	workerRunning
	// workerStopped is terminal.  Start on it is refused.
	workerStopped
)

func (s workerState) String() string {
	switch s {
	case workerCreated:
		return "created"
	case workerRunning:
		return "running"
	case workerStopped:
		return "stopped"
	default:
		return fmt.Sprintf("workerState(%d)", int(s))
	}
}

// Worker processes ReplicationJobs from a bounded FIFO queue.
//
// Each job is attempted up to MaxRetries times with exponential backoff
// (baseBackoff × 2^(attempt-1)).  Worker is safe for concurrent use.
// See the package doc for the lifecycle contract.
type Worker struct {
	queue  chan ReplicationJob
	events chan ReplicationEvent

	wg   sync.WaitGroup
	done chan struct{}

	// stateMu guards state.  It is held only across the state transition
	// itself, never across wg.Wait, so a Stop in progress cannot block a
	// concurrent Started query.
	stateMu sync.Mutex
	state   workerState

	// baseBackoff controls the initial retry delay.  Defaults to
	// defaultBaseBackoff; may be overridden in tests.
	baseBackoff time.Duration
}

// NewWorker creates a Worker with the given queue depth.
// Pass depth ≤ 0 to use DefaultQueueDepth (512).
func NewWorker(depth int) *Worker {
	if depth <= 0 {
		depth = DefaultQueueDepth
	}
	return &Worker{
		queue:       make(chan ReplicationJob, depth),
		events:      make(chan ReplicationEvent, depth),
		done:        make(chan struct{}),
		baseBackoff: defaultBaseBackoff,
	}
}

// Events returns the read-only channel of ReplicationEvents.
//
// Events are buffered to the same depth as the work queue.  Drain this
// channel or use a select with context.Done to avoid event drops.
func (w *Worker) Events() <-chan ReplicationEvent {
	return w.events
}

// QueueDepth returns the number of jobs currently waiting in the queue.
func (w *Worker) QueueDepth() int {
	return len(w.queue)
}

// Enqueue adds a job to the work queue.
// Returns an error when the queue is full so callers can log or propagate it.
// Enqueue is safe to call before Start.
func (w *Worker) Enqueue(job ReplicationJob) error {
	select {
	case w.queue <- job:
		return nil
	default:
		return fmt.Errorf("replication: queue full; key=%q → %q",
			job.Key, job.DestSite.Name())
	}
}

// Start launches the background worker goroutine and reports whether it did.
//
// Calling Start more than once is safe: the second call returns false and has no
// effect.  Start on a stopped Worker also returns false, and logs at Error —
// restarting a Worker is not supported (see the package Lifecycle doc), and a
// caller that tries has almost certainly lost track of its own lifecycle.
// [Coordinator.Start] turns that false into an error the operator can see rather
// than running with replication silently off (#84).
func (w *Worker) Start(ctx context.Context) bool {
	w.stateMu.Lock()
	switch w.state {
	case workerRunning:
		w.stateMu.Unlock()
		return false
	case workerStopped:
		w.stateMu.Unlock()
		slog.Error("replication: Start on a stopped worker; a Worker is single-use and cannot be restarted")
		return false
	}
	w.state = workerRunning
	w.wg.Add(1)
	w.stateMu.Unlock()

	go w.run(ctx)
	return true
}

// Started reports whether the worker's run goroutine is currently live.
func (w *Worker) Started() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	return w.state == workerRunning
}

// Stop signals the worker to exit and waits for it to finish the current job.
//
// Stop before Start is a genuine no-op on the goroutine — there is none — but it
// is still terminal: it moves a created Worker to stopped, so a later Start is
// refused.  Calling Stop more than once is safe.
//
// Stop waits without bound.  A job parked in an unresponsive site's PUT holds it
// there for as long as that PUT takes, which is the shutdown hang in #83; prefer
// [Worker.StopContext] anywhere a deadline matters.
func (w *Worker) Stop() {
	_ = w.StopContext(context.Background())
}

// StopContext signals the worker to exit and waits up to ctx for the current job
// to settle.  It returns nil once the run goroutine has returned, or ctx.Err() if
// ctx ends first.
//
// The signal half always happens, even on the error return: the worker is
// stopped, and the only thing the deadline gives up on is *observing* that it
// finished.  A caller that gets a non-nil error must treat the in-flight job as
// still running and its terminal event as possibly unemitted — the job is
// abandoned rather than cancelled, because transfer holds the caller's context
// and Stop has no authority to cancel it.
//
// Abandoning a goroutine is a deliberate trade.  A transfer blocked in a site's
// PUT does not become interruptible because shutdown would like it to be, so the
// options are a leaked goroutine in a process that is terminating anyway, or a
// process that never terminates.  #83 chose the first.
func (w *Worker) StopContext(ctx context.Context) error {
	w.stateMu.Lock()
	alreadyStopped := w.state == workerStopped
	w.state = workerStopped
	w.stateMu.Unlock()

	if !alreadyStopped {
		close(w.done)
	}

	// wg.Wait has no context-aware form, so it runs on its own goroutine and the
	// select below picks whichever finishes first.  This goroutine outlives a
	// timed-out StopContext by exactly as long as the abandoned job does.
	waited := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(waited)
	}()

	select {
	case <-waited:
		return nil
	case <-ctx.Done():
		slog.Warn("replication: worker did not stop within the deadline; abandoning the in-flight job",
			"error", ctx.Err())
		return ctx.Err()
	}
}

// ── Internal ─────────────────────────────────────────────────────────────────

func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case job := <-w.queue:
			w.runJob(ctx, job)
		}
	}
}

// runJob invokes processJob with a last-resort panic backstop.
//
// safeTransfer already converts a panic inside transfer into an ordinary
// attempt error, which is where a panic is overwhelmingly likely to originate.
// This outer recover covers the remainder of processJob — the retry loop, the
// log calls that dereference job fields, and emit — so that no panic in the
// worker can terminate the process that hosts it.  It emits EventFailed before
// returning, because the coordinator deletes a persisted job from the metadata
// store only on a terminal event; skipping it would leave an orphaned job that
// is re-enqueued on every restart.
func (w *Worker) runJob(ctx context.Context, job ReplicationJob) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("replication: recovered panic while processing job",
				"key", job.Key,
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()))
			w.emit(ReplicationEvent{
				Job:     job,
				Type:    EventFailed,
				Attempt: MaxRetries,
				Err:     fmt.Errorf("replication: panic processing job: %v", r),
			})
		}
	}()
	w.processJob(ctx, job)
}

func (w *Worker) processJob(ctx context.Context, job ReplicationJob) {
	w.emit(ReplicationEvent{Job: job, Type: EventStarted, Attempt: 1})

	var lastErr error
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		if attempt > 1 {
			delay := time.Duration(math.Pow(2, float64(attempt-1))) * w.baseBackoff
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				w.emit(ReplicationEvent{
					Job:     job,
					Type:    EventFailed,
					Attempt: attempt,
					Err:     ctx.Err(),
				})
				return
			case <-w.done:
				w.emit(ReplicationEvent{
					Job:     job,
					Type:    EventFailed,
					Attempt: attempt,
					Err:     fmt.Errorf("worker stopped: %w", lastErr),
				})
				return
			}
		}

		contentHash, err := safeTransfer(ctx, job)
		if err != nil {
			lastErr = err
			slog.Warn("replication: transfer attempt failed",
				"attempt", attempt, "max_retries", MaxRetries,
				"key", job.Key, "src", job.SourceSite.Name(), "dst", job.DestSite.Name(),
				"error", err)
			continue
		}

		w.emit(ReplicationEvent{Job: job, Type: EventCompleted, Attempt: attempt, ContentHash: contentHash})
		return
	}

	w.emit(ReplicationEvent{
		Job:     job,
		Type:    EventFailed,
		Attempt: MaxRetries,
		Err:     lastErr,
	})
}

func (w *Worker) emit(ev ReplicationEvent) {
	select {
	case w.events <- ev:
	default:
		slog.Warn("replication: events channel full; dropping event", "type", ev.Type, "key", ev.Job.Key)
	}
}

// safeTransfer calls transfer and converts a panic into an ordinary error so
// that one malformed object cannot terminate the coordinator process.
//
// A panic here is a bug, and the retry loop will re-run the same input twice
// more before giving up — which is the point: the failure is reported through
// the normal EventFailed path with the job's key attached, rather than as a
// process-wide crash whose only record is stderr on a daemon that is no longer
// running.  The panic value and stack are logged at Error so the bug is not
// silently swallowed.
func safeTransfer(ctx context.Context, job ReplicationJob) (hash string, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("replication: recovered panic during transfer",
				"key", job.Key,
				"src", job.SourceSite.Name(),
				"dst", job.DestSite.Name(),
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()))
			hash = ""
			err = fmt.Errorf("replication: panic during transfer of %q: %v", job.Key, r)
		}
	}()
	return transfer(ctx, job)
}

// shortChecksum truncates a checksum for logging, without assuming its length.
// Checksums originate in S3 user metadata and are not guaranteed to be 64-char
// SHA-256 hex: a non-ObjectFS backend or an ETag-derived value can be shorter
// (#72).
func shortChecksum(s string) string {
	const n = 8
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// transfer performs the actual byte movement: GET from source, PUT to dest.
// Returns the SHA-256 hex of the transferred content and any error.
//
// Fast path: if both sites expose ObjectInfo.Checksum (set by ObjectFS ≥ v0.10.0)
// and the checksums match, the transfer is skipped as the destination already
// holds identical content.  Backward compatible: if either checksum is empty
// the fast path is skipped and the full GET → PUT proceeds.
//
// v0.1.0 slow path: simple GET → PUT over the SiteMount interface.
// Future: replace with CargoShip streaming archive pipeline for large-scale,
// compressed inter-site transfers (tracked in globalfs #3 follow-on).
func transfer(ctx context.Context, job ReplicationJob) (string, error) {
	// Fast path: compare checksums before transferring.
	srcInfo, srcErr := job.SourceSite.Head(ctx, job.Key)
	if srcErr == nil && srcInfo != nil && srcInfo.Checksum != "" {
		destInfo, destErr := job.DestSite.Head(ctx, job.Key)
		if destErr == nil && destInfo != nil && destInfo.Checksum == srcInfo.Checksum {
			slog.Info("replication: skipping transfer, dest already has matching content",
				"key", job.Key,
				"checksum", shortChecksum(srcInfo.Checksum),
				"dest", job.DestSite.Name())
			return srcInfo.Checksum, nil
		}
	}

	// Slow path: full GET → PUT.
	data, err := job.SourceSite.Get(ctx, job.Key, 0, 0)
	if err != nil {
		return "", fmt.Errorf("get from %q: %w", job.SourceSite.Name(), err)
	}
	if err := job.DestSite.Put(ctx, job.Key, data); err != nil {
		return "", fmt.Errorf("put to %q: %w", job.DestSite.Name(), err)
	}

	// Compute content hash from transferred bytes.
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
