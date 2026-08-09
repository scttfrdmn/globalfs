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
// channel returned by Events to observe progress.
//
// The two kinds are not equally droppable, and the worker does not treat them
// as if they were (#93).  A terminal event is the only signal that the job
// finished: the coordinator deletes the durable job record and writes the
// dedup content hash when it receives one, so losing it leaves a phantom job
// that is re-enqueued on the next restart and a successful transfer that the
// content-hash index does not know about.  An EventStarted, by contrast, has no
// consumer that acts on it.  So:
//
//   - The buffer is sized from the number of emit sites per job, not from the
//     queue depth (see eventBufferSize).  Sizing alone is not a guarantee —
//     a stalled consumer overruns any fixed buffer — it just makes the
//     guarantee below rarely need to do anything.
//   - EventStarted is admitted only while the buffer is below its reserve (see
//     terminalReserveSlots), so it can never be the reason a terminal event has
//     nowhere to go.
//   - A terminal event waits for room, bounded by terminalEmitBudget, instead
//     of being discarded on the first full buffer.  The wait is deliberately
//     not cancellable by Stop or by the job's context: Stop exists to let the
//     in-flight job settle, and settling is precisely what emitting the
//     terminal event means.
//   - If the budget elapses the event is lost, and that is reported at Error
//     with the consequence named, plus a monotonic counter
//     (DroppedTerminalEvents) so the loss is observable after the log scrolls.
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
	"sync/atomic"
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

	// eventsPerJob is how many events processJob emits for one job: an
	// EventStarted, then exactly one of EventCompleted/EventFailed.  It is a
	// property of the emit sites in processJob, not a tuning knob — if a third
	// emit site is added, this is the number that has to change with it.
	eventsPerJob = 2

	// eventBufferSlack is extra room beyond one full queue's worth of events.
	// The queue is not the only source of jobs the consumer has to keep up with:
	// a job leaves the queue the moment the serial worker picks it up, so
	// callers can refill the queue while earlier jobs' events are still
	// unconsumed.  The slack absorbs that overlap so that the bounded wait in
	// emitTerminal stays a backstop for a genuinely stalled consumer rather
	// than something ordinary bursts reach.
	eventBufferSlack = 64

	// terminalEmitBudget bounds how long a terminal event waits for buffer room
	// before it is abandoned.  It is finite because the alternative — an
	// unbounded blocking send — makes any wedged consumer wedge the worker
	// goroutine, and Stop waits on that goroutine, trading a lost event for a
	// shutdown that never completes.  Ten seconds is orders of magnitude beyond
	// what a consumer that is running at all needs to service one event (the
	// coordinator's handler does two 5 s-bounded store calls at worst), and
	// Stop only ever waits on one job, so it bounds shutdown's exposure at one
	// budget.
	terminalEmitBudget = 10 * time.Second
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

// eventBufferSize returns the capacity for the events channel given a job queue
// depth.
//
// It is deliberately not depth.  Every job emits eventsPerJob events into this
// channel while occupying one queue slot, so a channel of depth held only
// depth/eventsPerJob jobs' worth of terminal events — at the shipped default
// depth of 8, a burst of five writes was enough to start discarding completions
// (#93).  Sizing it at depth*eventsPerJob restores the intended relationship,
// and the slack covers jobs that have already left the queue.
func eventBufferSize(depth int) int {
	return depth*eventsPerJob + eventBufferSlack
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

	// terminalBudget bounds the wait for buffer room when emitting a terminal
	// event.  Zero means terminalEmitBudget; may be shortened in tests.
	terminalBudget time.Duration

	// droppedTerminal counts terminal events that were abandoned because the
	// consumer did not free buffer room within the budget.  Each one is a job
	// the coordinator will never learn settled.
	droppedTerminal atomic.Uint64
}

// NewWorker creates a Worker with the given queue depth.
// Pass depth ≤ 0 to use DefaultQueueDepth (512).
func NewWorker(depth int) *Worker {
	if depth <= 0 {
		depth = DefaultQueueDepth
	}
	return &Worker{
		queue:       make(chan ReplicationJob, depth),
		events:      make(chan ReplicationEvent, eventBufferSize(depth)),
		done:        make(chan struct{}),
		baseBackoff: defaultBaseBackoff,
	}
}

// Events returns the read-only channel of ReplicationEvents.
//
// The buffer holds eventBufferSize(depth) events, which is more than the queue
// depth because each job emits several (see the package Events section).  Drain
// this channel or use a select with context.Done: a consumer that stops reading
// for longer than terminalEmitBudget still loses events, and losing a terminal
// event corrupts the coordinator's job bookkeeping.
func (w *Worker) Events() <-chan ReplicationEvent {
	return w.events
}

// DroppedTerminalEvents returns the monotonic count of EventCompleted and
// EventFailed events the worker could not deliver because the events buffer
// stayed full for terminalEmitBudget.
//
// A non-zero value means the coordinator's view of replication is wrong: for
// each drop there is a job record that will never be deleted (and is replayed
// on the next restart) and, for a completion, a content hash that was never
// written, so the same bytes will be transferred again.  It is exposed rather
// than only logged so the loss remains visible after the log line scrolls
// away; wiring it to a Prometheus counter belongs to the coordinator, which
// owns the metrics registry.
func (w *Worker) DroppedTerminalEvents() uint64 {
	return w.droppedTerminal.Load()
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

// isTerminal reports whether ev settles a job, i.e. whether the coordinator
// acts on it.  Only terminal events delete the durable job record and record
// the dedup content hash.
func isTerminal(t EventType) bool {
	return t == EventCompleted || t == EventFailed
}

// emit delivers ev to the events channel.
//
// The two kinds of event get different guarantees, because the cost of losing
// them differs by more than a log line (#93).  Terminal events go through
// emitTerminal, which waits for room; EventStarted is admitted non-blocking and
// only while the buffer is below the reserve, so a flood of started events
// cannot be the reason a completion has nowhere to go.
func (w *Worker) emit(ev ReplicationEvent) {
	if isTerminal(ev.Type) {
		w.emitTerminal(ev)
		return
	}

	// Non-terminal: droppable by design.  Its only consumer is metrics, and the
	// coordinator's handleWorkerEvent ignores it outright.
	if len(w.events)+terminalReserveSlots >= cap(w.events) {
		slog.Debug("replication: events buffer near capacity; dropping non-terminal event",
			"type", ev.Type, "key", ev.Job.Key,
			"buffered", len(w.events), "capacity", cap(w.events))
		return
	}
	select {
	case w.events <- ev:
	default:
		// Lost the race for the last unreserved slot.  Same outcome as above.
		slog.Debug("replication: events buffer full; dropping non-terminal event",
			"type", ev.Type, "key", ev.Job.Key)
	}
}

// terminalReserveSlots is the number of buffer slots withheld from
// EventStarted so that a terminal event always has somewhere to go.
//
// One is sufficient because the worker is serial: Start guards the run
// goroutine with sync.Once and run calls runJob sequentially, so at most one
// job is between its EventStarted and its terminal event at any instant.  If
// the worker ever grows a concurrent pool, this has to become that pool's size
// — the invariant is one reserved slot per in-flight job, not one overall.
const terminalReserveSlots = 1

// emitTerminal delivers a job's settling event, waiting up to the terminal
// budget for buffer room rather than discarding it on a full buffer.
//
// The wait deliberately does not select on w.done or the job's context.  Both
// mean "stop working", and this event is the record that the work already
// finished; abandoning it on cancellation would reintroduce #78's phantom job
// in a narrower window.  Stop waits for the in-flight job, and there is only
// ever one, so the worst case Stop inherits from this is a single budget.
//
// The budget is finite because an unbounded send would let a consumer that has
// stopped reading wedge the worker goroutine permanently, and Stop waits on
// that goroutine — trading a lost event for a shutdown that never completes.
// Under a stalled consumer the wait instead degrades into backpressure: the
// worker slows to one job per budget, the queue fills, and Put's enqueue
// backpressure (#79) reports the condition to writers, which is a far better
// failure mode than silently losing bookkeeping.
func (w *Worker) emitTerminal(ev ReplicationEvent) {
	// Fast path: room right now, which is the overwhelmingly common case given
	// eventBufferSize and the reserve above.
	select {
	case w.events <- ev:
		return
	default:
	}

	budget := w.terminalBudget
	if budget <= 0 {
		budget = terminalEmitBudget
	}

	slog.Warn("replication: events buffer full; waiting to deliver terminal event",
		"type", ev.Type, "key", ev.Job.Key,
		"dest", ev.Job.DestSite.Name(),
		"capacity", cap(w.events), "budget", budget)

	deadline := time.NewTimer(budget)
	defer deadline.Stop()

	// A select with a send case blocks until the send can proceed, so no polling
	// is needed: this parks the worker until either room appears or the budget
	// expires.
	select {
	case w.events <- ev:
		return
	case <-deadline.C:
	}

	// The timer and the send can both become ready in the same window, and
	// select picks at random between them, so try once more before giving up.
	select {
	case w.events <- ev:
		return
	default:
	}

	w.droppedTerminal.Add(1)
	slog.Error("replication: dropped terminal replication event; "+
		"the job record will not be deleted and will be re-enqueued on restart, "+
		"and its content hash was not recorded so the object will be transferred again",
		"type", ev.Type, "key", ev.Job.Key,
		"src", ev.Job.SourceSite.Name(), "dst", ev.Job.DestSite.Name(),
		"attempt", ev.Attempt, "waited", budget,
		"capacity", cap(w.events),
		"dropped_total", w.droppedTerminal.Load())
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
