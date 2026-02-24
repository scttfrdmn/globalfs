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
// Create a Worker with NewWorker, call Start to begin processing, then Stop
// when done.  Enqueue is safe to call before Start; jobs accumulate in the
// bounded queue.  Stop waits for the currently executing job to finish.
//
// # Events
//
// For each job the worker emits an EventStarted before the first attempt and
// either EventCompleted or EventFailed when the job settles.  Drain the
// channel returned by Events to observe progress.  The channel is buffered to
// the same depth as the work queue; if it fills, events are dropped (logged)
// rather than blocking the worker.
package replication

import (
	"context"
	"fmt"
	"log"
	"math"
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
	Job     ReplicationJob
	Type    EventType
	Attempt int
	Err     error // non-nil only for EventFailed
}

const (
	// DefaultQueueDepth is the default bounded queue size.
	DefaultQueueDepth = 512

	// MaxRetries is the maximum number of transfer attempts per job.
	MaxRetries = 3

	defaultBaseBackoff = 100 * time.Millisecond
)

// Worker processes ReplicationJobs from a bounded FIFO queue.
//
// Each job is attempted up to MaxRetries times with exponential backoff
// (baseBackoff × 2^(attempt-1)).  Worker is safe for concurrent use.
type Worker struct {
	queue  chan ReplicationJob
	events chan ReplicationEvent

	wg        sync.WaitGroup
	done      chan struct{}
	once      sync.Once
	closeOnce sync.Once

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

// Start launches the background worker goroutine.
// Calling Start multiple times is safe; only the first call has effect.
func (w *Worker) Start(ctx context.Context) {
	w.once.Do(func() {
		w.wg.Add(1)
		go w.run(ctx)
	})
}

// Stop signals the worker to exit and waits for it to finish the current job.
// Calling Stop before Start is safe.  Calling Stop multiple times is safe.
func (w *Worker) Stop() {
	w.once.Do(func() {}) // prevent any future Start
	w.closeOnce.Do(func() { close(w.done) })
	w.wg.Wait()
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
			w.processJob(ctx, job)
		}
	}
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

		if err := transfer(ctx, job); err != nil {
			lastErr = err
			log.Printf("replication: attempt %d/%d key=%q %q→%q: %v",
				attempt, MaxRetries, job.Key,
				job.SourceSite.Name(), job.DestSite.Name(), err)
			continue
		}

		w.emit(ReplicationEvent{Job: job, Type: EventCompleted, Attempt: attempt})
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
		log.Printf("replication: events channel full; dropping %s event for key=%q",
			ev.Type, ev.Job.Key)
	}
}

// transfer performs the actual byte movement: GET from source, PUT to dest.
//
// v0.1.0: simple GET → PUT over the SiteMount interface.
// Future: replace with CargoShip streaming archive pipeline for large-scale,
// compressed inter-site transfers (tracked in globalfs #3 follow-on).
func transfer(ctx context.Context, job ReplicationJob) error {
	data, err := job.SourceSite.Get(ctx, job.Key, 0, 0)
	if err != nil {
		return fmt.Errorf("get from %q: %w", job.SourceSite.Name(), err)
	}
	if err := job.DestSite.Put(ctx, job.Key, data); err != nil {
		return fmt.Errorf("put to %q: %w", job.DestSite.Name(), err)
	}
	return nil
}
