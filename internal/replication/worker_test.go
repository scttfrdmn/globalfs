package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ─── Test helpers ──────────────────────────────────────────────────────────────

// failClient is a thread-safe in-memory ObjectFSClient that can inject errors.
type failClient struct {
	mu      sync.Mutex
	data    map[string][]byte
	getErrs []error // consumed in order; nil = success
	putErrs []error // consumed in order; nil = success
}

func newFailClient(objs map[string][]byte) *failClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &failClient{data: objs}
}

func (f *failClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.getErrs) > 0 {
		err := f.getErrs[0]
		f.getErrs = f.getErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	v, ok := f.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (f *failClient) Put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.putErrs) > 0 {
		err := f.putErrs[0]
		f.putErrs = f.putErrs[1:]
		if err != nil {
			return err
		}
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.data[key] = cp
	return nil
}

func (f *failClient) Delete(_ context.Context, _ string) error                        { return nil }
func (f *failClient) List(_ context.Context, _ string, _ int) ([]objectfstypes.ObjectInfo, error) {
	return nil, nil
}
func (f *failClient) Head(_ context.Context, _ string) (*objectfstypes.ObjectInfo, error) {
	return nil, nil
}
func (f *failClient) Health(_ context.Context) error { return nil }
func (f *failClient) Close() error                   { return nil }

func (f *failClient) hasKey(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[key]
	return ok
}

func (f *failClient) putCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data) // approximation; use a counter for exact counts
}

// countingClient wraps failClient and tracks call counts.
type countingClient struct {
	*failClient
	mu       sync.Mutex
	getCalls int
	putCalls int
}

func newCountingClient(objs map[string][]byte) *countingClient {
	return &countingClient{failClient: newFailClient(objs)}
}

func (c *countingClient) Get(ctx context.Context, key string, off, sz int64) ([]byte, error) {
	c.mu.Lock()
	c.getCalls++
	c.mu.Unlock()
	return c.failClient.Get(ctx, key, off, sz)
}

func (c *countingClient) Put(ctx context.Context, key string, data []byte) error {
	c.mu.Lock()
	c.putCalls++
	c.mu.Unlock()
	return c.failClient.Put(ctx, key, data)
}

func (c *countingClient) getCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getCalls
}

func (c *countingClient) putCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putCalls
}

// makeMount creates a SiteMount backed by a failClient pre-loaded with objs.
func makeMount(name string, role types.SiteRole, objs map[string][]byte) (*site.SiteMount, *failClient) {
	fc := newFailClient(objs)
	return site.New(name, role, fc), fc
}

// fastWorker returns a Worker with a tiny base backoff to keep tests quick.
func fastWorker(depth int) *Worker {
	w := NewWorker(depth)
	w.baseBackoff = time.Millisecond
	return w
}

// drainEvent reads one event from w.Events() within timeout, or returns zero value.
func drainEvent(t *testing.T, w *Worker, timeout time.Duration) (ReplicationEvent, bool) {
	t.Helper()
	select {
	case ev := <-w.Events():
		return ev, true
	case <-time.After(timeout):
		return ReplicationEvent{}, false
	}
}

// ─── Tests ──────────────────────────────────────────────────────────────────────

// TestWorker_BasicTransfer verifies that a job is processed: data is GET-ted
// from source and PUT to dest.
func TestWorker_BasicTransfer(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{
		"genome.bam": []byte("sequence-data"),
	})
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "genome.bam", Size: 13}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dstClient.hasKey("genome.bam") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !dstClient.hasKey("genome.bam") {
		t.Error("destination did not receive key within 2s")
	}
}

// TestWorker_EmitsStartedAndCompleted verifies the event sequence for a
// successful job: EventStarted then EventCompleted.
func TestWorker_EmitsStartedAndCompleted(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{
		"sample.fastq": []byte("reads"),
	})
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "sample.fastq"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	ev1, ok := drainEvent(t, w, 2*time.Second)
	if !ok {
		t.Fatal("timed out waiting for first event")
	}
	if ev1.Type != EventStarted {
		t.Errorf("first event: got %q, want %q", ev1.Type, EventStarted)
	}
	if ev1.Job.Key != "sample.fastq" {
		t.Errorf("first event key: got %q, want %q", ev1.Job.Key, "sample.fastq")
	}

	ev2, ok := drainEvent(t, w, 2*time.Second)
	if !ok {
		t.Fatal("timed out waiting for second event")
	}
	if ev2.Type != EventCompleted {
		t.Errorf("second event: got %q, want %q", ev2.Type, EventCompleted)
	}
	if ev2.Attempt != 1 {
		t.Errorf("completed event attempt: got %d, want 1", ev2.Attempt)
	}
	if ev2.Err != nil {
		t.Errorf("completed event err: got %v, want nil", ev2.Err)
	}
}

// TestWorker_RetryOnTransientGetError verifies that a transient source error
// triggers a retry and the job eventually completes.
func TestWorker_RetryOnTransientGetError(t *testing.T) {
	t.Parallel()

	srcClient := &countingClient{
		failClient: &failClient{
			data:    map[string][]byte{"key": []byte("val")},
			getErrs: []error{errors.New("transient"), nil}, // fail once, then succeed
		},
	}
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "key"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	// Wait for completion event.
	var got ReplicationEvent
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventCompleted || ev.Type == EventFailed {
				got = ev
				goto done
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
done:
	if got.Type != EventCompleted {
		t.Fatalf("expected EventCompleted, got %q (err: %v)", got.Type, got.Err)
	}
	if got.Attempt != 2 {
		t.Errorf("expected completion on attempt 2, got %d", got.Attempt)
	}
	if srcClient.getCount() != 2 {
		t.Errorf("expected 2 Get calls (1 fail + 1 success), got %d", srcClient.getCount())
	}
	if !dstClient.hasKey("key") {
		t.Error("destination should have the key after retry")
	}
}

// TestWorker_ExhaustsRetries_EmitsFailed verifies that a permanently failing
// source results in EventFailed after MaxRetries attempts.
func TestWorker_ExhaustsRetries_EmitsFailed(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("storage offline")
	srcClient := &failClient{
		data:    map[string][]byte{},
		getErrs: []error{sentinel, sentinel, sentinel},
	}
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "missing"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	// Drain until we get EventFailed.
	var got ReplicationEvent
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventFailed {
				got = ev
				goto done
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
done:
	if got.Type != EventFailed {
		t.Fatalf("expected EventFailed, got %q", got.Type)
	}
	if !errors.Is(got.Err, sentinel) {
		t.Errorf("expected sentinel error, got %v", got.Err)
	}
	if got.Attempt != MaxRetries {
		t.Errorf("expected attempt %d, got %d", MaxRetries, got.Attempt)
	}
}

// TestWorker_QueueFull_ReturnsError verifies that enqueueing to a full queue
// returns a non-nil error without blocking or panicking.
func TestWorker_QueueFull_ReturnsError(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	// Do NOT start the worker — queue will fill immediately.
	w := NewWorker(2)

	job := ReplicationJob{SourceSite: src, DestSite: dst, Key: "k"}
	if err := w.Enqueue(job); err != nil { // slot 1
		t.Fatalf("slot 1: unexpected error: %v", err)
	}
	if err := w.Enqueue(job); err != nil { // slot 2
		t.Fatalf("slot 2: unexpected error: %v", err)
	}
	if err := w.Enqueue(job); err == nil { // full: must return error
		t.Error("expected error when queue is full, got nil")
	}
}

// TestWorker_StopBeforeStart verifies that Stop is safe to call without Start.
func TestWorker_StopBeforeStart(t *testing.T) {
	t.Parallel()
	w := NewWorker(4)
	w.Stop() // must not panic or deadlock
}

// TestWorker_MultipleJobs verifies that multiple queued jobs are all processed.
func TestWorker_MultipleJobs(t *testing.T) {
	t.Parallel()

	keys := []string{"a.bam", "b.bam", "c.bam"}
	srcObjs := make(map[string][]byte, len(keys))
	for _, k := range keys {
		srcObjs[k] = []byte(k + "-data")
	}
	src, _ := makeMount("src", types.SiteRolePrimary, srcObjs)
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(16)
	w.Start(ctx)
	defer w.Stop()

	for _, k := range keys {
		if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: k}); err != nil {
			t.Fatalf("Enqueue %q: unexpected error: %v", k, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, k := range keys {
			if !dstClient.hasKey(k) {
				allDone = false
				break
			}
		}
		if allDone {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("not all keys replicated within 3s")
}

// TestWorker_StopDuringBackoff_WrapsLastErr verifies that when the worker is
// stopped during a retry backoff sleep the EventFailed error wraps the last
// transfer error so the cause is not lost (#53).
func TestWorker_StopDuringBackoff_WrapsLastErr(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("disk-full")
	srcClient := &failClient{
		data:    map[string][]byte{},
		getErrs: []error{sentinel, sentinel, sentinel},
	}
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Worker{
		queue:       make(chan ReplicationJob, 8),
		events:      make(chan ReplicationEvent, 8),
		done:        make(chan struct{}),
		baseBackoff: 500 * time.Millisecond, // long enough that Stop fires mid-backoff
	}
	w.Start(ctx)

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "key"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	// Wait for EventStarted (attempt 1 fired and failed).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventStarted {
				goto started
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for EventStarted")
started:

	// Stop the worker while it is sleeping before attempt 2.
	w.Stop()

	var gotFailed bool
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventFailed {
				gotFailed = true
				if !errors.Is(ev.Err, sentinel) {
					t.Errorf("EventFailed.Err should wrap sentinel; got: %v", ev.Err)
				}
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
		if gotFailed {
			break
		}
	}
	if !gotFailed {
		t.Error("expected EventFailed after Stop during backoff")
	}
}

// TestWorker_ContextCancellation verifies that a pending job is abandoned when
// the context is cancelled during retry backoff.
func TestWorker_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Source always fails → worker will retry with backoff.
	sentinel := errors.New("always fails")
	srcClient := &failClient{
		data:    map[string][]byte{},
		getErrs: []error{sentinel, sentinel, sentinel},
	}
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		queue:       make(chan ReplicationJob, 8),
		events:      make(chan ReplicationEvent, 8),
		done:        make(chan struct{}),
		baseBackoff: 200 * time.Millisecond, // long enough that cancel fires during backoff
	}
	w.Start(ctx)

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "key"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	// Wait for EventStarted (first attempt fired).
	var found bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventStarted {
				found = true
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("timed out waiting for EventStarted")
	}

	// Cancel the context during backoff for attempt 2.
	cancel()

	// We should receive EventFailed (due to context cancellation).
	var gotFailed bool
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-w.Events():
			if ev.Type == EventFailed {
				gotFailed = true
				if !errors.Is(ev.Err, context.Canceled) {
					t.Errorf("EventFailed.Err: got %v, want context.Canceled", ev.Err)
				}
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
		if gotFailed {
			break
		}
	}
	if !gotFailed {
		t.Error("expected EventFailed after context cancellation")
	}

	w.Stop()
}
