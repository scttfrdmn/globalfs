package replication

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"

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
	// putGate, if non-nil, blocks Put until it is closed.  It deliberately
	// ignores the caller's context: a real S3 PUT parked in a connection pool or
	// a TCP retransmit does not return because someone cancelled a context, and
	// that is precisely the shutdown case StopContext has to bound (#83).
	putGate     chan struct{}
	putsEntered int
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
	// The gate is read under the lock but waited on outside it: blocking while
	// holding f.mu would deadlock any concurrent hasKey call.
	f.mu.Lock()
	gate := f.putGate
	f.putsEntered++
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}

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

// blockPuts makes every subsequent Put on this client wait.  The returned
// release is safe to call more than once.
func (f *failClient) blockPuts() (release func()) {
	gate := make(chan struct{})
	f.mu.Lock()
	f.putGate = gate
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// waitForPut blocks until a Put has reached the gate, so a test that needs a
// transfer to be genuinely in flight does not race the worker's scheduling.
func (f *failClient) waitForPut(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		entered := f.putsEntered
		f.mu.Unlock()
		if entered >= 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no Put reached the client within %v — the transfer never started", timeout)
}

func (f *failClient) Delete(_ context.Context, _ string) error { return nil }
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

// checksumClient is an ObjectFSClient whose Head returns a fixed checksum, so
// tests can drive the replication fast path with arbitrary checksum values —
// including ones shorter than the 8 characters the log line used to slice (#72).
type checksumClient struct {
	*failClient
	checksum string
}

func newChecksumClient(objs map[string][]byte, checksum string) *checksumClient {
	return &checksumClient{failClient: newFailClient(objs), checksum: checksum}
}

func (c *checksumClient) Head(_ context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	return &objectfstypes.ObjectInfo{Key: key, Checksum: c.checksum}, nil
}

// panicClient panics on Get, standing in for any bug reachable from transfer.
type panicClient struct {
	*failClient
}

func (p *panicClient) Get(_ context.Context, _ string, _, _ int64) ([]byte, error) {
	panic("synthetic transfer panic")
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

// ─── Lifecycle (#83, #84) ─────────────────────────────────────────────────────

// TestWorker_Lifecycle_StateMachine pins the one-directional contract from the
// package doc: Start once succeeds, Start again is a no-op, Stop is terminal, and
// a stopped worker refuses to restart.
func TestWorker_Lifecycle_StateMachine(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(4)
	if w.Started() {
		t.Error("Started() on a fresh worker: got true, want false")
	}
	if !w.Start(ctx) {
		t.Fatal("Start on a created worker: got false, want true")
	}
	if !w.Started() {
		t.Error("Started() after Start: got false, want true")
	}
	if w.Start(ctx) {
		t.Error("second Start on a running worker: got true, want false (it must not launch a second goroutine)")
	}

	w.Stop()
	if w.Started() {
		t.Error("Started() after Stop: got true, want false")
	}
	w.Stop() // idempotent: must not panic on a double close of w.done
	if w.Start(ctx) {
		t.Error("Start on a stopped worker: got true, want false — a Worker is single-use")
	}
}

// TestWorker_StopBeforeStart_IsTerminal is the worker half of #84.  Stop before
// Start used to be implemented as w.once.Do(func(){}), burning Start's Once, so a
// later Start silently did nothing and the caller had no way to find out.  The
// worker is still single-use — that part was intended — but the refusal now has to
// be observable, which is what lets Coordinator.Start turn it into an error.
func TestWorker_StopBeforeStart_IsTerminal(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(4)
	w.Stop()

	if w.Start(ctx) {
		t.Fatal("Start after a Stop-before-Start returned true, but the worker cannot run: " +
			"a caller that trusts this reports success while replication is off for the process lifetime")
	}

	// And the refusal is real, not just a return value: nothing drains the queue.
	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)
	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "k"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if dstClient.hasKey("k") {
		t.Error("a job was transferred by a worker that refused to start")
	}
}

// TestWorker_StopContext_BoundedWhenTransferWedged is the worker half of #83.
// A PUT parked in an unresponsive endpoint must not be able to hold shutdown open:
// StopContext returns when its budget expires and reports ctx.Err() so the caller
// knows the job was abandoned rather than settled.
func TestWorker_StopContext_BoundedWhenTransferWedged(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := dstClient.blockPuts()
	// Released before StopContext is measured would defeat the test; released in a
	// defer so the abandoned goroutine still exits when the test ends.
	defer release()

	w := fastWorker(4)
	if !w.Start(ctx) {
		t.Fatal("Start: got false")
	}
	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "k"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	dstClient.waitForPut(t, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()

	start := time.Now()
	err := w.StopContext(stopCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("StopContext returned nil while the transfer was still wedged in Put")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("StopContext error: got %v, want a wrapped context.DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("StopContext took %v with a 100ms budget — the wait is not actually bounded", elapsed)
	}

	// The signal half must have happened regardless of the error: releasing the
	// transfer lets the abandoned goroutine finish and exit rather than picking up
	// another job.
	if w.Started() {
		t.Error("Started() after a timed-out StopContext: got true, want false")
	}
}

// TestWorker_StopContext_ReturnsNilWhenSettled is the companion to the bounded
// case: with room to finish, StopContext reports success rather than a timeout.
func TestWorker_StopContext_ReturnsNilWhenSettled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(4)
	if !w.Start(ctx) {
		t.Fatal("Start: got false")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := w.StopContext(stopCtx); err != nil {
		t.Fatalf("StopContext on an idle worker: %v", err)
	}
}

// TestWorker_Lifecycle_ConcurrentStartStop runs Start and Stop against each other
// under -race.  Whichever wins, the invariant is the same: no goroutine survives
// the Stop, and neither call panics on a double close of w.done.
func TestWorker_Lifecycle_ConcurrentStartStop(t *testing.T) {
	t.Parallel()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		w := fastWorker(4)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); w.Start(ctx) }()
		go func() { defer wg.Done(); w.Stop() }()
		wg.Wait()

		// Stop is terminal either way, so the worker must be stopped when both
		// calls have returned — even if Start ran second and observed "stopped".
		if w.Started() {
			t.Fatalf("iteration %d: worker still running after a concurrent Start/Stop", i)
		}
		cancel()
	}
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

// ─── #72: short checksums and panic containment ────────────────────────────────

// TestShortChecksum covers the log-truncation helper at and around the boundary.
// Before the fix the call site was srcInfo.Checksum[:8], which panics for every
// input shorter than 8 bytes.
func TestShortChecksum(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"", ""},
		{"ab", "ab"},                    // the reproduced panic: length 2
		{"1234567", "1234567"},          // one below the boundary
		{"12345678", "12345678"},        // exactly the boundary: no ellipsis
		{"123456789", "12345678..."},    // one above
		{"deadbeefcafe", "deadbeef..."}, // typical hex prefix
		{"héllo", "héllo"},              // 6 bytes, 5 runes: no slice
		{"héllo-world", "héllo-w..."},   // multi-byte, sliced on a rune boundary
	}
	for _, tt := range tests {
		if got := shortChecksum(tt.in); got != tt.want {
			t.Errorf("shortChecksum(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestTransfer_ShortMatchingChecksum_DoesNotPanic is the regression test for
// #72.  Both sites report the same 2-character checksum, which takes the fast
// path straight into the log line that used to slice [:8].  On the pre-fix tree
// this panics with "slice bounds out of range [:8] with length 2"; the panic is
// what fails the test, so no assertion on it is needed beyond reaching the end.
func TestTransfer_ShortMatchingChecksum_DoesNotPanic(t *testing.T) {
	t.Parallel()

	const short = "ab" // shorter than the 8 bytes the log line sliced
	srcClient := newChecksumClient(map[string][]byte{"genome.bam": []byte("data")}, short)
	dstClient := newChecksumClient(nil, short)
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst := site.New("dst", types.SiteRoleBackup, dstClient)

	hash, err := transfer(context.Background(), ReplicationJob{
		SourceSite: src, DestSite: dst, Key: "genome.bam",
	})
	if err != nil {
		t.Fatalf("transfer: unexpected error: %v", err)
	}
	if hash != short {
		t.Errorf("fast path should return the source checksum: got %q, want %q", hash, short)
	}
	// The fast path must have skipped the copy entirely.
	if dstClient.hasKey("genome.bam") {
		t.Error("fast path transferred data despite matching checksums")
	}
}

// TestWorker_ShortMatchingChecksum_WorkerSurvives exercises the same input
// through the live worker goroutine.  On the pre-fix tree the panic is
// unrecovered and takes down the test binary — which is exactly what it does to
// the coordinator process in production.
func TestWorker_ShortMatchingChecksum_WorkerSurvives(t *testing.T) {
	t.Parallel()

	srcClient := newChecksumClient(map[string][]byte{"a.bam": []byte("data")}, "ab")
	dstClient := newChecksumClient(nil, "ab")
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst := site.New("dst", types.SiteRoleBackup, dstClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "a.bam"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	var got ReplicationEvent
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && got.Type == "" {
		ev, ok := drainEvent(t, w, 200*time.Millisecond)
		if ok && (ev.Type == EventCompleted || ev.Type == EventFailed) {
			got = ev
		}
	}
	if got.Type != EventCompleted {
		t.Fatalf("expected EventCompleted, got %q (err: %v)", got.Type, got.Err)
	}
	if got.ContentHash != "ab" {
		t.Errorf("ContentHash: got %q, want %q", got.ContentHash, "ab")
	}

	// The worker must still be alive and able to process the next job.
	srcClient.checksum = "" // force the slow path this time
	dstClient.checksum = ""
	srcClient.data["b.bam"] = []byte("more")
	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "b.bam"}); err != nil {
		t.Fatalf("Enqueue second job: unexpected error: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dstClient.hasKey("b.bam") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("worker did not process the job after a short-checksum job")
}

// TestWorker_PanicInTransfer_FailsJobAndKeepsWorkerAlive verifies the panic
// backstop: an arbitrary panic reachable from transfer settles the job as
// EventFailed (so the coordinator can delete it from the metadata store) and
// leaves the worker goroutine running.
func TestWorker_PanicInTransfer_FailsJobAndKeepsWorkerAlive(t *testing.T) {
	t.Parallel()

	srcClient := &panicClient{failClient: newFailClient(map[string][]byte{"boom": []byte("x")})}
	src := site.New("src", types.SiteRolePrimary, srcClient)
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(8)
	w.Start(ctx)
	defer w.Stop()

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "boom"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	var got ReplicationEvent
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && got.Type == "" {
		ev, ok := drainEvent(t, w, 200*time.Millisecond)
		if ok && (ev.Type == EventCompleted || ev.Type == EventFailed) {
			got = ev
		}
	}
	if got.Type != EventFailed {
		t.Fatalf("expected EventFailed after a panic, got %q", got.Type)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "synthetic transfer panic") {
		t.Errorf("EventFailed.Err should name the panic value; got %v", got.Err)
	}

	// A healthy job must still go through on the same worker goroutine.
	good, _ := makeMount("src2", types.SiteRolePrimary, map[string][]byte{"ok": []byte("y")})
	if err := w.Enqueue(ReplicationJob{SourceSite: good, DestSite: dst, Key: "ok"}); err != nil {
		t.Fatalf("Enqueue after panic: unexpected error: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dstClient.hasKey("ok") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("worker did not process a job after recovering from a panic")
}

// ─── #93: the events buffer and terminal-event droppability ────────────────────

// fillEvents pushes n synthetic events straight into w.events, bypassing emit,
// so a test can put the buffer into an exactly-known state.
func fillEvents(t *testing.T, w *Worker, n int) {
	t.Helper()
	src, _ := makeMount("filler-src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("filler-dst", types.SiteRoleBackup, nil)
	for i := 0; i < n; i++ {
		select {
		case w.events <- ReplicationEvent{
			Job:  ReplicationJob{SourceSite: src, DestSite: dst, Key: "filler"},
			Type: EventStarted,
		}:
		default:
			t.Fatalf("fillEvents: buffer full after %d of %d (cap %d)", i, n, cap(w.events))
		}
	}
}

// drainTerminalEvents reads events until the channel has been quiet for the
// given settle period, returning the terminal events seen keyed by object key.
func drainTerminalEvents(w *Worker, settle time.Duration) map[string]int {
	seen := make(map[string]int)
	for {
		select {
		case ev := <-w.Events():
			if ev.Type == EventCompleted || ev.Type == EventFailed {
				seen[ev.Job.Key]++
			}
		case <-time.After(settle):
			return seen
		}
	}
}

// TestWorker_BurstBeyondHalfDepth_NoTerminalEventLost is the #93 regression.
//
// The events channel used to be sized to the queue depth, but processJob emits
// two events per job (EventStarted then a terminal one), so only depth/2 jobs'
// worth of completions fitted and the rest were discarded with a warn log.  A
// discarded completion is durable corruption, not a lost log line: the
// coordinator deletes the persisted job and records the dedup content hash only
// when it receives one.
//
// The consumer here reads nothing until every job has landed on the
// destination, which is exactly the shipped daemon's worst case — the drain
// goroutine descheduled while a burst of writes goes through.
func TestWorker_BurstBeyondHalfDepth_NoTerminalEventLost(t *testing.T) {
	t.Parallel()

	const depth = 8 // the shipped default (Performance.MaxConcurrentTransfers)
	keys := make([]string, depth)
	srcObjs := make(map[string][]byte, depth)
	for i := range keys {
		keys[i] = string(rune('a'+i)) + ".bam"
		srcObjs[keys[i]] = []byte(keys[i] + "-data")
	}

	src, _ := makeMount("src", types.SiteRolePrimary, srcObjs)
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(depth)
	w.Start(ctx)
	defer w.Stop()

	for _, k := range keys {
		if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: k}); err != nil {
			t.Fatalf("Enqueue %q: unexpected error: %v", k, err)
		}
	}

	// Deliberately read nothing until the transfers have all happened.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, k := range keys {
			if !dstClient.hasKey(k) {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, k := range keys {
		if !dstClient.hasKey(k) {
			t.Fatalf("transfer of %q did not happen; test cannot judge event delivery", k)
		}
	}

	seen := drainTerminalEvents(w, 500*time.Millisecond)
	for _, k := range keys {
		if seen[k] != 1 {
			t.Errorf("terminal events for %q: got %d, want exactly 1", k, seen[k])
		}
	}
	if len(seen) != depth {
		t.Errorf("jobs with a terminal event: got %d, want %d (buffer cap %d)",
			len(seen), depth, cap(w.events))
	}
	if got := w.DroppedTerminalEvents(); got != 0 {
		t.Errorf("DroppedTerminalEvents: got %d, want 0", got)
	}
}

// TestEventBufferSize_HoldsAWholeQueuesEvents pins the sizing relationship
// itself, so raising the queue depth or adding a third emit site to processJob
// cannot silently reintroduce the half-depth ceiling.
func TestEventBufferSize_HoldsAWholeQueuesEvents(t *testing.T) {
	t.Parallel()

	for _, depth := range []int{1, 2, 8, 64, DefaultQueueDepth} {
		if got, min := eventBufferSize(depth), depth*eventsPerJob; got < min {
			t.Errorf("eventBufferSize(%d) = %d, want ≥ %d (%d events per job)",
				depth, got, min, eventsPerJob)
		}
	}

	w := NewWorker(8)
	if got := cap(w.events); got < 8*eventsPerJob {
		t.Errorf("NewWorker(8) events cap: got %d, want ≥ %d", got, 8*eventsPerJob)
	}
	if got := cap(w.queue); got != 8 {
		t.Errorf("NewWorker(8) queue cap: got %d, want 8", got)
	}
}

// TestWorker_EventStartedNeverConsumesTheTerminalReserve verifies the asymmetry
// the fix rests on: with one slot left, a non-terminal event is refused and a
// terminal event takes it.  Without the reserve a job's own EventStarted can
// occupy the last slot and force its completion into the bounded wait.
func TestWorker_EventStartedNeverConsumesTheTerminalReserve(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)
	job := ReplicationJob{SourceSite: src, DestSite: dst, Key: "reserved"}

	w := NewWorker(4)
	fillEvents(t, w, cap(w.events)-terminalReserveSlots)
	before := len(w.events)

	w.emit(ReplicationEvent{Job: job, Type: EventStarted, Attempt: 1})
	if got := len(w.events); got != before {
		t.Errorf("EventStarted took a reserved slot: buffered %d → %d (cap %d)",
			before, got, cap(w.events))
	}

	w.emit(ReplicationEvent{Job: job, Type: EventCompleted, Attempt: 1, ContentHash: "abc"})
	if got := len(w.events); got != before+1 {
		t.Errorf("EventCompleted did not use the reserved slot: buffered %d → %d", before, got)
	}
	if got := w.DroppedTerminalEvents(); got != 0 {
		t.Errorf("DroppedTerminalEvents: got %d, want 0", got)
	}
}

// TestWorker_TerminalEventWaitsForRoom verifies that a full buffer makes a
// terminal event wait rather than vanish, and that it is delivered as soon as
// the consumer catches up.
func TestWorker_TerminalEventWaitsForRoom(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	w := NewWorker(2)
	w.terminalBudget = 5 * time.Second
	fillEvents(t, w, cap(w.events))

	// A consumer that is late, not absent.
	go func() {
		time.Sleep(100 * time.Millisecond)
		<-w.Events()
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.emit(ReplicationEvent{
			Job:         ReplicationJob{SourceSite: src, DestSite: dst, Key: "late"},
			Type:        EventCompleted,
			Attempt:     1,
			ContentHash: "hash",
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emit of a terminal event did not return within 5s")
	}

	if got := w.DroppedTerminalEvents(); got != 0 {
		t.Fatalf("DroppedTerminalEvents: got %d, want 0 (event should have waited, not dropped)", got)
	}

	var found bool
	for i := 0; i < cap(w.events); i++ {
		ev, ok := drainEvent(t, w, time.Second)
		if !ok {
			break
		}
		if ev.Type == EventCompleted && ev.Job.Key == "late" {
			found = true
			break
		}
	}
	if !found {
		t.Error("the delayed EventCompleted was never delivered")
	}
}

// TestWorker_TerminalEventDropIsCounted covers the case the bounded wait cannot
// save: a consumer that never reads at all.  The event is lost — the budget is
// finite on purpose, since an unbounded send would let that consumer wedge the
// worker goroutine and therefore Stop — but it must be counted, not merely
// warned about, so the resulting phantom job is attributable after the fact.
func TestWorker_TerminalEventDropIsCounted(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	w := NewWorker(2)
	w.terminalBudget = 50 * time.Millisecond
	fillEvents(t, w, cap(w.events))

	start := time.Now()
	w.emit(ReplicationEvent{
		Job:     ReplicationJob{SourceSite: src, DestSite: dst, Key: "doomed"},
		Type:    EventFailed,
		Attempt: MaxRetries,
		Err:     errors.New("nobody is listening"),
	})
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Errorf("emit gave up after %v, want ≥ the 50ms budget", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("emit took %v; the budget must bound it", elapsed)
	}
	if got := w.DroppedTerminalEvents(); got != 1 {
		t.Errorf("DroppedTerminalEvents: got %d, want 1", got)
	}

	// Monotonic, and only terminal drops count.
	w.emit(ReplicationEvent{
		Job:  ReplicationJob{SourceSite: src, DestSite: dst, Key: "ignored"},
		Type: EventStarted,
	})
	if got := w.DroppedTerminalEvents(); got != 1 {
		t.Errorf("a dropped EventStarted changed the terminal counter: got %d, want 1", got)
	}
}

// TestWorker_TerminalEmitIgnoresStopWhileWaiting pins a deliberate decision: the
// wait for buffer room does not select on w.done.  Stop means "let the in-flight
// job settle", and the terminal event *is* the record that it settled, so
// abandoning it on Stop would reintroduce #78's phantom job in a narrower
// window.  Stop's exposure is bounded at one budget because the worker is
// serial.
func TestWorker_TerminalEmitIgnoresStopWhileWaiting(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, nil)
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	w := NewWorker(2)
	w.terminalBudget = 5 * time.Second
	fillEvents(t, w, cap(w.events))
	close(w.done) // as if Stop had been called

	go func() {
		time.Sleep(100 * time.Millisecond)
		<-w.Events()
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.emit(ReplicationEvent{
			Job:         ReplicationJob{SourceSite: src, DestSite: dst, Key: "settling"},
			Type:        EventCompleted,
			Attempt:     1,
			ContentHash: "hash",
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emit did not return within 5s")
	}
	if got := w.DroppedTerminalEvents(); got != 0 {
		t.Errorf("terminal event abandoned because done was closed: dropped %d, want 0", got)
	}

	var found bool
	for i := 0; i < cap(w.events); i++ {
		ev, ok := drainEvent(t, w, time.Second)
		if !ok {
			break
		}
		if ev.Type == EventCompleted && ev.Job.Key == "settling" {
			found = true
			break
		}
	}
	if !found {
		t.Error("the terminal event of a job settling during Stop was never delivered")
	}
}

// TestWorker_StalledConsumerBecomesBackpressure verifies the failure mode the
// bounded wait produces under a genuinely stalled consumer: the worker stops
// consuming its queue, rather than racing ahead transferring objects whose
// bookkeeping is being discarded.
//
// That is the behavioural difference the fix buys beyond buffer sizing.  With
// the old non-blocking emit the worker ran at full speed against a stalled
// consumer, so the objects landed on the destination and the coordinator never
// heard about any of them.  Now the backlog is visible where callers can act on
// it: the queue fills, Enqueue reports it, and Put's backpressure path (#79)
// surfaces it to writers.
func TestWorker_StalledConsumerBecomesBackpressure(t *testing.T) {
	t.Parallel()

	const nJobs = 12
	keys := make([]string, nJobs)
	srcObjs := make(map[string][]byte, nJobs)
	for i := range keys {
		keys[i] = string(rune('a' + i))
		srcObjs[keys[i]] = []byte("x")
	}
	src, _ := makeMount("src", types.SiteRolePrimary, srcObjs)
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A queue big enough for every job, so a full queue can only come from the
	// worker having stopped draining it.
	w := fastWorker(nJobs)
	w.terminalBudget = time.Second
	// Pre-fill the events buffer so the very first terminal event has to wait.
	fillEvents(t, w, cap(w.events))
	w.Start(ctx)
	defer w.Stop()

	// Nothing ever reads Events().
	for _, k := range keys {
		if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: k}); err != nil {
			t.Fatalf("Enqueue %q: unexpected error: %v", k, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	var transferred int
	for _, k := range keys {
		if dstClient.hasKey(k) {
			transferred++
		}
	}
	// The worker parks in emitTerminal on the first job it settles, so at most
	// one transfer gets through.  Allowing two absorbs a slow scheduler.
	if transferred > 2 {
		t.Errorf("worker transferred %d of %d objects while its event consumer was stalled; "+
			"it should have blocked on delivering the first terminal event", transferred, nJobs)
	}
	if got := w.QueueDepth(); got == 0 {
		t.Error("queue drained to empty with a stalled consumer; the backlog is not visible to callers")
	}
	if got := w.DroppedTerminalEvents(); got != 0 {
		t.Errorf("DroppedTerminalEvents: got %d, want 0 within the budget", got)
	}
}

// TestWorker_StopCompletesWithNoEventConsumer is the safety property that makes
// the bounded wait acceptable: waiting for buffer room must not let an absent
// consumer wedge shutdown.  Stop waits on the worker goroutine, so if the wait
// were an unbounded blocking send this would hang forever instead of finishing
// with one counted drop.
func TestWorker_StopCompletesWithNoEventConsumer(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	dst, _ := makeMount("dst", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := fastWorker(1)
	w.terminalBudget = 200 * time.Millisecond
	fillEvents(t, w, cap(w.events)) // full buffer, and nothing will ever read it
	w.Start(ctx)

	if err := w.Enqueue(ReplicationJob{SourceSite: src, DestSite: dst, Key: "k"}); err != nil {
		t.Fatalf("Enqueue: unexpected error: %v", err)
	}

	stopped := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		w.Stop()
		stopped <- time.Since(start)
	}()

	select {
	case elapsed := <-stopped:
		// The queue holds one job, so Stop's exposure is one budget plus the
		// transfer.  The generous ceiling is about not deadlocking, not timing.
		if elapsed > 5*time.Second {
			t.Errorf("Stop took %v with no event consumer; the budget must bound it", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return with no event consumer: a blocking terminal send wedged shutdown")
	}
}
