package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ── Shutdown context derivation (#83) ─────────────────────────────────────────

// TestNewShutdownContext_IsNotDerivedFromCancelledRoot pins the detail that makes
// the #83 fix correct rather than merely bounded.
//
// The shipped shutdown path cancels the daemon's root context and only then tears
// the coordinator down.  A timeout context derived from that root is born
// cancelled, so every bounded wait inside CloseContext would return immediately —
// which looks like a successful fast shutdown and is actually the phantom-job
// condition #78 fixed, since the in-flight transfer is abandoned before its
// terminal event is emitted.
//
// The test asserts the correct behaviour *and* demonstrates the broken
// alternative, so the reason the code is written this way survives in executable
// form rather than only in a comment.
func TestNewShutdownContext_IsNotDerivedFromCancelledRoot(t *testing.T) {
	// Reproduce the daemon's state at the point of teardown.
	root, cancel := context.WithCancel(context.Background())
	cancel()

	// The broken alternative, to prove the hazard is real and not theoretical.
	derived, derivedCancel := context.WithTimeout(root, coordinatorShutdownTimeout)
	defer derivedCancel()
	if derived.Err() == nil {
		t.Fatal("a context derived from the cancelled root is NOT already cancelled; " +
			"the premise of this test (and of newShutdownContext's comment) is wrong " +
			"and the reasoning needs revisiting")
	}

	// The real thing must survive the root's cancellation.
	ctx, ctxCancel := newShutdownContext()
	defer ctxCancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("newShutdownContext returned an already-finished context (%v): "+
			"every bounded wait in CloseContext would give up immediately and the "+
			"in-flight transfer would be abandoned rather than settled (#78, #83)", err)
	}

	// And it must actually carry a deadline: StopContext and CloseContext only
	// impose their own default when the caller supplies none, so a missing
	// deadline here would silently move the bound back inside the coordinator.
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("newShutdownContext has no deadline; SIGTERM would rely on the " +
			"coordinator's internal default instead of the daemon's own bound")
	}
	if d := time.Until(deadline); d <= 0 || d > coordinatorShutdownTimeout+time.Second {
		t.Errorf("deadline is %v away, want ~%v", d, coordinatorShutdownTimeout)
	}
}

// ── Shutdown actually waits for the in-flight transfer ────────────────────────

// parkedClient wraps testMemClient to announce when a Put has been *entered*,
// which is what these tests need to synchronise on.
//
// Waiting for the key to appear instead would deadlock against the very gate that
// makes the transfer in-flight: the whole point is that the Put does not finish.
// entered is closed on the first Put, so the test can proceed the instant the
// worker is genuinely parked rather than sleeping a guessed interval.
type parkedClient struct {
	*testMemClient
	entered chan struct{}
	once    sync.Once
}

func newParkedClient() *parkedClient {
	return &parkedClient{
		testMemClient: newTestMemClient(nil),
		entered:       make(chan struct{}),
	}
}

func (p *parkedClient) Put(ctx context.Context, key string, data []byte) error {
	p.once.Do(func() { close(p.entered) })
	return p.testMemClient.Put(ctx, key, data)
}

// waitParked blocks until a Put has been entered, or fails the test.
func (p *parkedClient) waitParked(t *testing.T) {
	t.Helper()
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no replication Put was entered; the worker never picked up the job " +
			"and these tests cannot observe an in-flight transfer")
	}
}

// TestShutdown_WaitsForInFlightTransfer is the behavioural half of #83: with the
// context newShutdownContext produces, teardown waits for a parked transfer to
// settle; with one derived from the cancelled root it does not.
//
// This is the assertion that would have caught the mistake, as opposed to the
// derivation test above which only catches it if someone reads it.
func TestShutdown_WaitsForInFlightTransfer(t *testing.T) {
	newDaemon := func(t *testing.T) (*coordinator.Coordinator, *parkedClient, func()) {
		t.Helper()
		primary := newTestMemClient(nil)
		backup := newParkedClient()
		release := backup.blockPuts()

		c := coordinator.New(
			site.New("primary", types.SiteRolePrimary, primary),
			site.New("backup", types.SiteRoleBackup, backup),
		)
		// The daemon's root context, which shutdown cancels before teardown.  Start
		// takes it, so cancelling it is what stops the worker accepting new jobs —
		// the same wiring main.go uses.
		rootCtx, rootCancel := context.WithCancel(context.Background())
		if err := c.Start(rootCtx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Put so the worker dequeues a job and parks inside the backup's Put.
		if err := c.Put(context.Background(), "data/inflight", []byte("payload")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		backup.waitParked(t)

		rootCancel() // what main does before tearing down
		return c, backup, release
	}

	t.Run("fresh context lets the transfer settle", func(t *testing.T) {
		c, backup, release := newDaemon(t)

		// Release shortly after teardown begins, so the wait is what decides
		// whether the transfer completes.
		go func() {
			time.Sleep(50 * time.Millisecond)
			release()
		}()

		// Same derivation as newShutdownContext, with a shorter budget so the test
		// is fast; the property under test is the parentage, not the duration.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := c.CloseContext(ctx); err != nil {
			t.Fatalf("CloseContext with a fresh context: %v (want nil: the transfer "+
				"was given time to settle)", err)
		}
		if backup.keyCount() == 0 {
			t.Error("the in-flight transfer never landed on the backup: shutdown " +
				"abandoned it instead of letting it settle (#78)")
		}
	})

	t.Run("context derived from the cancelled root abandons it", func(t *testing.T) {
		c, backup, release := newDaemon(t)
		t.Cleanup(release)

		// The mistake newShutdownContext exists to prevent.
		root, rootCancel := context.WithCancel(context.Background())
		rootCancel()
		ctx, cancel := context.WithTimeout(root, 10*time.Second)
		defer cancel()

		err := c.CloseContext(ctx)
		if err == nil {
			t.Fatal("CloseContext with a context derived from the cancelled root " +
				"returned nil; it cannot have waited for anything, so this test no " +
				"longer distinguishes the two derivations")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error does not wrap context.Canceled: %v", err)
		}
		if backup.keyCount() != 0 {
			t.Log("note: the transfer landed anyway (scheduling); the error above is " +
				"still the signal that shutdown stopped observing it")
		}
	})
}

// ── Refused Start (#84) ───────────────────────────────────────────────────────

// TestStart_RefusalIsDetectable pins the contract main.go now depends on: a
// coordinator that has been stopped refuses Start with an error, so the check
// added at the Start call site can actually fire.
//
// This is deliberately a test of the boundary and not of main() itself.  The
// consequence of a refused Start in the daemon is os.Exit(1), which is not
// observable from a test in this package without restructuring main into an
// injectable run() returning an error — a change well beyond the scope of these
// commits and one that would touch every path in a 300-line function nothing
// else currently covers.  Rather than force it, this asserts the half that is
// real: that the error main keys off is produced, is non-nil, and identifies
// itself via errors.Is so the call site cannot be fooled by a formatting change.
// The os.Exit itself is reviewed, not tested, and is called out as such in the
// PR.
func TestStart_RefusalIsDetectable(t *testing.T) {
	c := coordinator.New(site.New("primary", types.SiteRolePrimary, newTestMemClient(nil)))
	c.Stop() // single-use lifecycle: this is now terminal

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("Start on a stopped coordinator returned nil; the daemon would run " +
			"with no replication worker and no health poller, reporting healthy (#84)")
	}
	if !errors.Is(err, coordinator.ErrStopped) {
		t.Errorf("Start error does not wrap ErrStopped: %v", err)
	}
}

// TestMustConfigure_PassesOnNil is a guard on the helper's happy path, which is
// the one every shipped boot takes.  The failure path calls os.Exit and is not
// testable in-process; see TestStart_RefusalIsDetectable for why that is being
// stated rather than worked around.
func TestMustConfigure_PassesOnNil(t *testing.T) {
	mustConfigure("nothing wrong", nil) // must not exit
}
