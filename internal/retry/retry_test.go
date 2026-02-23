package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/internal/retry"
)

var sentinel = errors.New("transient error")

// TestDo_SucceedsOnFirstAttempt verifies that fn is called once and the nil
// error is returned immediately.
func TestDo_SucceedsOnFirstAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Do(context.Background(), retry.Config{MaxAttempts: 3, InitialDelay: time.Hour}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

// TestDo_SucceedsOnSecondAttempt verifies that fn is retried after the first
// failure and the nil error from the second call is returned.
func TestDo_SucceedsOnSecondAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Do(context.Background(), retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   2.0,
	}, func() error {
		calls++
		if calls == 1 {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after retry, got %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

// TestDo_SucceedsOnLastAttempt verifies that success on the final attempt is
// returned correctly.
func TestDo_SucceedsOnLastAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	cfg := retry.Config{MaxAttempts: 4, InitialDelay: time.Millisecond, Multiplier: 1.0}
	err := retry.Do(context.Background(), cfg, func() error {
		calls++
		if calls < 4 {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil on last attempt, got %v", err)
	}
	if calls != 4 {
		t.Errorf("expected 4 calls, got %d", calls)
	}
}

// TestDo_FailsAllAttempts verifies that the last error is returned when every
// attempt fails.
func TestDo_FailsAllAttempts(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Do(context.Background(), retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

// TestDo_MaxAttemptsOne verifies that MaxAttempts=1 means no retry (fn called
// exactly once).
func TestDo_MaxAttemptsOne(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Do(context.Background(), retry.Config{MaxAttempts: 1, InitialDelay: time.Hour}, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

// TestDo_ZeroMaxAttemptsTreatedAsOne verifies that MaxAttempts≤0 is treated as 1.
func TestDo_ZeroMaxAttemptsTreatedAsOne(t *testing.T) {
	t.Parallel()

	calls := 0
	_ = retry.Do(context.Background(), retry.Config{MaxAttempts: 0}, func() error {
		calls++
		return sentinel
	})
	if calls != 1 {
		t.Errorf("MaxAttempts=0: expected 1 call, got %d", calls)
	}

	calls = 0
	_ = retry.Do(context.Background(), retry.Config{MaxAttempts: -5}, func() error {
		calls++
		return sentinel
	})
	if calls != 1 {
		t.Errorf("MaxAttempts=-5: expected 1 call, got %d", calls)
	}
}

// TestDo_MultiplierLessThanOneClampedToOne verifies that Multiplier<1.0
// results in a constant delay (treated as 1.0).
func TestDo_MultiplierLessThanOneClampedToOne(t *testing.T) {
	t.Parallel()

	calls := 0
	// If multiplier were 0 the second delay would be 0 ns — still no hang.
	// We just verify all attempts are made.
	err := retry.Do(context.Background(), retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   0.0, // should be clamped to 1.0
	}, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

// TestDo_MaxDelayCaps verifies that the inter-retry delay never exceeds MaxDelay.
func TestDo_MaxDelayCaps(t *testing.T) {
	t.Parallel()

	// With Multiplier=1000 and InitialDelay=1ms, MaxDelay=5ms, the test would
	// hang if capping were not applied.  Use a tight timeout to enforce it.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	calls := 0
	err := retry.Do(ctx, retry.Config{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		MaxDelay:     5 * time.Millisecond,
		Multiplier:   1000.0, // without capping: 1ms → 1s → 1000s …
	}, func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 5 {
		t.Errorf("expected 5 calls, got %d (context: %v)", calls, ctx.Err())
	}
}

// TestDo_ContextCancelledDuringWait verifies that a context cancelled while
// waiting for the next retry returns ctx.Err() promptly.
func TestDo_ContextCancelledDuringWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	err := retry.Do(ctx, retry.Config{
		MaxAttempts:  10,
		InitialDelay: time.Second, // long enough for cancel to fire
		Multiplier:   1.0,
	}, func() error {
		calls++
		if calls == 1 {
			// Cancel context after the first failure so the retry wait is aborted.
			cancel()
		}
		return sentinel
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected fn to be called exactly once before cancel, got %d", calls)
	}
}

// TestDo_ContextAlreadyCancelled verifies that Do calls fn once even when the
// context is already cancelled on entry (first attempt is always immediate).
func TestDo_ContextAlreadyCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling Do

	calls := 0
	err := retry.Do(ctx, retry.Config{MaxAttempts: 3, InitialDelay: time.Millisecond}, func() error {
		calls++
		return sentinel
	})

	// First attempt runs immediately; the retry wait then sees ctx.Done().
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestDo_DefaultConfig verifies the Default config is a valid configuration.
func TestDo_DefaultConfig(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Do(context.Background(), retry.Default, func() error {
		calls++
		if calls < retry.Default.MaxAttempts {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Default config: expected success on last attempt, got %v", err)
	}
	if calls != retry.Default.MaxAttempts {
		t.Errorf("Default config: expected %d calls, got %d", retry.Default.MaxAttempts, calls)
	}
}
