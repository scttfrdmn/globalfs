package circuitbreaker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
)

// TestBreaker_InitiallyClosed verifies that new resources start in Closed state
// and that Allow returns true before any failures are recorded.
func TestBreaker_InitiallyClosed(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(3, time.Second)

	if got := b.State("site-a"); got != circuitbreaker.StateClosed {
		t.Errorf("initial state: got %v, want Closed", got)
	}
	if !b.Allow("site-a") {
		t.Error("Allow should return true for a Closed circuit")
	}
}

// TestBreaker_OpensAfterThreshold verifies that the circuit opens after exactly
// threshold consecutive failures.
func TestBreaker_OpensAfterThreshold(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(3, time.Hour)

	for i := 1; i <= 2; i++ {
		b.RecordFailure("site-a")
		if got := b.State("site-a"); got != circuitbreaker.StateClosed {
			t.Errorf("after %d failures: expected Closed, got %v", i, got)
		}
	}

	b.RecordFailure("site-a")
	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("after threshold failures: expected Open, got %v", got)
	}
}

// TestBreaker_StaysOpenDuringCooldown verifies that Allow returns false while
// the cooldown has not yet elapsed.
func TestBreaker_StaysOpenDuringCooldown(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, time.Hour)
	b.RecordFailure("site-a")

	if b.Allow("site-a") {
		t.Error("Allow should return false for an Open circuit during cooldown")
	}
	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("expected Open, got %v", got)
	}
}

// TestBreaker_HalfOpenAfterCooldown verifies that the first Allow call after
// the cooldown transitions the circuit to HalfOpen and returns true.
func TestBreaker_HalfOpenAfterCooldown(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, 5*time.Millisecond)
	b.RecordFailure("site-a")

	time.Sleep(20 * time.Millisecond)

	if !b.Allow("site-a") {
		t.Error("Allow should return true after cooldown (probe)")
	}
	if got := b.State("site-a"); got != circuitbreaker.StateHalfOpen {
		t.Errorf("expected HalfOpen, got %v", got)
	}
}

// TestBreaker_ClosesAfterProbeSucceeds verifies that a successful probe
// transitions HalfOpen → Closed.
func TestBreaker_ClosesAfterProbeSucceeds(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, 5*time.Millisecond)
	b.RecordFailure("site-a")
	time.Sleep(20 * time.Millisecond)

	b.Allow("site-a") // transitions to HalfOpen; probe in flight
	b.RecordSuccess("site-a")

	if got := b.State("site-a"); got != circuitbreaker.StateClosed {
		t.Errorf("expected Closed after probe success, got %v", got)
	}
	// Consecutive requests should be allowed immediately.
	if !b.Allow("site-a") {
		t.Error("Allow should return true after circuit closes")
	}
}

// TestBreaker_ReopensAfterProbeFailure verifies that a failed probe
// transitions HalfOpen → Open and resets the cooldown timer.
func TestBreaker_ReopensAfterProbeFailure(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, 5*time.Millisecond)
	b.RecordFailure("site-a")
	time.Sleep(20 * time.Millisecond)

	b.Allow("site-a") // HalfOpen; probe in flight
	b.RecordFailure("site-a")

	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("expected Open after probe failure, got %v", got)
	}
	// Cooldown timer reset — block immediately.
	if b.Allow("site-a") {
		t.Error("Allow should return false after circuit re-opens")
	}
}

// TestBreaker_OnlyOneHalfOpenProbe verifies that only the first concurrent
// Allow call during HalfOpen returns true; subsequent callers are blocked.
func TestBreaker_OnlyOneHalfOpenProbe(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, 5*time.Millisecond)
	b.RecordFailure("site-a")
	time.Sleep(20 * time.Millisecond)

	// Simulate two concurrent callers.
	first := b.Allow("site-a")
	second := b.Allow("site-a")

	if !first {
		t.Error("first Allow should return true (probe)")
	}
	if second {
		t.Error("second Allow should return false (probe already in flight)")
	}
}

// TestBreaker_SuccessResetsFailureCounter verifies that RecordSuccess resets
// the consecutive failure count so the full threshold is required again.
func TestBreaker_SuccessResetsFailureCounter(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(3, time.Hour)

	b.RecordFailure("site-a")
	b.RecordFailure("site-a")
	b.RecordSuccess("site-a") // reset counter

	// Need threshold new failures to open again.
	b.RecordFailure("site-a")
	b.RecordFailure("site-a")
	if got := b.State("site-a"); got != circuitbreaker.StateClosed {
		t.Errorf("expected Closed (counter reset by success), got %v", got)
	}

	b.RecordFailure("site-a")
	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("expected Open after threshold reached again, got %v", got)
	}
}

// TestBreaker_IndependentResources verifies that each resource has its own
// independent circuit state.
func TestBreaker_IndependentResources(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, time.Hour)
	b.RecordFailure("site-a")

	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("site-a: expected Open, got %v", got)
	}
	if got := b.State("site-b"); got != circuitbreaker.StateClosed {
		t.Errorf("site-b: expected Closed (independent), got %v", got)
	}
}

// TestBreaker_Reset verifies that Reset clears all state.
func TestBreaker_Reset(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, time.Hour)
	b.RecordFailure("site-a")
	b.RecordFailure("site-b")

	b.Reset()

	for _, name := range []string{"site-a", "site-b"} {
		if got := b.State(name); got != circuitbreaker.StateClosed {
			t.Errorf("%s: expected Closed after Reset, got %v", name, got)
		}
		if !b.Allow(name) {
			t.Errorf("%s: Allow should return true after Reset", name)
		}
	}
}

// TestBreaker_ThresholdOne verifies that a threshold of 1 opens the circuit
// after a single failure.
func TestBreaker_ThresholdOne(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, time.Hour)
	b.RecordFailure("site-a")

	if got := b.State("site-a"); got != circuitbreaker.StateOpen {
		t.Errorf("expected Open after 1 failure (threshold=1), got %v", got)
	}
}

// TestBreaker_NonPositiveThresholdClamped verifies that threshold ≤ 0 is
// treated as 1 (the minimum).
func TestBreaker_NonPositiveThresholdClamped(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(0, time.Hour)
	b.RecordFailure("x")

	if got := b.State("x"); got != circuitbreaker.StateOpen {
		t.Errorf("expected Open after 1 failure (threshold clamped to 1), got %v", got)
	}
}

// TestBreaker_StateString verifies the string representation of each state.
func TestBreaker_StateString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		s    circuitbreaker.State
		want string
	}{
		{circuitbreaker.StateClosed, "closed"},
		{circuitbreaker.StateOpen, "open"},
		{circuitbreaker.StateHalfOpen, "half-open"},
		{circuitbreaker.State(99), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("State(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

// TestBreaker_State_TransitionsHalfOpenBeforeAllow verifies that State writes
// the Open → HalfOpen transition when the cooldown elapses, so that a
// subsequent Allow call still correctly grants the probe (#48).
func TestBreaker_State_TransitionsHalfOpenBeforeAllow(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(1, 5*time.Millisecond)
	b.RecordFailure("site-a")
	time.Sleep(20 * time.Millisecond)

	// Call State BEFORE Allow: must report HalfOpen and persist the transition.
	if got := b.State("site-a"); got != circuitbreaker.StateHalfOpen {
		t.Fatalf("State after cooldown: got %v, want HalfOpen", got)
	}

	// Allow must still grant the probe even though State already transitioned.
	if !b.Allow("site-a") {
		t.Error("Allow should return true for the first HalfOpen probe")
	}
	// A second Allow must be blocked (probe in flight).
	if b.Allow("site-a") {
		t.Error("second Allow should return false (probe already in flight)")
	}
}

// TestBreaker_ConcurrentSafe verifies that concurrent Allow/RecordSuccess/
// RecordFailure calls on the same resource do not race.
func TestBreaker_ConcurrentSafe(t *testing.T) {
	t.Parallel()

	b := circuitbreaker.New(5, time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if b.Allow("site-x") {
				if i%2 == 0 {
					b.RecordSuccess("site-x")
				} else {
					b.RecordFailure("site-x")
				}
			}
			_ = b.State("site-x")
		}(i)
	}
	wg.Wait()
}
