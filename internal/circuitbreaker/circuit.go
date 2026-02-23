// Package circuitbreaker provides a concurrent-safe per-resource circuit
// breaker that temporarily blocks requests to resources that are failing
// repeatedly, giving them time to recover.
//
// # States
//
// Each resource tracked by a [Breaker] moves through three states:
//
//   - Closed: normal operation; all requests are allowed through.
//   - Open: the circuit is tripped after consecutive failures exceed the
//     configured threshold; requests are blocked until the cooldown elapses.
//   - HalfOpen: after the cooldown, a single probe request is allowed through.
//     A successful probe closes the circuit; a failed probe re-opens it.
//
// # Usage
//
//	cb := circuitbreaker.New(5, 30*time.Second)
//
//	if cb.Allow("site-a") {
//	    err := doRequest()
//	    if err != nil {
//	        cb.RecordFailure("site-a")
//	    } else {
//	        cb.RecordSuccess("site-a")
//	    }
//	}
package circuitbreaker

import (
	"sync"
	"time"
)

// State represents the circuit state for a single resource.
type State int

const (
	// StateClosed is normal operation — requests flow through.
	StateClosed State = iota
	// StateOpen indicates the circuit is tripped — requests are blocked.
	StateOpen
	// StateHalfOpen allows a single probe request to test recovery.
	StateHalfOpen
)

// String returns a human-readable state label.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker is a concurrent-safe per-resource circuit breaker.
//
// Resources are identified by arbitrary string names (e.g. site names).
// Each resource has its own independent state machine.
type Breaker struct {
	mu        sync.Mutex
	threshold int           // consecutive failures needed to open
	cooldown  time.Duration // time before transitioning Open → HalfOpen
	resources map[string]*resourceState
}

type resourceState struct {
	state    State
	failures int       // consecutive failure count (reset on success)
	openedAt time.Time // when the circuit last transitioned to Open
	probing  bool      // true when a HalfOpen probe is in flight
}

// New creates a Breaker that opens after threshold consecutive failures and
// allows one probe request after cooldown has elapsed since the circuit opened.
// threshold must be ≥ 1; values ≤ 0 are treated as 1.
func New(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 1
	}
	return &Breaker{
		threshold: threshold,
		cooldown:  cooldown,
		resources: make(map[string]*resourceState),
	}
}

// Allow reports whether a request to the named resource is permitted and
// updates internal state accordingly.
//
//   - Closed:   always permitted.
//   - Open:     permitted only after the cooldown has elapsed; the resource
//     transitions to HalfOpen and the first caller is designated the probe.
//   - HalfOpen: permitted for the first concurrent caller (the probe); all
//     subsequent callers are blocked until the probe completes.
func (b *Breaker) Allow(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.getOrCreate(name)
	switch s.state {
	case StateClosed:
		return true

	case StateOpen:
		if time.Since(s.openedAt) < b.cooldown {
			return false
		}
		// Cooldown elapsed — transition to HalfOpen and allow this probe.
		s.state = StateHalfOpen
		s.probing = true
		return true

	case StateHalfOpen:
		if s.probing {
			// Another probe is already in flight; block this caller.
			return false
		}
		s.probing = true
		return true
	}
	return false
}

// RecordSuccess records a successful operation for the named resource.
// Resets the consecutive failure counter and closes the circuit (regardless
// of its current state).
func (b *Breaker) RecordSuccess(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.getOrCreate(name)
	s.failures = 0
	s.probing = false
	s.state = StateClosed
}

// RecordFailure records a failed operation for the named resource.
// When consecutive failures reach the threshold, or when the circuit is
// HalfOpen, the circuit opens (or re-opens) and the cooldown timer resets.
func (b *Breaker) RecordFailure(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.getOrCreate(name)
	s.probing = false
	s.failures++
	if s.state == StateHalfOpen || s.failures >= b.threshold {
		s.state = StateOpen
		s.openedAt = time.Now()
	}
}

// State returns the current circuit state for the named resource.
// When the cooldown has elapsed on an Open circuit, State writes the
// Open → HalfOpen transition so that subsequent Allow calls see a consistent
// state (matching the behaviour of Allow itself).
// Resources that have never been seen are reported as Closed.
func (b *Breaker) State(name string) State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.resources[name]; ok {
		// Persist Open → HalfOpen transition for consistency with Allow.
		if s.state == StateOpen && time.Since(s.openedAt) >= b.cooldown {
			s.state = StateHalfOpen
		}
		return s.state
	}
	return StateClosed
}

// Reset clears all circuit state, returning every resource to Closed.
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resources = make(map[string]*resourceState)
}

// getOrCreate returns the state for name, creating a Closed entry if absent.
// Caller must hold b.mu.
func (b *Breaker) getOrCreate(name string) *resourceState {
	if s, ok := b.resources[name]; ok {
		return s
	}
	s := &resourceState{state: StateClosed}
	b.resources[name] = s
	return s
}
