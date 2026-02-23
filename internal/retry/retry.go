// Package retry provides exponential-backoff retry logic for operations that
// may fail transiently.
//
// # Usage
//
//	cfg := retry.Config{
//	    MaxAttempts:  3,
//	    InitialDelay: 100 * time.Millisecond,
//	    MaxDelay:     2 * time.Second,
//	    Multiplier:   2.0,
//	}
//
//	err := retry.Do(ctx, cfg, func() error {
//	    return doOperation()
//	})
//
// # Timing
//
// The first call to fn is immediate (no pre-delay).  If it fails, subsequent
// attempts are separated by an exponentially growing pause:
//
//	attempt 0: immediate
//	attempt 1: wait InitialDelay
//	attempt 2: wait InitialDelay × Multiplier
//	attempt 3: wait min(InitialDelay × Multiplier², MaxDelay)
//	…
//
// Context cancellation aborts the wait and returns ctx.Err().
package retry

import (
	"context"
	"time"
)

// Config specifies how retries should be attempted.
type Config struct {
	// MaxAttempts is the total number of calls made (first attempt + retries).
	// Values ≤ 0 are treated as 1 (i.e. no retry).
	MaxAttempts int

	// InitialDelay is the pause before the first retry (attempt 1).
	InitialDelay time.Duration

	// MaxDelay caps the inter-retry pause.  0 means no cap.
	MaxDelay time.Duration

	// Multiplier scales the delay after each retry.  Values < 1.0 are
	// treated as 1.0 (constant backoff).
	Multiplier float64
}

// Default is a conservative configuration suitable for S3 object operations:
// up to 3 total attempts with 100 ms → 200 ms backoff.
var Default = Config{
	MaxAttempts:  3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     2 * time.Second,
	Multiplier:   2.0,
}

// Do calls fn up to cfg.MaxAttempts times, pausing between attempts using
// exponential backoff.  It returns the first nil error from fn, or the last
// non-nil error if every attempt fails.
//
// ctx cancellation or expiry during a backoff sleep aborts immediately and
// returns ctx.Err().  An already-cancelled context causes fn to be called
// once and returns the cancellation error.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	attempts := cfg.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	multiplier := cfg.Multiplier
	if multiplier < 1.0 {
		multiplier = 1.0
	}

	delay := cfg.InitialDelay
	var err error

	for i := 0; i < attempts; i++ {
		if i > 0 {
			// Wait before retry, honouring context cancellation.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			// Scale delay for the next retry.
			next := time.Duration(float64(delay) * multiplier)
			if cfg.MaxDelay > 0 && next > cfg.MaxDelay {
				next = cfg.MaxDelay
			}
			delay = next
		}

		if err = fn(); err == nil {
			return nil
		}
	}

	return err
}
