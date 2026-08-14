// Package namespace provides Namespace, a unified merged view across
// multiple SiteMounts.
//
// When the same object key appears at more than one site, the entry from
// the highest-priority site (lowest index in the slice passed to New) wins.
// Unreachable sites are skipped rather than returning an error for the
// entire listing — GlobalFS is designed for partial availability.
package namespace

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
)

// Namespace provides a unified, merged view across multiple SiteMounts.
// It is safe for concurrent use.
type Namespace struct {
	mu    sync.RWMutex
	sites []*site.SiteMount
}

// New creates a Namespace from an ordered slice of SiteMounts.
// Sites listed earlier have higher priority: their keys shadow identical
// keys from later sites.
func New(sites ...*site.SiteMount) *Namespace {
	cp := make([]*site.SiteMount, len(sites))
	copy(cp, sites)
	return &Namespace{sites: cp}
}

// AddSite appends a site at the lowest priority.
func (n *Namespace) AddSite(s *site.SiteMount) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sites = append(n.sites, s)
}

// Sites returns a snapshot of the ordered site list (highest priority first).
func (n *Namespace) Sites() []*site.SiteMount {
	n.mu.RLock()
	cp := make([]*site.SiteMount, len(n.sites))
	copy(cp, n.sites)
	n.mu.RUnlock()
	return cp
}

// List returns up to limit objects under prefix, merged across all sites.
//
// Results are ordered lexicographically by key, which is the order an S3 client
// expects and the order that makes truncation meaningful.  Keys are
// deduplicated: the entry from the highest-priority site holding a key wins.
//
// When one or more sites are unreachable their objects are omitted from the
// result, but the call still returns whatever data the healthy sites provided.
// A non-nil error is returned alongside the partial results so callers can
// detect and surface the degraded state rather than silently returning an
// incomplete listing.
// Pass limit ≤ 0 to retrieve all matching objects.
//
// # Truncation
//
// With limit > 0 the result is the first limit keys of the merged namespace in
// key order — not an arbitrary subset of it (#109).  Two properties combine to
// give that:
//
//   - Each site is asked for limit keys, and every site returns its own matches
//     in key order, so each per-site slice is that site's first limit keys.
//   - Sorting the union and cutting at limit therefore cuts the true merged
//     order, because a key among the merged first limit must also be among its
//     own site's first limit.  Nothing a site withheld under its own bound can
//     belong in the answer.
//
// So the per-site bound — which exists because unbounded listing on a large
// bucket is a DoS risk (#57) — costs no correctness once the merge is ordered.
// Before it was, the fixed site iteration order decided the outcome: the first
// site consumed the whole budget and a key held only on a lower-priority site
// was unreachable at every limit.
//
// The last key of a truncated result is a valid resumption point for a caller
// that wants to page, since the order is now total and stable.  Threading a
// start_after through the API and the objectfs SDK, which exposes no such
// parameter today, is left to the API-level work in #109's follow-up.
func (n *Namespace) List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	// Take a snapshot under the read lock so AddSite cannot race with
	// the fan-out goroutines below (fixes #39).
	n.mu.RLock()
	sites := make([]*site.SiteMount, len(n.sites))
	copy(sites, n.sites)
	n.mu.RUnlock()

	// Sites are visited highest-priority first, so the first entry recorded for
	// a key is the one that wins; later sites' copies are dropped on sight
	// rather than compared, which is what makes priority shadowing independent
	// of the sort below.
	byKey := make(map[string]objectfstypes.ObjectInfo)
	var siteErrs []error

	for _, s := range sites {
		items, err := s.List(ctx, prefix, limit)
		if err != nil {
			siteErrs = append(siteErrs, fmt.Errorf("site %q: %w", s.Name(), err))
			continue
		}
		for _, item := range items {
			if _, exists := byKey[item.Key]; !exists {
				byKey[item.Key] = item
			}
		}
	}

	result := make([]objectfstypes.ObjectInfo, 0, len(byKey))
	for _, item := range byKey {
		result = append(result, item)
	}
	// Sorting is what makes the truncation below well-defined; map iteration
	// order is deliberately randomised in Go, so without it the result would
	// not even be stable between two identical calls.
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	if len(siteErrs) > 0 {
		return result, fmt.Errorf("coordinator: List %q: partial results (%d site(s) unavailable): %w",
			prefix, len(siteErrs), errors.Join(siteErrs...))
	}
	return result, nil
}

// Close closes all sites in the namespace, returning the first error
// encountered (subsequent errors are still attempted).
func (n *Namespace) Close() error {
	n.mu.RLock()
	sites := make([]*site.SiteMount, len(n.sites))
	copy(sites, n.sites)
	n.mu.RUnlock()

	var firstErr error
	for _, s := range sites {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
