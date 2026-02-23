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

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
)

// Namespace provides a unified, merged view across multiple SiteMounts.
type Namespace struct {
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
	n.sites = append(n.sites, s)
}

// Sites returns the ordered list of sites (highest priority first).
func (n *Namespace) Sites() []*site.SiteMount {
	return n.sites
}

// List returns up to limit objects under prefix, merged across all sites.
//
// Keys are deduplicated: the first site that returns a key wins.
// When one or more sites are unreachable their objects are omitted from the
// result, but the call still returns whatever data the healthy sites provided.
// A non-nil error is returned alongside the partial results so callers can
// detect and surface the degraded state rather than silently returning an
// incomplete listing.
// Pass limit ≤ 0 to retrieve all matching objects.
func (n *Namespace) List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	seen := make(map[string]struct{})
	var result []objectfstypes.ObjectInfo
	var siteErrs []error

	for _, s := range n.sites {
		items, err := s.List(ctx, prefix, 0)
		if err != nil {
			siteErrs = append(siteErrs, fmt.Errorf("site %q: %w", s.Name(), err))
			continue
		}
		for _, item := range items {
			if _, exists := seen[item.Key]; !exists {
				seen[item.Key] = struct{}{}
				result = append(result, item)
			}
		}
	}

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
	var firstErr error
	for _, s := range n.sites {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
