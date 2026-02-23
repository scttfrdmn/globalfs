package namespace

import (
	"context"
	"errors"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

type mockClient struct {
	objects []objectfstypes.ObjectInfo
	listErr error
}

func (m *mockClient) List(_ context.Context, prefix string, _ int) ([]objectfstypes.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var result []objectfstypes.ObjectInfo
	for _, o := range m.objects {
		if len(o.Key) >= len(prefix) && o.Key[:len(prefix)] == prefix {
			result = append(result, o)
		}
	}
	return result, nil
}

func (m *mockClient) Head(_ context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	for _, o := range m.objects {
		if o.Key == key {
			cp := o
			return &cp, nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockClient) Health(_ context.Context) error { return nil }
func (m *mockClient) Close() error                   { return nil }

func makeMount(name string, role types.SiteRole, objects ...objectfstypes.ObjectInfo) *site.SiteMount {
	return site.New(name, role, &mockClient{objects: objects})
}

func makeErrorMount(name string) *site.SiteMount {
	return site.New(name, types.SiteRoleBurst, &mockClient{listErr: errors.New("site unavailable")})
}

func obj(key string, size int64) objectfstypes.ObjectInfo {
	return objectfstypes.ObjectInfo{Key: key, Size: size, LastModified: time.Now()}
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestNamespace_List_MergesAcrossTwoSites is the primary integration-style
// test described in issue #83: two in-process "sites" with distinct objects
// should produce a merged listing.
func TestNamespace_List_MergesAcrossTwoSites(t *testing.T) {
	t.Parallel()

	siteA := makeMount("onprem", types.SiteRolePrimary,
		obj("data/genome.bam", 1_000_000),
		obj("data/sample1.fastq", 50_000),
	)
	siteB := makeMount("cloud", types.SiteRoleBurst,
		obj("data/sample2.fastq", 60_000),
		obj("data/reference.fa", 200_000),
	)

	ns := New(siteA, siteB)
	items, err := ns.List(context.Background(), "data/", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}

	if len(items) != 4 {
		t.Errorf("expected 4 merged items, got %d: %v", len(items), items)
	}

	keys := make(map[string]struct{})
	for _, it := range items {
		keys[it.Key] = struct{}{}
	}
	for _, want := range []string{
		"data/genome.bam", "data/sample1.fastq",
		"data/sample2.fastq", "data/reference.fa",
	} {
		if _, ok := keys[want]; !ok {
			t.Errorf("expected key %q in merged result", want)
		}
	}
}

// TestNamespace_List_DeduplicatesKeys verifies that a key present at both
// sites appears only once, with the entry from the higher-priority site.
func TestNamespace_List_DeduplicatesKeys(t *testing.T) {
	t.Parallel()

	// "shared.bam" exists at both sites; siteA's version (size=999) should win.
	siteA := makeMount("primary", types.SiteRolePrimary,
		obj("shared.bam", 999),
	)
	siteB := makeMount("backup", types.SiteRoleBackup,
		obj("shared.bam", 111), // same key, different size
		obj("extra.bam", 500),
	)

	ns := New(siteA, siteB)
	items, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}

	var sharedCount int
	var sharedSize int64
	for _, it := range items {
		if it.Key == "shared.bam" {
			sharedCount++
			sharedSize = it.Size
		}
	}
	if sharedCount != 1 {
		t.Errorf("shared.bam should appear exactly once, got %d", sharedCount)
	}
	if sharedSize != 999 {
		t.Errorf("shared.bam size: got %d, want 999 (primary site wins)", sharedSize)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 unique items, got %d", len(items))
	}
}

// TestNamespace_List_RespectsLimit verifies that the limit parameter is
// applied to the merged result.
func TestNamespace_List_RespectsLimit(t *testing.T) {
	t.Parallel()

	siteA := makeMount("a", types.SiteRolePrimary,
		obj("k1", 1), obj("k2", 2), obj("k3", 3),
	)
	siteB := makeMount("b", types.SiteRoleBurst,
		obj("k4", 4), obj("k5", 5),
	)

	ns := New(siteA, siteB)
	items, err := ns.List(context.Background(), "", 2)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected limit=2 items, got %d", len(items))
	}
}

// TestNamespace_List_SkipsUnavailableSite verifies that an unavailable site
// does not block listing from healthy sites.
func TestNamespace_List_SkipsUnavailableSite(t *testing.T) {
	t.Parallel()

	healthy := makeMount("healthy", types.SiteRolePrimary,
		obj("data/file.txt", 42),
	)
	broken := makeErrorMount("broken")

	ns := New(healthy, broken)
	items, err := ns.List(context.Background(), "data/", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("expected 1 item from healthy site, got %d", len(items))
	}
	if items[0].Key != "data/file.txt" {
		t.Errorf("expected key data/file.txt, got %q", items[0].Key)
	}
}

// TestNamespace_AddSite verifies dynamic site addition.
func TestNamespace_AddSite(t *testing.T) {
	t.Parallel()

	ns := New()
	if len(ns.Sites()) != 0 {
		t.Fatalf("expected empty namespace, got %d sites", len(ns.Sites()))
	}

	ns.AddSite(makeMount("a", types.SiteRolePrimary, obj("x", 1)))
	ns.AddSite(makeMount("b", types.SiteRoleBurst, obj("y", 2)))

	items, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
}
