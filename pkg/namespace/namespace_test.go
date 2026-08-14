package namespace

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

type mockClient struct {
	objects []objectfstypes.ObjectInfo
	listErr error
}

func (m *mockClient) Get(_ context.Context, _ string, _, _ int64) ([]byte, error) { return nil, nil }
func (m *mockClient) Put(_ context.Context, _ string, _ []byte) error             { return nil }
func (m *mockClient) Delete(_ context.Context, _ string) error                    { return nil }

// List mirrors the contract of the real backend: matches are returned in key
// order, and limit takes the first that many of them.  Both matter for the
// merge tests below — an unsorted or unbounded mock would let a broken merge
// pass by accident (objectfs's ListObjects returns S3's lexicographic order and
// stops once limit entries have been collected).
func (m *mockClient) List(_ context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var result []objectfstypes.ObjectInfo
	for _, o := range m.objects {
		if len(o.Key) >= len(prefix) && o.Key[:len(prefix)] == prefix {
			result = append(result, o)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	if limit > 0 && len(result) > limit {
		result = result[:limit]
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

// ─── Ordered merge (#109) ─────────────────────────────────────────────────────

// keysOf is the shape every assertion below wants: the result as a key slice, in
// the order List returned it.
func keysOf(items []objectfstypes.ObjectInfo) []string {
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = it.Key
	}
	return keys
}

func equalKeys(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestNamespace_List_LowPriorityFirstKeyVisibleAtLimitOne is the test #109 asks
// for, and the whole point of the ordered merge.
//
// "aaa.dat" is the lexicographically first key in the namespace and lives only
// on the *lower*-priority site.  Under the old concatenate-then-truncate merge
// the higher-priority site's three keys consumed the entire budget and it was
// unreachable — not just at limit=1 but at every limit, because raising the
// limit raised the first site's contribution in lockstep.
func TestNamespace_List_LowPriorityFirstKeyVisibleAtLimitOne(t *testing.T) {
	t.Parallel()

	primary := makeMount("onprem", types.SiteRolePrimary,
		obj("mmm.dat", 1), obj("nnn.dat", 2), obj("ooo.dat", 3),
	)
	burst := makeMount("cloud", types.SiteRoleBurst,
		obj("aaa.dat", 4),
	)

	ns := New(primary, burst)

	items, err := ns.List(context.Background(), "", 1)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if got := keysOf(items); !equalKeys(got, []string{"aaa.dat"}) {
		t.Errorf("List(limit=1) = %v, want [aaa.dat]: the first key of the merged "+
			"namespace lives on the lower-priority site and must still be reachable", got)
	}

	// And the truncation point moves in key order as the limit grows, rather
	// than admitting more of whichever site happened to be visited first.
	items, err = ns.List(context.Background(), "", 2)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if got := keysOf(items); !equalKeys(got, []string{"aaa.dat", "mmm.dat"}) {
		t.Errorf("List(limit=2) = %v, want [aaa.dat mmm.dat]", got)
	}
}

// TestNamespace_List_SortsAcrossSites asserts the total order, which is what
// makes the truncation above well-defined and a caller's "resume after the last
// key I saw" valid.  Interleaved keys across three sites in a priority order
// that is deliberately not the key order.
func TestNamespace_List_SortsAcrossSites(t *testing.T) {
	t.Parallel()

	ns := New(
		makeMount("a", types.SiteRolePrimary, obj("b", 1), obj("e", 2)),
		makeMount("b", types.SiteRoleBurst, obj("a", 3), obj("f", 4)),
		makeMount("c", types.SiteRoleBackup, obj("c", 5), obj("d", 6)),
	)

	items, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	want := []string{"a", "b", "c", "d", "e", "f"}
	if got := keysOf(items); !equalKeys(got, want) {
		t.Errorf("List = %v, want %v (lexicographic across sites)", got, want)
	}
}

// TestNamespace_List_OrderIsStableAcrossCalls guards the property that the
// intermediate map cannot supply on its own: Go randomises map iteration order,
// so a merge that collected into a map and skipped the sort would return a
// different permutation on successive identical calls — and a truncated one
// would return a different *set*.
func TestNamespace_List_OrderIsStableAcrossCalls(t *testing.T) {
	t.Parallel()

	ns := New(
		makeMount("a", types.SiteRolePrimary, obj("k3", 1), obj("k1", 2), obj("k5", 3)),
		makeMount("b", types.SiteRoleBurst, obj("k4", 4), obj("k2", 5), obj("k6", 6)),
	)

	first, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	for i := 0; i < 20; i++ {
		again, err := ns.List(context.Background(), "", 0)
		if err != nil {
			t.Fatalf("List: unexpected error: %v", err)
		}
		if got, want := keysOf(again), keysOf(first); !equalKeys(got, want) {
			t.Fatalf("call %d returned %v, first call returned %v: order must be stable", i+2, got, want)
		}
	}
}

// TestNamespace_List_DedupPrefersHigherPriorityUnderOrdering pins that the sort
// did not become the thing that decides which copy of a shared key wins.
// Priority shadowing happens as sites are visited; the sort only arranges what
// survived.  Here the primary's copy is the one to keep even though it is
// indistinguishable by key from the backup's.
func TestNamespace_List_DedupPrefersHigherPriorityUnderOrdering(t *testing.T) {
	t.Parallel()

	ns := New(
		makeMount("primary", types.SiteRolePrimary, obj("shared", 999), obj("zzz", 1)),
		makeMount("backup", types.SiteRoleBackup, obj("aaa", 2), obj("shared", 111)),
	)

	items, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	want := []string{"aaa", "shared", "zzz"}
	if got := keysOf(items); !equalKeys(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	for _, it := range items {
		if it.Key == "shared" && it.Size != 999 {
			t.Errorf("shared size = %d, want 999 (higher-priority site's copy)", it.Size)
		}
	}
}

// TestNamespace_List_TruncatedPartialResultIsStillOrdered covers the interaction
// of the two error-free properties with the degraded path: a failing site does
// not disturb the order of what the others returned, and the partial-results
// error still arrives alongside the data (it is what drives the API's 207).
func TestNamespace_List_TruncatedPartialResultIsStillOrdered(t *testing.T) {
	t.Parallel()

	ns := New(
		makeMount("a", types.SiteRolePrimary, obj("d", 1), obj("b", 2)),
		makeErrorMount("broken"),
		makeMount("c", types.SiteRoleBackup, obj("a", 3), obj("c", 4)),
	)

	items, err := ns.List(context.Background(), "", 3)
	if err == nil {
		t.Error("expected a non-nil partial-results error with one site failing")
	}
	want := []string{"a", "b", "c"}
	if got := keysOf(items); !equalKeys(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}
}

// TestNamespace_List_TruncationMatchesTheFullMerge is the property the per-site
// bound has to satisfy, stated directly: truncating at limit must give the same
// answer as listing everything and taking the first limit keys.  If it ever
// diverges, the per-site bound is discarding a key that belongs in the result —
// exactly the #109 defect, in whatever new form it took.
func TestNamespace_List_TruncationMatchesTheFullMerge(t *testing.T) {
	t.Parallel()

	ns := New(
		makeMount("a", types.SiteRolePrimary, obj("m", 1), obj("c", 2), obj("x", 3)),
		makeMount("b", types.SiteRoleBurst, obj("a", 4), obj("n", 5)),
		makeMount("c", types.SiteRoleBackup, obj("b", 6), obj("z", 7), obj("d", 8)),
	)

	all, err := ns.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List(0): unexpected error: %v", err)
	}
	full := keysOf(all)
	if !sort.StringsAreSorted(full) {
		t.Fatalf("unlimited List is not sorted: %v", full)
	}

	for limit := 1; limit <= len(full); limit++ {
		got, err := ns.List(context.Background(), "", limit)
		if err != nil {
			t.Fatalf("List(%d): unexpected error: %v", limit, err)
		}
		if want := full[:limit]; !equalKeys(keysOf(got), want) {
			t.Errorf("List(%d) = %v, want %v (prefix of the full merge)",
				limit, keysOf(got), want)
		}
	}
}

// TestNamespace_List_SkipsUnavailableSite verifies that an unavailable site
// does not block listing from healthy sites, but that callers are informed of
// the partial results via a non-nil error.
func TestNamespace_List_SkipsUnavailableSite(t *testing.T) {
	t.Parallel()

	healthy := makeMount("healthy", types.SiteRolePrimary,
		obj("data/file.txt", 42),
	)
	broken := makeErrorMount("broken")

	ns := New(healthy, broken)
	items, err := ns.List(context.Background(), "data/", 0)
	// Partial results: error must be non-nil so callers can detect the degraded state.
	if err == nil {
		t.Error("List: expected non-nil error when a site is unavailable (partial results)")
	}
	// Healthy site data must still be returned alongside the error.
	if len(items) != 1 {
		t.Errorf("expected 1 item from healthy site, got %d", len(items))
	}
	if len(items) > 0 && items[0].Key != "data/file.txt" {
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
