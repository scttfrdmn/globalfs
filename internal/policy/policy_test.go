package policy

import (
	"context"
	"testing"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// nullClient satisfies site.ObjectFSClient with no-op methods.
// It is only used to construct SiteMounts; content is irrelevant for
// routing/policy tests.
type nullClient struct{}

func (n *nullClient) Get(_ context.Context, _ string, _, _ int64) ([]byte, error) { return nil, nil }
func (n *nullClient) Put(_ context.Context, _ string, _ []byte) error             { return nil }
func (n *nullClient) Delete(_ context.Context, _ string) error                    { return nil }
func (n *nullClient) List(_ context.Context, _ string, _ int) ([]objectfstypes.ObjectInfo, error) {
	return nil, nil
}
func (n *nullClient) Head(_ context.Context, _ string) (*objectfstypes.ObjectInfo, error) {
	return nil, nil
}
func (n *nullClient) Health(_ context.Context) error { return nil }
func (n *nullClient) Close() error                   { return nil }

// siteMount builds a SiteMount with the given name and role.
func siteMount(name string, role types.SiteRole) *site.SiteMount {
	return site.New(name, role, &nullClient{})
}

// siteNames extracts site names in order.
func siteNames(sites []*site.SiteMount) []string {
	names := make([]string, len(sites))
	for i, s := range sites {
		names[i] = s.Name()
	}
	return names
}

// ─── Rule.matchesKey ──────────────────────────────────────────────────────────

func TestRule_MatchesKey_Exact(t *testing.T) {
	t.Parallel()
	r := Rule{KeyPattern: "data/genome.bam"}
	if !r.matchesKey("data/genome.bam") {
		t.Error("exact pattern should match identical key")
	}
	if r.matchesKey("data/other.bam") {
		t.Error("exact pattern should not match different key")
	}
}

func TestRule_MatchesKey_GlobExtension(t *testing.T) {
	t.Parallel()
	r := Rule{KeyPattern: "*.bam"}
	cases := []struct {
		key  string
		want bool
	}{
		{"genome.bam", true},
		{"sample.bam", true},
		{"genome.fastq", false},
		{"dir/genome.bam", false}, // * does not cross /
	}
	for _, tc := range cases {
		got := r.matchesKey(tc.key)
		if got != tc.want {
			t.Errorf("matchesKey(%q): got %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestRule_MatchesKey_GlobDirectory(t *testing.T) {
	t.Parallel()
	r := Rule{KeyPattern: "data/*"}
	cases := []struct {
		key  string
		want bool
	}{
		{"data/genome.bam", true},
		{"data/sample.fastq", true},
		{"other/genome.bam", false},
		{"data/sub/genome.bam", false}, // * doesn't cross /
	}
	for _, tc := range cases {
		got := r.matchesKey(tc.key)
		if got != tc.want {
			t.Errorf("matchesKey(%q): got %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestRule_MatchesKey_RecursivePrefix(t *testing.T) {
	t.Parallel()
	r := Rule{KeyPattern: "genomes/"}
	cases := []struct {
		key  string
		want bool
	}{
		{"genomes/sample.bam", true},
		{"genomes/sub/deep.bam", true}, // recursive: trailing / matches all depths
		{"other/sample.bam", false},
		{"genomes", false}, // no trailing slash in key
	}
	for _, tc := range cases {
		got := r.matchesKey(tc.key)
		if got != tc.want {
			t.Errorf("matchesKey(%q): got %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestRule_MatchesKey_EmptyPattern(t *testing.T) {
	t.Parallel()
	r := Rule{} // empty KeyPattern matches everything
	for _, key := range []string{"anything", "data/nested/file.bam", ""} {
		if !r.matchesKey(key) {
			t.Errorf("empty pattern should match %q", key)
		}
	}
}

// ─── Rule.matchesOperation ────────────────────────────────────────────────────

func TestRule_MatchesOperation_Empty(t *testing.T) {
	t.Parallel()
	r := Rule{} // empty Operations matches all
	for _, op := range []OperationType{OperationRead, OperationWrite, OperationDelete} {
		if !r.matchesOperation(op) {
			t.Errorf("empty Operations should match %q", op)
		}
	}
}

func TestRule_MatchesOperation_Specific(t *testing.T) {
	t.Parallel()
	r := Rule{Operations: []OperationType{OperationRead, OperationWrite}}
	if !r.matchesOperation(OperationRead) {
		t.Error("should match read")
	}
	if !r.matchesOperation(OperationWrite) {
		t.Error("should match write")
	}
	if r.matchesOperation(OperationDelete) {
		t.Error("should not match delete")
	}
}

// ─── DefaultOrdering ──────────────────────────────────────────────────────────

func TestDefaultOrdering_PrimaryFirst(t *testing.T) {
	t.Parallel()
	sites := []*site.SiteMount{
		siteMount("burst1", types.SiteRoleBurst),
		siteMount("backup1", types.SiteRoleBackup),
		siteMount("primary1", types.SiteRolePrimary),
	}
	ordered := DefaultOrdering(sites)
	names := siteNames(ordered)
	want := []string{"primary1", "backup1", "burst1"}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("position %d: got %q, want %q", i, names[i], n)
		}
	}
}

func TestDefaultOrdering_MultiplePerRole(t *testing.T) {
	t.Parallel()
	sites := []*site.SiteMount{
		siteMount("burst-a", types.SiteRoleBurst),
		siteMount("primary-a", types.SiteRolePrimary),
		siteMount("primary-b", types.SiteRolePrimary),
		siteMount("backup-a", types.SiteRoleBackup),
	}
	ordered := DefaultOrdering(sites)
	names := siteNames(ordered)
	// primaries first preserving relative order, then backup, then burst
	want := []string{"primary-a", "primary-b", "backup-a", "burst-a"}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("position %d: got %q, want %q", i, names[i], n)
		}
	}
}

// ─── Engine.Route ─────────────────────────────────────────────────────────────

func TestEngine_Route_NoRules_DefaultOrdering(t *testing.T) {
	t.Parallel()
	e := New() // no rules
	sites := []*site.SiteMount{
		siteMount("burst", types.SiteRoleBurst),
		siteMount("primary", types.SiteRolePrimary),
	}
	result, err := e.Route(OperationRead, "any.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	if len(result) != 2 || result[0].Name() != "primary" {
		t.Errorf("expected primary first; got %v", siteNames(result))
	}
}

func TestEngine_Route_RuleMatchesTargetRoles(t *testing.T) {
	t.Parallel()
	// Rule: all .bam reads → primary only.
	e := New(Rule{
		Name:        "bam-primary",
		KeyPattern:  "*.bam",
		Operations:  []OperationType{OperationRead},
		TargetRoles: []types.SiteRole{types.SiteRolePrimary},
		Priority:    10,
	})

	sites := []*site.SiteMount{
		siteMount("primary", types.SiteRolePrimary),
		siteMount("backup", types.SiteRoleBackup),
		siteMount("burst", types.SiteRoleBurst),
	}

	result, err := e.Route(OperationRead, "genome.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Name() != "primary" {
		t.Errorf("expected only primary; got %v", siteNames(result))
	}
}

func TestEngine_Route_WrongOperation_NoMatch(t *testing.T) {
	t.Parallel()
	// Rule applies only to reads.
	e := New(Rule{
		Name:        "bam-read-primary",
		KeyPattern:  "*.bam",
		Operations:  []OperationType{OperationRead},
		TargetRoles: []types.SiteRole{types.SiteRolePrimary},
		Priority:    5,
	})

	sites := []*site.SiteMount{
		siteMount("primary", types.SiteRolePrimary),
		siteMount("burst", types.SiteRoleBurst),
	}

	// Write operation: rule should NOT match → default ordering (all sites).
	result, err := e.Route(OperationWrite, "genome.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 sites (default); got %v", siteNames(result))
	}
}

func TestEngine_Route_WrongKey_NoMatch(t *testing.T) {
	t.Parallel()
	e := New(Rule{
		Name:        "bam-only",
		KeyPattern:  "*.bam",
		TargetRoles: []types.SiteRole{types.SiteRolePrimary},
		Priority:    5,
	})

	sites := []*site.SiteMount{
		siteMount("primary", types.SiteRolePrimary),
		siteMount("burst", types.SiteRoleBurst),
	}

	// .fastq key → rule doesn't match → default ordering (all sites).
	result, err := e.Route(OperationRead, "sample.fastq", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 sites; got %v", siteNames(result))
	}
}

func TestEngine_Route_PriorityOrdering(t *testing.T) {
	t.Parallel()
	// Two rules both matching *.bam: lower Priority number wins.
	e := New(
		Rule{
			Name:        "low-priority-burst",
			KeyPattern:  "*.bam",
			TargetRoles: []types.SiteRole{types.SiteRoleBurst},
			Priority:    20,
		},
		Rule{
			Name:        "high-priority-primary",
			KeyPattern:  "*.bam",
			TargetRoles: []types.SiteRole{types.SiteRolePrimary},
			Priority:    5, // lower number = evaluated first
		},
	)

	sites := []*site.SiteMount{
		siteMount("primary", types.SiteRolePrimary),
		siteMount("burst", types.SiteRoleBurst),
	}

	result, err := e.Route(OperationRead, "genome.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	// High-priority rule (priority=5, primary) wins.
	if len(result) != 1 || result[0].Name() != "primary" {
		t.Errorf("expected primary (high-priority rule); got %v", siteNames(result))
	}
}

func TestEngine_Route_RecursivePrefix(t *testing.T) {
	t.Parallel()
	// Reads under "archive/" → backup only.
	e := New(Rule{
		Name:        "archive-backup",
		KeyPattern:  "archive/",
		Operations:  []OperationType{OperationRead},
		TargetRoles: []types.SiteRole{types.SiteRoleBackup},
		Priority:    1,
	})

	sites := []*site.SiteMount{
		siteMount("primary", types.SiteRolePrimary),
		siteMount("backup", types.SiteRoleBackup),
	}

	for _, key := range []string{"archive/2025/data.bam", "archive/old/sub/file.fastq"} {
		result, err := e.Route(OperationRead, key, sites)
		if err != nil {
			t.Fatalf("Route(%q): unexpected error: %v", key, err)
		}
		if len(result) != 1 || result[0].Name() != "backup" {
			t.Errorf("Route(%q): expected backup only; got %v", key, siteNames(result))
		}
	}

	// Key outside prefix → both sites (default ordering: primary first).
	result, err := e.Route(OperationRead, "data/fresh.bam", sites)
	if err != nil {
		t.Fatalf("Route(non-archive): unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Route(non-archive): expected 2 sites; got %v", siteNames(result))
	}
}

func TestEngine_Route_EmptyTargetRoles_AllSitesDefaultOrder(t *testing.T) {
	t.Parallel()
	// Rule matches but has no TargetRoles → all sites in default order.
	e := New(Rule{
		Name:       "all-sites",
		KeyPattern: "*.bam",
		Priority:   1,
		// TargetRoles intentionally empty
	})

	sites := []*site.SiteMount{
		siteMount("burst", types.SiteRoleBurst),
		siteMount("primary", types.SiteRolePrimary),
	}

	result, err := e.Route(OperationRead, "genome.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	// Default ordering: primary first.
	if len(result) != 2 || result[0].Name() != "primary" {
		t.Errorf("expected both sites, primary first; got %v", siteNames(result))
	}
}

// ─── NewFromConfig ────────────────────────────────────────────────────────────

func TestNewFromConfig_Valid(t *testing.T) {
	t.Parallel()
	cfgRules := []config.PolicyRuleConfig{
		{
			Name:        "genomics",
			KeyPattern:  "*.bam",
			Operations:  []string{"read", "write"},
			TargetRoles: []string{"primary"},
			Priority:    10,
		},
		{
			Name:        "archive",
			KeyPattern:  "archive/",
			Operations:  []string{"read"},
			TargetRoles: []string{"backup"},
			Priority:    20,
		},
	}

	e, err := NewFromConfig(cfgRules)
	if err != nil {
		t.Fatalf("NewFromConfig: unexpected error: %v", err)
	}
	rules := e.Rules()
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}
	// priority=10 comes first (lower priority number = higher precedence).
	if rules[0].Name != "genomics" {
		t.Errorf("expected first rule to be genomics (priority 10), got %q", rules[0].Name)
	}
}

func TestNewFromConfig_UnknownOperation(t *testing.T) {
	t.Parallel()
	cfgRules := []config.PolicyRuleConfig{
		{Name: "bad", KeyPattern: "*", Operations: []string{"sync"}},
	}
	_, err := NewFromConfig(cfgRules)
	if err == nil {
		t.Fatal("expected error for unknown operation, got nil")
	}
}

func TestNewFromConfig_UnknownRole(t *testing.T) {
	t.Parallel()
	cfgRules := []config.PolicyRuleConfig{
		{Name: "bad", KeyPattern: "*", TargetRoles: []string{"archive"}},
	}
	_, err := NewFromConfig(cfgRules)
	if err == nil {
		t.Fatal("expected error for unknown role, got nil")
	}
}

func TestNewFromConfig_Empty(t *testing.T) {
	t.Parallel()
	e, err := NewFromConfig(nil)
	if err != nil {
		t.Fatalf("NewFromConfig(nil): unexpected error: %v", err)
	}
	if len(e.Rules()) != 0 {
		t.Errorf("expected 0 rules for empty config, got %d", len(e.Rules()))
	}
}

// ─── Coordinator integration (via Engine.Route) ───────────────────────────────

// TestEngine_Route_WritesBurstOnly verifies a policy that routes writes to burst
// sites (e.g. "temp files go to cheap cloud storage").
func TestEngine_Route_WritesBurstOnly(t *testing.T) {
	t.Parallel()
	e := New(Rule{
		Name:        "tmp-burst",
		KeyPattern:  "tmp/",
		Operations:  []OperationType{OperationWrite},
		TargetRoles: []types.SiteRole{types.SiteRoleBurst},
		Priority:    1,
	})

	sites := []*site.SiteMount{
		siteMount("onprem", types.SiteRolePrimary),
		siteMount("cloud-burst", types.SiteRoleBurst),
	}

	result, err := e.Route(OperationWrite, "tmp/job123/output.bam", sites)
	if err != nil {
		t.Fatalf("Route: unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Name() != "cloud-burst" {
		t.Errorf("expected only cloud-burst; got %v", siteNames(result))
	}

	// Reads from tmp/ also hit burst (rule matches any operation).
	// But the rule only applies to writes — reads fall back to default.
	result2, err := e.Route(OperationRead, "tmp/job123/output.bam", sites)
	if err != nil {
		t.Fatalf("Route(read): unexpected error: %v", err)
	}
	if len(result2) != 2 {
		t.Errorf("read should return all sites (rule is write-only); got %v", siteNames(result2))
	}
}
