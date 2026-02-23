package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// memClient is a thread-safe in-memory ObjectFSClient for coordinator tests.
type memClient struct {
	mu        sync.Mutex
	objects   map[string][]byte
	healthErr error
	getErr    error
	putErr    error
	delErr    error
}

func newMemClient(objs map[string][]byte) *memClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &memClient{objects: objs}
}

func (m *memClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *memClient) Put(_ context.Context, key string, data []byte) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp
	return nil
}

func (m *memClient) Delete(_ context.Context, key string) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memClient) List(_ context.Context, prefix string, _ int) ([]objectfstypes.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []objectfstypes.ObjectInfo
	for k, v := range m.objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			result = append(result, objectfstypes.ObjectInfo{
				Key:          k,
				Size:         int64(len(v)),
				LastModified: time.Now(),
			})
		}
	}
	return result, nil
}

func (m *memClient) Head(_ context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return &objectfstypes.ObjectInfo{
		Key:          key,
		Size:         int64(len(data)),
		LastModified: time.Now(),
	}, nil
}

func (m *memClient) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}
	return out
}

func (m *memClient) hasKey(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}

func (m *memClient) Health(_ context.Context) error { return m.healthErr }
func (m *memClient) Close() error                   { return nil }

func makeMount(name string, role types.SiteRole, objs map[string][]byte) (*site.SiteMount, *memClient) {
	mc := newMemClient(objs)
	return site.New(name, role, mc), mc
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestCoordinator_Get_PrimaryFirst verifies that Get returns data from the
// primary site when it is available, and does not fall through to backup.
func TestCoordinator_Get_PrimaryFirst(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"genome.bam": []byte("primary-data"),
	})
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"genome.bam": []byte("backup-data"),
	})

	c := New(primary, backup)
	ctx := context.Background()

	data, err := c.Get(ctx, "genome.bam")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "primary-data" {
		t.Errorf("Get: got %q, want %q", data, "primary-data")
	}
}

// TestCoordinator_Get_FallsBackToBackup verifies that Get falls back to the
// backup site when the primary site fails.
func TestCoordinator_Get_FallsBackToBackup(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{getErr: errors.New("primary unavailable"), objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"sample.fastq": []byte("backup-content"),
	})

	c := New(primary, backup)
	data, err := c.Get(context.Background(), "sample.fastq")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "backup-content" {
		t.Errorf("Get: got %q, want %q", data, "backup-content")
	}
}

// TestCoordinator_Get_AllSitesFail verifies that Get returns an error when
// every site fails.
func TestCoordinator_Get_AllSitesFail(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("service unavailable")
	client := &memClient{getErr: sentinel, objects: map[string][]byte{}}
	s := site.New("only", types.SiteRolePrimary, client)

	c := New(s)
	_, err := c.Get(context.Background(), "missing.bam")
	if err == nil {
		t.Fatal("Get: expected error, got nil")
	}
}

// TestCoordinator_Get_NoSites verifies that Get returns an error when there
// are no sites registered.
func TestCoordinator_Get_NoSites(t *testing.T) {
	t.Parallel()

	c := New()
	_, err := c.Get(context.Background(), "any.bam")
	if err == nil {
		t.Fatal("Get: expected error with no sites, got nil")
	}
}

// TestCoordinator_Put_WritesToPrimaries verifies that Put synchronously writes
// to all primary sites.
func TestCoordinator_Put_WritesToPrimaries(t *testing.T) {
	t.Parallel()

	p1, mc1 := makeMount("primary-1", types.SiteRolePrimary, nil)
	p2, mc2 := makeMount("primary-2", types.SiteRolePrimary, nil)

	c := New(p1, p2)
	if err := c.Put(context.Background(), "output.bam", []byte("result")); err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	if !mc1.hasKey("output.bam") {
		t.Error("primary-1: key not written")
	}
	if !mc2.hasKey("output.bam") {
		t.Error("primary-2: key not written")
	}
}

// TestCoordinator_Put_AsyncReplicatesToBackup verifies that Put enqueues
// replication to backup sites and the replication worker delivers the data.
func TestCoordinator_Put_AsyncReplicatesToBackup(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(primary, backup)
	c.Start(ctx)
	defer c.Stop()

	if err := c.Put(ctx, "data.fastq", []byte("genome-data")); err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	// Wait for async replication (up to 2 seconds).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if backupClient.hasKey("data.fastq") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !backupClient.hasKey("data.fastq") {
		t.Error("backup site: async replication did not deliver key within 2s")
	}
}

// TestCoordinator_Put_PrimaryFailureReturnsError verifies that a primary
// write failure causes Put to return an error.
func TestCoordinator_Put_PrimaryFailureReturnsError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("disk full")
	client := &memClient{putErr: sentinel, objects: map[string][]byte{}}
	s := site.New("primary", types.SiteRolePrimary, client)

	c := New(s)
	if err := c.Put(context.Background(), "key", []byte("data")); err == nil {
		t.Fatal("Put: expected error on primary failure, got nil")
	}
}

// TestCoordinator_Delete_RemovesFromAllPrimaries verifies that Delete removes
// the key from all primary sites.
func TestCoordinator_Delete_RemovesFromAllPrimaries(t *testing.T) {
	t.Parallel()

	p1, mc1 := makeMount("p1", types.SiteRolePrimary, map[string][]byte{"f.bam": []byte("x")})
	p2, mc2 := makeMount("p2", types.SiteRolePrimary, map[string][]byte{"f.bam": []byte("x")})

	c := New(p1, p2)
	if err := c.Delete(context.Background(), "f.bam"); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	if mc1.hasKey("f.bam") {
		t.Error("p1: key still present after Delete")
	}
	if mc2.hasKey("f.bam") {
		t.Error("p2: key still present after Delete")
	}
}

// TestCoordinator_Delete_NonPrimaryErrorIgnored verifies that a failure on a
// non-primary site during Delete does not surface to the caller.
func TestCoordinator_Delete_NonPrimaryErrorIgnored(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	burstClient := &memClient{delErr: errors.New("unavailable"), objects: map[string][]byte{"k": []byte("v")}}
	burst := site.New("burst", types.SiteRoleBurst, burstClient)

	c := New(primary, burst)
	if err := c.Delete(context.Background(), "k"); err != nil {
		t.Errorf("Delete: non-primary error should be ignored, got: %v", err)
	}
}

// TestCoordinator_List_MergesAcrossSites verifies that List produces a
// deduplicated union across all registered sites.
func TestCoordinator_List_MergesAcrossSites(t *testing.T) {
	t.Parallel()

	siteA, _ := makeMount("a", types.SiteRolePrimary, map[string][]byte{
		"data/x.bam": []byte("x"),
		"data/y.bam": []byte("y"),
	})
	siteB, _ := makeMount("b", types.SiteRoleBurst, map[string][]byte{
		"data/z.bam": []byte("z"),
	})

	c := New(siteA, siteB)
	items, err := c.List(context.Background(), "data/", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("List: got %d items, want 3: %v", len(items), items)
	}
}

// TestCoordinator_Head_PrimaryFirst verifies that Head returns metadata from
// the primary site before trying backup.
func TestCoordinator_Head_PrimaryFirst(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"ref.fa": []byte("primary"),
	})
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"ref.fa": []byte("backup-longer"),
	})

	c := New(primary, backup)
	info, err := c.Head(context.Background(), "ref.fa")
	if err != nil {
		t.Fatalf("Head: unexpected error: %v", err)
	}
	// Primary data is "primary" (7 bytes).
	if info.Size != 7 {
		t.Errorf("Head: got size %d, want 7 (primary wins)", info.Size)
	}
}

// TestCoordinator_AddRemoveSite verifies dynamic site management.
func TestCoordinator_AddRemoveSite(t *testing.T) {
	t.Parallel()

	c := New()
	if len(c.Sites()) != 0 {
		t.Fatalf("expected 0 sites, got %d", len(c.Sites()))
	}

	sA, _ := makeMount("a", types.SiteRolePrimary, nil)
	sB, _ := makeMount("b", types.SiteRoleBurst, nil)

	c.AddSite(sA)
	c.AddSite(sB)

	if len(c.Sites()) != 2 {
		t.Fatalf("expected 2 sites after AddSite, got %d", len(c.Sites()))
	}

	c.RemoveSite("a")
	sites := c.Sites()
	if len(sites) != 1 || sites[0].Name() != "b" {
		t.Errorf("expected only site \"b\" after RemoveSite(\"a\"), got %v", sites)
	}
}

// TestCoordinator_Health_AllHealthy verifies that all nil errors are returned
// when every site is healthy.
func TestCoordinator_Health_AllHealthy(t *testing.T) {
	t.Parallel()

	sA, _ := makeMount("a", types.SiteRolePrimary, nil)
	sB, _ := makeMount("b", types.SiteRoleBackup, nil)

	c := New(sA, sB)
	report := c.Health(context.Background())

	if len(report) != 2 {
		t.Fatalf("Health: expected 2 entries, got %d", len(report))
	}
	for name, err := range report {
		if err != nil {
			t.Errorf("Health[%q]: expected nil, got %v", name, err)
		}
	}
}

// TestCoordinator_Health_UnhealthySiteReported verifies that a site returning
// an error is represented in the health report.
func TestCoordinator_Health_UnhealthySiteReported(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("connection refused")
	healthyMount, _ := makeMount("healthy", types.SiteRolePrimary, nil)
	unhealthyClient := &memClient{healthErr: sentinel, objects: map[string][]byte{}}
	unhealthy := site.New("sick", types.SiteRoleBurst, unhealthyClient)

	c := New(healthyMount, unhealthy)
	report := c.Health(context.Background())

	if report["healthy"] != nil {
		t.Errorf("Health[healthy]: expected nil, got %v", report["healthy"])
	}
	if !errors.Is(report["sick"], sentinel) {
		t.Errorf("Health[sick]: expected sentinel error, got %v", report["sick"])
	}
}
