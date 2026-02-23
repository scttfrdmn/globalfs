package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/lease"
	"github.com/scttfrdmn/globalfs/internal/metadata"
	"github.com/scttfrdmn/globalfs/internal/policy"
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

// TestCoordinator_SetPolicy_Get_RoutesToBurst verifies that after installing a
// policy that routes *.tmp reads to burst, Get queries the burst site first.
func TestCoordinator_SetPolicy_Get_RoutesToBurst(t *testing.T) {
	t.Parallel()

	primaryMount, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"job.tmp": []byte("primary-data"),
	})
	burstMount, burstClient := makeMount("burst", types.SiteRoleBurst, map[string][]byte{
		"job.tmp": []byte("burst-data"),
	})

	c := New(primaryMount, burstMount)
	// Install a policy: *.tmp reads → burst only.
	e := policy.New(policy.Rule{
		Name:        "tmp-to-burst",
		KeyPattern:  "*.tmp",
		Operations:  []policy.OperationType{policy.OperationRead},
		TargetRoles: []types.SiteRole{types.SiteRoleBurst},
		Priority:    1,
	})
	c.SetPolicy(e)

	data, err := c.Get(context.Background(), "job.tmp")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	// Policy routes to burst; burst has "burst-data".
	if string(data) != "burst-data" {
		t.Errorf("Get: got %q, want burst-data (policy should route to burst)", data)
	}
	_ = burstClient // confirms burstMount was queried
}

// TestCoordinator_SetPolicy_Put_RoutesToBurst verifies that a write policy
// that routes *.tmp to burst skips the primary for Put.
func TestCoordinator_SetPolicy_Put_RoutesToBurst(t *testing.T) {
	t.Parallel()

	primaryMount, primaryClient := makeMount("primary", types.SiteRolePrimary, nil)
	// Burst client is primary role in the context of routing but SiteRoleBurst
	// in terms of the coordinator's sync vs async distinction — so no sync write.
	burstMount, burstClient := makeMount("burst", types.SiteRoleBurst, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(primaryMount, burstMount)
	c.Start(ctx)
	defer c.Stop()

	// Policy: *.tmp writes → burst only (primary is not in TargetRoles).
	e := policy.New(policy.Rule{
		Name:        "tmp-writes-burst",
		KeyPattern:  "*.tmp",
		Operations:  []policy.OperationType{policy.OperationWrite},
		TargetRoles: []types.SiteRole{types.SiteRoleBurst},
		Priority:    1,
	})
	c.SetPolicy(e)

	if err := c.Put(ctx, "scratch.tmp", []byte("scratch")); err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	// Primary should NOT receive the write (it's not in TargetRoles).
	if primaryClient.hasKey("scratch.tmp") {
		t.Error("primary should not receive write (policy excludes it)")
	}

	// Burst is in TargetRoles but is non-primary role, so it gets async replication.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if burstClient.hasKey("scratch.tmp") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !burstClient.hasKey("scratch.tmp") {
		t.Error("burst should receive replication of *.tmp write within 2s")
	}
}

// TestCoordinator_SetPolicy_Nil_RevertsToDefault verifies that passing nil to
// SetPolicy restores default role-based routing.
func TestCoordinator_SetPolicy_Nil_RevertsToDefault(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"data.bam": []byte("primary-data"),
	})
	burst, _ := makeMount("burst", types.SiteRoleBurst, map[string][]byte{
		"data.bam": []byte("burst-data"),
	})

	c := New(primary, burst)
	// Install policy routing reads to burst.
	c.SetPolicy(policy.New(policy.Rule{
		Name:        "to-burst",
		KeyPattern:  "*.bam",
		Operations:  []policy.OperationType{policy.OperationRead},
		TargetRoles: []types.SiteRole{types.SiteRoleBurst},
		Priority:    1,
	}))

	// Revert to default.
	c.SetPolicy(nil)

	data, err := c.Get(context.Background(), "data.bam")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	// Default routing: primary first.
	if string(data) != "primary-data" {
		t.Errorf("Get after SetPolicy(nil): got %q, want primary-data", data)
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

// ─── Store integration tests ───────────────────────────────────────────────────

// TestCoordinator_SetStore_PersistsReplicationJob verifies that a Put to a
// non-primary site writes a ReplicationJob record to the store before the
// worker delivers it.
func TestCoordinator_SetStore_PersistsReplicationJob(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, _ := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := metadata.NewMemoryStore()
	c := New(primary, backup)
	c.SetStore(store)
	c.Start(ctx)
	defer c.Stop()

	if err := c.Put(ctx, "data/sample.bam", []byte("genome")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The replication job must appear in the store immediately after Put returns.
	jobs, err := store.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("expected at least one pending job after Put, got zero")
	}

	expectedID := makeJobID("primary", "backup", "data/sample.bam")
	var found bool
	for _, j := range jobs {
		if j.ID == expectedID {
			found = true
			if j.Key != "data/sample.bam" {
				t.Errorf("job Key: got %q, want %q", j.Key, "data/sample.bam")
			}
		}
	}
	if !found {
		t.Errorf("job %q not found in store; jobs: %v", expectedID, jobs)
	}
}

// TestCoordinator_SetStore_DeletesJobAfterReplication verifies that completed
// replication jobs are removed from the store by the drain goroutine.
func TestCoordinator_SetStore_DeletesJobAfterReplication(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := metadata.NewMemoryStore()
	c := New(primary, backup)
	c.SetStore(store)
	c.Start(ctx)
	defer c.Stop()

	if err := c.Put(ctx, "output.vcf", []byte("variant-calls")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Wait for the worker to deliver the replication (up to 3 seconds).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if backupClient.hasKey("output.vcf") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !backupClient.hasKey("output.vcf") {
		t.Fatal("backup: async replication did not deliver key within 3s")
	}

	// After delivery the job should be removed from the store.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		jobs, _ := store.GetPendingJobs(ctx)
		if len(jobs) == 0 {
			return // success: store is clean
		}
		time.Sleep(10 * time.Millisecond)
	}
	jobs, _ := store.GetPendingJobs(ctx)
	t.Errorf("expected store to be empty after replication, got %d pending job(s)", len(jobs))
}

// TestCoordinator_SetStore_RecoversPendingJobs verifies that jobs persisted in
// the store before Start are re-enqueued and eventually delivered.
func TestCoordinator_SetStore_RecoversPendingJobs(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"reads.fastq": []byte("sequence-data"),
	})
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-populate the store with a job that simulates a prior interrupted run.
	store := metadata.NewMemoryStore()
	preJob := &metadata.ReplicationJob{
		ID:         makeJobID("primary", "backup", "reads.fastq"),
		SourceSite: "primary",
		DestSite:   "backup",
		Key:        "reads.fastq",
		Size:       int64(len("sequence-data")),
		CreatedAt:  time.Now(),
	}
	if err := store.PutReplicationJob(ctx, preJob); err != nil {
		t.Fatalf("PutReplicationJob (setup): %v", err)
	}

	// Start the coordinator; recoverPendingJobs should re-enqueue the job.
	c := New(primary, backup)
	c.SetStore(store)
	c.Start(ctx)
	defer c.Stop()

	// Backup should eventually receive the recovered replication.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if backupClient.hasKey("reads.fastq") {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("backup: recovered job was not delivered within 3s")
}

// ─── Lease manager tests ──────────────────────────────────────────────────────

// TestCoordinator_SetLeaseManager_LeaderReplicates verifies that a coordinator
// that acquires the leader lease starts the worker and replicates to backup.
func TestCoordinator_SetLeaseManager_LeaderReplicates(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := lease.NewMemoryManager("coord-1")
	c := New(primary, backup)
	c.SetLeaseManager(mgr)
	c.Start(ctx)
	defer c.Stop()

	if err := c.Put(ctx, "data/reads.bam", []byte("genome")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Leader should start the worker; backup should receive the replication.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if backupClient.hasKey("data/reads.bam") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("leader coordinator: async replication not delivered within 2s")
}

// TestCoordinator_SetLeaseManager_StandbySkipsWorker verifies that a
// coordinator that cannot acquire the leader lease does not start the worker.
func TestCoordinator_SetLeaseManager_StandbySkipsWorker(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// mgr1 grabs the lease before the coordinator starts.
	mgr1, mgr2 := lease.NewMemoryManagerPair("other-node", "coord-2")
	preLeader, ok, err := mgr1.TryAcquire(ctx, "coordinator/leader", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("pre-acquire: ok=%v err=%v", ok, err)
	}
	defer preLeader.Release()

	// coord-2 starts with mgr2 — it will be in standby mode.
	c := New(primary, backup)
	c.SetLeaseManager(mgr2)
	c.Start(ctx)
	defer c.Stop()

	// Sync write to primary should still succeed.
	if err := c.Put(ctx, "scratch.tmp", []byte("temp")); err != nil {
		t.Fatalf("Put (standby): %v", err)
	}

	// No async replication should happen — the worker was not started.
	time.Sleep(300 * time.Millisecond)
	if backupClient.hasKey("scratch.tmp") {
		t.Error("standby coordinator should not replicate to backup")
	}
}

// TestCoordinator_SetLeaseManager_LeaseLossStopsWorker verifies that when the
// leader lease is lost the worker stops processing new replications.
func TestCoordinator_SetLeaseManager_LeaseLossStopsWorker(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := lease.NewMemoryManager("coord-1")
	c := New(primary, backup)
	c.SetLeaseManager(mgr)
	c.Start(ctx)
	defer c.Stop()

	// Verify the coordinator is operating as leader.
	if err := c.Put(ctx, "before.bam", []byte("d")); err != nil {
		t.Fatalf("Put (before lease loss): %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if backupClient.hasKey("before.bam") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !backupClient.hasKey("before.bam") {
		t.Fatal("leader: initial replication did not arrive within 2s")
	}

	// Simulate lease loss by revoking it from outside.
	c.mu.Lock()
	l := c.leaderLease
	c.mu.Unlock()
	if l == nil {
		t.Fatal("leaderLease not set after successful Start")
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release (simulated loss): %v", err)
	}

	// Allow some time for the transition goroutine to cancel workerCtx.
	time.Sleep(200 * time.Millisecond)

	// A new Put's async half should not be delivered — worker is stopped.
	if err := c.Put(ctx, "after.bam", []byte("d")); err != nil {
		// Put still writes to primaries; only replication is suppressed.
		t.Fatalf("Put after lease loss: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if backupClient.hasKey("after.bam") {
		t.Error("coordinator should not replicate after lease loss")
	}
}
