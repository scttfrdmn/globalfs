package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/lease"
	"github.com/scttfrdmn/globalfs/internal/metadata"
	"github.com/scttfrdmn/globalfs/internal/policy"
	"github.com/scttfrdmn/globalfs/internal/retry"
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
	// getFn, if non-nil, overrides the default Get behaviour.  Useful for
	// simulating sequences of transient failures in retry tests.
	getFn func(key string) ([]byte, error)
}

func newMemClient(objs map[string][]byte) *memClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &memClient{objects: objs}
}

func (m *memClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
	// getFn takes precedence over the static error/objects behaviour.
	if m.getFn != nil {
		return m.getFn(key)
	}
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

func (m *memClient) Health(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthErr
}
func (m *memClient) Close() error { return nil }

func (m *memClient) setHealthErr(err error) {
	m.mu.Lock()
	m.healthErr = err
	m.mu.Unlock()
}

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

// ─── Health polling ────────────────────────────────────────────────────────────

func TestCoordinator_HealthStatus_NilBeforeFirstPoll(t *testing.T) {
	t.Parallel()
	// Create coordinator but do NOT call Start — cache must stay nil.
	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)
	report, checkedAt := c.HealthStatus()
	if report != nil {
		t.Errorf("expected nil report before first poll, got %v", report)
	}
	if !checkedAt.IsZero() {
		t.Errorf("expected zero checkedAt before first poll, got %v", checkedAt)
	}
}

func TestCoordinator_HealthStatus_PopulatedAfterPoll(t *testing.T) {
	t.Parallel()
	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)
	// Use a very short poll interval so the test doesn't wait 30s.
	c.SetHealthPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Wait up to 500ms for the first poll to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		report, _ := c.HealthStatus()
		if report != nil {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("HealthStatus still nil after 500ms — background poll did not run")
}

func TestCoordinator_HealthStatus_ReflectsUnhealthySite(t *testing.T) {
	t.Parallel()
	primary, mc := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)
	c.SetHealthPollInterval(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Wait for first (healthy) poll.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if report, _ := c.HealthStatus(); report != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Inject an error into the client (use the mutex-protected setter).
	mc.setHealthErr(errors.New("s3 unreachable"))

	// Wait for a poll that reflects the error.
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if report, _ := c.HealthStatus(); report != nil && report["primary"] != nil {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("HealthStatus never reflected unhealthy site within 500ms")
}

func TestCoordinator_SetHealthPollInterval_StopsWithStop(t *testing.T) {
	t.Parallel()
	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)
	c.SetHealthPollInterval(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Stop should return quickly without hanging on the polling goroutine.
	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()
	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s — health polling goroutine may be leaked")
	}
}

// ─── preferHealthySites ────────────────────────────────────────────────────────

func TestPreferHealthySites_NilReport_Unchanged(t *testing.T) {
	t.Parallel()
	a, _ := makeMount("a", types.SiteRolePrimary, nil)
	b, _ := makeMount("b", types.SiteRoleBackup, nil)
	in := []*site.SiteMount{a, b}
	out := preferHealthySites(in, nil)
	if len(out) != 2 || out[0] != a || out[1] != b {
		t.Errorf("nil report: expected [a, b], got names %v", siteNames(out))
	}
}

func TestPreferHealthySites_AllHealthy_Unchanged(t *testing.T) {
	t.Parallel()
	a, _ := makeMount("a", types.SiteRolePrimary, nil)
	b, _ := makeMount("b", types.SiteRoleBackup, nil)
	report := map[string]error{"a": nil, "b": nil}
	out := preferHealthySites([]*site.SiteMount{a, b}, report)
	if len(out) != 2 || out[0] != a || out[1] != b {
		t.Errorf("all healthy: expected [a, b], got %v", siteNames(out))
	}
}

func TestPreferHealthySites_DegradedMovedLast(t *testing.T) {
	t.Parallel()
	a, _ := makeMount("a", types.SiteRolePrimary, nil)
	b, _ := makeMount("b", types.SiteRoleBackup, nil)
	c, _ := makeMount("c", types.SiteRoleBurst, nil)
	// b is degraded; a and c are healthy.
	report := map[string]error{
		"a": nil,
		"b": errors.New("timeout"),
		"c": nil,
	}
	out := preferHealthySites([]*site.SiteMount{a, b, c}, report)
	if len(out) != 3 {
		t.Fatalf("expected 3 sites, got %d", len(out))
	}
	// a and c should come first (in original order), b last.
	if out[0] != a || out[1] != c || out[2] != b {
		t.Errorf("expected [a, c, b], got %v", siteNames(out))
	}
}

func TestPreferHealthySites_AllDegraded_Unchanged(t *testing.T) {
	t.Parallel()
	a, _ := makeMount("a", types.SiteRolePrimary, nil)
	b, _ := makeMount("b", types.SiteRoleBackup, nil)
	report := map[string]error{
		"a": errors.New("err-a"),
		"b": errors.New("err-b"),
	}
	out := preferHealthySites([]*site.SiteMount{a, b}, report)
	// All degraded → original order preserved (all are "fallback").
	if len(out) != 2 || out[0] != a || out[1] != b {
		t.Errorf("all degraded: expected [a, b], got %v", siteNames(out))
	}
}

// ─── Get/Head health-aware routing ────────────────────────────────────────────

// TestCoordinator_Get_SkipsDegradedPrimary verifies that when the health cache
// marks the primary as degraded, Get returns data from the backup without
// first attempting the primary.
func TestCoordinator_Get_SkipsDegradedPrimary(t *testing.T) {
	t.Parallel()

	// Primary has data but health is flagged as degraded.
	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"genome.bam": []byte("primary-data"),
	})
	primaryClient.setHealthErr(errors.New("disk full"))

	// Backup also has the data.
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"genome.bam": []byte("backup-data"),
	})

	c := New(primary, backup)
	c.SetHealthPollInterval(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Wait for health cache to show primary as degraded.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r, _ := c.HealthStatus(); r != nil && r["primary"] != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	data, err := c.Get(ctx, "genome.bam")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	// Health-aware routing should return the backup's data, not the primary's.
	if string(data) != "backup-data" {
		t.Errorf("Get: got %q, want backup-data (degraded primary should be tried last)", string(data))
	}
}

// TestCoordinator_Get_FallsBackToDegradedWhenAllDegraded verifies that when
// all sites are marked degraded, Get still tries them (cache fallback).
func TestCoordinator_Get_FallsBackToDegradedWhenAllDegraded(t *testing.T) {
	t.Parallel()

	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"key": []byte("data"),
	})
	primaryClient.setHealthErr(errors.New("degraded"))

	c := New(primary)
	c.SetHealthPollInterval(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Wait for cache to mark primary degraded.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r, _ := c.HealthStatus(); r != nil && r["primary"] != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Even though primary is degraded in the cache, Get should still try it
	// (all-degraded fallback) and succeed because the client's Get works.
	data, err := c.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get (all-degraded fallback): unexpected error: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("Get (all-degraded fallback): got %q, want data", string(data))
	}
}

// TestCoordinator_Head_SkipsDegradedSite verifies the same health-aware
// reordering for Head operations.
func TestCoordinator_Head_SkipsDegradedSite(t *testing.T) {
	t.Parallel()

	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"file.txt": []byte("content"),
	})
	primaryClient.setHealthErr(errors.New("network error"))

	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"file.txt": []byte("content"),
	})

	c := New(primary, backup)
	c.SetHealthPollInterval(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r, _ := c.HealthStatus(); r != nil && r["primary"] != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	info, err := c.Head(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Head: unexpected error: %v", err)
	}
	if info.Key != "file.txt" {
		t.Errorf("Head: got key %q, want file.txt", info.Key)
	}
}

// siteNames is a test helper that extracts site names from a slice.
func siteNames(sites []*site.SiteMount) []string {
	names := make([]string, len(sites))
	for i, s := range sites {
		names[i] = s.Name()
	}
	return names
}

// ─── Circuit breaker tests ────────────────────────────────────────────────────

// TestCoordinator_CircuitBreaker_SkipsOpenCircuit verifies that Get skips a
// site whose circuit is open and succeeds via the next available site.
func TestCoordinator_CircuitBreaker_SkipsOpenCircuit(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{getErr: errors.New("primary down"), objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"obj": []byte("backup-data"),
	})

	cb := circuitbreaker.New(1, time.Hour) // opens after 1 failure
	// Manually open the primary circuit to simulate a prior failure.
	cb.RecordFailure("primary")

	c := New(primary, backup)
	c.SetCircuitBreaker(cb)

	data, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "backup-data" {
		t.Errorf("Get: got %q, want backup-data", string(data))
	}
}

// TestCoordinator_CircuitBreaker_FallbackWhenAllOpen verifies that Get still
// succeeds when all circuits are open (breaker bypassed as fallback).
func TestCoordinator_CircuitBreaker_FallbackWhenAllOpen(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"obj": []byte("data"),
	})

	cb := circuitbreaker.New(1, time.Hour)
	cb.RecordFailure("primary") // open the only circuit

	c := New(primary)
	c.SetCircuitBreaker(cb)

	// Even with all circuits open, Get should fall back and succeed.
	data, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get (all-open fallback): unexpected error: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("Get (all-open fallback): got %q, want data", string(data))
	}
}

// TestCoordinator_CircuitBreaker_RecordsFailures verifies that failed Get
// operations trip the circuit after threshold consecutive failures.
func TestCoordinator_CircuitBreaker_RecordsFailures(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{getErr: errors.New("unavailable"), objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)

	cb := circuitbreaker.New(3, time.Hour)
	c := New(primary)
	c.SetCircuitBreaker(cb)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = c.Get(ctx, "key") // each call records a failure
	}

	if got := cb.State("primary"); got != circuitbreaker.StateOpen {
		t.Errorf("expected circuit Open after 3 failures, got %v", got)
	}
}

// TestCoordinator_CircuitBreaker_RecordsSuccesses verifies that a successful
// Get records success and keeps the circuit closed.
func TestCoordinator_CircuitBreaker_RecordsSuccesses(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"obj": []byte("content"),
	})

	cb := circuitbreaker.New(3, time.Hour)
	// Warm up with failures just below threshold.
	cb.RecordFailure("primary")
	cb.RecordFailure("primary")

	c := New(primary)
	c.SetCircuitBreaker(cb)

	_, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}

	// Success should have reset the failure counter; circuit stays Closed.
	if got := cb.State("primary"); got != circuitbreaker.StateClosed {
		t.Errorf("expected circuit Closed after success, got %v", got)
	}
}

// TestCoordinator_CircuitBreaker_PutRecordsFailure verifies that a failed Put
// to a primary site records a failure in the circuit breaker.
func TestCoordinator_CircuitBreaker_PutRecordsFailure(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{putErr: errors.New("write error"), objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)

	cb := circuitbreaker.New(1, time.Hour)
	c := New(primary)
	c.SetCircuitBreaker(cb)

	_ = c.Put(context.Background(), "key", []byte("data"))

	if got := cb.State("primary"); got != circuitbreaker.StateOpen {
		t.Errorf("expected circuit Open after Put failure, got %v", got)
	}
}

// TestCoordinator_CircuitBreaker_NilIsNoop verifies that a nil circuit breaker
// (the default) does not affect coordinator behaviour.
func TestCoordinator_CircuitBreaker_NilIsNoop(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"k": []byte("v"),
	})
	c := New(primary)
	// No SetCircuitBreaker — cb is nil.

	data, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "v" {
		t.Errorf("Get: got %q, want v", string(data))
	}
}

// TestFilterByCircuitBreaker_AllowsClosedSites verifies the helper allows all
// sites when no circuits are open.
func TestFilterByCircuitBreaker_AllowsClosedSites(t *testing.T) {
	t.Parallel()

	s1, _ := makeMount("a", types.SiteRolePrimary, nil)
	s2, _ := makeMount("b", types.SiteRoleBackup, nil)
	cb := circuitbreaker.New(1, time.Hour)

	got := filterByCircuitBreaker(cb, []*site.SiteMount{s1, s2})
	if len(got) != 2 {
		t.Errorf("expected 2 sites, got %d", len(got))
	}
}

// TestFilterByCircuitBreaker_FiltersOpenCircuit verifies the helper removes
// open-circuit sites and preserves closed ones.
func TestFilterByCircuitBreaker_FiltersOpenCircuit(t *testing.T) {
	t.Parallel()

	s1, _ := makeMount("a", types.SiteRolePrimary, nil)
	s2, _ := makeMount("b", types.SiteRoleBackup, nil)
	cb := circuitbreaker.New(1, time.Hour)
	cb.RecordFailure("a") // open circuit for "a"

	got := filterByCircuitBreaker(cb, []*site.SiteMount{s1, s2})
	if len(got) != 1 || got[0].Name() != "b" {
		t.Errorf("expected only site-b, got %v", siteNames(got))
	}
}

// TestFilterByCircuitBreaker_FallbackWhenAllOpen verifies the helper returns
// the original slice when all circuits are open.
func TestFilterByCircuitBreaker_FallbackWhenAllOpen(t *testing.T) {
	t.Parallel()

	s1, _ := makeMount("a", types.SiteRolePrimary, nil)
	s2, _ := makeMount("b", types.SiteRoleBackup, nil)
	cb := circuitbreaker.New(1, time.Hour)
	cb.RecordFailure("a")
	cb.RecordFailure("b")

	got := filterByCircuitBreaker(cb, []*site.SiteMount{s1, s2})
	if len(got) != 2 {
		t.Errorf("all-open fallback: expected 2 sites, got %d", len(got))
	}
}

// TestFilterByCircuitBreaker_NilBreakerPassesThrough verifies the helper is a
// no-op when cb is nil.
func TestFilterByCircuitBreaker_NilBreakerPassesThrough(t *testing.T) {
	t.Parallel()

	s1, _ := makeMount("a", types.SiteRolePrimary, nil)
	got := filterByCircuitBreaker(nil, []*site.SiteMount{s1})
	if len(got) != 1 {
		t.Errorf("nil cb: expected 1 site, got %d", len(got))
	}
}

// ─── Retry tests ──────────────────────────────────────────────────────────────

// TestCoordinator_Retry_NilConfigIsNoop verifies that the default (no retry
// config) behaves identically to the pre-retry implementation.
func TestCoordinator_Retry_NilConfigIsNoop(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"obj": []byte("content"),
	})
	c := New(primary)
	// No SetRetryConfig — retryConfig is nil.

	data, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "content" {
		t.Errorf("Get: got %q, want content", string(data))
	}
}

// TestCoordinator_Retry_RecoversOnSecondAttempt verifies that a transient
// failure on the first call succeeds on the second retry without falling back
// to another site.
func TestCoordinator_Retry_RecoversOnSecondAttempt(t *testing.T) {
	t.Parallel()

	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"obj": []byte("primary-data"),
	})
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"obj": []byte("backup-data"),
	})

	calls := 0
	primaryClient.getFn = func(key string) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("transient")
		}
		return []byte("primary-data"), nil
	}

	c := New(primary, backup)
	c.SetRetryConfig(&retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	})

	data, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "primary-data" {
		t.Errorf("Get: got %q, want primary-data (should not fall back to backup)", string(data))
	}
	if calls != 2 {
		t.Errorf("primary getFn called %d times, want 2", calls)
	}
}

// TestCoordinator_Retry_FallsBackAfterAllRetriesExhausted verifies that the
// coordinator moves to the next site only after all per-site retries fail.
func TestCoordinator_Retry_FallsBackAfterAllRetriesExhausted(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)
	backup, _ := makeMount("backup", types.SiteRoleBackup, map[string][]byte{
		"obj": []byte("backup-data"),
	})

	primaryCalls := 0
	primaryClient.getFn = func(_ string) ([]byte, error) {
		primaryCalls++
		return nil, errors.New("primary always fails")
	}

	c := New(primary, backup)
	c.SetRetryConfig(&retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	})

	data, err := c.Get(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if string(data) != "backup-data" {
		t.Errorf("Get: got %q, want backup-data", string(data))
	}
	// Primary should have been tried MaxAttempts=3 times before giving up.
	if primaryCalls != 3 {
		t.Errorf("primary getFn called %d times, want 3 (MaxAttempts)", primaryCalls)
	}
}

// TestCoordinator_Retry_CBTrippedOnlyAfterAllRetriesExhausted verifies that
// the circuit breaker records a failure only once per site (after all retries),
// not once per attempt.
func TestCoordinator_Retry_CBTrippedOnlyAfterAllRetriesExhausted(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)
	primaryClient.getFn = func(_ string) ([]byte, error) {
		return nil, errors.New("unavailable")
	}

	cb := circuitbreaker.New(2, time.Hour) // opens after 2 recorded failures
	c := New(primary)
	c.SetCircuitBreaker(cb)
	c.SetRetryConfig(&retry.Config{
		MaxAttempts:  3, // 3 attempts per site; still only 1 RecordFailure call
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	})

	// First Get: 3 retries → 1 RecordFailure → failures=1 → circuit still Closed
	_, _ = c.Get(context.Background(), "obj")
	if got := cb.State("primary"); got != circuitbreaker.StateClosed {
		t.Errorf("after 1 Get (3 retries): expected Closed, got %v", got)
	}

	// Second Get: 3 retries → 1 RecordFailure → failures=2 → circuit Opens
	_, _ = c.Get(context.Background(), "obj")
	if got := cb.State("primary"); got != circuitbreaker.StateOpen {
		t.Errorf("after 2 Gets (threshold=2): expected Open, got %v", got)
	}
}

// TestCoordinator_Retry_ContextCancelledAbortsRetry verifies that context
// cancellation during a retry wait propagates correctly.
func TestCoordinator_Retry_ContextCancelledAbortsRetry(t *testing.T) {
	t.Parallel()

	primaryClient := &memClient{objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, primaryClient)

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	primaryClient.getFn = func(_ string) ([]byte, error) {
		calls++
		cancel() // cancel after first attempt so next retry wait is aborted
		return nil, errors.New("unavailable")
	}

	c := New(primary)
	c.SetRetryConfig(&retry.Config{
		MaxAttempts:  5,
		InitialDelay: time.Second, // long enough that cancel fires during wait
		Multiplier:   1.0,
	})

	_, err := c.Get(ctx, "obj")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// fn was called once; ctx cancelled during the first retry wait.
	if calls != 1 {
		t.Errorf("expected 1 call before cancel, got %d", calls)
	}
}

// TestDoWithRetry_NilConfigCallsOnce verifies that doWithRetry with nil config
// calls fn exactly once and returns its error.
func TestDoWithRetry_NilConfigCallsOnce(t *testing.T) {
	t.Parallel()

	calls := 0
	sentinel := errors.New("err")
	err := doWithRetry(context.Background(), nil, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}
