package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/types"
)

// storeTests runs the full Store contract against any implementation.
// Both MemoryStore and EtcdStore (integration) use this suite.
func storeTests(t *testing.T, s Store) {
	t.Helper()
	t.Run("PutGetSite", func(t *testing.T) { testPutGetSite(t, s) })
	t.Run("ListSites", func(t *testing.T) { testListSites(t, s) })
	t.Run("DeleteSite", func(t *testing.T) { testDeleteSite(t, s) })
	t.Run("PutGetPendingJobs", func(t *testing.T) { testPutGetPendingJobs(t, s) })
	t.Run("DeleteJob", func(t *testing.T) { testDeleteJob(t, s) })
	t.Run("Watch_SiteEvents", func(t *testing.T) { testWatchSiteEvents(t, s) })
	t.Run("Watch_ContextCancel", func(t *testing.T) { testWatchContextCancel(t, s) })
}

// ─── Individual test cases ────────────────────────────────────────────────────

func testPutGetSite(t *testing.T, s Store) {
	t.Helper()
	t.Parallel()

	ctx := context.Background()
	rec := &SiteRecord{
		Name:      "primary",
		Role:      types.SiteRolePrimary,
		Status:    types.SiteStatusActive,
		S3Bucket:  "hpc-data",
		S3Region:  "us-west-2",
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	if err := s.PutSite(ctx, rec); err != nil {
		t.Fatalf("PutSite: %v", err)
	}

	got, err := s.GetSite(ctx, "primary")
	if err != nil {
		t.Fatalf("GetSite: %v", err)
	}
	if got.Name != rec.Name {
		t.Errorf("Name: got %q, want %q", got.Name, rec.Name)
	}
	if got.Role != rec.Role {
		t.Errorf("Role: got %q, want %q", got.Role, rec.Role)
	}
	if got.S3Bucket != rec.S3Bucket {
		t.Errorf("S3Bucket: got %q, want %q", got.S3Bucket, rec.S3Bucket)
	}
}

func testListSites(t *testing.T, s Store) {
	t.Helper()
	t.Parallel()

	ctx := context.Background()
	sites := []*SiteRecord{
		{Name: "list-a", Role: types.SiteRolePrimary, Status: types.SiteStatusActive},
		{Name: "list-b", Role: types.SiteRoleBackup, Status: types.SiteStatusActive},
		{Name: "list-c", Role: types.SiteRoleBurst, Status: types.SiteStatusDegraded},
	}
	for _, sr := range sites {
		if err := s.PutSite(ctx, sr); err != nil {
			t.Fatalf("PutSite %q: %v", sr.Name, err)
		}
	}

	all, err := s.ListSites(ctx)
	if err != nil {
		t.Fatalf("ListSites: %v", err)
	}

	// The store may contain records from other sub-tests; just verify all
	// three names we put are present.
	found := make(map[string]bool)
	for _, sr := range all {
		found[sr.Name] = true
	}
	for _, sr := range sites {
		if !found[sr.Name] {
			t.Errorf("ListSites: missing site %q", sr.Name)
		}
	}
}

func testDeleteSite(t *testing.T, s Store) {
	t.Helper()
	t.Parallel()

	ctx := context.Background()
	rec := &SiteRecord{Name: "delete-me", Role: types.SiteRoleBurst, Status: types.SiteStatusActive}

	if err := s.PutSite(ctx, rec); err != nil {
		t.Fatalf("PutSite: %v", err)
	}
	if err := s.DeleteSite(ctx, "delete-me"); err != nil {
		t.Fatalf("DeleteSite: %v", err)
	}

	_, err := s.GetSite(ctx, "delete-me")
	if err == nil {
		t.Error("GetSite after DeleteSite: expected error, got nil")
	}

	// Second delete is a no-op; should not error.
	if err := s.DeleteSite(ctx, "delete-me"); err != nil {
		t.Errorf("DeleteSite (idempotent): unexpected error: %v", err)
	}
}

func testPutGetPendingJobs(t *testing.T, s Store) {
	t.Helper()
	t.Parallel()

	ctx := context.Background()
	job := &ReplicationJob{
		ID:         "job-put-get-1",
		SourceSite: "primary",
		DestSite:   "backup",
		Key:        "data/genome.bam",
		Size:       1024,
		CreatedAt:  time.Now().Truncate(time.Second),
	}

	if err := s.PutReplicationJob(ctx, job); err != nil {
		t.Fatalf("PutReplicationJob: %v", err)
	}

	jobs, err := s.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs: %v", err)
	}

	var found bool
	for _, j := range jobs {
		if j.ID == job.ID {
			found = true
			if j.Key != job.Key {
				t.Errorf("job Key: got %q, want %q", j.Key, job.Key)
			}
			if j.Size != job.Size {
				t.Errorf("job Size: got %d, want %d", j.Size, job.Size)
			}
		}
	}
	if !found {
		t.Errorf("GetPendingJobs: job %q not found", job.ID)
	}
}

func testDeleteJob(t *testing.T, s Store) {
	t.Helper()
	t.Parallel()

	ctx := context.Background()
	job := &ReplicationJob{
		ID:         "job-delete-1",
		SourceSite: "primary",
		DestSite:   "backup",
		Key:        "sample.fastq",
		CreatedAt:  time.Now(),
	}

	if err := s.PutReplicationJob(ctx, job); err != nil {
		t.Fatalf("PutReplicationJob: %v", err)
	}
	if err := s.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	jobs, err := s.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs: %v", err)
	}
	for _, j := range jobs {
		if j.ID == job.ID {
			t.Errorf("DeleteJob: job %q still present", job.ID)
		}
	}

	// Idempotent delete should not error.
	if err := s.DeleteJob(ctx, job.ID); err != nil {
		t.Errorf("DeleteJob (idempotent): unexpected error: %v", err)
	}
}

func testWatchSiteEvents(t *testing.T, s Store) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, "sites/")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Put a site and expect a WatchEventPut.
	rec := &SiteRecord{Name: "watch-site", Role: types.SiteRolePrimary, Status: types.SiteStatusActive}
	if err := s.PutSite(ctx, rec); err != nil {
		t.Fatalf("PutSite: %v", err)
	}

	var putSeen bool
	deadline := time.After(2 * time.Second)
loop:
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("Watch channel closed unexpectedly")
			}
			if ev.Type == WatchEventPut && ev.Key == "sites/watch-site" {
				putSeen = true
				break loop
			}
		case <-deadline:
			t.Fatal("timed out waiting for WatchEventPut")
		}
	}

	if !putSeen {
		t.Error("did not receive WatchEventPut for PutSite")
	}

	// Delete the site and expect a WatchEventDelete.
	if err := s.DeleteSite(ctx, "watch-site"); err != nil {
		t.Fatalf("DeleteSite: %v", err)
	}

	var delSeen bool
	deadline = time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("Watch channel closed unexpectedly")
			}
			if ev.Type == WatchEventDelete && ev.Key == "sites/watch-site" {
				delSeen = true
			}
		case <-deadline:
			goto checkDel
		}
		if delSeen {
			break
		}
	}
checkDel:
	if !delSeen {
		t.Error("did not receive WatchEventDelete for DeleteSite")
	}
}

func testWatchContextCancel(t *testing.T, s Store) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	ch, err := s.Watch(ctx, "sites/")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	cancel() // cancel immediately

	// Channel should be closed shortly after cancel.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed as expected
			}
		case <-deadline:
			t.Fatal("Watch channel not closed within 2s of context cancel")
		}
	}
}

// ─── MemoryStore suite ────────────────────────────────────────────────────────

func TestMemoryStore(t *testing.T) {
	storeTests(t, NewMemoryStore())
}

// ─── MemoryStore-specific tests ───────────────────────────────────────────────

// TestMemoryStore_GetSite_NotFound verifies the error path when a key is absent.
func TestMemoryStore_GetSite_NotFound(t *testing.T) {
	t.Parallel()
	s := NewMemoryStore()
	_, err := s.GetSite(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing site, got nil")
	}
}

// TestMemoryStore_PutSite_Overwrites verifies that a second PutSite with the
// same name replaces the previous record.
func TestMemoryStore_PutSite_Overwrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore()

	original := &SiteRecord{Name: "overwrite-me", Role: types.SiteRolePrimary, Status: types.SiteStatusActive}
	updated := &SiteRecord{Name: "overwrite-me", Role: types.SiteRolePrimary, Status: types.SiteStatusDegraded}

	if err := s.PutSite(ctx, original); err != nil {
		t.Fatalf("PutSite (original): %v", err)
	}
	if err := s.PutSite(ctx, updated); err != nil {
		t.Fatalf("PutSite (updated): %v", err)
	}

	got, err := s.GetSite(ctx, "overwrite-me")
	if err != nil {
		t.Fatalf("GetSite: %v", err)
	}
	if got.Status != types.SiteStatusDegraded {
		t.Errorf("Status: got %q, want %q", got.Status, types.SiteStatusDegraded)
	}
}

// TestMemoryStore_Watch_JobEvents verifies WatchEventPut and WatchEventDelete
// for replication jobs.
func TestMemoryStore_Watch_JobEvents(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewMemoryStore()
	ch, err := s.Watch(ctx, "jobs/")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	job := &ReplicationJob{ID: "j1", SourceSite: "a", DestSite: "b", Key: "k"}
	if err := s.PutReplicationJob(ctx, job); err != nil {
		t.Fatalf("PutReplicationJob: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != WatchEventPut {
			t.Errorf("expected WatchEventPut, got %q", ev.Type)
		}
		if ev.Key != "jobs/j1" {
			t.Errorf("Key: got %q, want jobs/j1", ev.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WatchEventPut on job")
	}
}

// TestMemoryStore_Mutated_Record_IsIsolated verifies that mutations to a
// returned SiteRecord do not affect the stored copy.
func TestMemoryStore_Mutated_Record_IsIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore()

	rec := &SiteRecord{Name: "iso", Role: types.SiteRolePrimary, Status: types.SiteStatusActive}
	if err := s.PutSite(ctx, rec); err != nil {
		t.Fatalf("PutSite: %v", err)
	}

	got, _ := s.GetSite(ctx, "iso")
	got.Status = types.SiteStatusUnavailable // mutate the returned copy

	// Stored record should be unchanged.
	got2, _ := s.GetSite(ctx, "iso")
	if got2.Status != types.SiteStatusActive {
		t.Errorf("stored record mutated: got %q, want %q", got2.Status, types.SiteStatusActive)
	}
}
