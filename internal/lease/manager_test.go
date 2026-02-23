package lease

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/internal/metadata"
)

// ─── Unit tests (in-memory backend) ───────────────────────────────────────────

func TestManager_Acquire_Release(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr := NewMemoryManager("node-1")

	l, err := mgr.Acquire(ctx, "test/res", 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if l.Resource != "test/res" {
		t.Errorf("Resource: got %q, want test/res", l.Resource)
	}

	if err := l.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

func TestManager_TryAcquire_WhenFree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr := NewMemoryManager("node-1")

	l, ok, err := mgr.TryAcquire(ctx, "try/res", 30*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !ok {
		t.Fatal("TryAcquire: expected ok=true when resource is free")
	}
	defer l.Release()
}

func TestManager_TryAcquire_WhenHeld(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr1, mgr2 := NewMemoryManagerPair("node-1", "node-2")

	l1, ok, err := mgr1.TryAcquire(ctx, "shared/leader", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("TryAcquire (node-1): ok=%v err=%v", ok, err)
	}
	defer l1.Release()

	_, ok2, err := mgr2.TryAcquire(ctx, "shared/leader", 30*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire (node-2): unexpected error: %v", err)
	}
	if ok2 {
		t.Error("TryAcquire (node-2): expected ok=false while held by node-1")
	}
}

func TestManager_IsLeader_BeforeAndAfterAcquire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr1, mgr2 := NewMemoryManagerPair("node-1", "node-2")

	// Neither is leader before acquire.
	ok, err := mgr1.IsLeader(ctx, "shared/leader")
	if err != nil || ok {
		t.Fatalf("IsLeader(node-1) before acquire: got (%v,%v), want (false,nil)", ok, err)
	}

	l, _, _ := mgr1.TryAcquire(ctx, "shared/leader", 30*time.Second)
	defer l.Release()

	ok, err = mgr1.IsLeader(ctx, "shared/leader")
	if err != nil || !ok {
		t.Errorf("IsLeader(node-1) after acquire: got (%v,%v), want (true,nil)", ok, err)
	}

	ok, err = mgr2.IsLeader(ctx, "shared/leader")
	if err != nil || ok {
		t.Errorf("IsLeader(node-2) while node-1 holds: got (%v,%v), want (false,nil)", ok, err)
	}
}

func TestManager_IsLeader_AfterRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr := NewMemoryManager("node-1")

	l, _, _ := mgr.TryAcquire(ctx, "rel/res", 30*time.Second)
	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	ok, err := mgr.IsLeader(ctx, "rel/res")
	if err != nil || ok {
		t.Errorf("IsLeader after Release: got (%v,%v), want (false,nil)", ok, err)
	}
}

func TestManager_KeepAlive_ClosesOnRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr := NewMemoryManager("node-1")

	l, err := mgr.Acquire(ctx, "ka/res", 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	lostCh := l.KeepAlive(ctx)

	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	select {
	case <-lostCh:
		// expected
	case <-time.After(2 * time.Second):
		t.Error("KeepAlive channel not closed within 2s of Release")
	}
}

func TestManager_KeepAlive_ClosesOnContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	mgr := NewMemoryManager("node-1")
	l, err := mgr.Acquire(ctx, "ctx/res", 30*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	lostCh := l.KeepAlive(ctx)
	cancel() // cancel context → keepalive goroutine revokes the lease

	select {
	case <-lostCh:
		// expected
	case <-time.After(2 * time.Second):
		t.Error("KeepAlive channel not closed within 2s of context cancel")
	}
}

func TestManager_TryAcquire_AfterRelease_Succeeds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr1, mgr2 := NewMemoryManagerPair("node-1", "node-2")

	l1, ok, _ := mgr1.TryAcquire(ctx, "handoff/leader", 30*time.Second)
	if !ok {
		t.Fatal("node-1 failed to acquire")
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	l2, ok, err := mgr2.TryAcquire(ctx, "handoff/leader", 30*time.Second)
	if err != nil || !ok {
		t.Errorf("node-2 TryAcquire after release: ok=%v err=%v", ok, err)
	}
	if l2 != nil {
		defer l2.Release()
	}
}

func TestManager_Acquire_Blocks_UntilRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mgr1, mgr2 := NewMemoryManagerPair("blocker", "waiter")

	l1, ok, err := mgr1.TryAcquire(ctx, "block/res", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("mgr1 TryAcquire: ok=%v err=%v", ok, err)
	}

	acquiredCh := make(chan *Lease, 1)
	go func() {
		l, _ := mgr2.Acquire(ctx, "block/res", 30*time.Second)
		acquiredCh <- l
	}()

	// Give mgr2 time to block.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-acquiredCh:
		t.Fatal("mgr2 acquired the lease before mgr1 released it")
	default:
	}

	// Release; mgr2 should acquire within the next retry window.
	l1.Release()

	select {
	case l2 := <-acquiredCh:
		if l2 == nil {
			t.Error("mgr2.Acquire returned nil after release")
		} else {
			l2.Release()
		}
	case <-time.After(3 * time.Second):
		t.Error("mgr2 did not acquire after mgr1 released (within 3s)")
	}
}

// ─── etcd integration tests ───────────────────────────────────────────────────

// TestLeaseManager_Etcd runs the lease contract against a live etcd cluster.
// Set ETCD_ENDPOINTS to a comma-separated list of endpoints to enable it.
func TestLeaseManager_Etcd(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("ETCD_ENDPOINTS not set; skipping etcd lease integration tests")
	}

	eps := strings.Split(endpoints, ",")
	cfg := metadata.EtcdConfig{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
		Prefix:      "/globalfs-lease-test/",
	}
	ctx := context.Background()

	store, err := metadata.NewEtcdStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	defer store.Close()

	mgr1 := NewManager("node-1", store)
	mgr2 := NewManager("node-2", store)

	// Ensure clean state.
	key := mgr1.resourceKey("test/leader")
	_ = mgr1.backend.del(ctx, key)

	// node-1 acquires.
	l1, ok, err := mgr1.TryAcquire(ctx, "test/leader", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("TryAcquire (node-1): ok=%v err=%v", ok, err)
	}
	defer l1.Release()

	// node-2 should fail.
	_, ok2, err := mgr2.TryAcquire(ctx, "test/leader", 30*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire (node-2): %v", err)
	}
	if ok2 {
		t.Error("node-2 acquired lease while node-1 holds it")
	}

	// node-1 is the leader.
	isLeader, err := mgr1.IsLeader(ctx, "test/leader")
	if err != nil || !isLeader {
		t.Errorf("IsLeader(node-1): got (%v,%v), want (true,nil)", isLeader, err)
	}

	// Release and verify node-2 can take over.
	if err := l1.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	l2, ok, err := mgr2.TryAcquire(ctx, "test/leader", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("TryAcquire (node-2) after release: ok=%v err=%v", ok, err)
	}
	defer l2.Release()

	isLeader, err = mgr2.IsLeader(ctx, "test/leader")
	if err != nil || !isLeader {
		t.Errorf("IsLeader(node-2) after acquiring: got (%v,%v), want (true,nil)", isLeader, err)
	}
}
