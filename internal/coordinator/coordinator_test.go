package coordinator

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	objectfserrors "github.com/scttfrdmn/objectfs/pkg/errors"
	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"
	objectfssdk "github.com/scttfrdmn/objectfs/sdks/go/objectfs"

	"github.com/scttfrdmn/globalfs/internal/cache"
	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/lease"
	"github.com/scttfrdmn/globalfs/internal/metadata"
	"github.com/scttfrdmn/globalfs/internal/metrics"
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
	// putGate, if non-nil, blocks Put until the channel is closed.  Tests that
	// assert on state which only exists *while* a transfer is in flight need
	// the transfer held open; without it the worker and the drain goroutine can
	// run to completion before the assertion executes.
	putGate chan struct{}
	// putsEntered counts calls that have reached the gate.  A test that wants to
	// act *while* a transfer is in flight has to know the transfer has actually
	// started, and polling the destination for the key cannot tell it — the key
	// only appears once the gate is released.
	putsEntered int
	// healthGate is the same device for Health.  It deliberately ignores the
	// caller's context, because that is what the real stack does: objectfs's
	// ClientManager.HealthCheck acquires a pooled client through
	// ConnectionPool.Get, which takes no context and waits up to 30 s of its own
	// before the ctx-aware HeadBucket is ever reached.  A gate that honoured ctx
	// would make the shutdown-bound tests pass for the wrong reason (#83).
	healthGate    chan struct{}
	healthEntered int
	// closes counts Close calls, so tests can assert that removing a site
	// releases exactly the resources it took (#80).
	closes int
}

func newMemClient(objs map[string][]byte) *memClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &memClient{objects: objs}
}

// notFound returns the error a real objectfs client returns for an absent key.
// It is code-matched by errors.Is against objectfssdk.ErrNotFound, including
// through wrapping, which is what the coordinator's classification relies on.
func notFound(key string) error {
	return objectfserrors.NewError(objectfserrors.ErrCodeObjectNotFound, "object does not exist").
		WithComponent("memclient").
		WithContext("key", key)
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
		return nil, notFound(key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *memClient) Put(_ context.Context, key string, data []byte) error {
	if m.putErr != nil {
		return m.putErr
	}
	// Read the gate under the lock, then wait outside it: blocking while holding
	// m.mu would deadlock any concurrent hasKey/keys call.
	m.mu.Lock()
	gate := m.putGate
	m.putsEntered++
	m.mu.Unlock()
	if gate != nil {
		<-gate
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
		return nil, notFound(key)
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
	gate := m.healthGate
	m.healthEntered++
	err := m.healthErr
	m.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return err
}

func (m *memClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closes++
	return nil
}

// closeCount reports how many times Close has been called.  Site removal must
// close exactly what it removed: an unclosed SiteMount leaks a connection pool,
// and closing one twice would be a double-free of the same pool.
func (m *memClient) closeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closes
}

func (m *memClient) setHealthErr(err error) {
	m.mu.Lock()
	m.healthErr = err
	m.mu.Unlock()
}

// blockPuts makes every subsequent Put on this client wait.  The returned
// function releases them and is safe to call more than once.
func (m *memClient) blockPuts() (release func()) {
	gate := make(chan struct{})
	m.mu.Lock()
	m.putGate = gate
	m.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// blockHealth makes every subsequent Health probe on this client wait.
func (m *memClient) blockHealth() (release func()) {
	gate := make(chan struct{})
	m.mu.Lock()
	m.healthGate = gate
	m.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// waitForPut blocks until at least n Put calls have reached the gate, or the
// timeout elapses (reported as a fatal test failure).
func (m *memClient) waitForPut(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	m.waitForEntry(t, "Put", n, timeout, func() int { return m.putsEntered })
}

// waitForHealth blocks until at least n Health probes have reached the gate.
func (m *memClient) waitForHealth(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	m.waitForEntry(t, "Health", n, timeout, func() int { return m.healthEntered })
}

// waitForEntry polls count (called under m.mu) until it reaches n, or fails.
func (m *memClient) waitForEntry(t *testing.T, what string, n int, timeout time.Duration, count func() int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		entered := count()
		m.mu.Unlock()
		if entered >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s call(s) reached the client within %v", n, what, timeout)
}

func makeMount(name string, role types.SiteRole, objs map[string][]byte) (*site.SiteMount, *memClient) {
	mc := newMemClient(objs)
	return site.New(name, role, mc), mc
}

// mustStart starts the coordinator and fails the test if it refuses.
//
// Start returns an error now (#84), and a test that discards it would silently
// exercise an unstarted coordinator — which is the exact failure mode these fixes
// are about, so it must not be possible to reintroduce it by inattention.
func mustStart(t *testing.T, ctx context.Context, c *Coordinator) {
	t.Helper()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// mustConfigure asserts that a Set* call before Start was accepted.
func mustConfigure(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("configuration call before Start was rejected: %v", err)
	}
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
	mustStart(t, ctx, c)
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

// ─── Site removal with duplicate names (#80) ──────────────────────────────────

// TestCoordinator_RemoveSite_DuplicateNamesRemovesExactlyOne is the core #80
// assertion.  Before the fix, one RemoveSite call filtered out every match while
// keeping only the last, so a coordinator with two sites named "primary" was
// emptied by a single call and only one of the two was closed.
func TestCoordinator_RemoveSite_DuplicateNamesRemovesExactlyOne(t *testing.T) {
	t.Parallel()

	first, firstClient := makeMount("primary", types.SiteRolePrimary, nil)
	second, secondClient := makeMount("primary", types.SiteRolePrimary, nil)
	other, otherClient := makeMount("backup", types.SiteRoleBackup, nil)

	c := New(first, second, other)

	if !c.RemoveSite("primary") {
		t.Fatal("RemoveSite(\"primary\"): reported not found")
	}

	sites := c.Sites()
	if len(sites) != 2 {
		t.Fatalf("after one RemoveSite the coordinator holds %d sites, want 2 — "+
			"a single call removed more than it was asked to; sites=%v", len(sites), siteNames(sites))
	}
	// The highest-priority match goes; the duplicate and the unrelated site stay.
	if got := siteNames(sites); got[0] != "primary" || got[1] != "backup" {
		t.Errorf("remaining sites = %v, want [primary backup]", got)
	}
	if sites[0] != second {
		t.Error("the wrong duplicate was kept: removal should take the highest-priority match")
	}

	// Whatever was removed must be closed, and nothing else may be.
	if got := firstClient.closeCount(); got != 1 {
		t.Errorf("removed site closed %d times, want 1 — an unclosed SiteMount leaks its connection pool", got)
	}
	if got := secondClient.closeCount(); got != 0 {
		t.Errorf("retained duplicate was closed %d times, want 0 — it is still in the routing set", got)
	}
	if got := otherClient.closeCount(); got != 0 {
		t.Errorf("unrelated site was closed %d times, want 0", got)
	}

	// A second call takes the duplicate, so the state is still reachable.
	if !c.RemoveSite("primary") {
		t.Fatal("second RemoveSite(\"primary\"): reported not found")
	}
	if got := siteNames(c.Sites()); len(got) != 1 || got[0] != "backup" {
		t.Errorf("after the second removal sites = %v, want [backup]", got)
	}
	if got := secondClient.closeCount(); got != 1 {
		t.Errorf("duplicate closed %d times after its own removal, want 1", got)
	}
	if got := firstClient.closeCount(); got != 1 {
		t.Errorf("first site closed %d times, want 1 — no double close", got)
	}
}

// TestCoordinator_RemoveSite_ClosesTheSiteItRemoved covers the ordinary
// unique-name case: removal is not just a list edit, it must release the
// connection pool (#62).
func TestCoordinator_RemoveSite_ClosesTheSiteItRemoved(t *testing.T) {
	t.Parallel()

	a, aClient := makeMount("a", types.SiteRolePrimary, nil)
	b, bClient := makeMount("b", types.SiteRoleBackup, nil)
	c := New(a, b)

	if !c.RemoveSite("a") {
		t.Fatal("RemoveSite(\"a\"): reported not found")
	}
	if got := aClient.closeCount(); got != 1 {
		t.Errorf("removed site closed %d times, want 1", got)
	}
	if got := bClient.closeCount(); got != 0 {
		t.Errorf("retained site closed %d times, want 0", got)
	}

	// A miss must not close anything.
	if c.RemoveSite("nope") {
		t.Error("RemoveSite(\"nope\"): reported a removal that did not happen")
	}
	if got := bClient.closeCount(); got != 0 {
		t.Errorf("after a miss, retained site closed %d times, want 0", got)
	}
}

// TestCoordinator_RemoveSite_ReplacesNamespaceView verifies the merged view is
// rebuilt, not just the site slice: a removed site must stop answering List.
func TestCoordinator_RemoveSite_ReplacesNamespaceView(t *testing.T) {
	t.Parallel()

	a, _ := makeMount("a", types.SiteRolePrimary, map[string][]byte{"only-on-a": []byte("x")})
	b, _ := makeMount("b", types.SiteRoleBackup, map[string][]byte{"only-on-b": []byte("y")})
	c := New(a, b)

	if !c.RemoveSite("a") {
		t.Fatal("RemoveSite(\"a\"): reported not found")
	}
	items, err := c.List(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, it := range items {
		if it.Key == "only-on-a" {
			t.Error("a removed site still contributes to List — the namespace view was not rebuilt")
		}
	}
}

// TestCoordinator_AddSiteUnique_RejectsDuplicateName verifies the invariant is
// enforced at the point of entry, so RemoveSite's ambiguity is unreachable in
// the first place.
func TestCoordinator_AddSiteUnique_RejectsDuplicateName(t *testing.T) {
	t.Parallel()

	first, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(first)

	dup, dupClient := makeMount("primary", types.SiteRoleBackup, nil) // different role, same name
	err := c.AddSiteUnique(dup)
	if err == nil {
		t.Fatal("AddSiteUnique accepted a duplicate name")
	}
	if !errors.Is(err, ErrDuplicateSite) {
		t.Errorf("expected ErrDuplicateSite, got %v", err)
	}
	if got := len(c.Sites()); got != 1 {
		t.Errorf("site count is %d after a rejected add, want 1", got)
	}
	// The rejected site belongs to the caller; AddSiteUnique must not close it.
	if got := dupClient.closeCount(); got != 0 {
		t.Errorf("rejected site was closed %d times, want 0 — ownership stays with the caller", got)
	}

	// A distinct name still works, and lands at lowest priority.
	fresh, _ := makeMount("backup", types.SiteRoleBackup, nil)
	if err := c.AddSiteUnique(fresh); err != nil {
		t.Fatalf("AddSiteUnique with a fresh name: %v", err)
	}
	if got := siteNames(c.Sites()); len(got) != 2 || got[1] != "backup" {
		t.Errorf("sites = %v, want [primary backup]", got)
	}
}

// TestCoordinator_AddSiteUnique_Concurrent verifies the check and the append are
// one atomic step: N racing adds of the same name must yield exactly one site.
func TestCoordinator_AddSiteUnique_Concurrent(t *testing.T) {
	t.Parallel()

	c := New()
	const racers = 16
	var wg sync.WaitGroup
	accepted := make(chan struct{}, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := makeMount("primary", types.SiteRolePrimary, nil)
			if err := c.AddSiteUnique(s); err == nil {
				accepted <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(accepted)

	if got := len(accepted); got != 1 {
		t.Errorf("%d concurrent adds of the same name were accepted, want 1", got)
	}
	if got := len(c.Sites()); got != 1 {
		t.Errorf("coordinator holds %d sites after racing adds, want 1", got)
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
	mustStart(t, ctx, c)
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

// TestCoordinator_Health_ImposesDeadlineWhenNone verifies that Health returns
// even when called with a plain background context (no deadline) by checking
// that the call completes in well under the defaultHealthTimeout (#47).
func TestCoordinator_Health_ImposesDeadlineWhenNone(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	start := time.Now()
	report := c.Health(context.Background())
	elapsed := time.Since(start)

	if report["primary"] != nil {
		t.Errorf("Health[primary]: expected nil, got %v", report["primary"])
	}
	// The call should complete quickly (well under the 30s defaultHealthTimeout).
	if elapsed > 5*time.Second {
		t.Errorf("Health took %v — expected well under 5s with fast in-memory backend", elapsed)
	}
}

// TestCoordinator_SetWorkerQueueDepth verifies that SetWorkerQueueDepth
// configures the worker before Start and does not affect running coordinators
// (#50).
func TestCoordinator_SetWorkerQueueDepth(t *testing.T) {
	t.Parallel()

	src, _ := makeMount("src", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	dst, dstClient := makeMount("dst", types.SiteRoleBackup, nil)

	c := New(src, dst)
	mustConfigure(t, c.SetWorkerQueueDepth(4)) // small depth to verify it's applied

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
	defer c.Stop()

	if err := c.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dstClient.hasKey("k") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("backup never received key — SetWorkerQueueDepth may have broken the worker")
}

// TestCoordinator_SetLeaseTTL verifies that SetLeaseTTL is wired to leader
// election and does not panic when a lease manager is absent (#50).
func TestCoordinator_SetLeaseTTL_NoLeaseManager(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)
	mustConfigure(t, c.SetLeaseTTL(30*time.Second)) // must not panic; no lease manager set

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
	defer c.Stop()
}

// ─── Store integration tests ───────────────────────────────────────────────────

// TestCoordinator_SetStore_PersistsReplicationJob verifies that a Put to a
// non-primary site writes a ReplicationJob record to the store before the
// worker delivers it.
func TestCoordinator_SetStore_PersistsReplicationJob(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A persisted job is deleted by the drain goroutine as soon as the transfer
	// completes, so its presence in the store is transient. Hold the destination
	// write open for the duration of the assertion; otherwise the worker and the
	// drain can both finish before GetPendingJobs runs and the job is legitimately
	// gone. That is what made this test flake on CI, where two cores and a loaded
	// runner widen the window enough for the race to land.
	release := backupClient.blockPuts()

	store := metadata.NewMemoryStore()
	c := New(primary, backup)
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)
	// Defers run LIFO, so this releases the blocked transfer *before* Stop runs.
	// The reverse order would have Stop wait on a worker parked inside Put, which
	// is the shutdown hang tracked in #83.
	defer c.Stop()
	defer release()

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
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)
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

// TestCoordinator_Put_StoreFailureSkipsEnqueue verifies that when
// PutReplicationJob returns an error the job is NOT enqueued in the worker,
// preserving the durability guarantee that the store is the source of truth.
// Regression test for #56.
func TestCoordinator_Put_StoreFailureSkipsEnqueue(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &failingPutJobStore{MemoryStore: metadata.NewMemoryStore()}
	c := New(primary, backup)
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)
	defer c.Stop()

	if err := c.Put(ctx, "genome.bam", []byte("data")); err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	// Give the worker time it would have needed to replicate, then assert the
	// backup site did NOT receive the object.
	time.Sleep(500 * time.Millisecond)

	if backupClient.hasKey("genome.bam") {
		t.Error("backup site should NOT have received the object when store.PutReplicationJob failed")
	}

	// The store itself should also have no pending jobs (PutReplicationJob failed).
	jobs, _ := store.GetPendingJobs(ctx)
	if len(jobs) != 0 {
		t.Errorf("expected zero pending jobs in store, got %d", len(jobs))
	}
}

// failingPutJobStore wraps MemoryStore and injects a PutReplicationJob failure.
type failingPutJobStore struct {
	*metadata.MemoryStore
}

func (f *failingPutJobStore) PutReplicationJob(_ context.Context, _ *metadata.ReplicationJob) error {
	return errors.New("simulated storage failure")
}

// ─── Full replication queue (#79) ─────────────────────────────────────────────

// TestCoordinator_Put_FullQueueDoesNotReportSuccess is the core #79 assertion:
// a write whose replication was not queued must not be reported as replicated.
// Before the fix, 40 sequential Puts at the shipped defaults all returned nil
// and 9 reached the backup.
func TestCoordinator_Put_FullQueueDoesNotReportSuccess(t *testing.T) {
	t.Parallel()

	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Park the worker inside the destination Put so nothing ever drains the
	// queue: whatever depth it has, it fills and stays full.
	release := backupClient.blockPuts()
	defer release()

	const depth = 2
	c := New(primary, backup)
	mustConfigure(t, c.SetWorkerQueueDepth(depth))
	// Keep the test quick; the default 2 s budget × N Puts is not worth waiting for.
	c.SetEnqueueBackpressure(50 * time.Millisecond)
	mustStart(t, ctx, c)
	// LIFO: release the blocked transfer before Stop, or Stop parks on a worker
	// inside Put and the test hangs instead of failing (#83).
	defer c.Stop()
	defer release()

	const total = 12
	var dropped, ok int
	for i := 0; i < total; i++ {
		err := c.Put(ctx, fmt.Sprintf("obj-%02d", i), []byte("payload"))
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrReplicationNotQueued):
			dropped++
		default:
			t.Fatalf("Put %d: unexpected error: %v", i, err)
		}
	}

	if dropped == 0 {
		t.Fatalf("all %d Puts reported success with a permanently full queue of depth %d; "+
			"the caller was told writes were replicated when they were not", total, depth)
	}

	// Every Put still stored the data on the primary — that is the whole point of
	// calling it a partial success rather than a failure.
	if got := len(primaryClient.keys()); got != total {
		t.Errorf("primary holds %d keys, want %d — the synchronous write must always land", got, total)
	}

	t.Logf("depth=%d: %d Puts queued, %d reported ErrReplicationNotQueued", depth, ok, dropped)
}

// TestCoordinator_Put_ErrReplicationNotQueuedIsDistinguishable verifies that the
// partial-success error is separable from a genuine write failure.  A caller
// that cannot tell them apart cannot decide whether to retry or to alarm.
func TestCoordinator_Put_ErrReplicationNotQueuedIsDistinguishable(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := backupClient.blockPuts()
	defer release()

	c := New(primary, backup)
	mustConfigure(t, c.SetWorkerQueueDepth(1))
	c.SetEnqueueBackpressure(-1) // no waiting: fail the first full Enqueue
	mustStart(t, ctx, c)
	defer c.Stop()
	defer release()

	// First Put fills the queue (depth 1); the worker takes it and parks in Put.
	if err := c.Put(ctx, "a", []byte("x")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	// Keep going until one is refused — the worker may have dequeued the first.
	var partial error
	for i := 0; i < 10 && partial == nil; i++ {
		if err := c.Put(ctx, fmt.Sprintf("b-%d", i), []byte("x")); err != nil {
			partial = err
		}
	}
	if partial == nil {
		t.Fatal("no Put was refused with a full queue and no backpressure budget")
	}
	if !errors.Is(partial, ErrReplicationNotQueued) {
		t.Errorf("full queue: expected ErrReplicationNotQueued, got %v", partial)
	}
	// It must not masquerade as a not-found or any other coordinator condition.
	if errors.Is(partial, ErrNotFound) {
		t.Errorf("full queue error should not wrap ErrNotFound: %v", partial)
	}

	// A genuine primary write failure is a different error entirely.
	badPrimary := site.New("bad", types.SiteRolePrimary,
		&memClient{putErr: errors.New("disk full"), objects: map[string][]byte{}})
	writeErr := New(badPrimary).Put(ctx, "k", []byte("x"))
	if writeErr == nil {
		t.Fatal("expected an error when the primary write fails")
	}
	if errors.Is(writeErr, ErrReplicationNotQueued) {
		t.Errorf("a failed primary write must not report ErrReplicationNotQueued: %v", writeErr)
	}
}

// TestCoordinator_Put_BackpressureWaitsForRoom verifies the preferred behaviour:
// a transient full queue is absorbed by waiting, not turned into an error.  A
// burst against a depth-1 queue should be fully replicated.
func TestCoordinator_Put_BackpressureWaitsForRoom(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(primary, backup)
	mustConfigure(t, c.SetWorkerQueueDepth(1)) // pathologically small; the worker drains it fast
	mustStart(t, ctx, c)
	defer c.Stop()

	const total = 20
	for i := 0; i < total; i++ {
		if err := c.Put(ctx, fmt.Sprintf("burst-%02d", i), []byte("payload")); err != nil {
			t.Fatalf("Put %d: queue room should have been waited for, got %v", i, err)
		}
	}

	// Every write should reach the backup; nothing was dropped.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(backupClient.keys()) == total {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("backup holds %d of %d objects — a burst was silently dropped", len(backupClient.keys()), total)
}

// TestCoordinator_Put_BackpressureRespectsContextCancellation verifies the wait
// is abortable.  An unbounded or uncancellable wait would let one wedged
// destination stall every writer, which is the reason the budget is finite.
func TestCoordinator_Put_BackpressureRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	release := backupClient.blockPuts()
	defer release()

	c := New(primary, backup)
	mustConfigure(t, c.SetWorkerQueueDepth(1))
	c.SetEnqueueBackpressure(time.Hour) // would hang without cancellation
	mustStart(t, startCtx, c)
	defer c.Stop()
	defer release()

	if err := c.Put(startCtx, "fill", []byte("x")); err != nil {
		t.Fatalf("first Put: %v", err)
	}

	putCtx, putCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer putCancel()

	done := make(chan error, 1)
	go func() {
		var err error
		// Keep writing until one blocks on the full queue and the ctx expires.
		for i := 0; i < 10; i++ {
			if err = c.Put(putCtx, fmt.Sprintf("blocked-%d", i), []byte("x")); err != nil {
				break
			}
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a Put to fail once the context expired")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected the context error to propagate, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put did not honour context cancellation while waiting for queue room")
	}
}

// TestCoordinator_Put_FullQueueIncrementsDroppedCounter verifies the drop is
// observable.  The queue-depth gauge cannot serve: it only updates on job
// settle, and it reads zero again once the backlog clears.
func TestCoordinator_Put_FullQueueIncrementsDroppedCounter(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := backupClient.blockPuts()
	defer release()

	reg := prometheus.NewRegistry()
	c := New(primary, backup)
	mustConfigure(t, c.SetMetrics(metrics.New(reg)))
	mustConfigure(t, c.SetWorkerQueueDepth(1))
	c.SetEnqueueBackpressure(-1)
	mustStart(t, ctx, c)
	defer c.Stop()
	defer release()

	for i := 0; i < 10; i++ {
		_ = c.Put(ctx, fmt.Sprintf("k-%d", i), []byte("x"))
	}

	got := counterValue(t, reg, "globalfs_replication_dropped_total")
	if got == 0 {
		t.Error("globalfs_replication_dropped_total is 0 after dropped enqueues — " +
			"a queue that discards writes has to be observable")
	}
	t.Logf("globalfs_replication_dropped_total = %v", got)
}

// counterValue reads a single unlabelled counter out of a registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, mtc := range f.GetMetric() {
			return mtc.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %q not found in registry", name)
	return 0
}

// TestCoordinator_Put_NoStore_FullQueueStillReports covers the shipped
// configuration: no metadata store, so nothing recovers a dropped job and the
// error to the caller is the only signal that exists.
func TestCoordinator_Put_NoStore_FullQueueStillReports(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := backupClient.blockPuts()
	defer release()

	c := New(primary, backup) // no SetStore
	mustConfigure(t, c.SetWorkerQueueDepth(1))
	c.SetEnqueueBackpressure(-1)
	mustStart(t, ctx, c)
	defer c.Stop()
	defer release()

	var sawError bool
	for i := 0; i < 10; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k-%d", i), []byte("x")); err != nil {
			if !errors.Is(err, ErrReplicationNotQueued) {
				t.Fatalf("Put: unexpected error: %v", err)
			}
			sawError = true
		}
	}
	if !sawError {
		t.Error("with no store and a full queue, Put reported success for an unreplicated write")
	}
}

// ─── Shutdown while busy (#78) ────────────────────────────────────────────────

// TestCoordinator_Stop_WhileTransferInFlight_SettlesTheJob is the test the suite
// did not have: Stop() against a coordinator with a transfer genuinely in
// flight.  Before #78, Stop() cancelled the event drain and only then called
// worker.Stop(), so the EventCompleted the finishing transfer emitted had no
// reader — the job stayed in the store as a phantom to be re-enqueued on the
// next boot, and the v0.2.0 dedup hash was lost.
func TestCoordinator_Stop_WhileTransferInFlight_SettlesTheJob(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hold the destination write open so the transfer is still running when
	// Stop() is entered.
	release := backupClient.blockPuts()
	defer release() // safety net: never leave the worker parked if we fail early

	store := metadata.NewMemoryStore()
	c := New(primary, backup)
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)

	const key = "data/sample.bam"
	if err := c.Put(ctx, key, []byte("genome")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The transfer must actually have begun, otherwise this test would pass
	// against the buggy ordering by shutting down before there was any event.
	backupClient.waitForPut(t, 1, 2*time.Second)

	// Stop() runs on its own goroutine because it blocks on the in-flight
	// transfer, and the transfer cannot finish until this goroutine releases it.
	stopped := make(chan struct{})
	go func() {
		c.Stop()
		close(stopped)
	}()

	// Let Stop() get as far as it can, then let the transfer complete.  This is
	// the SIGTERM-mid-transfer case: the shutdown is under way and the transfer
	// then succeeds.
	time.Sleep(50 * time.Millisecond)
	release()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s")
	}

	// The transfer succeeded, so the object is at the destination.
	if !backupClient.hasKey(key) {
		t.Fatal("backup did not receive the object — the transfer did not complete")
	}

	// Terminal event must have been consumed: no phantom job left behind.
	jobs, err := store.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("store has %d pending job(s) after Stop; the completion event was lost, "+
			"so this job would be re-enqueued and the object transferred again: %v", len(jobs), jobs)
	}

	// ...and the dedup hash must have been recorded.
	rec, err := store.GetReplicatedObject(ctx, "backup", key)
	if err != nil {
		t.Fatalf("GetReplicatedObject: %v", err)
	}
	if rec == nil {
		t.Fatal("no ReplicatedObject recorded for backup/" + key +
			"; the content-hash index now disagrees with reality")
	}
	if rec.ContentHash == "" {
		t.Error("ReplicatedObject recorded with an empty ContentHash")
	}
}

// TestCoordinator_Stop_DrainFlushesBufferedEvents covers the narrower window
// that ordering alone does not close: an event already emitted and sitting in
// the buffer when the drain's context is cancelled.  The shipped daemon produces
// exactly this — cmd/coordinator cancels the root context and then calls Close,
// so the drain can be gone before Stop is entered.
func TestCoordinator_Stop_DrainFlushesBufferedEvents(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := backupClient.blockPuts()
	defer release()

	store := metadata.NewMemoryStore()
	c := New(primary, backup)
	mustConfigure(t, c.SetStore(store))
	mustStart(t, ctx, c)

	const key = "reads.fastq"
	if err := c.Put(ctx, key, []byte("sequence")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	backupClient.waitForPut(t, 1, 2*time.Second)

	// Kill the drain goroutine first, the way the daemon does, and only then
	// release the transfer.  The completion event is emitted into a buffer with
	// no live reader; Stop's final flush is the only thing that can consume it.
	cancel()
	// Wait for the drain to observe the cancellation.  The health poller has its
	// own WaitGroup now and is irrelevant here.
	c.drainWg.Wait()
	release()

	c.Stop()

	jobs, err := store.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("store has %d pending job(s); a buffered terminal event went unread: %v", len(jobs), jobs)
	}
	if rec, _ := store.GetReplicatedObject(context.Background(), "backup", key); rec == nil {
		t.Error("dedup hash not recorded from the buffered completion event")
	}
}

// TestCoordinator_Stop_BeforeStart guards the ordering change against the
// Stop-before-Start path (tracked separately as #84): worker.Stop() now runs
// first, on a worker that was never started.
func TestCoordinator_Stop_BeforeStart(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	done := make(chan struct{})
	go func() {
		c.Stop() // must not panic and must not block
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() before Start() did not return within 2s")
	}
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
	mustConfigure(t, c.SetLeaseManager(mgr))
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetLeaseManager(mgr2))
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetLeaseManager(mgr))
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetHealthPollInterval(20*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetHealthPollInterval(20*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetHealthPollInterval(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)

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
	mustConfigure(t, c.SetHealthPollInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetHealthPollInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
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
	mustConfigure(t, c.SetHealthPollInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
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

// ─── Not-found classification (#77) ───────────────────────────────────────────

// TestCoordinator_Get_MissingKeyDoesNotTripBreaker verifies that reads of an
// absent key leave the circuit closed.  Before #77 every non-nil error recorded
// a failure, so five cache misses at the default threshold ejected a healthy
// site from routing for the whole cooldown.
func TestCoordinator_Get_MissingKeyDoesNotTripBreaker(t *testing.T) {
	t.Parallel()

	// The site is entirely healthy — it just does not hold this key.
	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{
		"present.bam": []byte("data"),
	})

	cb := circuitbreaker.New(5, time.Hour) // the shipped default threshold
	c := New(primary)
	c.SetCircuitBreaker(cb)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := c.Get(ctx, "absent.bam")
		if err == nil {
			t.Fatal("Get of an absent key should return an error")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get of an absent key: error should wrap ErrNotFound, got %v", err)
		}
	}

	if got := cb.State("primary"); got != circuitbreaker.StateClosed {
		t.Errorf("circuit after 10 misses on a healthy site: got %v, want closed", got)
	}

	// The site must still be readable — the point of the bug was that it wasn't.
	data, err := c.Get(ctx, "present.bam")
	if err != nil {
		t.Fatalf("Get of a present key after 10 misses: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("Get: got %q, want data", data)
	}
}

// TestCoordinator_Head_MissingKeyDoesNotTripBreaker is the Head half of #77.
func TestCoordinator_Head_MissingKeyDoesNotTripBreaker(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)

	cb := circuitbreaker.New(5, time.Hour)
	c := New(primary)
	c.SetCircuitBreaker(cb)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := c.Head(ctx, "absent.bam"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Head of an absent key: error should wrap ErrNotFound, got %v", err)
		}
	}

	if got := cb.State("primary"); got != circuitbreaker.StateClosed {
		t.Errorf("circuit after 10 Head misses: got %v, want closed", got)
	}
}

// TestCoordinator_Get_RealFailureStillTripsBreaker guards the other direction:
// the classification must not have made the breaker inert.
func TestCoordinator_Get_RealFailureStillTripsBreaker(t *testing.T) {
	t.Parallel()

	// A bare error carrying no objectfs code — the unclassifiable case, which
	// counts as a failure by design.
	client := &memClient{getErr: errors.New("S3 unreachable"), objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, client)

	cb := circuitbreaker.New(3, time.Hour)
	c := New(primary)
	c.SetCircuitBreaker(cb)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = c.Get(ctx, "key")
	}

	if got := cb.State("primary"); got != circuitbreaker.StateOpen {
		t.Errorf("circuit after 3 genuine failures: got %v, want open", got)
	}
}

// TestCoordinator_Get_NotFoundIsDistinguishableFromOutage verifies that the two
// conditions produce different errors, which is what lets the API layer return
// 404 for one and 502 for the other.
func TestCoordinator_Get_NotFoundIsDistinguishableFromOutage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	absent, _ := makeMount("absent", types.SiteRolePrimary, nil)
	_, err := New(absent).Get(ctx, "k")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("absent key: expected ErrNotFound, got %v", err)
	}
	// ErrNotFound wraps the objectfs sentinel, so callers may test either.
	if !errors.Is(err, objectfssdk.ErrNotFound) {
		t.Errorf("absent key: expected error to wrap objectfssdk.ErrNotFound, got %v", err)
	}

	down := site.New("down", types.SiteRolePrimary,
		&memClient{getErr: errors.New("connection refused"), objects: map[string][]byte{}})
	_, err = New(down).Get(ctx, "k")
	if err == nil {
		t.Fatal("unreachable site: expected an error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("unreachable site: must NOT report ErrNotFound, got %v", err)
	}
}

// TestCoordinator_Get_MixedNotFoundAndFailureIsNotNotFound verifies that one
// site failing to answer is enough to make the whole read an outage rather than
// an absence: the object may well exist on the site that could not be reached.
func TestCoordinator_Get_MixedNotFoundAndFailureIsNotNotFound(t *testing.T) {
	t.Parallel()

	absent, _ := makeMount("absent", types.SiteRolePrimary, nil)
	down := site.New("down", types.SiteRoleBackup,
		&memClient{getErr: errors.New("connection refused"), objects: map[string][]byte{}})

	_, err := New(absent, down).Get(context.Background(), "k")
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("one site unreachable: must not claim not-found, got %v", err)
	}
}

// TestCoordinator_Get_NotFoundIsNotRetried verifies that a missing key costs one
// round trip, not MaxAttempts.  Retrying an absent key is pointless: the site
// answered, and it will answer the same way three times.
func TestCoordinator_Get_NotFoundIsNotRetried(t *testing.T) {
	t.Parallel()

	client := &memClient{objects: map[string][]byte{}}
	primary := site.New("primary", types.SiteRolePrimary, client)

	calls := 0
	client.getFn = func(key string) ([]byte, error) {
		calls++
		return nil, notFound(key)
	}

	c := New(primary)
	c.SetRetryConfig(&retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	})

	_, err := c.Get(context.Background(), "absent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if calls != 1 {
		t.Errorf("absent key produced %d site calls, want 1 (not-found is not transient)", calls)
	}
}

// TestIsSiteFailure_Classification pins the classification table directly.
func TestIsSiteFailure_Classification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"object not found", notFound("k"), false},
		{"wrapped not found", fmt.Errorf("get from %q: %w", "primary", notFound("k")), false},
		{"context canceled", context.Canceled, false},
		{"wrapped context canceled", fmt.Errorf("site attempt: %w", context.Canceled), false},
		{"access denied", objectfserrors.NewError(objectfserrors.ErrCodeAccessDenied, "denied"), true},
		{"connection failed", objectfserrors.NewError(objectfserrors.ErrCodeConnectionFailed, "refused"), true},
		{"uncoded error counts as a failure", errors.New("something went wrong"), true},
	}
	for _, tc := range tests {
		if got := isSiteFailure(tc.err); got != tc.want {
			t.Errorf("isSiteFailure(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestRecordSiteResult_NotFoundRecordsSuccess verifies that a non-failure error
// records a success rather than merely skipping the failure.  The site was
// reached and it answered, which is what the breaker measures — and it means a
// site recovering via cache misses closes its circuit.
func TestRecordSiteResult_NotFoundRecordsSuccess(t *testing.T) {
	t.Parallel()

	cb := circuitbreaker.New(3, time.Hour)
	cb.RecordFailure("s")
	cb.RecordFailure("s")

	recordSiteResult(cb, "s", notFound("k"))

	// The failure counter must have been reset: one more genuine failure should
	// not be enough to open a threshold-3 circuit.
	recordSiteResult(cb, "s", errors.New("boom"))
	if got := cb.State("s"); got != circuitbreaker.StateClosed {
		t.Errorf("state: got %v, want closed (not-found should have reset the counter)", got)
	}
}

// TestRecordSiteResult_NilBreakerIsNoop guards the nil-breaker path.
func TestRecordSiteResult_NilBreakerIsNoop(t *testing.T) {
	t.Parallel()
	recordSiteResult(nil, "s", errors.New("boom")) // must not panic
	recordSiteResult(nil, "s", nil)
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

// ── Cache tests ───────────────────────────────────────────────────────────────

// TestCoordinator_Cache_NilIsNoop verifies that a coordinator without a cache
// configured still processes Get/Put/Delete correctly.
func TestCoordinator_Cache_NilIsNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cli := newMemClient(map[string][]byte{"k": []byte("v")})
	c := New(site.New("s1", types.SiteRolePrimary, cli))
	// No SetCache call — nil cache.
	data, err := c.Get(ctx, "k")
	if err != nil || string(data) != "v" {
		t.Fatalf("Get without cache: err=%v data=%q", err, data)
	}
}

// TestCoordinator_Cache_GetHitServesFromCache verifies that a second Get for
// the same key is served from the cache without calling the site.
func TestCoordinator_Cache_GetHitServesFromCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cli := newMemClient(map[string][]byte{"k": []byte("cached-value")})

	c := New(site.New("s1", types.SiteRolePrimary, cli))
	c.SetCache(cache.New(cache.Config{MaxBytes: 1024}))

	// First call — cache miss → fetches from site.
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Poison the site so any further site access would fail.
	cli.getErr = errors.New("site should not be called on cache hit")

	// Second call — must be served from cache.
	data, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("second Get (should hit cache): %v", err)
	}
	if string(data) != "cached-value" {
		t.Errorf("got %q, want %q", data, "cached-value")
	}
}

// TestCoordinator_Cache_PutInvalidatesKey verifies that Put evicts the cached
// value so the next Get re-fetches from the site.
func TestCoordinator_Cache_PutInvalidatesKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cli := newMemClient(map[string][]byte{"k": []byte("old")})

	c := New(site.New("s1", types.SiteRolePrimary, cli))
	c.SetCache(cache.New(cache.Config{MaxBytes: 1024}))

	// Populate the cache.
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("initial Get: %v", err)
	}

	// Write new data via Put — should invalidate the cached entry.
	if err := c.Put(ctx, "k", []byte("new")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Next Get should fetch from the (now updated) site, not the stale cache.
	data, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if string(data) != "new" {
		t.Errorf("got %q, want %q", data, "new")
	}
}

// TestCoordinator_Cache_DeleteInvalidatesKey verifies that Delete removes the
// cached value so a subsequent Get no longer returns stale data.
func TestCoordinator_Cache_DeleteInvalidatesKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cli := newMemClient(map[string][]byte{"k": []byte("v")})

	c := New(site.New("s1", types.SiteRolePrimary, cli))
	c.SetCache(cache.New(cache.Config{MaxBytes: 1024}))

	// Populate the cache.
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("initial Get: %v", err)
	}

	// Delete from coordinator — should invalidate cache.
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Poison the site error so if cache is consulted and returns a hit we'd
	// get stale data; the site returning an error confirms the cache is clear.
	cli.getErr = errors.New("object deleted")
	_, err := c.Get(ctx, "k")
	if err == nil {
		t.Error("expected error on Get after Delete (cache should be invalidated)")
	}
}

// TestCoordinator_Cache_SetCacheNilDisables verifies that passing nil to
// SetCache disables caching.
func TestCoordinator_Cache_SetCacheNilDisables(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cli := newMemClient(map[string][]byte{"k": []byte("v")})

	c := New(site.New("s1", types.SiteRolePrimary, cli))
	c.SetCache(cache.New(cache.Config{MaxBytes: 1024}))

	// Populate cache.
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Disable cache.
	c.SetCache(nil)

	// Poison the site — if cache was still active, this would be a hit.
	cli.getErr = errors.New("site error after cache disabled")

	_, err := c.Get(ctx, "k")
	if err == nil {
		t.Error("expected site error after cache disabled, got nil")
	}
}

// ── SiteInfos / circuit state tests ──────────────────────────────────────────

// TestSiteInfos_NoCircuitBreaker verifies that CircuitState is empty when no
// circuit breaker is registered.
func TestSiteInfos_NoCircuitBreaker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(m)

	infos := c.SiteInfos(ctx)
	if len(infos) != 1 {
		t.Fatalf("expected 1 site info, got %d", len(infos))
	}
	if infos[0].CircuitState != "" {
		t.Errorf("CircuitState should be empty without CB, got %q", infos[0].CircuitState)
	}
}

// TestSiteInfos_WithCircuitBreaker_Closed verifies that CircuitState is
// "closed" for an untripped site.
func TestSiteInfos_WithCircuitBreaker_Closed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(m)
	c.SetCircuitBreaker(circuitbreaker.New(5, 30*time.Second))

	infos := c.SiteInfos(ctx)
	if len(infos) != 1 {
		t.Fatalf("expected 1 site info, got %d", len(infos))
	}
	if infos[0].CircuitState != "closed" {
		t.Errorf("CircuitState: got %q, want %q", infos[0].CircuitState, "closed")
	}
}

// TestSiteInfos_WithCircuitBreaker_Open verifies that CircuitState is "open"
// after the threshold of consecutive failures.
func TestSiteInfos_WithCircuitBreaker_Open(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(m)
	cb := circuitbreaker.New(2, 30*time.Second)
	c.SetCircuitBreaker(cb)

	cb.RecordFailure("primary")
	cb.RecordFailure("primary")

	infos := c.SiteInfos(ctx)
	if len(infos) != 1 {
		t.Fatalf("expected 1 site info, got %d", len(infos))
	}
	if infos[0].CircuitState != "open" {
		t.Errorf("CircuitState: got %q, want %q", infos[0].CircuitState, "open")
	}
}

// TestSiteInfos_MultipleSites verifies that each site gets its own circuit
// state when multiple sites are registered.
func TestSiteInfos_MultipleSites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mA, _ := makeMount("site-a", types.SiteRolePrimary, nil)
	mB, _ := makeMount("site-b", types.SiteRoleBurst, nil)
	c := New(mA, mB)

	cb := circuitbreaker.New(1, 30*time.Second)
	c.SetCircuitBreaker(cb)

	// Trip only site-b.
	cb.RecordFailure("site-b")

	infos := c.SiteInfos(ctx)
	if len(infos) != 2 {
		t.Fatalf("expected 2 site infos, got %d", len(infos))
	}

	stateByName := make(map[string]string, 2)
	for _, info := range infos {
		stateByName[info.Name] = info.CircuitState
	}

	if stateByName["site-a"] != "closed" {
		t.Errorf("site-a: got %q, want %q", stateByName["site-a"], "closed")
	}
	if stateByName["site-b"] != "open" {
		t.Errorf("site-b: got %q, want %q", stateByName["site-b"], "open")
	}
}

// ─── Lifecycle tests (#82, #83, #84, #85, #86, #95) ───────────────────────────
//
// This block exists because the suite had a hole exactly the shape of five bugs:
// nothing called Stop against a busy coordinator, and nothing called any Set*
// after Start.  Every test here does one of those two things.

// coordinatorGoroutines counts live goroutines whose stack mentions one of the
// coordinator's background loops.
//
// Counting *all* goroutines would be useless here: the package's tests are
// parallel, so the total is dominated by unrelated work and by the runtime's own
// goroutines.  Matching frame names is what makes the count attributable, and it
// is why runHealthPollLoop and drainWorkerEvents are named methods rather than
// closures.
func coordinatorGoroutines() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, frame := range []string{
		"coordinator.(*Coordinator).runHealthPollLoop",
		"coordinator.(*Coordinator).drainWorkerEvents",
	} {
		n += strings.Count(string(buf), frame)
	}
	return n
}

// waitForGoroutines waits for the coordinator background goroutine count to fall
// to want, and reports the last observation if it never does.  Polling rather than
// sleeping is what makes this an assertion on the count instead of on a duration.
func waitForGoroutines(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		got = coordinatorGoroutines()
		if got <= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("coordinator background goroutines: got %d, want <= %d after %v — they were leaked",
		got, want, timeout)
}

// TestCoordinator_Lifecycle_ConcurrentStartStop_DoesNotLeak is #82.
//
// Start could not be ordered against Stop: Start released c.mu across the lease
// acquisition and recoverPendingJobs, and a Stop that landed in that window read
// storeCancel==nil and a zero storeWg, concluded there was nothing to tear down,
// and returned — after which Start launched a drain goroutine and a health poller
// under a context nothing would ever cancel.  Both leaked for the process
// lifetime, the poller waking every interval to probe sites forever.
//
// The assertion is on the goroutine count, not on elapsed time: a leak is a
// goroutine that is still there, and no sleep can prove its absence.
func TestCoordinator_Lifecycle_ConcurrentStartStop_DoesNotLeak(t *testing.T) {
	// Not parallel: it reads the process-wide goroutine dump, and a parallel
	// sibling starting a coordinator would be counted as this test's leak.
	baseline := coordinatorGoroutines()

	const iterations = 60
	for i := 0; i < iterations; i++ {
		primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
		backup, _ := makeMount("backup", types.SiteRoleBackup, nil)
		c := New(primary, backup)
		// A short interval makes a leaked poller cheap to detect and expensive to
		// ignore — it keeps probing after the coordinator is gone.
		mustConfigure(t, c.SetHealthPollInterval(5*time.Millisecond))
		mustConfigure(t, c.SetStore(metadata.NewMemoryStore()))

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			// Either outcome is contractual: Start won the race (nil) or Stop did
			// (ErrStopped).  What is not contractual is Start launching goroutines
			// that Stop has already decided not to wait for.
			if err := c.Start(ctx); err != nil && !errors.Is(err, ErrStopped) {
				t.Errorf("iteration %d: Start: unexpected error %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			c.Stop()
		}()
		wg.Wait()

		// The context is cancelled only *after* both calls return.  Cancelling
		// earlier would let ctx.Done tear down a leaked goroutine and hide the bug —
		// which is precisely what the daemon's `cancel(); c.Close()` does, and why
		// this leak survived in production without being noticed.
		cancel()
	}

	waitForGoroutines(t, baseline, 5*time.Second)
}

// TestCoordinator_Lifecycle_StopThenStart_Refuses is #84.
//
// Stop before Start burned the worker's start Once, so a subsequent Start brought
// up the drain goroutine and the health poller while silently leaving the worker
// dead.  The coordinator then looked healthy, Put returned nil, and nothing was
// ever replicated for the rest of the process's life.  It is now an error.
func TestCoordinator_Lifecycle_StopThenStart_Refuses(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)
	c := New(primary, backup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Stop() // e.g. a deferred Stop on a boot path that failed later

	err := c.Start(ctx)
	if err == nil {
		t.Fatal("Start after Stop returned nil; the caller has no way to learn that " +
			"replication is dead, which is the whole of #84")
	}
	if !errors.Is(err, ErrStopped) {
		t.Errorf("Start after Stop: got %v, want an error wrapping ErrStopped", err)
	}

	// And the claim is accurate: the coordinator really is not replicating.  A Put
	// still writes to the primary synchronously, so this asserts on the backup.
	if err := c.Put(ctx, "k", []byte("v")); err != nil &&
		!errors.Is(err, ErrReplicationNotQueued) {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if backupClient.hasKey("k") {
		t.Error("backup received the object from a coordinator that refused to start")
	}
}

// TestCoordinator_Lifecycle_StartIsIdempotent keeps the other half of the #84
// contract honest: a second Start on a *running* coordinator must succeed and must
// not launch a second set of background goroutines.
func TestCoordinator_Lifecycle_StartIsIdempotent(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mustStart(t, ctx, c)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("second Start on a running coordinator: got %v, want nil", err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatalf("third Start on a running coordinator: got %v, want nil", err)
	}

	// A double Add on drainWg would make this Stop wait for a Done that never
	// comes, and a double-launched drain would race the final flush.  Stop
	// returning nil within its budget is the assertion.
	if err := c.StopContext(context.Background()); err != nil {
		t.Errorf("Stop after three Starts: %v — Start launched goroutines it did not "+
			"account for", err)
	}
}

// TestCoordinator_Lifecycle_StopBeforeStart_IsTerminalNotFatal records the
// deliberate half of the #84 decision.  Stop-before-Start stays legal — a boot
// path that defers Stop and then fails must not panic — but it is terminal, and
// the terminality is what the previous test asserts.  Both halves are the contract.
func TestCoordinator_Lifecycle_StopBeforeStart_IsTerminalNotFatal(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	done := make(chan struct{})
	go func() {
		c.Stop()
		c.Stop() // idempotent
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop before Start did not return within 2s")
	}
}

// TestCoordinator_SetWorkerQueueDepth_AfterStart_Refuses is #85.
//
// The setter unconditionally replaced c.worker.  Called on a running coordinator
// it therefore orphaned the goroutine draining the live queue and installed a
// fresh worker that nobody would ever Start: every later Enqueue filled a queue
// with no consumer, Put kept returning nil, and replication was over.  The
// coordinator's own ReplicationQueueDepth then reported the *new* worker's depth,
// so the queue looked empty while jobs piled up in a channel nobody held.
func TestCoordinator_SetWorkerQueueDepth_AfterStart_Refuses(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)
	c := New(primary, backup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
	defer c.Stop()

	// Prove replication works before the bad call, so a failure afterwards is
	// attributable to the setter and not to a coordinator that never worked.
	if err := c.Put(ctx, "before", []byte("v1")); err != nil {
		t.Fatalf("Put(before): %v", err)
	}
	waitForKey(t, backupClient, "before", 2*time.Second)

	err := c.SetWorkerQueueDepth(64)
	if err == nil {
		t.Fatal("SetWorkerQueueDepth after Start returned nil; it used to replace the " +
			"running worker and end replication for the process lifetime (#85)")
	}
	if !errors.Is(err, ErrStarted) {
		t.Errorf("SetWorkerQueueDepth after Start: got %v, want an error wrapping ErrStarted", err)
	}

	// The rejection has to be a no-op, not a partial mutation: replication must
	// still work.  This is the assertion the old code failed.
	if err := c.Put(ctx, "after", []byte("v2")); err != nil {
		t.Fatalf("Put(after): %v", err)
	}
	waitForKey(t, backupClient, "after", 2*time.Second)
}

// TestCoordinator_GatedSetters_AfterStart_AllRefuse covers the rest of the frozen
// configuration set in one place, so a newly added setter that forgets the gate is
// noticed here rather than in production.
func TestCoordinator_GatedSetters_AfterStart_AllRefuse(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
	defer c.Stop()

	calls := map[string]func() error{
		"SetStore":              func() error { return c.SetStore(metadata.NewMemoryStore()) },
		"SetLeaseManager":       func() error { return c.SetLeaseManager(lease.NewMemoryManager("x")) },
		"SetMetrics":            func() error { return c.SetMetrics(metrics.New(prometheus.NewRegistry())) },
		"SetHealthPollInterval": func() error { return c.SetHealthPollInterval(time.Second) },
		"SetLeaseTTL":           func() error { return c.SetLeaseTTL(time.Second) },
		"SetWorkerQueueDepth":   func() error { return c.SetWorkerQueueDepth(8) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatalf("%s after Start: got nil, want an error wrapping ErrStarted", name)
			}
			if !errors.Is(err, ErrStarted) {
				t.Errorf("%s after Start: got %v, want an error wrapping ErrStarted", name, err)
			}
		})
	}
}

// TestCoordinator_DynamicSetters_AfterStart_StillApply is the negative control for
// the gate.  These five knobs are genuinely dynamic — they are read per operation,
// not copied into a goroutine at Start — so gating them would be a regression, not
// a safety improvement.  The distinction is the whole design of the contract, so it
// is asserted rather than left to the doc comment.
func TestCoordinator_DynamicSetters_AfterStart_StillApply(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	c := New(primary)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustStart(t, ctx, c)
	defer c.Stop()

	c.SetPolicy(policy.New())
	c.SetCache(cache.New(cache.Config{MaxBytes: 1024}))
	c.SetCircuitBreaker(circuitbreaker.New(3, time.Hour))
	c.SetRetryConfig(&retry.Config{MaxAttempts: 2, InitialDelay: time.Millisecond, Multiplier: 1.0})
	c.SetEnqueueBackpressure(10 * time.Millisecond)

	// The cache is the one whose effect is directly observable: a Get populates
	// it, so a second Get is served without touching the site.
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("cached Get: %v", err)
	}
}

// TestCoordinator_SetMetrics_ConcurrentWithPut is #86.
//
// SetMetrics writes c.m under c.mu while Put, Get and the event drain read the
// field with no lock at all.  It is a data race on an 8-byte pointer, which looks
// harmless and is not: the reader can observe the write out of order, so the
// `if c.m != nil` guard the old helpers relied on could pass while the value is
// still the zero it was reading a moment ago.  -race is the assertion; the test
// only has to create the overlap.
//
// The setter now refuses after Start, so the surviving window is the one that
// remains legal: a caller configuring a *created* coordinator while another
// goroutine uses it.  That is a real pattern (config assembly racing the first
// request) and it is what this exercises.
func TestCoordinator_SetMetrics_ConcurrentWithPut(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, map[string][]byte{"k": []byte("v")})
	backup, _ := makeMount("backup", types.SiteRoleBackup, nil)
	c := New(primary, backup)
	c.SetCache(cache.New(cache.Config{MaxBytes: 4096})) // makes Get touch the cache metrics
	// No worker is running here — the coordinator is deliberately left in the
	// created state, which is the window SetMetrics is still allowed in — so the
	// replication queue fills and never drains.  A negative budget makes Put try
	// once and report ErrReplicationNotQueued instead of waiting out the
	// backpressure timer on every call, which would turn this into a 5-minute test.
	c.SetEnqueueBackpressure(-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// Each call gets its own registry: re-registering the same collectors
			// would panic, which would mask the race this test is looking for.
			if err := c.SetMetrics(metrics.New(prometheus.NewRegistry())); err != nil {
				return // gated; nothing more to do
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// Errors are expected and irrelevant: the point is that Put reads c.m.
			_ = c.Put(ctx, fmt.Sprintf("key-%d", i), []byte("data"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = c.Get(ctx, "k") // reads c.m via the cache hit/miss counters
		}
	}()
	go func() {
		defer wg.Done()
		// AddSite/RemoveSite reach c.m through metricsSiteCountLocked, which is the
		// one read that happens with the write lock already held.
		for i := 0; i < iterations; i++ {
			name := fmt.Sprintf("extra-%d", i)
			c.AddSite(makeMountOnly(name))
			c.RemoveSite(name)
		}
	}()

	wg.Wait()
}

// TestCoordinator_ReplicationQueueDepth_ConcurrentWithSetWorkerQueueDepth is the
// second half of #85: the unlocked read of the c.worker pointer.  Both of these
// are exported, so this races two documented API calls against each other with
// nothing exotic in between — and it failed under -race before workerRef existed.
func TestCoordinator_ReplicationQueueDepth_ConcurrentWithSetWorkerQueueDepth(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	c := New(primary)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= 200; i++ {
			_ = c.SetWorkerQueueDepth(i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.ReplicationQueueDepth()
		}
	}()
	wg.Wait()
}

// TestCoordinator_StopContext_BoundedWhenTransferWedged is #83 on the worker path.
//
// Stop waited on the replication worker without any bound, and the worker waits
// for the in-flight transfer.  A destination site that never answers therefore held
// SIGTERM open indefinitely: the daemon's shutdown path is `cancel(); c.Close()`,
// and Close's wait is not on the ctx that was cancelled.  Operators saw the process
// survive its grace period and get SIGKILLed, losing the in-memory queue.
func TestCoordinator_StopContext_BoundedWhenTransferWedged(t *testing.T) {
	t.Parallel()

	primary, _ := makeMount("primary", types.SiteRolePrimary, nil)
	backup, backupClient := makeMount("backup", types.SiteRoleBackup, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := backupClient.blockPuts()
	// Released only on the way out.  Releasing before StopContext would turn this
	// into the settle-normally test; releasing in a defer that runs *after* the
	// measurement is what leaves the transfer genuinely wedged, which is the shape
	// #83 warns about — get it wrong and the test hangs instead of failing.
	defer release()

	c := New(primary, backup)
	mustConfigure(t, c.SetStore(metadata.NewMemoryStore()))
	mustStart(t, ctx, c)

	if err := c.Put(ctx, "wedged.bam", []byte("genome")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	backupClient.waitForPut(t, 1, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopCancel()

	start := time.Now()
	err := c.StopContext(stopCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("StopContext returned nil while a transfer was wedged in the destination's Put")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("StopContext error: got %v, want a wrapped context.DeadlineExceeded", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("StopContext took %v with a 200ms budget — shutdown is not bounded, "+
			"which is exactly the SIGTERM hang in #83", elapsed)
	}
}

// TestCoordinator_Stop_CompletesWhenHealthProbeWedged is the other #83 path, and
// the one that matters most in production because it needs no traffic at all.
//
// The health poller calls Health, which used to wg.Wait() for every probe.  objectfs
// probes are not context-aware — ClientManager.HealthCheck takes a pooled client via
// ConnectionPool.Get, which has a hard-coded 30 s timeout and no ctx parameter — so a
// saturated pool pinned Health open, pinned the poller open, and pinned Stop open,
// with an idle coordinator and no replication in flight.
//
// The assertion is that Stop *succeeds* quickly, not that it times out.  Both layers
// of the fix are visible in that: Health returns its partial report as soon as its
// context is cancelled, so cancelling the drain context is enough to retire the
// poller, and StopContext's own budget is never reached.  A wedged probe now costs
// one abandoned goroutine in a terminating process instead of the whole shutdown.
// Bounding Stop alone would have got a *timeout* here, and a non-zero exit code on
// every restart of a cluster with one slow endpoint.
func TestCoordinator_Stop_CompletesWhenHealthProbeWedged(t *testing.T) {
	t.Parallel()

	primary, primaryClient := makeMount("primary", types.SiteRolePrimary, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := primaryClient.blockHealth()
	// Released only on the way out, so the probe is genuinely wedged for the whole
	// of the measurement below.  Releasing it first is the mistake that turns this
	// into a test that hangs rather than one that fails.
	defer release()

	c := New(primary)
	mustConfigure(t, c.SetHealthPollInterval(10*time.Millisecond))
	mustStart(t, ctx, c)

	// The first poll happens immediately at Start, so this is the probe that wedges.
	primaryClient.waitForHealth(t, 1, 2*time.Second)

	// Stop runs on its own goroutine: on the pre-fix tree it never returns at all,
	// and a test that fails is worth more than a test that hangs.
	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		err := c.StopContext(context.Background())
		done <- result{err, time.Since(start)}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Errorf("StopContext: %v — a wedged probe should be abandoned, not reported as "+
				"a failed shutdown", r.err)
		}
		// defaultStopTimeout is 30 s, so anything near it means the bound fired
		// rather than the poller retiring on its own.
		if r.elapsed > 3*time.Second {
			t.Errorf("StopContext took %v — the poller is not retiring on context "+
				"cancellation, it is being timed out", r.elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopContext did not return within 10s while a health probe was wedged — " +
			"this is the SIGTERM hang in #83")
	}
}

// TestCoordinator_Health_ReportsTimedOutSitesAsUnknown pins the shape of the
// partial report.  Health returns on its deadline, and every site that has not
// answered is present in the map with ErrHealthTimeout — present, not omitted,
// because SiteInfos and preferHealthySites both index the report by site name and
// read a missing key as "healthy".
func TestCoordinator_Health_ReportsTimedOutSitesAsUnknown(t *testing.T) {
	t.Parallel()

	fast, _ := makeMount("fast", types.SiteRolePrimary, nil)
	slow, slowClient := makeMount("slow", types.SiteRoleBackup, nil)

	release := slowClient.blockHealth()
	defer release()

	c := New(fast, slow)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	report := c.Health(ctx)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("Health took %v with a 150ms deadline — the deadline is a request, not a guarantee", elapsed)
	}
	if len(report) != 2 {
		t.Fatalf("report has %d entries, want 2 (one per site); a missing key reads as healthy "+
			"to preferHealthySites: %v", len(report), report)
	}
	if err := report["slow"]; !errors.Is(err, ErrHealthTimeout) {
		t.Errorf("report[slow]: got %v, want an error wrapping ErrHealthTimeout", err)
	}
	if err := report["fast"]; err != nil {
		t.Errorf("report[fast]: got %v, want nil — a fast site must not be tarred with the slow one", err)
	}
}

// waitForKey polls a client for a key, failing the test if it never arrives.
func waitForKey(t *testing.T, mc *memClient, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mc.hasKey(key) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("key %q never replicated within %v", key, timeout)
}

// makeMountOnly is makeMount for tests that do not need the client handle.
func makeMountOnly(name string) *site.SiteMount {
	m, _ := makeMount(name, types.SiteRoleBackup, nil)
	return m
}
