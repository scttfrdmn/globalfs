package site

import (
	"context"
	"errors"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/types"
)

// mockClient implements ObjectFSClient for tests.
type mockClient struct {
	objects   []objectfstypes.ObjectInfo
	listErr   error
	healthErr error
	closed    bool
}

func (m *mockClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
	for _, o := range m.objects {
		if o.Key == key {
			return make([]byte, o.Size), nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockClient) Put(_ context.Context, _ string, _ []byte) error  { return nil }
func (m *mockClient) Delete(_ context.Context, _ string) error          { return nil }

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

func (m *mockClient) Health(_ context.Context) error { return m.healthErr }
func (m *mockClient) Close() error {
	m.closed = true
	return nil
}

func objectInfo(key string, size int64) objectfstypes.ObjectInfo {
	return objectfstypes.ObjectInfo{Key: key, Size: size, LastModified: time.Now()}
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestSiteMount_NameAndRole(t *testing.T) {
	t.Parallel()
	m := New("onprem", types.SiteRolePrimary, &mockClient{})
	if m.Name() != "onprem" {
		t.Errorf("Name: got %q, want %q", m.Name(), "onprem")
	}
	if m.Role() != types.SiteRolePrimary {
		t.Errorf("Role: got %q, want %q", m.Role(), types.SiteRolePrimary)
	}
}

func TestSiteMount_List_DelegatesToClient(t *testing.T) {
	t.Parallel()

	client := &mockClient{
		objects: []objectfstypes.ObjectInfo{
			objectInfo("data/a.fastq", 100),
			objectInfo("data/b.fastq", 200),
			objectInfo("other/c.txt", 50),
		},
	}
	m := New("site1", types.SiteRolePrimary, client)

	items, err := m.List(context.Background(), "data/", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d: %v", len(items), items)
	}
}

func TestSiteMount_List_PropagatesError(t *testing.T) {
	t.Parallel()
	client := &mockClient{listErr: errors.New("network error")}
	m := New("site1", types.SiteRolePrimary, client)

	if _, err := m.List(context.Background(), "", 0); err == nil {
		t.Error("expected error from client, got nil")
	}
}

func TestSiteMount_Head_DelegatesToClient(t *testing.T) {
	t.Parallel()
	client := &mockClient{
		objects: []objectfstypes.ObjectInfo{objectInfo("genome.bam", 1024)},
	}
	m := New("site1", types.SiteRolePrimary, client)

	info, err := m.Head(context.Background(), "genome.bam")
	if err != nil {
		t.Fatalf("Head: unexpected error: %v", err)
	}
	if info.Key != "genome.bam" {
		t.Errorf("Head Key: got %q, want %q", info.Key, "genome.bam")
	}
	if info.Size != 1024 {
		t.Errorf("Head Size: got %d, want 1024", info.Size)
	}
}

func TestSiteMount_Health_DelegatesToClient(t *testing.T) {
	t.Parallel()

	m := New("ok-site", types.SiteRolePrimary, &mockClient{})
	if err := m.Health(context.Background()); err != nil {
		t.Errorf("Health: unexpected error: %v", err)
	}

	sentinel := errors.New("connection refused")
	m2 := New("bad-site", types.SiteRoleBurst, &mockClient{healthErr: sentinel})
	if err := m2.Health(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("Health: got %v, want %v", err, sentinel)
	}
}

func TestSiteMount_Close_DelegatesToClient(t *testing.T) {
	t.Parallel()
	mc := &mockClient{}
	m := New("site1", types.SiteRolePrimary, mc)
	if err := m.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	if !mc.closed {
		t.Error("expected mockClient.closed to be true after Close")
	}
}
