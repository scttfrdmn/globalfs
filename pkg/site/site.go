// Package site provides the SiteMount type, which wraps an ObjectFS client
// to represent a single storage site in the GlobalFS namespace.
package site

import (
	"context"
	"fmt"

	objectfssdk "github.com/objectfs/objectfs/sdks/go/objectfs"
	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ObjectFSClient is the subset of *objectfssdk.Client used by SiteMount.
// Expressed as an interface so that tests can supply a lightweight mock
// instead of a real S3-backed client.
type ObjectFSClient interface {
	List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error)
	Head(ctx context.Context, key string) (*objectfstypes.ObjectInfo, error)
	Health(ctx context.Context) error
	Close() error
}

// Compile-time assertion: *objectfssdk.Client satisfies ObjectFSClient.
var _ ObjectFSClient = (*objectfssdk.Client)(nil)

// SiteMount represents an ObjectFS-backed storage site in the GlobalFS
// namespace.  It wraps an ObjectFSClient and exposes the operations
// GlobalFS needs: list objects, query metadata, and check connectivity.
type SiteMount struct {
	name   string
	role   types.SiteRole
	client ObjectFSClient
}

// New creates a SiteMount backed by the given client.
// client must not be nil.
func New(name string, role types.SiteRole, client ObjectFSClient) *SiteMount {
	return &SiteMount{
		name:   name,
		role:   role,
		client: client,
	}
}

// NewFromConfig creates a SiteMount by constructing a real objectfs.Client
// from the site configuration.  The caller must call Close when done.
//
// This performs a lightweight S3 health check on construction; it returns an
// error if credentials or connectivity are unavailable.
func NewFromConfig(ctx context.Context, cfg *config.SiteConfig) (*SiteMount, error) {
	opts := []objectfssdk.Option{
		objectfssdk.WithRegion(cfg.ObjectFS.S3Region),
	}
	if cfg.ObjectFS.S3Endpoint != "" {
		opts = append(opts, objectfssdk.WithEndpoint(cfg.ObjectFS.S3Endpoint))
	}

	client, err := objectfssdk.New(ctx, cfg.ObjectFS.S3Bucket, opts...)
	if err != nil {
		return nil, fmt.Errorf("site %s: failed to create objectfs client: %w", cfg.Name, err)
	}

	return New(cfg.Name, cfg.Role, client), nil
}

// Name returns the site's unique identifier.
func (m *SiteMount) Name() string { return m.name }

// Role returns the site's role (primary, burst, backup).
func (m *SiteMount) Role() types.SiteRole { return m.role }

// List returns up to limit ObjectInfo entries whose keys begin with prefix
// from this site's object store.  Pass limit ≤ 0 to retrieve all matches.
func (m *SiteMount) List(ctx context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	return m.client.List(ctx, prefix, limit)
}

// Head returns metadata for the object at key without fetching its content.
func (m *SiteMount) Head(ctx context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	return m.client.Head(ctx, key)
}

// Health checks connectivity to this site's ObjectFS instance.
func (m *SiteMount) Health(ctx context.Context) error {
	return m.client.Health(ctx)
}

// Close releases all resources held by this SiteMount.
func (m *SiteMount) Close() error {
	return m.client.Close()
}
