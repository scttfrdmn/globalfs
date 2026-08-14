package types

import (
	"time"
)

// SiteRole defines the role of a site in the GlobalFS cluster.
type SiteRole string

const (
	// SiteRolePrimary indicates the primary on-premises site.
	SiteRolePrimary SiteRole = "primary"

	// SiteRoleBurst indicates a cloud burst site for overflow compute.
	SiteRoleBurst SiteRole = "burst"

	// SiteRoleBackup indicates a backup/DR site.
	SiteRoleBackup SiteRole = "backup"
)

// SiteStatus defines the operational status of a site.
type SiteStatus string

const (
	// SiteStatusActive indicates the site is fully operational.
	SiteStatusActive SiteStatus = "active"

	// SiteStatusDegraded indicates the site is operational but with reduced performance.
	SiteStatusDegraded SiteStatus = "degraded"

	// SiteStatusUnavailable indicates the site is not accessible.
	SiteStatusUnavailable SiteStatus = "unavailable"
)

// ReplicationJob represents a data replication job.
type ReplicationJob struct {
	// ID is the unique job identifier.
	ID string `json:"id"`

	// Source is the source site name.
	Source string `json:"source"`

	// Destination is the destination site name.
	Destination string `json:"destination"`

	// Files is the list of files to replicate.
	Files []string `json:"files"`

	// Status is the current job status.
	Status ReplicationStatus `json:"status"`

	// Progress is the completion percentage (0-100).
	Progress int `json:"progress"`

	// BytesTransferred is the number of bytes transferred so far.
	BytesTransferred int64 `json:"bytes_transferred"`

	// TotalBytes is the total number of bytes to transfer.
	TotalBytes int64 `json:"total_bytes"`

	// StartTime is when the job started.
	StartTime time.Time `json:"start_time"`

	// CompletionTime is when the job completed (if finished).
	CompletionTime *time.Time `json:"completion_time,omitempty"`

	// Error contains error information if the job failed.
	Error string `json:"error,omitempty"`
}

// ReplicationStatus defines the status of a replication job.
type ReplicationStatus string

const (
	// ReplicationPending indicates the job is queued but not started.
	ReplicationPending ReplicationStatus = "pending"

	// ReplicationInProgress indicates the job is currently running.
	ReplicationInProgress ReplicationStatus = "in_progress"

	// ReplicationCompleted indicates the job finished successfully.
	ReplicationCompleted ReplicationStatus = "completed"

	// ReplicationFailed indicates the job failed.
	ReplicationFailed ReplicationStatus = "failed"

	// ReplicationCancelled indicates the job was cancelled.
	ReplicationCancelled ReplicationStatus = "cancelled"
)

// ReplicationPolicy defines a data placement policy.
//
// The yaml tags are load-bearing: pkg/config decodes this type straight out of
// the `policies:` block, and yaml.v3 falls back to the lowercased field name
// when a tag is absent — so `path_pattern` bound to nothing and every field
// here silently stayed at its zero value. That is why config.example.yaml
// failed `globalfs config validate` with "policies[0].path_pattern is
// required" while plainly containing one.
type ReplicationPolicy struct {
	// Name is the policy identifier.
	Name string `json:"name" yaml:"name"`

	// PathPattern is a glob pattern matching file paths.
	PathPattern string `json:"path_pattern" yaml:"path_pattern"`

	// Primary is the primary site name for files matching this policy.
	Primary string `json:"primary" yaml:"primary"`

	// ReplicateTo lists sites to replicate to.
	ReplicateTo []string `json:"replicate_to" yaml:"replicate_to"`

	// Priority affects scheduling (higher = higher priority).
	Priority int `json:"priority" yaml:"priority"`
}

// CoordinatorConfig contains coordinator configuration.
//
// See the note on ReplicationPolicy for why the yaml tags matter: without them
// `listen_addr: ":9000"` decoded to the empty string and the daemon bound the
// default port while reporting the operator's config as loaded.
type CoordinatorConfig struct {
	// ListenAddr is the address the coordinator listens on.
	ListenAddr string `json:"listen_addr" yaml:"listen_addr"`

	// EtcdEndpoints are the etcd cluster endpoints.
	EtcdEndpoints []string `json:"etcd_endpoints" yaml:"etcd_endpoints"`

	// LeaseTimeout is the TTL used when acquiring the distributed leader lease.
	// The coordinator daemon reads this and passes it to SetLeaseTTL at startup.
	LeaseTimeout time.Duration `json:"lease_timeout" yaml:"lease_timeout"`
}

// PerformanceConfig contains performance tuning settings.
type PerformanceConfig struct {
	// ReplicationQueueDepth sets the replication worker's queue capacity — the
	// number of jobs that may be waiting before Enqueue starts reporting
	// backpressure.  The coordinator daemon passes it to SetWorkerQueueDepth at
	// startup.
	//
	// This field was called max_concurrent_transfers and documented as "maximum
	// parallel replication jobs", which it never was: the worker's run loop is a
	// single goroutine consuming the queue serially, so raising the value bought
	// buffer, not throughput.  Renamed in #101 rather than made true, because a
	// pool of N consumers can reorder two Puts of the same key — seriality is
	// currently what guarantees per-key ordering, and replacing that guarantee
	// needs a design (per-key affinity, or accepted last-writer-wins) rather than
	// a wider channel.  A real pool may reclaim the old name later.
	ReplicationQueueDepth int `json:"replication_queue_depth" yaml:"replication_queue_depth"`
}
