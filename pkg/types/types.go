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
type ReplicationPolicy struct {
	// Name is the policy identifier.
	Name string `json:"name"`

	// PathPattern is a glob pattern matching file paths.
	PathPattern string `json:"path_pattern"`

	// Primary is the primary site name for files matching this policy.
	Primary string `json:"primary"`

	// ReplicateTo lists sites to replicate to.
	ReplicateTo []string `json:"replicate_to"`

	// Priority affects scheduling (higher = higher priority).
	Priority int `json:"priority"`
}

// CoordinatorConfig contains coordinator configuration.
type CoordinatorConfig struct {
	// ListenAddr is the address the coordinator listens on.
	ListenAddr string `json:"listen_addr"`

	// EtcdEndpoints are the etcd cluster endpoints.
	EtcdEndpoints []string `json:"etcd_endpoints"`

	// LeaseTimeout is the TTL used when acquiring the distributed leader lease.
	// The coordinator daemon reads this and passes it to SetLeaseTTL at startup.
	LeaseTimeout time.Duration `json:"lease_timeout"`
}

// PerformanceConfig contains performance tuning settings.
type PerformanceConfig struct {
	// MaxConcurrentTransfers sets the replication worker queue depth.
	// The coordinator daemon reads this and passes it to SetWorkerQueueDepth at startup.
	MaxConcurrentTransfers int `json:"max_concurrent_transfers"`
}
