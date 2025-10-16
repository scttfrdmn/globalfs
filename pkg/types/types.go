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

// SyncMode defines how data is replicated between sites.
type SyncMode string

const (
	// SyncEager replicates data eagerly before it's needed (pre-staging).
	SyncEager SyncMode = "eager"

	// SyncLazy fetches data on first access (lazy loading).
	SyncLazy SyncMode = "lazy"

	// SyncAsync replicates data in the background asynchronously.
	SyncAsync SyncMode = "async"

	// SyncNever never replicates data (site-local only).
	SyncNever SyncMode = "never"
)

// LeaseType defines the type of file access lease.
type LeaseType string

const (
	// LeaseRead allows read-only access to a file.
	LeaseRead LeaseType = "read"

	// LeaseWrite allows exclusive write access to a file.
	LeaseWrite LeaseType = "write"
)

// SiteInfo contains information about a GlobalFS site.
type SiteInfo struct {
	// Name is the unique identifier for this site.
	Name string `json:"name"`

	// Role defines the site's role in the cluster.
	Role SiteRole `json:"role"`

	// Status is the current operational status.
	Status SiteStatus `json:"status"`

	// ObjectFSEndpoint is the path to the local ObjectFS mount.
	ObjectFSEndpoint string `json:"objectfs_endpoint"`

	// S3Backend is the S3 storage backing this site.
	S3Backend string `json:"s3_backend"`

	// CargoShipEndpoint is the endpoint for bulk data transfers.
	CargoShipEndpoint string `json:"cargoship_endpoint"`

	// Health contains current health metrics.
	Health *HealthMetrics `json:"health,omitempty"`

	// LastSeen is the last time the coordinator heard from this site.
	LastSeen time.Time `json:"last_seen"`
}

// HealthMetrics contains site health information.
type HealthMetrics struct {
	// CPUUsage is the current CPU utilization percentage.
	CPUUsage float64 `json:"cpu_usage"`

	// MemoryUsage is the current memory utilization percentage.
	MemoryUsage float64 `json:"memory_usage"`

	// DiskAvailable is the available disk space in bytes.
	DiskAvailable uint64 `json:"disk_available"`

	// NetworkLatency is the average network latency in milliseconds.
	NetworkLatency float64 `json:"network_latency"`
}

// FileMetadata contains metadata about a file in the global namespace.
type FileMetadata struct {
	// Path is the global filesystem path.
	Path string `json:"path"`

	// Primary is the name of the primary site for this file.
	Primary string `json:"primary"`

	// Replicas lists sites that have replicas of this file.
	Replicas []string `json:"replicas"`

	// Size is the file size in bytes.
	Size int64 `json:"size"`

	// Checksum is the SHA256 checksum of the file.
	Checksum string `json:"checksum"`

	// LastModified is the last modification time.
	LastModified time.Time `json:"last_modified"`

	// PendingSync indicates if replication is pending.
	PendingSync bool `json:"pending_sync"`
}

// Lease represents a file access lease.
type Lease struct {
	// Path is the file path this lease covers.
	Path string `json:"path"`

	// SiteID is the site holding the lease.
	SiteID string `json:"site_id"`

	// Type is the lease type (read or write).
	Type LeaseType `json:"type"`

	// ExpiresAt is when this lease expires.
	ExpiresAt time.Time `json:"expires_at"`

	// Version is used for cache validation.
	Version uint64 `json:"version"`
}

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

	// SyncMode defines when replication occurs.
	SyncMode SyncMode `json:"sync_mode"`

	// Priority affects scheduling (higher = higher priority).
	Priority int `json:"priority"`
}

// CoordinatorConfig contains coordinator configuration.
type CoordinatorConfig struct {
	// ListenAddr is the address the coordinator listens on.
	ListenAddr string `json:"listen_addr"`

	// EtcdEndpoints are the etcd cluster endpoints.
	EtcdEndpoints []string `json:"etcd_endpoints"`

	// LeaseTimeout is the default lease timeout duration.
	LeaseTimeout time.Duration `json:"lease_timeout"`

	// HealthCheckInterval is how often to check site health.
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// NetworkConfig contains network-related configuration.
type NetworkConfig struct {
	// Bandwidth is the available bandwidth in bytes/sec.
	Bandwidth int64 `json:"bandwidth"`

	// Latency is the average network latency to other sites.
	Latency time.Duration `json:"latency"`
}

// PerformanceConfig contains performance tuning settings.
type PerformanceConfig struct {
	// MaxConcurrentTransfers limits concurrent replication jobs.
	MaxConcurrentTransfers int `json:"max_concurrent_transfers"`

	// TransferChunkSize is the size of transfer chunks in bytes.
	TransferChunkSize int64 `json:"transfer_chunk_size"`

	// CacheSize is the size of the local metadata cache.
	CacheSize string `json:"cache_size"`
}
