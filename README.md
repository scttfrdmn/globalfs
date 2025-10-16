# GlobalFS - Global Namespace for Hybrid HPC Clouds

**Status**: Design Phase
**Version**: 0.1.0-alpha
**Target Use Case**: HPC Cloud Bursting with Seamless Data Access

---

## Overview

GlobalFS provides a unified global namespace across on-premises and cloud compute resources, enabling seamless HPC cloud bursting. It coordinates multiple ObjectFS instances and uses CargoShip for efficient data movement.

### Key Capabilities

- **Global Namespace**: Single unified filesystem view across multiple sites
- **Hybrid Cloud**: On-premises primary + cloud burst compute
- **Seamless Access**: Transparent data access from any site
- **Intelligent Replication**: Lazy data movement with predictive pre-staging
- **High Performance**: Leverages ObjectFS local performance + CargoShip bulk transfers

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                GlobalFS Coordinator                      │
│  • Global namespace mapping                              │
│  • Distributed metadata (etcd/Raft)                     │
│  • Site health monitoring                               │
│  • Data placement policy engine                         │
│  • Cache coherency protocol                             │
└──────────┬────────────────────────────┬─────────────────┘
           │                            │
    ┌──────▼──────┐            ┌───────▼────────┐
    │  On-Prem    │            │     Cloud      │
    │  Site       │            │     Site       │
    │             │            │                │
    │  ObjectFS   │            │   ObjectFS     │
    │  Instance   │            │    Instance    │
    └──────┬──────┘            └───────┬────────┘
           │                            │
    ┌──────▼──────┐            ┌───────▼────────┐
    │  Local S3   │◄──────────►│   AWS S3       │
    │  (MinIO)    │  CargoShip │                │
    └─────────────┘  Transfer  └────────────────┘
```

### Component Responsibilities

**GlobalFS Coordinator**
- Manages global namespace → site mappings
- Coordinates metadata across sites
- Implements data placement policies
- Handles cache coherency
- Triggers data replication via CargoShip

**ObjectFS Instances**
- Local high-performance POSIX filesystem
- S3 backend optimization
- Caching and prefetching
- Reports local state to coordinator

**CargoShip**
- Bulk data transfers between sites
- Compression and deduplication
- Bandwidth optimization
- Progress tracking

---

## Use Case: HPC Cloud Bursting

### Scenario

1. **Normal State**: HPC cluster runs on-premises
   - 1000 compute nodes
   - Local high-speed storage (MinIO S3)
   - ObjectFS provides POSIX interface

2. **Burst Event**: Queue depth exceeds capacity
   - Detect: 500+ jobs queued
   - Decision: Burst to AWS
   - Action: Provision 500 cloud nodes

3. **Data Movement**: Intelligent replication
   - Pre-stage "hot" datasets (frequently accessed)
   - Lazy-fetch remaining data on-demand
   - CargoShip handles bulk transfers

4. **Seamless Compute**: Jobs run in cloud
   - Same /globalfs mount point
   - Transparent data access
   - Performance optimized by ObjectFS

5. **Completion**: Results sync back
   - Write results to cloud ObjectFS
   - CargoShip syncs back to on-prem
   - Tear down cloud resources

### Performance Characteristics

| Operation | On-Prem | First Cloud Access | Cached Cloud Access |
|-----------|---------|-------------------|---------------------|
| Read (small file) | <1ms | 50-100ms (fetch) | <1ms |
| Read (large file) | 10ms | 200ms-2s (fetch) | 10-50ms |
| Write | <1ms | <1ms | <1ms |
| Metadata ops | <1ms | 10-20ms | <1ms |

---

## Design Principles

### 1. Separation of Concerns

- **ObjectFS**: Local filesystem performance (single-site)
- **GlobalFS**: Global coordination (multi-site)
- **CargoShip**: Data movement (transfer optimization)

Each component does one thing well and can evolve independently.

### 2. Lazy Replication

Data is **not** eagerly replicated to all sites. Instead:

- **Hot datasets** are pre-staged based on policy
- **On-demand fetch** for first access (one-time cost)
- **Caching** at destination for subsequent access
- **Bandwidth efficiency** over immediate availability

Rationale: HPC datasets are often TB-PB scale. Full replication is wasteful.

### 3. Eventual Consistency

For cloud burst use case:

- **On-prem writes**: Immediately consistent (single site)
- **Cloud reads**: Eventually consistent (acceptable for batch compute)
- **Critical files**: Synchronous replication (configurable)
- **Bulk data**: Asynchronous background replication

### 4. Composability

GlobalFS is a coordination layer that **uses** ObjectFS and CargoShip:

```go
// GlobalFS doesn't reimplement filesystem or transfer logic
type GlobalFS struct {
    sites      map[string]*ObjectFSClient  // Delegate to ObjectFS
    metadata   *DistributedStore           // Coordinate state
    mover      *CargoShipClient            // Delegate transfers
}
```

---

## Technical Architecture

### Metadata Design

**Distributed Metadata Store** (etcd-based)

```
/globalfs/
  metadata/
    sites/
      onprem/
        status: "active"
        role: "primary"
        endpoint: "file:///mnt/objectfs-onprem"
        health: {"cpu": 20, "disk": "50GB free"}
      cloud/
        status: "active"
        role: "burst"
        endpoint: "file:///mnt/objectfs-cloud"
        health: {"cpu": 10, "disk": "100GB free"}

    namespace/
      /datasets/hot/simulation.dat
        primary: "onprem"
        replicas: ["cloud"]
        size: 10737418240
        checksum: "sha256:abc123..."
        last_modified: "2025-10-16T20:00:00Z"

      /results/job-1234/
        primary: "cloud"
        replicas: []
        pending_sync: true

    replication/
      jobs/
        job-5678:
          source: "onprem"
          destination: "cloud"
          files: ["/datasets/hot/simulation.dat"]
          status: "in_progress"
          progress: 45
```

### Data Placement Policies

**Policy Engine** determines where data lives:

```yaml
# globalfs-policy.yaml
policies:
  - name: hot_datasets
    pattern: /datasets/hot/*
    primary: onprem
    replicate_to: [cloud]
    sync_mode: eager  # Pre-stage before burst

  - name: input_data
    pattern: /inputs/**
    primary: onprem
    replicate_to: [cloud]
    sync_mode: lazy   # Fetch on first access

  - name: output_data
    pattern: /results/**
    primary: onprem   # Eventually sync back
    replicate_to: []
    sync_mode: async  # Background sync

  - name: scratch
    pattern: /scratch/**
    primary: null     # Local to each site
    replicate_to: []
    sync_mode: never
```

### Cache Coherency Protocol

**Problem**: Multiple sites may cache the same file

**Solution**: Lease-based invalidation

```
1. Site requests file access
2. Coordinator grants lease (read or write)
3. Other sites' caches invalidated on write lease
4. Lease expires → must revalidate
```

**Protocol:**
```go
type FileLeaseRequest struct {
    Path      string
    SiteID    string
    LeaseType LeaseType  // Read or Write
}

type FileLease struct {
    Path      string
    SiteID    string
    LeaseType LeaseType
    ExpiresAt time.Time
    Version   uint64     // For cache validation
}

// Coordinator tracks leases
type LeaseManager struct {
    mu     sync.RWMutex
    leases map[string][]FileLease  // path -> leases
}

// On write lease grant:
// 1. Invalidate all read leases
// 2. Increment version
// 3. Grant write lease to requester
```

---

## API Design

### GlobalFS Client API

```go
package globalfs

// Client provides global filesystem operations
type Client struct {
    config *Config
    coord  *CoordinatorClient
    local  *ObjectFSClient
}

// Mount a global namespace locally
func (c *Client) Mount(globalPath, localPath string) error

// Open a file (may trigger remote fetch)
func (c *Client) Open(path string) (*File, error)

// Create a file (writes to primary site)
func (c *Client) Create(path string) (*File, error)

// Stat file metadata (from coordinator)
func (c *Client) Stat(path string) (*FileInfo, error)

// ReadDir lists directory (from coordinator)
func (c *Client) ReadDir(path string) ([]FileInfo, error)

// Prefetch triggers eager replication
func (c *Client) Prefetch(paths []string) (*ReplicationJob, error)

// Sync ensures data is replicated to site
func (c *Client) Sync(paths []string, site string) error
```

### Configuration API

```go
// Config defines GlobalFS behavior
type Config struct {
    // Coordinator settings
    Coordinator CoordinatorConfig

    // Site definitions
    Sites []SiteConfig

    // Replication policies
    Policies []ReplicationPolicy

    // Performance tuning
    Performance PerformanceConfig
}

type SiteConfig struct {
    Name       string
    Role       SiteRole  // Primary, Burst, Backup
    ObjectFS   string    // ObjectFS mount endpoint
    S3Backend  string    // Underlying S3 storage
    CargoShip  string    // CargoShip endpoint for transfers
    Network    NetworkConfig
}

type ReplicationPolicy struct {
    Name        string
    PathPattern string  // Glob pattern
    Primary     string  // Primary site name
    ReplicateTo []string
    SyncMode    SyncMode  // Eager, Lazy, Async, Never
    Priority    int       // For scheduling
}

type SyncMode string

const (
    SyncEager SyncMode = "eager"  // Pre-stage before needed
    SyncLazy  SyncMode = "lazy"   // Fetch on first access
    SyncAsync SyncMode = "async"  // Background sync
    SyncNever SyncMode = "never"  // Never replicate
)
```

---

## Implementation Phases

### Phase 1: Foundation (3-4 months)

**Goal**: Basic 2-site coordinator with lazy replication

**Deliverables**:
- [x] Project structure and architecture docs
- [ ] Coordinator service (etcd-based metadata)
- [ ] ObjectFS client library
- [ ] Simple namespace mapping (path → site)
- [ ] Basic replication trigger (manual)
- [ ] CargoShip integration (file transfer)

**Milestones**:
- Week 1-2: Project setup, etcd integration
- Week 3-4: ObjectFS client wrapper
- Week 5-8: Namespace coordinator
- Week 9-12: Basic replication with CargoShip

### Phase 2: Intelligent Replication (3-4 months)

**Goal**: Policy-based replication with cache coherency

**Deliverables**:
- [ ] Policy engine (path patterns, sync modes)
- [ ] Lease-based cache coherency
- [ ] Burst detection and auto-scaling
- [ ] Pre-staging for hot datasets
- [ ] Progress tracking and monitoring

**Milestones**:
- Week 1-4: Policy engine implementation
- Week 5-8: Cache coherency protocol
- Week 9-12: Burst detection and integration

### Phase 3: Production Hardening (3-4 months)

**Goal**: Enterprise-ready with HA and monitoring

**Deliverables**:
- [ ] High availability (multi-coordinator)
- [ ] Failure recovery (site failures, network partitions)
- [ ] Comprehensive monitoring and metrics
- [ ] Performance optimization
- [ ] Security (authentication, encryption)
- [ ] Documentation and examples

**Milestones**:
- Week 1-4: HA coordinator (Raft consensus)
- Week 5-8: Failure recovery testing
- Week 9-12: Performance tuning and docs

---

## Technology Stack

### Core

- **Language**: Go 1.23+ (for consistency with ObjectFS/CargoShip)
- **Metadata Store**: etcd v3.5+ (distributed, consistent, proven)
- **RPC Framework**: gRPC (for coordinator-to-site communication)
- **Configuration**: YAML + Go structs

### Dependencies

- **ObjectFS**: File system operations
- **CargoShip**: Bulk data transfers
- **etcd**: Distributed metadata
- **gRPC**: Inter-component communication
- **Prometheus**: Metrics and monitoring

### Deployment

- **On-Prem**: systemd services
- **Cloud**: Kubernetes/ECS
- **Hybrid**: Consul/Nomad for service discovery

---

## Security Model

### Authentication

- **mTLS**: All coordinator-to-site communication encrypted
- **API Tokens**: For administrative operations
- **Integration**: Leverage existing ObjectFS/S3 auth

### Authorization

- **Site-level**: Which sites can join cluster
- **Path-level**: Which sites can access which paths
- **Policy-based**: Defined in configuration

### Data Protection

- **Encryption at Rest**: Via ObjectFS/S3 backend
- **Encryption in Transit**: Via CargoShip + mTLS
- **Audit Logging**: All replication and access events

---

## Monitoring and Observability

### Metrics (Prometheus)

```
globalfs_site_status{site="onprem",role="primary"} 1
globalfs_replication_jobs_active 3
globalfs_replication_bytes_transferred{src="onprem",dst="cloud"} 10737418240
globalfs_cache_hits{site="cloud"} 1523
globalfs_cache_misses{site="cloud"} 47
globalfs_lease_grants_total{type="read"} 5234
globalfs_lease_invalidations_total 42
```

### Logs (Structured JSON)

```json
{
  "timestamp": "2025-10-16T20:00:00Z",
  "level": "info",
  "component": "coordinator",
  "event": "replication_started",
  "job_id": "job-5678",
  "source": "onprem",
  "destination": "cloud",
  "files": 15,
  "total_bytes": 10737418240
}
```

### Tracing (OpenTelemetry)

- End-to-end request tracing
- Cross-site operation latency
- Replication job timelines

---

## Comparison with Alternatives

### vs. NFS/CIFS

| Feature | NFS/CIFS | GlobalFS |
|---------|----------|----------|
| Cross-site | Poor (WAN latency) | Optimized (local cache) |
| Consistency | Strong | Tunable |
| Performance | Network-bound | Cache-accelerated |
| S3 Integration | None | Native |
| Burst Support | Manual | Automated |

### vs. Ceph/GlusterFS

| Feature | Ceph/Gluster | GlobalFS |
|---------|--------------|----------|
| Setup Complexity | High | Medium |
| On-prem → Cloud | Complex | Native |
| S3 Backend | Addon (RGW) | Native (ObjectFS) |
| Burst Optimization | None | CargoShip |
| HPC Workload | Good | Optimized |

### vs. Manual Sync (rsync/s3sync)

| Feature | Manual Sync | GlobalFS |
|---------|-------------|----------|
| Namespace | Separate | Unified |
| Automation | Scripts | Policy-based |
| Cache Coherency | Manual | Automatic |
| Performance | Basic | Optimized |
| Burst Integration | Manual | Automated |

---

## Future Enhancements

### Multi-Master Writes

Currently, writes go to primary site. Future: allow writes at any site with conflict resolution.

### Automatic Tiering

Move cold data to cheaper storage (S3 Glacier) automatically based on access patterns.

### ML-Based Pre-staging

Use job history and access patterns to predict which data to pre-stage for bursts.

### Multi-Region Support

Extend beyond 2 sites to support multi-region deployments (US, EU, Asia).

### Read-Only Mirrors

Low-cost read-only replicas for disaster recovery.

---

## Getting Started

### Prerequisites

```bash
# ObjectFS installed and configured
objectfs version  # Should be 0.4.0+

# CargoShip installed
cargoship version  # Should be 0.4.5+

# etcd cluster running
etcdctl version  # Should be 3.5+
```

### Installation

```bash
# Clone repository
git clone https://github.com/yourorg/globalfs
cd globalfs

# Build
make build

# Install
sudo make install
```

### Quick Start

```bash
# 1. Start coordinator
globalfs coordinator start --config coordinator.yaml

# 2. Register sites
globalfs site register onprem --config site-onprem.yaml
globalfs site register cloud --config site-cloud.yaml

# 3. Mount global namespace
globalfs mount /globalfs --config globalfs.yaml

# 4. Use transparently
cd /globalfs
ls  # Shows unified view across sites
./my-hpc-job --data inputs/data.dat  # Transparent access
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

---

## License

Apache 2.0 - See [LICENSE](LICENSE)

---

## Contact

- **Issues**: https://github.com/yourorg/globalfs/issues
- **Discussions**: https://github.com/yourorg/globalfs/discussions
- **Slack**: #globalfs on HPC Cloud Workspace

---

## Related Projects

- **ObjectFS**: https://github.com/scttfrdmn/objectfs
- **CargoShip**: https://github.com/yourorg/cargoship
- **etcd**: https://etcd.io

---

**Status**: This project is in the design phase. Architecture and APIs are subject to change.
