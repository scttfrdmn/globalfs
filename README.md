# GlobalFS — Global Namespace for Hybrid HPC Clouds

**Version**: v0.1.0
**Status**: Production-ready coordinator for multi-site object routing
**License**: Apache 2.0

GlobalFS is a coordinator daemon and CLI for routing object operations across
multiple S3-backed sites. It provides a single unified namespace over two or
more [ObjectFS](https://github.com/scttfrdmn/objectfs) instances — on-premises
and cloud — enabling seamless HPC cloud bursting without changing application
code.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Configuration Reference](#configuration-reference)
5. [CLI Reference](#cli-reference)
6. [API Reference](#api-reference)
7. [Development](#development)

---

## Overview

### What GlobalFS does

- **Routes reads** to the nearest healthy site; falls back to other sites automatically
- **Routes writes** synchronously to primary sites; replicates asynchronously to others
- **Health monitors** all sites in the background and skips degraded ones
- **Circuit-breaks** failing sites after a configurable threshold; probes recovery
- **Retries** transient read failures with exponential back-off before giving up
- **Caches** hot objects in memory (LRU with byte-budget eviction) to reduce latency
- **Policy routes** operations by key pattern and operation type (read/write/delete)
- **Exposes** a REST API and `globalfs` CLI for runtime management

### What GlobalFS does not do

- It is not a FUSE filesystem — it routes object (key/value) operations, not POSIX calls
- It does not implement distributed consensus or etcd integration in v0.1.0
- It does not encrypt data in transit (terminate TLS at a load balancer)

---

## Architecture

```
                        ┌──────────────────────────┐
  globalfs CLI  ──────► │   globalfs-coordinator    │ ◄─── Prometheus /metrics
  REST clients  ──────► │   :8090  (HTTP API)       │
                        │                           │
                        │  Policy Engine            │
                        │  Health Cache             │
                        │  Circuit Breaker          │
                        │  Retry                    │
                        │  LRU Object Cache         │
                        │  Replication Worker       │
                        └──────┬──────────┬─────────┘
                               │          │
                    ┌──────────▼──┐   ┌───▼──────────┐
                    │  Site A     │   │  Site B       │
                    │  (primary)  │   │  (burst)      │
                    │             │   │               │
                    │  ObjectFS   │   │  ObjectFS     │
                    │  S3 bucket  │   │  S3 bucket    │
                    └─────────────┘   └───────────────┘
```

### Routing rules

| Operation | Behaviour |
|-----------|-----------|
| **Get / Head** | Tries sites in policy order; promotes healthy sites to front; applies circuit breaker filter; retries per site |
| **Put** | Writes synchronously to primary-role sites; enqueues async replication to others |
| **Delete** | Synchronous on primaries (error returned on failure); best-effort on others (logged) |
| **List** | Priority-merge across all sites; highest-priority site wins on key conflicts |

### Site roles

| Role | Meaning |
|------|---------|
| `primary` | Authoritative site; synchronous writes required |
| `burst` | Cloud overflow site; async replication target |
| `backup` | Read-only fallback; async replication target |

---

## Quick Start

### 1. Build

```bash
git clone https://github.com/scttfrdmn/globalfs
cd globalfs
make build
# produces: bin/globalfs-coordinator  bin/globalfs
```

### 2. Write a minimal config

```bash
./bin/globalfs config init --output config.yaml
# Edit the generated file or use the example below
```

Minimal two-site config:

```yaml
global:
  cluster_name: my-cluster

coordinator:
  listen_addr: ":8090"
  etcd_endpoints:
    - localhost:2379

sites:
  - name: onprem
    role: primary
    objectfs:
      s3_bucket: my-onprem-bucket
      s3_region: us-west-2

  - name: cloud
    role: burst
    objectfs:
      s3_bucket: my-cloud-bucket
      s3_region: us-east-1
```

### 3. Start the coordinator

```bash
# AWS credentials must be available (profile, env vars, or instance role)
AWS_PROFILE=myprofile ./bin/globalfs-coordinator --config config.yaml

# With API key authentication
GLOBALFS_API_KEY=secret ./bin/globalfs-coordinator --config config.yaml
```

The coordinator logs to stderr and listens on `:8090`.

### 4. Use the CLI

```bash
export GLOBALFS_COORDINATOR=http://localhost:8090
export GLOBALFS_API_KEY=secret   # if auth is enabled

# Check health
./bin/globalfs status

# List sites
./bin/globalfs site list

# Store and retrieve an object
echo "hello world" | ./bin/globalfs object put my-key --input -
./bin/globalfs object get my-key

# Coordinator runtime stats
./bin/globalfs info
```

### 5. Verify health and metrics

```bash
curl http://localhost:8090/healthz    # 200 OK when all primaries healthy
curl http://localhost:8090/readyz     # 200 OK once coordinator is started
curl http://localhost:8090/metrics    # Prometheus metrics
```

---

## Configuration Reference

Configuration is loaded from a YAML file passed to `--config`. Missing fields
use the defaults shown below. Generate a starter file with:

```bash
globalfs config init --output config.yaml
```

### `global`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cluster_name` | string | `globalfs-cluster` | Human-readable cluster identifier |
| `log_level` | string | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `log_file` | string | _(stderr)_ | Path to log file; empty = stderr |
| `metrics_enabled` | bool | `true` | Enable Prometheus metrics on `/metrics` |
| `metrics_port` | int | `9090` | Port for the metrics endpoint |

### `coordinator`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | string | `:8090` | HTTP server bind address |
| `etcd_endpoints` | []string | `[localhost:2379]` | etcd cluster endpoints (reserved for future use) |
| `lease_timeout` | duration | `60s` | Distributed lease TTL |
| `health_check_interval` | duration | `30s` | Per-site health check interval |

### `sites[]`

Each entry defines one storage site.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique site identifier |
| `role` | string | yes | `primary`, `burst`, or `backup` |
| `objectfs.s3_bucket` | string | yes | S3 bucket backing this site |
| `objectfs.s3_region` | string | yes | AWS region |
| `objectfs.s3_endpoint` | string | no | Custom endpoint (MinIO, LocalStack, etc.) |
| `objectfs.mount_point` | string | no | Local filesystem path (informational) |
| `cargoship.endpoint` | string | no | CargoShip service endpoint |
| `cargoship.enabled` | bool | no | Enable CargoShip for this site |
| `network.bandwidth` | int | no | Available bandwidth in bytes/sec |
| `network.latency` | duration | no | Expected round-trip latency |

### `policy`

Routing rules are evaluated in `priority` order (lower = higher precedence).
When no rule matches, sites are ordered: primary → backup → burst.

```yaml
policy:
  rules:
    - name: hot-reads
      key_pattern: "datasets/hot/*"  # glob, prefix (ends with /), or exact
      operations: [read]             # read, write, delete (empty = all)
      target_roles: [primary]        # primary, backup, burst (empty = all)
      priority: 10
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Rule identifier |
| `key_pattern` | string | Glob (`*`, `**`), prefix ending in `/`, or exact key |
| `operations` | []string | `read`, `write`, `delete` |
| `target_roles` | []string | `primary`, `backup`, `burst` |
| `priority` | int | Evaluation order (lower = first) |

### `resilience`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `health_poll_interval` | duration | `30s` | Background health check cadence |
| `circuit_breaker.enabled` | bool | `false` | Activate circuit breaking |
| `circuit_breaker.threshold` | int | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.cooldown` | duration | `30s` | Time before a probe is allowed after opening |
| `retry.enabled` | bool | `false` | Activate per-site retry |
| `retry.max_attempts` | int | `3` | Total attempts per site (1 = no retry) |
| `retry.initial_delay` | duration | `100ms` | Pause before first retry |
| `retry.max_delay` | duration | `2s` | Cap on inter-retry pause |
| `retry.multiplier` | float | `2.0` | Exponential back-off scale factor |

Retry applies only to **read** operations (Get, Head). Writes are fail-fast.

### `cache`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Activate in-memory LRU object cache |
| `max_bytes` | int | `67108864` | Maximum cache size in bytes (64 MiB) |
| `ttl` | duration | `0` | Entry TTL; `0` = entries never expire |

The cache is read-through: a Get hit returns data without contacting a site.
Put and Delete invalidate the affected key so stale data is never returned.

### `performance`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_concurrent_transfers` | int | `8` | Maximum parallel replication jobs |
| `transfer_chunk_size` | int | `16777216` | Transfer chunk size in bytes (16 MiB) |
| `cache_size` | string | `1GB` | Passed to ObjectFS (informational) |

---

## CLI Reference

### Global flags

```
--coordinator-addr  addr   Coordinator HTTP address (env: GLOBALFS_COORDINATOR, default: http://localhost:8090)
--api-key           key    API key for X-GlobalFS-API-Key auth (env: GLOBALFS_API_KEY)
--json                     Output in JSON format instead of a table
```

### `site`

#### `site list`

List registered sites with health and optional circuit state.

```
globalfs site list [--json]
```

Output columns: `NAME`, `ROLE`, `STATUS`, and `CIRCUIT` (when a circuit
breaker is configured).

#### `site add`

Register a new site at runtime without restarting the coordinator.

```
globalfs site add --name <name> --uri s3://<bucket>[?region=<r>&endpoint=<url>] --role <primary|burst|backup>
```

#### `site remove`

Deregister a site.

```
globalfs site remove --name <name>
```

### `object`

#### `object get`

Download an object to stdout or a file.

```
globalfs object get <key> [--output <file>]
```

#### `object put`

Upload an object from stdin or a file.

```
globalfs object put <key> [--input <file>]
```

#### `object delete`

Delete an object from all sites.

```
globalfs object delete <key>
```

#### `object head`

Show object metadata (size, ETag, last modified).

```
globalfs object head <key>
```

#### `object list`

List objects across all sites.

```
globalfs object list [--prefix <prefix>] [--limit <n>]
```

### `replicate`

Trigger manual replication of a key between two named sites.

```
globalfs replicate --key <key> --from <site> --to <site>
```

### `config`

#### `config init`

Write a starter configuration template to a file (or stdout).

```
globalfs config init [--output <file>]
```

#### `config validate`

Validate a YAML configuration file.

```
globalfs config validate <file>
```

#### `config show`

Print the resolved configuration (defaults merged with file values).

```
globalfs config show <file>
```

### `info`

Print coordinator runtime statistics (version, uptime, site count, queue depth, health summary).

```
globalfs info [--json]
```

### `status`

Print overall cluster health. Exits non-zero if any primary site is degraded.

```
globalfs status
```

### `version`

Print the CLI version string.

### `completion`

Generate shell completion scripts.

```
globalfs completion bash|zsh|fish|powershell
```

---

## API Reference

All endpoints are under `http://<coordinator-addr>/`. When API key
authentication is enabled, every request (except `/healthz` and `/readyz`)
must carry the header:

```
X-GlobalFS-API-Key: <key>
```

Responses are `application/json`. Error responses have the shape:

```json
{"error": "message"}
```

### Health endpoints

#### `GET /healthz`

Returns `200 OK` (body `OK`) when all primary sites are healthy.
Returns `503 Service Unavailable` (body `DEGRADED\n<site: error>`) otherwise.

Uses the background health cache; falls back to a live check on first startup.

#### `GET /readyz`

Returns `200 OK` once the coordinator has started. Always succeeds after boot.

### Metrics

#### `GET /metrics`

Prometheus metrics. Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `globalfs_object_operations_total` | counter | Operations by `operation` and `status` |
| `globalfs_object_operation_duration_seconds` | histogram | Operation latency |
| `globalfs_sites_current` | gauge | Number of registered sites |
| `globalfs_replication_jobs_total` | counter | Replication jobs by `status` |
| `globalfs_replication_queue_depth` | gauge | Jobs waiting in queue |
| `globalfs_cache_hits_total` | counter | Cache hits |
| `globalfs_cache_misses_total` | counter | Cache misses |
| `globalfs_cache_evictions_total` | counter | Cache evictions |
| `globalfs_cache_bytes` | gauge | Bytes currently stored in cache |

### Coordinator info

#### `GET /api/v1/info`

```json
{
  "version": "0.1.0",
  "uptime_seconds": 3600.5,
  "sites": 2,
  "is_leader": true,
  "replication_queue_depth": 0,
  "sites_by_role": {"primary": 1, "burst": 1},
  "health": {
    "healthy": 2,
    "unhealthy": 0,
    "last_checked_at": "2026-02-22T10:00:00Z"
  }
}
```

### Sites

#### `GET /api/v1/sites`

Returns all registered sites with health and circuit state.

```json
[
  {
    "name": "onprem",
    "role": "primary",
    "healthy": true,
    "circuit_state": "closed"
  },
  {
    "name": "cloud",
    "role": "burst",
    "healthy": false,
    "error": "connection timeout",
    "circuit_state": "open"
  }
]
```

`circuit_state` is omitted when no circuit breaker is configured.

#### `POST /api/v1/sites`

Register a new site. Returns `201 Created`.

```json
{
  "name": "cloud2",
  "role": "burst",
  "s3_bucket": "my-burst-bucket",
  "s3_region": "eu-west-1",
  "s3_endpoint": ""
}
```

#### `DELETE /api/v1/sites/{name}`

Deregister a site. Returns `204 No Content`.

### Replication

#### `POST /api/v1/replicate`

Enqueue manual replication of a key. Returns `202 Accepted`.

```json
{"key": "datasets/hot/sim.dat", "from": "onprem", "to": "cloud"}
```

### Objects

All object endpoints accept an arbitrary key path after `/api/v1/objects/`.

#### `GET /api/v1/objects/{key...}`

Returns object data as `application/octet-stream`.

#### `PUT /api/v1/objects/{key...}`

Stores the request body. Returns `201 Created` on success.

#### `DELETE /api/v1/objects/{key...}`

Deletes the object from all sites. Returns `204 No Content`.

#### `HEAD /api/v1/objects/{key...}`

Returns object metadata as response headers:

```
Content-Length: 1048576
ETag: "abc123"
Last-Modified: Sat, 22 Feb 2026 10:00:00 GMT
```

#### `GET /api/v1/objects?prefix=<p>&limit=<n>`

Lists objects. `prefix` and `limit` are optional. Returns:

```json
[
  {"key": "datasets/hot/sim.dat", "size": 1048576, "etag": "abc123", "last_modified": "..."},
  ...
]
```

---

## Development

### Requirements

- Go 1.26+
- AWS credentials (profile `aws` or environment variables) for integration tests

### Build and test

```bash
make build          # compile both binaries
make test           # go test -race ./...
make lint           # golangci-lint

go build ./...      # verify compilation only
```

### Integration tests

Integration tests hit real AWS S3. Set the `aws` named profile:

```bash
AWS_PROFILE=aws AWS_REGION=us-west-2 go test -race -tags=integration ./...
```

### Pre-commit hooks

The project uses pre-commit hooks (gofmt, go build, golangci-lint,
markdownlint). They run automatically on `git commit` and auto-fix what they
can. Re-stage and commit again if files are modified.

### Project layout

```
cmd/
  coordinator/    daemon binary (globalfs-coordinator)
  globalfs/       operator CLI binary (globalfs)
internal/
  cache/          in-memory LRU object cache
  circuitbreaker/ per-site three-state circuit breaker
  coordinator/    routing, health, replication orchestration
  lease/          distributed lease manager
  metadata/       replication job persistence
  metrics/        Prometheus instrumentation
  policy/         key-pattern routing rule engine
  replication/    background async replication worker
  retry/          exponential back-off retry
pkg/
  client/         Go client library
  config/         YAML configuration types and loader
  namespace/      multi-site object namespace (priority-merge list)
  site/           ObjectFS site connection wrapper
  types/          shared type definitions
```

---

## Related Projects

- **[ObjectFS](https://github.com/scttfrdmn/objectfs)** — POSIX-compliant FUSE filesystem for S3; provides the per-site backend
- **[CargoShip](https://github.com/scttfrdmn/cargoship)** — streaming archive/upload pipeline for S3 bulk transfers

---

## License

Apache 2.0 — Copyright 2025-2026 Scott Friedman. See [LICENSE](LICENSE).
