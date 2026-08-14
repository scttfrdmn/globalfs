# GlobalFS — Global Namespace for Hybrid HPC Clouds

**Status**: Single-instance coordinator for multi-site object routing
**License**: Apache 2.0

For the current version see the [latest release](https://github.com/scttfrdmn/globalfs/releases/latest)
and [CHANGELOG.md](CHANGELOG.md). This file deliberately does not carry a version
number: the one that used to be here said v0.1.0 at a repository tagged v0.2.1,
because it was hand-maintained and had no consumer that would notice (#105).

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
- It does not implement distributed consensus, leader election, or etcd integration
- It does not detect or repair divergence between sites once it has occurred
- It does not encrypt data in transit (terminate TLS at a load balancer)

### Deployment topology: run exactly one coordinator

**The coordinator is single-instance. Running two against the same buckets is
unsupported and silently produces divergent replicas.**

There is no leader election, no standby mode, and no mutual exclusion of any kind.
`internal/lease` and the etcd metadata store exist in the tree but nothing
constructs them — `IsLeader()` therefore returns a hardcoded `true`, and
`/api/v1/info` reports `"is_leader": true` on every coordinator that is running.
Two coordinators both believe they are leader, both accept writes, both replicate,
and each dedups only against its own in-memory state.

Nothing anywhere in the system would detect the result. There is no scrub, no
reconciliation pass, and no version, generation, or vector clock on any object, so
divergence between two sites is permanent rather than eventually consistent.

Earlier CHANGELOG entries (v0.2.0) list "Coordinator leader election" and "Standby
coordinator mode" under `### Added`. Those describe scaffolding that landed in the
tree and was never wired up; they are corrected in place rather than deleted, since
anyone who already read them holds a belief this note needs to reach (#107). Whether
to wire the store and the lease manager or remove them is [#112](https://github.com/scttfrdmn/globalfs/issues/112).

For availability, run one coordinator and restart it — it is stateless apart from
the replication queue, and a restart re-derives everything from the config file. A
second process is not a failover; it is a second writer.

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
| **Delete** | Synchronous on every routed site; an error is returned if any site may still hold the object |
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

Or copy [`examples/quickstart.yaml`](examples/quickstart.yaml), reproduced here:

```yaml
global:
  cluster_name: my-cluster

coordinator:
  listen_addr: ":8090"

sites:
  - name: onprem
    role: primary
    objectfs:
      mount_point: /mnt/objectfs-onprem
      s3_bucket: my-onprem-bucket
      s3_region: us-west-2

  - name: cloud
    role: burst
    objectfs:
      mount_point: /mnt/objectfs-cloud
      s3_bucket: my-cloud-bucket
      s3_region: us-east-1
```

Then check it before starting anything:

```bash
./bin/globalfs config validate config.yaml
```

Every key is checked against the schema, and an unrecognised one is an error
naming the key and its line rather than a value that gets silently discarded. If
you are upgrading a config written for v0.2.x or earlier, expect this to reject
fields that used to be accepted and ignored — see [CHANGELOG.md](CHANGELOG.md).

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

**Unknown keys are rejected.** A key that does not appear in the tables below is a
startup error naming the key and its line, not a value to be discarded. This
changed in v0.3.0 (#97): before it, a typo such as `listen_adrr` was accepted and
the daemon bound the default port, and `config validate` reported success. If a
config that used to load now fails, the field it names was already having no
effect — the error is the first honest report of that.

Fields removed in v0.3.0, which will now fail to load rather than being ignored:

| Removed field | What to do |
|---|---|
| `global.metrics_port` | Delete it. `/metrics` is a route on the main API listener, not a second port — see [Metrics](#metrics) |
| `sites[].cargoship.*` | Delete the block. Never read; the upstream feature it named was removed from ObjectFS |
| `performance.max_concurrent_transfers` | Rename to `performance.replication_queue_depth`; it sizes a queue, not a pool |
| `policies[].sync_mode` | Delete it. It was never a field on any struct and was silently discarded on every load |

Fields removed earlier, in v0.1.5, which the previous version of this table still
listed for eleven releases (#99):

| Removed field | Successor |
|---|---|
| `coordinator.health_check_interval` | `resilience.health_poll_interval` |
| `performance.cache_size` | `cache.max_bytes` |
| `performance.transfer_chunk_size` | none — belonged to a bandwidth-scheduling design that was never built |
| `network.bandwidth` | none, as above. The `network` section does not exist |
| `network.latency` | none, as above |

### `global`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cluster_name` | string | `globalfs-cluster` | Human-readable cluster identifier |
| `log_level` | string | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `log_file` | string | _(stderr)_ | Path to log file; empty = stderr. Opened in append mode; if it cannot be opened the daemon logs to stderr and says so rather than refusing to start |
| `metrics_enabled` | bool | `true` | Serve `/metrics`. When false the route is not registered and returns 404 |

### `coordinator`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | string | `:8090` | HTTP server bind address |
| `etcd_endpoints` | []string | `[localhost:2379]` | Accepted and unused; see below |
| `lease_timeout` | duration | `60s` | Accepted and unused; see below |

`etcd_endpoints` and `lease_timeout` configure the etcd metadata store and the
lease manager, and **nothing constructs either one**, so no connection to these
endpoints is ever attempted. Both fields are optional, and an empty
`etcd_endpoints: []` is valid — until v0.3.0 the validator required it, which made
a startup failure out of a value nothing read while the shipped example told
operators to leave it empty (#106). The fields are kept rather than removed
because they are the configuration surface for the store if it is ever wired.

### `sites[]`

Each entry defines one storage site.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique site identifier |
| `role` | string | yes | `primary`, `burst`, or `backup` |
| `objectfs.mount_point` | string | **yes** | Local path of this site's ObjectFS mount |
| `objectfs.s3_bucket` | string | yes | S3 bucket backing this site |
| `objectfs.s3_region` | string | yes | AWS region |
| `objectfs.s3_endpoint` | string | no | Custom endpoint (MinIO, LocalStack, etc.) |

`mount_point` is required — this table said otherwise for eleven releases while the
validator rejected configs that omitted it, including the one in this README's own
Quick Start (#96). GlobalFS itself only records the value; it routes S3 operations
and never touches the path. It is required because a site configured without one is
in practice a half-finished config, and refusing it at startup is better than a
FUSE mount nobody set up.

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
| `replication_queue_depth` | int | `8` | Replication jobs that may wait in the queue before `Put` reports backpressure |

**This is a queue size, not a parallelism setting.** The replication worker is a
single goroutine draining the queue serially, so raising this buys buffer, not
throughput. It was called `max_concurrent_transfers` and documented here as
"maximum parallel replication jobs", which it never was (#101).

Raising it makes `Put` tolerate a longer burst before it starts waiting, and then
answering `202` with `X-GlobalFS-Replication: pending`. Lowering it surfaces
backpressure sooner. Serial transfers are what currently guarantees that two `Put`s
of the same key replicate in order; a worker pool would need per-key affinity or an
accepted last-writer-wins before it could be added.

### `security`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allow_private_endpoints` | bool | `false` | Permit API-supplied `s3_endpoint` values that resolve into private address space |
| `allowed_endpoint_hosts` | []string | _(empty)_ | Exact-match hosts exempt from endpoint address checks |

These apply only to `s3_endpoint` in `POST /api/v1/sites`, not to the `sites:`
block of a config file — a config file is operator input, the API is not.

`POST /api/v1/sites` makes the coordinator sign a `HeadBucket` to the endpoint
with its own AWS credentials, so an unconstrained endpoint would hand a valid
SigV4 `Authorization` header to any host a caller names. Endpoints must be
absolute `http`/`https` origins, and are rejected if they resolve to loopback,
link-local (including IMDS at `169.254.169.254`), unspecified, or multicast
addresses — checked after DNS resolution, so a public name pointing at an
internal address is caught too.

Set `allow_private_endpoints: true` for in-cluster MinIO on an RFC1918 address.
It does **not** unblock loopback or link-local; use `allowed_endpoint_hosts` for
those:

```yaml
security:
  allow_private_endpoints: true
  allowed_endpoint_hosts:
    - minio.storage.svc.cluster.local
```

Known gap: the interval between resolving the host and the S3 SDK connecting is
a DNS-rebinding window. Closing it requires pinning the connection to the
resolved address, which the ObjectFS SDK does not currently expose.

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

### Object key restrictions

Request paths containing a `..` component are rejected with `400`, in both their
literal and percent-encoded spellings (`..`, `%2E%2E`, `%2e.`, `%2F%2E%2E%2F`).
The check runs before routing, so a traversal is never redirected onto another
endpoint — relevant if you authorize by path prefix at a proxy.

`..` is only rejected as a whole path component. Keys such as `a..b`,
`snapshot..`, and `v1..2/file.bam` are accepted and stored verbatim. S3 permits
a bare `..` component, so a key of that form cannot be addressed through this
API.

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
| `globalfs_replication_dropped_total` | counter | Writes whose replication was never queued |
| `globalfs_replication_terminal_events_dropped_total` | gauge | Replication outcomes the coordinator never received |
| `globalfs_delete_incomplete_total` | counter | Deletes that left the object readable somewhere |
| `globalfs_cache_hits_total` | counter | Cache hits |
| `globalfs_cache_misses_total` | counter | Cache misses |
| `globalfs_cache_evictions_total` | counter | Cache evictions |
| `globalfs_cache_bytes` | gauge | Bytes currently stored in cache |

The three middle rows are the ones to alert on, and any increase in any of them is
worth paging for — each counts an operation that reported success while leaving
the cluster in a state the caller was not told about:

- **`replication_dropped_total`** — the queue was full, so the write landed on its
  primary and nowhere else. The caller was told, via `ErrReplicationNotQueued` or
  a `202` from `PUT /api/v1/objects`, but a client that ignores that distinction
  now holds single-copy data it believes is replicated.
- **`replication_terminal_events_dropped_total`** — the transfer ran, and probably
  succeeded, but the coordinator never learned the outcome. Its job record is
  replayed on the next restart and its content hash was never written, so the
  same bytes transfer again. Reaching this requires an event consumer stalled for
  ten seconds, which is itself worth investigating.
- **`delete_incomplete_total`** — a `Delete` returned success with the object
  still present on at least one site, so it is still readable through the same
  API that just reported it gone.

`replication_terminal_events_dropped_total` is a gauge carrying a monotonic value:
the count is owned by the replication worker and mirrored into the registry with
`Set`, rather than incremented here, so it cannot drift from the worker's own
value across a scrape. `increase()` and `rate()` work on it as they would on a
counter, which is why it keeps the `_total` suffix.

### Coordinator info

#### `GET /api/v1/info`

```json
{
  "version": "0.3.0",
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

`is_leader` is always `true` and carries no information: there is no election, so
every running coordinator claims leadership. Do not use it to decide which of two
coordinators should be writing — see [Deployment
topology](#deployment-topology-run-exactly-one-coordinator).

`replication_queue_depth` is a live sample of the same value as the
`globalfs_replication_queue_depth` gauge.

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

`s3_endpoint` is optional; empty means the AWS default for the region. When
supplied it must be an absolute `http`/`https` origin (no userinfo, path, query,
or fragment) that does not resolve into a blocked address range — see
[`security`](#security). Rejected endpoints return `400`; a reachability failure
returns `502` with a generic message, the detail going to the coordinator log.

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

Returns object data as `application/octet-stream`. A key that exists at no routed
site returns `502` today, not `404` — the coordinator distinguishes absence from
outage internally but the handler does not yet map it, tracked as
[#110](https://github.com/scttfrdmn/globalfs/issues/110).

#### `PUT /api/v1/objects/{key...}`

Stores the request body. Bodies are capped at 256 MiB; a larger one returns `413`.

| Status | Meaning |
|--------|---------|
| `201 Created` | Stored on every primary and replication to all secondaries was queued |
| `202 Accepted` | **Stored and readable, but replication was not queued for at least one secondary** |

A `202` also carries `X-GlobalFS-Replication: pending` and a body naming the
destinations that got no job:

```json
{
  "key": "datasets/hot/sim.dat",
  "status": "stored; replication incomplete",
  "detail": "coordinator: replication not queued for [cloud]: replication: queue full"
}
```

The bytes are durable on every primary in the routed set and readable immediately —
a `202` is not a failed write, and treating it as one leads to retrying a write that
already committed. What it means is that the object is single-copy for now. Retrying
the identical PUT is safe and is the right response if you need the replica: the
primary write is idempotent and the coordinator skips destinations that already hold
the content hash. Alert on `globalfs_replication_dropped_total` rather than on this
status, since a client that ignores the distinction will not report it.

#### `DELETE /api/v1/objects/{key...}`

Deletes the object from all routed sites. Returns `204 No Content` only when every
one of them confirmed the delete; if any site still holds a copy the response is
`502` and `globalfs_delete_incomplete_total` increments. An object reported deleted
is not readable from any site.

#### `HEAD /api/v1/objects/{key...}`

Returns object metadata as response headers, and never a body — including on error,
where the status alone carries the result.

```
Content-Type: application/octet-stream
Content-Length: 1048576
ETag: "abc123"
Last-Modified: Sat, 22 Feb 2026 10:00:00 GMT
X-GlobalFS-Checksum: 9f86d081884c7d65...
```

`X-GlobalFS-Checksum` is the object's SHA-256 as recorded by ObjectFS, and is the
only way to obtain a digest without downloading the object. It is absent when the
backend has no checksum recorded for that object. `Content-Type` falls back to
`application/octet-stream`.

#### `GET /api/v1/objects?prefix=<p>&limit=<n>`

Lists objects across all sites. `prefix` and `limit` are optional; `limit` must be
a non-negative integer, and `0` or omitted means no limit.

The response is an **object**, not a bare array:

```json
{
  "prefix": "datasets/",
  "count": 2,
  "objects": [
    {"key": "datasets/hot/sim.dat", "size": 1048576, "etag": "abc123",
     "content_type": "application/octet-stream", "checksum": "", "last_modified": "..."}
  ]
}
```

| Status | Meaning |
|--------|---------|
| `200 OK` | Every routed site answered |
| `207 Multi-Status` | **At least one site answered and at least one failed — the listing may be missing keys** |
| `502 Bad Gateway` | No site answered |

A `207` also sets `X-GlobalFS-Partial: true`. Read the header rather than the status
if you are behind a proxy or a client library that treats any 2xx as complete: a
`207` listing is a subset of the true namespace, and the keys that are absent are
exactly the ones on the sites that failed. Do not use it to conclude that a key does
not exist, and do not use it as the input to a deletion or synchronisation pass. The
error detail naming the failed sites goes to the coordinator log, not the response.

Two caveats apply even to a `200`:

- **`checksum` is always empty from this endpoint.** S3's `ListObjectsV2` returns no
  user metadata, and the checksum lives there, so only `HEAD` can supply it.
  Similarly `size` is the stored (possibly compressed) size here and the
  uncompressed size from `HEAD`.
- **A `200` is not a guarantee that the listing is complete.** `limit` is passed to
  each site individually and the merged result is then truncated, so a
  lexicographically-early key held only on a lower-priority site can be missing at
  any limit. Tracked as [#109](https://github.com/scttfrdmn/globalfs/issues/109).

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

### Before committing

There are no git hooks in this repository — a previous version of this section
described pre-commit hooks that no `.pre-commit-config.yaml` has ever configured.
Run the checks yourself; CI runs the same ones:

```bash
gofmt -l .          # must print nothing
go vet ./...
make test           # go test -race ./...
make lint           # golangci-lint
```

CI additionally runs `go mod tidy` and fails on a diff, cross-compiles for
linux/darwin on amd64/arm64, builds with the `integration` tag, and validates every
shipped config example — including `examples/quickstart.yaml`, which is the YAML
embedded in the Quick Start above, so that block cannot go stale without CI saying
so.

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
- **[CargoShip](https://github.com/scttfrdmn/cargoship)** — streaming archive/upload pipeline for S3 bulk transfers. **Not integrated.** GlobalFS carried a `cargoship:` config block that nothing read; it was removed in v0.3.0 (#108) along with a comment promising a streaming pipeline that ObjectFS had itself deleted upstream

---

## License

Apache 2.0 — Copyright 2025-2026 Scott Friedman. See [LICENSE](LICENSE).
