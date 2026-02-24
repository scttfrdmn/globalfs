# Changelog

All notable changes to GlobalFS are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.2.0] - 2026-02-23

### Added
- `internal/replication/worker.go`: `ReplicationEvent.ContentHash` field (SHA-256 hex) — set to the transferred content's hash on `EventCompleted`, empty otherwise; enables callers to track what was actually replicated (#131)
- `internal/replication/worker.go`: `transfer()` fast path — if both source and destination expose `ObjectInfo.Checksum` (populated by ObjectFS ≥ v0.10.0) and they match, the full GET → PUT is skipped; backward compatible (empty checksum falls back to GET → PUT) (#131)
- `internal/metadata/store.go`: `ReplicatedObject` struct and two new `Store` interface methods — `PutReplicatedObject` / `GetReplicatedObject` — persist last-known content hash per (site, key) pair for coordinator-level dedup (#132)
- `internal/metadata/memory_store.go`, `etcd_store.go`: Implement `PutReplicatedObject` / `GetReplicatedObject`; etcd key pattern: `{prefix}replicated/{site}/{key}` (#132)
- `internal/coordinator/coordinator.go`: `drainWorkerEvents` records `ReplicatedObject` in the store on every `EventCompleted` with a non-empty `ContentHash` (#132)
- `internal/coordinator/coordinator.go`: `Put()` performs coordinator-level dedup before enqueue — skips replication to a destination if `GetReplicatedObject` confirms the current content hash is already present; saves a full GET+PUT round-trip for idempotent retries of large files (#132)

### Changed
- GlobalFS now depends on objectfs v0.10.0 (adds parallel range GETs and content SHA-256 metadata)

---

## [0.1.12] - 2026-02-24

### Fixed
- `pkg/config/config.go`: `Validate` now rejects a site config where `cargoship.enabled: true` but `cargoship.endpoint` is blank, surfacing the misconfiguration at startup rather than at runtime (#68)
- `cmd/coordinator/api.go`: `withObjectMetrics` now explicitly checks `m != nil` before instrumenting; previously relied on nil-safe receiver methods, which was fragile (#71)
- `cmd/coordinator/main.go`: shutdown errors from `srv.Shutdown` and `c.Close` are now logged at `slog.Error` level and cause the process to exit with code 1, so orchestrators detect failed shutdowns (#69)

### Changed
- `internal/coordinator/coordinator.go`, `internal/replication/worker.go`, `internal/metadata/etcd_store.go`: remaining `log.Printf` calls migrated to structured `slog.Info`/`slog.Warn`/`slog.Error` — zero `"log"` imports remain in non-main packages (#67)
- `internal/coordinator/coordinator.go`: inline `if c.m != nil { c.m.RecordXxx() }` guards replaced with private wrapper methods (`metricsCacheHit`, `metricsCacheMiss`, `metricsCacheEviction`, `metricsCacheBytes`, `metricsSiteCount`) that centralise the nil check (#70)

---

## [0.1.11] - 2026-02-23

### Fixed
- `internal/coordinator/coordinator.go`: `Put` enqueue-failure log now includes key and destination site name for easier diagnosis — was `"coordinator: %v"`, now `"coordinator: Put %q: enqueue async replication to %q: %v"` (#40)
- `internal/coordinator/coordinator.go`: `Get`, `Put`, and `Delete` cache-metric calls now wrapped in explicit `if m != nil` guards — metrics receiver methods are already nil-safe, but the guard makes intent clear at the call site (#51)
- `cmd/coordinator/main.go`: `MaxConcurrentTransfers` was already wired to `SetWorkerQueueDepth` in the `Start` setup block; closing stale issue (#50)

---

## [0.1.10] - 2026-02-23

### Fixed
- `internal/coordinator/coordinator.go`: `drainWorkerEvents` now uses `context.WithTimeout(context.Background(), 5s)` for `store.DeleteJob` calls instead of the already-cancelled shutdown context — prevents orphaned job records in the metadata store on coordinator shutdown (#61, carried from earlier audit)
- `internal/metadata/etcd_store.go`: `NewEtcdStore` now returns a clear error when `cfg.Endpoints` is empty instead of panicking with an index-out-of-range accessing `cfg.Endpoints[0]` (#61)
- `internal/coordinator/coordinator.go`: `RemoveSite` now closes the removed site's S3 client after releasing the mutex, preventing connection-pool leaks when sites are deregistered at runtime (#62)
- `cmd/coordinator/api.go`: `replicateHandler` now returns HTTP 503 Service Unavailable when the replication queue is full, instead of HTTP 400 Bad Request — queue-full is a server capacity condition, not a client error (#63)
- `internal/replication/worker.go`: `Worker.Stop()` now uses `sync.Once` (`closeOnce`) to close the `done` channel, eliminating a race condition where two concurrent `Stop()` callers could both observe the channel open and both attempt to close it (#64)
- `cmd/coordinator/main.go`: shutdown now calls `c.Close()` instead of `c.Stop()` followed by a manual loop over the startup mounts — `Close()` calls `ns.Close()` which covers all sites including those registered dynamically via `POST /api/v1/sites`, closing previously-leaked connections (#65)
- `cmd/coordinator/api.go`: `objectGetHandler` now checks the error returned by `w.Write(data)` and logs a warning on failure, making client disconnects visible in logs (#66)

---

## [0.1.9] - 2026-02-23

### Fixed
- `pkg/client/client.go`: `ListObjects` now accepts HTTP 207 Multi-Status in addition to 200 OK — previously any 207 response was treated as an error, discarding valid partial results from federated list operations (#57)
- `pkg/namespace/namespace.go`: `List` now passes the caller-supplied `limit` to each per-site `List` call instead of hardcoded `0` — passing `0` caused all objects to be fetched from every site before truncating, wasting bandwidth and risking OOM for large namespaces (#58)
- `internal/coordinator/coordinator.go`: `RemoveSite` now returns a `bool` (found/removed); `removeSiteHandler` uses the bool directly, eliminating the TOCTOU window between the prior `Sites()` pre-check and the remove call (#59)
- `internal/coordinator/coordinator.go`: `start()` now reads `leaseTTL` and `healthPollInterval` under `c.mu` and uses local copies, eliminating a data race where both fields could be modified by setters concurrently (#60)

---

## [0.1.8] - 2026-02-23

### Fixed
- `coordinator.Put` now skips `worker.Enqueue` when `store.PutReplicationJob` fails, preserving the durability guarantee that the metadata store is the authoritative source of truth before any replication work is scheduled (#56)

---

## [0.1.7] - 2026-02-23

### Fixed
- `NewEtcdStore` now logs a `slog.Warn` when `cli.Close()` fails on the ping-failure path instead of silently discarding the error with `_ = cli.Close()` (#55)

---

## [0.1.6] - 2026-02-23

### Fixed
- `MemoryStore.PutSite` and `PutReplicationJob` now log a `slog.Error` when `json.Marshal` fails rather than silently discarding the error and sending `nil` data to watchers (#52)
- `replication.Worker` stop-during-backoff now wraps `lastErr` into the `EventFailed` error (`"worker stopped: <cause>"`) so the transfer failure cause is not lost (#53)
- `cache.Cache.PutAndRecordEvictions` added: atomically inserts a value and returns the exact number of entries evicted, eliminating the TOCTOU eviction double-count in `Coordinator.Get` metrics (#54)

---

## [0.1.5] - 2026-02-23

### Fixed
- `cache.Invalidate` now uses a two-pass approach (collect matching elements, then remove) to avoid the undefined Go behaviour of modifying a map during range iteration, which could silently skip entries (#45)
- API key comparison replaced with `crypto/subtle.ConstantTimeCompare` to eliminate timing side-channel vulnerability in `X-GlobalFS-API-Key` validation (#46)
- `Coordinator.Health` now imposes a 30-second deadline when the caller's context has no deadline, preventing per-site goroutines from blocking indefinitely on unreachable sites (#47)
- `circuitbreaker.Breaker.State` now writes the Open → HalfOpen transition back to the internal state (matching `Allow`), so the reported state is consistent with subsequent `Allow` calls (#48)
- `memBackend.keepAlive` goroutine uses `context.WithTimeout(5s)` for the revoke call instead of `context.Background()`, bounding the goroutine's lifetime if revoke ever contends on the lock (#49)
- `Coordinator.AddSite`, `RemoveSite`, and `drainWorkerEvents` now guard `c.m` calls with explicit `if c.m != nil` checks rather than relying implicitly on nil-safe receivers (#51)

### Changed
- `CoordinatorConfig.HealthCheckInterval` field removed; use `resilience.health_poll_interval` (already wired since v0.1.0) (#50)
- `NetworkConfig` type and `SiteConfig.Network` field removed; bandwidth and latency were never consumed by the coordinator daemon (#50)
- `PerformanceConfig.TransferChunkSize` and `PerformanceConfig.CacheSize` fields removed; they were parsed but had no runtime effect (#50)
- `CoordinatorConfig.LeaseTimeout` is now consumed: the coordinator daemon calls `SetLeaseTTL` at startup, configuring the distributed leader-lease TTL (#50)
- `PerformanceConfig.MaxConcurrentTransfers` is now consumed: the coordinator daemon calls `SetWorkerQueueDepth` at startup, setting the replication worker queue capacity (#50)
- `coordinator.Coordinator` gains two new configuration methods: `SetLeaseTTL(time.Duration)` and `SetWorkerQueueDepth(int)`; both must be called before `Start` (#50)

---

## [0.1.4] - 2026-02-23

### Fixed
- `Namespace.List` takes a snapshot of the sites slice under `sync.RWMutex` before fan-out so concurrent `AddSite` calls cannot race with ongoing list iterations (#39)
- `replication.Worker.Enqueue` now returns an `error` when the queue is full instead of logging and silently dropping; coordinator callers log or propagate the error (#40)
- `addSiteHandler` and `replicateHandler` apply `http.MaxBytesReader` (1 MiB) to JSON request bodies and return `413 Request Entity Too Large` on oversized input (#41)
- Object key handlers (`GET`, `PUT`, `DELETE`, `HEAD`) reject keys containing null bytes or `..` path components with `400 Bad Request` (#42)
- `Coordinator.List` now routes through the policy engine (using `OperationRead` and the prefix as the key) and applies health-aware ordering and circuit-breaker filtering, matching the routing behaviour of `Get` and `Head` (#43)

### Changed
- `policy.Engine.Route` no longer takes a `context.Context` parameter; the argument was unused (`_ context.Context`) and all callers have been updated (#44)

---

## [0.1.3] - 2026-02-23

### Fixed
- `site.New()` now panics immediately when `client` is nil instead of deferring to a nil-pointer dereference on first use (#35)
- HTTP server gains `ReadHeaderTimeout: 5s` and `IdleTimeout: 60s` to mitigate Slowloris slow-header attacks and cap keep-alive lifetime (#36)

### Changed
- Version fallback changed from `"0.1.0"` to `"dev"` in both binaries so ad-hoc `go build` / `go run` builds are never mistaken for a released version (#34)
- `site list`, `site add`, `site remove`, and `replicate` CLI commands migrated from raw HTTP helpers to `pkg/client.Client` methods; `CircuitState` added to `client.SiteInfo` (#38)

### Removed
- Dead types removed from `pkg/types`: `SyncMode` + constants, `LeaseType` + constants, `Lease`, `SiteInfo`, `HealthMetrics`, `FileMetadata` (#37)
- Unused `Priority` field removed from `internal/metadata.ReplicationJob` and `internal/replication.ReplicationJob` (#37)
- Duplicate raw HTTP helpers `apiPost` and `apiDelete` removed from the CLI after client migration (#38)

---

## [0.1.2] - 2026-02-23

### Fixed
- `config.Validate()` now rejects invalid `log_level` values and validates `resilience` and `cache` fields when the respective feature is enabled (#30)
- `MemoryStore.notify()` is now called after releasing the write lock, eliminating the deadlock risk when watcher consumers call back into store methods; `safeWatchSend` guards the narrow close-after-snapshot race (#32)
- `coordinator.Put` now persists the replication job to the store *before* enqueueing it in the worker, closing a race where a fast worker could complete and `DeleteJob` could run before `PutReplicationJob` (#32)
- etcd `Watch` now checks `resp.Err()` and logs compaction/reconnect errors so missed events are surfaced rather than silently dropped (#33)

### Added
- Unit tests for `addSiteHandler`, `removeSiteHandler`, and `replicateHandler` in the coordinator HTTP API (#31)
- Unit tests for all new `config.Validate()` paths — 10 new cases (#30)

---

## [0.1.1] - 2026-02-23

### Fixed
- `cache.Cache` no longer evicts entries when a new value would fit within the remaining budget, only when it would exceed it (#25)
- `Coordinator.Start()` is now guarded by `sync.Once` so calling it multiple times does not launch duplicate background goroutines (#26)
- `objectPutHandler` enforces a 32 MiB request-body limit via `http.MaxBytesReader`, returning `413 Request Entity Too Large` on oversized uploads (#27)
- `setupLogger` is now called before config parsing so the `--log-level` flag takes effect for all startup log lines (#28)
- `namespace.Namespace.List` returns a non-nil error alongside partial results when one or more sites fail, and the HTTP handler responds with `207 Multi-Status` in that case (#29)

---

## [0.1.0] - 2026-02-22

First production-ready release of the GlobalFS coordinator.

### Added

#### Core coordinator (#1)
- In-memory coordinator with role-based routing (primary → backup → burst)
- `Get`, `Put`, `Delete`, `Head`, `List` operations across multiple `SiteMount` instances
- `partitionByRole` helper for write/delete fan-out
- Concurrent site-health checks via `Health(ctx)`

#### Policy engine (#2)
- Rule-based routing engine (`internal/policy`) with glob, prefix, and exact key-pattern matching
- Routing by operation type (`read`, `write`, `delete`) and target site role
- Priority ordering — lower value = higher precedence
- `policy.New()` empty engine falls back to default role ordering
- YAML configuration via `policy.rules[]`

#### Replication worker (#3)
- Bounded retriable background replication worker (`internal/replication`)
- `ReplicationJob{SourceSite, DestSite, Key, Size}` queue with configurable capacity
- Per-job retry with exponential back-off; configurable max attempts
- `Events()` channel for completed/failed job notifications
- Wired into coordinator `Put` for async replication to non-primary sites

#### Coordinator daemon binary (#4)
- `globalfs-coordinator` daemon (`cmd/coordinator`)
- Flags: `--config`, `--log-level`, `--bind-addr`, `--api-key`, `--health-poll-interval`, `--version`
- `GLOBALFS_API_KEY` environment variable support
- Structured logging via `log/slog`
- Graceful shutdown on `SIGINT`/`SIGTERM` (30 s drain window)
- `/healthz` and `/readyz` HTTP endpoints

#### Metadata store (#5)
- `metadata.Store` interface with `PutReplicationJob`, `GetPendingJobs`, `DeleteJob`
- In-memory implementation for testing
- etcd v3 implementation for production persistence
- Coordinator recovers pending replication jobs from the store on startup
- Worker event drain updates and cleans the store

#### Distributed lease manager (#6)
- etcd-backed distributed lease manager (`internal/lease`)
- `TryAcquire`, `KeepAlive`, `Release` API
- Coordinator leader election: only the lease holder starts the replication worker
- Standby coordinator mode when another instance holds the lease

#### Operator CLI (#7)
- `globalfs` cobra CLI (`cmd/globalfs`)
- Global flags: `--coordinator-addr`, `--api-key`, `--json`
- `GLOBALFS_COORDINATOR` / `GLOBALFS_API_KEY` environment variables
- Shell completions for bash, zsh, fish, and PowerShell

#### Object API endpoints and client (#8, #9, #10)
- REST endpoints: `GET/PUT/DELETE/HEAD /api/v1/objects/{key...}`, `GET /api/v1/objects`
- `pkg/client` Go client library with `Get`, `Put`, `Delete`, `Head`, `List`
- CLI subcommands: `object get/put/delete/head/list` with `--input`/`--output` file flags
- `--prefix` and `--limit` flags on `object list`

#### Site management API and CLI (#7)
- `POST /api/v1/sites` — register a site at runtime
- `DELETE /api/v1/sites/{name}` — deregister a site at runtime
- CLI: `site list`, `site add --name --uri --role`, `site remove --name`
- `GET /api/v1/sites` returns health-annotated site list
- `replicate --key --from --to` CLI and `POST /api/v1/replicate` API

#### API key authentication (#12)
- Middleware: `X-GlobalFS-API-Key` header validation
- `/healthz` and `/readyz` exempt from auth checks
- `--api-key` flag and `GLOBALFS_API_KEY` env var on both daemon and CLI

#### Request ID and access logging (#13)
- `X-Request-ID` middleware: echoes incoming ID or generates a UUID v4
- Access logging middleware: method, path, status, latency, request ID

#### Config CLI subcommand (#14)
- `config init [--output]` — writes starter YAML template
- `config validate <file>` — validates a config file
- `config show <file>` — prints the resolved configuration

#### Coordinator info endpoint (#15)
- `GET /api/v1/info` — version, uptime, site count, is_leader, queue depth, health summary
- `globalfs info [--json]` CLI command
- `globalfs status` — overall health check; exits non-zero on degraded primaries

#### Background health polling (#16)
- Background goroutine polls all sites at a configurable interval (default 30 s)
- Health results cached; `HealthStatus()` returns cached report without blocking
- First access falls back to live check when cache is not yet populated
- `--health-poll-interval` daemon flag; overridable via `resilience.health_poll_interval` in config

#### Health-aware routing (#17)
- `preferHealthySites` reorders the policy-routed site list so healthy sites appear first
- Degraded sites remain as fallback — never omitted — to handle stale cache
- Applied to both `Get` and `Head`

#### Circuit breaker (#18)
- Three-state per-site circuit breaker (`internal/circuitbreaker`): Closed → Open → HalfOpen
- `New(threshold, cooldown)`, `Allow`, `RecordSuccess`, `RecordFailure`, `State`, `Reset`
- Coordinator wiring: `SetCircuitBreaker`; `filterByCircuitBreaker` skips open circuits on reads
- All-open fallback: when every circuit is open the filter is bypassed so callers are never completely blocked
- Circuit breaker records success/failure for Put and Delete operations too
- Configuration: `resilience.circuit_breaker.{enabled,threshold,cooldown}`

#### Per-site retry (#19)
- Exponential back-off retry (`internal/retry`): `Do(ctx, Config, fn)`
- `Config{MaxAttempts, InitialDelay, MaxDelay, Multiplier}` with `Default` preset
- Applied to `Get` and `Head` only; writes are fail-fast by design
- Circuit breaker records a single failure only after **all** retry attempts are exhausted
- Configuration: `resilience.retry.{enabled,max_attempts,initial_delay,max_delay,multiplier}`

#### Resilience YAML wiring (#20)
- `pkg/config`: `CircuitBreakerConfig`, `RetryConfig`, `ResilienceConfig` structs
- `resilience:` section in `config.example.yaml` and config init template
- Coordinator daemon reads and applies all resilience settings at startup

#### In-memory LRU object cache (#21)
- Byte-budget LRU cache (`internal/cache`): `Get`, `Put`, `Delete`, `Invalidate`, `Stats`, `Len`
- Optional per-entry TTL with lazy expiry on access
- Read-through integration in coordinator `Get`; invalidation on `Put` and `Delete`
- Prometheus metrics: `globalfs_cache_{hits,misses,evictions}_total`, `globalfs_cache_bytes`
- Configuration: `cache.{enabled,max_bytes,ttl}`

#### Circuit state in sites API (#22)
- `SiteInfo.CircuitState string` (`json:"circuit_state,omitempty"`)
- `SiteInfos()` populates `CircuitState` from the circuit breaker when registered
- `GET /api/v1/sites` includes `circuit_state` per site
- `globalfs site list` shows a `CIRCUIT` column when any site has circuit state data

#### Documentation (#23)
- README rewritten with Overview, Architecture, Quick Start, Configuration Reference,
  CLI Reference, API Reference, and Development guide

### Changed

- `--health-poll-interval` flag default changed from `30s` to `""` so that
  `resilience.health_poll_interval` in config takes precedence when set

---

[0.1.0]: https://github.com/scttfrdmn/globalfs/releases/tag/v0.1.0
