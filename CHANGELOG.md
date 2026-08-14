# Changelog

All notable changes to GlobalFS are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

Work toward v0.3.0, whose theme is truth in reporting: a key, a metric, or a
documented behaviour must mean what it says. Several entries below are breaking,
batched into one release on purpose so that operators absorb the breakage once.

### Breaking

- `LoadFromFile` decodes with `KnownFields(true)`: an unknown or misspelled config key is now a startup failure naming the key and line, rather than being silently discarded. `listen_adrr: ":9000"` was previously accepted and the daemon bound the default port (#97)
- `global.metrics_port` removed. It implied a second listener that never existed; `/metrics` is served on the coordinator's own `listen_addr` (#98)
- `sites[].cargoship` and its validator removed. The validator *rejected* good-faith configs for a feature this repo never implemented and that objectfs deleted upstream in the version already depended on (#108)
- `performance.max_concurrent_transfers` renamed to `performance.replication_queue_depth`. It sized a channel buffer, never a worker pool — the replication run loop is a single goroutine. Renamed rather than made true: N consumers can reorder two Puts of one key, and seriality is currently what guarantees per-key ordering (#101)
- `policies[].sync_mode` removed from `config.example.yaml`. `ReplicationPolicy` never had such a field; it was discarded on every load (#97)
- The `202` body from `PUT /api/v1/objects/{key...}` replaces its `detail` string with a `pending_sites` list. `detail` held the server-side error verbatim; the destination names inside it were the only part a caller could use, and getting at them meant parsing formatted prose. `pkg/client` still reads `detail` when a pre-v0.3.0 coordinator sends it, so a new client works against an old server; a new server never sends it (#110)
- Storage-layer failures no longer return the underlying error text. `GET`, `PUT`, `DELETE`, `HEAD`, and `LIST` on `/api/v1/objects` answer `{"error": "upstream storage error", "request_id": "..."}`, and `SiteInfo.Error` on `GET /api/v1/sites` is the fixed string `health check failed`. Anything keyed off the old message text needs the status code and the log instead (#110)

Every removed key above is rejected by name under strict decoding, so an
un-migrated config fails loudly at startup instead of running with a setting
that does nothing.

### Fixed

- `coordinator.etcd_endpoints` is no longer required. Nothing reads it, and the shipped example told operators to leave it empty — which produced a config that would not load. Kept in the struct as the surface for the metadata store when it is wired (#106)
- `global.metrics_enabled: false` now stops `/metrics` from being registered, and `global.log_file` now opens the named file in append mode, falling back to stderr with a warning rather than refusing to start. Both were parsed and read by nothing (#98)
- `globalfs_sites_current` reported 0 for the entire life of an ordinary coordinator. It was written only by `AddSite`/`RemoveSite`, and sites supplied to `New` — every site in a config-file deployment — never pass through either. `SetMetrics` now publishes the count as it installs the registry (#102)
- `globalfs_replication_queue_depth` was written only after a job reached a terminal state, making every sample the low-water mark. It now publishes on the enqueue edge as well, including when the enqueue fails, since a full queue is the most important depth there is (#103)
- `Worker.DroppedTerminalEvents()` had no reader: a dropped terminal event was visible in the log at the moment it happened and nowhere afterwards, though each one is a job whose outcome the coordinator never learned. Now exported as `globalfs_replication_terminal_events_dropped_total`, published from the health-poll loop as well as the event drain so a coordinator that drops events and then goes quiet still reports it (#137)
- `Namespace.List` merges sites in key order instead of concatenating them, so `GET /api/v1/objects?limit=n` returns the namespace's first *n* keys. Previously the first site consumed the whole budget and a lexicographically-early key held only on a lower-priority site was unreachable at every limit — raising the limit raised the first site's contribution in lockstep. Listings are also stable between identical calls, which a concatenation of per-site results was not (#109)
- `pkg/client` bounds every response body it reads. Error bodies are capped at 4 KiB and truncated silently; control-plane responses at 1 MiB; object and list bodies by the new `WithMaxResponseSize` (default 1 GiB), where exceeding the cap wraps the new `ErrResponseTooLarge` rather than truncating. Previously a misconfigured or hostile endpoint could drive unbounded allocation in a CLI whose endpoint is a flag — a reverted-code check read 92 GB before the HTTP timeout intervened (#111)
- `client.WithTimeout` now applies whatever order the options are given in. It assigned into `c.httpClient.Timeout` as it ran, so a `WithHTTPClient` appearing after it replaced the whole client and discarded the timeout — silently, and in the direction that hangs (#111)
- `key_pattern` matching now uses [doublestar](https://github.com/bmatcuk/doublestar), so `**` crosses `/` as documented. It used `path.Match`, to which `**` is just two adjacent stars and therefore still barred from crossing a `/` — every recursive pattern the README and the shipped examples advertised matched nothing below the first level. A pattern that matches nothing is not an error, so the rule silently never fired and its objects were placed by whatever rule won instead (#100)
- An unparseable `key_pattern` or `path_pattern` is now rejected at config load, naming the rule, by both `Configuration.Validate` and `policy.NewFromConfig`. It previously failed the same way: `Match` returns an error, `matchesKey` reports no match, and the outcome is indistinguishable from a valid pattern that happens not to match (#100)
- The `policies:` patterns in `config.example.yaml` were rooted at `/`, and object keys have no leading slash, so none of them could ever match — independently of the `**` defect. Corrected, with the block now labelled as validated-but-never-read: nothing in the coordinator reads `cfg.Policies` (#100)
- `GET` and `HEAD /api/v1/objects/{key...}` return `404` for a key that is absent at every routed site, and `502` only when no site could answer. Both were `502`. #77 created `coordinator.ErrNotFound` to make the two distinguishable internally, but no handler ever mapped it — and collapsing them is what made `HEAD` worth probing for existence, since the probe was the only way to get the answer the status code should have given (#110)
- Site-layer error text no longer reaches API clients. An AWS SDK error names the bucket, region, and endpoint, so any failing request taught an authenticated caller the internal topology — how many sites, their names, and the storage behind each. The detail now goes to the coordinator log at `ERROR` (or `WARN` for the `207` partial-list and site-health paths, which previously logged nothing at all) against the request ID that the response carries in both the body and `X-Request-ID`. Site names themselves are kept where they were already public: `GET /api/v1/sites` still names sites, and the `202` still names the destinations that got no replication job (#110)
- `POST /api/v1/replicate` classifies a full queue with `errors.Is` against the new `replication.ErrQueueFull` rather than searching the coordinator's error text for `"queue full"`. A substring match on another package's message reclassifies the `503` as a `400` the moment that wording changes — telling the caller their request was malformed when it was in fact valid and retryable. This became load-bearing once the message stopped being echoed to the client, since the status code is then the entire answer (#110)
- `coordinator.Put` returns a typed `*ReplicationNotQueuedError` carrying `Key`, `Destinations`, and `Cause`. Both `cmd/coordinator` and `pkg/client` wanted the destination names, each documented in a comment that parsing another package's formatted error was a silent breakage waiting to happen, and each did it anyway. `Unwrap() []error` returns the sentinel and the cause, so `errors.Is(err, ErrReplicationNotQueued)` and tests against the underlying error both still hold (#110)

### Documentation

- Retracted the leader-election and standby-mode claims in the 0.1.0 entry and the `SetLeaseTTL` claim in 0.1.5, in place rather than by deletion. There is no leader election in any released version; `IsLeader()` returns a hardcoded `true`. New README section **Deployment topology: run exactly one coordinator** states the constraint and its consequence (#107)
- The Quick Start config now lives at `examples/quickstart.yaml` and is covered by CI's `config-examples` job. The version embedded in the README omitted `objectfs.mount_point` and so failed validation on the very next documented command; nothing caught it, because no test reads YAML out of a Markdown file (#96)
- Configuration Reference rewritten against the struct, with two tables of removed fields and their successors — the five deleted in v0.1.5 were still documented eleven releases later. Two new tests assert the README's field names resolve against `config.Configuration` by reflection, and that the removed names stay absent (#99)
- Objects API documentation corrected: the `202` partial-replication response and its `X-GlobalFS-Replication` header, the `{prefix, count, objects}` list envelope rather than a bare array, the `207` partial-list status and `X-GlobalFS-Partial`, `HEAD`'s `Content-Type` and `X-GlobalFS-Checksum`, and `DELETE`'s all-sites-confirmed contract (#104)
- `GET /api/v1/objects` now documents its key ordering and what a `limit` truncates, replacing the caveat that a `200` might silently omit keys — which #109 fixed (#104, #109)
- Dropped the README version stamp, which read `v0.1.0` at a repo tagged `v0.2.3`, in favour of pointing at the releases page and this file (#105)
- New Configuration Reference subsection **`key_pattern` syntax**, with a table of every supported form and an explicit statement that a single `*` stops at a `/` while `**` crosses it. `examples/coordinator-config.yaml`'s comment block gained the recursive forms it omitted (#100)
- API Reference documents the sanitized error shape, `request_id`, and where the withheld detail goes; status tables for `GET`, `PUT`, and `POST /api/v1/replicate`; the `404` on `GET`/`HEAD` and why `DELETE` of an absent key is still `204`; `pending_sites` in place of `detail`; and the fixed `health check failed` on `GET /api/v1/sites`. The `GET` section had been carrying a forward reference to #110 as an open issue (#110)
- Removed the pre-commit hooks section. No `.pre-commit-config.yaml`, git hook, or CI reference to it exists anywhere in the repo — the only occurrence of the word was the README's own claim that the hooks run automatically on `git commit`. Replaced with the commands CI actually runs

## [0.2.3] - 2026-08-13

**This release contains the v0.2.2 milestone as well as v0.2.3.** Both tranches
were developed against `main` and neither was tagged as it landed, so 0.2.2 is
skipped as a version number rather than tagged retroactively at a commit that was
never released. Entries are grouped by kind rather than by milestone, so read the
issue numbers: **#72–#81 is v0.2.2** — the worker panic, the two authorization
escapes, the truncated-read and write-timeout corruption pair,
not-found-as-site-failure, the two shutdown ordering defects, and the disagreeing
default ports. **#82–#95 and #130–#132 is v0.2.3** — the single-use lifecycle,
delete correctness, and cache coherence.

Twenty-seven issues, and the shape of them is worth stating: most were **success
reported for something that had not succeeded** — `Put` returning `nil` with the
write unreplicated, `Delete` returning `nil` with copies still readable,
`GetObject` returning a truncated body with a nil error, a cache serving pre-write
bytes indefinitely. None would have appeared in a log as an error. Two entries in
`### Changed` are therefore about callers: `Delete` now returns an error where it
returned `nil`, and `PUT /api/v1/objects` can answer 202.

### Added
- `.github/workflows/ci.yml`: continuous integration, which this repository has
  never had. Six jobs: `test` (build, vet, `go test -race`), `tidy` (go.mod/go.sum
  are what the source requires, and no `replace` points at a filesystem path),
  `cross-build` (all four goreleaser targets, both binaries, plus `go vet` to
  type-check the tests `go build` skips), `lint` (whole-repo gofmt, blocking;
  golangci-lint `--new-from-merge-base` on new code), `build-tags` (compiles the
  documented `integration` tag, which nothing type-checked), and
  `config-examples` (every shipped YAML plus the `config init` template through
  `globalfs config validate`). The absence of CI is why the objectfs import path
  below could break for five months while the test suite passed
- `pkg/config`: `TestLoad_TypesStructsBindFromYAML` and
  `TestShippedConfigsAreValid` — regression cover for the YAML binding defect
  below. Both fail on the pre-fix tree
- `pkg/config/config.go`: `SecurityConfig` (`security.allow_private_endpoints`,
  `security.allowed_endpoint_hosts`) — the escape hatch for the S3 endpoint
  validation below. Private address space is rejected unless explicitly
  permitted; loopback and link-local require a host on the exact-match
  allowlist (#76)
- `internal/coordinator/coordinator.go`: `ErrNotFound`, wrapping
  `objectfssdk.ErrNotFound`, returned by `Get`/`Head` when every routed site
  reports the key absent. `errors.Is` matches either sentinel. Not yet mapped to
  a 404 by the HTTP layer — that is #110 (#77)
- `internal/coordinator/coordinator.go`: `AddSiteUnique` and `ErrDuplicateSite`
  — checks the name and appends under a single lock hold, so racing callers
  cannot both claim one name. `AddSite` is retained for config load, which
  rejects duplicates earlier. Wired into `POST /api/v1/sites` as a 409 later in
  this release (#80, #131)
- `internal/coordinator/coordinator.go`: `ErrReplicationNotQueued` and
  `SetEnqueueBackpressure`, plus `globalfs_replication_dropped_total` in
  `internal/metrics` — a monotonic counter, because the queue-depth gauge
  returns to zero once a backlog clears and leaves no evidence it existed (#79)
- `internal/coordinator/coordinator.go`: `StopContext` and `CloseContext` —
  `Stop`/`Close` with a caller-supplied deadline, returning an error when the
  budget elapses before shutdown finishes. A non-nil error does not mean the
  coordinator is still running: the state transition, the worker's stop signal
  and both context cancellations all happen before anything is waited on. What
  it reports is that shutdown gave up *observing* the finish, so a transfer may
  be mid-flight or a site left unclosed (#83)
- `internal/coordinator/coordinator.go`: `ErrStarted` and `ErrStopped`, the two
  refusals of the single-use lifecycle contract below (#82, #84, #85)
- `internal/replication/worker.go`: `DroppedTerminalEvents()` — a monotonic
  count of terminal events abandoned because the events buffer stayed full for
  the whole emit budget. Each one is a job the coordinator never learns settled,
  so the count is exposed rather than only logged, where it would scroll away.
  Deliberately not a Prometheus counter: `internal/replication` does not import
  `internal/metrics`, and wiring it belongs to the coordinator that owns the
  registry — filed as a follow-up (#93)
- `pkg/client/client.go`: `ErrReplicationPending`, `ErrUnexpectedRedirect`, and
  `ErrInvalidKey`. `ErrReplicationPending` is an unusual contract worth stating
  plainly — it is a **non-nil error for a committed write**. A nil would claim
  durability the coordinator never claimed, which is the "success while
  something is wrong" shape this release is largely about, merely inverted. Test
  for it with `errors.Is` and do not retry the write (#130, #132)
- `cmd/coordinator/main.go`: `X-GlobalFS-Replication: pending` on a 202 from
  `PUT /api/v1/objects/{key...}`, following the `X-GlobalFS-Partial`/207
  precedent — a bare 202 is ambiguous, since `replicateHandler` returns one for
  ordinary success, and a header is what a proxy, an access log, or a metrics
  pipeline can act on without parsing a body (#130)
- `cmd/globalfs`: `object put --json` gains `replication_pending` and `detail`,
  both `omitempty`, so a fully replicated write serialises byte-identically to
  before and a caller who wants to gate on incomplete replication can (#130)
- `internal/coordinator`: `ErrDeleteIncomplete`, returned by `Delete` when the
  object may still be readable at a named site. A `Delete` that reported success
  while leaving copies behind was the defect; the sentinel is how a caller
  distinguishes "gone everywhere" from "gone from some places", and the error
  text names the survivors so the operator knows where to look (#87)
- `internal/cache`: `Generation()` and `PutIfUnchanged()` — the invalidation
  fence that makes a read-through fill safe against a write that lands while the
  fill is in flight. Every invalidation bumps a counter; a filler reads it before
  its remote read and inserts only if it has not moved (#89, #90)
- `internal/metrics`: `globalfs_delete_incomplete_total` and
  `RecordDeleteIncomplete()`. Monotonic, for the same reason
  `globalfs_replication_dropped_total` is: an incomplete delete is an object
  still readable through the API that just reported it gone, and that has to stay
  visible after a later retry succeeds and after the log line scrolls. Any
  non-zero value is a correctness event, and under a retention or erasure
  obligation a compliance one (#87)

### Security
- `cmd/coordinator/api.go`, `main.go`: path traversal crossed the authorization
  boundary. `validateObjectKey` rejects `..`, but `http.ServeMux` path-cleans
  the target and answers `307` *before* it selects a handler, so the validator
  never saw a literal `..`: `DELETE /api/v1/objects/../sites/primary` returned
  `307 Location=/api/v1/sites/primary`, and Go's HTTP client replays both the
  method and the `X-GlobalFS-API-Key` header on a 307 — so `pkg/client`
  following that redirect deregistered a site and reported success. A reverse
  proxy permitting `/api/v1/objects/` and denying `/api/v1/sites/` was bypassed
  the same way, seeing only the object path. Fixed with a `rejectUnsafePath`
  middleware that inspects `r.URL.EscapedPath()` before the mux can dispatch,
  rejecting with 400 rather than cleaning — cleaning would leave a traversal
  silently succeeding as a delete of a different key. Each segment is unescaped
  exactly once, so an encoded separator (`%2F%2E%2E%2F`) is caught while a
  legitimate literal `%2E` in a key (arriving as `%252E`) still routes. The
  guard sits inside the API-key check, so an unauthenticated probe gets 401 and
  cannot use the 400 as an oracle. `validateObjectKey` is kept as a second
  layer for callers that invoke handlers directly (#73)
- `cmd/coordinator/api.go`: `POST /api/v1/sites` signed a request to any host
  the caller named. `s3_endpoint` went unvalidated into a `HeadBucket`, so the
  coordinator sent its own credentials' SigV4 `Authorization` header to an
  attacker-chosen address — including `169.254.169.254` — and the 502 body
  echoed the transport error, distinguishing an open port from a closed one and
  making the endpoint a port scanner with an oracle. `validateS3Endpoint` now
  runs before any client is constructed: http/https origin only, no userinfo,
  no path or query, then every resolved address is checked, so a hostname
  pointing at internal space is caught too and one public A record does not
  license a second private one. The 502 body is now a constant string, with
  detail logged for the operator instead. **Known gap:** the interval between
  resolving and dialling is a DNS-rebinding window. Closing it requires pinning
  the connection to the checked address via a `DialContext` hook, and the
  objectfs SDK exposes no transport option — tracked upstream (#76)
- `pkg/client/client.go`: the client followed HTTP redirects, and Go's
  `http.Client` replays both the method and the `X-GlobalFS-API-Key` header on a
  307 — so a redirect became a second, differently-targeted, still-authenticated
  request. This is the client half of the #73 exploit above. It now refuses every
  redirect with an error wrapping `ErrUnexpectedRedirect`, chosen over
  `http.ErrUseLastResponse` because that surfaces a 307 as an ordinary `*APIError`
  every call site would have to recognise, whereas an error means the follow-up
  request is never built and the key never reaches the wire a second time. The
  policy is installed after the options so a client passed via `WithHTTPClient`
  is covered, on a shallow copy so the caller's own client is untouched and its
  Transport still pools, and a caller-set `CheckRedirect` is respected — this is a
  default, not a prohibition. Object keys are also validated locally against
  `ErrInvalidKey` before a request is sent. Since #73 closed the server side, this
  is hardening for pre-#73 and misconfigured-proxy deployments rather than a live
  hole. Not covered: `cmd/globalfs` has its own `httpClient` for two raw GETs to
  fixed paths that bypass `pkg/client` (#132)

### Fixed
- `internal/replication/worker.go`: `srcInfo.Checksum[:8]` sliced an unvalidated
  string to shorten a **log line**, so a checksum under 8 characters panicked
  with `slice bounds out of range [:8] with length 2` — and with no `recover`
  anywhere in the worker or coordinator, that log line killed the whole daemon.
  Replaced with a length-checked `shortChecksum`. Two `recover` layers added on
  top, because a malformed log argument taking down a daemon is a failure of
  containment rather than of that one line: `safeTransfer` converts a transfer
  panic into an ordinary attempt error so the existing retry and `EventFailed`
  path handles it, and `runJob` is a last-resort backstop that emits
  `EventFailed` before returning. That emission is load-bearing — the
  coordinator deletes a persisted job only on a terminal event, so a silent
  recover would strand the job and re-enqueue it on every restart. A recovered
  panic is logged at Error with its stack (#72)
- `pkg/client/client.go`: `GetObject` was `return io.ReadAll(resp.Body)`, so a
  read that failed mid-body returned partial content next to the error and the
  CLI, having checked `err` at a different layer, wrote a short file and
  reported success — 42 MB of a 64 MB object. The partial slice is now
  discarded and the error wrapped with the key and the byte count, plus a
  `Content-Length` cross-check as an independent assertion that does not depend
  on the transport reporting the truncation. Note the original diagnosis was
  wrong in mechanism: `net/http` does surface a short `Content-Length`-delimited
  body as `unexpected EOF`; the information was present at every layer and the
  function signature is what let it be discarded. A chunked or HTTP/1.0
  close-delimited short body remains undetectable here, asserted honestly by
  `TestGetObject_NoContentLength_ShortBodyUndetected` so a future streaming path
  has to confront it (#74)
- `cmd/coordinator/main.go`, `api.go`: `WriteTimeout` is an absolute deadline on
  the whole response, not an idle timeout, so it truncated large object GETs
  mid-body; the same arithmetic made the documented 256 MiB PUT cap unreachable,
  needing 25.6 MiB/s sustained. The strict server-wide deadlines are kept —
  they are correct for the JSON control endpoints — and the object handlers now
  replace them per request via `http.ResponseController`, sized
  `clamp(bytes / 1 MiB/s, 30s, 10m)`. The rate is set by the constraint that the
  advertised size cap must be reachable, not by taste, and
  `TestTransferDeadline_SizeCapIsReachable` fails if the two drift apart. PUT
  extends both deadlines: `net/http` arms them together when the request headers
  finish, so a slow upload otherwise leaves no time to send the 201 it earned
  (#75)
- `internal/coordinator/coordinator.go`: a missing object counted as a site
  failure, so five lookups of absent keys ejected a healthy site from routing
  for the whole breaker cooldown — each after three retries. There was no
  `errors.Is(err, ErrNotFound)` anywhere in the tree. All five breaker call
  sites now route through `recordSiteResult`/`isSiteFailure`, which delegates
  the classification to objectfs's own `IsServiceFailure` so GlobalFS cannot
  drift from the layer below. A non-failure error records a *success*: a site
  that answers "no such key" was reached, authenticated, and answered
  correctly, which is exactly what the breaker measures. `context.Canceled` is
  named explicitly because `retry.Do` returns a bare `ctx.Err()` carrying no
  objectfs code, and an unclassified error counts as a failure — a breaker that
  opens too eagerly recovers on its cooldown, one that never opens does not.
  objectfs added its `ProbeAfter` mechanism after this same bug took a mount
  permanently offline one layer down (#77)
- `internal/coordinator/coordinator.go`: `Stop()` cancelled the event drain
  before stopping the worker, so terminal events emitted during shutdown were
  lost — leaving a phantom job in the store and discarding the dedup hash.
  Teardown is now producer (`worker.Stop`) → consumer (cancel, `storeWg.Wait`) →
  a final non-blocking `flushWorkerEvents` on the calling goroutine.
  **Reordering alone was not sufficient**, which is why the flush exists:
  `cmd/coordinator` cancels the root context and *then* calls `Close`, so the
  drain can already have exited on `ctx.Done` before `Stop` is entered, and the
  same is true after a lost leader lease. `drainWorkerEvents` also flushes on
  its own `ctx.Done`. Verified by disabling the flush while keeping the
  corrected order — `TestCoordinator_Stop_DrainFlushesBufferedEvents` still
  fails (#78)
- `internal/coordinator/coordinator.go`: a full replication queue was logged at
  warn level and `Put` returned nil, so callers were told a write was
  replicated when it was not — 31 of 40 Puts dropped at the shipped defaults,
  every one reporting success. `Put` now applies bounded backpressure (2s
  budget, 5ms poll, configurable via `SetEnqueueBackpressure`) and then returns
  an error wrapping `ErrReplicationNotQueued`, documented explicitly as a
  *partial success*: the bytes are durable on every primary, the named
  secondaries do not have them, and retrying the identical Put is safe because
  the primary write is idempotent and content-hash dedup skips destinations
  that already hold the content. The budget is finite on purpose — unbounded
  blocking would make the async path synchronous and let one wedged destination
  stall every writer. The HTTP layer initially mapped every `Put` error to 502,
  reporting this partial success as a gateway failure; that is corrected to 202
  Accepted later in this release (#79, #130)
- `internal/coordinator/coordinator.go`: `RemoveSite` had no `break`, so with
  duplicate site names it kept only the last match, filtered out all of them,
  and closed exactly one — leaking the others' connection pools. It now splices
  out the highest-priority match by index and closes that site. The close stays
  outside the lock, now with a comment saying why, so the next edit does not
  move it — `Close` is brought into line with it later in this release (#80, #95)
- `pkg/config/config.go`, `cmd/coordinator/main.go`, `cmd/globalfs/main.go`,
  `config.example.yaml`, `README.md`: the daemon defaulted to `:8080` and the
  CLI to `:8090`, so out of the box they could not talk. Unified on `:8090` via
  `config.DefaultListenPort` with `DefaultListenAddr` and
  `DefaultCoordinatorURL` derived from it, so the two cannot drift again.
  `config.example.yaml` also said `:8080` — worse than the reported symptom,
  since copying the shipped example reproduced the bug *with* an explicit config
  file — and is covered by `TestShippedConfigs_ListenAddrMatchesDefault` (#81)
- `pkg/types/types.go`: `ReplicationPolicy`, `CoordinatorConfig`, and
  `PerformanceConfig` carried only `json` tags, but `pkg/config` decodes all
  three from YAML. yaml.v3 falls back to the lowercased field name when a tag is
  absent, so `path_pattern`, `listen_addr`, `etcd_endpoints`, `lease_timeout`,
  and `max_concurrent_transfers` bound to nothing and every field silently
  stayed at its zero value — after which `LoadFromFile`'s caller overwrote them
  with defaults. An operator setting `listen_addr: ":9000"` got `:8080`, and a
  `lease_timeout` or `max_concurrent_transfers` was discarded outright while
  `globalfs config show` reported the file as loaded. Added `yaml` tags to all
  three. Found by the `config-examples` CI job on its first run:
  `config.example.yaml` — the file README.md tells users to copy — failed
  `globalfs config validate` with `policies[0].path_pattern is required` while
  plainly containing one
- `internal/coordinator/coordinator.go`, `internal/replication/worker.go`:
  `Stop()` racing `Start()` permanently leaked the drain and health goroutines —
  59 of 60 iterations leaked a pair. The cause was a missing mutual-exclusion
  invariant rather than a missing nil check: `Start` has to release `c.mu` across
  the lease acquisition and `recoverPendingJobs`, and a `Stop` that slipped into
  that window saw `storeCancel` still nil and `storeWg` still zero, concluded
  there was nothing to stop, and returned — after which `Start` launched a drain
  goroutine and a health poller under a context nothing would ever cancel. A
  `lifecycleMu` now serialises `Start` against `Stop` for their whole duration
  (#82)
- `internal/replication/worker.go`: `Stop()` before `Start()` permanently
  disabled replication, and nothing said so. `Stop` was `w.once.Do(func() {})` —
  a no-op whose only purpose was burning the `sync.Once` so a later `Start` could
  not run — while the doc comment one line above claimed "Calling Stop before
  Start is safe." The coordinator's other goroutines still launched, so the
  daemon looked healthy with replication dead. Replaced with an explicit
  `created → running → stopped` state machine on both Worker and Coordinator:
  Stop-before-Start is legal *and terminal*, and `Start` on a stopped instance
  is refused with an error wrapping `ErrStopped` (#84)
- `internal/coordinator/coordinator.go`: `SetWorkerQueueDepth` after `Start`
  replaced `c.worker` while documenting that it did not, orphaning the running
  worker and ending all replication. The six configuration setters now return an
  error under a freeze-at-Start rule — `Start` snapshots configuration in one
  lock hold and every setter writing those fields refuses afterwards, with a
  negative-control test pinning which setters are gated so the gate cannot
  quietly grow. An error rather than genuine reconfiguration: the depth *is* a
  channel's buffer, and a channel's buffer cannot be resized, so
  "reconfigurable" would mean a second channel plus either discarding queued jobs
  or one worker serving two queues — a feature, not a setter (#85)
- `internal/coordinator/coordinator.go`: `c.m` was read without the lock while
  `SetMetrics` wrote it under `c.mu`. Reads now go through a `metrics()`
  accessor, with call sites that already hold the lock using an explicit
  `metricsSiteCountLocked` (#86)
- `internal/coordinator/coordinator.go`, `internal/replication/worker.go`,
  `cmd/coordinator/main.go`: `Stop()`/`Close()` waited forever on an unresponsive
  site, so SIGTERM never completed. **The filed diagnosis was partly wrong** and
  is worth recording: `Health` does impose `defaultHealthTimeout` on its context,
  so the real question was whether `site.Health` honours a context at all.
  objectfs v0.12.0's `ConnectionPool.Get()` is hard-coded to
  `GetWithTimeout(30s)` and takes **no context**, so site probes genuinely ignore
  deadlines and both layers needed fixing: bounded waits and a partial `Health`
  report in the coordinator, and an explicit deadline at the daemon's call site.
  The shutdown context derives from `context.Background()`, *not* from the
  daemon's already-cancelled root — a context derived from a cancelled parent is
  born cancelled, so passing one to `CloseContext` would make every bounded wait
  return immediately, abandoning the in-flight transfer with its terminal event
  unemitted and the drain already gone, which is precisely the phantom job #78
  fixed. `newShutdownContext` is a named function so that derivation is testable,
  and the test pins both directions (#83)
- `internal/coordinator/coordinator.go`: `Close()` held `c.mu` across per-site
  network teardown, blocking every method that touches the site set — including
  `Sites()` and the health endpoint — for as long as a connection-pool drain
  took. Sites are now closed outside the lock, as `RemoveSite` already did (#95)
- `cmd/coordinator/main.go`: five lifecycle errors were discarded at boot. The
  serious one is `c.Start`, which under the new contract can refuse: the daemon
  would have gone on serving HTTP with no replication worker and no health
  poller, `/healthz` answering from a cache nothing refreshed, and nothing in the
  log saying why. The four configuration setters are fatal too, via
  `mustConfigure`: they all run before `Start` where the contract is to return
  nil, so a non-nil error means a setter moved below `Start` — a bug in the boot
  order, not in the operator's file. Fatal rather than logged because the value
  came from the config file, so `config show` would keep reporting it while the
  daemon ran on the default, which is the reported-versus-effective divergence
  #81 and the YAML-tag bug both produced (#83, #84)
- `internal/replication/worker.go`: the events channel was sized at the job queue
  depth while `processJob` emits two events per job, so terminal events were
  dropped past half depth — at the shipped default depth of 8, an 8-job burst
  delivered terminal events for exactly 4. A dropped terminal event is not
  cosmetic: the coordinator deletes the durable job record and writes the dedup
  content hash on that event, so losing it strands a phantom job that is
  re-enqueued on the next restart and loses the hash. Sizing alone was rejected
  as insufficient — it moves the threshold and the loss stays silent — so three
  changes: the buffer is `depth*eventsPerJob + eventBufferSlack`; `EventStarted`
  can no longer occupy the last slot, so the harmless half of the pair cannot
  displace the harmful half; and terminal events wait for room under a 10s
  budget. That wait deliberately ignores `w.done` and the job context, since both
  mean "stop working" and a terminal event is the record that the work already
  finished. The budget is finite because an unbounded send would let a wedged
  consumer wedge the worker that `Stop` waits on. A still-lost terminal event is
  logged at Error naming both consequences, and counted (#93)
- `cmd/coordinator/api.go`, `pkg/client/client.go`, `cmd/globalfs/main.go`: a
  write that reached every primary but could not queue replication returned 502,
  telling callers to retry a write that had already committed and making a queue
  backlog look like an outage to anything alerting on 5xx. Now 202 with a
  `X-GlobalFS-Replication: pending` header and a distinct body type — not
  `errorResponse`, since a client keying off `{"error": ...}` would read the
  success as a failure, which is this defect moved from the status code into the
  body. **The status code alone did not fix it**: `pkg/client.PutObject` matched
  201 exactly, so the new 202 arrived at the only first-party consumer as an
  `*APIError` and `globalfs object put` printed a committed write as a failure.
  The client now returns `ErrReplicationPending` and the CLI reports success with
  a caveat on stderr at exit 0 — a non-zero exit is the only signal `set -e` and
  CI runners read, and would relocate the same harm into the exit code. A
  round-trip test drives the real handler and the real client over a real socket,
  which is what per-side stubs could not catch (#79, #130)
- `cmd/coordinator/api.go`: `POST /api/v1/sites` still accepted a duplicate site
  name. It now uses `AddSiteUnique` and returns 409. Two details matter: the
  rejected mount is closed, because `AddSiteUnique` deliberately leaves it open
  and the S3 connection pool `site.NewFromConfig` opened would otherwise leak —
  #80's leak arriving by a new route — and a name check runs before endpoint
  validation and `NewFromConfig`, so a duplicate no longer costs a DNS lookup and
  a signed `HeadBucket` against a caller-supplied endpoint. That pre-check races
  by construction and says so; `AddSiteUnique` under the lock remains
  authoritative, verified by 16 concurrent registrations that all see the name
  free (#131)
- `internal/cache`, `internal/coordinator`: the read-through cache served bytes a
  write had already replaced, and there was no bound on how long. A read-through
  fill is two steps with a network round-trip between them, so a reader parked
  after its site read and before its cache write repopulated the entry *after* a
  concurrent `Put` or `Delete` had invalidated it — and the invalidation found
  nothing to remove, because the entry did not exist yet. With `Cache.TTL`
  defaulting to `0` the resurrected entry never expires, so it was served until
  LRU pressure happened to evict it or the process restarted. Fixed with a
  generation fence: every invalidation bumps a counter, `Get` reads it *before*
  the site read, and the fill goes through `PutIfUnchanged`, which compares and
  inserts under one lock acquisition. The load-bearing detail is that
  `Cache.Delete` bumps the counter **whether or not the key was present** — the
  case that matters is exactly the one where it is absent because a fill for it is
  still in flight. That also meant `Put` and `Delete` needed no change, which is
  what let this land alongside the delete work below without the two colliding.
  The `TTL=0` half is deliberately *not* fixed: see Changed (#89, #90)
- `internal/coordinator/coordinator.go`: `Delete` returned `nil` while the object
  was still readable. Failures at non-primary sites were logged and discarded, and
  — not in the filed report — the primary loop returned on the *first* error, so
  every replica was left untouched: the failure mode maximised the number of
  surviving copies of an object the caller had asked to be erased. `Delete` now
  attempts every routed site and returns an error wrapping `ErrDeleteIncomplete`
  naming the survivors, with `globalfs_delete_incomplete_total` alongside it.
  A site-level not-found counts as *gone*, which is what keeps the fix idempotent:
  without it, every retry of an already-completed delete would report incomplete
  forever and "retry the same Delete" would not be usable advice. Any other error
  means "may still be there", because a refused delete and a delete that succeeded
  but failed to acknowledge are indistinguishable from here, and assuming survival
  is the direction that reports a problem instead of hiding one (#87)
- `internal/coordinator/coordinator.go`: `Put`'s error paths skipped cache
  invalidation. Primaries are written sequentially and the first failure returns
  immediately, so a `Put` that reported an error could still have mutated an
  earlier primary while the cache went on serving the pre-`Put` value — with
  `TTL=0`, for the life of the process. The invalidation is now a `defer` taken
  before the first site is touched, which covers the early returns by construction
  rather than by remembering to duplicate it above each new `return` — and `Put`
  gained another error path this same release (#79's partial success). `Delete`
  got the same treatment for the same reason. Invalidating more than necessary is
  the safe direction: a spurious invalidation costs one cache miss, a missed one
  serves stale data indefinitely (#91)
- `internal/replication/worker.go`: a transfer already in flight re-created an
  object that had been deleted everywhere. `transfer` read the source and wrote
  the destination with nothing in between that could notice, so a delete landing
  in that span was undone at a site the operator was not watching — and
  `Coordinator.Get` then served the resurrected replica, because any replica
  satisfies a read. The source is now re-checked immediately before the
  destination PUT, which narrows the window from the whole GET→PUT span (minutes
  for a large object, or the queue wait plus two retry backoffs) to the Head→Put
  gap. The guard is deliberately asymmetric: only an unambiguous code-matched
  not-found abandons the transfer, because treating an unreachable source as a
  delete would silently halt replication for the duration of any source-side
  incident. An abandoned transfer settles `EventCompleted` with **no** content
  hash — recording one would make the dedup index claim the destination holds
  content it does not, which is the same class of quiet defect one layer over.
  The residual Head→Put window stays open by necessity; see Changed (#92)
- `internal/coordinator/coordinator.go`: the circuit breaker leaked HalfOpen probe
  permits, ejecting recovered sites from read routing for the life of the process.
  `filterByCircuitBreaker` called `Allow` for every candidate, but `Get` and `Head`
  use only the first site that answers — and `Allow` is not a predicate: on a
  HalfOpen circuit it *takes* the single probe permit, which only a recorded
  outcome releases. `List` was worse and was not in the filed report: it acquired
  for every candidate and recorded nothing for any of them. Filtering now tests
  `State`, which consumes nothing, and `Get`/`Head` range over an
  `attemptableSites` iterator that takes each permit as it yields the site, paired
  1:1 with the `recordSiteResult` that releases it. `List` reads breaker state and
  acquires nothing at all: `namespace.List` folds per-site failures into one
  joined error, so it cannot attribute an outcome to a site without
  reimplementing the merge — and excluding a HalfOpen site would silently truncate
  the namespace, the one failure mode a listing must not have. Two traps recorded
  because neither is visible from the fix: splitting acquisition from filtering
  makes them two decisions at different moments, so without a second pass when
  every candidate refuses, a read can attempt *no* site and return not-found — a
  404 for data that exists, worse than the leak. And `State` cannot detect the
  leak it diagnoses, since it persists the Open→HalfOpen transition itself and
  reads `HalfOpen` for a healthy probe-eligible site and a stranded one alike;
  that is why nothing in `/api/v1/sites` ever looked wrong (#94)
- `internal/metadata/etcd_store.go`: removed `replicatedPrefix`, which was
  never called and duplicated a string literal already inlined at its one
  would-be call site
- `go.mod` and 10 Go files: the objectfs import path is now
  `github.com/scttfrdmn/objectfs`, the path the module actually declares and
  publishes. It was `github.com/objectfs/objectfs`, which objectfs itself
  renamed away from on 2026-08-02 — and which is a real, unrelated Python
  project owned by someone else, so anyone reading these import paths to find
  the source landed on a stranger's repository. `go build ./...` failed at
  module resolution before compiling anything.
- `go.mod`: removed both `replace` directives. They pointed at `../objectfs`
  and `../cargoship`; the latter does not exist, which is what surfaced the
  breakage, and the former resolved to a directory declaring a different module
  path. Neither is needed — objectfs and cargoship are both public and tagged,
  so the build now resolves entirely from the module proxy with no local paths.

### Changed
- **`Coordinator.Delete` now returns an error where it used to return `nil`.** A
  delete that leaves the object readable at any routed site reports
  `ErrDeleteIncomplete` and names the survivors. Callers that treated a nil from
  `Delete` as "gone" were getting an answer the coordinator could not support;
  callers that treat *any* error as "nothing happened" now need to distinguish,
  because a partial delete removed real copies and retrying is both safe and the
  intended response. `DELETE /api/v1/objects/{key...}` maps this to 502 for now,
  which is not wrong but says less than it could — a partial delete with named
  sites is not the same as a delete that achieved nothing (#87)
- **Two known gaps are left open rather than papered over.** `Cache.TTL` still
  defaults to `0`, so a cache entry has no expiry. The generation fence closes the
  race that put stale bytes there, so this is no longer a correctness bug — but a
  non-zero default would bound the damage from any *future* staleness defect, and
  unboundedness is what made #89/#90 severity-high rather than a transient
  inconsistency. Recommended: 5–15 minutes. Changing a shipped default is an
  operator-visible decision, so it is not made here. Separately, #92's Head→Put
  window still exists: closing it needs `Delete` to invalidate queued replication
  jobs, which needs a durable record of the delete that no shipped deployment has
  (`SetStore` still has no non-test callers). Both are documented at the code so
  the next reader does not assume they are handled (#89, #90, #92)
- **`Coordinator.Delete`'s burst-only fix came from elsewhere than filed.** #88
  asked for `Put`'s promote-the-first-non-primary rule to be shared with `Delete`,
  and it now is, as `partitionForWrite`. But that promotion is *behaviourally
  inert* in the rewritten `Delete`, which concatenates primaries and others into
  one target list and reports every site's outcome identically — reverting it
  breaks no test. The burst-only symptom is genuinely fixed; it is fixed by #87's
  change. The shared helper is kept on its original rationale rather than a
  correctness one: `Put` grew the promotion with a comment explaining why it was
  necessary, `Delete` never did, and neither call site showed the omission on its
  own. `Put` still depends on it for real (#87, #88)
- **Coordinator and Worker are now single-use.** The lifecycle is
  `created → running → stopped`: `Start` is idempotent, `Stop` before `Start` is
  legal but terminal, and `Stop`-then-`Start` is refused with `ErrStopped`.
  Configuration is frozen at `Start` — six setters return an error afterwards
  instead of silently taking effect or, worse, orphaning the running worker. This
  makes #82 and #84 invalid by construction rather than defensively patched. A
  process that needs to restart replication should exit and let its supervisor
  start a fresh coordinator, which is the recovery a single-use lifecycle allows
  (#82, #84, #85)
- **Worst-case time from SIGTERM to exit is now 60 seconds**, in two sequential
  30s windows: the HTTP drain, then coordinator teardown. Set the termination
  grace period of whatever supervises the daemon above 60s — below it, SIGKILL
  preempts the bounded shutdown and none of #83's work runs. A shutdown that
  exhausts its budget exits non-zero: the process is terminating either way, but
  a transfer abandoned mid-flight or a site left unclosed is not a clean
  shutdown, and an orchestrator reading `$?` is entitled to the difference (#83)
- **`PUT /api/v1/objects/{key...}` can now answer 202**, which is a new response
  code for that endpoint. A pre-v0.2.3 `pkg/client` build talking to a v0.2.3
  coordinator matches 201 exactly, so it reports a committed write as
  `coordinator error (202)` and may re-upload data that is already stored. Not a
  correctness problem — the write is durable and the retry is idempotent — but a
  spurious failure and a wasted upload. Client and coordinator are versioned
  together, though nothing enforces that they are *deployed* together: a pinned
  dependency in a downstream Go program, or a stale `globalfs` binary on an
  operator's laptop, is enough to hit this (#130)
- objectfs dependency upgraded v0.10.0 → v0.12.0 (two minor releases accrued
  while GlobalFS was dormant; the module rename above shipped among them). The
  `ObjectInfo.Checksum` fast path in `internal/replication/worker.go` is
  unaffected. Transitive AWS SDK, gRPC, and Prometheus dependencies moved
  forward with it.
- `go.mod`: added `toolchain go1.26.5`. The CI workflow passes
  `go-version-file: go.mod` to setup-go, which reads the `go` line as an exact
  version spec — so without this every job, including release builds, would
  compile against the go1.26.0 standard library and the advisories fixed in
  1.26.3 through 1.26.5. objectfs pins the same version for the same reason
- `gofmt -w .` across 12 files: 44 lines of comment alignment and five one-line
  metrics wrappers gofmt wants expanded. Whitespace only — the suite passes
  unchanged — done here so the new gofmt gate can be whole-repo and blocking
  rather than scoped to changed files

## [0.2.1] - 2026-02-24

Tagged and released without a section here; recorded retroactively at 0.2.3 so the
file agrees with the tag list.

### Changed
- objectfs dependency updated v0.9.0 → v0.10.0 — parallel range GETs, a
  chunk-aware cache, and the content SHA-256 metadata that 0.2.0's replication
  fast path reads. Worth recording precisely, because 0.2.0's own notes said it
  "depends on objectfs v0.10.0" while its `go.mod` pinned v0.9.0: the checksum
  fast path was documented as available one release before the dependency
  providing it was required. It degraded to the full GET → PUT rather than
  failing, which is why nothing caught it
- **Neither 0.2.0 nor 0.2.1 was installable.** Both tags carry
  `replace github.com/objectfs/objectfs => ../objectfs`, so `go install` of either
  fails outside a working tree with a sibling objectfs checkout. The module path
  was also a repository belonging to someone else. Both are fixed in 0.2.3, which
  is the first release resolvable entirely from the module proxy — see the CI and
  import-path entries above

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
- `CoordinatorConfig.LeaseTimeout` is now consumed: the coordinator daemon calls `SetLeaseTTL` at startup, configuring the distributed leader-lease TTL (#50) — *correction added in v0.3.0 (#107): the call happens, but the TTL it sets is only read when acquiring the leader lease, and no lease manager is ever registered, so the value has no observable effect. See the 0.1.0 note under "Distributed lease manager".*
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

> **Correction, added in v0.3.0 (#107).** The two entries below describe code that
> landed in the tree and was never connected. `SetLeaseManager` has no non-test
> callers, so `c.leaseManager` is always nil and `IsLeader()` returns a hardcoded
> `true`: **there is no leader election and no standby mode in any released
> version.** Two coordinators pointed at the same buckets both believe they are
> leader, both accept writes, and both replicate, with nothing anywhere able to
> detect the resulting divergence.
>
> The lines are annotated rather than deleted, because anyone who has already read
> them holds a belief that needs correcting, and a silent removal reaches nobody.
> The same applies to the "etcd v3 implementation for production persistence" entry
> under **Metadata store** above: the code exists, nothing constructs it. Whether to
> wire it or remove it is issue #112.
> The README's "Deployment topology" section is the current statement.

- etcd-backed distributed lease manager (`internal/lease`)
- `TryAcquire`, `KeepAlive`, `Release` API
- ~~Coordinator leader election: only the lease holder starts the replication worker~~ — never wired; see the correction above
- ~~Standby coordinator mode when another instance holds the lease~~ — never wired; see the correction above

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

[Unreleased]: https://github.com/scttfrdmn/globalfs/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/scttfrdmn/globalfs/compare/v0.2.1...v0.2.3
[0.2.1]: https://github.com/scttfrdmn/globalfs/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/scttfrdmn/globalfs/compare/v0.1.12...v0.2.0
[0.1.12]: https://github.com/scttfrdmn/globalfs/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/scttfrdmn/globalfs/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/scttfrdmn/globalfs/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/scttfrdmn/globalfs/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/scttfrdmn/globalfs/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/scttfrdmn/globalfs/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/scttfrdmn/globalfs/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/scttfrdmn/globalfs/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/scttfrdmn/globalfs/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/scttfrdmn/globalfs/compare/v0.1.0...v0.1.3
[0.1.0]: https://github.com/scttfrdmn/globalfs/releases/tag/v0.1.0
