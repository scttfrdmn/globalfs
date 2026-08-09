package main

// api.go — REST API handlers mounted under /api/v1/
//
// GET    /api/v1/info                 → coordinator runtime information
// GET    /api/v1/sites                → list all sites with health
// POST   /api/v1/sites                → register a new site
// DELETE /api/v1/sites/{name}         → deregister a site
// POST   /api/v1/replicate            → trigger manual replication
//
// GET    /api/v1/objects              → list objects (?prefix=&limit=)
// GET    /api/v1/objects/{key...}     → get object data
// PUT    /api/v1/objects/{key...}     → store object data
// DELETE /api/v1/objects/{key...}     → delete object
// HEAD   /api/v1/objects/{key...}     → object metadata headers

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/internal/metrics"
	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// apiKeyHeader is the HTTP request header checked for authentication.
const apiKeyHeader = "X-GlobalFS-API-Key"

// maxObjectBodyBytes is the maximum number of bytes accepted for a PUT object
// request body.  Requests that exceed this limit are rejected with 413 to
// prevent memory exhaustion from unbounded reads.
const maxObjectBodyBytes = 256 * 1024 * 1024 // 256 MiB

// maxJSONBodyBytes is the maximum number of bytes accepted for JSON request
// bodies (site registration, replicate).  1 MiB is far more than any valid
// payload requires.
const maxJSONBodyBytes = 1 << 20 // 1 MiB

// validateObjectKey returns an error if key contains path-traversal characters
// (null bytes or ".." components) that could bypass bucket prefix boundaries.
//
// This is the second of two layers and it is not the load-bearing one.  It runs
// inside the handler, on the key http.ServeMux has already extracted, and by
// then the mux has path-cleaned the request — a literal ".." never reaches here
// at all (see rejectUnsafePath for what does).  Keep it: it still catches a
// traversal that arrives through the decoding of an escaped separator
// ("%2F%2E%2E%2F" reaches the handler as key "/../sites/..."), and it guards
// callers that invoke the handlers directly rather than through the mux.
func validateObjectKey(key string) error {
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("key contains null byte")
	}
	for _, part := range strings.Split(key, "/") {
		if part == ".." {
			return fmt.Errorf("key contains path traversal component")
		}
	}
	return nil
}

// ── Path traversal guard ──────────────────────────────────────────────────────

// hasUnsafePathSegment reports whether escapedPath contains a ".." path
// component or a null byte, considering both the literal and the
// percent-encoded spelling of each.
//
// It takes the *escaped* path (r.URL.EscapedPath()) rather than r.URL.Path,
// because r.URL.Path is already percent-decoded: "%2E%2E" and ".." are
// indistinguishable there, and only one of the two survives the mux.  Each
// segment is unescaped exactly once and then re-split on "/", so an encoded
// separator ("%2F%2E%2E%2F") is caught as well.
//
// Decoding once and no more is deliberate.  A key containing a literal "%2E"
// is legal in S3 and arrives as "%252E"; unescaping twice would reject it.
func hasUnsafePathSegment(escapedPath string) bool {
	for _, seg := range strings.Split(escapedPath, "/") {
		// Fast path: no escaping to consider.
		if !strings.Contains(seg, "%") {
			if seg == ".." {
				return true
			}
			continue
		}
		dec, err := url.PathUnescape(seg)
		if err != nil {
			// Malformed percent-encoding.  net/http normally rejects this before
			// a handler runs; treat it as hostile rather than guessing.
			return true
		}
		if strings.Contains(dec, "\x00") {
			return true
		}
		for _, part := range strings.Split(dec, "/") {
			if part == ".." {
				return true
			}
		}
	}
	return false
}

// rejectUnsafePath rejects requests whose raw path contains a ".." component
// before http.ServeMux can dispatch them.
//
// This has to be middleware; it cannot live in a handler.  ServeMux path-cleans
// the target and answers with a 307 *before* it picks a handler, so
// validateObjectKey never sees a literal "..":
//
//	DELETE /api/v1/objects/../sites/primary        -> 307 Location=/api/v1/sites/primary
//	DELETE /api/v1/objects/a/b/../../../sites/x    -> 307 Location=/api/v1/sites/x
//	DELETE /api/v1/objects/data/..                 -> 307 Location=/api/v1/objects/
//
// Go's http.Client replays both the method and the X-GlobalFS-API-Key header on
// a 307, so pkg/client following that first redirect turns an object-scoped
// DELETE into a site deregistration and reports success (#73).  A reverse proxy
// that allows /api/v1/objects/ and denies /api/v1/sites/ is bypassed the same
// way, because the proxy only ever sees the object path.
//
// Rejecting rather than cleaning is the point.  Cleaning would keep the third
// case above silently succeeding as a delete of a different key, and "data/.."
// is a legal S3 key this server cannot route — 400 says so, a 307 does not.
//
// The guard applies to every path, not just /api/v1/objects/: no route this
// server exposes has a legitimate ".." component, and confining it to one
// prefix would mean deciding which prefix *after* the mux had already decided
// for us.
func rejectUnsafePath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasUnsafePathSegment(r.URL.EscapedPath()) {
			slog.Warn("api: rejected request with unsafe path",
				"method", r.Method,
				"raw_path", r.URL.EscapedPath(),
				"request_id", requestIDFromCtx(r.Context()),
				"remote_addr", r.RemoteAddr,
			)
			if r.Method == http.MethodHead {
				// HEAD must not return a body.
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeError(w, http.StatusBadRequest,
				"invalid request path: path traversal component not permitted")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── API key middleware ────────────────────────────────────────────────────────

// apiKeyMiddleware returns an HTTP middleware that enforces API key auth.
// Requests must carry the correct key in the X-GlobalFS-API-Key header.
// The /healthz and /readyz endpoints are always exempt (for health probes).
// When key is empty the middleware is a no-op — auth is disabled.
func apiKeyMiddleware(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			// Always allow health/readiness probes (no auth needed by load balancers).
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
				next.ServeHTTP(w, r)
				return
			}
			if subtle.ConstantTimeCompare([]byte(r.Header.Get(apiKeyHeader)), []byte(key)) != 1 {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ── Request ID middleware ─────────────────────────────────────────────────────

// requestIDHeader is the HTTP header used to propagate the request correlation ID.
const requestIDHeader = "X-Request-ID"

// requestIDCtxKey is the unexported context key for the request ID value.
type requestIDCtxKey struct{}

// requestIDFromCtx returns the request ID stored in ctx, or "" if not set.
func requestIDFromCtx(ctx context.Context) string {
	id, _ := ctx.Value(requestIDCtxKey{}).(string)
	return id
}

// generateRequestID produces a 16-character hex string from crypto/rand.
// Falls back to a nanosecond timestamp if entropy is unavailable.
func generateRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", b)
}

// requestIDMiddleware ensures every request and response carries a correlation ID.
//
//   - If the incoming request already has X-Request-ID, that value is reused
//     (allows upstream proxies and the CLI to propagate their own trace IDs).
//   - Otherwise a new ID is generated.
//
// The ID is stored in the request context (use requestIDFromCtx) and echoed
// on the response as X-Request-ID.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = generateRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDCtxKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ── Access logging middleware ─────────────────────────────────────────────────

// loggingMiddleware emits one structured log line per request after the handler
// returns, including method, path, status code, latency, request ID, and the
// client's remote address.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sr, r)
		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sr.code,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", requestIDFromCtx(r.Context()),
			"remote_addr", r.RemoteAddr,
		)
	})
}

// ── Request / response types ──────────────────────────────────────────────────

type addSiteRequest struct {
	Name       string         `json:"name"`
	Role       types.SiteRole `json:"role"`
	S3Bucket   string         `json:"s3_bucket"`
	S3Region   string         `json:"s3_region"`
	S3Endpoint string         `json:"s3_endpoint,omitempty"`
}

type replicateRequest struct {
	Key  string `json:"key"`
	From string `json:"from"`
	To   string `json:"to"`
}

type replicateResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	From   string `json:"from"`
	To     string `json:"to"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("api: encode response", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorResponse{Error: msg})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// ── S3 endpoint validation (SSRF guard) ───────────────────────────────────────

// errEndpointRejected is the generic error returned to the caller when an
// endpoint fails validation, replacing the transport error the handler used to
// echo back.  The detail goes to the log instead: a 502 body containing
// `malformed HTTP response "SSH-2.0-OpenSSH_9.6"` versus `connection refused`
// distinguishes an open port from a closed one, which turns this endpoint into a
// port scanner with an oracle (#76).
var errEndpointRejected = errors.New("s3_endpoint is not permitted")

// endpointResolver is the hook validateS3Endpoint uses to resolve a host to
// addresses.  Overridable so tests can exercise the DNS-name-to-internal-address
// case without depending on a resolver that returns a particular answer.
type endpointResolver func(ctx context.Context, host string) ([]net.IPAddr, error)

// defaultEndpointResolver resolves via the system resolver.
func defaultEndpointResolver(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

// isDisallowedAddr reports whether ip is in an address class a caller-supplied
// S3 endpoint must not reach, and returns a short reason for the log.
//
// allowPrivate relaxes only the private classes (RFC1918, unique-local, CGNAT).
// Loopback, link-local, unspecified, and multicast stay blocked regardless:
// 169.254.169.254 is the highest-value target in this class and nothing that
// serves S3 legitimately lives there.
func isDisallowedAddr(ip net.IP, allowPrivate bool) (bool, string) {
	switch {
	case ip.IsUnspecified():
		return true, "unspecified address"
	case ip.IsLoopback():
		return true, "loopback address"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		// Covers 169.254.0.0/16 (and so IMDS) and fe80::/10.
		return true, "link-local address"
	case ip.IsInterfaceLocalMulticast(), ip.IsMulticast():
		return true, "multicast address"
	}
	if !allowPrivate {
		// net.IP.IsPrivate covers RFC1918 and RFC4193 unique-local.
		if ip.IsPrivate() {
			return true, "private address (set security.allow_private_endpoints to permit)"
		}
		// RFC6598 shared address space, 100.64.0.0/10 — not covered by IsPrivate
		// and routable to carrier-internal infrastructure.
		if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return true, "shared address space (set security.allow_private_endpoints to permit)"
		}
	}
	return false, ""
}

// validateS3Endpoint checks a caller-supplied S3 endpoint before the coordinator
// signs a request to it.  An empty endpoint means "use the AWS default" and is
// always allowed.
//
// It returns a detailed reason for the log and errEndpointRejected for the
// caller; reason is "" exactly when err is nil.  Requirements:
//
//   - absolute URL with an http or https scheme and a host;
//   - no userinfo, and no path/query/fragment beyond "/" (an S3 endpoint is an
//     origin; a path here is a sign the value is being used for something else);
//   - every address the host resolves to passes isDisallowedAddr.
//
// Resolution happens rather than string matching, so "imds.attacker.example"
// pointing at 169.254.169.254 is caught too, and *all* answers must pass — one
// public A record does not license a second private one.
//
// Known limitation, called out in the issue: the gap between resolving here and
// the SDK dialling is a DNS-rebinding window.  Closing it needs the connection
// pinned to the address checked, i.e. a DialContext hook on the HTTP client the
// S3 backend uses — and objectfs's SDK exposes no way to supply one
// (sdks/go/objectfs/options.go has WithEndpoint/WithRegion/WithTLS and no
// transport option).  So this narrows the attack from "any host by name" to "a
// host whose DNS flips between two answers inside one HeadBucket", and the rest
// belongs upstream.
func validateS3Endpoint(ctx context.Context, endpoint string, sec config.SecurityConfig, resolve endpointResolver) (reason string, err error) {
	if endpoint == "" {
		return "", nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Sprintf("unparseable URL: %v", err), errEndpointRejected
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Sprintf("scheme %q is not http or https", u.Scheme), errEndpointRejected
	}
	if u.Host == "" {
		return "URL has no host", errEndpointRejected
	}
	if u.User != nil {
		return "URL must not contain userinfo", errEndpointRejected
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Sprintf("URL must not contain a path (got %q)", u.Path), errEndpointRejected
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "URL must not contain a query or fragment", errEndpointRejected
	}

	host := u.Hostname()
	if host == "" {
		return "URL has no host", errEndpointRejected
	}

	// Exact-match allowlist: the narrow escape hatch, and the only way to permit
	// a loopback endpoint (LocalStack in a test harness, say).
	for _, allowed := range sec.AllowedEndpointHosts {
		if strings.EqualFold(strings.TrimSpace(allowed), host) {
			return "", nil
		}
	}

	// An IP literal needs no resolution; check it directly so a bad literal is
	// rejected without a DNS round trip.
	if ip := net.ParseIP(host); ip != nil {
		if bad, reason := isDisallowedAddr(ip, sec.AllowPrivateEndpoints); bad {
			return reason, errEndpointRejected
		}
		return "", nil
	}

	addrs, err := resolve(ctx, host)
	if err != nil {
		return fmt.Sprintf("host does not resolve: %v", err), errEndpointRejected
	}
	if len(addrs) == 0 {
		return "host resolves to no addresses", errEndpointRejected
	}
	for _, a := range addrs {
		if bad, reason := isDisallowedAddr(a.IP, sec.AllowPrivateEndpoints); bad {
			return fmt.Sprintf("resolves to %s: %s", a.IP, reason), errEndpointRejected
		}
	}
	return "", nil
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// sitesListHandler handles GET /api/v1/sites — returns all sites with health.
func sitesListHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		infos := c.SiteInfos(ctx)
		writeJSON(w, http.StatusOK, infos)
	}
}

// addSiteHandler handles POST /api/v1/sites — register a new site at runtime.
//
// sec constrains which s3_endpoint values are accepted; see validateS3Endpoint.
func addSiteHandler(daemonCtx context.Context, c *coordinator.Coordinator, sec config.SecurityConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
		var req addSiteRequest
		if err := decodeJSON(r, &req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("request body exceeds maximum size of %d bytes", maxJSONBodyBytes))
				return
			}
			writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
			return
		}
		if req.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		if req.S3Bucket == "" {
			writeError(w, http.StatusBadRequest, "s3_bucket is required")
			return
		}
		if req.Role == "" {
			req.Role = types.SiteRolePrimary
		}

		// Verify the role value is valid.
		switch req.Role {
		case types.SiteRolePrimary, types.SiteRoleBackup, types.SiteRoleBurst:
		default:
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid role %q (primary|backup|burst)", req.Role))
			return
		}

		// Validate the endpoint before anything signs a request to it.  site
		// .NewFromConfig performs a HeadBucket, so an unchecked endpoint means the
		// coordinator sends its own credentials' SigV4 Authorization header to a
		// host the caller chose (#76).
		connectCtx, cancel := context.WithTimeout(daemonCtx, 30*time.Second)
		defer cancel()

		if reason, err := validateS3Endpoint(connectCtx, req.S3Endpoint, sec, defaultEndpointResolver); err != nil {
			// The reason is logged, never returned: see errEndpointRejected.
			slog.Warn("api: add site: endpoint rejected",
				"name", req.Name,
				"endpoint", req.S3Endpoint,
				"reason", reason,
				"request_id", requestIDFromCtx(r.Context()),
				"remote_addr", r.RemoteAddr,
			)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		siteCfg := &config.SiteConfig{
			Name: req.Name,
			Role: req.Role,
			ObjectFS: config.ObjectFSConfig{
				S3Bucket:   req.S3Bucket,
				S3Region:   req.S3Region,
				S3Endpoint: req.S3Endpoint,
			},
		}

		mount, err := site.NewFromConfig(connectCtx, siteCfg)
		if err != nil {
			// Generic message: the transport error separates an open non-HTTP port
			// ("malformed HTTP response \"SSH-2.0-...\"") from a closed one
			// ("connection refused"), which is a scan oracle (#76).  Detail is
			// logged for the operator, who can see it, rather than returned to the
			// caller, who should not.
			slog.Warn("api: add site: connect failed", "name", req.Name, "error", err)
			writeError(w, http.StatusBadGateway, "failed to connect to site")
			return
		}

		c.AddSite(mount)
		slog.Info("api: site added", "name", req.Name, "role", req.Role)

		writeJSON(w, http.StatusCreated, coordinator.SiteInfo{
			Name:    mount.Name(),
			Role:    mount.Role(),
			Healthy: true,
		})
	}
}

// removeSiteHandler handles DELETE /api/v1/sites/{name} — deregister a site.
func removeSiteHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			writeError(w, http.StatusBadRequest, "site name required in path")
			return
		}

		// RemoveSite atomically checks existence and removes the site,
		// eliminating the TOCTOU race between a separate Sites() snapshot
		// and a subsequent RemoveSite call (#58).
		if !c.RemoveSite(name) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("site %q not found", name))
			return
		}
		slog.Info("api: site removed", "name", name)
		w.WriteHeader(http.StatusNoContent)
	}
}

// replicateHandler handles POST /api/v1/replicate — enqueue manual replication.
func replicateHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
		var req replicateRequest
		if err := decodeJSON(r, &req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("request body exceeds maximum size of %d bytes", maxJSONBodyBytes))
				return
			}
			writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
			return
		}
		if req.Key == "" {
			writeError(w, http.StatusBadRequest, "key is required")
			return
		}
		if req.From == "" {
			writeError(w, http.StatusBadRequest, "from is required")
			return
		}
		if req.To == "" {
			writeError(w, http.StatusBadRequest, "to is required")
			return
		}

		if err := c.Replicate(r.Context(), req.Key, req.From, req.To); err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "queue full") {
				status = http.StatusServiceUnavailable
			}
			writeError(w, status, err.Error())
			return
		}

		slog.Info("api: replication triggered", "key", req.Key, "from", req.From, "to", req.To)
		writeJSON(w, http.StatusAccepted, replicateResponse{
			Status: "accepted",
			Key:    req.Key,
			From:   req.From,
			To:     req.To,
		})
	}
}

// ── Object API ────────────────────────────────────────────────────────────────

// listObjectsResponse is the JSON envelope for GET /api/v1/objects.
type listObjectsResponse struct {
	Prefix  string                     `json:"prefix"`
	Count   int                        `json:"count"`
	Objects []objectfstypes.ObjectInfo `json:"objects"`
}

// setObjectHeaders writes standard object metadata headers derived from info.
func setObjectHeaders(w http.ResponseWriter, info *objectfstypes.ObjectInfo) {
	ct := info.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	if !info.LastModified.IsZero() {
		w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	}
	if info.ETag != "" {
		w.Header().Set("ETag", info.ETag)
	}
	if info.Checksum != "" {
		w.Header().Set("X-GlobalFS-Checksum", info.Checksum)
	}
}

// objectListHandler handles GET /api/v1/objects — list objects by prefix.
func objectListHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		limit := 0
		if ls := r.URL.Query().Get("limit"); ls != "" {
			n, err := strconv.Atoi(ls)
			if err != nil || n < 0 {
				writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
				return
			}
			limit = n
		}

		objects, err := c.List(r.Context(), prefix, limit)
		if err != nil && len(objects) == 0 {
			// All sites failed — no data to return.
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if objects == nil {
			objects = []objectfstypes.ObjectInfo{}
		}
		status := http.StatusOK
		if err != nil {
			// Partial results: some sites were unreachable but at least one
			// contributed data.  Use 207 Multi-Status to signal degraded state.
			status = http.StatusMultiStatus
			w.Header().Set("X-GlobalFS-Partial", "true")
		}
		writeJSON(w, status, listObjectsResponse{
			Prefix:  prefix,
			Count:   len(objects),
			Objects: objects,
		})
	}
}

// ── Per-request transfer deadlines ────────────────────────────────────────────
//
// http.Server's ReadTimeout and WriteTimeout are absolute deadlines on the whole
// request and the whole response — not idle timeouts.  The values in
// cmd/coordinator/main.go are right for a JSON control API and wrong for the
// object routes that share the same server, and the arithmetic is not close
// (#75):
//
//	WriteTimeout 10s vs a 64 MiB GET  -> truncated mid-body at ~42 MB
//	ReadTimeout  10s vs a 256 MiB PUT -> needs 25.6 MiB/s sustained to
//	                                     even reach the advertised cap
//
// So the strict server-wide deadlines stay, and the object handlers replace them
// per request with a budget derived from the payload size and a floor.  A stalled
// transfer is still cut off — the point is to size the deadline to the work, not
// to remove it.

// minTransferThroughputBytesPerSec is the slowest transfer rate the object routes
// will tolerate.  A deadline is computed as size/rate, so this is what decides
// whether a large transfer gets time to finish.
//
// 1 MiB/s is deliberately near the floor of plausible: it gives the documented
// 256 MiB cap 256s and makes the advertised limit reachable, which is the defect
// the issue identifies. Raising it re-breaks slow clients; the timeout and the
// size cap have to be consistent with each other, and that is the constraint
// that sets this number rather than taste.
const minTransferThroughputBytesPerSec = 1 << 20 // 1 MiB/s

// minTransferDeadline is the floor applied to every computed budget, so a small
// object is not held to an unreasonably tight deadline (a 4 KiB object would
// otherwise get 4ms) and a zero-length one still gets time to be written.
const minTransferDeadline = 30 * time.Second

// maxTransferDeadline caps the budget regardless of size.  Slowloris protection
// is the reason there is a ceiling at all: without one, a caller advertising a
// large Content-Length could hold a connection indefinitely.  256 MiB at 1 MiB/s
// is 256s, so this leaves headroom over the documented cap without being open
// ended.
const maxTransferDeadline = 10 * time.Minute

// transferDeadline returns the time budget for moving n bytes: n at
// minTransferThroughputBytesPerSec, clamped to
// [minTransferDeadline, maxTransferDeadline].
//
// A negative or unknown size (Content-Length of -1 on a chunked upload) yields
// the floor.
func transferDeadline(n int64) time.Duration {
	if n <= 0 {
		return minTransferDeadline
	}
	d := time.Duration(n/minTransferThroughputBytesPerSec) * time.Second
	if d < minTransferDeadline {
		return minTransferDeadline
	}
	if d > maxTransferDeadline {
		return maxTransferDeadline
	}
	return d
}

// extendWriteDeadline replaces the server-wide WriteTimeout for this response
// with a budget sized for n bytes.  Returns the deadline applied, or 0 if the
// ResponseWriter does not support deadline control.
//
// http.ErrNotSupported is not an error worth logging loudly: httptest.Recorder
// returns it, and so does any ResponseWriter wrapper that does not implement
// Unwrap. The handler proceeds under the server-wide deadline in that case,
// which is the pre-fix behaviour rather than a regression.
func extendWriteDeadline(w http.ResponseWriter, n int64) time.Duration {
	d := transferDeadline(n)
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			slog.Warn("api: set write deadline", "error", err)
		}
		return 0
	}
	return d
}

// extendReadDeadline does the same for a request body of n bytes.
func extendReadDeadline(w http.ResponseWriter, n int64) time.Duration {
	d := transferDeadline(n)
	if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(d)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			slog.Warn("api: set read deadline", "error", err)
		}
		return 0
	}
	return d
}

// objectGetHandler handles GET /api/v1/objects/{key...} — retrieve object data.
func objectGetHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "object key required in path")
			return
		}
		if err := validateObjectKey(key); err != nil {
			writeError(w, http.StatusBadRequest, "invalid object key: "+err.Error())
			return
		}

		data, err := c.Get(r.Context(), key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}

		// The exact response size is known here, so the deadline is sized to it
		// before a byte is written. Under the server-wide WriteTimeout of 10s any
		// object needing longer than that was truncated mid-body, after a 200 and
		// an accurate Content-Length had already been sent (#75).
		extendWriteDeadline(w, int64(len(data)))

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(data); err != nil {
			// Nothing can be done for the client — the status and Content-Length
			// are already committed — but this must not be swallowed: it is the
			// only record that the response the client received was short.
			slog.Warn("api: write response body truncated",
				"key", key,
				"bytes_expected", len(data),
				"request_id", requestIDFromCtx(r.Context()),
				"error", err)
		}
	}
}

// objectPutHandler handles PUT /api/v1/objects/{key...} — store object data.
func objectPutHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "object key required in path")
			return
		}
		if err := validateObjectKey(key); err != nil {
			writeError(w, http.StatusBadRequest, "invalid object key: "+err.Error())
			return
		}

		// Size the read deadline from the advertised body length before reading.
		// Content-Length is untrusted, but it is only used to grant time, and the
		// grant is bounded by maxTransferDeadline and the MaxBytesReader below —
		// so overstating it buys a slow-loris no more than maxTransferDeadline,
		// which is the ceiling regardless. A chunked upload reports -1 and gets
		// the floor.
		//
		// Without this, ReadTimeout of 10s against the documented 256 MiB cap
		// required 25.6 MiB/s sustained for the limit to be reachable at all: an
		// 8 MiB upload at 2 MiB/s failed after 229 KB (#75).
		extendReadDeadline(w, r.ContentLength)

		// The *write* deadline has to be extended here too, which is not obvious.
		// net/http arms both deadlines when it finishes reading the request
		// headers, so WriteTimeout is already ticking while the body is still
		// arriving. A body that takes longer than WriteTimeout to upload leaves no
		// time to answer it: the response write fails, the connection is closed,
		// and the client sees EOF rather than the 201 its upload earned. Found by
		// TestObjectPut_SlowUploadNotCutOff, which failed exactly that way with
		// only the read deadline extended.
		extendWriteDeadline(w, r.ContentLength)

		r.Body = http.MaxBytesReader(w, r.Body, maxObjectBodyBytes)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeError(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("request body exceeds maximum size of %d bytes", maxObjectBodyBytes))
				return
			}
			writeError(w, http.StatusBadRequest, "read request body: "+err.Error())
			return
		}

		if err := c.Put(r.Context(), key, data); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}

		slog.Info("api: object stored", "key", key, "bytes", len(data))
		w.WriteHeader(http.StatusCreated)
	}
}

// objectDeleteHandler handles DELETE /api/v1/objects/{key...} — remove object.
func objectDeleteHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "object key required in path")
			return
		}
		if err := validateObjectKey(key); err != nil {
			writeError(w, http.StatusBadRequest, "invalid object key: "+err.Error())
			return
		}

		if err := c.Delete(r.Context(), key); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}

		slog.Info("api: object deleted", "key", key)
		w.WriteHeader(http.StatusNoContent)
	}
}

// objectHeadHandler handles HEAD /api/v1/objects/{key...} — object metadata.
func objectHeadHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			// HEAD must not return a body; use a plain 400 status.
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := validateObjectKey(key); err != nil {
			// HEAD must not return a body.
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		info, err := c.Head(r.Context(), key)
		if err != nil {
			// HEAD must not return a body.
			w.WriteHeader(http.StatusBadGateway)
			return
		}

		setObjectHeaders(w, info)
		w.WriteHeader(http.StatusOK)
	}
}

// ── Info handler ──────────────────────────────────────────────────────────────

// healthSummary is embedded in infoResponse to show the cached health state.
type healthSummary struct {
	Healthy       int        `json:"healthy"`
	Unhealthy     int        `json:"unhealthy"`
	LastCheckedAt *time.Time `json:"last_checked_at,omitempty"`
}

// infoResponse is the JSON payload for GET /api/v1/info.
type infoResponse struct {
	Version               string         `json:"version"`
	UptimeSeconds         float64        `json:"uptime_seconds"`
	Sites                 int            `json:"sites"`
	SitesByRole           map[string]int `json:"sites_by_role"`
	ReplicationQueueDepth int            `json:"replication_queue_depth"`
	IsLeader              bool           `json:"is_leader"`
	Health                healthSummary  `json:"health"`
}

// infoHandler handles GET /api/v1/info — returns coordinator runtime stats.
func infoHandler(c *coordinator.Coordinator, version string, startTime time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sites := c.Sites()
		byRole := make(map[string]int)
		for _, s := range sites {
			byRole[string(s.Role())]++
		}

		var hs healthSummary
		if report, checkedAt := c.HealthStatus(); report != nil {
			t := checkedAt
			hs.LastCheckedAt = &t
			for _, s := range sites {
				if err := report[s.Name()]; err != nil {
					hs.Unhealthy++
				} else {
					hs.Healthy++
				}
			}
		}

		writeJSON(w, http.StatusOK, infoResponse{
			Version:               version,
			UptimeSeconds:         time.Since(startTime).Seconds(),
			Sites:                 len(sites),
			SitesByRole:           byRole,
			ReplicationQueueDepth: c.ReplicationQueueDepth(),
			IsLeader:              c.IsLeader(),
			Health:                hs,
		})
	}
}

// ── Metrics middleware ────────────────────────────────────────────────────────

// statusRecorder wraps http.ResponseWriter to capture the HTTP status code
// written by a handler so it can be forwarded to metrics.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.code = code
	sr.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the underlying ResponseWriter to http.ResponseController.
//
// This is required, not cosmetic. Both loggingMiddleware and withObjectMetrics
// wrap the writer in a statusRecorder, and ResponseController walks Unwrap to
// find something implementing SetWriteDeadline. Without this method every object
// handler's deadline call returns http.ErrNotSupported and silently does nothing
// — the handler keeps the server-wide 10s deadline and #75 is unfixed while
// looking fixed. Verified: with the method absent, SetWriteDeadline through a
// statusRecorder returns "feature not supported"; with it, nil.
func (sr *statusRecorder) Unwrap() http.ResponseWriter { return sr.ResponseWriter }

// withObjectMetrics wraps a handler to record operation duration and status.
// m may be nil; when nil the handler is called without instrumentation.
func withObjectMetrics(operation string, m *metrics.Metrics, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if m == nil {
			next(w, r)
			return
		}
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next(sr, r)
		status := "ok"
		if sr.code >= 400 {
			status = "error"
		}
		m.RecordOperation(operation, status, time.Since(start))
	}
}

// buildHandler wraps mux in the server's middleware chain, applied innermost →
// outermost:
//
//	mux → rejectUnsafePath → apiKey (when key != "") → logging → requestID
//
// requestID is outermost so every response carries a correlation ID; logging
// wraps apiKey so auth rejections are recorded too.  When key is "" the auth
// layer is omitted entirely.
//
// The order of the two innermost layers is load-bearing. rejectUnsafePath must
// wrap the mux directly — it is the only layer that observes the request target
// before ServeMux path-cleans it and 307s a traversal onto a different route
// (#73) — and it must sit inside apiKey so an unauthenticated traversal probe
// gets 401, not the 400 that would confirm the path was parsed.
//
// This lives here rather than inline in main so tests can exercise the same
// chain the daemon serves instead of a hand-assembled approximation.
func buildHandler(mux *http.ServeMux, apiKey string) http.Handler {
	handler := rejectUnsafePath(mux)
	if apiKey != "" {
		handler = apiKeyMiddleware(apiKey)(handler)
	}
	handler = loggingMiddleware(handler)
	return requestIDMiddleware(handler)
}

// registerAPIRoutes registers all /api/v1/* endpoints on mux.
// daemonCtx is the coordinator's parent context, used for S3 connection setup.
// m may be nil; when non-nil, object handler latency and status are recorded.
// sec constrains which s3_endpoint values POST /api/v1/sites will accept.
func registerAPIRoutes(mux *http.ServeMux, daemonCtx context.Context, c *coordinator.Coordinator, m *metrics.Metrics, sec config.SecurityConfig) {
	mux.HandleFunc("GET /api/v1/sites", sitesListHandler(c))
	mux.HandleFunc("POST /api/v1/sites", addSiteHandler(daemonCtx, c, sec))
	mux.HandleFunc("DELETE /api/v1/sites/{name}", removeSiteHandler(c))
	mux.HandleFunc("POST /api/v1/replicate", replicateHandler(c))

	mux.HandleFunc("GET /api/v1/objects", withObjectMetrics("list", m, objectListHandler(c)))
	mux.HandleFunc("GET /api/v1/objects/{key...}", withObjectMetrics("get", m, objectGetHandler(c)))
	mux.HandleFunc("PUT /api/v1/objects/{key...}", withObjectMetrics("put", m, objectPutHandler(c)))
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", withObjectMetrics("delete", m, objectDeleteHandler(c)))
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", withObjectMetrics("head", m, objectHeadHandler(c)))
}
