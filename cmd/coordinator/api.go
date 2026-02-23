package main

// api.go — REST API handlers mounted under /api/v1/
//
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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/internal/metrics"
	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

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
func addSiteHandler(daemonCtx context.Context, c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req addSiteRequest
		if err := decodeJSON(r, &req); err != nil {
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

		siteCfg := &config.SiteConfig{
			Name: req.Name,
			Role: req.Role,
			ObjectFS: config.ObjectFSConfig{
				S3Bucket:   req.S3Bucket,
				S3Region:   req.S3Region,
				S3Endpoint: req.S3Endpoint,
			},
		}

		connectCtx, cancel := context.WithTimeout(daemonCtx, 30*time.Second)
		defer cancel()

		mount, err := site.NewFromConfig(connectCtx, siteCfg)
		if err != nil {
			slog.Warn("api: add site: connect failed", "name", req.Name, "error", err)
			writeError(w, http.StatusBadGateway, "failed to connect to site: "+err.Error())
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

		// Verify the site exists.
		var found bool
		for _, s := range c.Sites() {
			if s.Name() == name {
				found = true
				break
			}
		}
		if !found {
			writeError(w, http.StatusNotFound, fmt.Sprintf("site %q not found", name))
			return
		}

		c.RemoveSite(name)
		slog.Info("api: site removed", "name", name)
		w.WriteHeader(http.StatusNoContent)
	}
}

// replicateHandler handles POST /api/v1/replicate — enqueue manual replication.
func replicateHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req replicateRequest
		if err := decodeJSON(r, &req); err != nil {
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
			writeError(w, http.StatusBadRequest, err.Error())
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
	Prefix  string                    `json:"prefix"`
	Count   int                       `json:"count"`
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
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if objects == nil {
			objects = []objectfstypes.ObjectInfo{}
		}
		writeJSON(w, http.StatusOK, listObjectsResponse{
			Prefix:  prefix,
			Count:   len(objects),
			Objects: objects,
		})
	}
}

// objectGetHandler handles GET /api/v1/objects/{key...} — retrieve object data.
func objectGetHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "object key required in path")
			return
		}

		data, err := c.Get(r.Context(), key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
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

		data, err := io.ReadAll(r.Body)
		if err != nil {
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

// withObjectMetrics wraps a handler to record operation duration and status.
// m may be nil (calls are no-ops when metrics are not configured).
func withObjectMetrics(operation string, m *metrics.Metrics, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

// registerAPIRoutes registers all /api/v1/* endpoints on mux.
// daemonCtx is the coordinator's parent context, used for S3 connection setup.
// m may be nil; when non-nil, object handler latency and status are recorded.
func registerAPIRoutes(mux *http.ServeMux, daemonCtx context.Context, c *coordinator.Coordinator, m *metrics.Metrics) {
	mux.HandleFunc("GET /api/v1/sites", sitesListHandler(c))
	mux.HandleFunc("POST /api/v1/sites", addSiteHandler(daemonCtx, c))
	mux.HandleFunc("DELETE /api/v1/sites/{name}", removeSiteHandler(c))
	mux.HandleFunc("POST /api/v1/replicate", replicateHandler(c))

	mux.HandleFunc("GET /api/v1/objects", withObjectMetrics("list", m, objectListHandler(c)))
	mux.HandleFunc("GET /api/v1/objects/{key...}", withObjectMetrics("get", m, objectGetHandler(c)))
	mux.HandleFunc("PUT /api/v1/objects/{key...}", withObjectMetrics("put", m, objectPutHandler(c)))
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", withObjectMetrics("delete", m, objectDeleteHandler(c)))
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", withObjectMetrics("head", m, objectHeadHandler(c)))
}
