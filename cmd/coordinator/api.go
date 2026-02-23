package main

// api.go — REST API handlers mounted under /api/v1/
//
// GET    /api/v1/sites         → list all sites with health
// POST   /api/v1/sites         → register a new site
// DELETE /api/v1/sites/{name}  → deregister a site
// POST   /api/v1/replicate     → trigger manual replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/scttfrdmn/globalfs/internal/coordinator"
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

// registerAPIRoutes registers all /api/v1/* endpoints on mux.
// daemonCtx is the coordinator's parent context, used for S3 connection setup.
func registerAPIRoutes(mux *http.ServeMux, daemonCtx context.Context, c *coordinator.Coordinator) {
	mux.HandleFunc("GET /api/v1/sites", sitesListHandler(c))
	mux.HandleFunc("POST /api/v1/sites", addSiteHandler(daemonCtx, c))
	mux.HandleFunc("DELETE /api/v1/sites/{name}", removeSiteHandler(c))
	mux.HandleFunc("POST /api/v1/replicate", replicateHandler(c))
}
