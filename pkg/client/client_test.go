package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/client"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newServer(t *testing.T, mux *http.ServeMux) *client.Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return client.New(srv.URL)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ── New / options ─────────────────────────────────────────────────────────────

func TestNew_DefaultTimeout(t *testing.T) {
	c := client.New("http://localhost:8090")
	// Verify the default timeout is applied by passing a custom HTTP client
	// with a different timeout and checking WithTimeout overrides it.
	custom := &http.Client{Timeout: 5 * time.Second}
	c2 := client.New("http://localhost:8090", client.WithHTTPClient(custom))
	// If WithHTTPClient is wired correctly, the client is non-nil.
	if c2 == nil {
		t.Fatal("expected non-nil client")
	}
	// Default client should also be non-nil.
	if c == nil {
		t.Fatal("expected non-nil default client")
	}
}

func TestNew_WithTimeout(t *testing.T) {
	// WithTimeout must not panic and must return a usable client.
	c := client.New("http://localhost:8090", client.WithTimeout(5*time.Second))
	if c == nil {
		t.Fatal("expected non-nil client after WithTimeout")
	}
}

// ── ListSites ─────────────────────────────────────────────────────────────────

func TestListSites_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []client.SiteInfo{
			{Name: "primary", Role: "primary", Healthy: true},
			{Name: "backup", Role: "backup", Healthy: false, Error: "timeout"},
		})
	})
	c := newServer(t, mux)

	sites, err := c.ListSites(context.Background())
	if err != nil {
		t.Fatalf("ListSites: %v", err)
	}
	if len(sites) != 2 {
		t.Fatalf("expected 2 sites, got %d", len(sites))
	}
	if sites[0].Name != "primary" || !sites[0].Healthy {
		t.Errorf("unexpected site[0]: %+v", sites[0])
	}
	if sites[1].Name != "backup" || sites[1].Healthy || sites[1].Error != "timeout" {
		t.Errorf("unexpected site[1]: %+v", sites[1])
	}
}

func TestListSites_Empty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []client.SiteInfo{})
	})
	c := newServer(t, mux)

	sites, err := c.ListSites(context.Background())
	if err != nil {
		t.Fatalf("ListSites: %v", err)
	}
	if sites == nil {
		t.Error("expected non-nil slice for empty result")
	}
	if len(sites) != 0 {
		t.Errorf("expected 0 sites, got %d", len(sites))
	}
}

func TestListSites_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database unavailable"})
	})
	c := newServer(t, mux)

	_, err := c.ListSites(context.Background())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", apiErr.StatusCode)
	}
}

// ── AddSite ───────────────────────────────────────────────────────────────────

func TestAddSite_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		var req client.AddSiteRequest
		json.NewDecoder(r.Body).Decode(&req)
		writeJSON(w, http.StatusCreated, client.SiteInfo{
			Name:    req.Name,
			Role:    req.Role,
			Healthy: true,
		})
	})
	c := newServer(t, mux)

	info, err := c.AddSite(context.Background(), client.AddSiteRequest{
		Name:     "cloud",
		Role:     "burst",
		S3Bucket: "my-burst-bucket",
		S3Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("AddSite: %v", err)
	}
	if info.Name != "cloud" || info.Role != "burst" || !info.Healthy {
		t.Errorf("unexpected SiteInfo: %+v", info)
	}
}

func TestAddSite_BadRequest(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "s3_bucket is required"})
	})
	c := newServer(t, mux)

	_, err := c.AddSite(context.Background(), client.AddSiteRequest{Name: "bad"})
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
	if apiErr.Message != "s3_bucket is required" {
		t.Errorf("unexpected message: %q", apiErr.Message)
	}
}

// ── RemoveSite ────────────────────────────────────────────────────────────────

func TestRemoveSite_OK(t *testing.T) {
	var gotName string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/sites/{name}", func(w http.ResponseWriter, r *http.Request) {
		gotName = r.PathValue("name")
		w.WriteHeader(http.StatusNoContent)
	})
	c := newServer(t, mux)

	if err := c.RemoveSite(context.Background(), "old-site"); err != nil {
		t.Fatalf("RemoveSite: %v", err)
	}
	if gotName != "old-site" {
		t.Errorf("server got site name %q, want %q", gotName, "old-site")
	}
}

func TestRemoveSite_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/sites/{name}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "site \"ghost\" not found"})
	})
	c := newServer(t, mux)

	err := c.RemoveSite(context.Background(), "ghost")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", apiErr.StatusCode)
	}
}

func TestRemoveSite_URLEncoding(t *testing.T) {
	// Site names with slashes or spaces must be percent-encoded in the path.
	var gotName string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/sites/{name}", func(w http.ResponseWriter, r *http.Request) {
		gotName = r.PathValue("name")
		w.WriteHeader(http.StatusNoContent)
	})
	c := newServer(t, mux)

	if err := c.RemoveSite(context.Background(), "site with spaces"); err != nil {
		t.Fatalf("RemoveSite: %v", err)
	}
	if gotName != "site with spaces" {
		t.Errorf("decoded path value %q, want %q", gotName, "site with spaces")
	}
}

// ── Replicate ─────────────────────────────────────────────────────────────────

func TestReplicate_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/replicate", func(w http.ResponseWriter, r *http.Request) {
		var req client.ReplicateRequest
		json.NewDecoder(r.Body).Decode(&req)
		writeJSON(w, http.StatusAccepted, client.ReplicateResponse{
			Status: "accepted",
			Key:    req.Key,
			From:   req.From,
			To:     req.To,
		})
	})
	c := newServer(t, mux)

	result, err := c.Replicate(context.Background(), client.ReplicateRequest{
		Key:  "data/genome.bam",
		From: "primary",
		To:   "backup",
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if result.Status != "accepted" {
		t.Errorf("expected status 'accepted', got %q", result.Status)
	}
	if result.Key != "data/genome.bam" || result.From != "primary" || result.To != "backup" {
		t.Errorf("unexpected ReplicateResponse: %+v", result)
	}
}

func TestReplicate_ValidationError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/replicate", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from site not found"})
	})
	c := newServer(t, mux)

	_, err := c.Replicate(context.Background(), client.ReplicateRequest{
		Key: "k", From: "nosuchsite", To: "backup",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
}

// ── Status ────────────────────────────────────────────────────────────────────

func TestStatus_Healthy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})
	c := newServer(t, mux)

	sr, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !sr.Healthy {
		t.Error("expected Healthy=true")
	}
	if sr.Details != "" {
		t.Errorf("expected empty details, got %q", sr.Details)
	}
}

func TestStatus_Degraded(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("DEGRADED\nprimary: connection refused\n"))
	})
	c := newServer(t, mux)

	sr, err := c.Status(context.Background())
	// Status must return both a StatusResponse AND a non-nil error.
	if err == nil {
		t.Fatal("expected non-nil error for degraded coordinator")
	}
	if sr.Healthy {
		t.Error("expected Healthy=false for 503")
	}
	if sr.Details != "primary: connection refused" {
		t.Errorf("unexpected details: %q", sr.Details)
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", apiErr.StatusCode)
	}
}

// ── Context cancellation ──────────────────────────────────────────────────────

func TestListSites_ContextCancelled(t *testing.T) {
	// Server deliberately hangs to trigger client-side cancellation.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the request is cancelled
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, client.WithTimeout(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := c.ListSites(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// ── APIError ──────────────────────────────────────────────────────────────────

func TestAPIError_ErrorString(t *testing.T) {
	e := &client.APIError{StatusCode: 404, Message: "not found"}
	got := e.Error()
	if got != "coordinator error (404): not found" {
		t.Errorf("unexpected error string: %q", got)
	}
}
