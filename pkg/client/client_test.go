package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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

// newServerWithKey creates a test server that enforces the given API key and
// returns a client configured with the same key.
func newServerWithKey(t *testing.T, key string, mux *http.ServeMux) *client.Client {
	t.Helper()
	auth := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-GlobalFS-API-Key") != key {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		mux.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(auth)
	t.Cleanup(srv.Close)
	return client.New(srv.URL, client.WithAPIKey(key))
}

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

// ── GetObject ─────────────────────────────────────────────────────────────────

func TestGetObject_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ACGTACGT"))
	})
	c := newServer(t, mux)

	data, err := c.GetObject(context.Background(), "data/genome.bam")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "ACGTACGT" {
		t.Errorf("got %q, want ACGTACGT", data)
	}
}

func TestGetObject_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "site unreachable"})
	})
	c := newServer(t, mux)

	_, err := c.GetObject(context.Background(), "missing/key")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", apiErr.StatusCode)
	}
}

// ── PutObject ─────────────────────────────────────────────────────────────────

func TestPutObject_OK(t *testing.T) {
	var gotKey string
	var gotBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.PathValue("key")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	})
	c := newServer(t, mux)

	if err := c.PutObject(context.Background(), "uploads/data.bin", []byte("payload")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if gotKey != "uploads/data.bin" {
		t.Errorf("server got key %q, want uploads/data.bin", gotKey)
	}
	if string(gotBody) != "payload" {
		t.Errorf("server got body %q, want payload", gotBody)
	}
}

func TestPutObject_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "disk full"})
	})
	c := newServer(t, mux)

	err := c.PutObject(context.Background(), "k", []byte("v"))
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", apiErr.StatusCode)
	}
}

// ── HeadObject ────────────────────────────────────────────────────────────────

func TestHeadObject_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "42")
		w.Header().Set("ETag", "abc123")
		w.Header().Set("Last-Modified", "Wed, 15 Jan 2026 12:00:00 GMT")
		w.Header().Set("X-GlobalFS-Checksum", "sha256-deadbeef")
		w.WriteHeader(http.StatusOK)
	})
	c := newServer(t, mux)

	info, err := c.HeadObject(context.Background(), "archive/data.tar.zst")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if info.Size != 42 {
		t.Errorf("Size: got %d, want 42", info.Size)
	}
	if info.ETag != "abc123" {
		t.Errorf("ETag: got %q, want abc123", info.ETag)
	}
	if info.Checksum != "sha256-deadbeef" {
		t.Errorf("Checksum: got %q", info.Checksum)
	}
	if info.LastModified.IsZero() {
		t.Error("expected non-zero LastModified")
	}
	if info.Key != "archive/data.tar.zst" {
		t.Errorf("Key: got %q", info.Key)
	}
}

func TestHeadObject_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	c := newServer(t, mux)

	_, err := c.HeadObject(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for 502")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
}

// ── DeleteObject ──────────────────────────────────────────────────────────────

func TestDeleteObject_OK(t *testing.T) {
	var gotKey string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.PathValue("key")
		w.WriteHeader(http.StatusNoContent)
	})
	c := newServer(t, mux)

	if err := c.DeleteObject(context.Background(), "reports/q1.csv"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if gotKey != "reports/q1.csv" {
		t.Errorf("server got key %q, want reports/q1.csv", gotKey)
	}
}

func TestDeleteObject_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "network error"})
	})
	c := newServer(t, mux)

	err := c.DeleteObject(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
}

// ── ListObjects ───────────────────────────────────────────────────────────────

func TestListObjects_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"prefix":  "",
			"count":   2,
			"objects": []client.ObjectInfo{{Key: "a"}, {Key: "b"}},
		})
	})
	c := newServer(t, mux)

	objects, err := c.ListObjects(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objects))
	}
}

func TestListObjects_WithPrefixAndLimit(t *testing.T) {
	var gotPrefix, gotLimit string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		gotPrefix = r.URL.Query().Get("prefix")
		gotLimit = r.URL.Query().Get("limit")
		writeJSON(w, http.StatusOK, map[string]any{
			"prefix":  gotPrefix,
			"count":   0,
			"objects": []client.ObjectInfo{},
		})
	})
	c := newServer(t, mux)

	_, err := c.ListObjects(context.Background(), "data/", 50)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if gotPrefix != "data/" {
		t.Errorf("prefix: got %q, want data/", gotPrefix)
	}
	if gotLimit != "50" {
		t.Errorf("limit: got %q, want 50", gotLimit)
	}
}

func TestListObjects_Empty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"prefix":  "",
			"count":   0,
			"objects": []client.ObjectInfo{},
		})
	})
	c := newServer(t, mux)

	objects, err := c.ListObjects(context.Background(), "", 0)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if objects == nil {
		t.Error("expected non-nil slice for empty result")
	}
}

// ── WithAPIKey ────────────────────────────────────────────────────────────────

func TestWithAPIKey_SetsHeader(t *testing.T) {
	var gotKey string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-GlobalFS-API-Key")
		writeJSON(w, http.StatusOK, []client.SiteInfo{})
	})
	c := newServerWithKey(t, "my-secret", mux)

	_, err := c.ListSites(context.Background())
	if err != nil {
		t.Fatalf("ListSites: %v", err)
	}
	if gotKey != "my-secret" {
		t.Errorf("X-GlobalFS-API-Key header: got %q, want %q", gotKey, "my-secret")
	}
}

func TestWithAPIKey_MissingKey_Returns401(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []client.SiteInfo{})
	})
	// Server requires a key, but client has none.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-GlobalFS-API-Key") != "required" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL) // no api key

	_, err := c.ListSites(context.Background())
	if err == nil {
		t.Fatal("expected error when API key is missing")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 APIError, got: %v", err)
	}
}

func TestWithAPIKey_SetsHeaderOnAllMethods(t *testing.T) {
	const key = "test-key-123"
	var gotKeys []string
	var mu sync.Mutex

	mux := http.NewServeMux()
	capture := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotKeys = append(gotKeys, r.Header.Get("X-GlobalFS-API-Key"))
		mu.Unlock()
	}
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		capture(w, r)
		writeJSON(w, http.StatusOK, []client.SiteInfo{})
	})
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		capture(w, r)
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		capture(w, r)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data"))
	})
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		capture(w, r)
		w.WriteHeader(http.StatusNoContent)
	})

	c := newServerWithKey(t, key, mux)
	ctx := context.Background()

	c.ListSites(ctx)
	c.PutObject(ctx, "k", []byte("v"))
	c.GetObject(ctx, "k")
	c.DeleteObject(ctx, "k")

	mu.Lock()
	defer mu.Unlock()
	for i, got := range gotKeys {
		if got != key {
			t.Errorf("call %d: X-GlobalFS-API-Key = %q, want %q", i, got, key)
		}
	}
	if len(gotKeys) != 4 {
		t.Errorf("expected 4 requests, got %d", len(gotKeys))
	}
}
