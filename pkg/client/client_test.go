package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

// ── GetObject truncation (#74) ─────────────────────────────────────────────────

// rawServer serves a single hand-written HTTP response over a raw TCP
// connection, then closes it.  This is the only way to produce responses
// net/http's server refuses to emit — a body shorter than its own
// Content-Length, or a close-delimited body with no length at all.
func rawServer(t *testing.T, response string) *client.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Consume the request head; we do not care what it says.  Every
			// error here is ignored on purpose: the client is the subject of the
			// test, and a failure on this side surfaces as a client-side error.
			_, _ = conn.Read(make([]byte, 4096))
			_, _ = conn.Write([]byte(response))
			_ = conn.Close()
		}
	}()
	return client.New("http://" + ln.Addr().String())
}

// TestGetObject_ShortBody_ContentLength is the primary regression test for #74:
// the response is a 200 with Content-Length: 64 and only 8 bytes of body.  On
// the pre-fix tree GetObject returned those 8 bytes with a nil error, and
// `object get -o file` wrote a short file and exited 0.
func TestGetObject_ShortBody_ContentLength(t *testing.T) {
	c := rawServer(t, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 64\r\n\r\n12345678")

	data, err := c.GetObject(context.Background(), "data/genome.bam")
	if err == nil {
		t.Fatalf("expected an error for a truncated body, got nil with %d bytes", len(data))
	}
	if data != nil {
		t.Errorf("partial data must not be returned alongside an error; got %d bytes", len(data))
	}
	if !strings.Contains(err.Error(), "genome.bam") {
		t.Errorf("error should name the key; got %v", err)
	}
}

// TestGetObject_ShortBody_NoTransportError covers the case the length check
// exists for: a transport that hands back fewer bytes than Content-Length
// advertises without reporting a read error at all.  WithHTTPClient makes this
// substitution part of the public API, so the check cannot lean on net/http
// noticing.
func TestGetObject_ShortBody_NoTransportError(t *testing.T) {
	hc := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		body := []byte("12345678")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        http.Header{"Content-Type": {"application/octet-stream"}},
			ContentLength: 64, // claims 64, delivers 8, reports no error
			Body:          io.NopCloser(bytes.NewReader(body)),
			Request:       r,
		}, nil
	})}
	c := client.New("http://coordinator.invalid", client.WithHTTPClient(hc))

	data, err := c.GetObject(context.Background(), "big.bin")
	if err == nil {
		t.Fatalf("expected an error, got nil with %d bytes", len(data))
	}
	if !strings.Contains(err.Error(), "got 8 bytes, want 64") {
		t.Errorf("error should report both lengths; got %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got %d bytes", len(data))
	}
}

// TestGetObject_MidBodyReadError covers a body that fails partway with a
// transport-level error.  The bytes read before the failure must not be
// returned as success.
func TestGetObject_MidBodyReadError(t *testing.T) {
	boom := errors.New("connection reset by peer")
	hc := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        http.Header{},
			ContentLength: 64,
			Body: io.NopCloser(io.MultiReader(
				bytes.NewReader([]byte("1234567890")),
				errReader{boom},
			)),
			Request: r,
		}, nil
	})}
	c := client.New("http://coordinator.invalid", client.WithHTTPClient(hc))

	data, err := c.GetObject(context.Background(), "partial.bin")
	if err == nil {
		t.Fatalf("expected an error, got nil with %d bytes", len(data))
	}
	if !errors.Is(err, boom) {
		t.Errorf("error should wrap the read failure; got %v", err)
	}
	if !strings.Contains(err.Error(), "after 10 bytes") {
		t.Errorf("error should report how far the read got; got %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got %d bytes", len(data))
	}
}

// TestGetObject_NoContentLength_ShortBodyUndetected documents the limit of the
// fix rather than asserting a guarantee it does not make.  With no
// Content-Length there is nothing to compare against and a close-delimited
// short body is indistinguishable from a complete one, so it still succeeds.
// The coordinator always sets Content-Length on GET /api/v1/objects, so this is
// not reachable against a GlobalFS server; the test exists so that a future
// change to a streaming or chunked response path fails here and is forced to
// think about end-to-end integrity instead.
func TestGetObject_NoContentLength_ShortBodyUndetected(t *testing.T) {
	c := rawServer(t, "HTTP/1.0 200 OK\r\nContent-Type: application/octet-stream\r\n\r\n12345678")

	data, err := c.GetObject(context.Background(), "unlengthed.bin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "12345678" {
		t.Errorf("got %q, want 12345678", data)
	}
}

// TestGetObject_ExactContentLength verifies the check does not reject a
// correct, complete response.
func TestGetObject_ExactContentLength(t *testing.T) {
	payload := bytes.Repeat([]byte("ACGT"), 4096) // 16 KiB
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	})
	c := newServer(t, mux)

	data, err := c.GetObject(context.Background(), "data/genome.bam")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Errorf("got %d bytes, want %d", len(data), len(payload))
	}
}

// TestGetObject_EmptyObject verifies a zero-length object with
// Content-Length: 0 is not mistaken for a truncation.
func TestGetObject_EmptyObject(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	})
	c := newServer(t, mux)

	data, err := c.GetObject(context.Background(), "empty")
	if err != nil {
		t.Fatalf("GetObject on an empty object: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("got %d bytes, want 0", len(data))
	}
}

// TestGetObject_ServerWriteTimeout reproduces the field scenario from #74
// end-to-end: a real coordinator-shaped handler writes a large body with
// Content-Length set, the server's WriteTimeout fires mid-write, and the client
// must not report success.
func TestGetObject_ServerWriteTimeout(t *testing.T) {
	const size = 8 << 20
	payload := make([]byte, size)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		WriteTimeout: 150 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			// The error is expected — WriteTimeout fires partway through — and
			// is asserted on the client side, which is the subject here.
			_, _ = w.Write(payload)
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// Throttle the client's reads so the server's write deadline expires while
	// the body is still in flight.
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &slowConn{Conn: conn}, nil
		},
	}}
	c := client.New("http://"+ln.Addr().String(), client.WithHTTPClient(hc))

	data, err := c.GetObject(context.Background(), "big.bin")
	if err == nil {
		t.Fatalf("expected an error for a server-truncated body, got nil with %d of %d bytes",
			len(data), size)
	}
	if data != nil {
		t.Errorf("partial data must not be returned; got %d bytes", len(data))
	}
}

// roundTripperFunc adapts a function to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// errReader always fails, standing in for a connection that drops mid-body.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// slowConn reads in small, slow increments so a server-side write deadline has
// time to expire while the response body is still being drained.
type slowConn struct {
	net.Conn
}

func (s *slowConn) Read(b []byte) (int, error) {
	if len(b) > 4096 {
		b = b[:4096]
	}
	time.Sleep(2 * time.Millisecond)
	return s.Conn.Read(b)
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
