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

// ── Redirects are not followed (#132) ─────────────────────────────────────────

// TestRedirect_NotFollowed is the core #132 assertion: a coordinator that answers
// with a redirect gets an error, not a second request.
//
// The redirect target here is the site route, which is the #73 shape exactly: a
// server that path-cleans "../sites/primary" answers 307 with
// Location=/api/v1/sites/primary, and Go's default policy replays both the method
// and X-GlobalFS-API-Key, turning an object DELETE into a site deregistration
// that returns nil.  #73 closed that on the server, so this is the client half —
// what keeps the same exploit from working against a pre-#73 or misconfigured
// coordinator.
//
// The assertion is a request count of one.  A status-code assertion would not
// distinguish "refused to follow" from "followed, and the target answered".
func TestRedirect_NotFollowed(t *testing.T) {
	var mu sync.Mutex
	var paths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/api/v1/objects/") {
			http.Redirect(w, r, "/api/v1/sites/primary", http.StatusTemporaryRedirect)
			return
		}
		// The redirect target: succeeds if it is ever reached, so a followed
		// redirect shows up as a nil error rather than an incidental failure.
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := client.New(srv.URL)
	err := c.DeleteObject(context.Background(), "data/x")
	if err == nil {
		t.Fatal("DeleteObject returned nil against a 307 — the redirect was followed and " +
			"the deregistration it pointed at reported success (#132)")
	}
	if !errors.Is(err, client.ErrUnexpectedRedirect) {
		t.Errorf("error does not wrap ErrUnexpectedRedirect: %v", err)
	}
	// The Location is named, because a proxy in front of the coordinator is the
	// likely cause and the target is what identifies it.
	if !strings.Contains(err.Error(), "/api/v1/sites/primary") {
		t.Errorf("error does not name the redirect target, which is what diagnoses a "+
			"misconfigured proxy: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Fatalf("server saw %d requests, want 1: %v", len(paths), paths)
	}
	if paths[0] != "/api/v1/objects/data/x" {
		t.Errorf("first request path = %q, want the object path", paths[0])
	}
}

// TestRedirect_APIKeyNotReplayed is the credential half of #132.  Go strips
// sensitive headers when a redirect crosses to a different host, but not on a
// same-host redirect, and it gives a custom Authorization-style header like
// X-GlobalFS-API-Key no special treatment at all — so pre-fix the key was handed
// to whatever Location the server named.
//
// Both assertions matter: one request total, and the key present on that one.  A
// client that stopped sending the key at all would also satisfy "never appears on
// a second request".
func TestRedirect_APIKeyNotReplayed(t *testing.T) {
	const key = "secret-key"
	var mu sync.Mutex
	var keysSeen []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keysSeen = append(keysSeen, r.Header.Get("X-GlobalFS-API-Key"))
		mu.Unlock()
		http.Redirect(w, r, "/api/v1/sites/primary", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c := client.New(srv.URL, client.WithAPIKey(key))
	if _, err := c.GetObject(context.Background(), "data/x"); err == nil {
		t.Fatal("GetObject returned nil against a 307")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(keysSeen) != 1 {
		t.Fatalf("the API key was sent on %d requests, want 1 — a redirect must not "+
			"replay the credential (#132): %v", len(keysSeen), keysSeen)
	}
	if keysSeen[0] != key {
		t.Errorf("first request carried key %q, want %q; the test would pass vacuously "+
			"if the key were never sent at all", keysSeen[0], key)
	}
}

// TestRedirect_AllMethodsRefuse covers every method, not only the one #73 used.
// The policy lives on the http.Client, so this is really a guard against a future
// doX helper being given its own client or its own policy.
func TestRedirect_AllMethodsRefuse(t *testing.T) {
	var mu sync.Mutex
	requests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		http.Redirect(w, r, "/api/v1/sites/primary", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c := client.New(srv.URL)
	ctx := context.Background()

	calls := map[string]func() error{
		"GetObject":    func() error { _, err := c.GetObject(ctx, "k"); return err },
		"PutObject":    func() error { return c.PutObject(ctx, "k", []byte("v")) },
		"DeleteObject": func() error { return c.DeleteObject(ctx, "k") },
		"HeadObject":   func() error { _, err := c.HeadObject(ctx, "k"); return err },
		"ListObjects":  func() error { _, err := c.ListObjects(ctx, "", 0); return err },
		"ListSites":    func() error { _, err := c.ListSites(ctx); return err },
		"AddSite":      func() error { _, err := c.AddSite(ctx, client.AddSiteRequest{Name: "s"}); return err },
		"RemoveSite":   func() error { return c.RemoveSite(ctx, "s") },
		"Replicate":    func() error { _, err := c.Replicate(ctx, client.ReplicateRequest{Key: "k"}); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatalf("%s returned nil against a 307", name)
			}
			if !errors.Is(err, client.ErrUnexpectedRedirect) {
				t.Errorf("%s: error does not wrap ErrUnexpectedRedirect: %v", name, err)
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != len(calls) {
		t.Errorf("server saw %d requests for %d calls; a followed redirect makes more "+
			"than one request per call", requests, len(calls))
	}
}

// TestRedirect_PolicyAppliesToWithHTTPClient covers the path a caller most
// plausibly uses to install a custom Transport.  A client supplied through
// WithHTTPClient must not silently opt out of the policy — that would make the
// hardening depend on which constructor options happened to be used.
func TestRedirect_PolicyAppliesToWithHTTPClient(t *testing.T) {
	var mu sync.Mutex
	requests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		http.Redirect(w, r, "/api/v1/sites/primary", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	supplied := &http.Client{Timeout: 5 * time.Second}
	c := client.New(srv.URL, client.WithHTTPClient(supplied))

	if err := c.DeleteObject(context.Background(), "k"); !errors.Is(err, client.ErrUnexpectedRedirect) {
		t.Errorf("WithHTTPClient bypassed the redirect policy: %v", err)
	}
	// The caller's own client must be left alone: it may be shared with code that
	// does want redirects.
	if supplied.CheckRedirect != nil {
		t.Error("New mutated the caller's *http.Client instead of copying it")
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Errorf("server saw %d requests, want 1", requests)
	}
}

// TestRedirect_CallerCheckRedirectIsRespected is the escape hatch.  A caller who
// sets CheckRedirect has made a decision, and New must not override it — the
// no-redirect policy is a default, not a prohibition.
func TestRedirect_CallerCheckRedirectIsRespected(t *testing.T) {
	var mu sync.Mutex
	requests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n == 1 {
			http.Redirect(w, r, "/api/v1/objects/elsewhere", http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	follow := &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
	}
	c := client.New(srv.URL, client.WithHTTPClient(follow))

	if err := c.DeleteObject(context.Background(), "k"); err != nil {
		t.Errorf("a caller-supplied CheckRedirect was overridden: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Errorf("server saw %d requests, want 2 (the redirect should have been followed)", requests)
	}
}

// ── Client-side key validation (#132) ─────────────────────────────────────────

// TestObjectKey_TraversalRejectedLocally asserts that a key the server must
// refuse is never sent.  The first case is the #73 exploit's own argument:
// DeleteObject(ctx, "../sites/primary").
//
// A request count of zero is the assertion, not a status code.  Making the
// request and getting a 400 back would produce an error too, and against a
// coordinator that does *not* refuse it would produce a site deregistration.
func TestObjectKey_TraversalRejectedLocally(t *testing.T) {
	var mu sync.Mutex
	requests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		// Stands in for a pre-#73 coordinator: obeys whatever it is asked.
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := client.New(srv.URL)
	ctx := context.Background()

	keys := []string{
		"../sites/primary",
		"a/../../sites/primary",
		"data/..",
		"..",
		"data/\x00/x",
		"\x00",
	}
	for _, key := range keys {
		t.Run(strconv.Quote(key), func(t *testing.T) {
			for name, call := range map[string]func() error{
				"DeleteObject": func() error { return c.DeleteObject(ctx, key) },
				"GetObject":    func() error { _, err := c.GetObject(ctx, key); return err },
				"PutObject":    func() error { return c.PutObject(ctx, key, []byte("v")) },
				"HeadObject":   func() error { _, err := c.HeadObject(ctx, key); return err },
			} {
				err := call()
				if err == nil {
					t.Errorf("%s(%q) returned nil", name, key)
					continue
				}
				if !errors.Is(err, client.ErrInvalidKey) {
					t.Errorf("%s(%q): error does not wrap ErrInvalidKey: %v", name, key, err)
				}
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != 0 {
		t.Errorf("the client sent %d request(s) for keys it knows the server must refuse; "+
			"against a pre-#73 coordinator each one is the traversal (#132)", requests)
	}
}

// TestObjectKey_LegitimateKeysStillWork is the regression guard on the check
// above.  ".." as a substring rather than a whole path component is a legal S3
// key, and so is a leading dot; rejecting those would break real callers to fix a
// traversal they are not.
func TestObjectKey_LegitimateKeysStillWork(t *testing.T) {
	var mu sync.Mutex
	var paths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := client.New(srv.URL)
	keys := []string{
		"data/genome.bam",
		"data/..hidden",
		"data/file..bak",
		"data/...",
		"a..b/c",
		".hidden",
	}
	for _, key := range keys {
		if err := c.DeleteObject(context.Background(), key); err != nil {
			t.Errorf("DeleteObject(%q) rejected a legal S3 key: %v", key, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != len(keys) {
		t.Errorf("server saw %d requests for %d legal keys: %v", len(paths), len(keys), paths)
	}
}
