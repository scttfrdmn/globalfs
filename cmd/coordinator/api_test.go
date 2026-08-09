package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/scttfrdmn/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// ── In-memory site client ─────────────────────────────────────────────────────

// testMemClient is a thread-safe in-memory ObjectFSClient for handler tests.
type testMemClient struct {
	mu        sync.Mutex
	objects   map[string][]byte
	getErr    error
	putErr    error
	delErr    error
	headErr   error
	listErr   error
	healthErr error

	// getDelay stalls Get before it returns, so a test can make a response take
	// longer to deliver than the server's WriteTimeout allows without needing a
	// payload large enough to be slow on its own merits (#75). Set before the
	// client is handed to a coordinator; not mutated afterwards.
	getDelay time.Duration
}

func newTestMemClient(objs map[string][]byte) *testMemClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &testMemClient{objects: objs}
}

func (m *testMemClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
	if m.getDelay > 0 {
		time.Sleep(m.getDelay)
	}
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found: " + key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *testMemClient) Put(_ context.Context, key string, data []byte) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp
	return nil
}

func (m *testMemClient) Delete(_ context.Context, key string) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *testMemClient) List(_ context.Context, prefix string, limit int) ([]objectfstypes.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []objectfstypes.ObjectInfo
	for k, v := range m.objects {
		if strings.HasPrefix(k, prefix) {
			result = append(result, objectfstypes.ObjectInfo{
				Key:          k,
				Size:         int64(len(v)),
				LastModified: time.Now(),
				ETag:         "etag-" + k,
			})
		}
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *testMemClient) Head(_ context.Context, key string) (*objectfstypes.ObjectInfo, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found: " + key)
	}
	return &objectfstypes.ObjectInfo{
		Key:          key,
		Size:         int64(len(data)),
		LastModified: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
		ETag:         "etag-" + key,
		Checksum:     "sha256-abc",
	}, nil
}

func (m *testMemClient) Health(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthErr
}
func (m *testMemClient) Close() error { return nil }

func (m *testMemClient) hasKey(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}

// ── Test coordinator factory ──────────────────────────────────────────────────

func makeTestCoordinator(t *testing.T, objs map[string][]byte) (*coordinator.Coordinator, *testMemClient) {
	t.Helper()
	mc := newTestMemClient(objs)
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s)
	c.Start(context.Background())
	t.Cleanup(c.Stop)
	return c, mc
}

// ── objectListHandler ─────────────────────────────────────────────────────────

func TestObjectList_OK(t *testing.T) {
	c, _ := makeTestCoordinator(t, map[string][]byte{
		"data/a.bam": []byte("reads-a"),
		"data/b.bam": []byte("reads-b"),
		"meta/c.txt": []byte("notes"),
	})

	req := httptest.NewRequest("GET", "/api/v1/objects", nil)
	w := httptest.NewRecorder()
	objectListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp listObjectsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 3 {
		t.Errorf("expected count=3, got %d", resp.Count)
	}
	if len(resp.Objects) != 3 {
		t.Errorf("expected 3 objects, got %d", len(resp.Objects))
	}
}

func TestObjectList_WithPrefix(t *testing.T) {
	c, _ := makeTestCoordinator(t, map[string][]byte{
		"data/a.bam": []byte("reads-a"),
		"data/b.bam": []byte("reads-b"),
		"meta/c.txt": []byte("notes"),
	})

	req := httptest.NewRequest("GET", "/api/v1/objects?prefix=data/", nil)
	w := httptest.NewRecorder()
	objectListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp listObjectsResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Prefix != "data/" {
		t.Errorf("prefix: got %q, want %q", resp.Prefix, "data/")
	}
	if resp.Count != 2 {
		t.Errorf("expected count=2, got %d", resp.Count)
	}
}

func TestObjectList_WithLimit(t *testing.T) {
	c, _ := makeTestCoordinator(t, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	})

	req := httptest.NewRequest("GET", "/api/v1/objects?limit=2", nil)
	w := httptest.NewRecorder()
	objectListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp listObjectsResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Count != 2 {
		t.Errorf("expected count=2, got %d", resp.Count)
	}
}

func TestObjectList_InvalidLimit(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	req := httptest.NewRequest("GET", "/api/v1/objects?limit=notanumber", nil)
	w := httptest.NewRecorder()
	objectListHandler(c)(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// ── objectGetHandler ──────────────────────────────────────────────────────────

func TestObjectGet_OK(t *testing.T) {
	c, _ := makeTestCoordinator(t, map[string][]byte{
		"data/genome.bam": []byte("ACGTACGT"),
	})

	req := httptest.NewRequest("GET", "/api/v1/objects/data/genome.bam", nil)
	req.SetPathValue("key", "data/genome.bam")
	w := httptest.NewRecorder()
	objectGetHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Body.String() != "ACGTACGT" {
		t.Errorf("body: got %q, want %q", w.Body.String(), "ACGTACGT")
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Content-Type: got %q, want application/octet-stream", ct)
	}
	if cl := w.Header().Get("Content-Length"); cl != "8" {
		t.Errorf("Content-Length: got %q, want 8", cl)
	}
}

func TestObjectGet_MissingKey(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	req := httptest.NewRequest("GET", "/api/v1/objects/", nil)
	req.SetPathValue("key", "")
	w := httptest.NewRecorder()
	objectGetHandler(c)(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestObjectGet_CoordinatorError(t *testing.T) {
	c, mc := makeTestCoordinator(t, nil)
	mc.getErr = errors.New("S3 unreachable")

	req := httptest.NewRequest("GET", "/api/v1/objects/any", nil)
	req.SetPathValue("key", "any")
	w := httptest.NewRecorder()
	objectGetHandler(c)(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

// ── objectPutHandler ──────────────────────────────────────────────────────────

func TestObjectPut_OK(t *testing.T) {
	c, mc := makeTestCoordinator(t, nil)

	body := []byte("hello world")
	req := httptest.NewRequest("PUT", "/api/v1/objects/greetings/hello.txt",
		bytes.NewReader(body))
	req.SetPathValue("key", "greetings/hello.txt")
	w := httptest.NewRecorder()
	objectPutHandler(c)(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if !mc.hasKey("greetings/hello.txt") {
		t.Error("expected key to be stored in mock client")
	}
}

func TestObjectPut_MissingKey(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	req := httptest.NewRequest("PUT", "/api/v1/objects/", bytes.NewReader([]byte("x")))
	req.SetPathValue("key", "")
	w := httptest.NewRecorder()
	objectPutHandler(c)(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestObjectPut_CoordinatorError(t *testing.T) {
	c, mc := makeTestCoordinator(t, nil)
	mc.putErr = errors.New("disk full")

	req := httptest.NewRequest("PUT", "/api/v1/objects/key",
		bytes.NewReader([]byte("data")))
	req.SetPathValue("key", "key")
	w := httptest.NewRecorder()
	objectPutHandler(c)(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

// ── objectDeleteHandler ───────────────────────────────────────────────────────

func TestObjectDelete_OK(t *testing.T) {
	c, mc := makeTestCoordinator(t, map[string][]byte{
		"reports/q1.csv": []byte("data"),
	})

	req := httptest.NewRequest("DELETE", "/api/v1/objects/reports/q1.csv", nil)
	req.SetPathValue("key", "reports/q1.csv")
	w := httptest.NewRecorder()
	objectDeleteHandler(c)(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
	if mc.hasKey("reports/q1.csv") {
		t.Error("expected key to be removed from mock client")
	}
}

func TestObjectDelete_MissingKey(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	req := httptest.NewRequest("DELETE", "/api/v1/objects/", nil)
	req.SetPathValue("key", "")
	w := httptest.NewRecorder()
	objectDeleteHandler(c)(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestObjectDelete_CoordinatorError(t *testing.T) {
	c, mc := makeTestCoordinator(t, map[string][]byte{"k": []byte("v")})
	mc.delErr = errors.New("network error")

	req := httptest.NewRequest("DELETE", "/api/v1/objects/k", nil)
	req.SetPathValue("key", "k")
	w := httptest.NewRecorder()
	objectDeleteHandler(c)(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

// ── objectHeadHandler ─────────────────────────────────────────────────────────

func TestObjectHead_OK(t *testing.T) {
	c, _ := makeTestCoordinator(t, map[string][]byte{
		"archive/data.tar.zst": []byte("compressed"),
	})

	req := httptest.NewRequest("HEAD", "/api/v1/objects/archive/data.tar.zst", nil)
	req.SetPathValue("key", "archive/data.tar.zst")
	w := httptest.NewRecorder()
	objectHeadHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// HEAD must not have a body.
	body, _ := io.ReadAll(w.Body)
	// httptest recorder captures body even for HEAD — the framework (not handler)
	// strips it for real HEAD responses; here we just check headers are correct.
	_ = body

	if cl := w.Header().Get("Content-Length"); cl != "10" {
		t.Errorf("Content-Length: got %q, want 10", cl)
	}
	if etag := w.Header().Get("ETag"); etag != "etag-archive/data.tar.zst" {
		t.Errorf("ETag: got %q", etag)
	}
	if ck := w.Header().Get("X-GlobalFS-Checksum"); ck != "sha256-abc" {
		t.Errorf("X-GlobalFS-Checksum: got %q", ck)
	}
	lm := w.Header().Get("Last-Modified")
	if lm == "" {
		t.Error("expected Last-Modified header")
	}
}

func TestObjectHead_CoordinatorError(t *testing.T) {
	c, mc := makeTestCoordinator(t, map[string][]byte{"k": []byte("v")})
	mc.headErr = errors.New("timeout")

	req := httptest.NewRequest("HEAD", "/api/v1/objects/k", nil)
	req.SetPathValue("key", "k")
	w := httptest.NewRecorder()
	objectHeadHandler(c)(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

// ── Integration: full mux routing ────────────────────────────────────────────

// TestObjectAPI_FullRoundtrip verifies PUT → GET → HEAD → DELETE via the mux.
func TestObjectAPI_FullRoundtrip(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	mux := http.NewServeMux()
	registerAPIRoutes(mux, context.Background(), c, nil, config.SecurityConfig{})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	key := "integration/genome.bam"
	payload := []byte("GATTACA")
	url := srv.URL + "/api/v1/objects/" + key

	// PUT
	putReq, _ := http.NewRequest("PUT", url, bytes.NewReader(payload))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: expected 201, got %d", putResp.StatusCode)
	}

	// GET
	getResp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	gotData, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET: expected 200, got %d", getResp.StatusCode)
	}
	if string(gotData) != string(payload) {
		t.Errorf("GET body: got %q, want %q", gotData, payload)
	}

	// HEAD
	headReq, _ := http.NewRequest("HEAD", url, nil)
	headResp, err := http.DefaultClient.Do(headReq)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD: expected 200, got %d", headResp.StatusCode)
	}
	if cl := headResp.Header.Get("Content-Length"); cl != "7" {
		t.Errorf("HEAD Content-Length: got %q, want 7", cl)
	}

	// LIST
	listResp, err := http.Get(srv.URL + "/api/v1/objects?prefix=integration/")
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	listBody, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("LIST: expected 200, got %d", listResp.StatusCode)
	}
	var listed listObjectsResponse
	json.Unmarshal(listBody, &listed)
	if listed.Count != 1 {
		t.Errorf("LIST count: got %d, want 1", listed.Count)
	}

	// DELETE
	delReq, _ := http.NewRequest("DELETE", url, nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: expected 204, got %d", delResp.StatusCode)
	}

	// GET after DELETE should fail
	getAfter, _ := http.Get(url)
	getAfter.Body.Close()
	if getAfter.StatusCode == http.StatusOK {
		t.Error("expected non-200 after DELETE")
	}
}

// ── apiKeyMiddleware ──────────────────────────────────────────────────────────

func TestAPIKeyMiddleware_NoKey_AllowsAll(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("")(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("no-key middleware: expected 200, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_CorrectKey_Passes(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	req.Header.Set(apiKeyHeader, "s3cr3t")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("correct key: expected 200, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_MissingKey_Returns401(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	// no key header
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("missing key: expected 401, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_WrongKey_Returns401(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	req.Header.Set(apiKeyHeader, "wrong-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: expected 401, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_HealthzExempt(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/healthz", nil)
	// deliberately omit the key
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("/healthz should be exempt from auth, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_ReadyzExempt(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("/readyz should be exempt from auth, got %d", w.Code)
	}
}

func TestAPIKeyMiddleware_401ResponseBody(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := apiKeyMiddleware("s3cr3t")(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("401 Content-Type: got %q, want application/json", ct)
	}
	var body struct{ Error string }
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode 401 body: %v", err)
	}
	if body.Error == "" {
		t.Error("expected non-empty error field in 401 response")
	}
}

// ── requestIDMiddleware ───────────────────────────────────────────────────────

func TestRequestIDMiddleware_GeneratesIDWhenAbsent(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	id := w.Header().Get(requestIDHeader)
	if id == "" {
		t.Error("expected X-Request-ID to be set in response when absent from request")
	}
}

func TestRequestIDMiddleware_EchoesIncomingID(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	req.Header.Set(requestIDHeader, "upstream-trace-42")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if got := w.Header().Get(requestIDHeader); got != "upstream-trace-42" {
		t.Errorf("X-Request-ID: got %q, want %q", got, "upstream-trace-42")
	}
}

func TestRequestIDMiddleware_UniqueIDsPerRequest(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(inner)

	const n = 20
	ids := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		id := w.Header().Get(requestIDHeader)
		if id == "" {
			t.Fatalf("request %d: empty X-Request-ID", i)
		}
		ids[id] = true
	}
	if len(ids) != n {
		t.Errorf("expected %d unique IDs, got %d unique among %d requests", n, len(ids), n)
	}
}

func TestRequestIDMiddleware_StoresIDInContext(t *testing.T) {
	var ctxID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID = requestIDFromCtx(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(inner)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(requestIDHeader, "ctx-check-id")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if ctxID != "ctx-check-id" {
		t.Errorf("context request ID: got %q, want %q", ctxID, "ctx-check-id")
	}
}

func TestRequestIDMiddleware_GeneratedIDMatchesContext(t *testing.T) {
	var ctxID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID = requestIDFromCtx(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(inner)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	respID := w.Header().Get(requestIDHeader)
	if respID == "" {
		t.Fatal("expected X-Request-ID in response")
	}
	if ctxID != respID {
		t.Errorf("context ID %q does not match response header ID %q", ctxID, respID)
	}
}

// ── loggingMiddleware ─────────────────────────────────────────────────────────

func TestLoggingMiddleware_PreservesStatusCode(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	handler := loggingMiddleware(inner)

	req := httptest.NewRequest("POST", "/api/v1/objects/key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status code: got %d, want 201", w.Code)
	}
}

func TestLoggingMiddleware_PreservesResponseBody(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "preserved")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	})
	handler := loggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Body.String() != "hello world" {
		t.Errorf("body: got %q, want %q", w.Body.String(), "hello world")
	}
	if w.Header().Get("X-Custom") != "preserved" {
		t.Errorf("X-Custom header not preserved: got %q", w.Header().Get("X-Custom"))
	}
}

func TestLoggingMiddleware_DefaultsTo200WhenNoWriteHeader(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("implicit 200"))
	})
	handler := loggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("implicit 200: got %d, want 200", w.Code)
	}
}

func TestLoggingMiddleware_ChainedWithRequestID(t *testing.T) {
	// Verify that when the two middlewares are chained, the logger sees the
	// request ID in the context (integration of the two middlewares).
	var capturedID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = requestIDFromCtx(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := requestIDMiddleware(loggingMiddleware(inner))

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	req.Header.Set(requestIDHeader, "chain-test-id")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if capturedID != "chain-test-id" {
		t.Errorf("chained middlewares: context ID = %q, want %q", capturedID, "chain-test-id")
	}
	if got := w.Header().Get(requestIDHeader); got != "chain-test-id" {
		t.Errorf("response X-Request-ID = %q, want %q", got, "chain-test-id")
	}
}

// ── infoHandler ───────────────────────────────────────────────────────────────

func TestInfoHandler_ReturnsJSON(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	startTime := time.Now().Add(-5 * time.Minute)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "1.2.3", startTime)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var info infoResponse
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatalf("unmarshal infoResponse: %v", err)
	}
	if info.Version != "1.2.3" {
		t.Errorf("version: got %q, want 1.2.3", info.Version)
	}
}

func TestInfoHandler_UptimePositive(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)
	startTime := time.Now().Add(-10 * time.Second)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", startTime)(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.UptimeSeconds < 10 {
		t.Errorf("uptime_seconds: got %f, want >= 10", info.UptimeSeconds)
	}
}

func TestInfoHandler_SitesCounted(t *testing.T) {
	mc1 := newTestMemClient(nil)
	mc2 := newTestMemClient(nil)
	s1 := site.New("primary", types.SiteRolePrimary, mc1)
	s2 := site.New("backup", types.SiteRoleBackup, mc2)
	c := coordinator.New(s1, s2)
	c.Start(context.Background())
	t.Cleanup(c.Stop)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", time.Now())(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.Sites != 2 {
		t.Errorf("sites: got %d, want 2", info.Sites)
	}
	if info.SitesByRole["primary"] != 1 {
		t.Errorf("sites_by_role.primary: got %d, want 1", info.SitesByRole["primary"])
	}
	if info.SitesByRole["backup"] != 1 {
		t.Errorf("sites_by_role.backup: got %d, want 1", info.SitesByRole["backup"])
	}
}

func TestInfoHandler_SingleNodeIsLeader(t *testing.T) {
	// Without a LeaseManager configured, IsLeader always returns true.
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", time.Now())(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if !info.IsLeader {
		t.Error("single-node coordinator: is_leader should be true")
	}
}

func TestInfoHandler_QueueDepthZero(t *testing.T) {
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", time.Now())(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.ReplicationQueueDepth != 0 {
		t.Errorf("queue depth: got %d, want 0", info.ReplicationQueueDepth)
	}
}

func TestInfoHandler_HealthSummary_NoCacheYet(t *testing.T) {
	// When no poll has run, health.last_checked_at should be absent and counts 0.
	mc := newTestMemClient(nil)
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s) // NOT started — no background poll
	t.Cleanup(c.Stop)

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", time.Now())(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.Health.LastCheckedAt != nil {
		t.Errorf("expected nil LastCheckedAt before first poll, got %v", info.Health.LastCheckedAt)
	}
	if info.Health.Healthy != 0 || info.Health.Unhealthy != 0 {
		t.Errorf("expected 0/0 before first poll, got healthy=%d unhealthy=%d",
			info.Health.Healthy, info.Health.Unhealthy)
	}
}

func TestInfoHandler_HealthSummary_AfterPoll(t *testing.T) {
	mc := newTestMemClient(nil)
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s)
	c.SetHealthPollInterval(10 * time.Millisecond)
	c.Start(context.Background())
	t.Cleanup(c.Stop)

	// Wait for the first poll.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if report, _ := c.HealthStatus(); report != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	w := httptest.NewRecorder()
	infoHandler(c, "v1", time.Now())(w, req)

	var info infoResponse
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.Health.LastCheckedAt == nil {
		t.Fatal("expected non-nil LastCheckedAt after poll")
	}
	if info.Health.Healthy != 1 || info.Health.Unhealthy != 0 {
		t.Errorf("expected 1 healthy, 0 unhealthy; got healthy=%d unhealthy=%d",
			info.Health.Healthy, info.Health.Unhealthy)
	}
}

// ── healthzHandler with cached health ─────────────────────────────────────────

func TestHealthzHandler_UsesCachedHealth(t *testing.T) {
	// Start coordinator with a fast poll so the cache is populated quickly.
	mc := newTestMemClient(nil)
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s)
	c.SetHealthPollInterval(10 * time.Millisecond)
	c.Start(context.Background())
	t.Cleanup(c.Stop)

	// Wait for at least one poll.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r, _ := c.HealthStatus(); r != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	healthzHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 from cached healthy result, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "OK") {
		t.Errorf("expected 'OK' in body, got %q", w.Body.String())
	}
}

func TestHealthzHandler_NoCacheUsesLiveCheck(t *testing.T) {
	// If the cache is nil, healthzHandler falls back to a live check.
	mc := newTestMemClient(nil)
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s) // NOT started — no cache
	t.Cleanup(c.Stop)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	healthzHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 from live check, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHealthzHandler_CacheDegradedSite(t *testing.T) {
	// Once the cache shows an unhealthy primary, healthz should return 503.
	mc := newTestMemClient(nil)
	mc.healthErr = errors.New("disk full")
	s := site.New("primary", types.SiteRolePrimary, mc)
	c := coordinator.New(s)
	c.SetHealthPollInterval(10 * time.Millisecond)
	c.Start(context.Background())
	t.Cleanup(c.Stop)

	// Wait for cache to reflect the error.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r, _ := c.HealthStatus(); r != nil && r["primary"] != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	healthzHandler(c)(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for cached degraded primary, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "DEGRADED") {
		t.Errorf("expected 'DEGRADED' in body, got %q", w.Body.String())
	}
}

// ── objectPutHandler: body size limit (#27) ───────────────────────────────────

func TestObjectPut_BodyTooLarge(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	// Build a body that is exactly one byte over the limit.
	body := make([]byte, maxObjectBodyBytes+1)
	req := httptest.NewRequest("PUT", "/api/v1/objects/big", bytes.NewReader(body))
	req.SetPathValue("key", "big")
	w := httptest.NewRecorder()
	objectPutHandler(c)(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", w.Code)
	}
}

// ── addSiteHandler ────────────────────────────────────────────────────────────

func TestAddSite_MissingName(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"s3_bucket":"bucket","s3_region":"us-west-2"}`
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing name: expected 400, got %d", w.Code)
	}
}

func TestAddSite_MissingBucket(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"name":"site2","s3_region":"us-west-2"}`
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing bucket: expected 400, got %d", w.Code)
	}
}

func TestAddSite_InvalidRole(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"name":"site2","s3_bucket":"b","s3_region":"us-west-2","role":"invalid"}`
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid role: expected 400, got %d", w.Code)
	}
}

func TestAddSite_InvalidJSON(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader("{not json}"))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: expected 400, got %d", w.Code)
	}
}

func TestAddSite_S3Unreachable_Returns502(t *testing.T) {
	// Provides a syntactically valid request that will fail to connect.
	// We use a context with a short timeout so the test does not hang.
	//
	// The endpoint is loopback, which the #76 SSRF guard blocks by default — so
	// this test now allowlists that host explicitly. That is the point of the
	// allowlist: reaching an unreachable loopback port is a legitimate thing for
	// an operator to configure, and an unremarkable thing for a caller to be
	// denied. Without the allowlist entry this request is a 400, and
	// TestAddSite_EndpointRejected_Loopback asserts exactly that.
	c, _ := makeTestCoordinator(t, nil)

	body := `{"name":"remote","s3_bucket":"bucket","s3_region":"us-east-1","s3_endpoint":"http://127.0.0.1:19999"}`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body)).WithContext(ctx)
	w := httptest.NewRecorder()
	sec := config.SecurityConfig{AllowedEndpointHosts: []string{"127.0.0.1"}}
	addSiteHandler(context.Background(), c, sec)(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("unreachable S3: expected 502, got %d: %s", w.Code, w.Body.String())
	}

	// The 502 body must not carry the transport error: an open non-HTTP port and
	// a closed one produce different messages, which is a scan oracle (#76).
	body502 := w.Body.String()
	for _, leak := range []string{"connection refused", "dial tcp", "127.0.0.1", "malformed HTTP response"} {
		if strings.Contains(body502, leak) {
			t.Errorf("502 body leaks transport detail %q: %s", leak, body502)
		}
	}
}

// ── s3_endpoint SSRF guard (#76) ──────────────────────────────────────────────

// TestAddSite_EndpointRejected_NoSignedRequestSent is the end-to-end assertion
// for #76: pointing s3_endpoint at a listener the caller controls must not cause
// the coordinator to send it anything.
//
// Pre-fix the handler passed the endpoint straight to objectfssdk.WithEndpoint
// and site.NewFromConfig performed a HeadBucket against it, delivering a live
// SigV4 Authorization header derived from the coordinator's own AWS credentials.
// The listener here stands in for the attacker's host: receiving *any* request
// on it is the failure, so the assertion is a request count of zero rather than
// a status code. It is allowlisted so that only the address checks are out of
// the way — the guard's URL-shape rules still apply, and the endpoint is
// otherwise a perfectly ordinary one.
func TestAddSite_EndpointRejected_NoSignedRequestSent(t *testing.T) {
	var mu sync.Mutex
	var received []string

	victim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, r.Method+" "+r.URL.Path+" auth="+r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer victim.Close()

	c, _ := makeTestCoordinator(t, nil)

	// Not allowlisted, and loopback: must be rejected before anything is signed.
	body := `{"name":"probe","s3_bucket":"probe","s3_region":"us-west-2","s3_endpoint":"` + victim.URL + `"}`
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 0 {
		t.Errorf("coordinator sent %d request(s) to a caller-chosen host — SSRF with "+
			"signed credentials (#76): %v", len(received), received)
	}
}

// TestAddSite_EndpointRejected covers the endpoint shapes the handler must refuse
// before signing anything, and asserts the response body carries no detail about
// why (the reason goes to the log; see errEndpointRejected).
func TestAddSite_EndpointRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		endpoint string
	}{
		{"link-local IMDS", "http://169.254.169.254"},
		{"IMDS with path", "http://169.254.169.254/latest/meta-data/"},
		{"loopback v4", "http://127.0.0.1:9000"},
		{"loopback name", "http://localhost:9000"},
		{"loopback v6", "http://[::1]:9000"},
		{"private RFC1918 10/8", "http://10.0.0.5:9000"},
		{"private RFC1918 192.168/16", "http://192.168.1.10:9000"},
		{"private RFC1918 172.16/12", "http://172.16.0.1:9000"},
		{"CGNAT 100.64/10", "http://100.64.0.1:9000"},
		{"IPv6 link-local", "http://[fe80::1]:9000"},
		{"IPv6 unique-local", "http://[fd00::1]:9000"},
		{"unspecified v4", "http://0.0.0.0:9000"},
		{"file scheme", "file:///etc/passwd"},
		{"gopher scheme", "gopher://10.0.0.1:70"},
		{"no scheme", "169.254.169.254"},
		{"scheme only", "http://"},
		{"userinfo", "http://user:pass@s3.example.com"},
		{"query string", "http://s3.example.com?x=1"},
		{"fragment", "http://s3.example.com#f"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := makeTestCoordinator(t, nil)

			body := `{"name":"x","s3_bucket":"b","s3_region":"us-west-2","s3_endpoint":"` + tc.endpoint + `"}`
			req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body))
			w := httptest.NewRecorder()
			addSiteHandler(context.Background(), c, config.SecurityConfig{})(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("endpoint %q: got %d, want 400: %s", tc.endpoint, w.Code, w.Body.String())
			}
			// The response must not explain itself: "connection refused" vs
			// "malformed HTTP response" vs "link-local" are all distinguishing
			// signals a scanner can read.
			for _, leak := range []string{"link-local", "loopback", "private", "resolve", "dial"} {
				if strings.Contains(strings.ToLower(w.Body.String()), leak) {
					t.Errorf("endpoint %q: response body leaks the reason %q: %s",
						tc.endpoint, leak, w.Body.String())
				}
			}
			if got := len(c.Sites()); got != 1 {
				t.Errorf("site count changed to %d; the rejected site was registered", got)
			}
		})
	}
}

// TestValidateS3Endpoint_Allowed covers the values that must keep working: an
// empty endpoint (use AWS's default), an ordinary public host, and the two
// escape hatches operators need.
func TestValidateS3Endpoint_Allowed(t *testing.T) {
	t.Parallel()

	// A resolver that answers deterministically, so the test does not depend on
	// DNS. "public.example" is public; "internal.example" is RFC1918.
	resolve := func(_ context.Context, host string) ([]net.IPAddr, error) {
		switch host {
		case "public.example":
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		case "internal.example":
			return []net.IPAddr{{IP: net.ParseIP("10.1.2.3")}}, nil
		}
		return nil, errors.New("no such host")
	}

	cases := []struct {
		name     string
		endpoint string
		sec      config.SecurityConfig
	}{
		{"empty means AWS default", "", config.SecurityConfig{}},
		{"public host", "https://public.example", config.SecurityConfig{}},
		{"public host with trailing slash", "https://public.example/", config.SecurityConfig{}},
		{"public IP literal", "https://93.184.216.34", config.SecurityConfig{}},
		{"private allowed by opt-in", "http://10.0.0.5:9000",
			config.SecurityConfig{AllowPrivateEndpoints: true}},
		{"private DNS name allowed by opt-in", "http://internal.example:9000",
			config.SecurityConfig{AllowPrivateEndpoints: true}},
		{"loopback allowed by allowlist", "http://127.0.0.1:4566",
			config.SecurityConfig{AllowedEndpointHosts: []string{"127.0.0.1"}}},
		{"allowlist is case-insensitive", "http://MinIO.Local:9000",
			config.SecurityConfig{AllowedEndpointHosts: []string{"minio.local"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if reason, err := validateS3Endpoint(context.Background(), tc.endpoint, tc.sec, resolve); err != nil {
				t.Errorf("validateS3Endpoint(%q) = %v (%s), want nil", tc.endpoint, err, reason)
			}
		})
	}
}

// TestValidateS3Endpoint_ResolvedAddressIsChecked is the half of #76 that string
// matching would miss: a public-looking DNS name whose A record points into
// private or link-local space. The check runs after resolution, and every answer
// must pass — one public address does not license a second internal one.
func TestValidateS3Endpoint_ResolvedAddressIsChecked(t *testing.T) {
	t.Parallel()

	resolve := func(_ context.Context, host string) ([]net.IPAddr, error) {
		switch host {
		case "imds.attacker.example":
			return []net.IPAddr{{IP: net.ParseIP("169.254.169.254")}}, nil
		case "internal.attacker.example":
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		case "mixed.attacker.example":
			// One public answer and one internal: must still be rejected.
			return []net.IPAddr{
				{IP: net.ParseIP("93.184.216.34")},
				{IP: net.ParseIP("169.254.169.254")},
			}, nil
		case "empty.example":
			return nil, nil
		}
		return nil, errors.New("no such host")
	}

	for _, host := range []string{
		"imds.attacker.example",
		"internal.attacker.example",
		"mixed.attacker.example",
		"empty.example",
		"nxdomain.example",
	} {
		endpoint := "https://" + host
		reason, err := validateS3Endpoint(context.Background(), endpoint, config.SecurityConfig{}, resolve)
		if err == nil {
			t.Errorf("validateS3Endpoint(%q) = nil, want rejection — a DNS name pointing at "+
				"an internal address must be caught after resolution (#76)", endpoint)
			continue
		}
		if !errors.Is(err, errEndpointRejected) {
			t.Errorf("validateS3Endpoint(%q) returned %v, want errEndpointRejected", endpoint, err)
		}
		if reason == "" {
			t.Errorf("validateS3Endpoint(%q) gave no reason to log", endpoint)
		}
	}
}

// TestIsDisallowedAddr_PrivateOptInDoesNotUnblockLinkLocal pins the deliberate
// asymmetry in the opt-in: allow_private_endpoints exists for in-cluster MinIO on
// an RFC1918 address, and must not thereby open 169.254.169.254 or loopback.
func TestIsDisallowedAddr_PrivateOptInDoesNotUnblockLinkLocal(t *testing.T) {
	t.Parallel()

	stillBlocked := []string{
		"169.254.169.254", // IMDS
		"127.0.0.1",
		"::1",
		"fe80::1",
		"0.0.0.0",
		"224.0.0.1", // multicast
	}
	for _, s := range stillBlocked {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test address %q", s)
		}
		if bad, _ := isDisallowedAddr(ip, true); !bad {
			t.Errorf("isDisallowedAddr(%s, allowPrivate=true) = false; "+
				"the private opt-in must not unblock this class (#76)", s)
		}
	}

	// And the addresses the opt-in is actually for do become allowed.
	for _, s := range []string{"10.0.0.5", "192.168.1.1", "172.16.0.1", "fd00::1", "100.64.0.1"} {
		ip := net.ParseIP(s)
		if bad, _ := isDisallowedAddr(ip, false); !bad {
			t.Errorf("isDisallowedAddr(%s, allowPrivate=false) = false, want blocked", s)
		}
		if bad, reason := isDisallowedAddr(ip, true); bad {
			t.Errorf("isDisallowedAddr(%s, allowPrivate=true) = true (%s), want allowed", s, reason)
		}
	}
}

// ── removeSiteHandler ─────────────────────────────────────────────────────────

func TestRemoveSite_NotFound(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("DELETE", "/api/v1/sites/nonexistent", nil)
	req.SetPathValue("name", "nonexistent")
	w := httptest.NewRecorder()
	removeSiteHandler(c)(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("missing site: expected 404, got %d", w.Code)
	}
}

func TestRemoveSite_MissingNameInPath(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("DELETE", "/api/v1/sites/", nil)
	req.SetPathValue("name", "")
	w := httptest.NewRecorder()
	removeSiteHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("empty name: expected 400, got %d", w.Code)
	}
}

func TestRemoveSite_Success(t *testing.T) {
	t.Parallel()
	// Coordinator starts with one primary site named "primary".
	c, _ := makeTestCoordinator(t, nil)

	if len(c.Sites()) != 1 {
		t.Fatalf("expected 1 site before removal, got %d", len(c.Sites()))
	}

	req := httptest.NewRequest("DELETE", "/api/v1/sites/primary", nil)
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()
	removeSiteHandler(c)(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
	if len(c.Sites()) != 0 {
		t.Errorf("expected 0 sites after removal, got %d", len(c.Sites()))
	}
}

// ── replicateHandler ──────────────────────────────────────────────────────────

func TestReplicate_MissingKey(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"from":"primary","to":"backup"}`
	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader(body))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing key: expected 400, got %d", w.Code)
	}
}

func TestReplicate_MissingFrom(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"key":"data/file.bam","to":"backup"}`
	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader(body))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing from: expected 400, got %d", w.Code)
	}
}

func TestReplicate_MissingTo(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	body := `{"key":"data/file.bam","from":"primary"}`
	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader(body))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing to: expected 400, got %d", w.Code)
	}
}

func TestReplicate_InvalidJSON(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader("{not json}"))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: expected 400, got %d", w.Code)
	}
}

func TestReplicate_UnknownSourceSite(t *testing.T) {
	t.Parallel()
	// Coordinator has only "primary"; "backup" does not exist as source.
	mc2 := newTestMemClient(nil)
	s2 := site.New("backup", types.SiteRoleBackup, mc2)
	c, _ := makeTestCoordinator(t, nil)
	c.AddSite(s2)

	body := `{"key":"data/file.bam","from":"unknown","to":"backup"}`
	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader(body))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("unknown from: expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestReplicate_Success(t *testing.T) {
	t.Parallel()
	mc2 := newTestMemClient(nil)
	s2 := site.New("backup", types.SiteRoleBackup, mc2)
	c, _ := makeTestCoordinator(t, nil)
	c.AddSite(s2)

	body := `{"key":"data/genome.bam","from":"primary","to":"backup"}`
	req := httptest.NewRequest("POST", "/api/v1/replicate", strings.NewReader(body))
	w := httptest.NewRecorder()
	replicateHandler(c)(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp replicateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("status: got %q, want %q", resp.Status, "accepted")
	}
	if resp.Key != "data/genome.bam" {
		t.Errorf("key: got %q, want %q", resp.Key, "data/genome.bam")
	}
}

// ── sitesListHandler circuit_state tests ──────────────────────────────────────

// TestSitesList_NoCircuitBreaker verifies that circuit_state is absent from
// the JSON response when no circuit breaker is configured.
func TestSitesList_NoCircuitBreaker(t *testing.T) {
	t.Parallel()
	cli := newTestMemClient(nil)
	c := coordinator.New(site.New("primary", types.SiteRolePrimary, cli))

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	sitesListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var sites []struct {
		Name         string `json:"name"`
		CircuitState string `json:"circuit_state"`
	}
	if err := json.NewDecoder(w.Body).Decode(&sites); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sites) != 1 {
		t.Fatalf("expected 1 site, got %d", len(sites))
	}
	if sites[0].CircuitState != "" {
		t.Errorf("circuit_state should be absent without CB, got %q", sites[0].CircuitState)
	}
}

// TestSitesList_WithCircuitBreaker_Closed verifies that circuit_state is
// "closed" for a healthy site when a circuit breaker is registered.
func TestSitesList_WithCircuitBreaker_Closed(t *testing.T) {
	t.Parallel()

	cli := newTestMemClient(nil)
	c := coordinator.New(site.New("primary", types.SiteRolePrimary, cli))

	cb := circuitbreaker.New(5, 30*time.Second)
	c.SetCircuitBreaker(cb)

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	sitesListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var sites []struct {
		Name         string `json:"name"`
		CircuitState string `json:"circuit_state"`
	}
	if err := json.NewDecoder(w.Body).Decode(&sites); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sites) == 0 {
		t.Fatal("expected at least one site")
	}
	if sites[0].CircuitState != "closed" {
		t.Errorf("circuit_state: got %q, want %q", sites[0].CircuitState, "closed")
	}
}

// TestSitesList_WithCircuitBreaker_Open verifies that circuit_state is "open"
// after the threshold of failures is exceeded.
func TestSitesList_WithCircuitBreaker_Open(t *testing.T) {
	t.Parallel()

	cli := newTestMemClient(nil)
	c := coordinator.New(site.New("primary", types.SiteRolePrimary, cli))

	cb := circuitbreaker.New(2, 30*time.Second) // open after 2 failures
	c.SetCircuitBreaker(cb)

	// Record enough failures to open the circuit.
	cb.RecordFailure("primary")
	cb.RecordFailure("primary")

	req := httptest.NewRequest("GET", "/api/v1/sites", nil)
	w := httptest.NewRecorder()
	sitesListHandler(c)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var sites []struct {
		Name         string `json:"name"`
		CircuitState string `json:"circuit_state"`
	}
	if err := json.NewDecoder(w.Body).Decode(&sites); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sites) == 0 {
		t.Fatal("expected at least one site")
	}
	if sites[0].CircuitState != "open" {
		t.Errorf("circuit_state: got %q, want %q", sites[0].CircuitState, "open")
	}
}

// ── Transfer deadlines (#75) ──────────────────────────────────────────────────

// TestTransferDeadline_Sizing covers the budget arithmetic: proportional to size
// at minTransferThroughputBytesPerSec, clamped at both ends.
func TestTransferDeadline_Sizing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		size int64
		want time.Duration
	}{
		{"unknown length gets the floor", -1, minTransferDeadline},
		{"zero gets the floor", 0, minTransferDeadline},
		{"small object gets the floor", 4096, minTransferDeadline},
		{"floor holds up to its equivalent size", 30 << 20, minTransferDeadline},
		{"64 MiB at 1 MiB/s", 64 << 20, 64 * time.Second},
		{"documented 256 MiB cap is reachable", maxObjectBodyBytes, 256 * time.Second},
		{"absurd size is capped", 1 << 40, maxTransferDeadline},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := transferDeadline(tc.size); got != tc.want {
				t.Errorf("transferDeadline(%d) = %v, want %v", tc.size, got, tc.want)
			}
		})
	}
}

// TestTransferDeadline_SizeCapIsReachable is the consistency check the issue asks
// for: an advertised limit that cannot be reached at realistic bandwidth is a
// defect. The budget for a maximum-size body must be at least the time that body
// takes at the throughput floor.
func TestTransferDeadline_SizeCapIsReachable(t *testing.T) {
	t.Parallel()

	needed := time.Duration(maxObjectBodyBytes/minTransferThroughputBytesPerSec) * time.Second
	if got := transferDeadline(maxObjectBodyBytes); got < needed {
		t.Errorf("a %d-byte body gets %v but needs %v at %d B/s — the documented cap "+
			"is unreachable (#75)", maxObjectBodyBytes, got, needed, minTransferThroughputBytesPerSec)
	}
	if maxTransferDeadline < needed {
		t.Errorf("maxTransferDeadline (%v) is below the time a maximum-size body needs "+
			"(%v): the cap and the timeout contradict each other", maxTransferDeadline, needed)
	}
}

// TestStatusRecorder_UnwrapReachesDeadlineControl is a small test for the trap
// that makes the rest of #75's fix a no-op.
//
// loggingMiddleware and withObjectMetrics both wrap the ResponseWriter in a
// statusRecorder, and http.ResponseController finds SetWriteDeadline by walking
// Unwrap. Without statusRecorder.Unwrap every deadline call returns
// ErrNotSupported and quietly does nothing, so the handlers keep the 10s
// server-wide deadline and the bug survives a fix that looks correct. Verified to
// fail with the Unwrap method removed.
func TestStatusRecorder_UnwrapReachesDeadlineControl(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 1)
	srv := httptest.NewServer(withObjectMetrics("get", nil,
		loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Two statusRecorder layers deep, as in the real chain.
			errCh <- http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Minute))
			w.WriteHeader(http.StatusOK)
		})).ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()

	if err := <-errCh; err != nil {
		t.Errorf("SetWriteDeadline through the middleware chain: %v — the deadline "+
			"never reaches the connection, so #75 is unfixed", err)
	}
}

// TestObjectGet_LargeBodyNotTruncated is the end-to-end regression test for #75.
//
// It runs a real http.Server with WriteTimeout deliberately far shorter than the
// response takes to deliver, which is the production shape of the bug: an
// absolute deadline on the whole response rather than an idle one. The handler
// must extend it per request and the client must receive every byte.
//
// The coordinator's Get is made slow rather than the object made huge: a 64 MiB
// payload would be needed to beat a 10s timeout honestly, and a slow Get with a
// 1s timeout tests the same deadline arithmetic in a second rather than a minute.
func TestObjectGet_LargeBodyNotTruncated(t *testing.T) {
	// The payload has to exceed the socket buffers, or the whole response is
	// buffered by the kernel and returns before any deadline could bite.
	const size = 8 << 20
	payload := bytes.Repeat([]byte("A"), size)

	c, mc := makeTestCoordinator(t, map[string][]byte{"big.bin": payload})
	mc.getDelay = 2 * time.Second // exceeds the server's WriteTimeout below

	mux := http.NewServeMux()
	registerAPIRoutes(mux, context.Background(), c, nil, config.SecurityConfig{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler:      buildHandler(mux, ""),
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second, // shorter than the response takes
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	resp, err := http.Get("http://" + ln.Addr().String() + "/api/v1/objects/big.bin")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if got := resp.ContentLength; got != size {
		t.Fatalf("Content-Length: got %d, want %d", got, size)
	}

	// io.ReadAll's error is the whole point: a truncated body shows up here as
	// "unexpected EOF" against an accurate Content-Length, which is exactly what
	// the issue reports.
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v — the response was truncated after %d of %d bytes "+
			"because WriteTimeout is an absolute deadline (#75)", err, len(got), size)
	}
	if len(got) != size {
		t.Errorf("body length: got %d, want %d (truncated mid-body, #75)", len(got), size)
	}
	if !bytes.Equal(got, payload) {
		t.Error("body content differs from the stored object")
	}
}

// TestObjectPut_SlowUploadNotCutOff is the same defect in the read direction: a
// body arriving more slowly than ReadTimeout allows must still be accepted, which
// is what makes the documented 256 MiB cap reachable below 25.6 MiB/s.
func TestObjectPut_SlowUploadNotCutOff(t *testing.T) {
	const size = 1 << 20
	payload := bytes.Repeat([]byte("B"), size)

	c, mc := makeTestCoordinator(t, nil)

	mux := http.NewServeMux()
	registerAPIRoutes(mux, context.Background(), c, nil, config.SecurityConfig{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler:      buildHandler(mux, ""),
		ReadTimeout:  1 * time.Second, // shorter than the upload takes
		WriteTimeout: 1 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// A body delivered in chunks over ~2s, with an accurate Content-Length so the
	// handler can size the deadline from it.
	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		const chunks = 8
		for i := 0; i < chunks; i++ {
			if _, err := pw.Write(payload[i*size/chunks : (i+1)*size/chunks]); err != nil {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()

	req, err := http.NewRequest(http.MethodPut,
		"http://"+ln.Addr().String()+"/api/v1/objects/slow.bin", pr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.ContentLength = size

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v — a slow upload was cut off by the server-wide "+
			"ReadTimeout (#75)", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d (%s), want 201", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !mc.hasKey("slow.bin") {
		t.Error("object was not stored")
	}
}

// ── Path traversal guard (#73) ────────────────────────────────────────────────

// makeTestServer serves the real middleware chain — buildHandler over the real
// route table — so these tests exercise what the daemon exercises, including the
// ServeMux path-cleaning step that is the whole subject of #73. Assembling the
// handlers by hand, as most tests in this file do, would bypass the mux and
// therefore bypass the bug.
func makeTestServer(t *testing.T, objs map[string][]byte, apiKey string) (*httptest.Server, *coordinator.Coordinator, *testMemClient) {
	t.Helper()
	c, mc := makeTestCoordinator(t, objs)
	mux := http.NewServeMux()
	registerAPIRoutes(mux, context.Background(), c, nil, config.SecurityConfig{})
	srv := httptest.NewServer(buildHandler(mux, apiKey))
	t.Cleanup(srv.Close)
	return srv, c, mc
}

// noRedirectClient is an http.Client that surfaces a 3xx instead of following
// it. Following is what makes #73 exploitable — Go's client replays the method
// and the X-GlobalFS-API-Key header on a 307 — so a test that follows redirects
// cannot distinguish "rejected" from "redirected, then succeeded elsewhere".
func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// TestPathTraversal_RejectedNotRedirected is the core regression test for #73.
//
// Each hostile target must produce a 400 from the guard. Before the fix the
// literal forms produced a 307 whose Location pointed at a *different route*,
// which the supported client then followed with the API key attached — so
// asserting "no Location header, and not a 3xx" is what fails on the pre-fix
// tree, where merely asserting "not 2xx" would have passed.
func TestPathTraversal_RejectedNotRedirected(t *testing.T) {
	t.Parallel()
	srv, _, _ := makeTestServer(t, map[string][]byte{"data/x": []byte("payload")}, "")
	client := noRedirectClient()

	cases := []struct {
		name   string
		method string
		target string
	}{
		// Literal "..": the mux used to 307 these onto /api/v1/sites/*, turning
		// an object-scoped call into a site deregistration.
		{"literal dotdot crosses to sites", http.MethodDelete, "/api/v1/objects/../sites/primary"},
		{"literal dotdot multi segment", http.MethodDelete, "/api/v1/objects/a/b/../../../sites/primary"},
		{"literal dotdot on GET reads site inventory", http.MethodGet, "/api/v1/objects/../sites"},
		{"literal dotdot on PUT", http.MethodPut, "/api/v1/objects/tenants/A/../B/owned.txt"},
		{"literal dotdot on HEAD", http.MethodHead, "/api/v1/objects/../sites"},
		// Trailing "..": used to 307 to /api/v1/objects/, i.e. a request for one
		// key answered as a request for another. The issue is explicit that 400 is
		// the wanted outcome here, not a redirect.
		{"trailing dotdot", http.MethodDelete, "/api/v1/objects/data/.."},
		// Encoded forms. These already reached the handler pre-fix and were caught
		// by validateObjectKey; they must stay rejected.
		{"encoded dotdot upper", http.MethodDelete, "/api/v1/objects/%2E%2E/sites/primary"},
		{"encoded dotdot lower", http.MethodDelete, "/api/v1/objects/%2e%2e/sites/primary"},
		{"mixed encoded dotdot", http.MethodDelete, "/api/v1/objects/%2e./sites/primary"},
		{"encoded separator and dotdot", http.MethodDelete, "/api/v1/objects/%2F%2E%2E%2Fsites/primary"},
		// Non-object routes get the same treatment; no route here has a
		// legitimate ".." component.
		{"traversal under sites", http.MethodDelete, "/api/v1/sites/../sites/primary"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(tc.method, srv.URL+tc.target, strings.NewReader("body"))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if loc := resp.Header.Get("Location"); loc != "" {
				t.Errorf("%s %s: served a redirect to %q — a client that follows it "+
					"crosses the authorization boundary (#73)", tc.method, tc.target, loc)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %s: got %d, want 400", tc.method, tc.target, resp.StatusCode)
			}
		})
	}
}

// TestPathTraversal_DoesNotReachSiteHandlers is the impact assertion: the exact
// call from the issue must leave the site inventory intact. Pre-fix this
// returned 204 and removed the site.
func TestPathTraversal_DoesNotReachSiteHandlers(t *testing.T) {
	t.Parallel()
	srv, c, _ := makeTestServer(t, nil, "")

	if got := len(c.Sites()); got != 1 {
		t.Fatalf("precondition: expected 1 site, got %d", got)
	}

	// A client that *does* follow redirects — pkg/client's behaviour, and the
	// behaviour that made this exploitable.
	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/objects/../sites/primary", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		t.Errorf("traversal returned 204 — the site deregistration succeeded (#73)")
	}
	if got := len(c.Sites()); got != 1 {
		t.Errorf("site count went 1 -> %d: an object-scoped DELETE deregistered a site (#73)", got)
	}
}

// TestPathTraversal_AuthCheckedBeforePathGuard pins the middleware order. The
// guard sits inside apiKeyMiddleware, so an unauthenticated traversal probe must
// be answered 401 — a 400 would confirm to an unauthenticated caller that the
// path was parsed, and would mean the guard runs outside auth.
func TestPathTraversal_AuthCheckedBeforePathGuard(t *testing.T) {
	t.Parallel()
	srv, _, _ := makeTestServer(t, nil, "s3cret")
	client := noRedirectClient()

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/objects/../sites/primary", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated traversal: got %d, want 401", resp.StatusCode)
	}

	// With the key, the same request is rejected by the guard rather than served.
	req2, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/objects/../sites/primary", nil)
	req2.Header.Set(apiKeyHeader, "s3cret")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Errorf("authenticated traversal: got %d, want 400", resp2.StatusCode)
	}
}

// TestPathTraversal_LegitimateKeysStillWork guards against the fix over-reaching.
// S3 keys may contain dots in every arrangement except a bare ".." component, and
// a key holding a literal percent-encoded "%2E" (sent as "%252E") must survive:
// unescaping twice in the guard would reject it.
func TestPathTraversal_LegitimateKeysStillWork(t *testing.T) {
	t.Parallel()
	srv, _, mc := makeTestServer(t, nil, "")
	client := noRedirectClient()

	cases := []struct {
		name    string
		target  string
		wantKey string
	}{
		{"dotdot as substring", "/api/v1/objects/a..b", "a..b"},
		{"dotdot inside a segment", "/api/v1/objects/data/v1..2/file.bam", "data/v1..2/file.bam"},
		{"three dots is not a traversal", "/api/v1/objects/.../file", ".../file"},
		{"dots inside a filename", "/api/v1/objects/archive.tar.gz", "archive.tar.gz"},
		{"dotdot suffix on a segment", "/api/v1/objects/snapshot..", "snapshot.."},
		{"double-encoded percent survives", "/api/v1/objects/pct%252E%252E/f", "pct%2E%2E/f"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPut, srv.URL+tc.target, strings.NewReader("payload"))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("PUT %s: got %d (%s), want 201 — the guard rejected a legal S3 key",
					tc.target, resp.StatusCode, strings.TrimSpace(string(body)))
			}
			if !mc.hasKey(tc.wantKey) {
				t.Errorf("PUT %s: stored under some other key, want %q", tc.target, tc.wantKey)
			}
		})
	}
}

// TestHasUnsafePathSegment is the unit-level table for the guard's predicate,
// covering spellings that are awkward to drive through a live server.
func TestHasUnsafePathSegment(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path string
		want bool
	}{
		{"/api/v1/objects/data/x", false},
		{"/api/v1/objects/a..b", false},
		{"/api/v1/objects/...", false},
		{"/api/v1/objects/.", false},
		{"/api/v1/objects/%2E", false},   // a single encoded dot is a legal key
		{"/api/v1/objects/%2E%2E", true}, // encoded ".."
		{"/api/v1/objects/../x", true},
		{"/api/v1/objects/x/..", true},
		{"..", true},
		{"/api/v1/objects/%2e./x", true},
		{"/api/v1/objects/.%2e/x", true},
		{"/api/v1/objects/%2F%2E%2E%2F", true}, // encoded separator hiding a ".."
		{"/api/v1/objects/%00", true},          // null byte
		{"/api/v1/objects/%zz", true},          // malformed encoding: treat as hostile
		{"/api/v1/objects/pct%252E%252E", false},
	}

	for _, tc := range cases {
		if got := hasUnsafePathSegment(tc.path); got != tc.want {
			t.Errorf("hasUnsafePathSegment(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// TestWithObjectMetrics_NilMetrics verifies that withObjectMetrics calls the
// next handler without panicking when m is nil.
func TestWithObjectMetrics_NilMetrics(t *testing.T) {
	t.Parallel()
	called := false
	handler := withObjectMetrics("get", nil, func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler(w, r)
	if !called {
		t.Error("next handler was not called")
	}
	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}
}
