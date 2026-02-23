package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	objectfstypes "github.com/objectfs/objectfs/pkg/types"

	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/coordinator"
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
}

func newTestMemClient(objs map[string][]byte) *testMemClient {
	if objs == nil {
		objs = make(map[string][]byte)
	}
	return &testMemClient{objects: objs}
}

func (m *testMemClient) Get(_ context.Context, key string, _, _ int64) ([]byte, error) {
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
	registerAPIRoutes(mux, context.Background(), c, nil)
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
	addSiteHandler(context.Background(), c)(w, req)

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
	addSiteHandler(context.Background(), c)(w, req)

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
	addSiteHandler(context.Background(), c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid role: expected 400, got %d", w.Code)
	}
}

func TestAddSite_InvalidJSON(t *testing.T) {
	t.Parallel()
	c, _ := makeTestCoordinator(t, nil)

	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader("{not json}"))
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: expected 400, got %d", w.Code)
	}
}

func TestAddSite_S3Unreachable_Returns502(t *testing.T) {
	// Provides a syntactically valid request that will fail to connect.
	// We use a context with a short timeout so the test does not hang.
	c, _ := makeTestCoordinator(t, nil)

	body := `{"name":"remote","s3_bucket":"bucket","s3_region":"us-east-1","s3_endpoint":"http://127.0.0.1:19999"}`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/api/v1/sites", strings.NewReader(body)).WithContext(ctx)
	w := httptest.NewRecorder()
	addSiteHandler(context.Background(), c)(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("unreachable S3: expected 502, got %d: %s", w.Code, w.Body.String())
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

