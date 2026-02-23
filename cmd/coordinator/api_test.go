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

func (m *testMemClient) Health(_ context.Context) error { return nil }
func (m *testMemClient) Close() error                   { return nil }

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
