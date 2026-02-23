package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/scttfrdmn/globalfs/pkg/client"
	"gopkg.in/yaml.v3"
)

// newTestServer spins up an httptest.Server and returns its URL.
func newTestServer(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// runCmd executes a cobra command tree and returns captured stdout.
// addr is prepended as --coordinator-addr so the command hits the test server.
func runCmd(t *testing.T, addr string, args ...string) (string, error) {
	t.Helper()
	root := buildRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	// Inject --coordinator-addr before user-provided args.
	fullArgs := append([]string{"--coordinator-addr", addr}, args...)
	root.SetArgs(fullArgs)
	err := root.Execute()
	return buf.String(), err
}

// runCmdWithStdin is like runCmd but injects an io.Reader as stdin.
func runCmdWithStdin(t *testing.T, stdin io.Reader, addr string, args ...string) (string, error) {
	t.Helper()
	root := buildRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetIn(stdin)
	fullArgs := append([]string{"--coordinator-addr", addr}, args...)
	root.SetArgs(fullArgs)
	err := root.Execute()
	return buf.String(), err
}

// ─── version ──────────────────────────────────────────────────────────────────

func TestVersion_Text(t *testing.T) {
	out, err := runCmd(t, "http://localhost:0", "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.Contains(out, "globalfs") {
		t.Errorf("version output missing 'globalfs': %q", out)
	}
}

func TestVersion_JSON(t *testing.T) {
	out, err := runCmd(t, "http://localhost:0", "version", "--json")
	if err != nil {
		t.Fatalf("version --json: %v", err)
	}
	var v map[string]string
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("unmarshal version JSON: %v (output: %q)", err, out)
	}
	if v["version"] == "" {
		t.Error("version JSON missing 'version' key")
	}
}

// ─── parseS3URI ───────────────────────────────────────────────────────────────

func TestParseS3URI(t *testing.T) {
	cases := []struct {
		input   string
		bucket  string
		region  string
		wantErr bool
	}{
		{"s3://my-bucket?region=us-west-2", "my-bucket", "us-west-2", false},
		{"s3://bucket-only", "bucket-only", "", false},
		{"https://bucket", "", "", true},  // wrong scheme
		{"s3://", "", "", true},           // no bucket
	}
	for _, tc := range cases {
		bucket, region, _, err := parseS3URI(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseS3URI(%q): expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseS3URI(%q): unexpected error: %v", tc.input, err)
			continue
		}
		if bucket != tc.bucket {
			t.Errorf("parseS3URI(%q): bucket got %q, want %q", tc.input, bucket, tc.bucket)
		}
		if region != tc.region {
			t.Errorf("parseS3URI(%q): region got %q, want %q", tc.input, region, tc.region)
		}
	}
}

// ─── site list ────────────────────────────────────────────────────────────────

func TestSiteList_TableOutput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]siteInfo{
			{Name: "primary", Role: "primary", Healthy: true},
			{Name: "backup", Role: "backup", Healthy: false, Error: "timeout"},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "site", "list")
	if err != nil {
		t.Fatalf("site list: %v", err)
	}
	if !strings.Contains(out, "primary") {
		t.Errorf("expected 'primary' in output: %q", out)
	}
	if !strings.Contains(out, "degraded") {
		t.Errorf("expected 'degraded' in output: %q", out)
	}
}

func TestSiteList_JSONOutput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]siteInfo{
			{Name: "s1", Role: "primary", Healthy: true},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "site", "list", "--json")
	if err != nil {
		t.Fatalf("site list --json: %v", err)
	}
	var sites []siteInfo
	if err := json.Unmarshal([]byte(out), &sites); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if len(sites) != 1 || sites[0].Name != "s1" {
		t.Errorf("unexpected sites: %v", sites)
	}
}

// TestSiteList_WithCircuitState verifies that the CIRCUIT column is shown when
// any site carries a circuit_state in the response.
func TestSiteList_WithCircuitState(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]siteInfo{
			{Name: "primary", Role: "primary", Healthy: true, CircuitState: "closed"},
			{Name: "burst", Role: "burst", Healthy: true, CircuitState: "open"},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "site", "list")
	if err != nil {
		t.Fatalf("site list: %v", err)
	}
	if !strings.Contains(out, "CIRCUIT") {
		t.Errorf("expected CIRCUIT column header in output: %q", out)
	}
	if !strings.Contains(out, "closed") {
		t.Errorf("expected 'closed' state in output: %q", out)
	}
	if !strings.Contains(out, "open") {
		t.Errorf("expected 'open' state in output: %q", out)
	}
}

// TestSiteList_NoCircuitState verifies that the CIRCUIT column is absent when
// no site carries circuit state (no circuit breaker configured).
func TestSiteList_NoCircuitState(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]siteInfo{
			{Name: "primary", Role: "primary", Healthy: true},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "site", "list")
	if err != nil {
		t.Fatalf("site list: %v", err)
	}
	if strings.Contains(out, "CIRCUIT") {
		t.Errorf("CIRCUIT column should be absent without circuit state data: %q", out)
	}
}

// ─── site remove ─────────────────────────────────────────────────────────────

func TestSiteRemove_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/sites/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "site", "remove", "--name", "old-site")
	if err != nil {
		t.Fatalf("site remove: %v", err)
	}
	if !strings.Contains(out, "deregistered") {
		t.Errorf("expected 'deregistered' in output: %q", out)
	}
}

func TestSiteRemove_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/sites/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	})
	addr := newTestServer(t, mux)

	_, err := runCmd(t, addr, "site", "remove", "--name", "ghost")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

// ─── replicate ────────────────────────────────────────────────────────────────

func TestReplicate_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/replicate", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(replicateResponse{
			Status: "accepted",
			Key:    req["key"],
			From:   req["from"],
			To:     req["to"],
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "replicate", "--key", "data/genome.bam", "--from", "primary", "--to", "backup")
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if !strings.Contains(out, "accepted") {
		t.Errorf("expected 'accepted' in output: %q", out)
	}
}

func TestReplicate_MissingFlag(t *testing.T) {
	_, err := runCmd(t, "http://localhost:0", "replicate", "--key", "k")
	if err == nil {
		t.Fatal("expected error when --from/--to missing")
	}
}

// ─── status ───────────────────────────────────────────────────────────────────

func TestStatus_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK in output: %q", out)
	}
}

func TestStatus_Degraded(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("DEGRADED\nprimary: timeout\n"))
	})
	addr := newTestServer(t, mux)

	_, err := runCmd(t, addr, "status")
	if err == nil {
		t.Fatal("expected non-nil error for degraded status")
	}
}

func TestStatus_JSONOutput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "status", "--json")
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	var s statusResponse
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if s.Status != "OK" {
		t.Errorf("status JSON: got %q, want OK", s.Status)
	}
}

// ─── completion ───────────────────────────────────────────────────────────────

func TestCompletion_Bash(t *testing.T) {
	// completion writes directly to os.Stdout, not the cobra output buffer,
	// so we can only verify it doesn't error.
	root := buildRoot()
	root.SetArgs([]string{"completion", "bash"})
	if err := root.Execute(); err != nil {
		t.Errorf("completion bash: %v", err)
	}
}

func TestCompletion_InvalidShell(t *testing.T) {
	root := buildRoot()
	root.SetArgs([]string{"completion", "notathing"})
	if err := root.Execute(); err == nil {
		t.Error("expected error for unsupported shell")
	}
}

// ─── object get ───────────────────────────────────────────────────────────────

func TestObjectGet_OK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ACGTACGT"))
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "get", "data/genome.bam")
	if err != nil {
		t.Fatalf("object get: %v", err)
	}
	if out != "ACGTACGT" {
		t.Errorf("expected ACGTACGT in output, got %q", out)
	}
}

func TestObjectGet_ToFile(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("file contents"))
	})
	addr := newTestServer(t, mux)

	tmp, err := os.CreateTemp(t.TempDir(), "objectget-*.dat")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tmp.Close()

	out, err := runCmd(t, addr, "object", "get", "some/key", "--output", tmp.Name())
	if err != nil {
		t.Fatalf("object get --output: %v", err)
	}
	// Success message should reference the file name and byte count.
	if !strings.Contains(out, "13 bytes") {
		t.Errorf("expected byte count in output, got %q", out)
	}

	got, readErr := os.ReadFile(tmp.Name())
	if readErr != nil {
		t.Fatalf("read temp file: %v", readErr)
	}
	if string(got) != "file contents" {
		t.Errorf("file content: got %q, want %q", got, "file contents")
	}
}

func TestObjectGet_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": "site unreachable"})
	})
	addr := newTestServer(t, mux)

	_, err := runCmd(t, addr, "object", "get", "missing/key")
	if err == nil {
		t.Fatal("expected error for 502 response")
	}
}

// ─── object put ───────────────────────────────────────────────────────────────

func TestObjectPut_OK(t *testing.T) {
	var gotKey string
	var gotBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.PathValue("key")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	})
	addr := newTestServer(t, mux)

	// Write payload to a temp file and use --input.
	tmp, _ := os.CreateTemp(t.TempDir(), "put-*.dat")
	tmp.WriteString("hello world")
	tmp.Close()

	out, err := runCmd(t, addr, "object", "put", "uploads/hello.txt", "--input", tmp.Name())
	if err != nil {
		t.Fatalf("object put: %v", err)
	}
	if !strings.Contains(out, "stored") {
		t.Errorf("expected 'stored' in output, got %q", out)
	}
	if gotKey != "uploads/hello.txt" {
		t.Errorf("server got key %q, want uploads/hello.txt", gotKey)
	}
	if string(gotBody) != "hello world" {
		t.Errorf("server got body %q, want hello world", gotBody)
	}
}

func TestObjectPut_FromStdin(t *testing.T) {
	var gotBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	})
	addr := newTestServer(t, mux)

	stdin := strings.NewReader("stdin data")
	out, err := runCmdWithStdin(t, stdin, addr, "object", "put", "stdin/key")
	if err != nil {
		t.Fatalf("object put (stdin): %v", err)
	}
	if !strings.Contains(out, "stored") {
		t.Errorf("expected 'stored' in output, got %q", out)
	}
	if string(gotBody) != "stdin data" {
		t.Errorf("server got body %q, want stdin data", gotBody)
	}
}

func TestObjectPut_JSONOutput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	addr := newTestServer(t, mux)

	tmp, _ := os.CreateTemp(t.TempDir(), "put-*.dat")
	tmp.WriteString("data")
	tmp.Close()

	out, err := runCmd(t, addr, "object", "put", "k", "--input", tmp.Name(), "--json")
	if err != nil {
		t.Fatalf("object put --json: %v", err)
	}
	var result objectPutResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if result.Key != "k" || result.Bytes != 4 {
		t.Errorf("unexpected result: %+v", result)
	}
}

// ─── object delete ────────────────────────────────────────────────────────────

func TestObjectDelete_OK(t *testing.T) {
	var gotKey string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.PathValue("key")
		w.WriteHeader(http.StatusNoContent)
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "delete", "reports/q1.csv")
	if err != nil {
		t.Fatalf("object delete: %v", err)
	}
	if !strings.Contains(out, "deleted") {
		t.Errorf("expected 'deleted' in output, got %q", out)
	}
	if gotKey != "reports/q1.csv" {
		t.Errorf("server got key %q, want reports/q1.csv", gotKey)
	}
}

func TestObjectDelete_JSONOutput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "delete", "mykey", "--json")
	if err != nil {
		t.Fatalf("object delete --json: %v", err)
	}
	var result objectDeleteResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if result.Key != "mykey" || !result.Deleted {
		t.Errorf("unexpected result: %+v", result)
	}
}

// ─── object head ──────────────────────────────────────────────────────────────

func TestObjectHead_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", "abc123")
		w.Header().Set("Last-Modified", "Wed, 15 Jan 2026 12:00:00 GMT")
		w.Header().Set("X-GlobalFS-Checksum", "sha256-beef")
		w.WriteHeader(http.StatusOK)
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "head", "archive/data.tar.zst")
	if err != nil {
		t.Fatalf("object head: %v", err)
	}
	if !strings.Contains(out, "archive/data.tar.zst") {
		t.Errorf("expected key in output, got %q", out)
	}
	if !strings.Contains(out, "1024") {
		t.Errorf("expected size in output, got %q", out)
	}
	if !strings.Contains(out, "abc123") {
		t.Errorf("expected etag in output, got %q", out)
	}
}

func TestObjectHead_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "42")
		w.Header().Set("ETag", "etag-xyz")
		w.WriteHeader(http.StatusOK)
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "head", "myfile.txt", "--json")
	if err != nil {
		t.Fatalf("object head --json: %v", err)
	}
	var info client.ObjectInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if info.Key != "myfile.txt" {
		t.Errorf("key: got %q, want myfile.txt", info.Key)
	}
	if info.Size != 42 {
		t.Errorf("size: got %d, want 42", info.Size)
	}
}

// ─── object list ──────────────────────────────────────────────────────────────

func TestObjectList_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prefix":  "",
			"count":   2,
			"objects": []client.ObjectInfo{
				{Key: "data/a.bam", Size: 100},
				{Key: "data/b.bam", Size: 200},
			},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "list")
	if err != nil {
		t.Fatalf("object list: %v", err)
	}
	if !strings.Contains(out, "data/a.bam") {
		t.Errorf("expected key in output, got %q", out)
	}
	if !strings.Contains(out, "data/b.bam") {
		t.Errorf("expected key in output, got %q", out)
	}
}

func TestObjectList_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prefix":  "data/",
			"count":   1,
			"objects": []client.ObjectInfo{{Key: "data/genome.bam", Size: 512}},
		})
	})
	addr := newTestServer(t, mux)

	out, err := runCmd(t, addr, "object", "list", "--json")
	if err != nil {
		t.Fatalf("object list --json: %v", err)
	}
	var objects []client.ObjectInfo
	if err := json.Unmarshal([]byte(out), &objects); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if len(objects) != 1 || objects[0].Key != "data/genome.bam" {
		t.Errorf("unexpected objects: %v", objects)
	}
}

func TestObjectList_WithFlags(t *testing.T) {
	var gotPrefix, gotLimit string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		gotPrefix = r.URL.Query().Get("prefix")
		gotLimit = r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prefix":  gotPrefix,
			"count":   0,
			"objects": []client.ObjectInfo{},
		})
	})
	addr := newTestServer(t, mux)

	_, err := runCmd(t, addr, "object", "list", "--prefix", "genomes/", "--limit", "25")
	if err != nil {
		t.Fatalf("object list --prefix --limit: %v", err)
	}
	if gotPrefix != "genomes/" {
		t.Errorf("prefix: got %q, want genomes/", gotPrefix)
	}
	if gotLimit != "25" {
		t.Errorf("limit: got %q, want 25", gotLimit)
	}
}

// ── API key auth ──────────────────────────────────────────────────────────────

// newAuthTestServer creates a test server that requires apiKey on every request
// except /healthz and /readyz.
func newAuthTestServer(t *testing.T, apiKey string, mux *http.ServeMux) string {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" {
			if r.Header.Get("X-GlobalFS-API-Key") != apiKey {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestAuth_CorrectKey_SiteList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	})
	addr := newAuthTestServer(t, "my-secret", mux)

	out, err := runCmd(t, addr, "--api-key", "my-secret", "site", "list")
	if err != nil {
		t.Fatalf("expected success with correct key, got: %v (output: %s)", err, out)
	}
}

func TestAuth_MissingKey_SiteList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	})
	addr := newAuthTestServer(t, "my-secret", mux)

	_, err := runCmd(t, addr, "site", "list") // no --api-key
	if err == nil {
		t.Fatal("expected error when API key is missing")
	}
}

func TestAuth_WrongKey_SiteList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	})
	addr := newAuthTestServer(t, "my-secret", mux)

	_, err := runCmd(t, addr, "--api-key", "wrong-key", "site", "list")
	if err == nil {
		t.Fatal("expected error with wrong API key")
	}
}

func TestAuth_EnvVar_SiteList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sites", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	})
	addr := newAuthTestServer(t, "env-secret", mux)

	t.Setenv("GLOBALFS_API_KEY", "env-secret")
	out, err := runCmd(t, addr, "site", "list") // no --api-key flag; env var should supply it
	if err != nil {
		t.Fatalf("expected success via env var, got: %v (output: %s)", err, out)
	}
}

func TestAuth_CorrectKey_ObjectGet(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data"))
	})
	addr := newAuthTestServer(t, "obj-key", mux)

	out, err := runCmd(t, addr, "--api-key", "obj-key", "object", "get", "mykey", "--output", "-")
	if err != nil {
		t.Fatalf("object get with key: %v (output: %s)", err, out)
	}
}

// ── config validate ───────────────────────────────────────────────────────────

// validConfigYAML is a minimal config that passes Validate().
const validConfigYAML = `
global:
  cluster_name: test-cluster
coordinator:
  listen_addr: ":8090"
  etcd_endpoints:
    - localhost:2379
sites:
  - name: primary
    role: primary
    objectfs:
      mount_point: /mnt/primary
      s3_bucket: my-bucket
      s3_region: us-west-2
`

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp config: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestConfigValidate_Valid(t *testing.T) {
	path := writeConfigFile(t, validConfigYAML)
	out, err := runCmd(t, "http://localhost:0", "config", "validate", path)
	if err != nil {
		t.Fatalf("config validate (valid): %v (output: %s)", err, out)
	}
	if !strings.Contains(out, "valid") {
		t.Errorf("expected 'valid' in output, got %q", out)
	}
}

func TestConfigValidate_MissingFile(t *testing.T) {
	_, err := runCmd(t, "http://localhost:0", "config", "validate", "/does/not/exist.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestConfigValidate_InvalidYAML(t *testing.T) {
	path := writeConfigFile(t, ":\tinvalid: yaml: [")
	_, err := runCmd(t, "http://localhost:0", "config", "validate", path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestConfigValidate_ValidationError(t *testing.T) {
	// Valid YAML but no sites → Validate() returns error.
	noSites := `
global:
  cluster_name: test-cluster
coordinator:
  listen_addr: ":8090"
  etcd_endpoints:
    - localhost:2379
sites: []
`
	path := writeConfigFile(t, noSites)
	_, err := runCmd(t, "http://localhost:0", "config", "validate", path)
	if err == nil {
		t.Fatal("expected validation error for config with no sites")
	}
	if !strings.Contains(err.Error(), "validation failed") {
		t.Errorf("error should mention 'validation failed': %v", err)
	}
}

// ── config show ───────────────────────────────────────────────────────────────

func TestConfigShow_YAML(t *testing.T) {
	path := writeConfigFile(t, validConfigYAML)
	out, err := runCmd(t, "http://localhost:0", "config", "show", path)
	if err != nil {
		t.Fatalf("config show: %v (output: %s)", err, out)
	}
	// The output should be valid YAML that contains the cluster name.
	var m map[string]any
	if err := yaml.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("config show output is not valid YAML: %v\n%s", err, out)
	}
	global, ok := m["global"].(map[string]any)
	if !ok {
		t.Fatalf("expected 'global' key in output, got: %v", m)
	}
	if global["cluster_name"] != "test-cluster" {
		t.Errorf("cluster_name: got %v, want test-cluster", global["cluster_name"])
	}
}

func TestConfigShow_JSON(t *testing.T) {
	path := writeConfigFile(t, validConfigYAML)
	out, err := runCmd(t, "http://localhost:0", "config", "show", path, "--json")
	if err != nil {
		t.Fatalf("config show --json: %v (output: %s)", err, out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("config show --json output is not valid JSON: %v\n%s", err, out)
	}
	// Configuration struct has no JSON tags; Go uses exported field names (e.g. "Global").
	global, ok := m["Global"].(map[string]any)
	if !ok {
		t.Fatalf("expected 'Global' key in JSON output, got: %v", m)
	}
	if global["ClusterName"] != "test-cluster" {
		t.Errorf("ClusterName: got %v, want test-cluster", global["ClusterName"])
	}
}

func TestConfigShow_MissingFile(t *testing.T) {
	_, err := runCmd(t, "http://localhost:0", "config", "show", "/no/such/file.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// ── config init ───────────────────────────────────────────────────────────────

func TestConfigInit_Stdout(t *testing.T) {
	out, err := runCmd(t, "http://localhost:0", "config", "init")
	if err != nil {
		t.Fatalf("config init: %v (output: %s)", err, out)
	}
	if !strings.Contains(out, "GlobalFS coordinator configuration") {
		t.Errorf("expected header comment in output, got %q", out)
	}
	if !strings.Contains(out, "cluster_name") {
		t.Errorf("expected cluster_name in template, got %q", out)
	}
	// Template should be parseable YAML.
	var m map[string]any
	if err := yaml.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("config init output is not valid YAML: %v\n%s", err, out)
	}
}

func TestConfigInit_ToFile(t *testing.T) {
	outFile := t.TempDir() + "/new-config.yaml"
	out, err := runCmd(t, "http://localhost:0", "config", "init", "--output", outFile)
	if err != nil {
		t.Fatalf("config init --output: %v (output: %s)", err, out)
	}
	if !strings.Contains(out, outFile) {
		t.Errorf("expected output path in message, got %q", out)
	}

	contents, readErr := os.ReadFile(outFile)
	if readErr != nil {
		t.Fatalf("read output file: %v", readErr)
	}
	if !strings.Contains(string(contents), "cluster_name") {
		t.Errorf("written file missing cluster_name, got %q", string(contents))
	}
}

// ── info ──────────────────────────────────────────────────────────────────────

func newInfoTestServer(t *testing.T, info coordinatorInfo) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(info)
	})
	return newTestServer(t, mux)
}

func TestInfo_TableOutput(t *testing.T) {
	info := coordinatorInfo{
		Version:               "0.1.0-alpha",
		UptimeSeconds:         3723,
		Sites:                 2,
		SitesByRole:           map[string]int{"primary": 1, "backup": 1},
		ReplicationQueueDepth: 3,
		IsLeader:              true,
	}
	addr := newInfoTestServer(t, info)

	out, err := runCmd(t, addr, "info")
	if err != nil {
		t.Fatalf("info: %v (output: %s)", err, out)
	}
	if !strings.Contains(out, "0.1.0-alpha") {
		t.Errorf("expected version in output, got %q", out)
	}
	if !strings.Contains(out, "1h2m3s") {
		t.Errorf("expected formatted uptime in output, got %q", out)
	}
	if !strings.Contains(out, "primary") {
		t.Errorf("expected 'primary' in output, got %q", out)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("expected 'true' for is_leader in output, got %q", out)
	}
}

func TestInfo_JSONOutput(t *testing.T) {
	info := coordinatorInfo{
		Version:       "0.2.0",
		UptimeSeconds: 60,
		Sites:         1,
		SitesByRole:   map[string]int{"primary": 1},
		IsLeader:      true,
	}
	addr := newInfoTestServer(t, info)

	out, err := runCmd(t, addr, "info", "--json")
	if err != nil {
		t.Fatalf("info --json: %v (output: %s)", err, out)
	}
	var got coordinatorInfo
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v (output: %q)", err, out)
	}
	if got.Version != "0.2.0" {
		t.Errorf("version: got %q, want 0.2.0", got.Version)
	}
	if got.Sites != 1 {
		t.Errorf("sites: got %d, want 1", got.Sites)
	}
}

func TestInfo_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
	})
	addr := newTestServer(t, mux)

	_, err := runCmd(t, addr, "info")
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

// ── formatUptime ──────────────────────────────────────────────────────────────

func TestFormatUptime(t *testing.T) {
	cases := []struct {
		secs float64
		want string
	}{
		{0, "0s"},
		{45, "45s"},
		{60, "1m0s"},
		{90, "1m30s"},
		{3600, "1h0m0s"},
		{3723, "1h2m3s"},
		{86400, "24h0m0s"},
	}
	for _, tc := range cases {
		got := formatUptime(tc.secs)
		if got != tc.want {
			t.Errorf("formatUptime(%v): got %q, want %q", tc.secs, got, tc.want)
		}
	}
}
