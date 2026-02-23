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
