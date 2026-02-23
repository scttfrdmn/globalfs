package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
