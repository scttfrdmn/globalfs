package metadata

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestEtcdStore runs the full Store contract against a live etcd cluster.
//
// The test is skipped unless the ETCD_ENDPOINTS environment variable is set.
// Set it to a comma-separated list of etcd endpoints, e.g.:
//
//	ETCD_ENDPOINTS=localhost:2379 go test -race -v ./internal/metadata/...
//
// A quick way to start etcd for testing:
//
//	docker run --rm -p 2379:2379 \
//	  -e ALLOW_NONE_AUTHENTICATION=yes \
//	  bitnami/etcd:latest
func TestEtcdStore(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("ETCD_ENDPOINTS not set; skipping etcd integration tests")
	}

	eps := strings.Split(endpoints, ",")
	cfg := EtcdConfig{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
		// Use a test-specific prefix to avoid collisions with other data.
		Prefix: "/globalfs-test/",
	}

	ctx := context.Background()
	store, err := NewEtcdStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	defer store.Close()

	// Clean up any leftover keys from a previous test run.
	t.Cleanup(func() { cleanEtcdPrefix(t, store, cfg.Prefix) })
	cleanEtcdPrefix(t, store, cfg.Prefix)

	storeTests(t, store)
}

// cleanEtcdPrefix deletes all keys under the store prefix using a range delete.
func cleanEtcdPrefix(t *testing.T, store *EtcdStore, _ string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := store.client.Delete(ctx, store.prefix,
		clientv3.WithPrefix())
	if err != nil {
		t.Logf("cleanup: %v (non-fatal)", err)
	}
}
