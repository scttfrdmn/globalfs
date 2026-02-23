package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/config"
)

// TestNewDefault_ResilienceDefaults verifies that NewDefault populates all
// resilience fields with the correct zero values and defaults.
func TestNewDefault_ResilienceDefaults(t *testing.T) {
	t.Parallel()

	cfg := config.NewDefault()
	r := cfg.Resilience

	if r.HealthPollInterval != 30*time.Second {
		t.Errorf("HealthPollInterval: got %v, want 30s", r.HealthPollInterval)
	}

	cb := r.CircuitBreaker
	if cb.Enabled {
		t.Error("CircuitBreaker.Enabled should default to false")
	}
	if cb.Threshold != 5 {
		t.Errorf("CircuitBreaker.Threshold: got %d, want 5", cb.Threshold)
	}
	if cb.Cooldown != 30*time.Second {
		t.Errorf("CircuitBreaker.Cooldown: got %v, want 30s", cb.Cooldown)
	}

	ret := r.Retry
	if ret.Enabled {
		t.Error("Retry.Enabled should default to false")
	}
	if ret.MaxAttempts != 3 {
		t.Errorf("Retry.MaxAttempts: got %d, want 3", ret.MaxAttempts)
	}
	if ret.InitialDelay != 100*time.Millisecond {
		t.Errorf("Retry.InitialDelay: got %v, want 100ms", ret.InitialDelay)
	}
	if ret.MaxDelay != 2*time.Second {
		t.Errorf("Retry.MaxDelay: got %v, want 2s", ret.MaxDelay)
	}
	if ret.Multiplier != 2.0 {
		t.Errorf("Retry.Multiplier: got %f, want 2.0", ret.Multiplier)
	}
}

// TestLoadFromFile_ResilienceFields verifies that resilience settings are
// correctly parsed from a YAML file.
func TestLoadFromFile_ResilienceFields(t *testing.T) {
	t.Parallel()

	yaml := `
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
      mount_point: /tmp/mnt
      s3_bucket: test-bucket
      s3_region: us-west-2
resilience:
  health_poll_interval: 15s
  circuit_breaker:
    enabled: true
    threshold: 3
    cooldown: 1m
  retry:
    enabled: true
    max_attempts: 5
    initial_delay: 200ms
    max_delay: 10s
    multiplier: 1.5
`
	f := writeTempFile(t, yaml)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(f); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	r := cfg.Resilience

	if r.HealthPollInterval != 15*time.Second {
		t.Errorf("HealthPollInterval: got %v, want 15s", r.HealthPollInterval)
	}

	cb := r.CircuitBreaker
	if !cb.Enabled {
		t.Error("CircuitBreaker.Enabled should be true")
	}
	if cb.Threshold != 3 {
		t.Errorf("CircuitBreaker.Threshold: got %d, want 3", cb.Threshold)
	}
	if cb.Cooldown != time.Minute {
		t.Errorf("CircuitBreaker.Cooldown: got %v, want 1m", cb.Cooldown)
	}

	ret := r.Retry
	if !ret.Enabled {
		t.Error("Retry.Enabled should be true")
	}
	if ret.MaxAttempts != 5 {
		t.Errorf("Retry.MaxAttempts: got %d, want 5", ret.MaxAttempts)
	}
	if ret.InitialDelay != 200*time.Millisecond {
		t.Errorf("Retry.InitialDelay: got %v, want 200ms", ret.InitialDelay)
	}
	if ret.MaxDelay != 10*time.Second {
		t.Errorf("Retry.MaxDelay: got %v, want 10s", ret.MaxDelay)
	}
	if ret.Multiplier != 1.5 {
		t.Errorf("Retry.Multiplier: got %f, want 1.5", ret.Multiplier)
	}
}

// TestLoadFromFile_ResilienceOmitted verifies that omitting the resilience
// section leaves the default values intact.
func TestLoadFromFile_ResilienceOmitted(t *testing.T) {
	t.Parallel()

	yaml := `
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
      mount_point: /tmp/mnt
      s3_bucket: test-bucket
      s3_region: us-west-2
`
	f := writeTempFile(t, yaml)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(f); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	// Defaults should be unchanged when the section is absent.
	if cfg.Resilience.HealthPollInterval != 30*time.Second {
		t.Errorf("HealthPollInterval: got %v, want 30s (default)", cfg.Resilience.HealthPollInterval)
	}
	if cfg.Resilience.CircuitBreaker.Enabled {
		t.Error("CircuitBreaker.Enabled should remain false when omitted")
	}
	if cfg.Resilience.Retry.Enabled {
		t.Error("Retry.Enabled should remain false when omitted")
	}
}

// TestLoadFromFile_CircuitBreakerOnly verifies partial resilience config.
func TestLoadFromFile_CircuitBreakerOnly(t *testing.T) {
	t.Parallel()

	yaml := `
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
      mount_point: /tmp/mnt
      s3_bucket: test-bucket
      s3_region: us-west-2
resilience:
  circuit_breaker:
    enabled: true
    threshold: 10
`
	f := writeTempFile(t, yaml)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(f); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	if !cfg.Resilience.CircuitBreaker.Enabled {
		t.Error("CircuitBreaker.Enabled should be true")
	}
	if cfg.Resilience.CircuitBreaker.Threshold != 10 {
		t.Errorf("Threshold: got %d, want 10", cfg.Resilience.CircuitBreaker.Threshold)
	}
	// Retry should remain at its default (disabled).
	if cfg.Resilience.Retry.Enabled {
		t.Error("Retry.Enabled should remain false when omitted")
	}
}

// TestNewDefault_CacheDefaults verifies that NewDefault populates all cache
// fields with the correct zero values and defaults.
func TestNewDefault_CacheDefaults(t *testing.T) {
	t.Parallel()

	cfg := config.NewDefault()
	cc := cfg.Cache

	if cc.Enabled {
		t.Error("Cache.Enabled should default to false")
	}
	if cc.MaxBytes != 64*1024*1024 {
		t.Errorf("Cache.MaxBytes: got %d, want 67108864 (64 MiB)", cc.MaxBytes)
	}
	if cc.TTL != 0 {
		t.Errorf("Cache.TTL: got %v, want 0", cc.TTL)
	}
}

// TestLoadFromFile_CacheFields verifies that cache settings are correctly
// parsed from a YAML file.
func TestLoadFromFile_CacheFields(t *testing.T) {
	t.Parallel()

	yaml := `
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
      mount_point: /tmp/mnt
      s3_bucket: test-bucket
      s3_region: us-west-2
cache:
  enabled: true
  max_bytes: 134217728
  ttl: 5m
`
	f := writeTempFile(t, yaml)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(f); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	cc := cfg.Cache
	if !cc.Enabled {
		t.Error("Cache.Enabled should be true")
	}
	if cc.MaxBytes != 134217728 {
		t.Errorf("Cache.MaxBytes: got %d, want 134217728", cc.MaxBytes)
	}
	if cc.TTL != 5*time.Minute {
		t.Errorf("Cache.TTL: got %v, want 5m", cc.TTL)
	}
}

// TestLoadFromFile_CacheOmitted verifies that omitting the cache section
// leaves the default values intact.
func TestLoadFromFile_CacheOmitted(t *testing.T) {
	t.Parallel()

	yaml := `
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
      mount_point: /tmp/mnt
      s3_bucket: test-bucket
      s3_region: us-west-2
`
	f := writeTempFile(t, yaml)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(f); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	if cfg.Cache.Enabled {
		t.Error("Cache.Enabled should remain false when omitted")
	}
	if cfg.Cache.MaxBytes != 64*1024*1024 {
		t.Errorf("Cache.MaxBytes: got %d, want 67108864 (default)", cfg.Cache.MaxBytes)
	}
}

// writeTempFile writes content to a temp file and returns its path.
// The file is removed when the test completes.
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	f.Close()
	return filepath.Clean(f.Name())
}
