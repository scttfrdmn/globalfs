package config_test

import (
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/types"
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

// ── Validate resilience/cache fields (#30) ───────────────────────────────────

// baseValidConfig returns a minimal valid configuration for Validate() tests.
func baseValidConfig() *config.Configuration {
	cfg := config.NewDefault()
	cfg.Sites = []config.SiteConfig{
		{
			Name: "primary",
			Role: "primary",
			ObjectFS: config.ObjectFSConfig{
				MountPoint: "/mnt",
				S3Bucket:   "test-bucket",
				S3Region:   "us-west-2",
			},
		},
	}
	return cfg
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Global.LogLevel = "VERBOSE"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid log_level, got nil")
	}
}

func TestValidate_CircuitBreaker_ThresholdZero(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.CircuitBreaker.Enabled = true
	cfg.Resilience.CircuitBreaker.Threshold = 0
	cfg.Resilience.CircuitBreaker.Cooldown = 30 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for threshold=0 with CB enabled, got nil")
	}
}

func TestValidate_CircuitBreaker_CooldownZero(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.CircuitBreaker.Enabled = true
	cfg.Resilience.CircuitBreaker.Threshold = 5
	cfg.Resilience.CircuitBreaker.Cooldown = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for cooldown=0 with CB enabled, got nil")
	}
}

func TestValidate_CircuitBreaker_Disabled_IgnoresBadValues(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.CircuitBreaker.Enabled = false
	cfg.Resilience.CircuitBreaker.Threshold = 0 // would be invalid if enabled
	if err := cfg.Validate(); err != nil {
		t.Errorf("disabled circuit breaker should not validate threshold: %v", err)
	}
}

func TestValidate_Retry_MaxAttemptsZero(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.Retry.Enabled = true
	cfg.Resilience.Retry.MaxAttempts = 0
	cfg.Resilience.Retry.Multiplier = 2.0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_attempts=0 with retry enabled, got nil")
	}
}

func TestValidate_Retry_MultiplierBelowOne(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.Retry.Enabled = true
	cfg.Resilience.Retry.MaxAttempts = 3
	cfg.Resilience.Retry.Multiplier = 0.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for multiplier < 1.0 with retry enabled, got nil")
	}
}

func TestValidate_Retry_InitialDelayExceedsMaxDelay(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.Retry.Enabled = true
	cfg.Resilience.Retry.MaxAttempts = 3
	cfg.Resilience.Retry.Multiplier = 2.0
	cfg.Resilience.Retry.InitialDelay = 5 * time.Second
	cfg.Resilience.Retry.MaxDelay = 1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for initial_delay > max_delay, got nil")
	}
}

func TestValidate_Cache_MaxBytesZero(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Cache.Enabled = true
	cfg.Cache.MaxBytes = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_bytes=0 with cache enabled, got nil")
	}
}

func TestValidate_Cache_Disabled_IgnoresBadValues(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Cache.Enabled = false
	cfg.Cache.MaxBytes = 0 // would be invalid if enabled
	if err := cfg.Validate(); err != nil {
		t.Errorf("disabled cache should not validate max_bytes: %v", err)
	}
}

func TestValidate_Valid_ResilienceAndCache(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Resilience.CircuitBreaker.Enabled = true
	cfg.Resilience.CircuitBreaker.Threshold = 5
	cfg.Resilience.CircuitBreaker.Cooldown = 30 * time.Second
	cfg.Resilience.Retry.Enabled = true
	cfg.Resilience.Retry.MaxAttempts = 3
	cfg.Resilience.Retry.Multiplier = 2.0
	cfg.Resilience.Retry.InitialDelay = 100 * time.Millisecond
	cfg.Resilience.Retry.MaxDelay = 2 * time.Second
	cfg.Cache.Enabled = true
	cfg.Cache.MaxBytes = 64 * 1024 * 1024
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should pass validation: %v", err)
	}
}

// The three TestValidate_CargoShip_* tests that stood here were removed with the
// CargoShip config block itself (#108).  They asserted that
// `cargoship.enabled: true` without an endpoint failed validation — real
// behaviour, for a feature this repository never implemented and that objectfs
// deleted upstream.  A `cargoship:` key is now rejected at decode time, which
// TestLoad_RejectsRemovedCargoShipBlock below covers.

// TestValidate_EtcdEndpointsNotRequired pins the #106 decision: the field is
// accepted, unused, and optional.
//
// It was required, so `etcd_endpoints: []` — which the shipped example told
// operators to use for a single-node deployment — produced a config that would
// not load.  Nothing reads the value: SetStore and SetLeaseManager have no
// non-test callers, so no connection to these endpoints is ever attempted.
// Requiring it bought a startup failure and nothing else.
func TestValidate_EtcdEndpointsNotRequired(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Coordinator.EtcdEndpoints = nil
	if err := cfg.Validate(); err != nil {
		t.Errorf("etcd_endpoints is unused and must not be required: %v", err)
	}

	cfg.Coordinator.EtcdEndpoints = []string{}
	if err := cfg.Validate(); err != nil {
		t.Errorf("empty etcd_endpoints must validate: %v", err)
	}
}

// ── Key pattern validation (#100) ────────────────────────────────────────────

func TestValidateKeyPattern(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pattern string
		wantErr bool
	}{
		{"", false},                  // matches every key
		{"genomes/", false},          // literal prefix, not a glob
		{"data/**", false},           // recursive
		{"**/*.bam", false},          // recursive from the root
		{"data/**/raw/*", false},     // ** in the middle
		{"[a-z]*.bam", false},        // a balanced character class is fine
		{`data/[a-`, true},           // unbalanced [
		{`data/x\`, true},            // trailing backslash
		{`**[a`, true},               // unbalanced [ following a **
		{`ok/[abc]/still/ok`, false}, // balanced mid-pattern
		{`a]b`, false},               // a lone ] is a literal, not a syntax error
	}
	for _, tc := range cases {
		err := config.ValidateKeyPattern(tc.pattern)
		if (err != nil) != tc.wantErr {
			t.Errorf("ValidateKeyPattern(%q) = %v, wantErr %v", tc.pattern, err, tc.wantErr)
		}
	}
}

// TestValidate_RejectsBadKeyPattern covers both pattern-carrying surfaces.
//
// policy.rules is the one that reaches the routing engine; policies is the
// legacy placement block, which Validate has always checked and which nothing
// reads — but it is where the shipped example's patterns live, so a bad one
// there still misleads whoever copies it.
func TestValidate_RejectsBadKeyPattern(t *testing.T) {
	t.Parallel()

	cfg := baseValidConfig()
	cfg.Policy.Rules = []config.PolicyRuleConfig{
		{Name: "good", KeyPattern: "data/**"},
		{Name: "broken", KeyPattern: `data/[a-`},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted an unparseable policy.rules key_pattern")
	}
	if !strings.Contains(err.Error(), "broken") {
		t.Errorf("error should name the offending rule, got %v", err)
	}

	cfg = baseValidConfig()
	cfg.Policies = []types.ReplicationPolicy{
		{Name: "legacy-broken", PathPattern: `inputs/[a-`, Primary: "primary"},
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted an unparseable policies path_pattern")
	}
	if !strings.Contains(err.Error(), "legacy-broken") {
		t.Errorf("error should name the offending policy, got %v", err)
	}
}

// TestValidate_AcceptsRecursivePatterns guards against a validator that rejects
// the very syntax #100 made work.  Every pattern here appears in the shipped
// example configs or the README.
func TestValidate_AcceptsRecursivePatterns(t *testing.T) {
	t.Parallel()
	cfg := baseValidConfig()
	cfg.Policy.Rules = []config.PolicyRuleConfig{
		{Name: "inputs", KeyPattern: "inputs/**"},
		{Name: "bams", KeyPattern: "**/*.bam"},
		{Name: "hot", KeyPattern: "datasets/hot/*"},
		{Name: "scratch", KeyPattern: "scratch/"},
		{Name: "all", KeyPattern: ""},
	}
	cfg.Policies = []types.ReplicationPolicy{
		{Name: "results", PathPattern: "results/**", Primary: "primary"},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("recursive patterns must validate: %v", err)
	}
}

// TestLoad_TypesStructsBindFromYAML covers the three structs in pkg/types that
// pkg/config decodes YAML into. They carried only json tags, and yaml.v3 falls
// back to the lowercased field name when a tag is absent — so `listen_addr`
// bound to nothing, every field stayed at its zero value, and Load then
// overwrote them with defaults. An operator's port, etcd endpoints, lease TTL,
// and worker queue depth were all silently discarded while `config show`
// reported the file as loaded.
//
// The values below deliberately differ from every default in NewDefault, so a
// regression cannot pass by coincidence.
func TestLoad_TypesStructsBindFromYAML(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, `
global:
  cluster_name: bind-test
coordinator:
  listen_addr: ":19999"
  lease_timeout: 90s
  etcd_endpoints:
    - etcd-a:2379
    - etcd-b:2379
performance:
  replication_queue_depth: 32
policies:
  - name: hot
    path_pattern: "/datasets/hot/*"
    primary: onprem
    replicate_to: [cloud]
    priority: 10
sites:
  - name: onprem
    role: primary
    objectfs:
      mount_point: /mnt/onprem
      s3_bucket: onprem-bucket
      s3_region: us-west-2
  - name: cloud
    role: burst
    objectfs:
      mount_point: /mnt/cloud
      s3_bucket: cloud-bucket
      s3_region: us-east-1
`)

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	// types.CoordinatorConfig
	if got := cfg.Coordinator.ListenAddr; got != ":19999" {
		t.Errorf("Coordinator.ListenAddr = %q, want \":19999\"", got)
	}
	if got := cfg.Coordinator.LeaseTimeout; got != 90*time.Second {
		t.Errorf("Coordinator.LeaseTimeout = %v, want 90s", got)
	}
	if got := cfg.Coordinator.EtcdEndpoints; len(got) != 2 || got[0] != "etcd-a:2379" {
		t.Errorf("Coordinator.EtcdEndpoints = %v, want [etcd-a:2379 etcd-b:2379]", got)
	}

	// types.PerformanceConfig
	if got := cfg.Performance.ReplicationQueueDepth; got != 32 {
		t.Errorf("Performance.ReplicationQueueDepth = %d, want 32", got)
	}

	// types.ReplicationPolicy — the field whose absence made config.example.yaml
	// fail validation with "policies[0].path_pattern is required".
	if len(cfg.Policies) != 1 {
		t.Fatalf("len(Policies) = %d, want 1", len(cfg.Policies))
	}
	p := cfg.Policies[0]
	if p.Name != "hot" {
		t.Errorf("Policies[0].Name = %q, want \"hot\"", p.Name)
	}
	if p.PathPattern != "/datasets/hot/*" {
		t.Errorf("Policies[0].PathPattern = %q, want \"/datasets/hot/*\"", p.PathPattern)
	}
	if p.Primary != "onprem" {
		t.Errorf("Policies[0].Primary = %q, want \"onprem\"", p.Primary)
	}
	if len(p.ReplicateTo) != 1 || p.ReplicateTo[0] != "cloud" {
		t.Errorf("Policies[0].ReplicateTo = %v, want [cloud]", p.ReplicateTo)
	}
	if p.Priority != 10 {
		t.Errorf("Policies[0].Priority = %d, want 10", p.Priority)
	}

	// The policy block above only validates if PathPattern actually bound.
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// TestShippedConfigsAreValid loads every config file this repository presents as
// a starting point. config.example.yaml is the file README.md tells users to
// copy, and it did not validate: the yaml-tag defect above meant its `policies:`
// block decoded to five empty policies. CI runs `globalfs config validate` over
// these same files; this test is the half that runs on a laptop.
func TestShippedConfigsAreValid(t *testing.T) {
	t.Parallel()

	// Relative to pkg/config/, where this test runs.
	shipped := []string{
		"../../config.example.yaml",
		"../../examples/coordinator-config.yaml",
	}

	for _, path := range shipped {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			if _, err := os.Stat(path); err != nil {
				t.Fatalf("shipped config is missing: %v", err)
			}
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(path); err != nil {
				t.Fatalf("LoadFromFile: %v", err)
			}
			if err := cfg.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

// TestDefaultPorts_DaemonAndCLIAgree is the regression test for #81: the daemon
// default (NewDefault().Coordinator.ListenAddr) was ":8080" while the CLI default
// (cmd/globalfs, --coordinator-addr) was "http://localhost:8090", so a coordinator
// started with no config file listened on a port no CLI command ever dialled.
//
// Changing the ":8080" literal alone would have left the same failure mode one
// refactor away, so both defaults now derive from DefaultListenPort. This test
// asserts the derivation still holds: if someone reintroduces a bare literal in
// either place, the port comparison below fails.
func TestDefaultPorts_DaemonAndCLIAgree(t *testing.T) {
	t.Parallel()

	// The daemon default must come from the shared constant, not a literal.
	if got := config.NewDefault().Coordinator.ListenAddr; got != config.DefaultListenAddr {
		t.Errorf("NewDefault().Coordinator.ListenAddr = %q, want %q", got, config.DefaultListenAddr)
	}

	// The daemon listen address must be a host:port that net.Listen accepts, and
	// its port is the one a client has to dial.
	_, daemonPort, err := net.SplitHostPort(config.DefaultListenAddr)
	if err != nil {
		t.Fatalf("DefaultListenAddr %q is not a valid host:port: %v", config.DefaultListenAddr, err)
	}

	// The CLI default must be an absolute http URL pointing at that same port.
	u, err := url.Parse(config.DefaultCoordinatorURL)
	if err != nil {
		t.Fatalf("DefaultCoordinatorURL %q does not parse: %v", config.DefaultCoordinatorURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		t.Errorf("DefaultCoordinatorURL scheme = %q, want http or https", u.Scheme)
	}
	if u.Port() != daemonPort {
		t.Errorf("CLI default dials port %q but the daemon listens on %q (%q vs %q) — "+
			"the out-of-the-box experience is broken (#81)",
			u.Port(), daemonPort, config.DefaultCoordinatorURL, config.DefaultListenAddr)
	}
}

// TestShippedConfigs_ListenAddrMatchesDefault keeps the YAML files users copy in
// agreement with the compiled-in default. config.example.yaml said ":8080" while
// examples/coordinator-config.yaml and the `config init` template said ":8090";
// copying the former produced the #81 failure even with an explicit config file.
func TestShippedConfigs_ListenAddrMatchesDefault(t *testing.T) {
	t.Parallel()

	// Relative to pkg/config/, where this test runs.
	shipped := []string{
		"../../config.example.yaml",
		"../../examples/coordinator-config.yaml",
	}

	for _, path := range shipped {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(path); err != nil {
				t.Fatalf("LoadFromFile: %v", err)
			}
			if got := cfg.Coordinator.ListenAddr; got != config.DefaultListenAddr {
				t.Errorf("coordinator.listen_addr = %q, want %q — a user who copies this "+
					"file gets a coordinator the CLI default cannot reach (#81)",
					got, config.DefaultListenAddr)
			}
		})
	}
}

// ─── Strict decoding (#97) ────────────────────────────────────────────────────
//
// LoadFromFile rejects unknown keys.  These tests are the reason the change is
// worth its breakage: before it, every case below loaded successfully and the
// setting was discarded in silence, while `config validate` passed and the daemon
// started.

// TestLoad_RejectsUnknownField covers the plain typo, which is the case that
// motivated #97: `listen_adrr: ":9000"` was accepted and the daemon bound the
// default port instead, with nothing logged.  The operator had a config file
// containing the port they wanted and a daemon on a different one.
func TestLoad_RejectsUnknownField(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, `
coordinator:
  listen_adrr: ":9000"
`)
	cfg := config.NewDefault()
	err := cfg.LoadFromFile(path)
	if err == nil {
		t.Fatal("expected an error for the misspelled key listen_adrr, got nil")
	}
	// The message must name the offending key, or an operator cannot act on it.
	if !strings.Contains(err.Error(), "listen_adrr") {
		t.Errorf("error does not name the unknown key: %v", err)
	}
}

// TestLoad_RejectsFieldsDeletedInV015 covers the three fields that `config init`
// itself emitted after v0.1.5 removed them from the struct.  The generated
// template validated clean while `config show` reported none of the three, so an
// operator could edit transfer_chunk_size, restart, see the config accepted, and
// have nothing change.
func TestLoad_RejectsFieldsDeletedInV015(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, yaml string }{
		{"health_check_interval", "coordinator:\n  health_check_interval: 30s\n"},
		{"transfer_chunk_size", "performance:\n  transfer_chunk_size: 16777216\n"},
		{"cache_size", "performance:\n  cache_size: 1073741824\n"},
		{"network.bandwidth", "network:\n  bandwidth: 10Gbps\n"},
		{"network.latency", "network:\n  latency: 20ms\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(writeTempFile(t, tc.yaml)); err == nil {
				t.Errorf("%s was removed in v0.1.5 and must be rejected, got nil", tc.name)
			}
		})
	}
}

// TestLoad_RejectsRemovedCargoShipBlock covers #108's removal.  A leftover
// cargoship: block is now a load failure rather than a validated no-op — which
// is the upgrade note: the block has to come out of existing configs.
func TestLoad_RejectsRemovedCargoShipBlock(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, `
sites:
  - name: onprem
    role: primary
    cargoship:
      enabled: true
      endpoint: http://cargoship:8081
`)
	cfg := config.NewDefault()
	err := cfg.LoadFromFile(path)
	if err == nil {
		t.Fatal("expected an error for the removed cargoship block, got nil")
	}
	if !strings.Contains(err.Error(), "cargoship") {
		t.Errorf("error does not name the removed key: %v", err)
	}
}

// TestLoad_RejectsRenamedPerformanceField covers #101's rename.  The old name is
// the one every existing config carries, so it must fail loudly rather than
// leave the queue at its default depth.
func TestLoad_RejectsRenamedPerformanceField(t *testing.T) {
	t.Parallel()

	cfg := config.NewDefault()
	err := cfg.LoadFromFile(writeTempFile(t, "performance:\n  max_concurrent_transfers: 32\n"))
	if err == nil {
		t.Fatal("expected an error for the renamed max_concurrent_transfers, got nil")
	}

	// And the new name must bind, or the rename traded one silent discard for
	// another.
	cfg = config.NewDefault()
	if err := cfg.LoadFromFile(writeTempFile(t, "performance:\n  replication_queue_depth: 32\n")); err != nil {
		t.Fatalf("replication_queue_depth must load: %v", err)
	}
	if got := cfg.Performance.ReplicationQueueDepth; got != 32 {
		t.Errorf("ReplicationQueueDepth = %d, want 32", got)
	}
}

// TestLoad_RejectsRemovedMetricsPort covers #98.  metrics_port implied a second
// listener that never existed — /metrics is a route on the main authenticated
// mux — so the field is gone rather than wired.
func TestLoad_RejectsRemovedMetricsPort(t *testing.T) {
	t.Parallel()

	cfg := config.NewDefault()
	if err := cfg.LoadFromFile(writeTempFile(t, "global:\n  metrics_port: 9090\n")); err == nil {
		t.Error("expected an error for the removed metrics_port, got nil")
	}
}

// TestLoad_EmptyFileIsNotAnError pins the one case where strict decoding must not
// fire.  An empty file yields io.EOF from the decoder with the target untouched;
// that is a config to be judged by Validate, whose message names the specific
// missing field, rather than a parse failure reported as a bare EOF.
func TestLoad_EmptyFileIsNotAnError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content string }{
		{"empty", ""},
		{"only-comments", "# nothing but a comment\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(writeTempFile(t, tc.content)); err != nil {
				t.Fatalf("LoadFromFile on an empty config: %v", err)
			}
			// Defaults must survive: the decode touched nothing.
			if got := cfg.Coordinator.ListenAddr; got != config.DefaultListenAddr {
				t.Errorf("ListenAddr = %q, want the default %q", got, config.DefaultListenAddr)
			}
		})
	}
}

// TestLoad_ShippedTemplateHasNoUnknownFields is the gate that would have caught
// the `config init` drift.  TestShippedConfigsAreValid covers the two YAML files
// on disk; the template is a Go string constant in cmd/globalfs, so it is
// verified there (TestConfigInit_TemplateLoadsStrictly).  This asserts the
// property that matters for both: every key a shipped config contains is a key
// the struct has.
func TestLoad_ShippedConfigsHaveNoUnknownFields(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob("../../examples/*.yaml")
	if err != nil {
		t.Fatalf("glob examples: %v", err)
	}
	// Globbed rather than listed, so a new example under examples/ is covered the
	// moment it is added.  config.example.yaml is named explicitly because it is
	// the only one outside that directory.
	paths = append(paths, "../../config.example.yaml")
	if len(paths) < 3 {
		t.Fatalf("found only %d shipped configs (%v); the glob is probably wrong, "+
			"and a test that silently checks nothing is worse than no test", len(paths), paths)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(path); err != nil {
				t.Fatalf("shipped config has a key the struct lacks: %v", err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("shipped config does not validate: %v", err)
			}
		})
	}
}

// ─── README field tables match the struct (#99) ───────────────────────────────

// TestREADME_DocumentsOnlyRealFields is the durable half of #99.
//
// Five fields deleted in v0.1.5 stayed in the README's reference tables for
// eleven releases, and the only thing that would ever have caught it is a human
// reading both.  Now that decoding is strict (#97), a documented-but-absent field
// is worse than a no-op: an operator who copies it gets a daemon that refuses to
// start, so this drift has been promoted from cosmetic to breaking and needs a
// gate.
//
// Matching is by dotted suffix rather than exact path, because the tables are
// organised by config section while some of them document a nested element — the
// `policy` table lists the fields of a *rule* (policy.rules[].name), not of
// PolicyConfig.  A suffix match still catches every real drift: a field that was
// deleted or renamed appears nowhere in the struct tree under any prefix.
func TestREADME_DocumentsOnlyRealFields(t *testing.T) {
	t.Parallel()

	real := yamlPaths(reflect.TypeOf(config.Configuration{}), "")
	if len(real) < 25 {
		t.Fatalf("walked only %d yaml paths out of Configuration; the reflection "+
			"is wrong and this test would pass on anything", len(real))
	}

	for _, field := range readmeFieldCells(t) {
		if !hasSuffixPath(real, field) {
			t.Errorf("README documents %q, which is not a field of config.Configuration "+
				"under any prefix.\nWith strict decoding an operator who copies it gets a "+
				"daemon that will not start. Remove it from the table, or name its "+
				"successor.", field)
		}
	}
}

// TestREADME_RemovedFieldsAreActuallyGone is the inverse assertion, and it is the
// one that keeps the "fields removed in …" tables honest: those tables tell an
// operator to delete a key, so a key that quietly came back would make the
// documentation wrong in the more dangerous direction.
func TestREADME_RemovedFieldsAreActuallyGone(t *testing.T) {
	t.Parallel()

	real := yamlPaths(reflect.TypeOf(config.Configuration{}), "")
	for _, field := range []string{
		// Removed in v0.3.0 (#98, #101, #108, and the sync_mode found while
		// cleaning config.example.yaml).
		"metrics_port",
		"cargoship",
		"max_concurrent_transfers",
		"sync_mode",
		// Removed in v0.1.5, documented until v0.3.0 (#99).
		"health_check_interval",
		"cache_size",
		"transfer_chunk_size",
		"network",
	} {
		if hasSuffixPath(real, field) {
			t.Errorf("README's removed-fields table says to delete %q, but it is a "+
				"field of config.Configuration again. Either the field came back and "+
				"the table is wrong, or the name was reused for something else — "+
				"which is worse, because an operator following the table deletes a "+
				"live setting.", field)
		}
	}
}

// yamlPaths walks a struct type and returns every dotted yaml path in it.
//
// Slices are traversed through their element type without an index, so
// `sites[].objectfs.s3_bucket` appears as "sites.objectfs.s3_bucket": the tables
// document field names, not positions.
func yamlPaths(t reflect.Type, prefix string) map[string]bool {
	out := make(map[string]bool)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return out
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if tag == "" || tag == "-" {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + "." + tag
		}
		out[path] = true

		ft := f.Type
		for ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array {
			ft = ft.Elem()
		}
		// time.Duration is an int64 and a struct-free leaf; anything else that is
		// a struct is a nested section worth descending into.
		if ft.Kind() == reflect.Struct {
			for k := range yamlPaths(ft, path) {
				out[k] = true
			}
		}
	}
	return out
}

// hasSuffixPath reports whether any known path equals field or ends with
// "." + field, so a segment boundary is required: "cache_size" must not match
// "cache.size" or "max_bytes_cache_size".
func hasSuffixPath(paths map[string]bool, field string) bool {
	for p := range paths {
		if p == field || strings.HasSuffix(p, "."+field) {
			return true
		}
	}
	return false
}

// readmeFieldCells extracts the first column of every table in the README's
// Configuration Reference section, which by convention is a backticked field
// name.
//
// It reads only between "## Configuration Reference" and the next "## " heading,
// and skips the two "fields removed in …" tables — those exist precisely to name
// keys the struct no longer has, and are asserted in the opposite direction by
// TestREADME_RemovedFieldsAreActuallyGone.
// firstCell returns the first cell of a markdown table row, trimmed.
func firstCell(line string) string {
	cells := strings.Split(strings.Trim(line, "|"), "|")
	if len(cells) == 0 {
		return ""
	}
	return strings.TrimSpace(cells[0])
}

func readmeFieldCells(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	var fields []string
	inSection, inFieldTable := false, false
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "## Configuration Reference"):
			inSection = true
			continue
		case inSection && strings.HasPrefix(line, "## "):
			inSection = false
		}
		if !inSection {
			continue
		}

		// Table selection is positive: a table is scanned only when its header row's
		// first cell is exactly "Field".  Every live reference table is shaped that
		// way, and the alternative — excluding the tables that are not field lists —
		// has already failed once: the removed-field tables were excluded by name
		// while the `key_pattern` syntax table (header "Pattern", first column full
		// of globs) was not, and its rows arrived here as field names (#100).
		if !strings.HasPrefix(line, "|") {
			inFieldTable = false
			continue
		}
		if first := firstCell(line); first == "Field" {
			inFieldTable = true
			continue
		}
		if !inFieldTable {
			continue
		}

		cell := firstCell(line)
		// Skip separator rows and any prose cell.
		if !strings.HasPrefix(cell, "`") || !strings.HasSuffix(cell, "`") {
			continue
		}
		name := strings.Trim(cell, "`")
		// Wildcards in the removed tables ("sites[].cargoship.*") never reach here,
		// but a `*` anywhere is not a field name.
		if name == "" || strings.Contains(name, "*") {
			continue
		}
		fields = append(fields, name)
	}

	if len(fields) < 20 {
		t.Fatalf("extracted only %d field names from the README's Configuration "+
			"Reference (%v); the parser is out of step with the document's shape, "+
			"and a test that reads nothing passes on anything", len(fields), fields)
	}
	return fields
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
