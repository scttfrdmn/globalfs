package config

import (
	"fmt"
	"os"
	"time"

	"github.com/scttfrdmn/globalfs/pkg/types"
	"gopkg.in/yaml.v3"
)

// PolicyRuleConfig defines a single routing rule in YAML configuration.
// Operations and TargetRoles are represented as string slices for
// human-friendly YAML; use policy.NewFromConfig to convert to typed Rules.
type PolicyRuleConfig struct {
	// Name is a human-readable identifier for the rule.
	Name string `yaml:"name"`

	// KeyPattern is matched against object keys (glob, prefix ending in "/",
	// or exact match).  An empty pattern matches all keys.
	KeyPattern string `yaml:"key_pattern"`

	// Operations lists which operation types this rule applies to.
	// Valid values: "read", "write", "delete".  Empty matches all.
	Operations []string `yaml:"operations"`

	// TargetRoles restricts routing to sites with these roles.
	// Valid values: "primary", "backup", "burst".  Empty returns all sites.
	TargetRoles []string `yaml:"target_roles"`

	// Priority controls evaluation order.  Lower values = higher precedence.
	Priority int `yaml:"priority"`
}

// PolicyConfig holds policy engine configuration.
type PolicyConfig struct {
	// Rules is the ordered list of routing rules loaded from YAML.
	Rules []PolicyRuleConfig `yaml:"rules"`
}

// Configuration represents the complete GlobalFS configuration.
type Configuration struct {
	// Global settings
	Global GlobalConfig `yaml:"global"`

	// Coordinator settings
	Coordinator types.CoordinatorConfig `yaml:"coordinator"`

	// Sites configuration
	Sites []SiteConfig `yaml:"sites"`

	// Policy routing rules
	Policy PolicyConfig `yaml:"policy"`

	// Replication policies (legacy placement policies; superseded by Policy)
	Policies []types.ReplicationPolicy `yaml:"policies"`

	// Performance tuning
	Performance types.PerformanceConfig `yaml:"performance"`
}

// GlobalConfig contains global settings.
type GlobalConfig struct {
	// ClusterName is the name of this GlobalFS cluster.
	ClusterName string `yaml:"cluster_name"`

	// LogLevel defines the logging level (DEBUG, INFO, WARN, ERROR).
	LogLevel string `yaml:"log_level"`

	// LogFile is the path to the log file (empty for stdout).
	LogFile string `yaml:"log_file"`

	// MetricsEnabled enables Prometheus metrics export.
	MetricsEnabled bool `yaml:"metrics_enabled"`

	// MetricsPort is the port for metrics endpoint.
	MetricsPort int `yaml:"metrics_port"`
}

// SiteConfig defines a GlobalFS site.
type SiteConfig struct {
	// Name is the unique site identifier.
	Name string `yaml:"name"`

	// Role defines the site's role (primary, burst, backup).
	Role types.SiteRole `yaml:"role"`

	// ObjectFS configuration
	ObjectFS ObjectFSConfig `yaml:"objectfs"`

	// CargoShip configuration
	CargoShip CargoShipConfig `yaml:"cargoship"`

	// Network configuration
	Network types.NetworkConfig `yaml:"network"`
}

// ObjectFSConfig contains ObjectFS-specific settings for a site.
type ObjectFSConfig struct {
	// MountPoint is the local path where ObjectFS is mounted.
	MountPoint string `yaml:"mount_point"`

	// S3Bucket is the S3 bucket backing this ObjectFS instance.
	S3Bucket string `yaml:"s3_bucket"`

	// S3Region is the AWS region for the S3 bucket.
	S3Region string `yaml:"s3_region"`

	// S3Endpoint is an optional custom S3 endpoint (for MinIO, etc.).
	S3Endpoint string `yaml:"s3_endpoint,omitempty"`
}

// CargoShipConfig contains CargoShip-specific settings.
type CargoShipConfig struct {
	// Endpoint is the CargoShip service endpoint.
	Endpoint string `yaml:"endpoint"`

	// Enabled indicates if CargoShip is enabled for this site.
	Enabled bool `yaml:"enabled"`
}

// NewDefault returns a default configuration.
func NewDefault() *Configuration {
	return &Configuration{
		Global: GlobalConfig{
			ClusterName:    "globalfs-cluster",
			LogLevel:       "INFO",
			MetricsEnabled: true,
			MetricsPort:    9090,
		},
		Coordinator: types.CoordinatorConfig{
			ListenAddr:          ":8080",
			EtcdEndpoints:       []string{"localhost:2379"},
			LeaseTimeout:        60 * time.Second,
			HealthCheckInterval: 30 * time.Second,
		},
		Sites:    []SiteConfig{},
		Policies: []types.ReplicationPolicy{},
		Performance: types.PerformanceConfig{
			MaxConcurrentTransfers: 8,
			TransferChunkSize:      16 * 1024 * 1024, // 16MB
			CacheSize:              "1GB",
		},
	}
}

// LoadFromFile loads configuration from a YAML file.
func (c *Configuration) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

// SaveToFile saves configuration to a YAML file.
func (c *Configuration) SaveToFile(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate validates the configuration.
func (c *Configuration) Validate() error {
	// Validate global settings
	if c.Global.ClusterName == "" {
		return fmt.Errorf("global.cluster_name is required")
	}

	// Validate coordinator settings
	if c.Coordinator.ListenAddr == "" {
		return fmt.Errorf("coordinator.listen_addr is required")
	}
	if len(c.Coordinator.EtcdEndpoints) == 0 {
		return fmt.Errorf("coordinator.etcd_endpoints is required")
	}

	// Validate sites
	if len(c.Sites) == 0 {
		return fmt.Errorf("at least one site is required")
	}

	siteNames := make(map[string]bool)
	hasPrimary := false

	for i, site := range c.Sites {
		if site.Name == "" {
			return fmt.Errorf("sites[%d].name is required", i)
		}
		if siteNames[site.Name] {
			return fmt.Errorf("duplicate site name: %s", site.Name)
		}
		siteNames[site.Name] = true

		if site.Role == types.SiteRolePrimary {
			hasPrimary = true
		}

		// Validate ObjectFS config
		if site.ObjectFS.MountPoint == "" {
			return fmt.Errorf("sites[%d].objectfs.mount_point is required", i)
		}
		if site.ObjectFS.S3Bucket == "" {
			return fmt.Errorf("sites[%d].objectfs.s3_bucket is required", i)
		}
		if site.ObjectFS.S3Region == "" {
			return fmt.Errorf("sites[%d].objectfs.s3_region is required", i)
		}
	}

	if !hasPrimary {
		return fmt.Errorf("at least one site with role 'primary' is required")
	}

	// Validate policies
	for i, policy := range c.Policies {
		if policy.Name == "" {
			return fmt.Errorf("policies[%d].name is required", i)
		}
		if policy.PathPattern == "" {
			return fmt.Errorf("policies[%d].path_pattern is required", i)
		}
		if policy.Primary != "" && !siteNames[policy.Primary] {
			return fmt.Errorf("policies[%d].primary references unknown site: %s", i, policy.Primary)
		}
		for _, replica := range policy.ReplicateTo {
			if !siteNames[replica] {
				return fmt.Errorf("policies[%d].replicate_to references unknown site: %s", i, replica)
			}
		}
	}

	return nil
}

// GetSite returns the configuration for a named site.
func (c *Configuration) GetSite(name string) (*SiteConfig, error) {
	for _, site := range c.Sites {
		if site.Name == name {
			return &site, nil
		}
	}
	return nil, fmt.Errorf("site not found: %s", name)
}

// GetPrimarySite returns the primary site configuration.
func (c *Configuration) GetPrimarySite() (*SiteConfig, error) {
	for _, site := range c.Sites {
		if site.Role == types.SiteRolePrimary {
			return &site, nil
		}
	}
	return nil, fmt.Errorf("no primary site configured")
}
