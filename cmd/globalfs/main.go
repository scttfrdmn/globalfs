// Command globalfs is the operator CLI for a GlobalFS deployment.
//
// It communicates exclusively with the coordinator daemon over HTTP and does
// not require direct access to any storage site.
//
// Usage:
//
//	globalfs [--coordinator-addr <addr>] [--json] <command>
//
// Commands:
//
//	site list                                             – list sites and health
//	site add --name <n> --uri s3://bucket --role <r>     – register a site
//	site remove --name <n>                                – deregister a site
//	replicate --key <k> --from <site> --to <site>         – trigger replication
//	object get  <key> [--output <file>]                   – download object
//	object put  <key> [--input  <file>]                   – upload object
//	object delete <key>                                   – delete object
//	object head   <key>                                   – show object metadata
//	object list  [--prefix <p>] [--limit <n>]             – list objects
//	config validate <file>                                – validate a config file
//	config show     <file>                                – show resolved config
//	config init     [--output <file>]                     – write starter config template
//	info                                                  – coordinator runtime stats
//	status                                                – overall health
//	version                                               – print CLI version
//	completion bash|zsh|fish|powershell                   – shell completions
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/scttfrdmn/globalfs/pkg/client"
	"github.com/scttfrdmn/globalfs/pkg/config"
)

// version is set via -ldflags at build time (see Makefile).
// Falls back to "dev" so that `go run` / `go build` without the Makefile
// never reports a real release version.
var version = "dev"

// coordinatorAddr, apiKey, and jsonOutput are global flags inherited by all subcommands.
var (
	coordinatorAddr string
	apiKey          string
	jsonOutput      bool
)

func main() {
	if err := buildRoot().Execute(); err != nil {
		os.Exit(1)
	}
}

// ── Root command ──────────────────────────────────────────────────────────────

func buildRoot() *cobra.Command {
	root := &cobra.Command{
		Use:          "globalfs",
		Short:        "GlobalFS operator CLI",
		Long:         "Inspect and control a running GlobalFS deployment.",
		SilenceUsage: true,
	}

	root.PersistentFlags().StringVar(
		&coordinatorAddr,
		"coordinator-addr",
		envOrDefault("GLOBALFS_COORDINATOR", "http://localhost:8090"),
		"Coordinator HTTP address (env: GLOBALFS_COORDINATOR)",
	)
	root.PersistentFlags().StringVar(
		&apiKey,
		"api-key",
		envOrDefault("GLOBALFS_API_KEY", ""),
		"API key for X-GlobalFS-API-Key authentication (env: GLOBALFS_API_KEY)",
	)
	root.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	root.AddCommand(
		buildSiteCmd(),
		buildObjectCmd(),
		buildConfigCmd(),
		buildReplicateCmd(),
		buildInfoCmd(),
		buildStatusCmd(),
		buildVersionCmd(),
		buildCompletionCmd(root),
	)
	return root
}

// ── site ──────────────────────────────────────────────────────────────────────

func buildSiteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "site",
		Short: "Manage registered sites",
	}
	cmd.AddCommand(buildSiteListCmd(), buildSiteAddCmd(), buildSiteRemoveCmd())
	return cmd
}

// site list ───────────────────────────────────────────────────────────────────

func buildSiteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered sites and their health",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := newClient()
			sites, err := c.ListSites(context.Background())
			if err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), sites)
			}

			// Show a CIRCUIT column only when at least one site has circuit
			// state data (i.e. a circuit breaker is configured).
			showCircuit := false
			for _, s := range sites {
				if s.CircuitState != "" {
					showCircuit = true
					break
				}
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			if showCircuit {
				fmt.Fprintln(tw, "NAME\tROLE\tSTATUS\tCIRCUIT")
			} else {
				fmt.Fprintln(tw, "NAME\tROLE\tSTATUS")
			}
			for _, s := range sites {
				status := "healthy"
				if !s.Healthy {
					status = "degraded"
					if s.Error != "" {
						status += ": " + s.Error
					}
				}
				if showCircuit {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, s.Role, status, s.CircuitState)
				} else {
					fmt.Fprintf(tw, "%s\t%s\t%s\n", s.Name, s.Role, status)
				}
			}
			return tw.Flush()
		},
	}
}

// site add ────────────────────────────────────────────────────────────────────

func buildSiteAddCmd() *cobra.Command {
	var (
		name string
		uri  string
		role string
	)
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Register a new site with the coordinator",
		Example: `  globalfs site add --name onprem --uri s3://my-bucket?region=us-west-2 --role primary
  globalfs site add --name cloud --uri s3://burst-bucket?region=us-east-1&endpoint=https://s3.amazonaws.com --role burst`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if uri == "" {
				return fmt.Errorf("--uri is required")
			}

			bucket, region, endpoint, err := parseS3URI(uri)
			if err != nil {
				return fmt.Errorf("invalid --uri: %w", err)
			}
			if role == "" {
				role = "primary"
			}

			c := newClient()
			added, err := c.AddSite(context.Background(), client.AddSiteRequest{
				Name:       name,
				Role:       role,
				S3Bucket:   bucket,
				S3Region:   region,
				S3Endpoint: endpoint,
			})
			if err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), added)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Site %q (%s) registered successfully.\n", added.Name, added.Role)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Site name (required)")
	cmd.Flags().StringVar(&uri, "uri", "", "S3 URI, e.g. s3://bucket?region=us-west-2 (required)")
	cmd.Flags().StringVar(&role, "role", "primary", "Site role: primary|backup|burst")
	return cmd
}

// site remove ─────────────────────────────────────────────────────────────────

func buildSiteRemoveCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Deregister a site from the coordinator",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}

			c := newClient()
			if err := c.RemoveSite(context.Background(), name); err != nil {
				return err
			}
			if !jsonOutput {
				fmt.Fprintf(cmd.OutOrStdout(), "Site %q deregistered.\n", name)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Site name to remove (required)")
	return cmd
}

// ── object ────────────────────────────────────────────────────────────────────

func buildObjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "object",
		Short: "Manage objects in the global namespace",
	}
	cmd.AddCommand(
		buildObjectGetCmd(),
		buildObjectPutCmd(),
		buildObjectDeleteCmd(),
		buildObjectHeadCmd(),
		buildObjectListCmd(),
	)
	return cmd
}

// object get ──────────────────────────────────────────────────────────────────

func buildObjectGetCmd() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Download an object to a file or stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			c := newClient()
			data, err := c.GetObject(context.Background(), key)
			if err != nil {
				return err
			}

			if output == "" || output == "-" {
				_, err = cmd.OutOrStdout().Write(data)
				return err
			}

			f, err := os.Create(output)
			if err != nil {
				return fmt.Errorf("create %q: %w", output, err)
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				return fmt.Errorf("write %q: %w", output, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s: %d bytes\n", output, len(data))
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output file (- for stdout)")
	return cmd
}

// object put ──────────────────────────────────────────────────────────────────

type objectPutResult struct {
	Key   string `json:"key"`
	Bytes int    `json:"bytes"`
}

func buildObjectPutCmd() *cobra.Command {
	var input string
	cmd := &cobra.Command{
		Use:   "put <key>",
		Short: "Upload an object from a file or stdin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			var r io.Reader
			if input == "" || input == "-" {
				r = cmd.InOrStdin()
			} else {
				f, err := os.Open(input)
				if err != nil {
					return fmt.Errorf("open %q: %w", input, err)
				}
				defer f.Close()
				r = f
			}

			data, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}

			c := newClient()
			if err := c.PutObject(context.Background(), key, data); err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), objectPutResult{Key: key, Bytes: len(data)})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "stored %q (%d bytes)\n", key, len(data))
			return nil
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "Input file (- for stdin)")
	return cmd
}

// object delete ───────────────────────────────────────────────────────────────

type objectDeleteResult struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
}

func buildObjectDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete an object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			c := newClient()
			if err := c.DeleteObject(context.Background(), key); err != nil {
				return err
			}
			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), objectDeleteResult{Key: key, Deleted: true})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted %q\n", key)
			return nil
		},
	}
}

// object head ─────────────────────────────────────────────────────────────────

func buildObjectHeadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "head <key>",
		Short: "Show object metadata without downloading content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			c := newClient()
			info, err := c.HeadObject(context.Background(), key)
			if err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), info)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "KEY\tSIZE\tLAST_MODIFIED\tETAG\tCHECKSUM")
			lm := ""
			if !info.LastModified.IsZero() {
				lm = info.LastModified.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n",
				info.Key, info.Size, lm, info.ETag, info.Checksum)
			return tw.Flush()
		},
	}
}

// object list ─────────────────────────────────────────────────────────────────

func buildObjectListCmd() *cobra.Command {
	var (
		prefix string
		limit  int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List objects in the global namespace",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := newClient()
			objects, err := c.ListObjects(context.Background(), prefix, limit)
			if err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), objects)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "KEY\tSIZE\tLAST_MODIFIED")
			for _, o := range objects {
				lm := ""
				if !o.LastModified.IsZero() {
					lm = o.LastModified.UTC().Format(time.RFC3339)
				}
				fmt.Fprintf(tw, "%s\t%d\t%s\n", o.Key, o.Size, lm)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "Key prefix filter")
	cmd.Flags().IntVar(&limit, "limit", 0, "Maximum number of results (0 = all)")
	return cmd
}

// ── config ────────────────────────────────────────────────────────────────────

func buildConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Work with coordinator configuration files",
	}
	cmd.AddCommand(buildConfigValidateCmd(), buildConfigShowCmd(), buildConfigInitCmd())
	return cmd
}

// config validate ─────────────────────────────────────────────────────────────

func buildConfigValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <file>",
		Short: "Validate a coordinator YAML configuration file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(args[0]); err != nil {
				return fmt.Errorf("cannot parse %s: %w", args[0], err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Configuration is valid.")
			return nil
		},
	}
}

// config show ─────────────────────────────────────────────────────────────────

func buildConfigShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <file>",
		Short: "Show the resolved configuration (after applying defaults)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.NewDefault()
			if err := cfg.LoadFromFile(args[0]); err != nil {
				return fmt.Errorf("cannot parse %s: %w", args[0], err)
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), cfg)
			}

			data, err := yaml.Marshal(cfg)
			if err != nil {
				return fmt.Errorf("marshal config: %w", err)
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
}

// config init ─────────────────────────────────────────────────────────────────

// configTemplate is a commented starter YAML that serves as a practical example.
const configTemplate = `# GlobalFS coordinator configuration
# See https://github.com/scttfrdmn/globalfs for full documentation.

global:
  cluster_name: my-globalfs-cluster
  log_level: INFO             # DEBUG | INFO | WARN | ERROR
  metrics_enabled: true
  metrics_port: 9090

coordinator:
  listen_addr: ":8090"
  etcd_endpoints:
    - localhost:2379
  lease_timeout: 60s
  health_check_interval: 30s

sites:
  - name: primary
    role: primary             # primary | backup | burst
    objectfs:
      mount_point: /mnt/globalfs/primary
      s3_bucket: my-primary-bucket
      s3_region: us-west-2
    cargoship:
      enabled: false
      endpoint: ""

  # - name: backup
  #   role: backup
  #   objectfs:
  #     mount_point: /mnt/globalfs/backup
  #     s3_bucket: my-backup-bucket
  #     s3_region: us-east-1

policy:
  rules: []
  # rules:
  #   - name: read-from-primary
  #     key_pattern: ""
  #     operations: [read]
  #     target_roles: [primary]
  #     priority: 10

performance:
  max_concurrent_transfers: 8
  transfer_chunk_size: 16777216  # 16 MiB
  cache_size: "1GB"

# Resilience — fault tolerance for site routing
resilience:
  health_poll_interval: 30s   # background health check cadence

  circuit_breaker:
    enabled: false            # set true to isolate failing sites automatically
    threshold: 5              # consecutive failures before circuit opens
    cooldown: 30s             # wait before probe after circuit opens

  retry:
    enabled: false            # set true to retry transient read failures
    max_attempts: 3           # total attempts per site (1 = no retry)
    initial_delay: 100ms      # pause before first retry
    max_delay: 2s             # cap on inter-retry pause
    multiplier: 2.0           # exponential scale factor

# Cache — in-memory LRU object cache for read-hot workloads
cache:
  enabled: false              # set true to enable read-through caching
  max_bytes: 67108864         # maximum cache size in bytes (default 64 MiB)
  ttl: 0s                     # entry TTL; 0 means entries never expire
`

func buildConfigInitCmd() *cobra.Command {
	var outputPath string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Write a starter configuration template",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if outputPath == "" {
				_, err := fmt.Fprint(cmd.OutOrStdout(), configTemplate)
				return err
			}

			if err := os.WriteFile(outputPath, []byte(configTemplate), 0644); err != nil {
				return fmt.Errorf("write %s: %w", outputPath, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Configuration template written to %s\n", outputPath)
			return nil
		},
	}
	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "Write template to file instead of stdout")
	return cmd
}

// ── replicate ─────────────────────────────────────────────────────────────────

func buildReplicateCmd() *cobra.Command {
	var (
		key  string
		from string
		to   string
	)
	cmd := &cobra.Command{
		Use:   "replicate",
		Short: "Trigger manual replication of an object",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if key == "" {
				return fmt.Errorf("--key is required")
			}
			if from == "" {
				return fmt.Errorf("--from is required")
			}
			if to == "" {
				return fmt.Errorf("--to is required")
			}

			c := newClient()
			result, err := c.Replicate(context.Background(), client.ReplicateRequest{
				Key: key, From: from, To: to,
			})
			if err != nil {
				return err
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), result)
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"Replication accepted: key=%q from=%s to=%s\n",
				result.Key, result.From, result.To)
			return nil
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Object key to replicate (required)")
	cmd.Flags().StringVar(&from, "from", "", "Source site name (required)")
	cmd.Flags().StringVar(&to, "to", "", "Destination site name (required)")
	return cmd
}

// ── info ──────────────────────────────────────────────────────────────────────

// coordinatorInfo mirrors the infoResponse from the coordinator's /api/v1/info endpoint.
type coordinatorInfo struct {
	Version               string         `json:"version"`
	UptimeSeconds         float64        `json:"uptime_seconds"`
	Sites                 int            `json:"sites"`
	SitesByRole           map[string]int `json:"sites_by_role"`
	ReplicationQueueDepth int            `json:"replication_queue_depth"`
	IsLeader              bool           `json:"is_leader"`
}

func buildInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show coordinator runtime statistics",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := apiGet("/api/v1/info")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmtAPIError(resp)
			}

			var info coordinatorInfo
			if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), info)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintf(tw, "version\t%s\n", info.Version)
			fmt.Fprintf(tw, "uptime\t%s\n", formatUptime(info.UptimeSeconds))
			fmt.Fprintf(tw, "sites\t%d\n", info.Sites)
			for _, role := range []string{"primary", "backup", "burst"} {
				if n := info.SitesByRole[role]; n > 0 {
					fmt.Fprintf(tw, "  %s\t%d\n", role, n)
				}
			}
			fmt.Fprintf(tw, "replication_queue\t%d\n", info.ReplicationQueueDepth)
			fmt.Fprintf(tw, "is_leader\t%v\n", info.IsLeader)
			return tw.Flush()
		},
	}
}

// formatUptime converts a float64 number of seconds into a human-readable string
// of the form "1h2m3s", "45m0s", or "12s".
func formatUptime(secs float64) string {
	d := time.Duration(secs) * time.Second
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// ── status ────────────────────────────────────────────────────────────────────

type statusResponse struct {
	Status  string `json:"status"`
	Details string `json:"details,omitempty"`
}

func buildStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show overall system health",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := apiGet("/healthz")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			text := string(bytes.TrimSpace(body))

			healthy := resp.StatusCode == http.StatusOK
			status := "OK"
			details := ""
			if !healthy {
				status = "DEGRADED"
				// Strip the leading "DEGRADED\n" the coordinator writes.
				lines := bytes.SplitN(bytes.TrimSpace(body), []byte("\n"), 2)
				if len(lines) > 1 {
					details = string(bytes.TrimSpace(lines[1]))
				} else {
					details = text
				}
			}

			if jsonOutput {
				out := statusResponse{Status: status, Details: details}
				if err := printJSON(cmd.OutOrStdout(), out); err != nil {
					return err
				}
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), status)
				if details != "" {
					fmt.Fprintln(cmd.OutOrStdout(), details)
				}
			}

			if !healthy {
				return fmt.Errorf("coordinator is degraded")
			}
			return nil
		},
	}
}

// ── version ───────────────────────────────────────────────────────────────────

func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the globalfs CLI version",
		Run: func(cmd *cobra.Command, _ []string) {
			if jsonOutput {
				_ = printJSON(cmd.OutOrStdout(), map[string]string{"version": version})
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "globalfs %s\n", version)
			}
		},
	}
}

// ── completion ────────────────────────────────────────────────────────────────

func buildCompletionCmd(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:       "completion [bash|zsh|fish|powershell]",
		Short:     "Generate shell completion scripts",
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		Args:      cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return root.GenBashCompletion(os.Stdout)
			case "zsh":
				return root.GenZshCompletion(os.Stdout)
			case "fish":
				return root.GenFishCompletion(os.Stdout, true)
			case "powershell":
				return root.GenPowerShellCompletion(os.Stdout)
			default:
				return fmt.Errorf("unsupported shell: %s (bash|zsh|fish|powershell)", args[0])
			}
		},
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 30 * time.Second}

// newClient creates a coordinator client with the current global flags applied.
func newClient() *client.Client {
	return client.New(coordinatorAddr, client.WithAPIKey(apiKey))
}

func apiURL(path string) string {
	return coordinatorAddr + path
}

// setAuthHeader adds the API key header to req when a key is configured.
func setAuthHeader(req *http.Request) {
	if apiKey != "" {
		req.Header.Set("X-GlobalFS-API-Key", apiKey)
	}
}

func apiGet(path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, apiURL(path), nil)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	setAuthHeader(req)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	return resp, nil
}

func fmtAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	var e struct{ Error string }
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return fmt.Errorf("coordinator error (%d): %s", resp.StatusCode, e.Error)
	}
	return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
}

// ── Output helpers ────────────────────────────────────────────────────────────

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ── URI parsing ───────────────────────────────────────────────────────────────

// parseS3URI parses a URI like s3://bucket?region=us-west-2&endpoint=https://...
// and returns (bucket, region, endpoint).
func parseS3URI(raw string) (bucket, region, endpoint string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", "", fmt.Errorf("scheme must be s3, got %q", u.Scheme)
	}
	bucket = u.Host
	if bucket == "" {
		return "", "", "", fmt.Errorf("bucket name is required (e.g. s3://bucket-name)")
	}
	q := u.Query()
	region = q.Get("region")
	endpoint = q.Get("endpoint")
	return bucket, region, endpoint, nil
}

// ── Misc ──────────────────────────────────────────────────────────────────────

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
