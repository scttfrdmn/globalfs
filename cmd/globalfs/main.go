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

	"github.com/scttfrdmn/globalfs/pkg/client"
)

// version is set via -ldflags at build time (see Makefile).
var version = "0.1.0-alpha"

// coordinatorAddr and jsonOutput are global flags inherited by all subcommands.
var (
	coordinatorAddr string
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
	root.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	root.AddCommand(
		buildSiteCmd(),
		buildObjectCmd(),
		buildReplicateCmd(),
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

// siteInfo mirrors coordinator.SiteInfo for JSON decoding.
type siteInfo struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

func buildSiteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered sites and their health",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := apiGet("/api/v1/sites")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmtAPIError(resp)
			}

			var sites []siteInfo
			if err := json.NewDecoder(resp.Body).Decode(&sites); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			if jsonOutput {
				return printJSON(cmd.OutOrStdout(), sites)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tROLE\tSTATUS")
			for _, s := range sites {
				status := "healthy"
				if !s.Healthy {
					status = "degraded"
					if s.Error != "" {
						status += ": " + s.Error
					}
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\n", s.Name, s.Role, status)
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

			body := map[string]string{
				"name":        name,
				"role":        role,
				"s3_bucket":   bucket,
				"s3_region":   region,
				"s3_endpoint": endpoint,
			}
			resp, err := apiPost("/api/v1/sites", body)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				return fmtAPIError(resp)
			}

			var added siteInfo
			if err := json.NewDecoder(resp.Body).Decode(&added); err != nil {
				return fmt.Errorf("decode response: %w", err)
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

			resp, err := apiDelete(fmt.Sprintf("/api/v1/sites/%s", url.PathEscape(name)))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusNoContent:
				if !jsonOutput {
					fmt.Fprintf(cmd.OutOrStdout(), "Site %q deregistered.\n", name)
				}
				return nil
			default:
				return fmtAPIError(resp)
			}
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
			c := client.New(coordinatorAddr)
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

			c := client.New(coordinatorAddr)
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
			c := client.New(coordinatorAddr)
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
			c := client.New(coordinatorAddr)
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
			c := client.New(coordinatorAddr)
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

// ── replicate ─────────────────────────────────────────────────────────────────

type replicateResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	From   string `json:"from"`
	To     string `json:"to"`
}

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

			body := map[string]string{"key": key, "from": from, "to": to}
			resp, err := apiPost("/api/v1/replicate", body)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				return fmtAPIError(resp)
			}

			var result replicateResponse
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("decode response: %w", err)
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

func apiURL(path string) string {
	return coordinatorAddr + path
}

func apiGet(path string) (*http.Response, error) {
	resp, err := httpClient.Get(apiURL(path))
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	return resp, nil
}

func apiPost(path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := httpClient.Post(apiURL(path), "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	return resp, nil
}

func apiDelete(path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, apiURL(path), nil)
	if err != nil {
		return nil, fmt.Errorf("DELETE %s: %w", path, err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("DELETE %s: %w", path, err)
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
