// Command globalfs-coordinator is the long-running GlobalFS coordinator daemon.
//
// It loads site configuration from a YAML file, builds SiteMount instances,
// starts the in-memory coordinator and background replication worker, and
// exposes an HTTP server for health checks and Prometheus metrics.
//
// Usage:
//
//	globalfs-coordinator [flags]
//
// Flags:
//
//	--config     Path to YAML configuration file
//	--log-level  Log level: DEBUG, INFO, WARN, ERROR  (default INFO)
//	--bind-addr  HTTP server address                  (default :8090)
//	--version    Print version and exit
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/internal/policy"
	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// version is set via -ldflags at build time (see Makefile).
var version = "0.1.0-alpha"

func main() {
	configPath  := flag.String("config", "", "Path to YAML configuration file")
	logLevelStr := flag.String("log-level", "INFO", "Log level: DEBUG, INFO, WARN, ERROR")
	bindAddr    := flag.String("bind-addr", "", "HTTP server address (default :8090, or coordinator.listen_addr from config)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("globalfs-coordinator %s\n", version)
		os.Exit(0)
	}

	setupLogger(*logLevelStr)

	// ── Load configuration ────────────────────────────────────────────────────

	cfg := config.NewDefault()
	if *configPath != "" {
		if err := cfg.LoadFromFile(*configPath); err != nil {
			slog.Error("failed to load configuration", "path", *configPath, "error", err)
			os.Exit(1)
		}
		slog.Info("configuration loaded", "path", *configPath)
	}

	// Resolve bind address: explicit flag > config > default.
	addr := *bindAddr
	if addr == "" {
		addr = cfg.Coordinator.ListenAddr
	}
	if addr == "" {
		addr = ":8090"
	}

	// Override log level from config if not set via flag.
	if *logLevelStr == "INFO" && cfg.Global.LogLevel != "" {
		setupLogger(cfg.Global.LogLevel)
	}

	// ── Build site mounts ─────────────────────────────────────────────────────

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mounts []*site.SiteMount
	for i := range cfg.Sites {
		m, err := site.NewFromConfig(ctx, &cfg.Sites[i])
		if err != nil {
			slog.Warn("failed to connect to site; skipping",
				"site", cfg.Sites[i].Name, "error", err)
			continue
		}
		mounts = append(mounts, m)
		slog.Info("site connected", "name", m.Name(), "role", m.Role())
	}

	if len(mounts) == 0 {
		slog.Warn("no sites available; coordinator will serve health endpoints but route no traffic")
	}

	// ── Build coordinator ─────────────────────────────────────────────────────

	c := coordinator.New(mounts...)

	if len(cfg.Policy.Rules) > 0 {
		eng, err := policy.NewFromConfig(cfg.Policy.Rules)
		if err != nil {
			slog.Error("failed to build policy engine", "error", err)
			os.Exit(1)
		}
		c.SetPolicy(eng)
		slog.Info("policy engine loaded", "rules", len(cfg.Policy.Rules))
	}

	c.Start(ctx)
	slog.Info("coordinator started",
		"sites", len(mounts),
		"cluster", cfg.Global.ClusterName,
		"bind-addr", addr)

	// ── HTTP server ───────────────────────────────────────────────────────────

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler(c))
	mux.HandleFunc("/readyz", readyzHandler())
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("HTTP server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig)

	// Stop accepting new HTTP requests (30s drain window).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP server shutdown error", "error", err)
	}

	// Cancel the main context so the replication worker exits its loop,
	// then stop the coordinator (waits for the current job to finish).
	cancel()
	c.Stop()

	// Close all site connections.
	for _, m := range mounts {
		if err := m.Close(); err != nil {
			slog.Warn("error closing site", "name", m.Name(), "error", err)
		}
	}

	slog.Info("coordinator stopped")
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

// healthzHandler returns 200 OK if all primary sites are healthy, 503 otherwise.
// A coordinator with no primary sites is considered healthy (pass-through).
func healthzHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report := c.Health(r.Context())

		var unhealthy []string
		for _, s := range c.Sites() {
			if s.Role() != types.SiteRolePrimary {
				continue
			}
			if err := report[s.Name()]; err != nil {
				unhealthy = append(unhealthy, fmt.Sprintf("%s: %v", s.Name(), err))
			}
		}
		sort.Strings(unhealthy)

		if len(unhealthy) == 0 {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "OK")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "DEGRADED\n%s\n", strings.Join(unhealthy, "\n"))
	}
}

// readyzHandler returns 200 OK once the coordinator is running.
// By the time the HTTP server is accepting requests the coordinator has
// already been started, so this endpoint always returns 200.
func readyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	}
}

// ── Logging ───────────────────────────────────────────────────────────────────

// setupLogger configures the global slog logger with the given level.
func setupLogger(level string) {
	var lvl slog.Level
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN", "WARNING":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h))
}
