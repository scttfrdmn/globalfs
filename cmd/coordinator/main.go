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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/scttfrdmn/globalfs/internal/cache"
	"github.com/scttfrdmn/globalfs/internal/circuitbreaker"
	"github.com/scttfrdmn/globalfs/internal/coordinator"
	"github.com/scttfrdmn/globalfs/internal/metrics"
	"github.com/scttfrdmn/globalfs/internal/policy"
	"github.com/scttfrdmn/globalfs/internal/retry"
	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// version is set via -ldflags at build time (see Makefile).
var version = "0.1.0"

func main() {
	configPath       := flag.String("config", "", "Path to YAML configuration file")
	logLevelStr      := flag.String("log-level", "INFO", "Log level: DEBUG, INFO, WARN, ERROR")
	bindAddr         := flag.String("bind-addr", "", "HTTP server address (default :8090, or coordinator.listen_addr from config)")
	apiKeyFlag       := flag.String("api-key", "", "Shared API key for X-GlobalFS-API-Key auth (env: GLOBALFS_API_KEY; empty = disabled)")
	healthPollStr    := flag.String("health-poll-interval", "", "Interval between background site health checks (e.g. 15s, 1m); overrides config")
	showVersion      := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	// Env var overrides the flag default if the flag was not explicitly set.
	apiKey := *apiKeyFlag
	if apiKey == "" {
		apiKey = os.Getenv("GLOBALFS_API_KEY")
	}

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

	m := metrics.New(prometheus.DefaultRegisterer)
	c.SetMetrics(m)

	// ── Resilience: health polling ────────────────────────────────────────────
	// Priority: explicit --health-poll-interval flag > resilience.health_poll_interval
	// in config > built-in default (30s inside coordinator.Start).
	healthPoll := cfg.Resilience.HealthPollInterval
	if *healthPollStr != "" {
		if d, err := time.ParseDuration(*healthPollStr); err == nil && d > 0 {
			healthPoll = d
		} else if err != nil {
			slog.Warn("invalid --health-poll-interval; ignoring", "value", *healthPollStr, "error", err)
		}
	}
	if healthPoll > 0 {
		c.SetHealthPollInterval(healthPoll)
		slog.Info("health polling configured", "interval", healthPoll)
	}

	// ── Resilience: circuit breaker ───────────────────────────────────────────
	if cfg.Resilience.CircuitBreaker.Enabled {
		cbCfg := cfg.Resilience.CircuitBreaker
		threshold := cbCfg.Threshold
		if threshold <= 0 {
			threshold = 5
		}
		cooldown := cbCfg.Cooldown
		if cooldown <= 0 {
			cooldown = 30 * time.Second
		}
		cb := circuitbreaker.New(threshold, cooldown)
		c.SetCircuitBreaker(cb)
		slog.Info("circuit breaker enabled", "threshold", threshold, "cooldown", cooldown)
	}

	// ── Resilience: retry ─────────────────────────────────────────────────────
	if cfg.Resilience.Retry.Enabled {
		retryCfg := cfg.Resilience.Retry
		maxAttempts := retryCfg.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}
		initialDelay := retryCfg.InitialDelay
		if initialDelay <= 0 {
			initialDelay = 100 * time.Millisecond
		}
		maxDelay := retryCfg.MaxDelay
		if maxDelay <= 0 {
			maxDelay = 2 * time.Second
		}
		multiplier := retryCfg.Multiplier
		if multiplier < 1.0 {
			multiplier = 2.0
		}
		c.SetRetryConfig(&retry.Config{
			MaxAttempts:  maxAttempts,
			InitialDelay: initialDelay,
			MaxDelay:     maxDelay,
			Multiplier:   multiplier,
		})
		slog.Info("retry configured",
			"max_attempts", maxAttempts,
			"initial_delay", initialDelay,
			"max_delay", maxDelay,
			"multiplier", multiplier)
	}

	// ── Cache ──────────────────────────────────────────────────────────────────
	if cfg.Cache.Enabled {
		maxBytes := cfg.Cache.MaxBytes
		if maxBytes <= 0 {
			maxBytes = 64 * 1024 * 1024 // 64 MiB
		}
		oc := cache.New(cache.Config{
			MaxBytes: maxBytes,
			TTL:      cfg.Cache.TTL,
		})
		c.SetCache(oc)
		slog.Info("object cache enabled", "max_bytes", maxBytes, "ttl", cfg.Cache.TTL)
	}

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

	startTime := time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler(c))
	mux.HandleFunc("/readyz", readyzHandler())
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("GET /api/v1/info", infoHandler(c, version, startTime))
	registerAPIRoutes(mux, ctx, c, m)

	// Build middleware chain (applied innermost → outermost):
	//   mux → apiKey (when set) → logging → requestID
	// requestID is outermost: every response gets a correlation ID.
	// logging wraps apiKey so auth rejections are also recorded.
	var handler http.Handler = mux
	if apiKey != "" {
		handler = apiKeyMiddleware(apiKey)(handler)
		slog.Info("API key authentication enabled")
	}
	handler = loggingMiddleware(handler)
	handler = requestIDMiddleware(handler)

	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
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
//
// It uses the background health poll cache when available, avoiding live S3
// calls on every probe.  On first startup (before the initial poll completes)
// it falls back to a live health check.
func healthzHandler(c *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report, _ := c.HealthStatus()
		if report == nil {
			// Cache not yet populated — fall back to a live check.
			report = c.Health(r.Context())
		}

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
