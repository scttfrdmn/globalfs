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
	"io"
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
// Falls back to "dev" so that `go run` / `go build` without the Makefile
// never reports a real release version.
var version = "dev"

// coordinatorShutdownTimeout bounds coordinator teardown after the HTTP server
// has drained.  It is the outer half of #83: the coordinator bounds its own
// waits, and this bounds the whole of CloseContext including the site closes that
// follow the stop, so SIGTERM returns even if a site's connection-pool drain
// never does.
//
// 30 s matches internal/coordinator's own defaultStopTimeout and the HTTP drain
// window above, which keeps the worst case a caller has to reason about at two
// windows rather than three.  See newShutdownContext for why this is still
// load-bearing rather than redundant with the coordinator's own default.
const coordinatorShutdownTimeout = 30 * time.Second

// newShutdownContext returns the context used to bound coordinator teardown.
//
// It deliberately derives from context.Background() and *not* from the daemon's
// root context, which the shutdown path has already cancelled.  A context derived
// from a cancelled parent is born cancelled — Err() is non-nil immediately — so
// passing one to CloseContext would make every bounded wait inside it return at
// once: Worker.StopContext would abandon the in-flight transfer instead of
// letting it settle, and its terminal event would go unemitted with the drain
// already gone.  That is exactly the phantom-job condition #78 fixed, so
// deriving here would reintroduce it while appearing to add a safety bound.
//
// The cancellation of the root context is still what tells the worker's run loop
// to stop accepting new jobs; the point is that the *deadline for observing the
// current one finishing* has to be independent of it.
//
// This is a function rather than three inline lines so the derivation can be
// asserted in a test.  It is the kind of detail that reads as obviously correct
// either way and is only obviously wrong at 3 a.m. during a rolling restart.
func newShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), coordinatorShutdownTimeout)
}

// mustConfigure exits the daemon if a coordinator configuration call was refused.
//
// Every Set* call in main runs during boot, before Start, where the coordinator's
// contract is to return nil.  A non-nil error therefore cannot be caused by
// anything the operator wrote in the config file: it means the call arrived after
// Start, which at this point in main is unreachable and so is a programming error
// in this file — a reordering that moved a setter below Start.
//
// It is fatal rather than logged-and-continued because the failure is otherwise
// invisible in the way that matters.  The value came from the config file, so
// `config show` would keep reporting it while the daemon ran on the default —
// exactly the divergence between reported and effective configuration that #81
// and the yaml-tag bug produced, and the reason those were worth fixing.  A
// deployment is better off failing to start than running with a queue depth, a
// lease TTL, or a health cadence that nobody chose.
//
// The label is the operator-facing name of the setting rather than the Go method,
// since the person reading the log is likelier to be holding the YAML than the
// source.  internal/coordinator also logs the method name itself.
func mustConfigure(what string, err error) {
	if err == nil {
		return
	}
	slog.Error("coordinator configuration was refused; this is a bug in the daemon's boot order, "+
		"not a problem with your configuration file",
		"setting", what, "error", err)
	os.Exit(1)
}

func main() {
	configPath := flag.String("config", "", "Path to YAML configuration file")
	logLevelStr := flag.String("log-level", "INFO", "Log level: DEBUG, INFO, WARN, ERROR")
	bindAddr := flag.String("bind-addr", "", "HTTP server address (default :8090, or coordinator.listen_addr from config)")
	apiKeyFlag := flag.String("api-key", "", "Shared API key for X-GlobalFS-API-Key auth (env: GLOBALFS_API_KEY; empty = disabled)")
	healthPollStr := flag.String("health-poll-interval", "", "Interval between background site health checks (e.g. 15s, 1m); overrides config")
	showVersion := flag.Bool("version", false, "Print version and exit")
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
		addr = config.DefaultListenAddr
	}

	// Override log level from config only when the --log-level flag was not
	// explicitly provided on the command line.  flag.Visit visits only flags
	// that were actually set, so this correctly handles the case where the
	// user passes --log-level INFO explicitly (should not be overridden).
	var logLevelExplicit bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "log-level" {
			logLevelExplicit = true
		}
	})
	// Reconfigure the logger from config for level, destination, or both.  The
	// level from --log-level wins when it was given explicitly; global.log_file
	// has no flag equivalent, so it always applies (#98).
	logLevel := *logLevelStr
	if !logLevelExplicit && cfg.Global.LogLevel != "" {
		logLevel = cfg.Global.LogLevel
	}
	// The returned file is deliberately not closed.  It must stay open for the
	// whole process lifetime, and main exits via os.Exit on every path — including
	// the successful one — so a defer here would not run anyway.  Nothing is lost
	// by that: slog's TextHandler issues one unbuffered Write per record, so there
	// is no buffered output to flush, and the kernel closes the descriptor at
	// exit.  A defer would only imply a guarantee that does not exist.
	if setupLoggerTo(logLevel, cfg.Global.LogFile) != nil {
		slog.Info("logging to file", "path", cfg.Global.LogFile)
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
	mustConfigure("metrics", c.SetMetrics(m))

	// ── Leader lease TTL (from coordinator.lease_timeout) ─────────────────────
	if cfg.Coordinator.LeaseTimeout > 0 {
		mustConfigure("leader lease TTL", c.SetLeaseTTL(cfg.Coordinator.LeaseTimeout))
		slog.Info("leader lease TTL configured", "ttl", cfg.Coordinator.LeaseTimeout)
	}

	// ── Replication worker queue depth (from performance.replication_queue_depth) ─
	if cfg.Performance.ReplicationQueueDepth > 0 {
		mustConfigure("replication worker queue depth",
			c.SetWorkerQueueDepth(cfg.Performance.ReplicationQueueDepth))
		slog.Info("replication worker depth configured", "depth", cfg.Performance.ReplicationQueueDepth)
	}

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
		mustConfigure("health poll interval", c.SetHealthPollInterval(healthPoll))
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

	// A refused Start is fatal.  Under the lifecycle contract Start returns an
	// error wrapping ErrStopped, and a daemon that continued past it would serve
	// HTTP with no replication worker and no health poller: writes would reach
	// primaries and be reported as stored, /healthz would answer from a cache
	// nothing refreshes, and nothing in the log would say why.  That is #84's
	// silent-standby failure with a process supervisor keeping it alive.  Exiting
	// non-zero lets the supervisor restart into a fresh coordinator, which is the
	// only recovery a single-use lifecycle allows.
	if err := c.Start(ctx); err != nil {
		slog.Error("failed to start coordinator", "error", err)
		os.Exit(1)
	}
	slog.Info("coordinator started",
		"sites", len(mounts),
		"cluster", cfg.Global.ClusterName,
		"bind-addr", addr)

	// ── HTTP server ───────────────────────────────────────────────────────────

	startTime := time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler(c))
	mux.HandleFunc("/readyz", readyzHandler())

	// global.metrics_enabled now gates the route (#98).  It previously bound
	// nothing at all, so an operator who set it false to keep cardinality — or
	// site and bucket names — off a shared network got metrics served anyway,
	// with `config show` confirming the setting they had asked for.
	//
	// Registration is skipped entirely rather than the handler returning 404, so
	// there is no route to probe.  Note the coordinator still collects metrics
	// internally when this is off; the flag controls exposure, not measurement.
	if cfg.Global.MetricsEnabled {
		mux.Handle("/metrics", promhttp.Handler())
	} else {
		slog.Info("metrics endpoint disabled by global.metrics_enabled")
	}
	mux.HandleFunc("GET /api/v1/info", infoHandler(c, version, startTime))
	registerAPIRoutes(mux, ctx, c, m, cfg.Security)

	// buildHandler applies the middleware chain; see its doc comment for the
	// order and why the path-traversal guard has to wrap the mux directly.
	if apiKey != "" {
		slog.Info("API key authentication enabled")
	}
	handler := buildHandler(mux, apiKey)

	// ReadTimeout and WriteTimeout are absolute deadlines on the whole request and
	// the whole response, not idle timeouts. These values are correct for the JSON
	// control endpoints and wrong for the object routes sharing this server, which
	// is why the object handlers replace them per request via
	// http.ResponseController — see the transfer-deadline block in api.go. They
	// are kept strict here so that a slow or stalled control-plane request is
	// still cut off promptly; raising them globally would have been the wrong fix
	// for #75, since it weakens every endpoint to accommodate two.
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second, // mitigate Slowloris slow-header attacks
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second, // cap keep-alive lifetime
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

	// Stop accepting new HTTP requests.  Same budget as the coordinator teardown
	// below, and the two are sequential, so the worst-case time from signal to exit
	// is 2 × coordinatorShutdownTimeout.  That matters for the termination grace
	// period configured in whatever supervises this process: at 60 s total it must
	// be higher than that, or the supervisor's SIGKILL arrives first and the
	// bounded shutdown never gets to run.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), coordinatorShutdownTimeout)
	defer shutdownCancel()

	exitCode := 0

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
		exitCode = 1
	}

	// Cancel the main context so the replication worker stops taking new jobs,
	// then tear the coordinator down under a bound of our own.
	//
	// c.Close() would self-bound at the same 30 s, but the budget has to start
	// here rather than inside: Close's own default begins when Close is entered,
	// which is fine today and stops being fine the moment anything is added
	// between the cancel and the teardown.  Passing an explicit context also makes
	// the deadline visible at the call site instead of being a property of the
	// callee, and is the only way to make the derivation testable — see
	// newShutdownContext for why it must not descend from the cancelled ctx.
	cancel()
	closeCtx, closeCancel := newShutdownContext()
	defer closeCancel()
	if err := c.CloseContext(closeCtx); err != nil {
		// Non-zero exit on a timed-out teardown is deliberate (#69's reasoning
		// applied to #83): the process is terminating either way, but a transfer
		// abandoned mid-flight or a site left unclosed is not a clean shutdown, and
		// an orchestrator or an operator reading $? is entitled to know the
		// difference.
		slog.Error("error closing coordinator; shutdown was not clean",
			"timeout", coordinatorShutdownTimeout, "error", err)
		exitCode = 1
	}

	slog.Info("coordinator stopped")
	if exitCode != 0 {
		os.Exit(exitCode)
	}
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

// setupLogger configures the global slog logger with the given level, writing to
// stderr.
func setupLogger(level string) {
	setupLoggerTo(level, "")
}

// setupLoggerTo configures the global slog logger with the given level, writing
// to path — or to stderr when path is empty.
//
// It returns the opened file so the caller can close it, and nil when logging to
// stderr.  A path that cannot be opened is reported and falls back to stderr
// rather than being fatal: a daemon that refuses to start because of a log
// destination has turned an observability preference into an outage, and the
// operator loses the very channel that would have explained why (#98).
//
// The file is opened append-only.  Truncating would discard the previous run's
// logs at exactly the moment they are most likely to be wanted — after a crash
// and restart.  No rotation is performed; that is logrotate's job, and this
// handler will keep writing to a renamed inode until the process restarts.
func setupLoggerTo(level, path string) *os.File {
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

	var w io.Writer = os.Stderr
	var f *os.File
	var openErr error
	if strings.TrimSpace(path) != "" {
		f, openErr = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if openErr == nil {
			w = f
		}
	}

	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h))

	// Logged after SetDefault so the warning lands on stderr, where a reader who
	// is looking for the missing log file will actually see it.
	if openErr != nil {
		slog.Warn("cannot open global.log_file; logging to stderr",
			"path", path, "error", openErr)
	}
	return f
}
