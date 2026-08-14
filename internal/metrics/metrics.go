// Package metrics provides Prometheus instrumentation for the GlobalFS coordinator.
//
// All methods on *Metrics are nil-safe; pass nil when no instrumentation is
// desired (e.g., in unit tests that don't care about metrics output).
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metric descriptors for the coordinator.
type Metrics struct {
	objectOpsTotal        *prometheus.CounterVec
	objectOpsDuration     *prometheus.HistogramVec
	sitesCurrent          prometheus.Gauge
	replicationTotal      *prometheus.CounterVec
	replicationQueueDepth prometheus.Gauge
	replicationDropped    prometheus.Counter
	terminalEventsDropped prometheus.Gauge
	deleteIncomplete      prometheus.Counter
	cacheHits             prometheus.Counter
	cacheMisses           prometheus.Counter
	cacheEvictions        prometheus.Counter
	cacheBytes            prometheus.Gauge
}

// New creates a Metrics instance and registers all descriptors with reg.
// Use prometheus.DefaultRegisterer in production and prometheus.NewRegistry()
// in tests to avoid cross-test pollution.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		objectOpsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "globalfs_object_operations_total",
				Help: "Total number of object operations by operation type and status.",
			},
			[]string{"operation", "status"},
		),
		objectOpsDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "globalfs_object_operation_duration_seconds",
				Help:    "Duration of object operations in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		sitesCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "globalfs_sites_current",
			Help: "Current number of registered sites.",
		}),
		replicationTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "globalfs_replication_jobs_total",
				Help: "Total number of replication jobs by final status.",
			},
			[]string{"status"},
		),
		replicationQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "globalfs_replication_queue_depth",
			Help: "Current number of jobs waiting in the replication queue.",
		}),
		replicationDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "globalfs_replication_dropped_total",
			Help: "Total number of replication jobs rejected because the queue was full. " +
				"Non-zero means writes were not replicated; alert on any increase.",
		}),
		// A Gauge, despite the _total name and the monotonic quantity, because the
		// authoritative count lives in the worker's atomic.Uint64 and this mirrors
		// it with Set.  A Counter would mean Inc-ing here as well, which either
		// double-counts or drifts from the worker's value across a scrape.  Its
		// _total suffix is kept because promql treats it as a counter for
		// increase()/rate() purposes and that is how an operator will query it.
		terminalEventsDropped: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "globalfs_replication_terminal_events_dropped_total",
			Help: "Total terminal replication events the coordinator never received. " +
				"Each one is a job whose outcome was lost: its persisted record is " +
				"replayed on restart and its content hash is missing, so the same " +
				"bytes transfer again. Distinct from replication_dropped_total, " +
				"which counts transfers that never started; alert on any increase.",
		}),
		deleteIncomplete: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "globalfs_delete_incomplete_total",
			Help: "Total number of deletes that left the object present on at least one site. " +
				"Non-zero means objects reported deleted are still readable; alert on any increase.",
		}),
		cacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "globalfs_cache_hits_total",
			Help: "Total number of cache hits.",
		}),
		cacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "globalfs_cache_misses_total",
			Help: "Total number of cache misses.",
		}),
		cacheEvictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "globalfs_cache_evictions_total",
			Help: "Total number of cache entries evicted due to byte-budget pressure.",
		}),
		cacheBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "globalfs_cache_bytes",
			Help: "Current number of bytes stored in the object cache.",
		}),
	}
	reg.MustRegister(
		m.objectOpsTotal,
		m.objectOpsDuration,
		m.sitesCurrent,
		m.replicationTotal,
		m.replicationQueueDepth,
		m.replicationDropped,
		m.terminalEventsDropped,
		m.deleteIncomplete,
		m.cacheHits,
		m.cacheMisses,
		m.cacheEvictions,
		m.cacheBytes,
	)
	return m
}

// RecordOperation records the duration and outcome of an object operation.
// operation should be one of: get, put, delete, head, list.
// status should be "ok" or "error".
func (m *Metrics) RecordOperation(operation, status string, dur time.Duration) {
	if m == nil {
		return
	}
	m.objectOpsTotal.WithLabelValues(operation, status).Inc()
	m.objectOpsDuration.WithLabelValues(operation).Observe(dur.Seconds())
}

// SetSiteCount updates the current number of registered sites gauge.
func (m *Metrics) SetSiteCount(n int) {
	if m == nil {
		return
	}
	m.sitesCurrent.Set(float64(n))
}

// RecordReplication increments the replication jobs counter for a given status.
// status should be "completed" or "failed".
func (m *Metrics) RecordReplication(status string) {
	if m == nil {
		return
	}
	m.replicationTotal.WithLabelValues(status).Inc()
}

// SetReplicationQueueDepth updates the current replication queue depth gauge.
func (m *Metrics) SetReplicationQueueDepth(n int) {
	if m == nil {
		return
	}
	m.replicationQueueDepth.Set(float64(n))
}

// RecordReplicationDropped increments the count of replication jobs that were
// rejected because the queue was full.
//
// This is monotonic and never reset, unlike the queue-depth gauge: a drop is a
// write that was not replicated, and the operator needs to see that it happened
// even if the queue has since emptied.  Any non-zero value is a durability
// event (#79).
func (m *Metrics) RecordReplicationDropped() {
	if m == nil {
		return
	}
	m.replicationDropped.Inc()
}

// SetDroppedTerminalEvents publishes the worker's count of terminal replication
// events that the coordinator never received.
//
// This is a Set of a monotonic value rather than an increment, because the worker
// owns the count (an atomic.Uint64 it increments when the events buffer stays
// full for the whole emit budget) and this only mirrors it.  Callers should pass
// Worker.DroppedTerminalEvents() directly.
//
// It counts a different failure from RecordReplicationDropped, and the two must
// not be folded together: that one means the transfer never started and Put told
// the caller so via ErrReplicationNotQueued.  This one means the transfer ran and
// probably succeeded, Put already returned success, and only the coordinator's
// record of the outcome was lost — so the persisted job is replayed on the next
// restart and the dedup hash was never written.  The two demand opposite
// responses, shed load versus re-drive reconciliation, which is why one series
// could not serve both (#137).
func (m *Metrics) SetDroppedTerminalEvents(n uint64) {
	if m == nil {
		return
	}
	m.terminalEventsDropped.Set(float64(n))
}

// RecordDeleteIncomplete increments the count of deletes that could not be
// completed at every routed site.
//
// It is monotonic for the same reason RecordReplicationDropped is: an incomplete
// delete is an object that is still readable through the same API that just
// reported it gone, and the operator needs to see that it happened even after
// a later retry succeeds.  Any non-zero value is a correctness event, and for a
// deployment under a retention or erasure obligation it is a compliance one
// (#87).
func (m *Metrics) RecordDeleteIncomplete() {
	if m == nil {
		return
	}
	m.deleteIncomplete.Inc()
}

// RecordCacheHit increments the cache hit counter.
func (m *Metrics) RecordCacheHit() {
	if m == nil {
		return
	}
	m.cacheHits.Inc()
}

// RecordCacheMiss increments the cache miss counter.
func (m *Metrics) RecordCacheMiss() {
	if m == nil {
		return
	}
	m.cacheMisses.Inc()
}

// RecordCacheEviction increments the cache eviction counter.
func (m *Metrics) RecordCacheEviction() {
	if m == nil {
		return
	}
	m.cacheEvictions.Inc()
}

// SetCacheBytes updates the current cache size gauge.
func (m *Metrics) SetCacheBytes(n int64) {
	if m == nil {
		return
	}
	m.cacheBytes.Set(float64(n))
}
