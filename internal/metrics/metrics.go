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
	}
	reg.MustRegister(
		m.objectOpsTotal,
		m.objectOpsDuration,
		m.sitesCurrent,
		m.replicationTotal,
		m.replicationQueueDepth,
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
