package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scttfrdmn/globalfs/internal/metrics"
)

// gather is a helper that collects all metric families from a registry
// and returns a map from metric name to the family.
func gather(t *testing.T, reg *prometheus.Registry) map[string]interface{} {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	m := make(map[string]interface{}, len(families))
	for _, f := range families {
		m[f.GetName()] = f
	}
	return m
}

func TestNew_RegistersMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	if m == nil {
		t.Fatal("New returned nil")
	}
	// Set gauge values so they appear in Gather output.
	m.SetSiteCount(1)
	m.SetReplicationQueueDepth(0)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{
		"globalfs_sites_current",
		"globalfs_replication_queue_depth",
		"globalfs_replication_terminal_events_dropped_total",
	} {
		if !names[want] {
			t.Errorf("metric %q not found in registry after New", want)
		}
	}
}

func TestNilSafe(t *testing.T) {
	t.Parallel()
	var m *metrics.Metrics
	// None of these should panic.
	m.RecordOperation("get", "ok", time.Millisecond)
	m.SetSiteCount(3)
	m.RecordReplication("completed")
	m.SetReplicationQueueDepth(5)
	m.SetDroppedTerminalEvents(7)
}

// TestSetDroppedTerminalEvents_MirrorsRatherThanAccumulates pins the property
// that makes a Gauge the right type for a monotonic quantity here (#137): the
// worker owns the count and this only reflects it, so publishing the same value
// repeatedly — which the health poll does every interval — must be a no-op.
//
// An Inc-based Counter would have turned each poll into another increment, and
// the metric would climb without a single new drop.
func TestSetDroppedTerminalEvents_MirrorsRatherThanAccumulates(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	const name = "globalfs_replication_terminal_events_dropped_total"
	read := func() float64 {
		t.Helper()
		families, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, f := range families {
			if f.GetName() == name {
				return f.Metric[0].Gauge.GetValue()
			}
		}
		t.Fatalf("%s not found in registry", name)
		return 0
	}

	// Present from New at zero, without any drop having occurred: an absent
	// series is indistinguishable from a scrape failure, so "no drops yet" has
	// to be expressible.
	if got := read(); got != 0 {
		t.Errorf("%s = %v before any publish, want 0", name, got)
	}

	m.SetDroppedTerminalEvents(3)
	if got := read(); got != 3 {
		t.Errorf("%s = %v, want 3", name, got)
	}

	// Three more polls with the worker's count unchanged.
	for i := 0; i < 3; i++ {
		m.SetDroppedTerminalEvents(3)
	}
	if got := read(); got != 3 {
		t.Errorf("%s = %v after re-publishing the same count, want 3; the value "+
			"must mirror the worker rather than accumulate per poll", name, got)
	}

	m.SetDroppedTerminalEvents(5)
	if got := read(); got != 5 {
		t.Errorf("%s = %v, want 5", name, got)
	}
}

func TestSetSiteCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.SetSiteCount(4)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "globalfs_sites_current" {
			if got := f.Metric[0].Gauge.GetValue(); got != 4 {
				t.Errorf("globalfs_sites_current: got %v, want 4", got)
			}
			return
		}
	}
	t.Error("globalfs_sites_current not found after SetSiteCount")
}

func TestSetReplicationQueueDepth(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.SetReplicationQueueDepth(42)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "globalfs_replication_queue_depth" {
			if got := f.Metric[0].Gauge.GetValue(); got != 42 {
				t.Errorf("globalfs_replication_queue_depth: got %v, want 42", got)
			}
			return
		}
	}
	t.Error("globalfs_replication_queue_depth not found after SetReplicationQueueDepth")
}

func TestRecordOperation_IncrementsCounter(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.RecordOperation("get", "ok", 10*time.Millisecond)
	m.RecordOperation("get", "ok", 20*time.Millisecond)
	m.RecordOperation("put", "error", 5*time.Millisecond)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "globalfs_object_operations_total" {
			// {get,ok}=2 and {put,error}=1 → two distinct label combinations.
			if got := len(f.Metric); got != 2 {
				t.Errorf("expected 2 label combinations, got %d", got)
			}
			// Find the {get,ok} series and verify count=2.
			for _, metric := range f.Metric {
				var op, status string
				for _, lp := range metric.Label {
					switch lp.GetName() {
					case "operation":
						op = lp.GetValue()
					case "status":
						status = lp.GetValue()
					}
				}
				if op == "get" && status == "ok" {
					if got := metric.Counter.GetValue(); got != 2 {
						t.Errorf("get/ok counter: got %v, want 2", got)
					}
				}
			}
			return
		}
	}
	t.Error("globalfs_object_operations_total not found")
}

func TestRecordOperation_RecordsDuration(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.RecordOperation("delete", "ok", 50*time.Millisecond)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "globalfs_object_operation_duration_seconds" {
			if len(f.Metric) == 0 {
				t.Error("expected at least one histogram series")
			}
			// SampleCount should be 1.
			if got := f.Metric[0].Histogram.GetSampleCount(); got != 1 {
				t.Errorf("histogram sample count: got %d, want 1", got)
			}
			return
		}
	}
	t.Error("globalfs_object_operation_duration_seconds not found")
}

func TestRecordReplication(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.RecordReplication("completed")
	m.RecordReplication("completed")
	m.RecordReplication("failed")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "globalfs_replication_jobs_total" {
			// Two label combos: {completed} and {failed}.
			if got := len(f.Metric); got != 2 {
				t.Errorf("expected 2 label combinations, got %d", got)
			}
			for _, metric := range f.Metric {
				for _, lp := range metric.Label {
					if lp.GetName() == "status" && lp.GetValue() == "completed" {
						if got := metric.Counter.GetValue(); got != 2 {
							t.Errorf("completed counter: got %v, want 2", got)
						}
					}
				}
			}
			return
		}
	}
	t.Error("globalfs_replication_jobs_total not found")
}
