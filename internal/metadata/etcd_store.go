package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfig holds the parameters for connecting to an etcd cluster.
type EtcdConfig struct {
	// Endpoints is the list of etcd server URLs (e.g. ["localhost:2379"]).
	Endpoints []string

	// DialTimeout is the maximum time to wait for an initial connection.
	// Defaults to 5 seconds if zero.
	DialTimeout time.Duration

	// Prefix is the key namespace under which all GlobalFS data is stored.
	// Defaults to "/globalfs/" if empty.  A trailing slash is appended if absent.
	Prefix string
}

// EtcdStore is an etcd-backed implementation of [Store].
//
// All keys are namespaced under EtcdConfig.Prefix.  EtcdStore is safe for
// concurrent use.
type EtcdStore struct {
	client *clientv3.Client
	prefix string // always ends with "/"
}

// NewEtcdStore opens a connection to the etcd cluster described by cfg and
// returns a ready-to-use EtcdStore.  A lightweight status check is performed
// against the first endpoint; if it fails the client is closed and an error
// is returned.
func NewEtcdStore(ctx context.Context, cfg EtcdConfig) (*EtcdStore, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "/globalfs/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd: connect to %v: %w", cfg.Endpoints, err)
	}

	// Verify connectivity with a short-lived context.
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if _, err := cli.Status(pingCtx, cfg.Endpoints[0]); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcd: ping %q: %w", cfg.Endpoints[0], err)
	}

	return &EtcdStore{client: cli, prefix: prefix}, nil
}

// Client returns the underlying etcd client.  The returned value must not be
// closed by the caller; use [EtcdStore.Close] to release the connection.
func (e *EtcdStore) Client() *clientv3.Client { return e.client }

// Prefix returns the key namespace used by this store (always ends with "/").
func (e *EtcdStore) Prefix() string { return e.prefix }

// ── Key helpers ────────────────────────────────────────────────────────────────

func (e *EtcdStore) siteKey(name string) string    { return e.prefix + "sites/" + name }
func (e *EtcdStore) sitesPrefix() string           { return e.prefix + "sites/" }
func (e *EtcdStore) jobKey(id string) string       { return e.prefix + "jobs/" + id }
func (e *EtcdStore) jobsPrefix() string            { return e.prefix + "jobs/" }

// ── Site registry ──────────────────────────────────────────────────────────────

// PutSite serialises site as JSON and stores it under the sites/ prefix.
func (e *EtcdStore) PutSite(ctx context.Context, site *SiteRecord) error {
	data, err := json.Marshal(site)
	if err != nil {
		return fmt.Errorf("etcd: marshal site %q: %w", site.Name, err)
	}
	_, err = e.client.Put(ctx, e.siteKey(site.Name), string(data))
	return err
}

// GetSite retrieves the site record by name.
func (e *EtcdStore) GetSite(ctx context.Context, name string) (*SiteRecord, error) {
	resp, err := e.client.Get(ctx, e.siteKey(name))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("etcd: site %q not found", name)
	}
	var s SiteRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &s); err != nil {
		return nil, fmt.Errorf("etcd: unmarshal site %q: %w", name, err)
	}
	return &s, nil
}

// ListSites returns all site records stored under the sites/ prefix.
func (e *EtcdStore) ListSites(ctx context.Context) ([]*SiteRecord, error) {
	resp, err := e.client.Get(ctx, e.sitesPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	sites := make([]*SiteRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var s SiteRecord
		if err := json.Unmarshal(kv.Value, &s); err != nil {
			return nil, fmt.Errorf("etcd: unmarshal site: %w", err)
		}
		sites = append(sites, &s)
	}
	return sites, nil
}

// DeleteSite removes the record for the named site.
func (e *EtcdStore) DeleteSite(ctx context.Context, name string) error {
	_, err := e.client.Delete(ctx, e.siteKey(name))
	return err
}

// ── Replication queue ──────────────────────────────────────────────────────────

// PutReplicationJob serialises job as JSON and stores it under the jobs/ prefix.
func (e *EtcdStore) PutReplicationJob(ctx context.Context, job *ReplicationJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("etcd: marshal job %q: %w", job.ID, err)
	}
	_, err = e.client.Put(ctx, e.jobKey(job.ID), string(data))
	return err
}

// GetPendingJobs returns all job records stored under the jobs/ prefix.
func (e *EtcdStore) GetPendingJobs(ctx context.Context) ([]*ReplicationJob, error) {
	resp, err := e.client.Get(ctx, e.jobsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	jobs := make([]*ReplicationJob, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var j ReplicationJob
		if err := json.Unmarshal(kv.Value, &j); err != nil {
			return nil, fmt.Errorf("etcd: unmarshal job: %w", err)
		}
		jobs = append(jobs, &j)
	}
	return jobs, nil
}

// DeleteJob removes the job record with the given ID.
func (e *EtcdStore) DeleteJob(ctx context.Context, id string) error {
	_, err := e.client.Delete(ctx, e.jobKey(id))
	return err
}

// ── Watch ──────────────────────────────────────────────────────────────────────

// Watch returns a channel of WatchEvents for all keys matching prefix under
// the store's namespace.  The channel is closed when ctx is cancelled.
//
// prefix is relative to the store prefix (e.g. "sites/" or "jobs/").
func (e *EtcdStore) Watch(ctx context.Context, prefix string) (<-chan WatchEvent, error) {
	watchKey := e.prefix + strings.TrimPrefix(prefix, "/")
	wch := e.client.Watch(ctx, watchKey, clientv3.WithPrefix())

	out := make(chan WatchEvent, 64)
	go func() {
		defer close(out)
		for resp := range wch {
			for _, ev := range resp.Events {
				we := WatchEvent{Key: string(ev.Kv.Key)}
				switch ev.Type {
				case clientv3.EventTypePut:
					we.Type = WatchEventPut
					we.Value = ev.Kv.Value
				case clientv3.EventTypeDelete:
					we.Type = WatchEventDelete
				}
				select {
				case out <- we:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// ── Lifecycle ──────────────────────────────────────────────────────────────────

// Close closes the underlying etcd client connection.
func (e *EtcdStore) Close() error {
	return e.client.Close()
}
