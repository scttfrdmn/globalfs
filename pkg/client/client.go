// Package client provides a Go client for the GlobalFS coordinator HTTP API.
//
// It is the programmatic counterpart to the globalfs CLI and allows Go
// applications to interact with a running GlobalFS deployment without
// shelling out to the CLI binary.
//
// Basic usage:
//
//	c := client.New("http://coordinator:8090")
//	sites, err := c.ListSites(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, s := range sites {
//	    fmt.Printf("%s (%s) healthy=%v\n", s.Name, s.Role, s.Healthy)
//	}
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ── Wire types ────────────────────────────────────────────────────────────────

// SiteInfo describes a registered GlobalFS site as returned by the coordinator.
type SiteInfo struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

// AddSiteRequest is the payload for registering a new site.
type AddSiteRequest struct {
	Name       string `json:"name"`
	Role       string `json:"role"`
	S3Bucket   string `json:"s3_bucket"`
	S3Region   string `json:"s3_region,omitempty"`
	S3Endpoint string `json:"s3_endpoint,omitempty"`
}

// ReplicateRequest is the payload for triggering manual object replication.
type ReplicateRequest struct {
	Key  string `json:"key"`
	From string `json:"from"`
	To   string `json:"to"`
}

// ReplicateResponse is the coordinator's reply to a replicate request.
type ReplicateResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	From   string `json:"from"`
	To     string `json:"to"`
}

// StatusResponse reports overall coordinator health.
type StatusResponse struct {
	// Healthy is true when the coordinator returned HTTP 200.
	Healthy bool
	// Details contains per-site error lines when unhealthy.
	Details string
}

// ObjectInfo describes a stored object as returned by the coordinator.
type ObjectInfo struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	LastModified time.Time         `json:"last_modified"`
	ETag         string            `json:"etag,omitempty"`
	ContentType  string            `json:"content_type,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	Checksum     string            `json:"checksum,omitempty"`
}

// listObjectsResponse mirrors the coordinator's list envelope.
type listObjectsResponse struct {
	Prefix  string       `json:"prefix"`
	Count   int          `json:"count"`
	Objects []ObjectInfo `json:"objects"`
}

// APIError is returned when the coordinator responds with a non-2xx status.
// Callers can use errors.As to inspect the status code.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("coordinator error (%d): %s", e.StatusCode, e.Message)
}

// apiKeyHeader is the HTTP request header used for authentication.
const apiKeyHeader = "X-GlobalFS-API-Key"

// ── Client ────────────────────────────────────────────────────────────────────

// Client communicates with a GlobalFS coordinator over HTTP.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string // set via WithAPIKey; empty means no auth header is sent
}

// Option is a functional option for New.
type Option func(*Client)

// WithHTTPClient replaces the default *http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithTimeout sets the HTTP client timeout (default 30s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.httpClient.Timeout = d }
}

// WithAPIKey sets the API key sent as X-GlobalFS-API-Key on every request.
// Pass an empty string to send no authentication header.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// setAPIKey adds the X-GlobalFS-API-Key header to req when an API key is set.
func (c *Client) setAPIKey(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set(apiKeyHeader, c.apiKey)
	}
}

// New creates a Client that speaks to the coordinator at coordinatorAddr.
// coordinatorAddr should include scheme and host, e.g. "http://localhost:8090".
func New(coordinatorAddr string, opts ...Option) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(coordinatorAddr, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ── Public methods ────────────────────────────────────────────────────────────

// ListSites returns all registered sites and their health.
// An empty slice (never nil) is returned when no sites are registered.
func (c *Client) ListSites(ctx context.Context) ([]SiteInfo, error) {
	resp, err := c.doGet(ctx, "/api/v1/sites")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var sites []SiteInfo
	if err := json.NewDecoder(resp.Body).Decode(&sites); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if sites == nil {
		sites = []SiteInfo{}
	}
	return sites, nil
}

// AddSite registers a new site with the coordinator. It returns the newly
// created SiteInfo on success.
func (c *Client) AddSite(ctx context.Context, req AddSiteRequest) (SiteInfo, error) {
	resp, err := c.doPost(ctx, "/api/v1/sites", req)
	if err != nil {
		return SiteInfo{}, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusCreated); err != nil {
		return SiteInfo{}, err
	}

	var info SiteInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return SiteInfo{}, fmt.Errorf("decode response: %w", err)
	}
	return info, nil
}

// RemoveSite deregisters the named site from the coordinator.
// It returns nil on success or *APIError if the site is not found.
func (c *Client) RemoveSite(ctx context.Context, name string) error {
	resp, err := c.doDelete(ctx, "/api/v1/sites/"+url.PathEscape(name))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkStatus(resp, http.StatusNoContent)
}

// Replicate triggers manual replication of a single object key from one site
// to another. It returns the coordinator's acknowledgement.
func (c *Client) Replicate(ctx context.Context, req ReplicateRequest) (ReplicateResponse, error) {
	resp, err := c.doPost(ctx, "/api/v1/replicate", req)
	if err != nil {
		return ReplicateResponse{}, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusAccepted); err != nil {
		return ReplicateResponse{}, err
	}

	var result ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ReplicateResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

// Status checks the coordinator's /healthz endpoint. It always returns a
// StatusResponse; additionally it returns a non-nil *APIError when the
// coordinator is degraded (HTTP 503).
func (c *Client) Status(ctx context.Context) (StatusResponse, error) {
	resp, err := c.doGet(ctx, "/healthz")
	if err != nil {
		return StatusResponse{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	text := strings.TrimSpace(string(body))

	if resp.StatusCode == http.StatusOK {
		return StatusResponse{Healthy: true}, nil
	}

	// Coordinator writes "DEGRADED\nsite: reason\n..."
	details := ""
	lines := strings.SplitN(text, "\n", 2)
	if len(lines) > 1 {
		details = strings.TrimSpace(lines[1])
	} else {
		details = text
	}
	sr := StatusResponse{Healthy: false, Details: details}
	return sr, &APIError{StatusCode: resp.StatusCode, Message: details}
}

// ── Object methods ────────────────────────────────────────────────────────────

// GetObject retrieves the full content of the object at key.
func (c *Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.doGet(ctx, "/api/v1/objects/"+key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// PutObject stores data under key. It returns nil on success.
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	resp, err := c.doPut(ctx, "/api/v1/objects/"+key, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkStatus(resp, http.StatusCreated)
}

// HeadObject returns metadata for the object at key without fetching its
// content. The returned ObjectInfo is populated from HTTP response headers.
func (c *Client) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := c.doHead(ctx, "/api/v1/objects/"+key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, &APIError{StatusCode: resp.StatusCode, Message: "head request failed"}
	}

	info := &ObjectInfo{
		Key:         key,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
		Checksum:    resp.Header.Get("X-GlobalFS-Checksum"),
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		info.Size, _ = strconv.ParseInt(cl, 10, 64)
	}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		info.LastModified, _ = http.ParseTime(lm)
	}
	return info, nil
}

// DeleteObject removes the object at key. It returns nil on success.
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	resp, err := c.doDelete(ctx, "/api/v1/objects/"+key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkStatus(resp, http.StatusNoContent)
}

// ListObjects returns up to limit objects whose keys begin with prefix.
// Pass prefix="" to list all objects. Pass limit ≤ 0 to retrieve all matches.
// An empty (never nil) slice is returned when no objects match.
func (c *Client) ListObjects(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error) {
	params := url.Values{}
	if prefix != "" {
		params.Set("prefix", prefix)
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	path := "/api/v1/objects"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	resp, err := c.doGet(ctx, path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var envelope listObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if envelope.Objects == nil {
		envelope.Objects = []ObjectInfo{}
	}
	return envelope.Objects, nil
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func (c *Client) doGet(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %s: %w", path, err)
	}
	c.setAPIKey(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	return resp, nil
}

func (c *Client) doPost(ctx context.Context, path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("build POST %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAPIKey(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	return resp, nil
}

func (c *Client) doPut(ctx context.Context, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build PUT %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	c.setAPIKey(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("PUT %s: %w", path, err)
	}
	return resp, nil
}

func (c *Client) doHead(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build HEAD %s: %w", path, err)
	}
	c.setAPIKey(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HEAD %s: %w", path, err)
	}
	return resp, nil
}

func (c *Client) doDelete(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build DELETE %s: %w", path, err)
	}
	c.setAPIKey(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("DELETE %s: %w", path, err)
	}
	return resp, nil
}

// checkStatus returns an *APIError when resp.StatusCode != wantCode.
func checkStatus(resp *http.Response, wantCode int) error {
	if resp.StatusCode == wantCode {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	// Try to extract {"error":"..."} message.
	var e struct{ Error string }
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return &APIError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	return &APIError{
		StatusCode: resp.StatusCode,
		Message:    strings.TrimSpace(string(body)),
	}
}
