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
	"errors"
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
	Name         string `json:"name"`
	Role         string `json:"role"`
	Healthy      bool   `json:"healthy"`
	Error        string `json:"error,omitempty"`
	CircuitState string `json:"circuit_state,omitempty"`
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

// ErrUnexpectedRedirect reports that the coordinator answered with a redirect,
// which this client refuses to follow.
//
// The GlobalFS API has no legitimate redirects, so one is either a
// misconfiguration — a reverse proxy in front of the coordinator rewriting or
// canonicalising the path — or an attempt to steer a request somewhere else.
// Both are better reported to the caller than transparently obeyed.
//
// Following one is not neutral.  Go's http.Client replays the method *and* the
// X-GlobalFS-API-Key header on a 307, so a redirect turns into a second,
// differently-targeted, still-authenticated request.  That is the mechanism that
// made #73 exploitable: against a server that path-cleaned ".." into a 307,
// DeleteObject(ctx, "../sites/primary") followed the redirect and deregistered a
// site, returning nil.  #73 closed that on the server, so this is hardening for
// a pre-#73 or misconfigured coordinator rather than a live hole against a
// current one — but the credential is carried by the client, and the client is
// where the decision to hand it to a new target is made.
//
// The error names the redirect target, since a proxy misconfiguration is the
// likely cause and the Location is the thing that identifies it.
var ErrUnexpectedRedirect = errors.New("coordinator returned an unexpected redirect")

// ErrReplicationPending reports that a PutObject write was committed but
// replication to one or more secondaries could not be queued.
//
// The write succeeded.  The bytes are durable on every primary and the object is
// readable immediately; only the asynchronous copies to the named secondaries
// were not scheduled, and the coordinator answered 202 rather than 201 to say so
// (#130).  A caller must not retry the write on this error: the retry would be a
// second full upload of data that is already stored.
//
// It is returned as an error rather than swallowed into a nil precisely because
// the write is not wholly complete.  A nil would make the client report full
// durability the coordinator did not claim — the "success while something is
// wrong" shape that #130 and its neighbours are about, just inverted.  So the
// contract is: non-nil, but committed.  Test for it and continue:
//
//	err := c.PutObject(ctx, key, data)
//	switch {
//	case errors.Is(err, client.ErrReplicationPending):
//	    // stored; log it, do not retry
//	case err != nil:
//	    return err
//	}
//
// A sentinel is used rather than a distinct exported type, or a changed
// PutObject signature, for three reasons.  It matches how the coordinator
// already communicates this condition internally (ErrReplicationNotQueued), so
// the same shape survives the whole round trip.  errors.Is answers the only
// question a caller has to ask, with no string matching and no type assertion,
// and it stays correct if the error is later wrapped further up.  And it does not
// break the signature, so every existing caller that treats a non-nil error as
// "did not fully succeed" remains *safe* by default — conservative, never
// silently wrong — while a caller who cares can opt into the distinction.
//
// The wrapped message carries the coordinator's detail string, which names the
// destinations that got no job.  As on the server side, those names are not
// broken out into fields: obtaining them would mean parsing another package's
// formatted output, which is a silent breakage waiting for that format to
// change.
var ErrReplicationPending = errors.New("object stored but replication was not queued")

// ErrInvalidKey reports that an object key was rejected locally, before any
// request was sent.
//
// The rule matches the coordinator's own validateObjectKey — no ".." path
// components and no null bytes — deliberately: a client should not send a
// request it already knows the server must refuse, and a local failure is
// immediate and unambiguous where a 400 from a coordinator the caller may not
// control is neither.  Against a server that does *not* refuse it, this is the
// check that stops the request being made at all.
var ErrInvalidKey = errors.New("invalid object key")

// ErrResponseTooLarge reports that the coordinator's response body exceeded the
// client's size limit and was therefore not returned (#111).
//
// It is an error rather than a truncation, deliberately and for the same reason
// as #74: a short body handed back with a nil error is indistinguishable from a
// complete one, and this client's contract is that a nil error means whole data.
// A caller who legitimately needs a bigger object raises the cap with
// [WithMaxResponseSize]; a caller who has been pointed at the wrong endpoint
// finds out instead of allocating whatever it sends.
//
// Error bodies are the exception: those are truncated silently, because the
// status code is the outcome and the body is only a message.
var ErrResponseTooLarge = errors.New("coordinator response exceeds the client's size limit")

// Response body limits.
//
// The endpoint this client talks to is a user-supplied flag, so every body it
// reads is attacker-controlled in the threat model that matters — a
// misconfiguration pointing at something that is not a coordinator, a proxy
// returning a large error page, or a hostile host. The server side has bounded
// request bodies since v0.1.4 (http.MaxBytesReader, 413); this is the missing
// half.
//
// Two fixed caps and one configurable one, split by what determines the size:
//
//   - maxErrorBodyBytes — an error message. Nothing legitimate is longer, and
//     exceeding it truncates silently rather than erroring, because losing the
//     status code to a body-read failure is strictly worse than a clipped
//     message.
//   - maxControlBodyBytes — responses whose size is set by the API's shape
//     rather than by stored data: the site list, an added site, a replicate
//     acknowledgement, the 202 detail, /healthz. A megabyte is thousands of
//     sites.
//   - WithMaxResponseSize — the two responses whose size is set by how much
//     data is stored: an object body and a list envelope. Only these need to be
//     the caller's decision.
const (
	maxErrorBodyBytes   = 4 << 10
	maxControlBodyBytes = 1 << 20

	// defaultMaxResponseSize is generous rather than tight: the point is to have
	// a finite ceiling at all, not to second-guess object sizes. It is four
	// times the coordinator's own 256 MiB PUT cap, so no object this client
	// could have stored through the API is refused on the way back.
	defaultMaxResponseSize = 1 << 30
)

// readAtMost reads up to limit bytes, reporting separately whether the reader
// had more to give.
//
// It reads limit+1 bytes to distinguish "exactly limit bytes, complete" from
// "limit bytes and counting", which io.LimitReader alone cannot: a LimitReader
// that hits its bound returns io.EOF, identical to a body that simply ended.
// That difference is the whole point — one is a valid response and the other
// must not be returned as one.
func readAtMost(r io.Reader, limit int64) (data []byte, exceeded bool, err error) {
	data, err = io.ReadAll(io.LimitReader(r, limit+1))
	if int64(len(data)) > limit {
		return data[:limit], true, err
	}
	return data, false, err
}

// decodeControlJSON decodes a control-plane response body into v under
// maxControlBodyBytes.
//
// Reading into a buffer first, rather than handing the body to a
// json.Decoder wrapped in an io.LimitReader, is what lets an over-long body be
// named as such: a truncated stream reaches the decoder as a syntax error, and
// "unexpected end of JSON input" is a misleading thing to report when the real
// cause is that the response did not stop (#111).
func decodeControlJSON(r io.Reader, what string, v any) error {
	data, exceeded, err := readAtMost(r, maxControlBodyBytes)
	if exceeded {
		return fmt.Errorf("%s: %w (limit %d bytes)", what, ErrResponseTooLarge, int64(maxControlBodyBytes))
	}
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// validateKey rejects object keys that could escape their intended path.
//
// Same rule, and same reasoning, as cmd/coordinator's validateObjectKey: a ".."
// component can be path-cleaned by an intermediary or a server's mux into a
// different route, and a null byte truncates the key for anything downstream
// written in C.  Both are checked on the literal key the caller passed, which is
// the only spelling this client has — it does not percent-encode the key when
// building the URL, so nothing here can hide behind an escape.
func validateKey(key string) error {
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("%w %q: contains null byte", ErrInvalidKey, key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == ".." {
			return fmt.Errorf("%w %q: contains path traversal component", ErrInvalidKey, key)
		}
	}
	return nil
}

// refuseRedirects is the http.Client CheckRedirect policy: never follow, and
// report where the redirect pointed.
//
// Returning an error rather than http.ErrUseLastResponse is the choice that makes
// the condition impossible to miss.  ErrUseLastResponse would surface the 307 as
// an ordinary *APIError, which every call site would then have to recognise as a
// redirect on its own; an error here means no follow-up request is ever built —
// CheckRedirect runs before it is sent — and so the API key is never put on the
// wire a second time.
//
// via[0] is the original request, which is the useful thing to name alongside the
// target: "this is where you asked to go, this is where you were sent".
func refuseRedirects(req *http.Request, via []*http.Request) error {
	from := ""
	if len(via) > 0 && via[0].URL != nil {
		from = via[0].URL.String()
	}
	return fmt.Errorf("%w: %s -> %s (not followed; the API key is not replayed)",
		ErrUnexpectedRedirect, from, req.URL)
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client communicates with a GlobalFS coordinator over HTTP.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string // set via WithAPIKey; empty means no auth header is sent

	// maxResponseSize caps object and list bodies; see WithMaxResponseSize.
	maxResponseSize int64

	// timeout holds WithTimeout's value until New resolves it, so that the
	// timeout does not depend on option order (#111).  nil means "not set by an
	// option" — distinct from a zero Duration, which http.Client reads as no
	// timeout at all and which a caller may legitimately want.
	timeout *time.Duration
}

// Option is a functional option for New.
type Option func(*Client)

// WithHTTPClient replaces the default *http.Client.
//
// The supplied client is not used directly: New takes a shallow copy and, if the
// copy has no CheckRedirect of its own, installs the no-redirect policy on it.
// The copy shares the Transport, so connection pooling and any custom
// RoundTripper are preserved, and the caller's client is left unmodified.  A
// caller that sets CheckRedirect keeps it — including one that deliberately
// follows redirects, which is then their decision and not a default.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithTimeout sets the HTTP client timeout (default 30s).
//
// It applies whatever order the options are given in, including alongside
// [WithHTTPClient]: the value is recorded here and installed by New once every
// option has run (#111).  It previously assigned straight into c.httpClient.Timeout,
// so a WithHTTPClient appearing after it replaced the whole client and discarded
// the timeout — silently, and in the direction that hangs.
//
// A zero duration means no timeout, matching http.Client, and is distinguishable
// from not passing the option at all.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = &d }
}

// WithMaxResponseSize caps the bytes this client will read from an object body or
// a list response (default 1 GiB).  A response that exceeds the cap is reported
// as an error wrapping [ErrResponseTooLarge] rather than truncated (#111).
//
// Pass a value ≤ 0 to lift the cap, which restores the pre-v0.3.0 behaviour of
// allocating whatever the server sends.  That is a real choice for a trusted
// coordinator moving very large objects, and a bad one for a CLI pointed at an
// endpoint from a flag, which is why it is not the default.
//
// The limit does not apply to error bodies or to the small control-plane
// responses; those carry their own fixed caps, and an over-long error message is
// truncated rather than turned into a failure.
func WithMaxResponseSize(n int64) Option {
	return func(c *Client) { c.maxResponseSize = n }
}

// objectBodyLimit returns the effective cap for object and list bodies, and
// whether one applies at all.
func (c *Client) objectBodyLimit() (int64, bool) {
	if c.maxResponseSize <= 0 {
		return 0, false
	}
	return c.maxResponseSize, true
}

// readBounded reads a response body under the object/list cap, mapping an
// over-long body to ErrResponseTooLarge.  what names the thing being read so the
// error says which call was refused.
func (c *Client) readBounded(r io.Reader, what string) ([]byte, error) {
	limit, bounded := c.objectBodyLimit()
	if !bounded {
		return io.ReadAll(r)
	}
	data, exceeded, err := readAtMost(r, limit)
	if exceeded {
		return nil, fmt.Errorf("%s: %w (limit %d bytes; raise it with WithMaxResponseSize)",
			what, ErrResponseTooLarge, limit)
	}
	return data, err
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
//
// The returned Client does not follow HTTP redirects: a 3xx with a Location is
// reported as an error wrapping [ErrUnexpectedRedirect] rather than obeyed, so
// the API key is never replayed to a target the coordinator (or an intermediary)
// chose.  See ErrUnexpectedRedirect for why that matters (#132).
func New(coordinatorAddr string, opts ...Option) *Client {
	c := &Client{
		baseURL:         strings.TrimRight(coordinatorAddr, "/"),
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		maxResponseSize: defaultMaxResponseSize,
	}
	for _, o := range opts {
		o(c)
	}
	// Everything below runs after the options, so that no option's effect depends
	// on where in the list it appeared (#111).
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	// A copy is taken whenever this constructor needs to change anything about the
	// client, so the caller's own *http.Client is never mutated: it may be shared
	// with unrelated code that wants different redirect or timeout behaviour.
	// Copying shares the Transport, so pooling and any custom RoundTripper survive.
	if c.timeout != nil || c.httpClient.CheckRedirect == nil {
		hc := *c.httpClient
		if c.timeout != nil {
			hc.Timeout = *c.timeout
		}
		// The no-redirect policy covers a WithHTTPClient-supplied client too —
		// that is the path a caller most plausibly uses to install a custom
		// Transport, and it would otherwise silently opt out.  A caller who set
		// CheckRedirect themselves keeps it, including one that deliberately
		// follows redirects.
		if hc.CheckRedirect == nil {
			hc.CheckRedirect = refuseRedirects
		}
		c.httpClient = &hc
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
	if err := decodeControlJSON(resp.Body, "read site list", &sites); err != nil {
		return nil, err
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
	if err := decodeControlJSON(resp.Body, "read added site", &info); err != nil {
		return SiteInfo{}, err
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
	if err := decodeControlJSON(resp.Body, "read replicate acknowledgement", &result); err != nil {
		return ReplicateResponse{}, err
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

	// A read error here is deliberately ignored: the status code is what
	// determines health, and the body only supplies human-readable detail, so a
	// truncated read degrades the message rather than the answer.  Bounded at
	// maxControlBodyBytes — /healthz emits one line per unhealthy site (#111).
	body, _, _ := readAtMost(resp.Body, maxControlBodyBytes)
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
//
// GetObject never returns partial content with a nil error: a read that fails
// mid-body, a body shorter than the advertised Content-Length, or a body longer
// than the client's size cap is reported as an error and the partial bytes are
// discarded (#74, #111).  Callers can therefore treat a nil error as meaning the
// returned slice is the whole object.  The cap defaults to 1 GiB and is set by
// [WithMaxResponseSize]; exceeding it wraps [ErrResponseTooLarge].
//
// A key containing a ".." component or a null byte is rejected locally, without
// any request being sent; the returned error wraps [ErrInvalidKey] (#132).
func (c *Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	resp, err := c.doGet(ctx, "/api/v1/objects/"+key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}

	// Bounded by WithMaxResponseSize, and an over-long body is an error rather
	// than a truncation — the same rule as the short-body check below, from the
	// same principle: this method's contract is that a nil error means the whole
	// object (#74, #111).
	data, err := c.readBounded(resp.Body, fmt.Sprintf("read object %q body", key))
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, err
		}
		// Discard the partial bytes: returning them alongside an error invites
		// callers to use whichever value they check first.
		return nil, fmt.Errorf("read object %q body after %d bytes: %w", key, len(data), err)
	}
	// Cross-check against the advertised length: an independent assertion that
	// does not rely on the transport reporting the truncation.  net/http does
	// report it (a short Content-Length delimited body surfaces as
	// io.ErrUnexpectedEOF), but WithHTTPClient lets callers substitute any
	// RoundTripper, and this check holds regardless of which one is in play.
	//
	// resp.ContentLength is -1 when the length is unknown — a chunked response
	// or an HTTP/1.0 close-delimited body — and the check is then skipped
	// because there is nothing to compare against.  The coordinator always sets
	// Content-Length on GET /api/v1/objects, so the check is live in practice.
	if resp.ContentLength >= 0 && int64(len(data)) != resp.ContentLength {
		return nil, fmt.Errorf("short object body for %q: got %d bytes, want %d",
			key, len(data), resp.ContentLength)
	}
	return data, nil
}

// PutObject stores data under key. It returns nil when the object is stored and
// fully replicated (the coordinator's 201).
//
// It returns an error wrapping [ErrReplicationPending] when the object is stored
// but replication could not be queued (the coordinator's 202).  That case is a
// committed write and must not be retried; see [ErrReplicationPending].  Any
// other non-2xx is an *[APIError] and the write did not happen.
//
// An invalid key is rejected locally; see GetObject and [ErrInvalidKey].
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := c.doPut(ctx, "/api/v1/objects/"+key, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Branch here rather than teaching checkStatus a set of acceptable codes.
	// checkStatus's job is "this code is wrong, here is why", and the two 2xx
	// codes do not mean the same thing to the caller — a multi-status form of
	// checkStatus would collapse them back into one and lose the distinction this
	// method exists to report.  ListObjects treats 207 the same way, for the same
	// reason (see its comment); this keeps the two consistent.
	if resp.StatusCode == http.StatusAccepted {
		return fmt.Errorf("put object %q: %w: %s", key, ErrReplicationPending, putPartialDetail(resp))
	}
	return checkStatus(resp, http.StatusCreated)
}

// objectPutPartialResponse mirrors the coordinator's 202 body for a stored write
// whose replication was not queued.  It is deliberately not an error envelope on
// the wire, so it cannot be read out by checkStatus.
type objectPutPartialResponse struct {
	Key          string   `json:"key"`
	Status       string   `json:"status"`
	PendingSites []string `json:"pending_sites,omitempty"`
	RequestID    string   `json:"request_id,omitempty"`
	// Detail is what coordinators before v0.3.0 sent in place of PendingSites: the
	// server-side error verbatim.  Still read, so this client keeps working against
	// one; new coordinators do not send it (#110).
	Detail string `json:"detail,omitempty"`
}

// putPartialDetail extracts the coordinator's explanation from a 202 body,
// falling back to a fixed string when the body is absent or unparseable.
//
// The fallback matters: the caller's decision is driven by ErrReplicationPending,
// which is already established by the status code, so a body that cannot be read
// must not be allowed to turn a committed write into a different outcome.  An
// older coordinator, or a proxy that strips the body, still yields the correct
// sentinel with a vaguer message.
//
// PendingSites is preferred over Detail because it is the structured form of the
// same information: the sites are read from a field rather than recovered from a
// formatted error string (#110).  Detail remains the fallback for a pre-v0.3.0
// coordinator, so an older server still produces a message naming the sites.
func putPartialDetail(resp *http.Response) string {
	var partial objectPutPartialResponse
	// Bounded like any other control-plane body, and the error is discarded for
	// the reason in the doc comment: the outcome is already fixed by the status
	// code, so an unreadable or over-long body may only cost detail (#111).
	if err := decodeControlJSON(resp.Body, "read partial-put detail", &partial); err == nil {
		switch {
		case len(partial.PendingSites) > 0:
			return fmt.Sprintf("replication not queued for %v", partial.PendingSites)
		case partial.Detail != "":
			return partial.Detail
		case partial.Status != "":
			return partial.Status
		}
	}
	return "coordinator reported the write as stored but not fully replicated"
}

// HeadObject returns metadata for the object at key without fetching its
// content. The returned ObjectInfo is populated from HTTP response headers.
//
// An invalid key is rejected locally; see GetObject and [ErrInvalidKey].
func (c *Client) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
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
//
// An invalid key is rejected locally, without a request being sent — this is the
// method the #73 exploit used, as DeleteObject(ctx, "../sites/primary"), and the
// destructive one of the four.  See [ErrInvalidKey].
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
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
//
// Results are in lexicographic key order across every site, and limit truncates
// that order — so limit=n returns the namespace's first n keys, and the last key
// of a truncated result is a valid place to resume from (#109).  Checksum is
// always empty from this call; only [Client.HeadObject] can supply it.
//
// The response body is capped; see [WithMaxResponseSize].
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

	// The coordinator returns 200 on full success and 207 Multi-Status when
	// results are partial (some sites unavailable).  Both carry a valid JSON
	// body; treat both as success so callers receive whatever data is available.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusMultiStatus {
		if err := checkStatus(resp, http.StatusOK); err != nil {
			return nil, err
		}
	}

	// The list envelope is bounded by WithMaxResponseSize rather than the fixed
	// control-plane cap: its size is set by how many objects are stored, which is
	// the caller's own limit parameter times the size of an entry, and is
	// therefore the caller's business to size (#111).
	body, err := c.readBounded(resp.Body, fmt.Sprintf("read object list for prefix %q", prefix))
	if err != nil {
		return nil, err
	}
	var envelope listObjectsResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
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
	// A read error is ignored on purpose: an *APIError carrying the status code and
	// a partial message is strictly better than losing the status code to a
	// body-read failure.
	//
	// Bounded at maxErrorBodyBytes and truncated silently (#111).  This was the
	// sharpest of the unbounded reads — it buffered an arbitrarily large *error*
	// body purely to build a message — and it is also the one place where
	// truncating beats erroring, since the status code is the outcome and an
	// error path must not acquire a new way to fail.
	body, _, _ := readAtMost(resp.Body, maxErrorBodyBytes)
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
