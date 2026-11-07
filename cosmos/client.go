package cosmos

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apeirs/cosmosmix/mixnet"
)

// UserAgent is sent upstream instead of whatever the local caller used.
const UserAgent = "Mozilla/5.0"

// Client sends requests to upstream nodes, always via the mixnet Dialer.
// There is no clearnet fallback.
type Client struct {
	D       *mixnet.Dialer
	Timeout time.Duration // per upstream attempt, until the response headers arrive
	Log     func(format string, args ...any)

	once  sync.Once
	http  *http.Client
	Calls atomic.Int64
}

func (c *Client) client() *http.Client {
	c.once.Do(func() {
		// Keep-alives on: one mixnet stream + TLS handshake is expensive, reuse it.
		c.http = &http.Client{
			Transport: &http.Transport{
				Proxy:               nil, // never an environment proxy
				DialContext:         c.D.DialContext,
				TLSHandshakeTimeout: 90 * time.Second,
				TLSClientConfig:     &tls.Config{RootCAs: c.D.RootCAs},
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConnsPerHost: 4,
			},
			// Follow redirects ourselves (over the mixnet) and only to https:
			// a Location header must never reach the caller, who would follow
			// it over the clearnet.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if req.URL.Scheme != "https" {
					return fmt.Errorf("refusing redirect to non-https %s", req.URL.Host)
				}
				if len(via) >= 5 {
					return errors.New("too many redirects")
				}
				return nil
			},
		}
	})
	return c.http
}

func (c *Client) logf(f string, a ...any) {
	if c.Log != nil {
		c.Log(f, a...)
	}
}

// Request is one HTTP request relative to an upstream base URL.
type Request struct {
	Method      string // GET or POST
	Path        string // escaped path below the upstream base, e.g. "/status"
	RawQuery    string // passed through untouched
	Body        []byte
	ContentType string
	Label       string // for logs only; must never contain request params
}

// Response is an upstream answer. The caller closes Body.
type Response struct {
	Status      int
	ContentType string
	Body        io.ReadCloser
	Upstream    string
}

// Join appends an escaped path and query to an upstream base URL.
func Join(base, path, rawQuery string) (*url.URL, error) {
	if err := CheckURL(base); err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u, err := url.Parse(strings.TrimRight(base, "/") + path)
	if err != nil {
		return nil, err
	}
	u.RawQuery = rawQuery
	return u, nil
}

// HostOf returns the host of an upstream URL, for logs.
func HostOf(u string) string {
	if p, err := url.Parse(u); err == nil && p.Host != "" {
		return p.Host
	}
	return u
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelBody) Close() error { err := b.ReadCloser.Close(); b.cancel(); return err }

// retryable: provider-side trouble worth trying the next upstream for.
func retryable(status int) bool { return status >= 500 || status == 429 }

// Do sends req to the first upstream that answers without a server error.
// 2xx-4xx answers are returned as they are (a node's 400/404 is an answer).
// If every upstream answered 5xx/429, the last such answer is returned.
func (c *Client) Do(ctx context.Context, urls []string, req Request) (*Response, error) {
	if len(urls) == 0 {
		return nil, errors.New("no upstream URL configured")
	}
	if req.Method == "" {
		req.Method = "GET"
	}
	var errs []error
	var fallback *Response
	for _, base := range urls {
		u, err := Join(base, req.Path, req.RawQuery)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		host := u.Host
		tmo := c.Timeout
		if tmo == 0 {
			tmo = 50 * time.Second
		}
		actx, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(tmo, cancel)
		var body io.Reader
		if req.Body != nil {
			body = bytes.NewReader(req.Body)
		}
		hr, _ := http.NewRequestWithContext(actx, req.Method, u.String(), body)
		// Nothing about the local caller is forwarded: no Origin, Referer,
		// cookies or wallet User-Agent. Only these headers go upstream.
		hr.Header = http.Header{}
		hr.Header.Set("User-Agent", UserAgent)
		hr.Header.Set("Accept", "application/json")
		if req.Body != nil {
			ct := req.ContentType
			if ct == "" {
				ct = "application/json"
			}
			hr.Header.Set("Content-Type", ct)
		}
		t0 := time.Now()
		resp, err := c.client().Do(hr)
		stopped := timer.Stop()
		if err != nil {
			cancel()
			// *url.Error carries the full URL (addresses, tx hashes in REST
			// paths); keep only the cause so logs and errors never hold params.
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err
			}
			if !stopped {
				err = fmt.Errorf("no answer within %s", tmo)
			}
			c.logf("%-4s %s %s failed after %.2fs: %v", "up", host, req.Label, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", host, err))
			if ctx.Err() != nil || errors.Is(err, mixnet.ErrProxyUnreachable) {
				break // the next upstream would need the same missing proxy
			}
			continue
		}
		if retryable(resp.StatusCode) {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			cancel()
			c.logf("%-4s %s %s answered HTTP %d in %.2fs, trying the next upstream", "up", host, req.Label, resp.StatusCode, time.Since(t0).Seconds())
			errs = append(errs, fmt.Errorf("%s: HTTP %d", host, resp.StatusCode))
			fallback = &Response{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), Body: io.NopCloser(bytes.NewReader(b)), Upstream: base}
			continue
		}
		c.Calls.Add(1)
		c.logf("%-4s %s %s answered HTTP %d in %.2fs", "up", host, req.Label, resp.StatusCode, time.Since(t0).Seconds())
		return &Response{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"),
			Body: cancelBody{resp.Body, cancel}, Upstream: base}, nil
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, errors.Join(errs...)
}

// Get is Do for a GET whose answer is read whole (at most 32 MiB).
func (c *Client) Get(ctx context.Context, urls []string, path, rawQuery, label string) (int, []byte, string, error) {
	resp, err := c.Do(ctx, urls, Request{Method: "GET", Path: path, RawQuery: rawQuery, Label: label})
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	return resp.Status, b, resp.Upstream, err
}

// RPCError is a JSON-RPC error answer from a CometBFT node.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

func (e *RPCError) Error() string {
	s := fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
	if e.Data != "" {
		s += ": " + e.Data
	}
	return s
}

// RPC calls a CometBFT JSON-RPC method with POST and returns its result.
func (c *Client) RPC(ctx context.Context, urls []string, method string, params any) (json.RawMessage, string, error) {
	if params == nil {
		params = map[string]any{}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	resp, err := c.Do(ctx, urls, Request{Method: "POST", Path: "/", Body: body, Label: method})
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, resp.Upstream, err
	}
	var r struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, resp.Upstream, fmt.Errorf("HTTP %d, not JSON-RPC: %.120q", resp.Status, b)
	}
	if r.Error != nil {
		return nil, resp.Upstream, r.Error
	}
	if resp.Status != 200 {
		return nil, resp.Upstream, fmt.Errorf("HTTP %d: %.120q", resp.Status, b)
	}
	return r.Result, resp.Upstream, nil
}

// REST GETs a REST/LCD path and decodes the JSON answer into v.
func (c *Client) REST(ctx context.Context, urls []string, path, rawQuery string, v any) (string, error) {
	status, b, used, err := c.Get(ctx, urls, path, rawQuery, "rest "+RESTLabel(path))
	if err != nil {
		return used, err
	}
	if status != 200 {
		var e struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(b, &e) == nil && e.Message != "" {
			return used, fmt.Errorf("HTTP %d from %s: %s", status, HostOf(used), e.Message)
		}
		return used, fmt.Errorf("HTTP %d from %s: %.120q", status, HostOf(used), b)
	}
	return used, json.Unmarshal(b, v)
}

// RESTLabel turns a REST path into a log-safe route template: segments that
// could be an address, hash, height or other parameter become "*".
func RESTLabel(path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i, s := range segs {
		if !safeSegment(s) {
			segs[i] = "*"
		}
	}
	return "/" + strings.Join(segs, "/")
}

func safeSegment(s string) bool {
	if s == "" || len(s) > 24 {
		return false
	}
	// route words: letters and '_' only, or an API version like v1, v1beta1, v2alpha1
	if strings.Trim(s, "abcdefghijklmnopqrstuvwxyz_") == "" {
		return true
	}
	if s[0] == 'v' {
		rest := strings.TrimLeft(s[1:], "0123456789")
		for _, p := range []string{"beta", "alpha"} {
			if strings.HasPrefix(rest, p) {
				rest = strings.TrimLeft(rest[len(p):], "0123456789")
			}
		}
		return rest == "" && len(s) > 1 && s[1] >= '0' && s[1] <= '9'
	}
	return false
}

// RPCLabel names the CometBFT method(s) of a request, for logs (never params).
func RPCLabel(method, path string, body []byte) string {
	if method == "GET" {
		seg := strings.Trim(path, "/")
		if seg == "" {
			return "GET /"
		}
		if safeSegment(seg) {
			return seg
		}
		return "GET *"
	}
	var one struct{ Method string }
	if json.Unmarshal(body, &one) == nil && one.Method != "" {
		return clip(one.Method)
	}
	var batch []struct{ Method string }
	if json.Unmarshal(body, &batch) == nil {
		var ms []string
		for _, b := range batch {
			ms = append(ms, clip(b.Method))
		}
		return "[" + strings.Join(ms, ",") + "]"
	}
	return "?"
}

func clip(m string) string {
	if safeSegment(m) {
		return m
	}
	return "*"
}

// ---------------------------------------------------------------- websocket

// WSConn is an upgraded websocket stream to an upstream node.
type WSConn struct {
	net.Conn
	R        *bufio.Reader // may hold bytes already read past the handshake
	Accept   string        // Sec-WebSocket-Accept from the node
	Protocol string
	Upstream string
}

// Websocket opens a websocket (CometBFT /websocket) to the first upstream
// that accepts the upgrade, over the mixnet with TLS end to end. key is the
// caller's Sec-WebSocket-Key, so the caller can check the accept value itself.
func (c *Client) Websocket(ctx context.Context, urls []string, path, key, protocol string) (*WSConn, error) {
	var errs []error
	for _, base := range urls {
		u, err := Join(base, path, "")
		if err != nil {
			errs = append(errs, err)
			continue
		}
		port := u.Port()
		if port == "" {
			port = "443"
		}
		t0 := time.Now()
		tmo := c.Timeout
		if tmo == 0 {
			tmo = 50 * time.Second
		}
		actx, cancel := context.WithTimeout(ctx, tmo)
		ws, err := c.wsOnce(actx, u, port, key, protocol)
		cancel()
		if err != nil {
			c.logf("%-4s %s websocket failed after %.2fs: %v", "up", u.Host, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", u.Host, err))
			if ctx.Err() != nil || errors.Is(err, mixnet.ErrProxyUnreachable) {
				break
			}
			continue
		}
		ws.Upstream = base
		c.logf("%-4s %s websocket open in %.2fs", "up", u.Host, time.Since(t0).Seconds())
		return ws, nil
	}
	if len(errs) == 0 {
		return nil, errors.New("no upstream URL configured")
	}
	return nil, errors.Join(errs...)
}

func (c *Client) wsOnce(ctx context.Context, u *url.URL, port, key, protocol string) (*WSConn, error) {
	tc, err := c.D.DialTLS(ctx, net.JoinHostPort(u.Hostname(), port), &tls.Config{
		ServerName: u.Hostname(), RootCAs: c.D.RootCAs, NextProtos: []string{"http/1.1"}})
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = tc.SetDeadline(dl)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n", u.RequestURI(), u.Host)
	fmt.Fprintf(&b, "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nUser-Agent: %s\r\n", key, UserAgent)
	if protocol != "" {
		fmt.Fprintf(&b, "Sec-WebSocket-Protocol: %s\r\n", protocol)
	}
	b.WriteString("\r\n")
	if _, err := io.WriteString(tc, b.String()); err != nil {
		tc.Close()
		return nil, err
	}
	br := bufio.NewReader(tc)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		tc.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		resp.Body.Close()
		tc.Close()
		return nil, fmt.Errorf("upgrade refused: HTTP %d", resp.StatusCode)
	}
	_ = tc.SetDeadline(time.Time{})
	return &WSConn{Conn: tc, R: br, Accept: resp.Header.Get("Sec-WebSocket-Accept"), Protocol: resp.Header.Get("Sec-WebSocket-Protocol")}, nil
}
