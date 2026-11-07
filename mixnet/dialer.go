package mixnet

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// DNS modes.
const (
	// DNSDoH resolves names with DNS-over-HTTPS sent *through the mixnet*
	// (Cloudflare 1.1.1.1, then Google 8.8.8.8, both reached by IP literal),
	// then asks the exit for an IPv4 address. Some exit gateways have broken
	// IPv6 routes; when the exit resolves a dual-stack name itself it can pick
	// an unreachable AAAA record and stall (seen live with mixpool, 2026-10-10).
	DNSDoH = "doh"
	// DNSExit hands the hostname to the exit gateway's network requester.
	DNSExit = "exit"
)

type dohResolver struct {
	ip, path string
}

var dohResolvers = []dohResolver{
	{"1.1.1.1", "/dns-query?type=A&name="},
	{"8.8.8.8", "/resolve?type=A&name="},
}

// Tracer receives one human-readable line per network step.
type Tracer func(format string, args ...any)

// Dialer opens TCP streams through the Nym mixnet (SOCKS5 to nym-socks5-client).
// The local resolver is never used: names are resolved over the mixnet.
type Dialer struct {
	Proxy   string        // host:port of nym-socks5-client
	DNS     string        // DNSDoH (default) or DNSExit
	Timeout time.Duration // per-stream SOCKS handshake timeout (mixnet round trips are slow)
	Trace   Tracer
	// RootCAs for the DoH resolvers (nil = system roots). Tests override it.
	RootCAs *x509.CertPool
	// Resolvers overrides the DoH resolver list (tests).
	Resolvers []string

	mu    sync.Mutex
	cache map[string]dnsEntry
}

type dnsEntry struct {
	until time.Time
	addrs []string
}

func (d *Dialer) trace(format string, args ...any) {
	if d.Trace != nil {
		d.Trace(format, args...)
	}
}

func (d *Dialer) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return 60 * time.Second
}

// DialContext opens a TCP stream to addr ("host:port") through the mixnet.
// It has the signature net/http and gRPC expect.
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("mixnet: unsupported network %q", network)
	}
	host, port, err := splitPort(addr)
	if err != nil {
		return nil, err
	}
	target := host
	if d.DNS != DNSExit {
		if addrs, err := d.Resolve(ctx, host); err == nil {
			target = addrs[rand.IntN(len(addrs))]
		} else if errors.Is(err, ErrProxyUnreachable) {
			return nil, err
		} else {
			d.trace("dns  %s: DoH over the mixnet failed (%v); letting the exit resolve it", host, err)
		}
	}
	t0 := time.Now()
	d.trace("dial %s:%d -> socks5 %s -> mixnet -> exit -> %s:%d", host, port, d.Proxy, target, port)
	conn, err := dialSocks(ctx, d.Proxy, target, port, d.timeout())
	if err != nil {
		d.trace("fail %s:%d after %.2fs: %v", host, port, time.Since(t0).Seconds(), err)
		return nil, err
	}
	d.trace("open %s:%d socks5 CONNECT accepted by the local mixnet client in %.2fs", host, port, time.Since(t0).Seconds())
	return conn, nil
}

// DialTLS opens a stream through the mixnet and runs TLS end to end with the
// destination: the exit gateway only relays ciphertext. The certificate is
// verified against serverName (defaults to the host in addr).
func (d *Dialer) DialTLS(ctx context.Context, addr string, cfg *tls.Config) (*tls.Conn, error) {
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := cfg.Clone()
	if c.ServerName == "" {
		c.ServerName, _, _ = net.SplitHostPort(addr)
	}
	tc := tls.Client(raw, c)
	t0 := time.Now()
	if err := tc.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, fmt.Errorf("TLS with %s over the mixnet: %w", c.ServerName, err)
	}
	st := tc.ConnectionState()
	d.trace("tls  %s %s alpn=%q verified for %s (%.2fs)", c.ServerName, tls.VersionName(st.Version), st.NegotiatedProtocol, c.ServerName, time.Since(t0).Seconds())
	return tc, nil
}

// Resolve returns IPv4 addresses for host using DoH over the mixnet.
func (d *Dialer) Resolve(ctx context.Context, host string) ([]string, error) {
	if _, err := netip.ParseAddr(host); err == nil {
		return []string{host}, nil
	}
	d.mu.Lock()
	if e, ok := d.cache[host]; ok && time.Now().Before(e.until) {
		d.mu.Unlock()
		return e.addrs, nil
	}
	d.mu.Unlock()
	resolvers := d.Resolvers
	if resolvers == nil {
		for _, r := range dohResolvers {
			resolvers = append(resolvers, r.ip)
		}
	}
	var errs []error
	for _, ip := range resolvers {
		path := "/dns-query?type=A&name="
		for _, r := range dohResolvers {
			if r.ip == ip {
				path = r.path
			}
		}
		t0 := time.Now()
		addrs, err := d.dohQuery(ctx, ip, path+url.QueryEscape(host))
		if err != nil {
			if errors.Is(err, ErrProxyUnreachable) {
				return nil, err
			}
			d.trace("dns  DoH via %s failed after %.2fs: %v", ip, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", ip, err))
			continue
		}
		d.trace("dns  %s -> %v (DoH via %s, over the mixnet, %.2fs)", host, addrs, ip, time.Since(t0).Seconds())
		d.mu.Lock()
		if d.cache == nil {
			d.cache = map[string]dnsEntry{}
		}
		d.cache[host] = dnsEntry{time.Now().Add(5 * time.Minute), addrs}
		d.mu.Unlock()
		return addrs, nil
	}
	return nil, fmt.Errorf("could not resolve %s over the mixnet: %w", host, errors.Join(errs...))
}

func (d *Dialer) dohQuery(ctx context.Context, ip, pathQuery string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	conn, err := dialSocks(ctx, d.Proxy, ip, 443, d.timeout())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// ServerName is an IP literal: Go verifies it against the certificate's IP SANs.
	tc := tls.Client(conn, &tls.Config{ServerName: ip, RootCAs: d.RootCAs, NextProtos: []string{"http/1.1"}})
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = tc.SetDeadline(dl)
	}
	req := "GET " + pathQuery + " HTTP/1.1\r\nHost: " + ip + "\r\nAccept: application/dns-json\r\nUser-Agent: Mozilla/5.0\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(tc, req); err != nil {
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("DoH HTTP %d", resp.StatusCode)
	}
	var body struct {
		Answer []struct {
			Type int    `json:"type"`
			Data string `json:"data"`
		}
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return nil, err
	}
	var out []string
	for _, a := range body.Answer {
		if a.Type == 1 {
			if ip, err := netip.ParseAddr(a.Data); err == nil && ip.Is4() {
				out = append(out, ip.String())
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no A records")
	}
	return out, nil
}

// HTTPClient returns an http.Client whose every connection goes through the
// mixnet. Proxy settings from the environment are ignored on purpose.
func (d *Dialer) HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         d.DialContext,
			TLSHandshakeTimeout: d.timeout(),
			DisableKeepAlives:   true,
			TLSClientConfig:     &tls.Config{RootCAs: d.RootCAs},
		},
	}
}

// HostPort normalises "host", "host:port" or "https://host:port" to host:port,
// using defPort when none is given.
func HostPort(s string, defPort int) (string, error) {
	if u, err := url.Parse(s); err == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http" || u.Scheme == "grpc" || u.Scheme == "grpcs") {
		s = u.Host
	}
	if _, _, err := net.SplitHostPort(s); err != nil {
		s = net.JoinHostPort(s, strconv.Itoa(defPort))
	}
	_, _, err := splitPort(s)
	return s, err
}
