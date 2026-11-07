package cosmosmix_test

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apeirs/cosmosmix/cosmos"
	"github.com/apeirs/cosmosmix/gateway"
	"github.com/apeirs/cosmosmix/internal/fakes"
	"github.com/apeirs/cosmosmix/mixnet"
)

const (
	testAddr = "n10c7h7wae558d5zejysktztheca6t3gc3vrnep9"
	testTx   = "CpIBCo8BChwvY29zbW9zLmJhbmsudjFiZXRhMS5Nc2dTZW5kEm8="
)

// seen is one request as the fake upstream node received it.
type seen struct {
	Host, Method, Path, RawQuery, Body string
	Header                             http.Header
}

type world struct {
	socks *fakes.SOCKS
	cl    *cosmos.Client
	mu    sync.Mutex
	got   []seen
	logs  []string
}

func (w *world) record(r *http.Request) seen {
	b, _ := io.ReadAll(r.Body)
	s := seen{r.Host, r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Clone()}
	w.mu.Lock()
	w.got = append(w.got, s)
	w.mu.Unlock()
	return s
}

func (w *world) last(host string) seen {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.got) - 1; i >= 0; i-- {
		if w.got[i].Host == host {
			return w.got[i]
		}
	}
	return seen{}
}

func (w *world) logf(f string, a ...any) {
	w.mu.Lock()
	w.logs = append(w.logs, fmt.Sprintf(f, a...))
	w.mu.Unlock()
}

func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func newWorld(t *testing.T) *world {
	t.Helper()
	ca := fakes.NewCA()
	w := &world{}
	serve := func(h http.HandlerFunc, name string) string {
		ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{ca.Leaf(name)}, NextProtos: []string{"http/1.1"}})
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: h}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		return ln.Addr().String()
	}
	rpcNode := serve(func(rw http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.record(r)
			conn, brw, err := rw.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", wsAccept(r.Header.Get("Sec-WebSocket-Key")))
			io.Copy(conn, brw) // echo
			return
		}
		s := w.record(r)
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Set-Cookie", "track=1")
		rw.Header().Set("Location", "https://rpc.test/elsewhere")
		switch {
		case r.Method == "GET" && strings.HasSuffix(s.Path, "/status"):
			io.WriteString(rw, `{"jsonrpc":"2.0","id":-1,"result":{"node_info":{"network":"testnet-1"},"sync_info":{"latest_block_height":"123","latest_block_time":"2026-11-07T10:00:00Z"}}}`)
		case r.Method == "GET":
			io.WriteString(rw, `{"jsonrpc":"2.0","id":-1,"result":{"response":{"code":0,"value":"AQ=="}}}`)
		default:
			var q struct {
				ID     json.RawMessage
				Method string
			}
			json.Unmarshal([]byte(s.Body), &q)
			io.WriteString(rw, `{"jsonrpc":"2.0","id":`+string(q.ID)+`,"result":{"code":2,"codespace":"sdk","log":"tx parse error","hash":"ABCD"}}`)
		}
	}, "rpc.test")
	restNode := serve(func(rw http.ResponseWriter, r *http.Request) {
		s := w.record(r)
		switch {
		case s.Path == "/redirect-plain":
			http.Redirect(rw, r, "http://rest.test/cosmos/bank/v1beta1/balances/"+testAddr, http.StatusFound)
		case s.Path == "/redirect-https":
			http.Redirect(rw, r, "https://rest.test/cosmos/bank/v1beta1/balances/"+testAddr, http.StatusFound)
		case strings.HasPrefix(s.Path, "/cosmos/bank/v1beta1/balances/"):
			rw.Header().Set("Content-Type", "application/json")
			io.WriteString(rw, `{"balances":[{"denom":"unym","amount":"1500000"}],"pagination":{"next_key":null,"total":"1"}}`)
		default:
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(404)
			io.WriteString(rw, `{"code":5,"message":"Not Implemented","details":[]}`)
		}
	}, "rest.test")
	bad := serve(func(rw http.ResponseWriter, r *http.Request) { w.record(r); http.Error(rw, "boom", 500) }, "bad.test")
	busy := serve(func(rw http.ResponseWriter, r *http.Request) { w.record(r); http.Error(rw, "slow down", 429) }, "busy.test")
	doh := fakes.DoH(ca, "1.1.1.1", map[string][]string{
		"rpc.test": {"10.0.0.9"}, "rest.test": {"10.0.0.10"}, "bad.test": {"10.0.0.8"}, "busy.test": {"10.0.0.7"}})
	w.socks = fakes.NewSOCKS(map[string]string{"1.1.1.1:443": doh, "10.0.0.9:443": rpcNode, "10.0.0.10:443": restNode,
		"10.0.0.8:443": bad, "10.0.0.7:443": busy})
	t.Cleanup(w.socks.Close)
	d := &mixnet.Dialer{Proxy: w.socks.Addr, DNS: mixnet.DNSDoH, RootCAs: ca.Pool, Resolvers: []string{"1.1.1.1"}, Timeout: 5 * time.Second}
	w.cl = &cosmos.Client{D: d, Timeout: 10 * time.Second, Log: w.logf}
	return w
}

func (w *world) registry(t *testing.T) *cosmos.Registry {
	reg := cosmos.NewRegistry()
	if err := reg.Add("testnet", []string{"https://bad.test", "https://rpc.test/hub"}, false); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("testnet", []string{"https://busy.test", "https://rest.test"}, true); err != nil {
		t.Fatal(err)
	}
	return reg
}

func (w *world) gateway(t *testing.T, restOnly bool) *httptest.Server {
	g := &gateway.Gateway{Client: w.cl, Chains: w.registry(t), Default: "testnet", RESTOnly: restOnly, Log: w.logf}
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url string, hdr map[string]string, body string) (*http.Response, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, url, rd)
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// assertOnlyMixnet: every connection went through the SOCKS proxy, the only
// name lookups were DoH over it, and no hostname was handed to the exit.
func assertOnlyMixnet(t *testing.T, w *world, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, r := range w.socks.Seen() {
		got[r.Host] = true
		if r.ATYP != 1 {
			t.Errorf("SOCKS CONNECT %v: DoH mode must connect by IPv4 literal", r)
		}
	}
	for _, h := range append(want, "1.1.1.1") {
		if !got[h] {
			t.Errorf("SOCKS never saw %s (saw %v)", h, got)
		}
	}
}

// The CometBFT RPC passes through untouched in both its forms: the GET URI
// form (query string byte for byte) and POST JSON-RPC (body byte for byte).
// The first upstream fails (HTTP 500), the second answers; its base path is kept.
func TestRPCPassThroughOverMixnet(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, false)
	q := `path=%22%2Fcosmos.bank.v1beta1.Query%2FBalance%22&data=0x0a2b6e31&height=0&prove=false`
	resp, body := do(t, "GET", srv.URL+"/testnet/rpc/abci_query?"+q, nil, "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"value":"AQ=="`) {
		t.Fatalf("abci_query: %d %s", resp.StatusCode, body)
	}
	s := w.last("rpc.test")
	if s.Path != "/hub/abci_query" || s.RawQuery != q {
		t.Fatalf("upstream saw path %q query %q", s.Path, s.RawQuery)
	}
	if w.last("bad.test").Path == "" {
		t.Fatal("first upstream was never tried")
	}
	rpcBody := `{"jsonrpc":"2.0","id":42,"method":"broadcast_tx_sync","params":{"tx":"` + testTx + `"}}`
	resp, body = do(t, "POST", srv.URL+"/testnet/rpc", map[string]string{"Content-Type": "application/json"}, rpcBody)
	if resp.StatusCode != 200 || !strings.Contains(body, `"id":42`) || !strings.Contains(body, "tx parse error") {
		t.Fatalf("broadcast: %d %s", resp.StatusCode, body)
	}
	if s := w.last("rpc.test"); s.Body != rpcBody || s.Method != "POST" || s.Path != "/hub/" {
		t.Fatalf("upstream got %s %s %q", s.Method, s.Path, s.Body)
	}
	assertOnlyMixnet(t, w, "10.0.0.8", "10.0.0.9")
}

// /<chain>/rest on the main port, the bare REST listener, and the default
// chain's RPC at the root (gaiad --node tcp://127.0.0.1:26657).
func TestRESTRoutesAndDefaultRoot(t *testing.T) {
	w := newWorld(t)
	main := w.gateway(t, false)
	rest := w.gateway(t, true)
	for _, u := range []string{
		main.URL + "/testnet/rest/cosmos/bank/v1beta1/balances/" + testAddr,
		rest.URL + "/testnet/cosmos/bank/v1beta1/balances/" + testAddr,
		rest.URL + "/cosmos/bank/v1beta1/balances/" + testAddr,
	} {
		resp, body := do(t, "GET", u, nil, "")
		if resp.StatusCode != 200 || !strings.Contains(body, `"unym"`) {
			t.Fatalf("%s: %d %s", u, resp.StatusCode, body)
		}
		if s := w.last("rest.test"); s.Path != "/cosmos/bank/v1beta1/balances/"+testAddr {
			t.Fatalf("upstream REST path %q", s.Path)
		}
	}
	// A node's 404 is an answer, passed through as is.
	resp, body := do(t, "GET", main.URL+"/testnet/rest/cosmos/nothing", nil, "")
	if resp.StatusCode != 404 || !strings.Contains(body, "Not Implemented") {
		t.Fatalf("REST 404: %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, "POST", main.URL+"/", nil, `{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`)
	if resp.StatusCode != 200 || w.last("rpc.test").Path != "/hub/" {
		t.Fatalf("root RPC: %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, "GET", main.URL+"/status", nil, "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"latest_block_height":"123"`) {
		t.Fatalf("root GET /status: %d %s", resp.StatusCode, body)
	}
	// GET / is the help page, not forwarded.
	if resp, body = do(t, "GET", main.URL+"/", nil, ""); !strings.Contains(body, "/<chain>/rpc") {
		t.Fatalf("help: %s", body)
	}
	for _, p := range []string{"/testnet/rpc/%2e%2e/x", "/testnet/other"} {
		if resp, _ = do(t, "GET", main.URL+p, nil, ""); resp.StatusCode != 404 {
			t.Fatalf("%s: want 404, got %d", p, resp.StatusCode)
		}
	}
	assertOnlyMixnet(t, w, "10.0.0.7", "10.0.0.10")
}

// Nothing about the local caller reaches the node, and nothing but the content
// type comes back (no Set-Cookie, no Location to follow over the clearnet).
func TestHeaderStripping(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, false)
	resp, _ := do(t, "GET", srv.URL+"/testnet/rpc/status", map[string]string{
		"Origin": "chrome-extension://dmkamcknogkgcdfhhbddcghachkejeap", "Cookie": "session=1", "Referer": "https://dapp.example/",
		"User-Agent": "Keplr/0.12", "X-Forwarded-For": "192.0.2.7", "Authorization": "Bearer x", "Accept-Language": "da-DK",
	}, "")
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	h := w.last("rpc.test").Header
	for _, k := range []string{"Origin", "Cookie", "Referer", "X-Forwarded-For", "Authorization", "Accept-Language"} {
		if h.Get(k) != "" {
			t.Errorf("%s leaked upstream: %q", k, h.Get(k))
		}
	}
	if h.Get("User-Agent") != cosmos.UserAgent {
		t.Errorf("User-Agent upstream = %q", h.Get("User-Agent"))
	}
	if resp.Header.Get("Set-Cookie") != "" || resp.Header.Get("Location") != "" {
		t.Errorf("upstream headers leaked back: %v", resp.Header)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "chrome-extension://dmkamcknogkgcdfhhbddcghachkejeap" {
		t.Errorf("extension origin not allowed: %v", resp.Header)
	}
}

func TestHostAndOriginGuard(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, false)
	before := len(w.socks.Seen())
	if resp, _ := do(t, "GET", srv.URL+"/testnet/rpc/status", map[string]string{"Host": "evil.example"}, ""); resp.StatusCode != 403 {
		t.Fatalf("rebinding Host accepted: %d", resp.StatusCode)
	}
	if resp, _ := do(t, "POST", srv.URL+"/testnet/rpc", map[string]string{"Origin": "https://evil.example"}, `{"method":"status"}`); resp.StatusCode != 403 {
		t.Fatalf("web page origin accepted: %d", resp.StatusCode)
	}
	if n := len(w.socks.Seen()); n != before {
		t.Fatalf("refused requests still opened %d mixnet streams", n-before)
	}
	g := &gateway.Gateway{Client: w.cl, Chains: w.registry(t), Default: "testnet", Origins: []string{"https://wallet.example"}}
	s2 := httptest.NewServer(g)
	defer s2.Close()
	if resp, _ := do(t, "GET", s2.URL+"/testnet/rpc/status", map[string]string{"Origin": "https://wallet.example"}, ""); resp.StatusCode != 200 {
		t.Fatalf("allowed origin refused: %d", resp.StatusCode)
	}
}

// Mixnet client down: 502 in the shape each API's clients expect, and no
// direct connection is ever attempted.
func TestNoClearnetFallback(t *testing.T) {
	w := newWorld(t)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	cl := &cosmos.Client{D: &mixnet.Dialer{Proxy: dead, Timeout: time.Second}, Timeout: 3 * time.Second}
	srv := httptest.NewServer(&gateway.Gateway{Client: cl, Chains: w.registry(t), Default: "testnet"})
	defer srv.Close()
	resp, body := do(t, "POST", srv.URL+"/testnet/rpc", nil, `{"jsonrpc":"2.0","id":1,"method":"status"}`)
	if resp.StatusCode != 502 || !strings.Contains(body, `"jsonrpc":"2.0"`) || !strings.Contains(body, "no SOCKS5 proxy") {
		t.Fatalf("RPC: want 502 JSON-RPC error, got %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, "GET", srv.URL+"/testnet/rest/cosmos/bank/v1beta1/balances/"+testAddr, nil, "")
	if resp.StatusCode != 502 || !strings.Contains(body, `"code":14`) {
		t.Fatalf("REST: want 502 gRPC-style error, got %d %s", resp.StatusCode, body)
	}
	if len(w.got) != 0 {
		t.Fatalf("an upstream was reached without the mixnet: %v", w.got)
	}
}

// Failure paths log no params either (Go's url.Error embeds the full URL).
func TestFailureLogsCarryNoParams(t *testing.T) {
	w := newWorld(t)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	cl := &cosmos.Client{D: &mixnet.Dialer{Proxy: dead, Timeout: time.Second}, Timeout: 3 * time.Second, Log: w.logf}
	srv := httptest.NewServer(&gateway.Gateway{Client: cl, Chains: w.registry(t), Default: "testnet", Log: w.logf})
	defer srv.Close()
	resp, body := do(t, "GET", srv.URL+"/testnet/rest/cosmos/bank/v1beta1/balances/"+testAddr, nil, "")
	do(t, "GET", srv.URL+"/testnet/rpc/abci_query?data=0x0a2b6e31", nil, "")
	w.mu.Lock()
	all := strings.Join(w.logs, "\n")
	w.mu.Unlock()
	if resp.StatusCode != 502 || strings.Contains(all+body, testAddr) || strings.Contains(all+body, "0x0a2b6e31") {
		t.Fatalf("param in failure log or answer:\n%s\n%s", all, body)
	}
	if strings.Count(all, "no SOCKS5 proxy") > 4 {
		t.Fatalf("kept trying upstreams with no proxy:\n%s", all)
	}
}

func TestFailoverAndRedirects(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	// 429 then a good node.
	var r struct {
		Balances []struct{ Denom, Amount string }
	}
	used, err := w.cl.REST(ctx, []string{"https://busy.test", "https://rest.test"}, "/cosmos/bank/v1beta1/balances/"+testAddr, "", &r)
	if err != nil || used != "https://rest.test" || len(r.Balances) != 1 {
		t.Fatalf("failover: %v %s %v", err, used, r)
	}
	// Every upstream failing with HTTP errors: the last answer is surfaced, not hidden.
	status, _, _, err := w.cl.Get(ctx, []string{"https://busy.test", "https://bad.test"}, "/status", "", "status")
	if err != nil || status != 500 {
		t.Fatalf("all-5xx: status %d err %v", status, err)
	}
	// A redirect is followed over the mixnet if it stays on https, refused otherwise.
	if _, err := w.cl.REST(ctx, []string{"https://rest.test"}, "/redirect-https", "", &r); err != nil {
		t.Fatalf("https redirect: %v", err)
	}
	if _, err := w.cl.REST(ctx, []string{"https://rest.test"}, "/redirect-plain", "", &r); err == nil || !strings.Contains(err.Error(), "non-https") {
		t.Fatalf("plain-http redirect followed: %v", err)
	}
}

func TestOnlyHTTPSUpstreams(t *testing.T) {
	w := newWorld(t)
	if _, _, _, err := w.cl.Get(context.Background(), []string{"http://rpc.test"}, "/status", "", "status"); err == nil {
		t.Fatal("plain http upstream accepted")
	}
	reg := cosmos.NewRegistry()
	for _, spec := range []string{"x=http://rpc.test", "x=https://user:pw@rpc.test", "rpc=https://rpc.test", "x=https://rpc.test?key=1"} {
		if err := reg.ParseAdd(spec, false); err == nil {
			t.Errorf("--add %s accepted", spec)
		}
	}
}

// Request params (addresses, txs, query data) never reach a log line.
func TestLogsCarryNoParams(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, false)
	do(t, "GET", srv.URL+"/testnet/rest/cosmos/bank/v1beta1/balances/"+testAddr, nil, "")
	do(t, "GET", srv.URL+"/testnet/rpc/abci_query?path=%22%2Fx%22&data=0x0a2b6e31", nil, "")
	do(t, "POST", srv.URL+"/testnet/rpc", nil, `{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_sync","params":{"tx":"`+testTx+`"}}`)
	w.mu.Lock()
	all := strings.Join(w.logs, "\n")
	w.mu.Unlock()
	for _, secret := range []string{testAddr, "0x0a2b6e31", testTx, "/x"} {
		if strings.Contains(all, secret) {
			t.Errorf("log contains %q:\n%s", secret, all)
		}
	}
	for _, want := range []string{"rest /cosmos/bank/v1beta1/balances/*", "abci_query", "broadcast_tx_sync"} {
		if !strings.Contains(all, want) {
			t.Errorf("log lacks %q:\n%s", want, all)
		}
	}
}

// /websocket subscriptions: the handshake is redone upstream over the mixnet
// and frames are spliced both ways.
func TestWebsocketSplice(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, false)
	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	fmt.Fprintf(conn, "GET /testnet/rpc/websocket HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nOrigin: chrome-extension://abc\r\nCookie: a=b\r\n\r\n", key)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 101 || resp.Header.Get("Sec-WebSocket-Accept") != wsAccept(key) {
		t.Fatalf("handshake: %d %v", resp.StatusCode, resp.Header)
	}
	s := w.last("rpc.test")
	if s.Path != "/hub/websocket" || s.Header.Get("Origin") != "" || s.Header.Get("Cookie") != "" {
		t.Fatalf("upstream websocket request: %s %v", s.Path, s.Header)
	}
	conn.Write([]byte("frame-bytes"))
	buf := make([]byte, 11)
	if _, err := io.ReadFull(br, buf); err != nil || string(buf) != "frame-bytes" {
		t.Fatalf("echo: %q %v", buf, err)
	}
	assertOnlyMixnet(t, w, "10.0.0.9")
}

func TestBuiltinChains(t *testing.T) {
	reg := cosmos.NewRegistry()
	for _, n := range []string{"nyx", "cosmoshub", "osmosis", "celestia"} {
		c, ok := reg.Get(n)
		if !ok || len(c.RPC) < 2 || len(c.REST) < 2 || c.ChainID == "" || c.Prefix == "" {
			t.Errorf("chain %s: %+v", n, c)
			continue
		}
		for _, u := range append(append([]string{}, c.RPC...), c.REST...) {
			if err := cosmos.CheckURL(u); err != nil {
				t.Errorf("%s: %v", n, err)
			}
		}
	}
}

// The upstream is a real, directly reachable TLS server on localhost. With
// the mixnet client down, nothing may reach it, in either DNS mode and at any
// layer (gateway RPC, gateway REST, CLI client): a direct dial would land here
// and be counted.
func TestNoFallbackEvenWhenDirectWouldWork(t *testing.T) {
	ca := fakes.NewCA()
	var mu sync.Mutex
	hits := 0
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{ca.Leaf("localhost", "127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		io.WriteString(rw, `{"jsonrpc":"2.0","id":1,"result":{}}`)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	// Nothing listens on this port at all (a listener that doesn't speak
	// SOCKS would test a different failure).
	dl, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := dl.Addr().String()
	dl.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	ups := []string{"https://localhost:" + port, "https://127.0.0.1:" + port}
	reg := cosmos.NewRegistry()
	if err := reg.Add("localnet", ups, false); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("localnet", ups, true); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{mixnet.DNSDoH, mixnet.DNSExit} {
		cl := &cosmos.Client{D: &mixnet.Dialer{Proxy: dead, DNS: mode, RootCAs: ca.Pool, Timeout: time.Second}, Timeout: 3 * time.Second}
		gw := httptest.NewServer(&gateway.Gateway{Client: cl, Chains: reg, Default: "localnet"})
		resp, body := do(t, "POST", gw.URL+"/localnet/rpc", nil, `{"jsonrpc":"2.0","id":1,"method":"status"}`)
		if resp.StatusCode != 502 || !strings.Contains(body, "no SOCKS5 proxy") {
			t.Errorf("dns=%s RPC: want 502, got %d %s", mode, resp.StatusCode, body)
		}
		resp, body = do(t, "GET", gw.URL+"/localnet/rest/cosmos/base/tendermint/v1beta1/node_info", nil, "")
		if resp.StatusCode != 502 {
			t.Errorf("dns=%s REST: want 502, got %d %s", mode, resp.StatusCode, body)
		}
		gw.Close()
		if _, _, err := cl.RPC(context.Background(), ups, "status", nil); err == nil {
			t.Errorf("dns=%s: CLI RPC answered without the mixnet", mode)
		}
		var v map[string]any
		if _, err := cl.REST(context.Background(), ups, "/cosmos/base/tendermint/v1beta1/node_info", "", &v); err == nil {
			t.Errorf("dns=%s: CLI REST answered without the mixnet", mode)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("the upstream was reached directly %d times", hits)
	}
}
