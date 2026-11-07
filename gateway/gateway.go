// Package gateway is a local Cosmos endpoint for wallets and tools. Point
// Keplr, Leap, cosmjs, nym-cli or gaiad at
//
//	http://127.0.0.1:26657/<chain>/rpc    CometBFT RPC (GET URI form, POST JSON-RPC, /websocket)
//	http://127.0.0.1:26657/<chain>/rest   Cosmos REST / LCD
//	http://127.0.0.1:26657/...            RPC of the default chain (for --node tcp://127.0.0.1:26657)
//
// and every request is forwarded to a public node through the Nym mixnet.
package gateway

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apeirs/cosmosmix/cosmos"
)

// Gateway serves the routes above. With RESTOnly it is the bare REST listener
// (http://127.0.0.1:1317/<chain>/..., root = default chain).
type Gateway struct {
	Client   *cosmos.Client
	Chains   *cosmos.Registry
	Default  string   // chain served at the root ("" = none)
	RESTOnly bool     // serve REST at the root instead of RPC
	Hosts    []string // extra accepted Host values
	Origins  []string // extra accepted Origin values ("*" = any web page)
	Timeout  time.Duration
	Log      func(string, ...any)

	Total, Failed, WS atomic.Int64
}

func (g *Gateway) logf(f string, a ...any) {
	if g.Log != nil {
		g.Log(f, a...)
	}
}

func (g *Gateway) hostOK(h string) bool {
	if hh, _, err := net.SplitHostPort(h); err == nil {
		h = hh
	}
	h = strings.Trim(h, "[]")
	switch h {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	for _, x := range g.Hosts {
		if x == h {
			return true
		}
	}
	return false
}

// originOK: no Origin (CLI tools, node scripts), a browser extension (Keplr,
// Leap), or an origin the user allowed. Any other web page is refused, so a
// site you visit can't use your gateway.
func (g *Gateway) originOK(o string) bool {
	if o == "" {
		return true
	}
	for _, p := range []string{"chrome-extension://", "moz-extension://", "safari-web-extension://"} {
		if strings.HasPrefix(o, p) {
			return true
		}
	}
	for _, x := range g.Origins {
		if x == "*" || x == o {
			return true
		}
	}
	return false
}

type route struct {
	chain *cosmos.Chain
	rest  bool
	sub   string // escaped, cleaned path below the upstream base
}

func (g *Gateway) route(escaped string) (route, bool) {
	// An encoded dot segment would be decoded upstream and could step out of
	// an upstream's base path (e.g. https://host/cosmoshub/%2e%2e/osmosis).
	if strings.Contains(strings.ToLower(escaped), "%2e") {
		return route{}, false
	}
	trimmed := strings.TrimPrefix(escaped, "/")
	first, after, _ := strings.Cut(trimmed, "/")
	clean := func(p string) string { return path.Clean("/" + p) }
	if c, ok := g.Chains.Get(first); ok && first != "" {
		if g.RESTOnly {
			return route{c, true, clean(after)}, true
		}
		kind, sub, _ := strings.Cut(after, "/")
		switch kind {
		case "rpc":
			return route{c, false, clean(sub)}, true
		case "rest":
			return route{c, true, clean(sub)}, true
		}
		return route{}, false
	}
	c, ok := g.Chains.Get(g.Default)
	if !ok || g.Default == "" {
		return route{}, false
	}
	return route{c, g.RESTOnly, clean(trimmed)}, true
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// DNS-rebinding guard: a web page must not reach this through a rebound name.
	if !g.hostOK(r.Host) {
		http.Error(w, "bad Host header", http.StatusForbidden)
		return
	}
	origin := r.Header.Get("Origin")
	if !g.originOK(origin) {
		http.Error(w, "origin not allowed (see --allow-origin)", http.StatusForbidden)
		return
	}
	h := w.Header()
	if origin != "" {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
	}
	if r.Method == "OPTIONS" {
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "content-type")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	escaped := r.URL.EscapedPath()
	if escaped == "/" && r.Method == "GET" && r.Header.Get("Upgrade") == "" {
		g.help(w)
		return
	}
	rt, ok := g.route(escaped)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		g.help(w)
		return
	}
	if r.Method != "GET" && r.Method != "POST" {
		http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
		return
	}
	urls := rt.chain.RPC
	kind := "rpc"
	if rt.rest {
		urls, kind = rt.chain.REST, "rest"
	}
	if len(urls) == 0 {
		http.Error(w, fmt.Sprintf("chain %s has no %s upstream (add one with --add%s)", rt.chain.Name, kind,
			map[bool]string{true: "-rest", false: ""}[rt.rest]), http.StatusNotFound)
		return
	}
	if !rt.rest && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		g.websocket(w, r, rt, urls)
		return
	}
	var body []byte
	if r.Method == "POST" {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil || !json.Valid(body) {
			http.Error(w, "POST body must be JSON", http.StatusBadRequest)
			return
		}
	}
	label, upLabel := cosmos.RESTLabel(rt.sub), ""
	if rt.rest {
		upLabel = "rest " + label
	} else {
		label = cosmos.RPCLabel(r.Method, rt.sub, body)
		upLabel = label
	}
	g.Total.Add(1)
	tmo := g.Timeout
	if tmo == 0 {
		tmo = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), tmo)
	defer cancel()
	t0 := time.Now()
	resp, err := g.Client.Do(ctx, urls, cosmos.Request{Method: r.Method, Path: rt.sub, RawQuery: r.URL.RawQuery, Body: body, Label: upLabel})
	if err != nil {
		g.Failed.Add(1)
		g.logf("%s %s %s FAILED after %.1fs: %v", rt.chain.Name, kind, label, time.Since(t0).Seconds(), err)
		g.fail(w, rt.rest, err)
		return
	}
	defer resp.Body.Close()
	// Only the content type comes back: no Set-Cookie, no Location, no server
	// headers that would tempt the caller to talk to the upstream directly.
	if resp.ContentType != "" {
		h.Set("Content-Type", resp.ContentType)
	}
	w.WriteHeader(resp.Status)
	n, _ := io.Copy(w, resp.Body)
	g.logf("%s %s %s -> mixnet -> %s HTTP %d, %d bytes in %.1fs", rt.chain.Name, kind, label, cosmos.HostOf(resp.Upstream), resp.Status, n, time.Since(t0).Seconds())
}

func (g *Gateway) fail(w http.ResponseWriter, rest bool, err error) {
	msg := fmt.Sprintf("cosmosmix: all upstreams failed over the mixnet (no clearnet fallback): %v", err)
	js, _ := json.Marshal(msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	if rest {
		fmt.Fprintf(w, `{"code":14,"message":%s,"details":[]}`, js) // gRPC code 14 = Unavailable
		return
	}
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":%s}}`, js)
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// websocket splices a client websocket to the node's /websocket over the mixnet.
// The handshake is redone upstream with only the websocket headers; frames
// pass through untouched (subscriptions, JSON-RPC over websocket).
func (g *Gateway) websocket(w http.ResponseWriter, r *http.Request, rt route, urls []string) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if r.Method != "GET" || key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "bad websocket handshake", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket not supported here", http.StatusInternalServerError)
		return
	}
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	up, err := g.Client.Websocket(ctx, urls, rt.sub, key, r.Header.Get("Sec-WebSocket-Protocol"))
	cancel()
	if err != nil {
		g.Failed.Add(1)
		g.logf("%s rpc websocket FAILED after %.1fs: %v", rt.chain.Name, time.Since(t0).Seconds(), err)
		g.fail(w, false, err)
		return
	}
	defer up.Close()
	sum := sha1.Sum([]byte(key + wsGUID))
	want := base64.StdEncoding.EncodeToString(sum[:])
	if up.Accept != want {
		g.logf("%s rpc websocket: upstream sent a wrong Sec-WebSocket-Accept, closing", rt.chain.Name)
		http.Error(w, "upstream websocket handshake invalid", http.StatusBadGateway)
		return
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + want + "\r\n"
	if up.Protocol != "" {
		resp += "Sec-WebSocket-Protocol: " + up.Protocol + "\r\n"
	}
	if _, err := io.WriteString(conn, resp+"\r\n"); err != nil {
		return
	}
	g.WS.Add(1)
	g.logf("%s rpc websocket -> mixnet -> %s open in %.1fs", rt.chain.Name, cosmos.HostOf(up.Upstream), time.Since(t0).Seconds())
	done := make(chan struct{}, 2)
	go func() { io.Copy(up.Conn, brw.Reader); done <- struct{}{} }()
	go func() { io.Copy(conn, up.R); done <- struct{}{} }()
	<-done
	g.logf("%s rpc websocket closed after %.0fs", rt.chain.Name, time.Since(t0).Seconds())
}

func (g *Gateway) help(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if g.RESTOnly {
		fmt.Fprintf(w, "cosmosmix REST gateway: Cosmos REST/LCD over the Nym mixnet.\n  /<chain>/...  REST of that chain; anything else goes to the default chain (%s)\n", g.Default)
	} else {
		fmt.Fprintf(w, "cosmosmix gateway: Cosmos RPC and REST over the Nym mixnet.\n")
		fmt.Fprintf(w, "  /<chain>/rpc   CometBFT RPC (GET /status, POST JSON-RPC, /websocket)\n  /<chain>/rest  Cosmos REST / LCD\n")
		if g.Default != "" {
			fmt.Fprintf(w, "  any other path: RPC of the default chain (%s)\n", g.Default)
		}
	}
	fmt.Fprintf(w, "chains:\n")
	for _, c := range g.Chains.All() {
		fmt.Fprintf(w, "  %-10s %-12s rpc upstreams %d, rest upstreams %d\n", c.Name, c.ChainID, len(c.RPC), len(c.REST))
	}
	fmt.Fprintf(w, "requests so far: %d, failed: %d, websockets: %d\n", g.Total.Load(), g.Failed.Load(), g.WS.Load())
}
