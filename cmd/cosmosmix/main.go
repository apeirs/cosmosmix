// Command cosmosmix: Cosmos SDK chain traffic (CometBFT RPC, REST/LCD) over the Nym mixnet.
//
//	cosmosmix nym setup         start nym-socks5-client on a random active exit gateway
//	cosmosmix serve             local gateway: http://127.0.0.1:26657/<chain>/rpc and /<chain>/rest
//	cosmosmix status|balance|broadcast   talk to a public node directly over the mixnet
//	cosmosmix probe             which public nodes answer Nym exit traffic, and how fast
//	cosmosmix ip                which IP do nodes see? (a Nym exit gateway)
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/apeirs/cosmosmix/cosmos"
	"github.com/apeirs/cosmosmix/gateway"
	"github.com/apeirs/cosmosmix/mixnet"
)

const version = "0.1.0"

var start = time.Now()

func tracef(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[%7.2fs] "+format+"\n", append([]any{time.Since(start).Seconds()}, args...)...)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "cosmosmix:", err)
	os.Exit(1)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// multi is a repeatable string flag that also splits on commas.
type multi []string

func (m *multi) String() string { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error {
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			*m = append(*m, p)
		}
	}
	return nil
}

type common struct {
	proxy, dns, chain, rpcURL, restURL string
	add, addREST                       multi
	verbose, jsonOut                   bool
	timeout                            time.Duration
	reg                                *cosmos.Registry
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.proxy, "proxy", envOr("COSMOSMIX_PROXY", "127.0.0.1:1080"), "nym-socks5-client SOCKS5 address ($COSMOSMIX_PROXY)")
	fs.StringVar(&c.dns, "dns", envOr("COSMOSMIX_DNS", mixnet.DNSDoH), "name resolution: doh (DoH over the mixnet, connect by IPv4) or exit")
	fs.StringVar(&c.chain, "chain", "nyx", "network: nyx cosmoshub osmosis celestia, or one added with --add")
	fs.StringVar(&c.rpcURL, "rpc", "", "use this https RPC URL instead of the chain's list")
	fs.StringVar(&c.restURL, "rest", "", "use this https REST/LCD URL instead of the chain's list")
	fs.Var(&c.add, "add", "extra chain RPC upstreams: name=https://rpc[|https://rpc2] (repeatable)")
	fs.Var(&c.addREST, "add-rest", "extra chain REST upstreams: name=https://lcd[|https://lcd2] (repeatable)")
	fs.BoolVar(&c.verbose, "v", false, "trace every mixnet step to stderr")
	fs.BoolVar(&c.jsonOut, "json", false, "JSON output")
	fs.DurationVar(&c.timeout, "timeout", 5*time.Minute, "overall timeout for the command")
}

func (c *common) registry() *cosmos.Registry {
	if c.reg != nil {
		return c.reg
	}
	c.reg = cosmos.NewRegistry()
	for _, a := range c.add {
		if err := c.reg.ParseAdd(a, false); err != nil {
			die(fmt.Errorf("--add: %w", err))
		}
	}
	for _, a := range c.addREST {
		if err := c.reg.ParseAdd(a, true); err != nil {
			die(fmt.Errorf("--add-rest: %w", err))
		}
	}
	return c.reg
}

func (c *common) dialer() *mixnet.Dialer {
	proxy, err := mixnet.ParseProxy(c.proxy)
	if err != nil {
		die(err)
	}
	if c.dns != mixnet.DNSDoH && c.dns != mixnet.DNSExit {
		die(fmt.Errorf("--dns must be doh or exit"))
	}
	d := &mixnet.Dialer{Proxy: proxy, DNS: c.dns}
	if c.verbose {
		d.Trace = tracef
	}
	return d
}

func (c *common) client() *cosmos.Client {
	cl := &cosmos.Client{D: c.dialer()}
	if c.verbose {
		cl.Log = tracef
	}
	return cl
}

func (c *common) chainInfo() *cosmos.Chain {
	ch, ok := c.registry().Get(c.chain)
	if !ok {
		if c.rpcURL != "" || c.restURL != "" {
			return &cosmos.Chain{Name: c.chain}
		}
		die(fmt.Errorf("unknown chain %q (see `cosmosmix chains`), or pass --rpc / --rest URL", c.chain))
	}
	return ch
}

func (c *common) rpcURLs() []string {
	if c.rpcURL != "" {
		if err := cosmos.CheckURL(c.rpcURL); err != nil {
			die(err)
		}
		return []string{c.rpcURL}
	}
	ch := c.chainInfo()
	if len(ch.RPC) == 0 {
		die(fmt.Errorf("chain %s has no RPC upstream; pass --rpc URL", ch.Name))
	}
	return ch.RPC
}

func (c *common) restURLs() []string {
	if c.restURL != "" {
		if err := cosmos.CheckURL(c.restURL); err != nil {
			die(err)
		}
		return []string{c.restURL}
	}
	ch := c.chainInfo()
	if len(ch.REST) == 0 {
		die(fmt.Errorf("chain %s has no REST upstream; pass --rest URL", ch.Name))
	}
	return ch.REST
}

func (c *common) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	return ctx, func() { stop(); cancel() }
}

func (c *common) out(v any, human string) {
	if c.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(v)
		return
	}
	fmt.Println(human)
}

// parse allows flags after positional arguments.
func parse(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			os.Exit(2)
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `cosmosmix `+version+`: Cosmos SDK chains (CometBFT RPC + REST/LCD) through the Nym mixnet

Mixnet client (official nym-socks5-client):
  cosmosmix nym setup [--binary PATH] [--port 1080]   init on a random active exit gateway, then start
  cosmosmix nym start | stop | status | exits

Local gateway for wallets and tools (Keplr, Leap, cosmjs, nym-cli, gaiad, osmosisd ...):
  cosmosmix serve [--listen 127.0.0.1:26657] [--default nyx] [--rest-listen 127.0.0.1:1317]
                  [--add name=https://rpc|https://rpc2] [--add-rest name=https://lcd]
     RPC   http://127.0.0.1:26657/nyx/rpc     (also /cosmoshub/rpc, /osmosis/rpc, /celestia/rpc)
     REST  http://127.0.0.1:26657/nyx/rest
     root  http://127.0.0.1:26657             RPC of the default chain, for --node tcp://127.0.0.1:26657

Talk to a public node directly over the mixnet:
  cosmosmix status [--chain nyx]               latest block height and time
  cosmosmix balance ADDRESS [--chain nyx]      bank balances (REST)
  cosmosmix broadcast TX_BASE64|FILE|-         forward a signed tx (broadcast_tx_sync); holds no keys
  cosmosmix chains                             built-in networks and upstreams
  cosmosmix probe [--chain all]                which upstreams work through the mixnet, and how fast
  cosmosmix ip                                 which IP do nodes see? (should be a Nym exit gateway)

Common flags: --chain NAME --rpc URL --rest URL --proxy 127.0.0.1:1080 --dns doh|exit -v --json --timeout 5m
cosmosmix never holds a private key, and there is no clearnet fallback: if the
mixnet client is down, cosmosmix refuses to connect.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "nym":
		cmdNym(args)
	case "serve":
		cmdServe(args)
	case "status":
		cmdStatus(args)
	case "balance":
		cmdBalance(args)
	case "broadcast":
		cmdBroadcast(args)
	case "chains":
		cmdChains(args)
	case "probe":
		cmdProbe(args)
	case "ip":
		cmdIP(args)
	case "version", "-version", "--version":
		fmt.Println("cosmosmix", version)
	default:
		usage()
		os.Exit(2)
	}
}

func cmdChains(args []string) {
	var c common
	fs := flag.NewFlagSet("chains", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "CHAIN\tCHAIN ID\tPREFIX\tKIND\tUPSTREAMS")
	for _, ch := range c.registry().All() {
		fmt.Fprintf(tw, "%s\t%s\t%s\trpc\t%s\n", ch.Name, ch.ChainID, ch.Prefix, strings.Join(ch.RPC, " "))
		fmt.Fprintf(tw, "\t\t\trest\t%s\n", strings.Join(ch.REST, " "))
	}
	tw.Flush()
}

func loopback(addr string) bool {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(h)
	return h == "localhost" || (ip != nil && ip.IsLoopback())
}

func cmdServe(args []string) {
	var c common
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	c.register(fs)
	listen := fs.String("listen", "127.0.0.1:26657", "local address for RPC (/<chain>/rpc), REST (/<chain>/rest) and the default chain's RPC at /")
	restListen := fs.String("rest-listen", "", "optional second listener serving bare REST, e.g. 127.0.0.1:1317 (/<chain>/... and the default chain at /)")
	def := fs.String("default", "nyx", "chain served at the root (\"\" = none)")
	hosts := fs.String("allow-host", "", "extra Host header values to accept (comma separated)")
	origins := fs.String("allow-origin", "", "web origins allowed to use the gateway, comma separated (\"*\" = any page; browser extensions and non-browser clients are always allowed)")
	parse(fs, args)
	reg := c.registry()
	if *def != "" {
		if _, ok := reg.Get(*def); !ok {
			die(fmt.Errorf("unknown --default chain %q", *def))
		}
	}
	cl := c.client()
	mk := func(restOnly bool) *gateway.Gateway {
		g := &gateway.Gateway{Client: cl, Chains: reg, Default: *def, RESTOnly: restOnly, Log: logf}
		if *hosts != "" {
			g.Hosts = strings.Split(*hosts, ",")
		}
		if *origins != "" {
			g.Origins = strings.Split(*origins, ",")
		}
		return g
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	run := func(addr string, h http.Handler, what string) {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			die(err)
		}
		if !loopback(addr) {
			fmt.Fprintf(os.Stderr, "WARNING: %s is not bound to loopback; anyone who can reach it can use your mixnet client\n", addr)
		}
		logf("cosmosmix %s on http://%s  (default chain %q; SOCKS5 %s)", what, ln.Addr(), *def, c.proxy)
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 30 * time.Second}
		go func() { <-ctx.Done(); srv.Close() }()
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			die(err)
		}
	}
	if *restListen != "" {
		go run(*restListen, mk(true), "REST gateway")
	}
	run(*listen, mk(false), "gateway")
}

func age(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	if d < 0 {
		d = 0
	}
	return d.String()
}

func cmdStatus(args []string) {
	var c common
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	ctx, cancel := c.ctx()
	defer cancel()
	urls := c.rpcURLs()
	status, body, used, err := c.client().Get(ctx, urls, "/status", "", "status")
	if err != nil {
		die(err)
	}
	var r struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
				Version string `json:"version"`
			} `json:"node_info"`
			SyncInfo struct {
				Height     string    `json:"latest_block_height"`
				Hash       string    `json:"latest_block_hash"`
				Time       time.Time `json:"latest_block_time"`
				CatchingUp bool      `json:"catching_up"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if status != 200 || json.Unmarshal(body, &r) != nil || r.Result.SyncInfo.Height == "" {
		die(fmt.Errorf("unexpected /status answer from %s: HTTP %d %.120q", cosmos.HostOf(used), status, body))
	}
	si := r.Result.SyncInfo
	human := fmt.Sprintf("%s (chain id %s): block %s at %s, %s old (via %s over the mixnet)", c.chain, r.Result.NodeInfo.Network,
		si.Height, si.Time.UTC().Format(time.RFC3339), age(si.Time), cosmos.HostOf(used))
	if ch, ok := c.registry().Get(c.chain); ok && ch.ChainID != "" && ch.ChainID != r.Result.NodeInfo.Network {
		human += fmt.Sprintf("\nWARNING: expected chain id %s, the node reports %s", ch.ChainID, r.Result.NodeInfo.Network)
	}
	if si.CatchingUp {
		human += "\nWARNING: that node is still catching up"
	}
	c.out(map[string]any{"chain": c.chain, "chain_id": r.Result.NodeInfo.Network, "height": si.Height, "time": si.Time,
		"hash": si.Hash, "catching_up": si.CatchingUp, "node_version": r.Result.NodeInfo.Version, "upstream": used}, human)
}

func cmdBalance(args []string) {
	var c common
	fs := flag.NewFlagSet("balance", flag.ExitOnError)
	c.register(fs)
	all := fs.Bool("all", false, "list every denom (default: the chain's own denom plus the first 5 others)")
	pos := parse(fs, args)
	if len(pos) != 1 {
		die(fmt.Errorf("usage: cosmosmix balance ADDRESS [--chain nyx] [--all]"))
	}
	addr := strings.ToLower(pos[0])
	hrp, err := cosmos.Bech32HRP(addr)
	if err != nil {
		die(err)
	}
	if ch, ok := c.registry().Get(c.chain); ok && ch.Prefix != "" && ch.Prefix != hrp {
		die(fmt.Errorf("address prefix %q does not match chain %s (prefix %q); pass --chain", hrp, ch.Name, ch.Prefix))
	}
	ctx, cancel := c.ctx()
	defer cancel()
	var r struct {
		Balances []struct{ Denom, Amount string } `json:"balances"`
	}
	used, err := c.client().REST(ctx, c.restURLs(), "/cosmos/bank/v1beta1/balances/"+url.PathEscape(addr), "pagination.limit=1000", &r)
	if err != nil {
		die(err)
	}
	native := ""
	if ch, ok := c.registry().Get(c.chain); ok {
		native = ch.Denom
	}
	var lines, others []string
	for _, b := range r.Balances {
		if b.Denom == native {
			lines = append(lines, "  "+cosmos.Display(b.Amount, b.Denom))
		} else {
			others = append(others, "  "+cosmos.Display(b.Amount, b.Denom))
		}
	}
	if native != "" && len(lines) == 0 {
		lines = append(lines, "  "+cosmos.Display("0", native))
	}
	if !*all && len(others) > 5 {
		n := len(others) - 5
		others = append(others[:5], fmt.Sprintf("  ... and %d more denoms (--all or --json to list them)", n))
	}
	lines = append(lines, others...)
	c.out(map[string]any{"address": addr, "chain": c.chain, "balances": r.Balances, "upstream": used},
		fmt.Sprintf("%s on %s (via %s over the mixnet):\n%s", addr, c.chain, cosmos.HostOf(used), strings.Join(lines, "\n")))
}

func cmdBroadcast(args []string) {
	var c common
	fs := flag.NewFlagSet("broadcast", flag.ExitOnError)
	c.register(fs)
	mode := fs.String("mode", "sync", "sync (wait for CheckTx) or async")
	pos := parse(fs, args)
	if len(pos) != 1 {
		die(fmt.Errorf("usage: cosmosmix broadcast TX_BASE64 | FILE | -   (a signed TxRaw, base64)"))
	}
	raw := pos[0]
	if raw == "-" {
		b, _ := io.ReadAll(io.LimitReader(os.Stdin, 16<<20))
		raw = string(b)
	} else if st, err := os.Stat(raw); err == nil && !st.IsDir() {
		b, err := os.ReadFile(raw)
		if err != nil {
			die(err)
		}
		raw = string(b)
	}
	raw = strings.TrimSpace(raw)
	txb, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(txb) == 0 {
		die(fmt.Errorf("not a base64-encoded signed transaction"))
	}
	method := map[string]string{"sync": "broadcast_tx_sync", "async": "broadcast_tx_async"}[*mode]
	if method == "" {
		die(fmt.Errorf("--mode must be sync or async"))
	}
	ctx, cancel := c.ctx()
	defer cancel()
	res, used, err := c.client().RPC(ctx, c.rpcURLs(), method, map[string]any{"tx": base64.StdEncoding.EncodeToString(txb)})
	if err != nil {
		die(fmt.Errorf("%v (via %s over the mixnet)", err, cosmos.HostOf(used)))
	}
	var r struct {
		Code      int    `json:"code"`
		Log       string `json:"log"`
		Codespace string `json:"codespace"`
		Hash      string `json:"hash"`
	}
	if err := json.Unmarshal(res, &r); err != nil {
		die(fmt.Errorf("unexpected answer: %.200s", res))
	}
	v := map[string]any{"code": r.Code, "codespace": r.Codespace, "log": r.Log, "hash": r.Hash, "upstream": used}
	if r.Code != 0 {
		c.out(v, fmt.Sprintf("rejected by the node: code %d (%s): %s\ntx hash %s (via %s over the mixnet)", r.Code, r.Codespace, r.Log, r.Hash, cosmos.HostOf(used)))
		os.Exit(1)
	}
	c.out(v, fmt.Sprintf("broadcast ok: %s (via %s over the mixnet)", r.Hash, cosmos.HostOf(used)))
}

type probeRow struct {
	chain, kind, upstream, result string
	secs                          float64
	order                         int
}

func cmdProbe(args []string) {
	var c common
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	c.register(fs)
	par := fs.Int("parallel", 4, "upstreams probed at once")
	parse(fs, args)
	if c.chain == "nyx" && !flagSet(fs, "chain") {
		c.chain = "all"
	}
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	var chains []*cosmos.Chain
	switch {
	case c.rpcURL != "" || c.restURL != "":
		ch := &cosmos.Chain{Name: "custom"}
		if c.rpcURL != "" {
			ch.RPC = []string{c.rpcURL}
		}
		if c.restURL != "" {
			ch.REST = []string{c.restURL}
		}
		chains = []*cosmos.Chain{ch}
	case c.chain == "all":
		chains = c.registry().All()
	default:
		chains = []*cosmos.Chain{c.chainInfo()}
	}
	type job struct {
		ch   *cosmos.Chain
		rest bool
		u    string
		n    int
	}
	var jobs []job
	for _, ch := range chains {
		for _, u := range ch.RPC {
			jobs = append(jobs, job{ch, false, u, len(jobs)})
		}
		for _, u := range ch.REST {
			jobs = append(jobs, job{ch, true, u, len(jobs)})
		}
	}
	rows := make([]probeRow, len(jobs))
	sem := make(chan struct{}, max(1, *par))
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			t0 := time.Now()
			res := probeOne(ctx, cl, j.ch, j.rest, j.u)
			kind := "rpc"
			if j.rest {
				kind = "rest"
			}
			rows[j.n] = probeRow{j.ch.Name, kind, strings.TrimPrefix(j.u, "https://"), res, time.Since(t0).Seconds(), j.n}
		}(j)
	}
	wg.Wait()
	sort.Slice(rows, func(a, b int) bool { return rows[a].order < rows[b].order })
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "CHAIN\tKIND\tUPSTREAM\tRESULT\tTIME")
	ok := 0
	for _, r := range rows {
		if strings.HasPrefix(r.result, "block") {
			ok++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.1fs\n", r.chain, r.kind, r.upstream, r.result, r.secs)
	}
	tw.Flush()
	fmt.Fprintf(os.Stderr, "%d of %d upstreams answered through the mixnet\n", ok, len(rows))
}

func flagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func short(err error) string {
	e := strings.ReplaceAll(err.Error(), "\n", "; ")
	if len(e) > 70 {
		e = e[:70]
	}
	return "FAIL " + e
}

func probeOne(ctx context.Context, cl *cosmos.Client, ch *cosmos.Chain, rest bool, u string) string {
	if !rest {
		status, body, _, err := cl.Get(ctx, []string{u}, "/status", "", "status")
		if err != nil {
			return short(err)
		}
		var r struct {
			Result struct {
				NodeInfo struct{ Network string } `json:"node_info"`
				SyncInfo struct {
					Height string `json:"latest_block_height"`
				} `json:"sync_info"`
			} `json:"result"`
		}
		if status != 200 || json.Unmarshal(body, &r) != nil || r.Result.SyncInfo.Height == "" {
			return fmt.Sprintf("FAIL HTTP %d, not a /status answer", status)
		}
		s := "block " + r.Result.SyncInfo.Height
		if ch.ChainID != "" && r.Result.NodeInfo.Network != ch.ChainID {
			s += " WRONG CHAIN " + r.Result.NodeInfo.Network
		}
		return s
	}
	var r struct {
		Block struct {
			Header struct {
				ChainID string `json:"chain_id"`
				Height  string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if _, err := cl.REST(ctx, []string{u}, "/cosmos/base/tendermint/v1beta1/blocks/latest", "", &r); err != nil {
		return short(err)
	}
	if r.Block.Header.Height == "" {
		return "FAIL no block header"
	}
	s := "block " + r.Block.Header.Height
	if ch.ChainID != "" && r.Block.Header.ChainID != ch.ChainID {
		s += " WRONG CHAIN " + r.Block.Header.ChainID
	}
	return s
}

func cmdIP(args []string) {
	var c common
	fs := flag.NewFlagSet("ip", flag.ExitOnError)
	c.register(fs)
	noLookup := fs.Bool("no-lookup", false, "don't look the IP up in the nym-api node list")
	parse(fs, args)
	d := c.dialer()
	ctx, cancel := c.ctx()
	defer cancel()
	showIP(ctx, &c, d, *noLookup)
}
