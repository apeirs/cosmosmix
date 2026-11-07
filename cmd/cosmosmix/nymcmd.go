package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apeirs/cosmosmix/mixnet"
	"github.com/apeirs/cosmosmix/nym"
)

// ---------------------------------------------------------------- nym

func cmdNym(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	sub := args[0]
	fs := flag.NewFlagSet("nym "+sub, flag.ExitOnError)
	binary := fs.String("binary", "", "path to nym-socks5-client ($COSMOSMIX_NYM_SOCKS5)")
	id := fs.String("id", nym.DefaultID, "client id (~/.nym/socks5-clients/ID)")
	port := fs.Int("port", nym.DefaultPort, "local SOCKS5 port")
	provider := fs.String("provider", "auto", "network requester address, or auto (random active exit gateway)")
	gw := fs.String("gateway", "", "entry gateway identity (default: chosen by the client)")
	noAnon := fs.Bool("no-anonymous", false, "disable reply SURBs (the exit then learns your mixnet address)")
	force := fs.Bool("force", false, "re-initialise (new identity, new exit)")
	minPerf := fs.Float64("min-performance", 0.95, "minimum exit performance for --provider auto")
	jsonOut := fs.Bool("json", false, "JSON output")
	parse(fs, args[1:])
	show := func(v any) {
		if *jsonOut {
			b, _ := json.MarshalIndent(v, "", "  ")
			fmt.Println(string(b))
			return
		}
		b, _ := json.Marshal(v)
		d := json.NewDecoder(strings.NewReader(string(b)))
		d.UseNumber()
		var m map[string]any
		if d.Decode(&m) != nil {
			fmt.Println(string(b))
			return
		}
		for _, k := range []string{"id", "pid", "socks5", "provider", "anonymous_replies", "client_address", "gateway", "log"} {
			fmt.Printf("%-18s %v\n", k, m[k])
		}
	}
	switch sub {
	case "setup":
		bin, err := nym.FindBinary(*binary)
		if err != nil {
			die(err)
		}
		cfg := nym.ConfigPath(*id)
		if _, err := os.Stat(cfg); err == nil && !*force {
			fmt.Fprintf(os.Stderr, "client %q already initialised (%s); use --force to re-init\n", *id, cfg)
		} else {
			if *force {
				nym.Stop(*id)
				_ = os.RemoveAll(nym.Home() + "/socks5-clients/" + *id)
			}
			prov := *provider
			if prov == "auto" {
				ps, err := nym.ListExitProviders(nym.NymAPI, *minPerf)
				if err != nil {
					die(err)
				}
				p, err := nym.PickProvider(ps)
				if err != nil {
					die(err)
				}
				fmt.Fprintf(os.Stderr, "exit gateway: node %d (%s) perf=%.2f version=%s, picked at random from %d\n", p.NodeID, p.Country, p.Performance, p.Version, len(ps))
				prov = p.Address
			}
			if _, err := nym.Init(bin, *id, prov, *port, !*noAnon, *gw); err != nil {
				die(err)
			}
			fmt.Fprintf(os.Stderr, "initialised %q -> %s\n", *id, prov)
		}
		st, err := nym.Start(bin, *id, 3*time.Minute)
		if err != nil {
			die(err)
		}
		show(st)
	case "start":
		bin, err := nym.FindBinary(*binary)
		if err != nil {
			die(err)
		}
		st, err := nym.Start(bin, *id, 3*time.Minute)
		if err != nil {
			die(err)
		}
		show(st)
	case "stop":
		if nym.Stop(*id) {
			fmt.Println("stopped")
		} else {
			fmt.Println("not running")
		}
	case "status":
		st, err := nym.GetStatus(*id)
		if err != nil {
			die(err)
		}
		show(st)
	case "exits":
		ps, err := nym.ListExitProviders(nym.NymAPI, *minPerf)
		if err != nil {
			die(err)
		}
		if *jsonOut {
			show(ps)
			return
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "NODE\tCC\tPERF\tVERSION\tNETWORK REQUESTER")
		for _, p := range ps {
			fmt.Fprintf(tw, "%d\t%s\t%.2f\t%s\t%s\n", p.NodeID, p.Country, p.Performance, p.Version, p.Address)
		}
		tw.Flush()
		fmt.Fprintf(os.Stderr, "%d active exit gateways with an exit-policy network requester\n", len(ps))
	default:
		die(fmt.Errorf("unknown nym subcommand %q", sub))
	}
}

func showIP(ctx context.Context, c *common, d *mixnet.Dialer, noLookup bool) {
	const svc = "https://api.ipify.org"
	req, _ := newGet(ctx, svc)
	t0 := time.Now()
	resp, err := d.HTTPClient(c.timeout).Do(req)
	if err != nil {
		die(fmt.Errorf("GET %s over the mixnet: %w", svc, err))
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	resp.Body.Close()
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		die(fmt.Errorf("unexpected answer from %s: HTTP %d %q", svc, resp.StatusCode, ip))
	}
	el := time.Since(t0).Seconds()
	res := map[string]any{"service": svc, "seen_ip": ip, "elapsed_s": el}
	human := fmt.Sprintf("%s sees this request coming from %s (%.2fs over the mixnet)", svc, ip, el)
	if !noLookup {
		// nym-api is queried directly, like nym-socks5-client does for topology.
		n, err := nym.NodeByIP(nym.NymAPI, ip)
		switch {
		case err != nil:
			human += fmt.Sprintf("\n(could not check the nym-api node list: %v)", err)
		case n == nil:
			human += "\nWARNING: that IP is not in the nym-api node list"
			res["nym_node"] = nil
		default:
			res["nym_node"] = map[string]any{"node_id": n.NodeID, "exit_nr": n.Description.Roles.ExitNR, "location": n.Description.Aux.Location}
			human += fmt.Sprintf("\n= Nym node %d (%s, exit gateway: %v) in the nym-api node list", n.NodeID, n.Description.Aux.Location, n.Description.Roles.ExitNR)
		}
	}
	c.out(res, human)
}

func newGet(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	return req, nil
}
