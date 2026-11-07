// Package cosmos talks to public Cosmos SDK nodes (CometBFT RPC and the
// REST/LCD API) through the Nym mixnet.
package cosmos

import (
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"sort"
	"strings"
)

// Chain is a network with public, keyless RPC and REST endpoints (tried in order).
type Chain struct {
	Name    string
	ChainID string
	Prefix  string // bech32 account prefix
	Denom   string // base staking/fee denom
	Symbol  string
	Exp     int // decimals of Symbol over Denom
	RPC     []string
	REST    []string
}

// Chains are the built-in networks. Any other one can be added with --add / --add-rest.
var Chains = []Chain{
	{"nyx", "nyx", "n", "unym", "NYM", 6,
		[]string{"https://rpc.nymtech.net", "https://nym-rpc.polkachu.com", "https://rpc.nyx.nodes.guru"},
		[]string{"https://api.nymtech.net", "https://nym-api.polkachu.com", "https://api.nyx.nodes.guru"}},
	{"cosmoshub", "cosmoshub-4", "cosmos", "uatom", "ATOM", 6,
		[]string{"https://cosmos-rpc.publicnode.com", "https://rpc.lavenderfive.com/cosmoshub", "https://rpc-cosmoshub.ecostake.com"},
		[]string{"https://cosmos-rest.publicnode.com", "https://rest.lavenderfive.com/cosmoshub", "https://rest-cosmoshub.ecostake.com"}},
	{"osmosis", "osmosis-1", "osmo", "uosmo", "OSMO", 6,
		[]string{"https://osmosis-rpc.publicnode.com", "https://rpc.osmosis.zone", "https://rpc.lavenderfive.com/osmosis"},
		[]string{"https://osmosis-rest.publicnode.com", "https://lcd.osmosis.zone", "https://osmosis-api.polkachu.com"}},
	{"celestia", "celestia", "celestia", "utia", "TIA", 6,
		[]string{"https://celestia-rpc.publicnode.com", "https://public-celestia-rpc.numia.xyz", "https://rpc.lavenderfive.com/celestia"},
		[]string{"https://celestia-rest.publicnode.com", "https://public-celestia-lcd.numia.xyz", "https://rest.lavenderfive.com/celestia"}},
}

// reserved names can't be chain names: they are the first path segment of
// RPC or REST routes served at the gateway root for the default chain.
var reserved = map[string]bool{"rpc": true, "rest": true, "websocket": true, "cosmos": true, "ibc": true,
	"cosmwasm": true, "status": true, "health": true, "all": true, "custom": true}

// Registry holds the built-in chains plus custom ones.
type Registry struct {
	m map[string]*Chain
}

// NewRegistry returns a registry with the built-in chains.
func NewRegistry() *Registry {
	r := &Registry{m: map[string]*Chain{}}
	for i := range Chains {
		c := Chains[i]
		c.RPC = append([]string(nil), c.RPC...)
		c.REST = append([]string(nil), c.REST...)
		r.m[c.Name] = &c
	}
	return r
}

// ValidName reports whether s can name a chain in gateway paths.
func ValidName(s string) error {
	if s == "" || len(s) > 40 || strings.Trim(s, "abcdefghijklmnopqrstuvwxyz0123456789-_") != "" {
		return fmt.Errorf("chain name %q: use lowercase letters, digits, - and _", s)
	}
	if reserved[s] {
		return fmt.Errorf("chain name %q is reserved (it is a route at the gateway root)", s)
	}
	return nil
}

// CheckURL accepts only https URLs without credentials, query or fragment.
func CheckURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%s: only https upstreams are allowed", s)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%s: no credentials, query or fragment in upstream URLs", s)
	}
	return nil
}

// Add sets the RPC (rest=false) or REST upstreams of a chain, creating it if new.
// Custom upstreams replace the built-in list of that kind.
func (r *Registry) Add(name string, urls []string, rest bool) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if err := ValidName(name); err != nil {
		return err
	}
	var clean []string
	for _, u := range urls {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u == "" {
			continue
		}
		if err := CheckURL(u); err != nil {
			return err
		}
		clean = append(clean, u)
	}
	if len(clean) == 0 {
		return fmt.Errorf("chain %q: no upstream URL", name)
	}
	c, ok := r.m[name]
	if !ok {
		c = &Chain{Name: name}
		r.m[name] = c
	}
	if rest {
		c.REST = clean
	} else {
		c.RPC = clean
	}
	return nil
}

// ParseAdd parses "name=https://a|https://b" into the registry.
func (r *Registry) ParseAdd(spec string, rest bool) error {
	n, u, ok := strings.Cut(spec, "=")
	if !ok {
		return fmt.Errorf("want name=https://url[|https://url2], got %q", spec)
	}
	return r.Add(n, strings.Split(u, "|"), rest)
}

// Get finds a chain by name (case-insensitive).
func (r *Registry) Get(name string) (*Chain, bool) {
	c, ok := r.m[strings.ToLower(strings.TrimSpace(name))]
	return c, ok
}

// All returns the chains, built-ins first in their usual order, then custom ones by name.
func (r *Registry) All() []*Chain {
	var out []*Chain
	seen := map[string]bool{}
	for _, b := range Chains {
		out = append(out, r.m[b.Name])
		seen[b.Name] = true
	}
	var extra []string
	for n := range r.m {
		if !seen[n] {
			extra = append(extra, n)
		}
	}
	sort.Strings(extra)
	for _, n := range extra {
		out = append(out, r.m[n])
	}
	return out
}

// ---------------------------------------------------------------- bech32

const charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

func polymod(values []byte) uint32 {
	gen := []uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := uint32(1)
	for _, v := range values {
		b := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ uint32(v)
		for i := 0; i < 5; i++ {
			if (b>>uint(i))&1 == 1 {
				chk ^= gen[i]
			}
		}
	}
	return chk
}

// Bech32HRP checks a bech32 address (checksum included) and returns its prefix.
func Bech32HRP(addr string) (string, error) {
	if len(addr) < 8 || len(addr) > 120 {
		return "", errors.New("not a bech32 address (bad length)")
	}
	if strings.ToLower(addr) != addr && strings.ToUpper(addr) != addr {
		return "", errors.New("not a bech32 address (mixed case)")
	}
	addr = strings.ToLower(addr)
	pos := strings.LastIndexByte(addr, '1')
	if pos < 1 || pos+7 > len(addr) {
		return "", errors.New("not a bech32 address")
	}
	hrp, data := addr[:pos], addr[pos+1:]
	values := make([]byte, 0, len(hrp)*2+1+len(data))
	for i := 0; i < len(hrp); i++ {
		values = append(values, hrp[i]>>5)
	}
	values = append(values, 0)
	for i := 0; i < len(hrp); i++ {
		values = append(values, hrp[i]&31)
	}
	for i := 0; i < len(data); i++ {
		d := strings.IndexByte(charset, data[i])
		if d < 0 {
			return "", fmt.Errorf("not a bech32 address (bad character %q)", data[i])
		}
		values = append(values, byte(d))
	}
	if polymod(values) != 1 {
		return "", errors.New("bech32 checksum mismatch")
	}
	return hrp, nil
}

// ---------------------------------------------------------------- amounts

// FormatAmount renders amount (base units) with exp decimals, trimming zeros.
func FormatAmount(amount string, exp int) string {
	n, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return amount
	}
	r := new(big.Rat).SetFrac(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
	s := r.FloatString(exp)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// knownDenoms maps base denoms of the built-in chains to display units.
var knownDenoms = map[string]struct {
	sym string
	exp int
}{
	"unym": {"NYM", 6}, "unyx": {"NYX", 6}, "uatom": {"ATOM", 6}, "uosmo": {"OSMO", 6}, "uion": {"ION", 6}, "utia": {"TIA", 6},
}

// Display converts a coin to a human amount when the denom is known.
func Display(amount, denom string) string {
	if k, ok := knownDenoms[denom]; ok {
		return FormatAmount(amount, k.exp) + " " + k.sym
	}
	return amount + " " + denom
}
