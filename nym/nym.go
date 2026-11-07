// Package nym sets up and runs the official nym-socks5-client release binary.
//
// cosmosmix does not reimplement the mixnet. Nym's own client does the Sphinx
// packet construction, cover traffic, Poisson delays and gateway handling, and
// exposes a SOCKS5 port that cosmosmix talks to.
package nym

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	NymAPI    = "https://validator.nymtech.net/api"
	DefaultID = "cosmosmix"
	// DefaultPort is 1080, Nym's own default for the SOCKS5 client.
	DefaultPort = 1080
)

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// Home is ~/.nym, where nym-socks5-client keeps its clients. (The release
// binary ignores $NYM_HOME, so cosmosmix does too, or it would look for the
// config in a place the client never wrote to.)
func Home() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".nym")
}

// ConfigPath of the socks5 client with this id.
func ConfigPath(id string) string {
	return filepath.Join(Home(), "socks5-clients", id, "config", "config.toml")
}

// StateDir is $XDG_STATE_HOME/cosmosmix (pid files, logs, local cert).
func StateDir() string {
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".local", "state")
	}
	d := filepath.Join(base, "cosmosmix")
	_ = os.MkdirAll(d, 0o700)
	return d
}

// FindBinary locates nym-socks5-client: explicit path, $COSMOSMIX_NYM_SOCKS5, then $PATH.
func FindBinary(explicit string) (string, error) {
	cands := []string{explicit, os.Getenv("COSMOSMIX_NYM_SOCKS5")}
	if p, err := exec.LookPath("nym-socks5-client"); err == nil {
		cands = append(cands, p)
	}
	for _, c := range cands {
		if c == "" {
			continue
		}
		if st, err := os.Stat(c); err == nil && !st.IsDir() && st.Mode()&0o111 != 0 {
			return c, nil
		}
	}
	return "", errors.New("nym-socks5-client not found. Download it from https://github.com/nymtech/nym/releases " +
		"(nym-binaries-*), check its sha256 against the release's hashes.json, chmod +x it, then pass " +
		"--binary PATH or set COSMOSMIX_NYM_SOCKS5")
}

// ExitProvider is a network requester on an active exit gateway.
type ExitProvider struct {
	NodeID      int      `json:"node_id"`
	Country     string   `json:"country"`
	Performance float64  `json:"performance"`
	Version     string   `json:"version"`
	Address     string   `json:"address"`
	IPs         []string `json:"ips"`
}

func getJSON(url string, v any) error {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	c := &http.Client{Timeout: 90 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// num accepts a JSON number or a numeric string.
type num float64

func (n *num) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	f, err := strconv.ParseFloat(s, 64)
	*n = num(f)
	return err
}

// ListExitProviders asks the nym-api for network requesters on currently
// active exit gateways that enforce the exit policy.
//
// This queries the public nym-api directly (not through the mixnet), exactly
// as nym-socks5-client itself does to fetch the network topology. It reveals
// that you use Nym, nothing about your wallet.
func ListExitProviders(api string, minPerf float64) ([]ExitProvider, error) {
	var skimmed struct {
		Nodes struct {
			Data []struct {
				NodeID      int    `json:"node_id"`
				Role        string `json:"role"`
				Performance num    `json:"performance"`
			} `json:"data"`
		} `json:"nodes"`
	}
	if err := getJSON(api+"/v1/unstable/nym-nodes/skimmed/exit-gateways/all", &skimmed); err != nil {
		return nil, err
	}
	desc, err := Described(api)
	if err != nil {
		return nil, err
	}
	var out []ExitProvider
	for _, n := range skimmed.Nodes.Data {
		if n.Role != "ExitGateway" || float64(n.Performance) < minPerf {
			continue
		}
		d, ok := desc[n.NodeID]
		if !ok || d.Description.NetworkRequester == nil || d.Description.NetworkRequester.Address == "" || !d.Description.NetworkRequester.UsesExitPolicy {
			continue
		}
		out = append(out, ExitProvider{
			NodeID:      n.NodeID,
			Country:     d.Description.Aux.Location,
			Performance: float64(n.Performance),
			Version:     d.Description.Build.Version,
			Address:     d.Description.NetworkRequester.Address,
			IPs:         d.Description.Host.IPs,
		})
	}
	return out, nil
}

// DescribedNode is the subset of /v1/nym-nodes/described we use.
type DescribedNode struct {
	NodeID      int `json:"node_id"`
	Description struct {
		Host struct {
			IPs []string `json:"ip_address"`
		} `json:"host_information"`
		NetworkRequester *struct {
			Address        string `json:"address"`
			UsesExitPolicy bool   `json:"uses_exit_policy"`
		} `json:"network_requester"`
		Aux struct {
			Location string `json:"location"`
		} `json:"auxiliary_details"`
		Build struct {
			Version string `json:"build_version"`
		} `json:"build_information"`
		Roles struct {
			ExitNR bool `json:"exit_nr"`
		} `json:"declared_role"`
	} `json:"description"`
}

// Described returns every node the nym-api describes, keyed by node id.
func Described(api string) (map[int]DescribedNode, error) {
	var d struct {
		Data []DescribedNode `json:"data"`
	}
	if err := getJSON(api+"/v1/nym-nodes/described?per_page=5000", &d); err != nil {
		return nil, err
	}
	m := make(map[int]DescribedNode, len(d.Data))
	for _, n := range d.Data {
		m[n.NodeID] = n
	}
	return m, nil
}

// NodeByIP finds which Nym node announces ip (used to show that a server saw
// a Nym exit gateway's address).
func NodeByIP(api, ip string) (*DescribedNode, error) {
	desc, err := Described(api)
	if err != nil {
		return nil, err
	}
	for _, n := range desc {
		for _, a := range n.Description.Host.IPs {
			if a == ip {
				n := n
				return &n, nil
			}
		}
	}
	return nil, nil
}

// PickProvider chooses at random (spreads load, keeps the choice unpredictable).
func PickProvider(ps []ExitProvider) (ExitProvider, error) {
	if len(ps) == 0 {
		return ExitProvider{}, errors.New("nym-api lists no active exit gateway with a network requester")
	}
	return ps[rand.IntN(len(ps))], nil
}

var (
	reCredReq = regexp.MustCompile(`(?m)^credential_requests_database\s*=`)
	reCredDB  = regexp.MustCompile(`(?m)^credentials_database = '(.*)/credentials_database\.db'\s*$`)
)

// PatchConfig works around nym-socks5-client 1.1.86 (v2026.19) writing a
// config it can't read: the template omits credential_requests_database while
// the loader requires it, so `run` fails right after `init` with
// "missing field `credential_requests_database`". Adds the default path.
func PatchConfig(id string) (bool, error) {
	p := ConfigPath(id)
	b, err := os.ReadFile(p)
	if err != nil {
		return false, err
	}
	changed, out := PatchConfigText(string(b))
	if !changed {
		return false, nil
	}
	return true, os.WriteFile(p, []byte(out), 0o600)
}

// PatchConfigText is PatchConfig on a string (for tests).
func PatchConfigText(text string) (bool, string) {
	if reCredReq.MatchString(text) {
		return false, text
	}
	loc := reCredDB.FindStringSubmatchIndex(text)
	if loc == nil {
		return false, text
	}
	dir := text[loc[2]:loc[3]]
	line := "\ncredential_requests_database = '" + dir + "/credential_requests.db'"
	return true, text[:loc[1]] + line + text[loc[1]:]
}

// Init runs `nym-socks5-client init` with reply SURBs on by default (the exit
// then never learns this client's mixnet address).
func Init(binary, id, provider string, port int, anonymous bool, gateway string) (string, error) {
	args := []string{"init", "--id", id, "--provider", provider, "--port", strconv.Itoa(port),
		"--use-reply-surbs", strconv.FormatBool(anonymous)}
	if gateway != "" {
		args = append(args, "--gateway", gateway)
	}
	cmd := exec.Command(binary, args...)
	out, err := cmd.CombinedOutput()
	text := ansi.ReplaceAllString(string(out), "")
	if err != nil {
		return text, fmt.Errorf("nym-socks5-client init failed: %v\n%s", err, tail(text, 2000))
	}
	_, err = PatchConfig(id)
	return text, err
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func pidFile(id string) string { return filepath.Join(StateDir(), id+".pid") }
func LogFile(id string) string { return filepath.Join(StateDir(), id+".log") }

// RunningPID returns the pid of the client started by cosmosmix, or 0.
func RunningPID(id string) int {
	b, err := os.ReadFile(pidFile(id))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0
	}
	if syscall.Kill(pid, 0) != nil {
		return 0
	}
	return pid
}

// Start launches `nym-socks5-client run` in the background and waits until it
// reports "Client startup finished".
func Start(binary, id string, wait time.Duration) (Status, error) {
	if RunningPID(id) != 0 {
		return GetStatus(id)
	}
	if _, err := PatchConfig(id); err != nil {
		return Status{}, err
	}
	logf, err := os.Create(LogFile(id))
	if err != nil {
		return Status{}, err
	}
	cmd := exec.Command(binary, "run", "--id", id)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return Status{}, err
	}
	_ = os.WriteFile(pidFile(id), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			b, _ := os.ReadFile(LogFile(id))
			return Status{}, fmt.Errorf("nym-socks5-client exited early:\n%s", tail(ansi.ReplaceAllString(string(b), ""), 2000))
		case <-time.After(500 * time.Millisecond):
		}
		b, _ := os.ReadFile(LogFile(id))
		if strings.Contains(string(b), "Client startup finished") {
			return GetStatus(id)
		}
	}
	return Status{}, fmt.Errorf("nym-socks5-client not ready after %s; see %s", wait, LogFile(id))
}

// Stop terminates the client started by cosmosmix.
func Stop(id string) bool {
	pid := RunningPID(id)
	if pid == 0 {
		return false
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	_ = os.Remove(pidFile(id))
	return true
}

// Status of a configured client.
type Status struct {
	ID               string `json:"id"`
	PID              int    `json:"pid"`
	SOCKS5           string `json:"socks5"`
	Provider         string `json:"provider"`
	AnonymousReplies bool   `json:"anonymous_replies"`
	ClientAddress    string `json:"client_address"`
	Gateway          string `json:"gateway"`
	Log              string `json:"log"`
}

var (
	reBind     = regexp.MustCompile(`(?m)^bind_address\s*=\s*'([^']*)'`)
	reProvider = regexp.MustCompile(`(?m)^provider_mix_address\s*=\s*'([^']*)'`)
	reAnon     = regexp.MustCompile(`(?m)^send_anonymously\s*=\s*(true|false)`)
	reAddr     = regexp.MustCompile(`The address of this client is: (\S+)`)
)

// GetStatus reads the client's config and log.
func GetStatus(id string) (Status, error) {
	b, err := os.ReadFile(ConfigPath(id))
	if err != nil {
		return Status{}, fmt.Errorf("client %q is not initialised (run `cosmosmix nym setup`): %w", id, err)
	}
	cfg := string(b)
	st := Status{ID: id, PID: RunningPID(id), Log: LogFile(id)}
	if m := reBind.FindStringSubmatch(cfg); m != nil {
		st.SOCKS5 = m[1]
	}
	if m := reProvider.FindStringSubmatch(cfg); m != nil {
		st.Provider = m[1]
	}
	if m := reAnon.FindStringSubmatch(cfg); m != nil {
		st.AnonymousReplies = m[1] == "true"
	}
	if lb, err := os.ReadFile(LogFile(id)); err == nil {
		ms := reAddr.FindAllStringSubmatch(ansi.ReplaceAllString(string(lb), ""), -1)
		if len(ms) > 0 {
			st.ClientAddress = ms[len(ms)-1][1]
			if i := strings.LastIndex(st.ClientAddress, "@"); i >= 0 {
				st.Gateway = st.ClientAddress[i+1:]
			}
		}
	}
	return st, nil
}
