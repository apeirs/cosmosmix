package nym

import (
	"path/filepath"
	"strings"
	"testing"
)

const cfg = `[storage_paths]
keys_dir = '/h/.nym/socks5-clients/cosmosmix/data'
credentials_database = '/h/.nym/socks5-clients/cosmosmix/data/credentials_database.db'
reply_surb_database = '/h/.nym/socks5-clients/cosmosmix/data/persistent_reply_store.sqlite'
`

func TestPatchConfigText(t *testing.T) {
	changed, out := PatchConfigText(cfg)
	if !changed || !strings.Contains(out, "credential_requests_database = '/h/.nym/socks5-clients/cosmosmix/data/credential_requests.db'\n") {
		t.Fatalf("not patched:\n%s", out)
	}
	if changed, again := PatchConfigText(out); changed || again != out {
		t.Fatal("patch is not idempotent")
	}
}

// nym-socks5-client ignores $NYM_HOME and always writes under ~/.nym, so the
// config path must not follow NYM_HOME either.
func TestConfigPathIgnoresNymHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("NYM_HOME", filepath.Join(home, "elsewhere"))
	want := filepath.Join(home, ".nym", "socks5-clients", "cosmosmix", "config", "config.toml")
	if got := ConfigPath("cosmosmix"); got != want {
		t.Fatalf("ConfigPath = %s, want %s", got, want)
	}
}
