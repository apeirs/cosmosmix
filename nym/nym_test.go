package nym

import (
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
