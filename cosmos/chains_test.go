package cosmos

import "testing"

func TestBech32(t *testing.T) {
	for addr, hrp := range map[string]string{
		"n10c7h7wae558d5zejysktztheca6t3gc3vrnep9":      "n",
		"cosmos1qypqxpq9qcrsszg2pvxq6rs0zqg3yyc5lzv7xu": "cosmos",
	} {
		got, err := Bech32HRP(addr)
		if err != nil || got != hrp {
			t.Errorf("%s: %q %v", addr, got, err)
		}
	}
	for _, bad := range []string{"n10c7h7wae558d5zejysktztheca6t3gc3vrnep8", "n10c7h7wae558d5zejysktztheca6t3gc3vrnEp9", "hello", "n1bbbbbbb"} {
		if _, err := Bech32HRP(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestRESTLabel(t *testing.T) {
	for in, want := range map[string]string{
		"/cosmos/bank/v1beta1/balances/n10c7h7wae558d5zejysktztheca6t3gc3vrnep9": "/cosmos/bank/v1beta1/balances/*",
		"/cosmos/base/tendermint/v1beta1/blocks/latest":                          "/cosmos/base/tendermint/v1beta1/blocks/latest",
		"/cosmos/tx/v1beta1/txs/0A1B2C":                                          "/cosmos/tx/v1beta1/txs/*",
		"/cosmos/base/tendermint/v1beta1/blocks/12345":                           "/cosmos/base/tendermint/v1beta1/blocks/*",
		"/ibc/apps/transfer/v1/denom_traces/ibc%2F27394F":                        "/ibc/apps/transfer/v1/denom_traces/*",
	} {
		if got := RESTLabel(in); got != want {
			t.Errorf("RESTLabel(%q) = %q, want %q", in, got, want)
		}
	}
	if got := RPCLabel("GET", "/abci_query", nil); got != "abci_query" {
		t.Error(got)
	}
	if got := RPCLabel("POST", "/", []byte(`[{"method":"status"},{"method":"n1secretaddr"}]`)); got != "[status,*]" {
		t.Error(got)
	}
}

func TestAmountsAndNames(t *testing.T) {
	if s := Display("1500000", "unym"); s != "1.5 NYM" {
		t.Error(s)
	}
	if s := Display("7", "ibc/ABC"); s != "7 ibc/ABC" {
		t.Error(s)
	}
	if s := FormatAmount("120000000", 6); s != "120" {
		t.Error(s)
	}
	for _, n := range []string{"rpc", "rest", "cosmos", "Bad Name", ""} {
		if ValidName(n) == nil {
			t.Errorf("%q accepted as chain name", n)
		}
	}
	if ValidName("juno") != nil {
		t.Error("juno refused")
	}
}
