# cosmosmix

Cosmos SDK chain traffic (CometBFT RPC and the REST/LCD API) through the [Nym mixnet](https://nym.com).
One Go binary, no dependencies outside the standard library, MIT.

```
cosmosmix nym setup                      # starts nym-socks5-client on a random active exit gateway
cosmosmix serve                          # local gateway: http://127.0.0.1:26657/<chain>/rpc and /<chain>/rest
cosmosmix status --chain osmosis         # latest block height and time
cosmosmix balance n1... --chain nyx      # bank balances (REST)
cosmosmix broadcast TX_BASE64            # forward an already signed tx; cosmosmix holds no keys
cosmosmix probe --chain all              # which public nodes answer Nym exit traffic, and how fast
cosmosmix ip                             # the IP that nodes see (a Nym exit gateway)
```

Built-in chains: `nyx` (Nym's own chain), `cosmoshub`, `osmosis`, `celestia`, each with three public RPC and three
public REST endpoints, tried in order. Add any other chain with
`--add juno=https://rpc1|https://rpc2 --add-rest juno=https://lcd1`.

## Why

A wallet asks its RPC provider about your addresses all the time: balances, delegations, account numbers before
signing, and finally `broadcast_tx_sync`. The provider sees all of that next to your IP, so it learns which IP holds
which addresses, and which IP sent which transaction. Keplr, Leap, cosmjs apps, `nym-cli` and `gaiad`/`osmosisd` all
let you set a custom RPC or REST URL, but almost none of them have a proxy setting. `cosmosmix serve` gives you
plain `http://127.0.0.1` URLs to put in those fields. The provider then sees a Nym exit gateway, not you.

## Use

```
export COSMOSMIX_NYM_SOCKS5=/path/to/nym-socks5-client    # official Nym release binary
go install github.com/apeirs/cosmosmix/cmd/cosmosmix@latest
cosmosmix nym setup            # add --port 1086 if something else already uses 1080; then set COSMOSMIX_PROXY=127.0.0.1:1086
cosmosmix serve &
```

The gateway has one port and two route prefixes per chain:

| What | URL |
|---|---|
| CometBFT RPC (GET URI form, POST JSON-RPC, `/websocket`) | `http://127.0.0.1:26657/<chain>/rpc` |
| Cosmos REST / LCD | `http://127.0.0.1:26657/<chain>/rest` |
| RPC of the default chain (`--default nyx`), at the root | `http://127.0.0.1:26657` |
| Optional bare REST listener (`--rest-listen 127.0.0.1:1317`) | `http://127.0.0.1:1317/<chain>/...`, root = default chain |

Put these into the RPC and REST fields of your client:

- **cosmjs** (tested): `StargateClient.connect("http://127.0.0.1:26657/nyx/rpc")`. Websocket subscriptions via
  `new WebsocketClient("ws://127.0.0.1:26657/nyx/rpc")` work too; cosmjs appends `/websocket` itself
  (`examples/cosmjs-client.mjs`).
- **curl** (tested): `curl http://127.0.0.1:26657/osmosis/rpc/status`,
  `curl http://127.0.0.1:26657/cosmoshub/rest/cosmos/bank/v1beta1/balances/cosmos1...`.
- **Keplr / Leap**: use `rpc: http://127.0.0.1:26657/nyx/rpc` and `rest: http://127.0.0.1:26657/nyx/rest` in the
  wallet's endpoint settings or in a chain suggestion. Requests from browser extensions are allowed. I have not
  yet tested this in the wallet UIs.
- **gaiad / osmosisd / nyxd** (not yet tested): `--node tcp://127.0.0.1:26657` reaches the default chain's RPC, so
  start the gateway with `--default cosmoshub` (or whichever chain the binary is for).

`broadcast_tx_sync`, `abci_query` and every other RPC method pass through untouched. The GET query string and the
POST body are forwarded byte for byte. A node's own answer (a 4xx, a CheckTx rejection) comes back as it is.

## How it uses the mixnet

- It drives the official `nym-socks5-client` and does no mixing itself. `nym setup` picks a random active exit
  gateway from the Nym API (one that runs an exit-policy network requester), with reply SURBs on, so the exit
  doesn't learn your mixnet address.
- Each upstream connection is a SOCKS5 stream through the entry gateway, three mix layers and the exit. TLS runs end
  to end with the node. Keep-alive reuses a stream, so a wallet polling `/status` doesn't pay a new handshake every
  time.
- Node hostnames are resolved by DNS-over-HTTPS *through the mixnet*, then connected by IPv4. The local resolver
  never learns which provider you use, and a broken IPv6 route at the exit can't stall a request.
- There is no clearnet fallback. With the mixnet client down, the gateway answers 502 (a JSON-RPC error for RPC, a
  gRPC-style `code 14` for REST) and the CLI refuses to run.
- Failover: a timeout, a connection error, HTTP 5xx or 429 moves on to the next upstream. Redirects are followed
  over the mixnet, and only to https.
- Nothing from the local caller is forwarded. Only a fixed `User-Agent`, `Accept` and `Content-Type` go upstream
  (no `Origin`, `Cookie`, `Referer`, `Authorization`, `X-Forwarded-For` or language headers). Only `Content-Type`
  comes back, never `Set-Cookie` or `Location`.
- The gateway listens on loopback and rejects a foreign `Host` header (DNS-rebinding guard). It also rejects any
  web-page `Origin`, so a site you visit can't use your gateway. Browser extensions and non-browser clients are
  allowed; `--allow-origin` adds more.
- Upstreams must be https. Logs name the chain, the RPC method or a REST route template
  (`/cosmos/bank/v1beta1/balances/*`), the upstream host and the timing. Addresses, tx bytes, query data and
  heights are never logged, not even in error messages.
- cosmosmix never holds a key. `broadcast` only forwards a transaction that is already signed.

## Limits (read these)

- **It hides your IP, not your addresses.** The node still sees which addresses you query and which transactions
  you send, and everything on chain stays public. cosmosmix removes the IP link, not the on-chain one.
- **One exit IP per client.** All requests leave through the same exit gateway, so a provider can link your
  requests to each other ("one user asked about these five addresses"). For unlinkable sessions, run separate
  client identities (`cosmosmix nym setup --id other --port 1087`).
- **Latency is seconds.** In my live run a request on a fresh connection took 4 to 10 s, and one on a reused stream took
  about 1 s. That is fine for balances, staking and broadcasts, and slow for an app that fires hundreds of queries.
- **cosmjs websocket clients give up after a fixed 10 s.** A websocket through the mixnet opened in 3 to 7 s in my
  tests. On a slow path it can miss that limit, and then cosmjs reports a connection timeout.
- **Some public nodes may block or rate-limit Nym exits.** In my run all 24 built-in upstreams answered. Run
  `cosmosmix probe` to see what works today.
- gRPC and gRPC-web are not proxied yet, only CometBFT RPC and REST. The wallet's own other traffic (price feeds,
  token lists, chain registries) isn't covered either.

## Tests

`go vet ./... && go test ./...` runs offline against a fake mixnet: a recording SOCKS5 proxy, a DoH server and TLS
nodes. The tests check that:

- every connection goes through the proxy, and the only name lookups are DoH over it;
- the GET URI form and POST JSON-RPC pass through byte for byte;
- REST routes and the root default chain work;
- failover covers 5xx and 429;
- redirects are followed only to https;
- request and response headers are stripped;
- the Host and Origin guards hold;
- with no proxy there is a 502 and no fallback;
- only https upstreams are accepted;
- no params reach the logs, on success or failure;
- the websocket splice works.

Live logs from the real Nym mainnet mixnet are in `examples/`.
