// Unmodified @cosmjs client pointed at the cosmosmix gateway. It knows nothing
// about Nym: it only gets a different RPC URL.
import { StargateClient } from "@cosmjs/stargate";
import { Comet38Client, Tendermint37Client, WebsocketClient } from "@cosmjs/tendermint-rpc";

const G = "http://127.0.0.1:26657";
const t0 = Date.now();
const secs = () => ((Date.now() - t0) / 1000).toFixed(1) + "s";

for (const [chain, addr] of [
  ["nyx", "n10c7h7wae558d5zejysktztheca6t3gc3vrnep9"],
  ["osmosis", "osmo1jv65s3grqf6v6jl3dp4t6c9t9rk99cd80yhvld"],
]) {
  const c = await StargateClient.connect(`${G}/${chain}/rpc`);
  const id = await c.getChainId();
  const h = await c.getHeight();
  const denom = chain === "nyx" ? "unym" : "uosmo";
  const bal = await c.getBalance(addr, denom);
  console.log(`[${secs()}] StargateClient.connect("${G}/${chain}/rpc"): chainId=${id} height=${h} ${addr.slice(0, 10)}… ${bal.amount} ${bal.denom}`);
  if (chain === "nyx") {
    try {
      await c.broadcastTxSync(Uint8Array.from([0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef]));
      console.log(`[${secs()}] broadcastTxSync(junk): accepted?!`);
    } catch (e) {
      console.log(`[${secs()}] broadcastTxSync(8 junk bytes) rejected by the node: ${String(e.message).slice(0, 160)}`);
    }
  }
  c.disconnect();
}

// CometBFT websocket subscription through the gateway (cosmjs appends /websocket itself).
const ws = new WebsocketClient(`ws://127.0.0.1:26657/nyx/rpc`, (err) => console.log("ws error", err?.message));
let tm;
try {
  tm = await Comet38Client.create(ws);
} catch {
  tm = await Tendermint37Client.create(ws);
}
const sub = tm.subscribeNewBlockHeader();
let n = 0;
await new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error("no NewBlockHeader within 120 s")), 120000);
  sub.addListener({
    next: (hd) => {
      console.log(`[${secs()}] websocket NewBlockHeader via ws://127.0.0.1:26657/nyx/rpc/websocket: height=${hd.height} time=${hd.time.toISOString()}`);
      if (++n === 3) { clearTimeout(timer); resolve(); }
    },
    error: (e) => { clearTimeout(timer); reject(e); },
    complete: () => {},
  });
});
tm.disconnect();
console.log(`[${secs()}] done`);
process.exit(0);
