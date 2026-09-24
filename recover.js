const dhive = require("@hiveio/dhive");

const sym = String(process.argv[2] || "").trim().toUpperCase();
if (!sym){ console.error("usage: node recover.js ITEM"); process.exit(1); }

function reqEnv(name){ const v = process.env[name]; if (!v){ console.error(`missing env: ${name}`); process.exit(1); } return v; }
const username = reqEnv("SVC_USER");
const activeKey = dhive.PrivateKey.fromString(reqEnv("SVC_KEY"));

const RPC_NODES = [
  "https://api.hive-engine.com/rpc/contracts",
  "https://api2.hive-engine.com/rpc/contracts",
  "https://herpc.dtools.dev/contracts",
  "https://engine.rishipanthee.com/contracts",
  "https://enginerpc.com/contracts",
];
const CHAIN_NODES = ["https://api.hive.blog", "https://api.deathwing.me", "https://anyx.io", "https://hive-api.arcange.eu"];
const CHAIN_ID = "ssc-mainnet-hive";
const MIN_BROADCAST_GAP_MS = 6000;

const client = new dhive.Client(CHAIN_NODES, { timeout: 15000 });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = (m) => console.log(`[${new Date().toISOString().slice(0, 19).replace("T", " ")}] ${m}`);

let rpcIdx = 0;
async function rpcFind(contract, table, query, limit){
  const body = { jsonrpc: "2.0", id: Date.now(), method: "find", params: { contract, table, query, limit: limit || 10, offset: 0 } };
  let lastErr;
  for (let i = 0; i < RPC_NODES.length * 2; i++){
    const url = RPC_NODES[rpcIdx % RPC_NODES.length];
    try {
      const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      const data = await res.json();
      if (data.error) throw new Error(data.error.message);
      return data.result || [];
    } catch (e){ lastErr = e; rpcIdx++; await sleep(400); }
  }
  throw lastErr;
}

let lastBroadcastAt = 0;
async function broadcastGate(){
  const wait = lastBroadcastAt + MIN_BROADCAST_GAP_MS - Date.now();
  if (wait > 0){ log(`wait ${(wait / 1000).toFixed(1)}s`); await sleep(wait); }
  lastBroadcastAt = Date.now();
}
async function broadcastCustomJson(json, label){
  await broadcastGate();
  log(`send ${label}`);
  const op = ["custom_json", { required_auths: [username], required_posting_auths: [], id: CHAIN_ID, json: JSON.stringify(json) }];
  const result = await client.broadcast.sendOperations([op], activeKey);
  log(`sent. tx: ${result.id}`);
  return result;
}

function roundDown(qty, precision){ const f = Math.pow(10, precision); return Math.floor(qty * f) / f; }

async function main(){
  log(`${sym}: checking balance`);
  const bal = await rpcFind("tokens", "balances", { account: username, symbol: sym }, 1);
  const balance = bal.length ? parseFloat(bal[0].balance) : 0;
  if (!(balance > 0)){ log(`${sym}: nothing to do`); return; }

  const precRows = await rpcFind("tokens", "tokens", { symbol: sym }, 1);
  const precision = (precRows[0] && typeof precRows[0].precision === "number") ? precRows[0].precision : 8;
  const q = roundDown(balance, precision);
  if (!(q > 0)){ log(`${sym}: amount below precision`); return; }
  log(`${sym}: ${q}, trying path 1`);

  try {
    const poolRows = await rpcFind("marketpools", "pools", { tokenPair: sym + ":SWAP.HIVE" }, 1);
    const flipRows = poolRows.length ? poolRows : await rpcFind("marketpools", "pools", { tokenPair: "SWAP.HIVE:" + sym }, 1);
    if (flipRows.length){
      await broadcastCustomJson({
        contractName: "marketpools", contractAction: "swapTokens",
        contractPayload: { tokenPair: flipRows[0].tokenPair, tokenSymbol: sym, tokenAmount: q.toFixed(precision), tradeType: "exactInput", minAmountOut: "0.00000001" },
      }, `path1 ${q} ${sym}`);
      log(`${sym}: path 1 done`);
      return;
    }
    log(`${sym}: path 1 unavailable, trying path 2`);
  } catch (e){ log(`${sym}: path 1 failed (${e.message}), trying path 2`); }

  const buyRows = await rpcFind("market", "buyBook", { symbol: sym }, 200);
  const buyBook = buyRows.map(r => ({ price: parseFloat(r.price), quantity: parseFloat(r.quantity) }))
    .filter(l => l.price > 0 && l.quantity > 0).sort((a, b) => b.price - a.price);
  if (!buyBook.length){ log(`${sym}: no path available — manual check needed`); return; }
  const price = (buyBook[0].price * 0.97).toFixed(8);
  await broadcastCustomJson({
    contractName: "market", contractAction: "sell",
    contractPayload: { symbol: sym, quantity: q.toFixed(precision), price },
  }, `path2 ${q} ${sym} @ ${price}`);
  log(`${sym}: path 2 done`);
}

main().catch(e => { console.error("error:", e.message); process.exit(1); });
