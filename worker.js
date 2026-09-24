const dhive = require("@hiveio/dhive");

const cfg = {
  username: reqEnv("SVC_USER"),
  keyWif: reqEnv("SVC_KEY"),
  minDeltaPct: numEnv("MIN_DELTA_PCT", 0.75),
  maxUnit: numEnv("MAX_UNIT_SIZE", 0),
  tolPct: numEnv("TOLERANCE_PCT", 1),
  minDelta: numEnv("MIN_DELTA_ABS", 0.001),
  itemCooldownSec: numEnv("ITEM_COOLDOWN_SEC", 15),
  stepDelaySec: Math.max(6, numEnv("STEP_DELAY_SEC", 6)),
  itemsEnv: (process.env.ITEMS || "ALL").trim(),
  srcEnabled: boolEnv("SRC_ENABLED", true),
  srcList: listEnv("SRC_LIST"),
  feedEnabled: boolEnv("FEED_ENABLED", true),
  live: boolEnv("LIVE", true),
  maxRuntimeMin: numEnv("MAX_RUNTIME_MIN", 340),
  detail: boolEnv("LOG_DETAIL", false),
};

function reqEnv(name){
  const v = process.env[name];
  if (!v) { console.error(`missing env: ${name}`); process.exit(1); }
  return v;
}
function numEnv(name, def){ const v = parseFloat(process.env[name]); return isFinite(v) ? v : def; }
function boolEnv(name, def){
  const v = process.env[name];
  if (v === undefined || v.trim() === "") return def;
  return /^(1|true|yes)$/i.test(v.trim());
}
function listEnv(name){
  return String(process.env[name] || "").split(",").map(s => s.trim()).filter(Boolean);
}

const RPC_NODES = [
  "https://api.hive-engine.com/rpc/contracts",
  "https://api2.hive-engine.com/rpc/contracts",
  "https://herpc.dtools.dev/contracts",
  "https://engine.rishipanthee.com/contracts",
  "https://enginerpc.com/contracts",
];
const BLOCKCHAIN_RPC_NODES = RPC_NODES.map(u => u.replace(/\/contracts$/, "/blockchain"));
const RPC_TIMEOUT_MS = 15000;
const RPC_MIN_GAP_MS = 350;
const RPC_MIN_GAP_MS_PRIORITY = 120;
const RPC_ROUNDS = 3;

const FEE = 0.0025;
const CHAIN_ID = "ssc-mainnet-hive";
const FIRST_LIST = listEnv("PRIO_LIST").map(s => s.toUpperCase());
const SKIP_SET = new Set(listEnv("SKIP_LIST").map(s => s.toUpperCase()));

const CHUNK_SIZE = 50;
const SIZE_ITERS = 60;
const REFRESH_INTERVAL_MS = 10000;
const SRC_INTERVAL_MS = 2000;
const SRC_HISTORY_LIMIT = 100;
const HISTORY_API = "https://accounts.hive-engine.com/accountHistory";
const FEED_POLL_MS = 1000;
const FEED_MAX_CATCHUP = 5;
const AUTO_DRAIN_MAX_PER_SYMBOL = 25;
const AUTO_DRAIN_MAX_MS = 25000;
const POST_ROUND_SETTLE_MS = 6000;

const MIN_BROADCAST_GAP_MS = 6000;

const sleep = ms => new Promise(r => setTimeout(r, ms));
function log(msg, level, detail){
  const t = new Date().toISOString().replace("T", " ").slice(0, 19);
  const tag = level === "err" ? "E" : level === "ok" ? "ok" : "i";
  const extra = (cfg.detail && detail) ? " | " + detail : "";
  console.log(`[${t}] [${tag}] ${msg}${extra}`);
}
function fmtPct(p){
  const a = Math.abs(p);
  if (a === 0) return "0";
  if (a >= 1) return p.toFixed(2);
  if (a >= 0.01) return p.toFixed(3);
  return p.toFixed(5);
}
function roundDown(qty, precision){
  const f = Math.pow(10, precision);
  return Math.floor(qty * f) / f;
}
function sortItems(list){
  const filtered = list.filter(s => !SKIP_SET.has(s));
  const rest = filtered.filter(s => !FIRST_LIST.includes(s)).sort();
  const front = FIRST_LIST.filter(s => filtered.includes(s));
  return front.concat(rest);
}

let rpcNodeIdx = 0, rpcNodeIdxPriority = 0, bcNodeIdx = 0;
let lastGate = 0, lastGatePriority = 0;
let rpcChain = Promise.resolve();

async function gateGeneric(getLast, setLast, minGapMs){
  for (;;){
    const now = Date.now();
    let last = getLast();
    if (last > now) last = now;
    const wait = last + minGapMs - now;
    if (wait <= 0){ setLast(now); return; }
    await sleep(wait + Math.random() * 80);
  }
}
const rpcGate = () => gateGeneric(() => lastGate, v => (lastGate = v), RPC_MIN_GAP_MS);
const rpcGatePriority = () => gateGeneric(() => lastGatePriority, v => (lastGatePriority = v), RPC_MIN_GAP_MS_PRIORITY);

async function rpcOnce(body, url){
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), RPC_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: ctl.signal,
    });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error.message || "rpc error");
    return data.result || [];
  } finally {
    clearTimeout(timer);
  }
}

function rpcCall(body){
  const run = async () => {
    let lastErr = null;
    const total = RPC_NODES.length * RPC_ROUNDS;
    for (let i = 0; i < total; i++){
      const url = RPC_NODES[rpcNodeIdx % RPC_NODES.length];
      await rpcGate();
      try { return await rpcOnce(body, url); }
      catch (e){
        lastErr = e;
        rpcNodeIdx++;
        const round = Math.floor(i / RPC_NODES.length);
        await sleep(400 * Math.pow(2, round) + Math.random() * 300);
      }
    }
    throw new Error("rpc unreachable: " + (lastErr ? lastErr.message : "unknown"));
  };
  rpcChain = rpcChain.then(run, run);
  return rpcChain;
}

async function rpcCallPriority(body){
  let lastErr = null;
  const total = RPC_NODES.length * 2;
  for (let i = 0; i < total; i++){
    const url = RPC_NODES[rpcNodeIdxPriority % RPC_NODES.length];
    await rpcGatePriority();
    try { return await rpcOnce(body, url); }
    catch (e){
      lastErr = e;
      rpcNodeIdxPriority++;
      await sleep(200 * Math.pow(2, Math.floor(i / RPC_NODES.length)) + Math.random() * 150);
    }
  }
  throw new Error("rpc unreachable (p): " + (lastErr ? lastErr.message : "unknown"));
}

function rpcFind(contract, table, query, limit, offset, priority){
  const body = {
    jsonrpc: "2.0", id: Date.now() + Math.random(), method: "find",
    params: { contract, table, query, limit: limit || 1000, offset: offset || 0 },
  };
  return priority ? rpcCallPriority(body) : rpcCall(body);
}

async function rpcBlockchainCall(body){
  let lastErr = null;
  const total = BLOCKCHAIN_RPC_NODES.length * 2;
  for (let i = 0; i < total; i++){
    const url = BLOCKCHAIN_RPC_NODES[bcNodeIdx % BLOCKCHAIN_RPC_NODES.length];
    try { return await rpcOnce(body, url); }
    catch (e){ lastErr = e; bcNodeIdx++; await sleep(300 + Math.random() * 200); }
  }
  throw new Error("chain rpc unreachable: " + (lastErr ? lastErr.message : "unknown"));
}
async function getLatestBlockNumber(){
  const info = await rpcBlockchainCall({ jsonrpc: "2.0", id: Date.now(), method: "getLatestBlockInfo", params: {} });
  return info && info.blockNumber;
}
async function getBlockInfo(blockNumber){
  return rpcBlockchainCall({ jsonrpc: "2.0", id: Date.now() + Math.random(), method: "getBlockInfo", params: { blockNumber } });
}

async function getVenues(){
  let offset = 0; const limit = 1000; let all = [];
  for (let page = 0; page < 20; page++){
    const rows = await rpcFind("marketpools", "pools", {}, limit, offset);
    all = all.concat(rows);
    if (rows.length < limit) break;
    offset += limit;
  }
  const map = {};
  all.forEach(r => {
    if (!r.tokenPair) return;
    const [base, quote] = r.tokenPair.split(":");
    const baseQty = parseFloat(r.baseQuantity), quoteQty = parseFloat(r.quoteQuantity);
    let sym, price;
    if (quote === "SWAP.HIVE"){ sym = base; price = quoteQty / baseQty; }
    else if (base === "SWAP.HIVE"){ sym = quote; price = baseQty / quoteQty; }
    else return;
    if (!isNaN(price) && price > 0) map[sym] = price;
  });
  return map;
}

async function getVenue(sym, priority){
  let rows = await rpcFind("marketpools", "pools", { tokenPair: sym + ":SWAP.HIVE" }, 1, 0, priority);
  let flip = false;
  if (!rows.length){
    rows = await rpcFind("marketpools", "pools", { tokenPair: "SWAP.HIVE:" + sym }, 1, 0, priority);
    flip = true;
  }
  if (!rows.length) return null;
  const r = rows[0];
  const baseQty = parseFloat(r.baseQuantity), quoteQty = parseFloat(r.quoteQuantity);
  let qtyA, qtyB;
  if (!flip){ qtyB = baseQty; qtyA = quoteQty; } else { qtyA = baseQty; qtyB = quoteQty; }
  if (!(qtyA > 0) || !(qtyB > 0)) return null;
  return { tokenPair: r.tokenPair, qtyA, qtyB };
}

async function fetchBook(sym, side, priority){
  const table = side === "buy" ? "buyBook" : "sellBook";
  const rows = await rpcFind("market", table, { symbol: sym }, 200, 0, priority);
  const levels = rows.map(r => ({ price: parseFloat(r.price), quantity: parseFloat(r.quantity) }))
    .filter(l => l.price > 0 && l.quantity > 0);
  levels.sort((a, b) => (side === "buy" ? b.price - a.price : a.price - b.price));
  return levels;
}

const precisionCache = {};
async function getTokenPrecision(sym){
  if (precisionCache[sym] !== undefined) return precisionCache[sym];
  try {
    const rows = await rpcFind("tokens", "tokens", { symbol: sym }, 1);
    const p = (rows[0] && typeof rows[0].precision === "number") ? rows[0].precision : 8;
    precisionCache[sym] = p;
    return p;
  } catch (e){ return 8; }
}

function walkBookBuy(book, qty){
  let remaining = qty, cost = 0, worstPrice = null;
  for (const lvl of book){
    const take = Math.min(remaining, lvl.quantity);
    if (take <= 0) continue;
    cost += take * lvl.price; worstPrice = lvl.price; remaining -= take;
    if (remaining <= 1e-9) break;
  }
  if (remaining > 1e-9) return null;
  return { cost, worstPrice };
}
function walkBookSell(book, qty){
  let remaining = qty, revenue = 0, worstPrice = null;
  for (const lvl of book){
    const take = Math.min(remaining, lvl.quantity);
    if (take <= 0) continue;
    revenue += take * lvl.price; worstPrice = lvl.price; remaining -= take;
    if (remaining <= 1e-9) break;
  }
  if (remaining > 1e-9) return null;
  return { revenue, worstPrice };
}
function calcOutA(qtyA, qtyB, tokensIn){
  if (tokensIn <= 0) return 0;
  const tEff = tokensIn * (1 - FEE);
  return qtyA * tEff / (qtyB + tEff);
}
function calcInB(qtyA, qtyB, tokensOut){
  if (!(tokensOut > 0) || tokensOut >= qtyB) return Infinity;
  const inEff = tokensOut * qtyA / (qtyB - tokensOut);
  return inEff / (1 - FEE);
}
function peakProfitQty(evalQ, qLo, qHi){
  let lo = qLo, hi = qHi;
  for (let i = 0; i < 80 && hi - lo > qLo * 1e-6; i++){
    const a = lo + (hi - lo) / 3, b = hi - (hi - lo) / 3;
    const ra = evalQ(a), rb = evalQ(b);
    const pa = ra ? ra.gain : -Infinity, pb = rb ? rb.gain : -Infinity;
    if (pa < pb) lo = a; else hi = b;
  }
  return (lo + hi) / 2;
}

async function calcA(sym, minDeltaPct, unitCap, priority){
  const [pool, sellBook, precision] = await Promise.all([
    getVenue(sym, priority), fetchBook(sym, "sell", priority), getTokenPrecision(sym),
  ]);
  if (!pool) return { ok: false, reason: "no_venue" };
  if (!sellBook.length) return { ok: false, reason: "empty_a" };
  const tick = Math.pow(10, -precision);
  const bookDepthQty = sellBook.reduce((s, l) => s + l.quantity, 0);

  const evalQ = (q) => {
    if (!(q > 0)) return null;
    const buy = walkBookBuy(sellBook, q);
    if (!buy || !(buy.cost > 0)) return null;
    const revenue = calcOutA(pool.qtyA, pool.qtyB, q);
    const gain = revenue - buy.cost;
    return { qty: q, cost: buy.cost, revenue, gain, gainPct: (gain / buy.cost) * 100,
      worstBuyPrice: buy.worstPrice, tokenPair: pool.tokenPair, precision };
  };

  let qMax = Math.min(pool.qtyB * 0.3, bookDepthQty);
  if (unitCap > 0){
    const top = evalQ(qMax);
    if (!top || top.cost > unitCap){
      let lo = 0, hi = qMax;
      for (let i = 0; i < SIZE_ITERS; i++){
        const mid = (lo + hi) / 2; const r = evalQ(mid);
        if (r && r.cost <= unitCap) lo = mid; else hi = mid;
      }
      qMax = lo;
    }
  }
  if (qMax < tick) return { ok: false, reason: "below_tick" };

  const rMin = evalQ(tick);
  if (!rMin) return { ok: false, reason: "no_quote" };
  if (rMin.gainPct < minDeltaPct){
    return { ok: false, reason: `below_min: ${rMin.gainPct.toFixed(3)} < ${minDeltaPct}`, detail: rMin };
  }

  let best = rMin;
  const rPeak = evalQ(peakProfitQty(evalQ, tick, qMax));
  if (rPeak && rPeak.gainPct >= minDeltaPct && (unitCap <= 0 || rPeak.cost <= unitCap)){
    best = rPeak;
  } else {
    const rMax = evalQ(qMax);
    if (rMax && rMax.gainPct >= minDeltaPct){ best = rMax; }
    else {
      let lo = tick, hi = qMax;
      for (let i = 0; i < SIZE_ITERS; i++){
        const mid = (lo + hi) / 2; const r = evalQ(mid);
        if (r && r.gainPct >= minDeltaPct && (unitCap <= 0 || r.cost <= unitCap)){ lo = mid; best = r; }
        else hi = mid;
      }
    }
  }
  const rounded = evalQ(roundDown(best.qty, precision));
  if (rounded && rounded.gainPct >= minDeltaPct && (unitCap <= 0 || rounded.cost <= unitCap)) best = rounded;
  return Object.assign({ ok: true }, best);
}

async function calcB(sym, minDeltaPct, unitCap, priority){
  const [pool, buyBook, precision] = await Promise.all([
    getVenue(sym, priority), fetchBook(sym, "buy", priority), getTokenPrecision(sym),
  ]);
  if (!pool) return { ok: false, reason: "no_venue" };
  if (!buyBook.length) return { ok: false, reason: "empty_b" };
  const tick = Math.pow(10, -precision);
  const bookDepthQty = buyBook.reduce((s, l) => s + l.quantity, 0);

  const evalQ = (q) => {
    if (!(q > 0)) return null;
    const cost = calcInB(pool.qtyA, pool.qtyB, q);
    if (!(cost > 0) || !isFinite(cost)) return null;
    const sell = walkBookSell(buyBook, q);
    if (!sell) return null;
    const gain = sell.revenue - cost;
    return { qty: q, cost, revenue: sell.revenue, gain, gainPct: (gain / cost) * 100,
      worstSellPrice: sell.worstPrice, tokenPair: pool.tokenPair, precision };
  };

  let qMax = Math.min(pool.qtyB * 0.3, bookDepthQty);
  if (unitCap > 0){
    const top = evalQ(qMax);
    if (!top || top.cost > unitCap){
      let lo = 0, hi = qMax;
      for (let i = 0; i < SIZE_ITERS; i++){
        const mid = (lo + hi) / 2; const r = evalQ(mid);
        if (r && r.cost <= unitCap) lo = mid; else hi = mid;
      }
      qMax = lo;
    }
  }
  if (qMax < tick) return { ok: false, reason: "below_tick" };

  const rMin = evalQ(tick);
  if (!rMin) return { ok: false, reason: "no_quote" };
  if (rMin.gainPct < minDeltaPct){
    return { ok: false, reason: `below_min: ${rMin.gainPct.toFixed(3)} < ${minDeltaPct}`, detail: rMin };
  }

  let best = rMin;
  const rPeak = evalQ(peakProfitQty(evalQ, tick, qMax));
  if (rPeak && rPeak.gainPct >= minDeltaPct && (unitCap <= 0 || rPeak.cost <= unitCap)){
    best = rPeak;
  } else {
    const rMax = evalQ(qMax);
    if (rMax && rMax.gainPct >= minDeltaPct){ best = rMax; }
    else {
      let lo = tick, hi = qMax;
      for (let i = 0; i < SIZE_ITERS; i++){
        const mid = (lo + hi) / 2; const r = evalQ(mid);
        if (r && r.gainPct >= minDeltaPct && (unitCap <= 0 || r.cost <= unitCap)){ lo = mid; best = r; }
        else hi = mid;
      }
    }
  }
  const rounded = evalQ(roundDown(best.qty, precision));
  if (rounded && rounded.gainPct >= minDeltaPct && (unitCap <= 0 || rounded.cost <= unitCap)) best = rounded;
  return Object.assign({ ok: true }, best);
}

const CHAIN_NODES = [
  "https://api.hive.blog",
  "https://api.deathwing.me",
  "https://anyx.io",
  "https://hive-api.arcange.eu",
  "https://techcoderx.com",
];
const client = new dhive.Client(CHAIN_NODES, { timeout: 15000 });
const signKey = dhive.PrivateKey.fromString(cfg.keyWif);

let lastBroadcastBlock = null;
let lastBroadcastAt = 0;
const BLOCK_POLL_MS = 400;

async function broadcastGate(){
  for (;;){
    let latest;
    try { latest = await getLatestBlockNumber(); }
    catch (e){ latest = null; }

    if (latest === null){
      const wait = lastBroadcastAt + MIN_BROADCAST_GAP_MS - Date.now();
      if (wait > 0){
        log("[gate] fallback wait", "info", `${(wait / 1000).toFixed(1)}s`);
        await sleep(wait);
      }
      break;
    }

    if (lastBroadcastBlock === null || latest > lastBroadcastBlock){
      lastBroadcastBlock = latest;
      break;
    }

    await sleep(BLOCK_POLL_MS);
  }
  lastBroadcastAt = Date.now();
}

async function broadcastCustomJson(json, label, detailText){
  await broadcastGate();
  const op = ["custom_json", {
    required_auths: [cfg.username],
    required_posting_auths: [],
    id: CHAIN_ID,
    json: JSON.stringify(json),
  }];
  log(`send ${label}`, "info", detailText);
  if (!cfg.live){
    log(`dry: not sent (${label})`, "info", JSON.stringify(json));
    return { id: "DRY_RUN", dryRun: true };
  }
  const result = await client.broadcast.sendOperations([op], signKey);
  log(`sent ${label}`, "ok", `tx ${result.id}`);
  return result;
}

const symbolCooldownUntil = {};
const symbolInFlight = new Set();
const seenTriggerKeys = new Map();

function isSymbolCooling(sym){ const u = symbolCooldownUntil[sym]; return !!u && Date.now() < u; }
function symbolCoolingSecondsLeft(sym){ const u = symbolCooldownUntil[sym]; return u ? Math.max(0, Math.ceil((u - Date.now()) / 1000)) : 0; }
function setSymbolCooldown(sym){ symbolCooldownUntil[sym] = Date.now() + Math.max(0, cfg.itemCooldownSec) * 1000; }
function extendCooldownAfterRound(sym){
  const ms = Math.max(Math.max(0, cfg.itemCooldownSec) * 1000, POST_ROUND_SETTLE_MS);
  symbolCooldownUntil[sym] = Math.max(symbolCooldownUntil[sym] || 0, Date.now() + ms);
}
function normTxId(id){ return String(id || "").replace(/-\d+$/, ""); }
function triggerAlreadySeen(txId, sym){
  if (!txId) return false;
  const key = normTxId(txId) + "|" + sym;
  const now = Date.now();
  if (seenTriggerKeys.size > 500){
    for (const [k, t] of seenTriggerKeys) if (now - t > 120000) seenTriggerKeys.delete(k);
  }
  if (seenTriggerKeys.has(key)) return true;
  seenTriggerKeys.set(key, now);
  return false;
}
function tryClaimSymbol(sym){
  if (symbolInFlight.has(sym) || isSymbolCooling(sym)) return false;
  symbolInFlight.add(sym); return true;
}
function releaseSymbol(sym){ symbolInFlight.delete(sym); }
function isOwnAccount(acc){ return String(acc || "").toLowerCase() === cfg.username.toLowerCase(); }

let execQueue = Promise.resolve();
function withExecLock(fn){
  const run = execQueue.then(fn, fn);
  execQueue = run.then(() => {}, () => {});
  return run;
}

async function stepWait(stepDelaySec){
  log(`step wait ${stepDelaySec}s`);
  await sleep(Math.max(stepDelaySec, 0) * 1000);
}

async function postCheck(sym, sizing, checkBook){
  await sleep(6000);
  try {
    const mine = await rpcFind("market", checkBook, { symbol: sym, account: cfg.username }, 100);
    const openQty = mine.reduce((s, r) => s + parseFloat(r.quantity || 0), 0);
    const kind = checkBook === "buyBook" ? "b" : "s";
    if (openQty > 0) log("check: remainder open", "err", `${sym} ${kind} ${openQty}`);
    else log("check: clean", "ok", `${sym} ${kind}`);
  } catch (e){ log("check failed", "err", `${sym} ${e.message}`); }
}

async function runPair(sym, qty, sizing, dir){
  const precision = await getTokenPrecision(sym);
  const q = roundDown(qty, precision);
  if (q <= 0) throw new Error("qty below precision");

  setSymbolCooldown(sym);

  if (dir === "B"){
    const maxIn = (sizing.cost * (1 + cfg.tolPct / 100)).toFixed(8);
    await broadcastCustomJson({
      contractName: "marketpools", contractAction: "swapTokens",
      contractPayload: { tokenPair: sizing.tokenPair, tokenSymbol: sym, tokenAmount: q.toFixed(precision), tradeType: "exactOutput", maxAmountIn: maxIn },
    }, "B1", `${q} ${sym} max ${maxIn}`);

    await stepWait(cfg.stepDelaySec);

    const MAX_LEG2_ATTEMPTS = 3;
    let sent = false, lastErr = null;
    for (let attempt = 1; attempt <= MAX_LEG2_ATTEMPTS && !sent; attempt++){
      let sellPrice;
      try {
        const freshBook = await fetchBook(sym, "buy", true);
        const fresh = walkBookSell(freshBook, q);
        if (fresh) sellPrice = fresh.worstPrice * (1 - cfg.tolPct / 100);
        else if (freshBook.length) sellPrice = freshBook[0].price * (1 - Math.max(cfg.tolPct, 2) / 100);
        else sellPrice = sizing.worstSellPrice * (1 - Math.max(cfg.tolPct, 2) / 100);
      } catch (e){ sellPrice = sizing.worstSellPrice * (1 - cfg.tolPct / 100); }
      sellPrice = sellPrice.toFixed(8);
      try {
        await broadcastCustomJson({
          contractName: "market", contractAction: "sell",
          contractPayload: { symbol: sym, quantity: q.toFixed(precision), price: sellPrice },
        }, `B2#${attempt}`, `${q} ${sym} @ ${sellPrice}`);
        sent = true;
      } catch (e){ lastErr = e; log(`step2 failed (try ${attempt})`, "err", `${sym} ${e.message}`); }
    }
    if (!sent){
      log(`ATTENTION: step2 not sent for ${sym} (${q}); run recover.js ${sym}`, "err", `${lastErr ? lastErr.message : "?"}`);
      return;
    }
    log("done", "ok", `${sym} d=${sizing.gain.toFixed(6)}`);
    await postCheck(sym, sizing, "sellBook");
    return;
  }

  const buyPrice = (sizing.worstBuyPrice * (1 + cfg.tolPct / 100)).toFixed(8);
  await broadcastCustomJson({
    contractName: "market", contractAction: "buy",
    contractPayload: { symbol: sym, quantity: q.toFixed(precision), price: buyPrice },
  }, "A1", `${q} ${sym} @ ${buyPrice}`);

  await stepWait(cfg.stepDelaySec);

  const MAX_LEG2_ATTEMPTS = 3;
  let sent = false, lastErr = null;
  for (let attempt = 1; attempt <= MAX_LEG2_ATTEMPTS && !sent; attempt++){
    let minOut;
    try {
      const freshPool = await getVenue(sym, true);
      if (freshPool){
        const freshRevenue = calcOutA(freshPool.qtyA, freshPool.qtyB, q);
        minOut = freshRevenue * (1 - cfg.tolPct / 100);
      } else minOut = 0.00000001;
    } catch (e){ minOut = 0.00000001; }
    minOut = minOut.toFixed(8);
    try {
      await broadcastCustomJson({
        contractName: "marketpools", contractAction: "swapTokens",
        contractPayload: { tokenPair: sizing.tokenPair, tokenSymbol: sym, tokenAmount: q.toFixed(precision), tradeType: "exactInput", minAmountOut: minOut },
      }, `A2#${attempt}`, `${q} ${sym} min ${minOut}`);
      sent = true;
    } catch (e){ lastErr = e; log(`step2 failed (try ${attempt})`, "err", `${sym} ${e.message}`); }
  }
  if (!sent){
    log(`ATTENTION: step2 not sent for ${sym} (${q}); run recover.js ${sym}`, "err", `${lastErr ? lastErr.message : "?"}`);
    return;
  }
  log("done", "ok", `${sym} d=${sizing.gain.toFixed(6)}`);
  await postCheck(sym, sizing, "buyBook");
}

async function runItem(sym, dir, opts){
  const ownClaim = !opts.claimed;
  if (ownClaim){
    if (symbolInFlight.has(sym)) return { sent: false, reason: "busy" };
    if (!isSymbolCooling(sym)) symbolInFlight.add(sym);
  }
  try { return await runItemInner(sym, dir, opts); }
  finally { if (ownClaim) releaseSymbol(sym); }
}

async function runItemInner(sym, dir, opts){
  const { priority } = opts;
  if (isSymbolCooling(sym)){
    log("skip: cooldown", "info", `${sym} ${symbolCoolingSecondsLeft(sym)}s`);
    return { sent: false, reason: "cooldown" };
  }

  let sizing = dir === "B"
    ? await calcB(sym, cfg.minDeltaPct, cfg.maxUnit, priority)
    : await calcA(sym, cfg.minDeltaPct, cfg.maxUnit, priority);

  if (!sizing || !sizing.ok || !(sizing.qty > 0)){
    return { sent: false, reason: (sizing && sizing.reason) || "unknown" };
  }
  if (!(sizing.gain > 0)) return { sent: false, reason: "none" };
  if (sizing.gain < cfg.minDelta) return { sent: false, reason: "dust" };

  const dirLabel = dir;
  log("queue", "info", `${sym} ${dirLabel} q=${sizing.qty.toFixed(6)} d=${sizing.gain.toFixed(6)} (${fmtPct(sizing.gainPct)}%)`);

  const queuedAt = Date.now();
  let executed = false, staleAbort = null;
  await withExecLock(async () => {
    if (Date.now() - queuedAt > 2500){
      const fresh = dir === "B"
        ? await calcB(sym, cfg.minDeltaPct, cfg.maxUnit, true)
        : await calcA(sym, cfg.minDeltaPct, cfg.maxUnit, true);
      if (!fresh || !fresh.ok || !(fresh.qty > 0) || !(fresh.gain > 0) || fresh.gain < cfg.minDelta){
        staleAbort = (fresh && fresh.reason) || "gone";
        return;
      }
      sizing = fresh;
    }
    executed = true;
    await runPair(sym, sizing.qty, sizing, dir);
  });

  if (executed){
    extendCooldownAfterRound(sym);
    scheduleRoundEndRecheck(sym);
  }
  if (staleAbort){
    log("stale", "info", `${sym} ${staleAbort}`);
    return { sent: false, reason: "stale: " + staleAbort };
  }
  return { sent: true, sizing };
}

function scheduleRoundEndRecheck(sym){
  const until = symbolCooldownUntil[sym] || Date.now();
  const delay = Math.max(0, until - Date.now()) + 250;
  setTimeout(() => { runRoundEndRecheck(sym).catch(e => log("recheck error", "err", `${sym} ${e.message}`)); }, delay);
}
async function runRoundEndRecheck(sym){
  if (!tryClaimSymbol(sym)) return;
  try {
    const [a, b] = await Promise.allSettled([
      calcA(sym, cfg.minDeltaPct, cfg.maxUnit, true),
      calcB(sym, cfg.minDeltaPct, cfg.maxUnit, true),
    ]);
    let bestDir = null, bestSizing = null;
    [["A", a], ["B", b]].forEach(([dir, res]) => {
      if (res.status !== "fulfilled") return;
      const s = res.value;
      if (s && s.ok && s.gain > 0 && s.gain >= cfg.minDelta){
        if (!bestSizing || s.gain > bestSizing.gain){ bestDir = dir; bestSizing = s; }
      }
    });
    if (!bestSizing) return;
    log("recheck hit", "ok", `${sym} ${bestDir} d=${bestSizing.gain.toFixed(6)}`);
    if (!cfg.live) return;
    await runItem(sym, bestDir, { priority: true, claimed: true });
  } finally { releaseSymbol(sym); }
}

async function drainHits(o, deadlineTs){
  let hits = 0;
  while (hits < AUTO_DRAIN_MAX_PER_SYMBOL && Date.now() < deadlineTs){
    const res = await runItem(o.symbol, o.dir, { priority: true });
    if (!res.sent) return hits;
    hits++;
  }
  return hits;
}

let autoExecBusy = false;
async function tryLive(currentFound){
  if (autoExecBusy || !currentFound.length) return;
  autoExecBusy = true;
  const deadlineTs = Date.now() + AUTO_DRAIN_MAX_MS;
  try {
    for (const o of currentFound){
      if (Date.now() >= deadlineTs) break;
      const hits = await drainHits(o, deadlineTs);
      if (hits > 0) log(`hits ${hits}`, "info", o.symbol);
    }
  } finally { autoExecBusy = false; }
}

let tokens = [];
let scanBusy = false;

async function refresh(){
  if (scanBusy) return;
  scanBusy = true;
  try {
    const [metricsRows, pools] = await Promise.all([
      (async () => {
        const map = {};
        for (let i = 0; i < tokens.length; i += CHUNK_SIZE){
          const chunk = tokens.slice(i, i + CHUNK_SIZE);
          const rows = await rpcFind("market", "metrics", { symbol: { "$in": chunk } }, CHUNK_SIZE);
          rows.forEach(r => (map[r.symbol] = r));
        }
        return map;
      })(),
      getVenues(),
    ]);

    const candidates = [];
    tokens.forEach(sym => {
      const m = metricsRows[sym];
      const heAskNum = m ? parseFloat(m.lowestAsk) : NaN;
      const heBidNum = m ? parseFloat(m.highestBid) : NaN;
      const mid = pools[sym];
      let poolSellNum = NaN, poolBuyNum = NaN;
      if (mid !== undefined && !isNaN(mid)){ poolSellNum = mid * (1 - FEE); poolBuyNum = mid / (1 - FEE); }

      let gainA = NaN;
      if (!isNaN(heAskNum) && heAskNum > 0 && !isNaN(poolSellNum) && poolSellNum > 0){
        gainA = ((poolSellNum - heAskNum) / heAskNum) * 100;
      }
      let gainB = NaN;
      if (!isNaN(heBidNum) && heBidNum > 0 && !isNaN(poolBuyNum) && poolBuyNum > 0){
        gainB = ((heBidNum - poolBuyNum) / poolBuyNum) * 100;
      }
      if (!isNaN(gainA) && gainA >= cfg.minDeltaPct) candidates.push({ symbol: sym, dir: "A" });
      if (!isNaN(gainB) && gainB >= cfg.minDeltaPct) candidates.push({ symbol: sym, dir: "B" });
    });

    if (candidates.length) log(`verify ${candidates.length}`);
    const verified = await Promise.all(candidates.map(async c => {
      try {
        const sizing = c.dir === "B"
          ? await calcB(c.symbol, cfg.minDeltaPct, cfg.maxUnit, false)
          : await calcA(c.symbol, cfg.minDeltaPct, cfg.maxUnit, false);
        if (!sizing || !sizing.ok) return null;
        if (!(sizing.gain >= cfg.minDelta)) return null;
        if (!(sizing.gainPct >= cfg.minDeltaPct)) return null;
        return { symbol: c.symbol, dir: c.dir, gain: sizing.gainPct, gainAbs: sizing.gain, qty: sizing.qty };
      } catch (e){ return null; }
    }));
    const found = verified.filter(Boolean).sort((a, b) => (b.gainAbs || 0) - (a.gainAbs || 0));

    if (found.length){
      log(`scan: ${found.length} hit`, "info", found.map(o => `${o.symbol}(${o.dir}, ${o.gainAbs.toFixed(6)})`).join(", "));
    } else {
      log("scan: none", "info", `${tokens.length}`);
    }

    if (cfg.live) await tryLive(found);
  } catch (e){
    log("scan error", "err", e.message);
  } finally {
    scanBusy = false;
  }
}

const srcLastId = {};
let srcBusy = false;

function isOrderRow(row){ return row && row.operation === "market_placeOrder"; }
function isSwapRow(row){
  if (!row) return false;
  if (row.contract === "marketpools" && row.action === "swapTokens") return true;
  if (row.contractName === "marketpools" && row.contractAction === "swapTokens") return true;
  const op = String(row.operation || row.event || "").toLowerCase();
  return op.includes("swaptoken") || (op.includes("marketpools") && op.includes("swap"));
}
function extractSwapSymbols(row){
  const out = new Set();
  const add = v => { if (v && typeof v === "string"){ const s = v.trim().toUpperCase(); if (s && s !== "SWAP.HIVE") out.add(s); } };
  add(row.symbolIn); add(row.symbolOut); add(row.tokenSymbol); add(row.symbol);
  if (row.tokenPair && typeof row.tokenPair === "string") row.tokenPair.split(":").forEach(add);
  return Array.from(out);
}
async function fetchHistory(account){
  const url = `${HISTORY_API}?account=${encodeURIComponent(account)}&limit=${SRC_HISTORY_LIMIT}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("HTTP " + res.status);
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

async function onTrigger(acc, sym, row){
  if (!sym) return;
  sym = sym.toUpperCase();
  if (isOwnAccount(acc)) return;
  if (triggerAlreadySeen(row && row.transactionId, sym)) return;
  if (symbolInFlight.has(sym)) return;
  if (!isSymbolCooling(sym)) symbolInFlight.add(sym);
  else return;
  try { await onTriggerInner(acc, sym, row); }
  finally { releaseSymbol(sym); }
}
async function onTriggerInner(acc, sym){
  if (isSymbolCooling(sym)) return;
  const [a, b] = await Promise.allSettled([
    calcA(sym, cfg.minDeltaPct, cfg.maxUnit, true),
    calcB(sym, cfg.minDeltaPct, cfg.maxUnit, true),
  ]);
  let bestDir = null, bestSizing = null;
  [["A", a], ["B", b]].forEach(([dir, res]) => {
    if (res.status !== "fulfilled") return;
    const s = res.value;
    if (s && s.ok && s.gain > 0 && s.gain >= cfg.minDelta){
      if (!bestSizing || s.gain > bestSizing.gain){ bestDir = dir; bestSizing = s; }
    }
  });
  if (!bestSizing) return;
  log("src hit", "ok", `${sym} ${bestDir} d=${bestSizing.gain.toFixed(6)}`);
  if (!cfg.live) return;
  await runItem(sym, bestDir, { priority: true, claimed: true });
}

async function srcTick(){
  if (srcBusy || !cfg.srcEnabled || !cfg.srcList.length) return;
  srcBusy = true;
  try {
    const results = await Promise.allSettled(cfg.srcList.map(acc => fetchHistory(acc)));
    const pending = [];
    cfg.srcList.forEach((acc, i) => {
      const res = results[i];
      if (res.status !== "fulfilled" || !res.value.length) return;
      const rows = res.value.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
      const lastId = srcLastId[acc];
      if (!lastId){ srcLastId[acc] = rows[rows.length - 1]._id; return; }
      const idx = rows.findIndex(r => r._id === lastId);
      const newRows = idx >= 0 ? rows.slice(idx + 1) : rows;
      if (newRows.length) srcLastId[acc] = newRows[newRows.length - 1]._id;
      for (const row of newRows){
        if (isOrderRow(row)){ if (row.symbol) pending.push({ acc, sym: row.symbol, row }); }
        else if (isSwapRow(row)){ extractSwapSymbols(row).forEach(sym => pending.push({ acc, sym, row })); }
      }
    });
    for (const { acc, sym, row } of pending){
      try { await onTrigger(acc, sym, row); }
      catch (e){ log("src error", "err", `#${cfg.srcList.indexOf(acc)}/${sym} ${e.message}`); }
    }
  } finally { srcBusy = false; }
}

let feedBusy = false;
let lastSeenBlock = null;

function extractMarketOrderSymbol(t){
  try { const p = typeof t.payload === "string" ? JSON.parse(t.payload) : t.payload; return p && p.symbol ? String(p.symbol).toUpperCase() : null; }
  catch (e){ return null; }
}
function extractPoolSwapSymbolsFromTx(t){
  const out = new Set();
  try {
    const p = typeof t.payload === "string" ? JSON.parse(t.payload) : t.payload;
    if (!p) return [];
    const add = v => { if (v && typeof v === "string"){ const s = v.trim().toUpperCase(); if (s && s !== "SWAP.HIVE") out.add(s); } };
    add(p.tokenSymbol);
    if (p.tokenPair && typeof p.tokenPair === "string") p.tokenPair.split(":").forEach(add);
  } catch (e){}
  return Array.from(out);
}
async function processBlock(bn){
  let block;
  try { block = await getBlockInfo(bn); } catch (e){ log("feed: read failed", "err", `#${bn} ${e.message}`); return; }
  const txs = (block && block.transactions) || [];
  const pending = []; const seen = new Set();
  for (const t of txs){
    if (!t) continue;
    if (t.contract === "market" && (t.action === "buy" || t.action === "sell")){
      const sym = extractMarketOrderSymbol(t);
      if (sym) pending.push({ acc: t.sender || "?", sym, row: { operation: "market_placeOrder", transactionId: t.transactionId } });
    } else if (t.contract === "marketpools" && t.action === "swapTokens"){
      extractPoolSwapSymbolsFromTx(t).forEach(sym => pending.push({ acc: t.sender || "?", sym, row: { contract: "marketpools", action: "swapTokens", transactionId: t.transactionId } }));
    }
  }
  for (const { acc, sym, row } of pending){
    if (seen.has(sym)) continue;
    seen.add(sym);
    try { await onTrigger(acc, sym, row); }
    catch (e){ log("feed error", "err", `#${bn} ${sym} ${e.message}`); }
  }
}
async function feedTick(){
  if (feedBusy || !cfg.feedEnabled) return;
  feedBusy = true;
  try {
    let latest;
    try { latest = await getLatestBlockNumber(); } catch (e){ return; }
    if (!latest) return;
    if (lastSeenBlock === null){ lastSeenBlock = latest; log("feed: on", "info", `#${latest}`); return; }
    if (latest <= lastSeenBlock) return;
    const from = Math.max(lastSeenBlock + 1, latest - FEED_MAX_CATCHUP + 1);
    for (let bn = from; bn <= latest; bn++) await processBlock(bn);
    lastSeenBlock = latest;
  } finally { feedBusy = false; }
}

async function loadItems(){
  if (cfg.itemsEnv.toUpperCase() === "ALL"){
    const venueMap = await getVenues();
    tokens = sortItems(Object.keys(venueMap));
  } else {
    tokens = sortItems(cfg.itemsEnv.split(",").map(s => s.trim().toUpperCase()).filter(Boolean));
  }
  log(`loaded ${tokens.length}`);
}

async function main(){
  log(`start live=${cfg.live}`);
  if (!cfg.live) log("dry mode");

  await loadItems();
  await refresh();

  const startedAt = Date.now();
  const deadline = startedAt + cfg.maxRuntimeMin * 60 * 1000;

  const scanTimer = setInterval(() => { refresh().catch(e => log("refresh error", "err", e.message)); }, REFRESH_INTERVAL_MS);
  const srcTimer = setInterval(() => { srcTick().catch(e => log("src tick error", "err", e.message)); }, SRC_INTERVAL_MS);
  const feedTimer = setInterval(() => { feedTick().catch(e => log("feed tick error", "err", e.message)); }, FEED_POLL_MS);
  const universeTimer = setInterval(() => { loadItems().catch(e => log("reload error", "err", e.message)); }, 10 * 60 * 1000);

  while (Date.now() < deadline) await sleep(5000);

  clearInterval(scanTimer); clearInterval(srcTimer); clearInterval(feedTimer); clearInterval(universeTimer);
  log(`runtime limit (${cfg.maxRuntimeMin}m) reached, exiting`);
  process.exit(0);
}

main().catch(e => { log("fatal: " + String((e && e.message) || e), "err"); process.exit(1); });
