// Vercel serverless function: GET /api/board            -> top Phoenix traders
//                             GET /api/board?trader=<addr> -> single trader report (wallet or Phoenix account)
// Source 1 (env BITQUERY_TOKEN): Bitquery Solana PerpetualPositions, realized PnL over 30 days (metric "pnl30d").
//   RealizedPnl is in PhUSD (Phoenix's quote currency).
// Source 2 (env HELIUS_API_KEY, used only when BITQUERY_TOKEN is missing): public Phoenix fills from the last 24h,
//   each transaction's signer (first signer = fee payer) resolved through Helius RPC, aggregated per wallet, then
//   30d PnL from Phoenix's public /v1/users/{wallet}/pnl (404 = not a Phoenix user -> dropped). Ranked by 30d PnL
//   (metric "pnl30d"); falls back to 24h volume (metric "vol24h") only if every PnL call fails.
// Neither env var set -> 503.

const ENDPOINT = "https://streaming.bitquery.io/graphql";
const TTL = 5 * 60 * 1000;
const LIMIT = 50;
const cache = new Map(); // in-memory per warm instance

async function gql(query, variables) {
  const token = process.env.BITQUERY_TOKEN;
  if (!token) throw new Error("BITQUERY_TOKEN not set");
  const r = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify({ query, variables }),
  });
  const j = await r.json().catch(() => null);
  if (!r.ok || !j || j.errors) throw new Error("bitquery: " + (j && j.errors ? JSON.stringify(j.errors).slice(0, 300) : r.status));
  return j.data;
}

const num = (v) => (v == null ? 0 : Number(v)) || 0;
const since = () => new Date(Date.now() - 30 * 86400e3).toISOString();
const B58 = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

// 1) Leaderboard: realized PnL per Phoenix trader account (PDA), closed positions, AMM excluded.
const Q_BOARD = `
query Board($since: DateTime, $limit: Int) {
  Solana {
    PerpetualPositions(
      limit: { count: $limit }
      orderBy: { descendingByField: "pnl" }
      where: { Block: { Time: { since: $since } }, Position: { TraderIsAmm: false, Closed: true } }
    ) {
      Position { Trader }
      pnl: sum(of: Position_RealizedPnl)
      closes: count
      wins: count(if: { Position: { RealizedPnl: { gt: 0 } } })
    }
  }
}`;

// Count of distinct traders with a close in the window.
const Q_COUNT = `
query Count($since: DateTime) {
  Solana {
    PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { TraderIsAmm: false, Closed: true } }
    ) {
      traders: uniq(of: Position_Trader)
    }
  }
}`;

// 2) Map Phoenix account (PDA) -> signing wallet (most frequent signer), plus last closes.
const Q_DETAIL = `
query Detail($since: DateTime, $traders: [String!]) {
  Solana {
    signers: PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Liquidation: false } }
    ) {
      Position { Trader }
      Transaction { Signer }
      n: count
    }
    recent: PerpetualPositions(
      limitBy: { by: Position_Trader, count: 6 }
      orderBy: { descending: Block_Time }
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Closed: true } }
    ) {
      Block { Time }
      Position { Trader Type RealizedPnl Asset { Symbol } }
    }
  }
}`;

// 3) Quote volume per trader from fills.
const Q_VOL = `
query Vol($since: DateTime, $traders: [String!]) {
  Solana {
    PerpetualFills(
      where: { Block: { Time: { since: $since } }, Fill: { Trader: { in: $traders } } }
    ) {
      Fill { Trader }
      vol: sum(of: Fill_Amount_Quote)
    }
  }
}`;

// Resolve a wallet (Signer) to its Phoenix trader account(s).
const Q_RESOLVE = `
query Resolve($since: DateTime, $addr: String) {
  Solana {
    PerpetualPositions(
      limit: { count: 5 }
      where: { Block: { Time: { since: $since } }, Transaction: { Signer: { is: $addr } }, Position: { TraderIsAmm: false } }
    ) {
      Position { Trader }
      n: count
    }
  }
}`;

const Q_ONE = `
query One($since: DateTime, $traders: [String!]) {
  Solana {
    PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Closed: true } }
    ) {
      Position { Trader }
      pnl: sum(of: Position_RealizedPnl)
      closes: count
      wins: count(if: { Position: { RealizedPnl: { gt: 0 } } })
    }
  }
}`;

function sym(s) { return String(s || "").replace(/-?PERP$/i, "").replace(/\/.*$/, ""); }

async function enrich(rows, s) {
  const accounts = rows.map((r) => r.account);
  const [det, vol] = await Promise.all([
    gql(Q_DETAIL, { since: s, traders: accounts }).catch(() => null),
    gql(Q_VOL, { since: s, traders: accounts }).catch(() => null),
  ]);
  const signer = {}, recent = {}, volume = {};
  if (det) {
    for (const r of det.Solana.signers || []) {
      const a = r.Position.Trader, w = r.Transaction.Signer, n = num(r.n);
      if (!signer[a] || n > signer[a].n) signer[a] = { w, n };
    }
    for (const r of det.Solana.recent || []) {
      const a = r.Position.Trader;
      (recent[a] = recent[a] || []).push({
        market: sym(r.Position.Asset && r.Position.Asset.Symbol),
        side: r.Position.Type || "",
        pnl: num(r.Position.RealizedPnl),
        time: Date.parse(r.Block.Time),
      });
    }
  }
  if (vol) for (const r of vol.Solana.PerpetualFills || []) volume[r.Fill.Trader] = num(r.vol);
  return rows.map((r) => ({
    ...r,
    wallet: signer[r.account] ? signer[r.account].w : r.account, // fee routing target = signing wallet
    walletResolved: !!signer[r.account],
    volume: volume[r.account] != null ? volume[r.account] : null,
    recent: recent[r.account] || [],
  }));
}

async function board() {
  const s = since();
  const [d, c] = await Promise.all([gql(Q_BOARD, { since: s, limit: LIMIT }), gql(Q_COUNT, { since: s }).catch(() => null)]);
  const rows = (d.Solana.PerpetualPositions || []).map((r) => ({
    account: r.Position.Trader, pnl: num(r.pnl), closes: num(r.closes), wins: num(r.wins),
  })).filter((r) => r.account);
  const traders = await enrich(rows, s);
  const total = c && c.Solana.PerpetualPositions[0] ? num(c.Solana.PerpetualPositions[0].traders) : traders.length;
  return { source: "bitquery", metric: "pnl30d", window: "30d", currency: "PhUSD", updated: new Date().toISOString(), total, traders };
}

async function one(addr) {
  const s = since();
  let accounts = [addr];
  const res = await gql(Q_RESOLVE, { since: s, addr }).catch(() => null);
  if (res) for (const r of res.Solana.PerpetualPositions || []) if (r.Position.Trader) accounts.push(r.Position.Trader);
  accounts = [...new Set(accounts)];
  const d = await gql(Q_ONE, { since: s, traders: accounts });
  const rows = (d.Solana.PerpetualPositions || []).map((r) => ({ account: r.Position.Trader, pnl: num(r.pnl), closes: num(r.closes), wins: num(r.wins) }));
  if (!rows.length) return { trader: null };
  rows.sort((a, b) => b.pnl - a.pnl);
  const [t] = await enrich([rows[0]], s);
  if (accounts[0] !== t.account) t.wallet = addr; // searched by wallet: route to that wallet
  return { metric: "pnl30d", source: "bitquery", trader: t };
}

/* ======================= Source 2: Phoenix public fills + Helius ======================= */
const PHX = "https://perp-api.phoenix.trade";
const FIRST = ["SOL", "BTC", "ETH"];
const MAX_RESOLVE = Number(process.env.SIDECAR_MAX_RESOLVE) || 300; // Helius getTransaction lookups per rebuild (1 credit each)
const MAX_TRADERS = Number(process.env.SIDECAR_MAX_TRADERS) || 150; // Phoenix PnL lookups per rebuild
const MIN_TX_USD = 50;          // ignore dust txs when picking what to resolve
const HELIUS_BATCH = 10;        // free tier: batches of 25+ get 429
const HELIUS_GAP = 1050;        // ms between batch starts (one batch at a time)
const MAX_PAGES = 40;           // Phoenix fills pages per rebuild (1000 fills each)
const DEADLINE = 45e3;          // stay inside Vercel's 60s maxDuration
const RESOLVE_UNTIL = 36e3;     // stop Helius lookups here, leaving time for PnL calls
const KNOWN_TTL = 3600e3;       // wallets stay on the board 1h after last seen in a rebuild (warm instance)
const SIG_TTL = 25 * 3600e3;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const SIGS = new Map();   // signature -> { w: wallet, t: seen }   (warm-instance cache, never re-resolved)
const KNOWN = new Map();  // wallet -> { agg, pnl, unrealized, nf, seen }

async function getJ(url, opt = {}, ms = 8000) {
  const c = new AbortController(); const tm = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(url, { ...opt, signal: c.signal });
    if (!r.ok) { const e = new Error(url.replace(/api-key=[^&]+/, "api-key=***") + " " + r.status); e.status = r.status; throw e; }
    return await r.json();
  } finally { clearTimeout(tm); }
}
async function pool(items, n, fn) {
  const out = new Array(items.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k], k).catch(() => null); } }));
  return out;
}

async function activeSymbols() {
  const list = await getJ(PHX + "/v1/view/exchange/markets");
  const act = (Array.isArray(list) ? list : []).filter((m) => m && m.symbol && String(m.marketStatus).toLowerCase() === "active").map((m) => m.symbol);
  return [...FIRST.filter((s) => act.includes(s)), ...act.filter((s) => !FIRST.includes(s))];
}

// Trader fills of the last 24h across active markets (keeper cranks and liquidations dropped).
const isKeeper = (t) => /crank|liquidat/i.test(String(t || ""));
async function fills24h(t0) {
  const start = Date.now() - 24 * 3600e3;
  const syms = await activeSymbols();
  const all = []; let pages = 0, skipped = 0;
  const take = (p) => (p && Array.isArray(p.data) ? p.data : []).forEach((f) => {
    if (isKeeper(f.instructionType)) { skipped++; return; }
    const ts = Date.parse(f.timestamp), q = Math.abs(Number(f.quoteQty)), b = Number(f.baseQty);
    if (ts >= start && isFinite(q) && f.transactionSignature) all.push({ market: f.marketSymbol, sig: f.transactionSignature, size: q, side: b >= 0 ? "buy" : "sell", time: ts, ix: f.instructionType || "" });
  });
  const first = await pool(syms, 8, (s) => getJ(`${PHX}/v1/trades/${encodeURIComponent(s)}/fills?limit=1000&startTime=${start}`));
  const open = [];
  first.forEach((p, i) => { pages++; take(p); if (p && p.hasMore && p.nextCursor) open.push({ s: syms[i], c: p.nextCursor }); });
  while (open.length && pages < MAX_PAGES && Date.now() - t0 < DEADLINE * 0.3) {
    const batch = open.splice(0, 6);
    const res = await pool(batch, 6, (o) => getJ(`${PHX}/v1/trades/${encodeURIComponent(o.s)}/fills?limit=1000&startTime=${start}&cursor=${encodeURIComponent(o.c)}`));
    res.forEach((p, i) => { pages++; if (p && p.hasMore && p.nextCursor && p.data && p.data.length) open.push({ s: batch[i].s, c: p.nextCursor }); take(p); });
  }
  return { fills: all, markets: syms.length, pages, skipped };
}

// Round-robin across markets: biggest tx of each market, then 2nd biggest, ... (txs >= $50, not yet resolved).
function pickTxs(txs, n) {
  const by = new Map();
  for (const x of txs) {
    if (x.size < MIN_TX_USD || SIGS.has(x.sig)) continue;
    const m = x.legs[0].market;
    (by.get(m) || by.set(m, []).get(m)).push(x);
  }
  const lanes = [...by.values()].map((l) => l.sort((a, b) => b.size - a.size))
    .sort((a, b) => b.reduce((s, x) => s + x.size, 0) - a.reduce((s, x) => s + x.size, 0));
  const out = [];
  for (let k = 0; out.length < n; k++) {
    let any = false;
    for (const l of lanes) { if (l[k]) { any = true; out.push(l[k]); if (out.length >= n) break; } }
    if (!any) break;
  }
  return out;
}

// signature -> signing wallet via Helius RPC: batches of 10, one at a time, ~1.05s apart, 429 -> backoff + retry.
async function resolveSigners(sigs, t0) {
  const url = "https://mainnet.helius-rpc.com/?api-key=" + encodeURIComponent(process.env.HELIUS_API_KEY);
  const call = (sig, id) => ({ jsonrpc: "2.0", id, method: "getTransaction", params: [sig, { encoding: "jsonParsed", maxSupportedTransactionVersion: 0, commitment: "confirmed" }] });
  let n = 0, limited = 0, lastReq = 0;
  const throttle = async () => { const w = lastReq + HELIUS_GAP - Date.now(); if (w > 0) await sleep(w); lastReq = Date.now(); };
  for (let i = 0; i < sigs.length; i += HELIUS_BATCH) {
    if (Date.now() - t0 > RESOLVE_UNTIL) break;
    const ch = sigs.slice(i, i + HELIUS_BATCH);
    let res = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      await throttle();
      try { res = await getJ(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(ch.map((s, k) => call(s, k))) }, 15000); break; }
      catch (e) { if (e.status === 429 && attempt < 3 && Date.now() - t0 < RESOLVE_UNTIL) { limited++; await sleep(1200 * attempt); continue; } break; }
    }
    if (Array.isArray(res)) res.forEach((x) => {
      const sig = x && x.id != null && ch[x.id];
      const keys = sig && x.result && x.result.transaction && x.result.transaction.message && x.result.transaction.message.accountKeys;
      const s0 = Array.isArray(keys) && keys.find((k) => k && k.signer); // first signer = fee payer = trader wallet
      if (s0) { SIGS.set(sig, { w: s0.pubkey || s0, t: Date.now() }); n++; }
    });
  }
  return { resolved: n, limited };
}

// 30d PnL from Phoenix's public per-wallet endpoint. Returns {pnl, unrealized}, "nf" for a non-Phoenix wallet (404),
// or throws on any other failure.
async function phoenixPnl(wallet) {
  let a;
  try { a = await getJ(`${PHX}/v1/users/${encodeURIComponent(wallet)}/pnl?resolution=1d&limit=31`); }
  catch (e) { if (e.status === 404) return "nf"; throw e; }
  const pts = (Array.isArray(a) ? a : []).filter((p) => p && isFinite(Number(p.cumulativePnl)))
    .sort((x, y) => Number(x.startTime != null ? x.startTime : x.timestamp) - Number(y.startTime != null ? y.startTime : y.timestamp));
  if (!pts.length) return "nf";
  const first = pts[0], last = pts[pts.length - 1];
  const pnl = pts.length > 1 ? Number(last.cumulativePnl) - Number(first.cumulativePnl) : Number(last.cumulativePnl);
  return { pnl, unrealized: isFinite(Number(last.unrealizedPnl)) ? Number(last.unrealizedPnl) : null };
}

function aggregate(txs) {
  const W = new Map();
  for (const x of txs) {
    const hit = SIGS.get(x.sig); if (!hit) continue;
    const w = hit.w;
    const r = W.get(w) || { wallet: w, account: w, volume: 0, trades: 0, last: 0, markets: {}, recent: [] };
    r.volume += x.size; r.trades += 1; r.last = Math.max(r.last, x.time);
    for (const l of x.legs) r.markets[l.market] = (r.markets[l.market] || 0) + l.size;
    const lead = x.legs[0];
    r.recent.push({ market: lead.market, side: lead.side, size: x.size, time: x.time, sig: x.sig });
    W.set(w, r);
  }
  for (const r of W.values()) {
    r.markets = Object.entries(r.markets).sort((a, b) => b[1] - a[1]).map((e) => e[0]);
    r.recent = r.recent.sort((a, b) => b.time - a.time).slice(0, 6);
  }
  return W;
}

async function phoenixBoard() {
  const t0 = Date.now(), now = Date.now();
  for (const [k, v] of SIGS) if (now - v.t > SIG_TTL) SIGS.delete(k);
  const { fills, markets, pages, skipped } = await fills24h(t0);
  if (!fills.length) throw new Error("no Phoenix fills returned");
  // one row per transaction (a market order can match several makers)
  const tx = new Map();
  for (const f of fills) {
    const x = tx.get(f.sig) || { sig: f.sig, size: 0, time: 0, legs: [] };
    x.size += f.size; x.time = Math.max(x.time, f.time); x.legs.push(f); tx.set(f.sig, x);
  }
  const txs = [...tx.values()];
  const { resolved, limited } = await resolveSigners(pickTxs(txs, MAX_RESOLVE).map((x) => x.sig), t0);
  // union: wallets seen this round (24h stats from every resolved tx still in the window) + known wallets < 1h old
  const W = aggregate(txs);
  for (const [w, agg] of W) { const k = KNOWN.get(w) || {}; KNOWN.set(w, { ...k, agg, seen: now }); }
  for (const [w, k] of KNOWN) if (now - k.seen > KNOWN_TTL) KNOWN.delete(w);
  const cand = [...KNOWN.entries()].filter(([, k]) => !k.nf).sort((a, b) => b[1].agg.volume - a[1].agg.volume).slice(0, MAX_TRADERS);
  if (!cand.length) throw new Error("no signers resolved" + (limited ? " (Helius rate limited)" : ""));
  let ok = 0, err = 0;
  await pool(cand, 8, async ([w, k]) => {
    try {
      const r = await phoenixPnl(w);
      if (r === "nf") { k.nf = true; return; }
      ok++; k.pnl = r.pnl; k.unrealized = r.unrealized; k.pnlT = Date.now();
    } catch (e) { err++; }
  });
  const meta = { source: "phoenix", currency: "USD", updated: new Date().toISOString(), fills: fills.length, keeperFillsSkipped: skipped, resolved, rateLimited: limited, known: KNOWN.size, markets, pages };
  const withPnl = cand.filter(([, k]) => !k.nf && k.pnl != null && Date.now() - (k.pnlT || 0) < KNOWN_TTL)
    .map(([w, k]) => ({ ...k.agg, wallet: w, account: w, pnl: k.pnl, unrealized: k.unrealized })).sort((a, b) => b.pnl - a.pnl);
  if (withPnl.length) {
    const stats = { traders: withPnl.length, combinedPnl30d: withPnl.reduce((s, t) => s + t.pnl, 0), best: { wallet: withPnl[0].wallet, pnl: withPnl[0].pnl } };
    return { ...meta, metric: "pnl30d", window: "30d", total: withPnl.length, checked: cand.length, stats, traders: withPnl.slice(0, LIMIT) };
  }
  if (ok === 0 && err > 0) {
    // every PnL call failed: fall back to 24h volume
    const vol = cand.map(([w, k]) => ({ ...k.agg, wallet: w, account: w })).sort((a, b) => b.volume - a.volume);
    const stats = { traders: vol.length, combinedVolume24h: vol.reduce((s, t) => s + t.volume, 0), best: { wallet: vol[0].wallet, volume: vol[0].volume } };
    return { ...meta, metric: "vol24h", window: "24h", total: vol.length, stats, traders: vol.slice(0, LIMIT) };
  }
  throw new Error("no Phoenix traders among resolved signers yet");
}

let building = null;
async function cachedBoard(fn) {
  const hit = cache.get("board");
  if (hit && Date.now() - hit.t < TTL) return hit.v;
  if (!building) building = fn().then((v) => { cache.set("board", { t: Date.now(), v }); return v; }).finally(() => { building = null; });
  return building;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=600");
  const useBitquery = !!process.env.BITQUERY_TOKEN, useHelius = !useBitquery && !!process.env.HELIUS_API_KEY;
  if (!useBitquery && !useHelius) {
    res.setHeader("Cache-Control", "no-store");
    return res.status(503).json({ error: "board not configured: set BITQUERY_TOKEN or HELIUS_API_KEY" });
  }
  try {
    const q = (req.query && req.query.trader) || new URL(req.url, "http://x").searchParams.get("trader");
    if (q) {
      if (!B58.test(q)) return res.status(400).json({ error: "invalid address" });
      if (useHelius) {
        const hit = cache.get("board"), b = hit && hit.v;
        const onBoard = b && b.traders.find((x) => x.wallet === q);
        if (onBoard) return res.status(200).json({ metric: b.metric, source: "phoenix", trader: onBoard });
        const p = await phoenixPnl(q);
        if (p === "nf") return res.status(200).json({ metric: "pnl30d", source: "phoenix", trader: null, notFound: true });
        const k = KNOWN.get(q), agg = k && k.agg;
        const t = { wallet: q, account: q, pnl: p.pnl, unrealized: p.unrealized, volume: agg ? agg.volume : null, trades: agg ? agg.trades : null, last: agg ? agg.last : null, markets: agg ? agg.markets : [], recent: agg ? agg.recent : [] };
        return res.status(200).json({ metric: "pnl30d", source: "phoenix", trader: t });
      }
      const key = "t:" + q, hit = cache.get(key);
      if (hit && Date.now() - hit.t < TTL) return res.status(200).json(hit.v);
      const v = await one(q);
      cache.set(key, { t: Date.now(), v });
      return res.status(200).json(v);
    }
    const v = await cachedBoard(useBitquery ? board : phoenixBoard);
    return res.status(200).json(v);
  } catch (e) {
    const stale = cache.get("board");
    if (stale && !(req.query && req.query.trader)) return res.status(200).json({ ...stale.v, stale: true });
    return res.status(502).json({ error: String(e.message || e).slice(0, 300) });
  }
};
module.exports.config = { maxDuration: 60 };
module.exports._test = { phoenixBoard, KNOWN, SIGS };
