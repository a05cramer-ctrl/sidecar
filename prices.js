// Vercel serverless function: GET /api/prices
// Live Phoenix Perpetuals mark prices for the ticker tape + SOL stat, 24h % from 1h candles,
// and SOL 1h candles for the hero background. Public Phoenix API, no key. Cached 15s.

const BASE = "https://perp-api.phoenix.trade";
const FIRST = ["SOL", "BTC", "ETH"];
const MAX = 14;
let mk = { t: 0, v: null };   // active markets, 10 min
let chg = { t: 0, v: null };  // 24h refs + SOL candles, 5 min
let out = { t: 0, v: null };  // full response, 15 s

async function j(path, ms = 7000) {
  const c = new AbortController(); const tm = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(BASE + path, { signal: c.signal, headers: { accept: "application/json" } });
    if (!r.ok) throw new Error(path + " " + r.status);
    return await r.json();
  } finally { clearTimeout(tm); }
}

async function markets() {
  if (mk.v && Date.now() - mk.t < 600e3) return mk.v;
  const list = await j("/v1/view/exchange/markets");
  const act = (Array.isArray(list) ? list : []).filter((m) => m && m.symbol && String(m.marketStatus).toLowerCase() === "active");
  const by = new Map(act.map((m) => [m.symbol, { symbol: m.symbol, name: (m.metadata && m.metadata.name) || m.symbol }]));
  const v = [...FIRST.filter((s) => by.has(s)).map((s) => by.get(s)), ...act.filter((m) => !FIRST.includes(m.symbol)).map((m) => by.get(m.symbol))].slice(0, MAX);
  mk = { t: Date.now(), v };
  return v;
}

const candle = (c) => ({ t: Number(c.time), o: Number(c.open), h: Number(c.high), l: Number(c.low), c: Number(c.close) });

async function refs(syms) {
  if (chg.v && Date.now() - chg.t < 300e3) return chg.v;
  const now = Date.now(), dayAgo = now - 24 * 3600e3;
  const ref = {};
  await Promise.all(syms.map(async (s) => {
    try {
      const a = await j(`/v1/candles/${encodeURIComponent(s)}?timeframe=1h&startTime=${now - 26 * 3600e3}&limit=40`);
      const cs = (Array.isArray(a) ? a : []).map(candle).filter((c) => isFinite(c.o)).sort((x, y) => x.t - y.t);
      const at = cs.find((c) => c.t >= dayAgo - 3600e3);
      if (at && at.t <= dayAgo + 2 * 3600e3 && at.o > 0) ref[s] = at.o; // only a real ~24h-old reference
    } catch (e) {}
  }));
  let candles = [];
  try {
    const a = await j(`/v1/candles/SOL?timeframe=1h&startTime=${now - 170 * 3600e3}&limit=200`);
    candles = (Array.isArray(a) ? a : []).map(candle).filter((c) => isFinite(c.c)).sort((x, y) => x.t - y.t).slice(-160);
  } catch (e) {}
  chg = { t: Date.now(), v: { ref, candles } };
  return chg.v;
}

async function build() {
  const ms = await markets();
  const syms = ms.map((m) => m.symbol);
  const [px, rf] = await Promise.all([
    Promise.all(syms.map((s) => j(`/v1/market/${encodeURIComponent(s)}/mark-price`).then((r) => Number(r && r.markPrice && r.markPrice.price)).catch(() => null))),
    refs(syms).catch(() => ({ ref: {}, candles: [] })),
  ]);
  const rows = ms.map((m, i) => {
    const p = px[i], r = rf.ref[m.symbol];
    return { symbol: m.symbol, name: m.name, price: isFinite(p) && p > 0 ? p : null, change: isFinite(p) && r ? ((p - r) / r) * 100 : null };
  }).filter((r) => r.price != null);
  if (!rows.length) throw new Error("no prices");
  return { source: "phoenix", updated: new Date().toISOString(), markets: rows, candles: rf.candles };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
  try {
    if (out.v && Date.now() - out.t < 15e3) return res.status(200).json(out.v);
    const v = await build();
    out = { t: Date.now(), v };
    return res.status(200).json(v);
  } catch (e) {
    if (out.v) return res.status(200).json({ ...out.v, stale: true });
    return res.status(502).json({ error: String(e.message || e).slice(0, 200) });
  }
};
