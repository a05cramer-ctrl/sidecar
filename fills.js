// Vercel serverless function: GET /api/fills
// Latest public Phoenix Perpetuals fills across the main markets. Public Phoenix API, no key. Cached 8s.

const BASE = "https://perp-api.phoenix.trade";
const FIRST = ["SOL", "BTC", "ETH"];
const N_MARKETS = 6;
let mk = { t: 0, v: null };
let out = { t: 0, v: null };

async function j(path, ms = 7000) {
  const c = new AbortController(); const tm = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(BASE + path, { signal: c.signal, headers: { accept: "application/json" } });
    if (!r.ok) throw new Error(path + " " + r.status);
    return await r.json();
  } finally { clearTimeout(tm); }
}

async function symbols() {
  if (mk.v && Date.now() - mk.t < 600e3) return mk.v;
  const list = await j("/v1/view/exchange/markets").catch(() => null);
  const act = (Array.isArray(list) ? list : []).filter((m) => m && m.symbol && String(m.marketStatus).toLowerCase() === "active").map((m) => m.symbol);
  const v = act.length ? [...FIRST.filter((s) => act.includes(s)), ...act.filter((s) => !FIRST.includes(s))].slice(0, N_MARKETS) : FIRST;
  mk = { t: Date.now(), v };
  return v;
}

async function build() {
  const syms = await symbols();
  const pages = await Promise.all(syms.map((s) => j(`/v1/trades/${encodeURIComponent(s)}/fills?limit=20`).catch(() => null)));
  const fills = [];
  pages.forEach((p, i) => (p && Array.isArray(p.data) ? p.data : []).forEach((f) => {
    const base = Number(f.baseQty), quote = Number(f.quoteQty), price = Number(f.price), time = Date.parse(f.timestamp);
    if (!isFinite(base) || !isFinite(quote) || !isFinite(time) || !f.transactionSignature) return;
    fills.push({ market: f.marketSymbol || syms[i], side: base >= 0 ? "buy" : "sell", size: Math.abs(quote), price, time, sig: f.transactionSignature });
  }));
  if (!fills.length) throw new Error("no fills");
  fills.sort((a, b) => b.time - a.time);
  return { source: "phoenix", updated: new Date().toISOString(), fills: fills.slice(0, 24) };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=8, stale-while-revalidate=20");
  try {
    if (out.v && Date.now() - out.t < 8e3) return res.status(200).json(out.v);
    const v = await build();
    out = { t: Date.now(), v };
    return res.status(200).json(v);
  } catch (e) {
    if (out.v) return res.status(200).json({ ...out.v, stale: true });
    return res.status(502).json({ error: String(e.message || e).slice(0, 200) });
  }
};
