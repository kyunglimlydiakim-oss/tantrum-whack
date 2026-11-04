// 탠트럼 두더지 랭킹 API (Vercel 서버리스 함수)
// 저장소: Upstash Redis REST. Vercel 마켓플레이스의 Upstash 연동이 넣어 주는 환경 변수를 그대로 쓴다.
//   KV_REST_API_URL / KV_REST_API_TOKEN  또는  UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
//
// GET  /api/scores?me=<playerId>         → { rows: [{ id, best, combo, nick }], me: { best, rank } | null }
// POST /api/scores { id, secret, best, combo, nick, durationMs }  → 최고 기록만 갱신
// POST /api/scores { id, secret, action: "nick", nick }            → 이름만 변경
//
// 각 플레이어는 브라우저에서 만든 id와 secret을 쓴다. 첫 기록 때 secret의 해시를 저장하고,
// 이후에는 같은 secret을 가진 브라우저만 그 기록을 바꿀 수 있다.

const crypto = require("crypto");

const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const ZSET = "tw:scores";
const player = id => "tw:p:" + id;
const LIMIT = 50;
const NICK_MAX = 12;
const MAX_HITS = 100000;
const HITS_PER_SEC = 8;          // 가장 빠른 단계에서도 넘기 어려운 속도
const POSTS_PER_MIN_PLAYER = 15;  // 플레이어 한 명당
const POSTS_PER_MIN_IP = 300;     // 회사처럼 여러 명이 같은 IP를 쓰는 경우를 위해 넉넉하게

const ID_RE = /^[A-Za-z0-9-]{8,64}$/;

async function redis(commands) {
  const r = await fetch(REDIS_URL.replace(/\/$/, "") + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + REDIS_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(commands)
  });
  if (!r.ok) throw new Error("redis " + r.status);
  const out = await r.json();
  return out.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}

const cleanNick = s => String(s ?? "").replace(/[\u0000-\u001f\u007f<>]/g, "").replace(/\s+/g, " ").trim().slice(0, NICK_MAX);
const toInt = v => (Number.isFinite(Number(v)) ? Math.floor(Number(v)) : NaN);
const sha = s => crypto.createHash("sha256").update(String(s)).digest("hex");

async function top(meId) {
  const [flat] = await redis([["ZREVRANGE", ZSET, "0", String(LIMIT - 1), "WITHSCORES"]]);
  const ids = [], scores = [];
  for (let i = 0; i < flat.length; i += 2) { ids.push(flat[i]); scores.push(toInt(flat[i + 1])); }
  const cmds = ids.map(id => ["HMGET", player(id), "nick", "combo"]);
  if (meId) cmds.push(["ZSCORE", ZSET, meId], ["ZREVRANK", ZSET, meId]);
  const res = cmds.length ? await redis(cmds) : [];
  const rows = ids.map((id, i) => ({ id, best: scores[i], nick: cleanNick(res[i]?.[0]), combo: toInt(res[i]?.[1]) || 0 }));
  let me = null;
  if (meId) {
    const s = res[ids.length], rk = res[ids.length + 1];
    if (s != null) me = { best: toInt(s), rank: rk == null ? null : rk + 1 };
  }
  return { rows, me };
}

module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (!REDIS_URL || !REDIS_TOKEN) return res.status(503).json({ error: "storage_not_configured" });

  try {
    if (req.method === "GET") {
      const me = typeof req.query.me === "string" && ID_RE.test(req.query.me) ? req.query.me : null;
      return res.status(200).json(await top(me));
    }
    if (req.method !== "POST") return res.status(405).json({ error: "method_not_allowed" });

    const b = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    if (!ID_RE.test(String(b.id)) || !ID_RE.test(String(b.secret))) return res.status(400).json({ error: "bad_player" });

    // 요청 속도 제한 (1분 창): 플레이어별로 좁게, IP별로는 넓게
    const ip = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim() || "unknown";
    const kPlayer = "tw:rl:p:" + b.id, kIp = "tw:rl:ip:" + sha(ip).slice(0, 24);
    const [, , nPlayer, nIp] = await redis([["SET", kPlayer, "0", "EX", "60", "NX"], ["SET", kIp, "0", "EX", "60", "NX"], ["INCR", kPlayer], ["INCR", kIp]]);
    if (nPlayer > POSTS_PER_MIN_PLAYER || nIp > POSTS_PER_MIN_IP) return res.status(429).json({ error: "rate_limited" });

    // 이 id의 주인 확인: 처음이면 secret 해시를 등록, 아니면 일치해야 한다
    const hash = sha(b.secret);
    const [, stored] = await redis([["HSETNX", player(b.id), "sec", hash], ["HGET", player(b.id), "sec"]]);
    if (stored !== hash) return res.status(403).json({ error: "not_owner" });

    if (b.action === "nick") {
      await redis([["HSET", player(b.id), "nick", cleanNick(b.nick)]]);
      return res.status(200).json({ ok: true });
    }

    const best = toInt(b.best), combo = toInt(b.combo), dur = toInt(b.durationMs);
    if (!(best >= 1 && best <= MAX_HITS)) return res.status(400).json({ error: "bad_score" });
    if (!(dur > 0) || best > (dur / 1000) * HITS_PER_SEC + 20) return res.status(400).json({ error: "implausible" });
    const safeCombo = combo >= 0 && combo <= best ? combo : 0;

    const [oldCombo] = await redis([["HGET", player(b.id), "combo"]]);
    const cmds = [["ZADD", ZSET, "GT", String(best), b.id]];
    if (safeCombo > (toInt(oldCombo) || 0)) cmds.push(["HSET", player(b.id), "combo", String(safeCombo)]);
    if (typeof b.nick === "string") cmds.push(["HSET", player(b.id), "nick", cleanNick(b.nick)]);
    cmds.push(["ZSCORE", ZSET, b.id], ["ZREVRANK", ZSET, b.id]);
    const out = await redis(cmds);
    const rank = out[out.length - 1], score = out[out.length - 2];
    return res.status(200).json({ ok: true, best: toInt(score), rank: rank == null ? null : rank + 1 });
  } catch (e) {
    console.error("scores api error:", e && e.message);   // Vercel 로그에서 원인 확인용
    return res.status(500).json({ error: "server_error" });
  }
};
