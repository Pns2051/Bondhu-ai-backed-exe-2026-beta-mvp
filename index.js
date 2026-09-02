/**
 * Bondhu AI — Backend Server (v3.3 FINAL, beta)
 * Bangladesh's first premier AI chat companion.
 *
 * Stack       : Node.js + Express + PostgreSQL (Neon) — designed for Render.
 * Features    : SSE word-by-word streaming · quad-tier AI fallback cascade
 *               (Groq → Gemini → Mistral → OpenRouter) · UUID sessions with
 *               auto-titles · multi-turn memory (last 10 messages) · device
 *               users with 50 starter credits · daily top-up-to-10 refill
 *               (Asia/Dhaka, cron + sleeping-server catch-up) · dual rate
 *               limiting (per-IP + per-device) · moderation blocklist ·
 *               admin API (stats/users/credits/ban) · usage_logs observability.
 *
 * Credit policy: 1 credit charged BEFORE generation; refunded only when
 * Bondhu AI itself fails to deliver. Deliberate disconnects keep the charge.
 *
 * v3.3 fixes:
 *  - Provider model defaults updated after retirements (llama-3.1-8b-instant
 *    and gemini-1.5-flash both returned 404 model_not_found in production).
 *    Groq → llama-3.3-70b-versatile, Gemini → gemini-2.5-flash.
 *  - Gemini 2.5 Flash "thinking" disabled (thinkingBudget: 0) for instant
 *    first tokens.
 *  - GET / status route + GET /api/chat → 405 wrong-method guard.
 *
 * Required env : DATABASE_URL, plus at least one provider key (GROQ_API_KEY
 *                recommended as tier 1).
 * Optional env : GEMINI_API_KEY, MISTRAL_API_KEY, OPENROUTER_API_KEY,
 *                ADMIN_KEY, CORS_ORIGINS, MODERATION_WORDS, PORT,
 *                GROQ_MODEL, GEMINI_MODEL, MISTRAL_MODEL, OPENROUTER_MODEL
 *                (any *_MODEL env var OVERRIDES the code defaults below.)
 */

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const cron = require('node-cron');
const { timingSafeEqual } = require('crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
// Always trust ONE proxy hop (Render edge). Harmless without a proxy;
// required with one, or every client shares 127.0.0.1 and breaks rate limits.
app.set('trust proxy', 1);

/* ────────────────────────────── CORS allowlist ────────────────────────────── */

const CORS_ORIGINS = (process.env.CORS_ORIGINS || '*')
  .split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

app.use(cors({
  origin(origin, cb) {
    if (CORS_ORIGINS.includes('*') || !origin) return cb(null, true);
    return cb(null, CORS_ORIGINS.includes(origin.toLowerCase()));
  },
}));

app.use(express.json({ limit: '1mb' }));

/* ──────────── Request log (JSON lines; also logs aborted streams) ─────────── */

app.use((req, res, next) => {
  if (req.method === 'OPTIONS' || req.path === '/health') return next();
  const t0 = Date.now();
  res.on('close', () => {
    console.log(JSON.stringify({
      t: new Date().toISOString(),
      method: req.method, path: req.path,
      status: res.statusCode, ms: Date.now() - t0,
    }));
  });
  next();
});

/* ────────────────────────────── Configuration ────────────────────────────── */

const SYSTEM_PROMPT = [
  "You are Bondhu AI, Bangladesh's first premier AI chat companion.",
  'Tone: Friendly, direct, concise.',
  'Language: Flexibly match Bengali, English, or Banglish based on user input.',
  'Rules: Never fabricate facts or official data; always start directly with the core answer.',
].join(' ');

const PORT = process.env.PORT || 3000;
const FREE_CREDITS = 50;
const HISTORY_LIMIT = 10;
const MAX_MESSAGE_LENGTH = 4000;
const MAX_DEVICE_ID_LENGTH = 200;
const DAILY_REFILL_TO = 10;       // top up to this when below this, once per Dhaka day
const REFILL_TZ = 'Asia/Dhaka';

const TIER_TIMEOUT_MS = 25000;    // watchdog: max wait for FIRST chunk or silent gap
const HEARTBEAT_MS = 15000;       // SSE keepalive (survives proxy idle timeouts)
const WORD_BUFFER_LIMIT = 256;    // flush safeguard for unbroken mega-tokens

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const MODELS = {
  // v3.3 FIX — previous defaults were retired by their providers (404
  // model_not_found in production logs, 2026-09). Verify before changing:
  // console.groq.com/docs/models, ai.google.dev model list.
  groq:       process.env.GROQ_MODEL       || 'llama-3.3-70b-versatile',
  gemini:     process.env.GEMINI_MODEL     || 'gemini-2.5-flash',
  mistral:    process.env.MISTRAL_MODEL    || 'mistral-small-latest',
  // OpenRouter free model IDs rotate often — if this ever 404s, pick a current
  // one from https://openrouter.ai/collections/free-models and set OPENROUTER_MODEL.
  openrouter: process.env.OPENROUTER_MODEL || 'meta-llama/llama-3.3-70b-instruct:free',
};

const ADMIN_KEY = process.env.ADMIN_KEY || '';

const BLOCKED_WORDS = (process.env.MODERATION_WORDS || '')
  .split(',').map((w) => w.trim().toLowerCase()).filter(Boolean);

function httpError(status, message, code) {
  const e = new Error(message);
  e.status = status;
  if (code) e.code = code;
  return e;
}

/* ───────────────────────── Database Connection (Neon) ───────────────────────── */

if (!process.env.DATABASE_URL) {
  console.error('FATAL: DATABASE_URL is not set.');
  process.exit(1);
}

const isLocalDB = process.env.DATABASE_URL.includes('localhost');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isLocalDB ? false : { rejectUnauthorized: false }, // Neon requires SSL
  max: 10,
  idleTimeoutMillis: 30000,
});

// Without this listener an idle-client error (Neon drops idle connections
// routinely) would crash the whole process.
pool.on('error', (err) => console.error('[DB pool] idle client error:', err.message));

async function initSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id               SERIAL PRIMARY KEY,
      device_id        TEXT NOT NULL UNIQUE,
      credit_balance   INTEGER NOT NULL DEFAULT ${FREE_CREDITS},
      last_refill_date DATE,
      banned           BOOLEAN NOT NULL DEFAULT FALSE,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      title      TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS messages (
      id         BIGSERIAL PRIMARY KEY,
      session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      role       TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
      content    TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS usage_logs (
      id          BIGSERIAL PRIMARY KEY,
      user_id     INTEGER,
      session_id  UUID,
      provider    TEXT NOT NULL,
      status      TEXT NOT NULL CHECK (status IN ('success', 'failure', 'client_abort')),
      latency_ms  INTEGER,
      reply_chars INTEGER,
      error       TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Idempotent migrations for older databases.
    ALTER TABLE users    ADD COLUMN IF NOT EXISTS last_refill_date DATE;
    ALTER TABLE users    ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS title TEXT;

    CREATE INDEX IF NOT EXISTS idx_messages_session ON messages (session_id, id);
    CREATE INDEX IF NOT EXISTS idx_messages_created ON messages (created_at);
    CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions (user_id);
    CREATE INDEX IF NOT EXISTS idx_usage_created    ON usage_logs (created_at);
    CREATE INDEX IF NOT EXISTS idx_usage_user       ON usage_logs (user_id);
  `);
}

/* ────────────────────── Daily Refill Engine (Asia/Dhaka) ────────────────────── */

const dhakaDay = new Intl.DateTimeFormat('en-CA', {
  timeZone: REFILL_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
});
const todayInDhaka = () => dhakaDay.format(new Date());

async function ensureUser(deviceId) {
  const up = await pool.query(
    `WITH ins AS (
       INSERT INTO users (device_id) VALUES ($1)
       ON CONFLICT (device_id) DO NOTHING
       RETURNING id
     )
     SELECT u.id, u.credit_balance, u.banned FROM users u WHERE u.device_id = $1`,
    [deviceId]
  );
  const { id, credit_balance, banned } = up.rows[0];

  // Top-up-to-10, once per Dhaka day (idempotent via last_refill_date).
  const refilled = await pool.query(
    `UPDATE users
        SET credit_balance = $3, last_refill_date = $2::date
      WHERE id = $1
        AND credit_balance < $3
        AND (last_refill_date IS NULL OR last_refill_date < $2::date)
      RETURNING credit_balance`,
    [id, todayInDhaka(), DAILY_REFILL_TO]
  );

  return {
    id, banned,
    creditBalance: refilled.rows[0]?.credit_balance ?? credit_balance,
    refilledNow: refilled.rowCount > 0,
  };
}

function scheduleMidnightRefill() {
  cron.schedule('0 0 * * *', async () => {
    try {
      const { rowCount } = await pool.query(
        `UPDATE users
            SET credit_balance = $2, last_refill_date = $1::date
          WHERE credit_balance < $2
            AND (last_refill_date IS NULL OR last_refill_date < $1::date)`,
        [todayInDhaka(), DAILY_REFILL_TO]
      );
      console.log(`[refill] Midnight top-up (${REFILL_TZ}): ${rowCount} user(s) refilled to ${DAILY_REFILL_TO}.`);
    } catch (err) {
      console.error('[refill] cron failed:', err.message);
    }
  }, { timezone: REFILL_TZ });
}

/* ─────────────────────────── Moderation filter ─────────────────────────── */

function screenMessage(text) {
  const lower = text.toLowerCase();
  for (const w of BLOCKED_WORDS) {
    if (lower.includes(w)) return { blocked: true, reason: 'message violates the content policy' };
  }
  const linkCount = (lower.match(/https?:\/\//g) || []).length;
  if (linkCount > 5) return { blocked: true, reason: 'too many links (spam protection)' };
  return { blocked: false };
}

/* ─────────────────────────── Rate Limiting ─────────────────────────── */

const apiLimiter = rateLimit({
  windowMs: 5 * 60 * 1000,
  limit: 500, // generous: BD mobile CGNAT = many real users per public IP
  standardHeaders: 'draft-6',
  legacyHeaders: false,
  skip: (req) => req.method === 'OPTIONS',
  handler: (_req, res) =>
    res.status(429).json({ error: 'Too many requests. একটু পরে আবার চেষ্টা করুন।' }),
});

const chatLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 20,
  standardHeaders: 'draft-6',
  legacyHeaders: false,
  skip: (req) => req.method === 'OPTIONS',
  keyGenerator: (req) => {
    const d = typeof req.body?.device_id === 'string' ? req.body.device_id.trim() : '';
    return d ? `dev:${d.slice(0, 100)}` : req.ip;
  },
  handler: (_req, res) =>
    res.status(429).json({ error: 'You are sending messages too fast. একটু ধৈর্য ধরুন! Try again in a minute.' }),
});

const adminLimiter = rateLimit({
  windowMs: 5 * 60 * 1000,
  limit: 120,
  standardHeaders: 'draft-6',
  legacyHeaders: false,
  skip: (req) => req.method === 'OPTIONS',
  handler: (_req, res) => res.status(429).json({ error: 'Admin rate limit exceeded.' }),
});

app.use('/api', apiLimiter);

/* ─────────────────────────── Admin authentication ─────────────────────────── */

function requireAdmin(req, res, next) {
  if (!ADMIN_KEY) {
    return res.status(503).json({ error: 'Admin API disabled: ADMIN_KEY is not configured.' });
  }
  const given = Buffer.from(String(req.get('x-admin-key') || ''));
  const expected = Buffer.from(ADMIN_KEY);
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
    return res.status(401).json({ error: 'Unauthorized.' });
  }
  next();
}

app.use('/api/admin', adminLimiter, requireAdmin);

/* ──────────────────────── Streaming: SSE plumbing ──────────────────────── */

async function* sseDataEvents(res) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, nl).replace(/\r$/, '');
        buffer = buffer.slice(nl + 1);
        if (line.startsWith('data:')) yield line.slice(5).trim();
      }
    }
    buffer += decoder.decode();
    const tail = buffer.trim();
    if (tail.startsWith('data:')) yield tail.slice(5).trim();
  } finally {
    reader.cancel().catch(() => {});
  }
}

/* ──────────────────────── Streaming: provider tiers ──────────────────────── */

async function* streamOpenAICompatible({ url, apiKey, model, messages, extraHeaders = {}, signal }) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}`, ...extraHeaders },
    body: JSON.stringify({ model, messages, temperature: 0.7, max_tokens: 1024, stream: true }),
    signal,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} — ${detail.slice(0, 200)}`);
  }
  for await (const payload of sseDataEvents(res)) {
    if (payload === '[DONE]') return;
    let evt;
    try { evt = JSON.parse(payload); } catch { continue; }
    const delta = evt?.choices?.[0]?.delta?.content ?? evt?.choices?.[0]?.text;
    if (typeof delta === 'string' && delta) yield delta;
  }
}

async function* streamGemini({ apiKey, model, history, userMessage, signal }) {
  const contents = [
    ...history.map((m) => ({
      role: m.role === 'assistant' ? 'model' : 'user',
      parts: [{ text: m.content }],
    })),
    { role: 'user', parts: [{ text: userMessage }] },
  ];

  const generationConfig = { temperature: 0.7, maxOutputTokens: 1024 };
  // v3.3 FIX: Gemini 2.5 Flash "thinks" by default — several seconds of silence
  // before the first token. For a chat companion we want instant replies.
  if (model.includes('2.5-flash')) {
    generationConfig.thinkingConfig = { thinkingBudget: 0 };
  }

  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
      body: JSON.stringify({
        system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
        contents,
        generationConfig,
      }),
      signal,
    }
  );
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} — ${detail.slice(0, 200)}`);
  }
  for await (const payload of sseDataEvents(res)) {
    let evt;
    try { evt = JSON.parse(payload); } catch { continue; }
    const text = (evt?.candidates?.[0]?.content?.parts || []).map((p) => p.text || '').join('');
    if (text) yield text;
  }
}

/* ─────────── Quad-Tier Streaming Cascade (Groq → Gemini → Mistral → OpenRouter) ─────────── */

async function* streamReplyWithFallback(history, userMessage, clientSignal, state) {
  const openAIMessages = [
    { role: 'system', content: SYSTEM_PROMPT },
    ...history,
    { role: 'user', content: userMessage },
  ];

  const tiers = [];
  if (process.env.GROQ_API_KEY) {
    tiers.push({
      name: 'Groq',
      run: (signal) => streamOpenAICompatible({
        url: 'https://api.groq.com/openai/v1/chat/completions',
        apiKey: process.env.GROQ_API_KEY, model: MODELS.groq, messages: openAIMessages, signal,
      }),
    });
  }
  if (process.env.GEMINI_API_KEY) {
    tiers.push({
      name: 'Gemini',
      run: (signal) => streamGemini({
        apiKey: process.env.GEMINI_API_KEY, model: MODELS.gemini, history, userMessage, signal,
      }),
    });
  }
  if (process.env.MISTRAL_API_KEY) {
    tiers.push({
      name: 'Mistral',
      run: (signal) => streamOpenAICompatible({
        url: 'https://api.mistral.ai/v1/chat/completions',
        apiKey: process.env.MISTRAL_API_KEY, model: MODELS.mistral, messages: openAIMessages, signal,
      }),
    });
  }
  if (process.env.OPENROUTER_API_KEY) {
    tiers.push({
      name: 'OpenRouter',
      run: (signal) => streamOpenAICompatible({
        url: 'https://openrouter.ai/api/v1/chat/completions',
        apiKey: process.env.OPENROUTER_API_KEY, model: MODELS.openrouter,
        messages: openAIMessages, extraHeaders: { 'X-Title': 'Bondhu AI' }, signal,
      }),
    });
  }

  if (tiers.length === 0) {
    throw httpError(500, 'No AI provider keys configured', 'ALL_TIERS_FAILED');
  }

  const failures = [];
  for (const tier of tiers) {
    if (clientSignal.aborted) throw httpError(499, 'client disconnected', 'CLIENT_DISCONNECTED');

    const ac = new AbortController();
    const onClientAbort = () => ac.abort(new Error('client disconnected'));
    clientSignal.addEventListener('abort', onClientAbort, { once: true });

    let watchdog = setTimeout(
      () => ac.abort(new Error('watchdog: no data for ' + TIER_TIMEOUT_MS + 'ms')),
      TIER_TIMEOUT_MS
    );
    const resetWatchdog = () => {
      clearTimeout(watchdog);
      watchdog = setTimeout(() => ac.abort(new Error('watchdog: stream stalled')), TIER_TIMEOUT_MS);
    };

    const tierStart = Date.now();
    let emitted = 0, chars = 0, errMsg = null, outcome = 'incomplete';

    try {
      for await (const chunk of tier.run(ac.signal)) {
        resetWatchdog();
        emitted++;
        chars += chunk.length;
        yield chunk;
      }
      if (emitted === 0) throw new Error('provider returned an empty stream');
      outcome = 'success';
      return; // full success — cascade stops
    } catch (err) {
      errMsg = err.message;
      outcome = clientSignal.aborted ? 'client_abort' : 'failure';
      if (clientSignal.aborted) {
        throw httpError(499, 'client disconnected', 'CLIENT_DISCONNECTED');
      }
      if (emitted > 0 && state.clientReceived) {
        // Words are already on the user's screen — retrying would duplicate.
        throw httpError(503, `${tier.name} failed mid-stream`, 'MID_STREAM_FAILURE');
      }
      if (emitted > 0) state.onDiscard?.(); // discard partial text before retrying
      failures.push(`${tier.name}: ${err.message}`);
      console.warn(`[AI] Tier "${tier.name}" failed → falling back. (${err.message})`);
    } finally {
      clearTimeout(watchdog);
      clientSignal.removeEventListener('abort', onClientAbort);
      // 'incomplete' = consumer broke out of its read loop (client closed tab).
      if (outcome === 'incomplete') outcome = 'client_abort';
      try {
        state.logUsage?.({
          provider: tier.name,
          status: outcome,
          latency_ms: Date.now() - tierStart,
          reply_chars: chars,
          error: errMsg,
        });
      } catch { /* observability must never break chat */ }
    }
  }

  console.warn('[AI] All tiers failed →', failures.join(' | '));
  throw httpError(503, 'All AI providers failed', 'ALL_TIERS_FAILED');
}

/* ───────────────────────── Word-by-Word Emitter ───────────────────────── */

function createWordEmitter(emit) {
  const WORD_RE = /^\s*\S+\s/; // leading whitespace + word + one trailing whitespace char
  let pending = '';

  const drain = () => {
    let m;
    while ((m = WORD_RE.exec(pending)) !== null) {
      emit(m[0]);
      pending = pending.slice(m[0].length);
    }
  };

  return {
    push(text) {
      if (!text) return;
      pending += text;
      drain();
      // Safeguard: models occasionally emit one giant unbroken token (code, URLs).
      if (pending.length > WORD_BUFFER_LIMIT) {
        emit(pending);
        pending = '';
      }
    },
    flush() { if (pending) { emit(pending); pending = ''; } },
    reset() { pending = ''; },
  };
}

/* ──────────────────────── Chat turn: shared pieces ──────────────────────── */

function makeUsageLogger(p) {
  return (e) => {
    pool.query(
      `INSERT INTO usage_logs (user_id, session_id, provider, status, latency_ms, reply_chars, error)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [p.userId, p.sessionId, e.provider, e.status,
       Number.isFinite(e.latency_ms) ? e.latency_ms : null,
       Number.isFinite(e.reply_chars) ? e.reply_chars : null,
       e.error ? String(e.error).slice(0, 300) : null]
    ).catch(() => {});
  };
}

// Refunds are once-per-turn (p.refunded flag) so overlapping failure paths
// can never double-refund.
function refundOnce(p) {
  if (p.refunded) return;
  p.refunded = true;
  console.log(`[credit] refunded 1 credit to user ${p.userId} (failed turn)`);
  return pool.query(
    'UPDATE users SET credit_balance = credit_balance + 1 WHERE id = $1',
    [p.userId]
  ).catch((e) => console.error('[refund] failed:', e.message));
}

// Removes sessions left with zero messages by a failed turn. The NOT EXISTS
// guard makes it a no-op for sessions with real history.
function cleanupEmptySession(sessionId) {
  pool.query(
    `DELETE FROM sessions
      WHERE id = $1
        AND NOT EXISTS (SELECT 1 FROM messages WHERE session_id = $1)`,
    [sessionId]
  ).catch(() => {});
}

/**
 * Runs BEFORE any SSE headers are written, so validation / 402 / 403 errors
 * still return proper JSON status codes. The credit is CHARGED here — as the
 * LAST step, after every query that can fail — so no error path above can
 * lose a charged credit, and no AI tokens are spent on an uncharged request.
 */
async function prepareChatRequest(body) {
  const { device_id, message, session_id, stream } = body || {};
  const deviceId = typeof device_id === 'string' ? device_id.trim() : '';
  const userMessage = typeof message === 'string' ? message.trim() : '';

  if (!deviceId || deviceId.length > MAX_DEVICE_ID_LENGTH) {
    throw httpError(400, 'A valid device_id is required.');
  }
  if (!userMessage) {
    throw httpError(400, 'Message cannot be empty. বার্তা খালি হতে পারবে না।');
  }
  if (userMessage.length > MAX_MESSAGE_LENGTH) {
    throw httpError(400, `Message too long (max ${MAX_MESSAGE_LENGTH} characters).`);
  }

  const screen = screenMessage(userMessage);
  if (screen.blocked) {
    throw httpError(400, `Message blocked: ${screen.reason}.`);
  }

  const user = await ensureUser(deviceId);

  if (user.banned) {
    throw httpError(403, 'আপনার অ্যাকাউন্টে প্রবেশাধিকার সীমিত হয়েছে। (Access restricted.)');
  }
  if (user.creditBalance <= 0) {
    throw httpError(402, 'আপনার ফ্রি ক্রেডিট শেষ হয়ে গেছে। রাত ১২টায় ১০টি ফ্রি মেসেজ যোগ হবে। (Out of credits — daily refill at midnight.)');
  }

  // Resolve session (create new one if absent / invalid / not owned by user).
  let sessionId = null;
  if (typeof session_id === 'string' && UUID_RE.test(session_id)) {
    const found = await pool.query(
      'SELECT id FROM sessions WHERE id = $1 AND user_id = $2',
      [session_id, user.id]
    );
    if (found.rowCount > 0) sessionId = found.rows[0].id;
  }
  if (!sessionId) {
    const created = await pool.query(
      'INSERT INTO sessions (user_id) VALUES ($1) RETURNING id',
      [user.id]
    );
    sessionId = created.rows[0].id;
  }

  // Multi-turn memory: last 10 messages, chronological order.
  const historyResult = await pool.query(
    `SELECT role, content FROM (
       SELECT id, role, content FROM messages
       WHERE session_id = $1
       ORDER BY id DESC
       LIMIT $2
     ) recent
     ORDER BY id ASC`,
    [sessionId, HISTORY_LIMIT]
  );

  // ── CHARGE (last step: nothing above this line can fail after money moves) ──
  const charge = await pool.query(
    `UPDATE users SET credit_balance = credit_balance - 1
      WHERE id = $1 AND credit_balance > 0
      RETURNING credit_balance`,
    [user.id]
  );
  if (charge.rowCount === 0) {
    // Drained by a concurrent request between the check above and here.
    throw httpError(402, 'আপনার ফ্রি ক্রেডিট শেষ হয়ে গেছে। (Out of credits.)');
  }

  return {
    userId: user.id,
    balance: charge.rows[0].credit_balance, // post-charge balance
    refilledNow: user.refilledNow,
    refunded: false,
    sessionId,
    history: historyResult.rows,
    userMessage,
    // Strict parse: false / "false" / 0 / "0" → JSON; anything else → SSE.
    wantsStream: !(stream === false || stream === 'false' || stream === 0 || stream === '0'),
  };
}

// Persist only — the credit was already charged in prepareChatRequest.
async function persistTurn(sessionId, userMessage, assistantReply) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      'INSERT INTO messages (session_id, role, content) VALUES ($1, $2, $3)',
      [sessionId, 'user', userMessage]
    );
    await client.query(
      'INSERT INTO messages (session_id, role, content) VALUES ($1, $2, $3)',
      [sessionId, 'assistant', assistantReply]
    );
    // Auto-title: first user message, capped at 60 chars (codepoint-safe slice).
    await client.query(
      'UPDATE sessions SET title = $2 WHERE id = $1 AND title IS NULL',
      [sessionId, [...userMessage].slice(0, 60).join('')]
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/* ─────────────────────── Response Mode: legacy JSON ─────────────────────── */

// `stream: false` keeps the v1 contract: { reply, session_id, remaining_credits }.
// Nothing is surfaced incrementally here, so a tier that dies mid-generation CAN
// still be fully retried on the next tier.
async function respondJSON(res, p, clientAc) {
  if (clientAc.signal.aborted) {
    refundOnce(p);
    cleanupEmptySession(p.sessionId);
    return res.status(499).json({ error: 'Request cancelled.' });
  }

  let reply = '';
  const state = {
    clientReceived: false,
    onDiscard: () => { reply = ''; },
    logUsage: makeUsageLogger(p),
  };

  try {
    for await (const chunk of streamReplyWithFallback(p.history, p.userMessage, clientAc.signal, state)) {
      reply += chunk;
    }
  } catch (err) {
    // Our failures refund; a deliberate mid-generation disconnect does not.
    if (err.code === 'CLIENT_DISCONNECTED') {
      cleanupEmptySession(p.sessionId);
    } else {
      refundOnce(p);
      cleanupEmptySession(p.sessionId);
    }
    if (err.code === 'ALL_TIERS_FAILED' || err.code === 'MID_STREAM_FAILURE' || err.code === 'CLIENT_DISCONNECTED') {
      return res.status(503).json({
        error: 'Bondhu AI এখন ব্যস্ত। একটু পরে আবার চেষ্টা করুন। (All providers unavailable — please retry.)',
      });
    }
    console.error('[chat:json]', err.message);
    return res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }

  if (!reply.trim()) {
    refundOnce(p);
    cleanupEmptySession(p.sessionId);
    return res.status(503).json({ error: 'Bondhu AI returned an empty reply. আবার চেষ্টা করুন।' });
  }

  try {
    await persistTurn(p.sessionId, p.userMessage, reply);
  } catch (err) {
    console.error('[chat:json] persist:', err.message);
    refundOnce(p);            // client received nothing → refund
    cleanupEmptySession(p.sessionId);
    return res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }

  return res.json({ reply, session_id: p.sessionId, remaining_credits: p.balance });
}

/* ─────────────────────── Response Mode: SSE stream ─────────────────────── */

async function respondSSE(res, p, clientAc) {
  // Disconnected during prepare → refund + never write to a dead socket.
  if (clientAc.signal.aborted) {
    refundOnce(p);
    cleanupEmptySession(p.sessionId);
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no', // disable proxy buffering
  });
  res.flushHeaders?.();
  res.socket?.setNoDelay?.(true);

  let clientGone = false;
  const markGone = () => { clientGone = true; clientAc.abort(); };
  res.on('close', markGone); // fires on normal completion too — idempotent

  const send = (event, data) => {
    if (!clientGone && !clientAc.signal.aborted) {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    }
  };

  let heartbeat = null; // declared OUTSIDE try so finally can always clear it
  try {
    heartbeat = setInterval(() => {
      if (!clientGone) res.write(': heartbeat\n\n');
    }, HEARTBEAT_MS);

    send('meta', {
      session_id: p.sessionId,
      remaining_credits: p.balance, // post-charge balance — safe to display
      daily_refill_applied: p.refilledNow,
    });

    const state = { clientReceived: false, onDiscard: null, logUsage: makeUsageLogger(p) };
    const words = createWordEmitter((word) => {
      send('delta', { text: word });
      state.clientReceived = true;
    });
    let reply = '';
    state.onDiscard = () => { reply = ''; words.reset(); };

    for await (const chunk of streamReplyWithFallback(p.history, p.userMessage, clientAc.signal, state)) {
      if (clientGone) break; // triggers upstream cancel via the generator's finally
      reply += chunk;
      words.push(chunk);
    }
    words.flush();

    if (clientGone) {
      // Walked away mid-generation: credit stands (tokens were consumed),
      // nothing is persisted, and the empty session is cleaned up.
      cleanupEmptySession(p.sessionId);
      return;
    }

    if (!reply.trim()) {
      send('error', { error: 'Bondhu AI এখন ব্যস্ত। একটু পরে আবার চেষ্টা করুন। (Credit refunded.)' });
      refundOnce(p);
      cleanupEmptySession(p.sessionId);
      return;
    }

    try {
      await persistTurn(p.sessionId, p.userMessage, reply);
    } catch (err) {
      console.error('[chat:sse] persist:', err.message);
      // Reply was already delivered — don't send a scary 'error' after it.
      // Client keeps the text; it just wasn't saved (persisted: false).
      cleanupEmptySession(p.sessionId);
      send('done', { reply, session_id: p.sessionId, remaining_credits: p.balance, persisted: false });
      return;
    }

    send('done', { reply, session_id: p.sessionId, remaining_credits: p.balance, persisted: true });
  } catch (err) {
    if (err.code === 'CLIENT_DISCONNECTED') {
      cleanupEmptySession(p.sessionId); // no refund: generation was (partly) consumed
    } else {
      if (!clientGone) {
        if (err.code === 'ALL_TIERS_FAILED' || err.code === 'MID_STREAM_FAILURE') {
          send('error', { error: 'Bondhu AI এর সংযোগ বিচ্ছিন্ন হয়েছে। আবার চেষ্টা করুন। (Stream interrupted — credit refunded.)' });
        } else {
          console.error('[chat:sse]', err.message);
          send('error', { error: 'Internal server error. একটু পরে আবার চেষ্টা করুন। (Credit refunded.)' });
        }
      }
      refundOnce(p);
      cleanupEmptySession(p.sessionId);
    }
  } finally {
    if (heartbeat) clearInterval(heartbeat);
    if (!clientGone) res.end();
  }
}

/* ──────────────────────────────── Routes ──────────────────────────────── */

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', db: 'connected' });
  } catch {
    res.status(503).json({ status: 'degraded', db: 'unreachable' });
  }
});

// v3.3 NEW: root status page — visiting the domain in a browser no longer
// returns a confusing 404.
app.get('/', (req, res) => {
  res.json({
    status: 'online',
    message: 'Bondhu AI Backend is running!',
    version: '3.3.0-beta',
    endpoints: {
      chat: 'POST /api/chat',
      health: 'GET /health',
      credits: 'GET /api/credits?device_id=...',
      sessions: 'GET /api/sessions?device_id=...',
      history: 'GET /api/history?device_id=...&session_id=...'
    }
  });
});

// v3.3 NEW: wrong-method guard — a GET to /api/chat explains itself instead of
// returning a misleading 404 "Route not found".
app.get('/api/chat', (req, res) => {
  res.status(405).json({ error: 'Method not allowed — /api/chat accepts POST only.' });
});

app.get('/api/credits', async (req, res) => {
  try {
    const deviceId = (req.query.device_id || '').toString().trim();
    if (!deviceId || deviceId.length > MAX_DEVICE_ID_LENGTH) {
      return res.status(400).json({ error: 'A valid device_id query parameter is required.' });
    }
    const user = await ensureUser(deviceId);
    res.json({
      device_id: deviceId,
      remaining_credits: user.creditBalance,
      banned: user.banned,
      daily_refill_applied: user.refilledNow,
    });
  } catch (err) {
    console.error('[credits]', err.message);
    res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }
});

app.get('/api/history', async (req, res) => {
  try {
    const sessionId = (req.query.session_id || '').toString();
    const deviceId = (req.query.device_id || '').toString().trim();
    if (!UUID_RE.test(sessionId) || !deviceId) {
      return res.status(400).json({ error: 'session_id and device_id are required.' });
    }
    const owned = await pool.query(
      `SELECT s.id FROM sessions s JOIN users u ON u.id = s.user_id
       WHERE s.id = $1 AND u.device_id = $2`,
      [sessionId, deviceId]
    );
    if (owned.rowCount === 0) return res.status(404).json({ error: 'Session not found.' });
    const { rows } = await pool.query(
      `SELECT role, content, created_at FROM messages
       WHERE session_id = $1 ORDER BY id ASC LIMIT 100`,
      [sessionId]
    );
    res.json({ session_id: sessionId, messages: rows });
  } catch (err) {
    console.error('[history]', err.message);
    res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }
});

app.get('/api/sessions', async (req, res) => {
  try {
    const deviceId = (req.query.device_id || '').toString().trim();
    if (!deviceId || deviceId.length > MAX_DEVICE_ID_LENGTH) {
      return res.status(400).json({ error: 'A valid device_id query parameter is required.' });
    }
    const { rows } = await pool.query(
      `SELECT s.id, s.title, s.created_at,
              (SELECT COUNT(*)::int FROM messages m WHERE m.session_id = s.id) AS message_count
         FROM sessions s JOIN users u ON u.id = s.user_id
        WHERE u.device_id = $1
        ORDER BY s.created_at DESC LIMIT 50`,
      [deviceId]
    );
    res.json({ sessions: rows });
  } catch (err) {
    console.error('[sessions]', err.message);
    res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }
});

app.delete('/api/sessions/:id', async (req, res) => {
  try {
    const sessionId = (req.params.id || '').toString();
    const deviceId = (req.query.device_id || '').toString().trim();
    if (!UUID_RE.test(sessionId) || !deviceId) {
      return res.status(400).json({ error: 'A valid session_id and device_id are required.' });
    }
    const r = await pool.query(
      `DELETE FROM sessions s USING users u
        WHERE s.id = $1 AND u.device_id = $2 AND s.user_id = u.id`,
      [sessionId, deviceId]
    );
    if (r.rowCount === 0) return res.status(404).json({ error: 'Session not found.' });
    res.json({ deleted: true, session_id: sessionId });
  } catch (err) {
    console.error('[sessions:delete]', err.message);
    res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
  }
});

/* ───────────────────────────── Main Chat Endpoint ───────────────────────────── */

app.post('/api/chat', chatLimiter, async (req, res) => {
  // Attach BEFORE the first await. If the client disconnects during the DB
  // prepare phase, 'close' fires right here — a listener attached later
  // (inside the responder, after the awaits) would never fire.
  const clientAc = new AbortController();
  res.on('close', () => clientAc.abort());
  res.on('error', () => {}); // a socket error must never become an uncaught exception

  let p;
  try {
    p = await prepareChatRequest(req.body);
  } catch (err) {
    // Only our own httpError messages are safe to show clients — raw errors
    // (DB internals, provider details) must never leak.
    const safeMessage = err.status ? err.message : 'Internal server error. একটু পরে আবার চেষ্টা করুন।';
    return res.status(err.status || 500).json({ error: safeMessage });
  }

  const respond = p.wantsStream ? respondSSE : respondJSON;
  // Express 4 does NOT catch async rejections — without this .catch, any
  // future regression inside a responder becomes a process crash.
  respond(res, p, clientAc).catch((err) => {
    console.error('[chat:dispatch]', err);
    try {
      if (!res.headersSent) res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
      else res.end();
    } catch { /* socket already gone */ }
  });
});

/* ──────────────────────────── Admin API ──────────────────────────── */

app.get('/api/admin/stats', async (req, res) => {
  try {
    const core = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM users) AS total_users,
        (SELECT COUNT(*)::int FROM users WHERE banned) AS banned_users,
        (SELECT COUNT(*)::int FROM sessions) AS total_sessions,
        (SELECT COUNT(*)::int FROM messages) AS total_messages,
        (SELECT COALESCE(SUM(credit_balance), 0)::int FROM users) AS credits_outstanding,
        (SELECT COUNT(*)::int FROM messages WHERE created_at > NOW() - INTERVAL '24 hours') AS messages_24h,
        (SELECT COUNT(*)::int FROM users WHERE created_at > NOW() - INTERVAL '24 hours') AS new_users_24h
    `);
    const tiers = await pool.query(`
      SELECT provider, status, COUNT(*)::int AS count, ROUND(AVG(latency_ms))::int AS avg_latency_ms
        FROM usage_logs WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY provider, status ORDER BY provider
    `);
    res.json({ totals: core.rows[0], providers_24h: tiers.rows });
  } catch (err) {
    console.error('[admin:stats]', err.message);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

app.get('/api/admin/users', async (req, res) => {
  try {
    const q = (req.query.q || '').toString().slice(0, 200);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.min(Math.max(parseInt(req.query.offset, 10) || 0, 0), 100000);
    const { rows } = await pool.query(
      `SELECT id, device_id, credit_balance, banned, last_refill_date, created_at
         FROM users
        WHERE ($1 = '' OR device_id ILIKE '%' || $1 || '%')
        ORDER BY id DESC LIMIT $2 OFFSET $3`,
      [q, limit, offset]
    );
    res.json({ users: rows });
  } catch (err) {
    console.error('[admin:users]', err.message);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

// Query-param based (NOT path param): device_id may contain '/' or unicode.
app.get('/api/admin/user', async (req, res) => {
  try {
    const deviceId = (req.query.device_id || '').toString().trim();
    if (!deviceId) return res.status(400).json({ error: 'device_id is required.' });
    const u = await pool.query(
      `SELECT id, device_id, credit_balance, banned, last_refill_date, created_at
         FROM users WHERE device_id = $1`,
      [deviceId]
    );
    if (u.rowCount === 0) return res.status(404).json({ error: 'User not found.' });
    const s = await pool.query(
      `SELECT id, title, created_at FROM sessions WHERE user_id = $1
        ORDER BY created_at DESC LIMIT 20`,
      [u.rows[0].id]
    );
    res.json({ user: u.rows[0], sessions: s.rows });
  } catch (err) {
    console.error('[admin:user]', err.message);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

// Grant credits: {device_id, amount, mode: "add" (default) | "set"}
app.post('/api/admin/credits', async (req, res) => {
  try {
    const { device_id, amount, mode } = req.body || {};
    const deviceId = typeof device_id === 'string' ? device_id.trim() : '';
    const amt = Number(amount);
    if (!deviceId) return res.status(400).json({ error: 'device_id is required.' });
    if (!Number.isInteger(amt) || amt < 0 || amt > 1000000) {
      return res.status(400).json({ error: 'amount must be an integer between 0 and 1000000.' });
    }
    const sql = mode === 'set'
      ? `UPDATE users SET credit_balance = $2 WHERE device_id = $1 RETURNING credit_balance`
      : `UPDATE users SET credit_balance = LEAST(credit_balance + $2, 1000000)
          WHERE device_id = $1 RETURNING credit_balance`;
    const r = await pool.query(sql, [deviceId, amt]);
    if (r.rowCount === 0) return res.status(404).json({ error: 'User not found.' });
    res.json({ device_id: deviceId, credit_balance: r.rows[0].credit_balance });
  } catch (err) {
    console.error('[admin:credits]', err.message);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

// Ban/unban: {device_id, banned: true|false}
app.post('/api/admin/ban', async (req, res) => {
  try {
    const { device_id, banned } = req.body || {};
    const deviceId = typeof device_id === 'string' ? device_id.trim() : '';
    if (!deviceId) return res.status(400).json({ error: 'device_id is required.' });
    if (typeof banned !== 'boolean') {
      return res.status(400).json({ error: 'banned must be true or false.' });
    }
    const r = await pool.query(
      `UPDATE users SET banned = $2 WHERE device_id = $1 RETURNING banned`,
      [deviceId, banned]
    );
    if (r.rowCount === 0) return res.status(404).json({ error: 'User not found.' });
    res.json({ device_id: deviceId, banned: r.rows[0].banned });
  } catch (err) {
    console.error('[admin:ban]', err.message);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

/* ─────────────────────────── 404 + Error Handlers ─────────────────────────── */

app.use((req, res) => res.status(404).json({ error: 'Route not found.' }));

app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'Invalid JSON body.' });
  }
  console.error('[server]', err.message);
  res.status(500).json({ error: 'Internal server error. একটু পরে আবার চেষ্টা করুন।' });
});

/* ──────────────────────────────── Bootstrap ──────────────────────────────── */

async function main() {
  await initSchema();
  console.log('[DB] Schema ready: users, sessions, messages, usage_logs');

  scheduleMidnightRefill();
  console.log(`[refill] Daily top-up armed for 00:00 ${REFILL_TZ} (to ${DAILY_REFILL_TO}, when below ${DAILY_REFILL_TO})`);

  if (!ADMIN_KEY) console.warn('[admin] ADMIN_KEY not set — admin API returns 503 (disabled).');

  const server = app.listen(PORT, () => {
    console.log(`Bondhu AI backend v3.3 (beta) is live on port ${PORT}`);
  });

  // Async errors must never crash a beta silently or loudly.
  process.on('unhandledRejection', (reason) => {
    console.error('[unhandledRejection]', reason);
  });
  process.on('uncaughtException', (err) => {
    console.error('[uncaughtException]', err);
    process.exit(1); // let Render restart a clean instance
  });

  process.on('SIGTERM', () => {
    console.log('[server] SIGTERM received — shutting down gracefully…');
    // Long-lived SSE streams would otherwise stall server.close() forever.
    const forceExit = setTimeout(() => process.exit(0), 10000);
    forceExit.unref?.();
    server.close(async () => {
      await pool.end().catch(() => {});
      process.exit(0);
    });
  });
}

main().catch((err) => {
  console.error('[boot] Failed to start:', err.message);
  process.exit(1);
});
