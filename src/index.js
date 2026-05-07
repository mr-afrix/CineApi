/**
 * Cine API — Self-scraping Movie, TV & Anime Stream API
 * Domain: api.cine.dpdns.org
 * All endpoints versioned under /cn/v1/*
 *
 * Scrapes: FlixHQ, VidSrc.to, VidSrc.me, Rive, 2embed, Zoro/Aniwatch, Gogoanime
 * Metadata: TMDB + AniList GraphQL
 * Subtitles: OpenSubtitles public search
 * Hybrid: got-scraping (TLS spoof) → Playwright fallback for CF-protected pages
 */

import Fastify from "fastify";
import cors from "@fastify/cors";
import compress from "@fastify/compress";
import { gotScraping } from "got-scraping";
import * as cheerio from "cheerio";
import Keyv from "keyv";
import KeyvSqlite from "@keyv/sqlite";
import { chromium } from "playwright";
import { createRequire } from "module";
import { mkdir } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/* ─────────────────────────── ENV / CONSTANTS ─────────────────────────── */

const PORT = parseInt(process.env.PORT || "3000", 10);
const NODE_ENV = process.env.NODE_ENV || "development";
const TMDB_KEY = process.env.TMDB_KEY || "1739012afb6a538588d51ce8e9bded3a";
const CACHE_DB_PATH = process.env.CACHE_DB_PATH || "./data/cache.sqlite";
const PLAYWRIGHT_POOL_SIZE = parseInt(process.env.PLAYWRIGHT_POOL_SIZE || "2", 10);
const SCRAPER_TIMEOUT = parseInt(process.env.SCRAPER_TIMEOUT || "20000", 10);
const PLAYWRIGHT_TIMEOUT = parseInt(process.env.PLAYWRIGHT_TIMEOUT || "30000", 10);

const TMDB = "https://api.themoviedb.org/3";
const IMG_W500 = "https://image.tmdb.org/t/p/w500";
const IMG_ORIG = "https://image.tmdb.org/t/p/original";
const IMG_W780 = "https://image.tmdb.org/t/p/w780";
const ANILIST_GQL = "https://graphql.anilist.co";

const TTL_STREAM = 30 * 60 * 1000;       // 30 min — streams change often
const TTL_META = 24 * 60 * 60 * 1000;    // 24 hrs — metadata is stable
const TTL_SEARCH = 60 * 60 * 1000;       // 1 hr
const TTL_SUBS = 2 * 60 * 60 * 1000;     // 2 hrs

/* ─────────────────────────── USER AGENTS ─────────────────────────── */

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

/* ─────────────────────────── CACHE LAYER ─────────────────────────── */

await mkdir(path.dirname(CACHE_DB_PATH), { recursive: true }).catch(() => {});

const store = new KeyvSqlite(`sqlite://${CACHE_DB_PATH}`);
const cache = new Keyv({ store, namespace: "cine" });

cache.on("error", (err) => console.error("[cache] error:", err.message));

async function cacheGet(key) {
  try { return await cache.get(key); } catch { return null; }
}

async function cacheSet(key, value, ttl) {
  try { await cache.set(key, value, ttl); } catch (e) { console.error("[cache] set error:", e.message); }
}

async function cacheDel(key) {
  try { await cache.delete(key); } catch {}
}

async function cacheClear() {
  try { await cache.clear(); } catch {}
}

/* ─────────────────────────── API KEY REGISTRY ─────────────────────────── */

// Parse API_KEYS env: "key:tier:rpm:name,key2:tier2:rpm2:name2"
function parseApiKeys() {
  const raw = process.env.API_KEYS || "cine-2026:enterprise:600:Master,cine-pro-2026:pro:200:Pro,cine-free:free:30:Free";
  const keys = {};
  for (const entry of raw.split(",")) {
    const parts = entry.trim().split(":");
    if (parts.length >= 4) {
      const [key, tier, rpm, ...nameParts] = parts;
      keys[key] = { tier, rpm: parseInt(rpm, 10), name: nameParts.join(":") };
    }
  }
  return keys;
}

const API_KEYS = parseApiKeys();

// In-memory sliding window — backed by SQLite so it survives restarts
async function checkKey(key) {
  if (!key || !API_KEYS[key]) {
    return { ok: false, status: 401, err: "Invalid or missing API key. Append ?api=YOUR_KEY" };
  }
  const meta = API_KEYS[key];
  const now = Date.now();
  const windowKey = `ratelimit:${key}`;
  let window = (await cacheGet(windowKey)) || [];
  window = window.filter((t) => now - t < 60_000);
  if (window.length >= meta.rpm) {
    return { ok: false, status: 429, err: `Rate limit exceeded (${meta.rpm} rpm for ${meta.tier} tier)`, retry: 60 };
  }
  window.push(now);
  await cacheSet(windowKey, window, 65_000); // slightly over 1 min TTL
  return { ok: true, meta };
}

/* ─────────────────────────── REQUEST STATS ─────────────────────────── */

const START_TIME = Date.now();

const stats = {
  total: 0,
  byEndpoint: {},
  errors: 0,
  startedAt: START_TIME,
};

/* ─────────────────────────── FASTIFY SETUP ─────────────────────────── */

const app = Fastify({
  logger: NODE_ENV !== "production",
  trustProxy: true,
  disableRequestLogging: NODE_ENV === "production",
});

await app.register(cors, {
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["*"],
});

await app.register(compress, { global: true });

// Auth + stats middleware
app.addHook("onRequest", async (req, reply) => {
  // Skip auth for root, health, favicon, and explorer
  const pub = ["/", "/cn", "/cn/v1", "/favicon.ico", "/cn/v1/health"];
  if (req.method === "OPTIONS" || pub.includes(req.url?.split("?")[0])) return;

  const key = req.query?.api;
  const check = await checkKey(key);
  if (!check.ok) {
    const headers = check.retry ? { "Retry-After": String(check.retry) } : {};
    reply.code(check.status).headers(headers).send({ success: false, data: null, error: check.err });
    return;
  }
  req.keyMeta = check.meta;
  req.apiKey = key;
});

app.addHook("onResponse", async (req) => {
  stats.total++;
  const ep = req.url?.split("?")[0];
  stats.byEndpoint[ep] = (stats.byEndpoint[ep] || 0) + 1;
});

/* ─────────────────────────── RESPONSE HELPERS ─────────────────────────── */

const ok = (data) => ({ success: true, data, error: null });
const fail = (err, status = 400) => ({ success: false, data: null, error: String(err) });

/* ─────────────────────────── HTTP HELPERS ─────────────────────────── */

const BASE_HEADERS = {
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Connection": "keep-alive",
  "Upgrade-Insecure-Requests": "1",
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-User": "?1",
  "Cache-Control": "max-age=0",
};

const JSON_HEADERS = {
  "Accept": "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Connection": "keep-alive",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
};

async function httpGet(url, { headers = {}, timeout = SCRAPER_TIMEOUT, json = false, referer = "" } = {}) {
  const ua = randomUA();
  const h = {
    "User-Agent": ua,
    ...(json ? JSON_HEADERS : BASE_HEADERS),
    ...(referer ? { Referer: referer, Origin: new URL(referer).origin } : {}),
    ...headers,
  };
  const resp = await gotScraping({
    url,
    headers: h,
    timeout: { request: timeout },
    followRedirects: true,
    https: { rejectUnauthorized: false },
    retry: { limit: 2, methods: ["GET"], statusCodes: [408, 429, 500, 502, 503, 504] },
  });
  if (json) {
    try { return JSON.parse(resp.body); } catch { throw new Error(`JSON parse failed for ${url}`); }
  }
  return resp.body;
}

async function httpPost(url, { body = {}, headers = {}, timeout = SCRAPER_TIMEOUT, referer = "" } = {}) {
  const ua = randomUA();
  const h = {
    "User-Agent": ua,
    "Content-Type": "application/json",
    ...JSON_HEADERS,
    ...(referer ? { Referer: referer, Origin: new URL(referer).origin } : {}),
    ...headers,
  };
  const resp = await gotScraping({
    url,
    method: "POST",
    headers: h,
    body: JSON.stringify(body),
    timeout: { request: timeout },
    followRedirects: true,
    https: { rejectUnauthorized: false },
    retry: { limit: 1, methods: ["POST"] },
  });
  try { return JSON.parse(resp.body); } catch { return resp.body; }
}

// Check if a response is a Cloudflare challenge page
function isCfChallenge(html) {
  if (typeof html !== "string") return false;
  return (
    html.includes("cf-browser-verification") ||
    html.includes("_cf_chl_opt") ||
    html.includes("cf_chl_prog") ||
    html.includes("Checking if the site connection is secure") ||
    html.includes("DDoS protection by Cloudflare") ||
    (html.includes("Just a moment") && html.includes("cloudflare"))
  );
}

/* ─────────────────────────── PLAYWRIGHT POOL ─────────────────────────── */

class BrowserPool {
  constructor(size) {
    this.size = size;
    this.browsers = [];
    this.queue = [];
    this.ready = false;
  }

  async init() {
    const execPath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined;
    for (let i = 0; i < this.size; i++) {
      const browser = await chromium.launch({
        headless: true,
        executablePath: execPath,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-accelerated-2d-canvas",
          "--no-first-run",
          "--no-zygote",
          "--disable-gpu",
          "--disable-blink-features=AutomationControlled",
          "--user-agent=" + randomUA(),
        ],
      });
      this.browsers.push({ browser, busy: false });
    }
    this.ready = true;
    console.log(`[playwright] pool ready — ${this.size} browser(s)`);
  }

  async acquire() {
    const slot = this.browsers.find((b) => !b.busy);
    if (slot) {
      slot.busy = true;
      return slot;
    }
    // Queue the request
    return new Promise((resolve) => {
      this.queue.push(resolve);
    });
  }

  release(slot) {
    slot.busy = false;
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      slot.busy = true;
      next(slot);
    }
  }

  async getPage(url, { timeout = PLAYWRIGHT_TIMEOUT, waitFor = "networkidle", extraHeaders = {} } = {}) {
    const slot = await this.acquire();
    const context = await slot.browser.newContext({
      userAgent: randomUA(),
      viewport: { width: 1920, height: 1080 },
      extraHTTPHeaders: {
        "Accept-Language": "en-US,en;q=0.9",
        ...extraHeaders,
      },
      ignoreHTTPSErrors: true,
    });
    const page = await context.newPage();
    // Stealth: remove navigator.webdriver
    await page.addInitScript(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => undefined });
      window.chrome = { runtime: {} };
    });
    try {
      await page.goto(url, { waitUntil: waitFor, timeout });
      // Wait for CF challenge to resolve if present
      const content = await page.content();
      if (isCfChallenge(content)) {
        await page.waitForTimeout(5000);
        await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});
      }
      const html = await page.content();
      const cookies = await context.cookies();
      return { html, cookies, page, context };
    } finally {
      await context.close().catch(() => {});
      this.release(slot);
    }
  }

  async destroy() {
    for (const slot of this.browsers) {
      await slot.browser.close().catch(() => {});
    }
  }
}

const pool = new BrowserPool(PLAYWRIGHT_POOL_SIZE);

// Hybrid fetch — got-scraping first, Playwright fallback for CF pages
async function hybridGet(url, opts = {}) {
  try {
    const html = await httpGet(url, opts);
    if (!isCfChallenge(html)) return html;
    console.log(`[hybrid] CF challenge detected for ${url}, using Playwright`);
  } catch (e) {
    console.log(`[hybrid] got-scraping failed for ${url}: ${e.message}, trying Playwright`);
  }
  if (!pool.ready) throw new Error("Playwright pool not ready");
  const { html } = await pool.getPage(url, { timeout: PLAYWRIGHT_TIMEOUT });
  return html;
}

/* ─────────────────────────── TMDB LAYER ─────────────────────────── */

async function tmdb(endpoint, params = {}) {
  const cacheKey = `tmdb:${endpoint}:${JSON.stringify(params)}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const url = new URL(`${TMDB}${endpoint}`);
  url.searchParams.set("api_key", TMDB_KEY);
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== "") url.searchParams.set(k, String(v));
  }

  const data = await httpGet(url.toString(), { json: true, headers: { "Accept": "application/json" } });
  await cacheSet(cacheKey, data, TTL_META);
  return data;
}

function fmtRuntime(mins) {
  if (!mins) return null;
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function mapMovie(m) {
  if (!m) return null;
  return {
    id: m.id,
    imdb_id: m.imdb_id || m.external_ids?.imdb_id || null,
    title: m.title || m.name || null,
    original_title: m.original_title || null,
    tagline: m.tagline || null,
    overview: m.overview || null,
    poster: m.poster_path ? `${IMG_W500}${m.poster_path}` : null,
    poster_hd: m.poster_path ? `${IMG_ORIG}${m.poster_path}` : null,
    backdrop: m.backdrop_path ? `${IMG_ORIG}${m.backdrop_path}` : null,
    backdrop_w780: m.backdrop_path ? `${IMG_W780}${m.backdrop_path}` : null,
    logo: (m.images?.logos?.[0]?.file_path) ? `${IMG_W500}${m.images.logos[0].file_path}` : null,
    rating: m.vote_average ? +m.vote_average.toFixed(1) : 0,
    votes: m.vote_count || 0,
    popularity: m.popularity ? +m.popularity.toFixed(2) : 0,
    year: (m.release_date || "").slice(0, 4) || null,
    release_date: m.release_date || null,
    runtime: m.runtime || null,
    runtime_formatted: fmtRuntime(m.runtime),
    status: m.status || null,
    language: m.original_language || null,
    spoken_languages: (m.spoken_languages || []).map((l) => ({ code: l.iso_639_1, name: l.english_name })),
    budget: m.budget || null,
    revenue: m.revenue || null,
    genres: m.genre_ids
      ? m.genre_ids.map((id) => ({ id }))
      : (m.genres || []).map((g) => ({ id: g.id, name: g.name })),
    production_companies: (m.production_companies || []).map((c) => ({
      id: c.id, name: c.name, logo: c.logo_path ? `${IMG_W500}${c.logo_path}` : null, country: c.origin_country,
    })),
    production_countries: (m.production_countries || []).map((c) => ({ code: c.iso_3166_1, name: c.name })),
    belongs_to_collection: m.belongs_to_collection
      ? { id: m.belongs_to_collection.id, name: m.belongs_to_collection.name, poster: m.belongs_to_collection.poster_path ? `${IMG_W500}${m.belongs_to_collection.poster_path}` : null }
      : null,
    type: "movie",
  };
}

function mapMovieFull(r) {
  const base = mapMovie(r);
  return {
    ...base,
    imdb_id: r.imdb_id || r.external_ids?.imdb_id || null,
    cast: (r.credits?.cast || []).slice(0, 30).map((c) => ({
      id: c.id, name: c.name, character: c.character, order: c.order,
      profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null,
      known_for: c.known_for_department,
    })),
    crew: (r.credits?.crew || [])
      .filter((c) => ["Director", "Writer", "Screenplay", "Producer", "Original Music Composer", "Director of Photography"].includes(c.job))
      .map((c) => ({ id: c.id, name: c.name, job: c.job, department: c.department, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
    trailer: (() => {
      const t = (r.videos?.results || []).find((v) => v.site === "YouTube" && v.type === "Trailer");
      if (!t) return null;
      return { youtube_key: t.key, url: `https://www.youtube.com/watch?v=${t.key}`, embed: `https://www.youtube.com/embed/${t.key}`, name: t.name };
    })(),
    videos: (r.videos?.results || []).map((v) => ({
      key: v.key, name: v.name, type: v.type, site: v.site,
      url: v.site === "YouTube" ? `https://www.youtube.com/watch?v=${v.key}` : null,
    })),
    images: {
      posters: (r.images?.posters || []).slice(0, 10).map((i) => ({
        url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1,
      })),
      backdrops: (r.images?.backdrops || []).slice(0, 10).map((i) => ({
        url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height,
      })),
      logos: (r.images?.logos || []).slice(0, 5).map((i) => ({
        url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1,
      })),
    },
    keywords: (r.keywords?.keywords || []).map((k) => ({ id: k.id, name: k.name })),
    recommendations: (r.recommendations?.results || []).slice(0, 12).map(mapMovie),
    similar: (r.similar?.results || []).slice(0, 12).map(mapMovie),
    external_ids: r.external_ids || null,
    watch_providers: r["watch/providers"]?.results || null,
    release_dates: (() => {
      const us = (r.release_dates?.results || []).find((x) => x.iso_3166_1 === "US");
      const cert = us?.release_dates?.find((d) => d.certification)?.certification;
      return { rating: cert || null, all: r.release_dates?.results || [] };
    })(),
  };
}

function mapTV(s) {
  if (!s) return null;
  return {
    id: s.id,
    title: s.name || s.title || null,
    original_title: s.original_name || null,
    overview: s.overview || null,
    poster: s.poster_path ? `${IMG_W500}${s.poster_path}` : null,
    poster_hd: s.poster_path ? `${IMG_ORIG}${s.poster_path}` : null,
    backdrop: s.backdrop_path ? `${IMG_ORIG}${s.backdrop_path}` : null,
    logo: (s.images?.logos?.[0]?.file_path) ? `${IMG_W500}${s.images.logos[0].file_path}` : null,
    rating: s.vote_average ? +s.vote_average.toFixed(1) : 0,
    votes: s.vote_count || 0,
    popularity: s.popularity ? +s.popularity.toFixed(2) : 0,
    year: (s.first_air_date || "").slice(0, 4) || null,
    first_air_date: s.first_air_date || null,
    genres: s.genre_ids
      ? s.genre_ids.map((id) => ({ id }))
      : (s.genres || []).map((g) => ({ id: g.id, name: g.name })),
    language: s.original_language || null,
    type: "tv",
  };
}

function mapTVFull(r) {
  const base = mapTV(r);
  return {
    ...base,
    imdb_id: r.external_ids?.imdb_id || null,
    tagline: r.tagline || null,
    status: r.status || null,
    number_of_seasons: r.number_of_seasons || null,
    number_of_episodes: r.number_of_episodes || null,
    episode_runtime: r.episode_run_time || [],
    networks: (r.networks || []).map((n) => ({ id: n.id, name: n.name, logo: n.logo_path ? `${IMG_W500}${n.logo_path}` : null, country: n.origin_country })),
    created_by: (r.created_by || []).map((c) => ({ id: c.id, name: c.name, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
    seasons: (r.seasons || []).map((s) => ({
      id: s.id, name: s.name, season_number: s.season_number, episode_count: s.episode_count,
      poster: s.poster_path ? `${IMG_W500}${s.poster_path}` : null,
      air_date: s.air_date, overview: s.overview,
    })),
    cast: (r.credits?.cast || []).slice(0, 30).map((c) => ({
      id: c.id, name: c.name, character: c.character, order: c.order,
      profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null,
      known_for: c.known_for_department,
    })),
    crew: (r.credits?.crew || [])
      .filter((c) => ["Creator", "Executive Producer", "Producer", "Director"].includes(c.job))
      .map((c) => ({ id: c.id, name: c.name, job: c.job, department: c.department, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
    trailer: (() => {
      const t = (r.videos?.results || []).find((v) => v.site === "YouTube" && v.type === "Trailer");
      if (!t) return null;
      return { youtube_key: t.key, url: `https://www.youtube.com/watch?v=${t.key}`, embed: `https://www.youtube.com/embed/${t.key}`, name: t.name };
    })(),
    videos: (r.videos?.results || []).map((v) => ({
      key: v.key, name: v.name, type: v.type, site: v.site,
      url: v.site === "YouTube" ? `https://www.youtube.com/watch?v=${v.key}` : null,
    })),
    images: {
      posters: (r.images?.posters || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1 })),
      backdrops: (r.images?.backdrops || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height })),
      logos: (r.images?.logos || []).slice(0, 5).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1 })),
    },
    keywords: (r.keywords?.results || []).map((k) => ({ id: k.id, name: k.name })),
    recommendations: (r.recommendations?.results || []).slice(0, 12).map(mapTV),
    similar: (r.similar?.results || []).slice(0, 12).map(mapTV),
    external_ids: r.external_ids || null,
    watch_providers: r["watch/providers"]?.results || null,
    content_ratings: (() => {
      const us = (r.content_ratings?.results || []).find((x) => x.iso_3166_1 === "US");
      return { rating: us?.rating || null, all: r.content_ratings?.results || [] };
    })(),
    spoken_languages: (r.spoken_languages || []).map((l) => ({ code: l.iso_639_1, name: l.english_name })),
    production_companies: (r.production_companies || []).map((c) => ({
      id: c.id, name: c.name, logo: c.logo_path ? `${IMG_W500}${c.logo_path}` : null, country: c.origin_country,
    })),
  };
}

// Get IMDB ID from TMDB ID
async function getImdbId(tmdbId, type = "movie") {
  const cacheKey = `imdbid:${type}:${tmdbId}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  try {
    const r = await tmdb(`/${type}/${tmdbId}/external_ids`);
    const imdbId = r.imdb_id || null;
    if (imdbId) await cacheSet(cacheKey, imdbId, TTL_META);
    return imdbId;
  } catch { return null; }
}

/* ─────────────────────────── ANILIST LAYER ─────────────────────────── */

function mapAnime(m) {
  if (!m) return null;
  return {
    id: m.id,
    mal_id: m.idMal || null,
    title: m.title?.english || m.title?.romaji || m.title?.native || null,
    title_romaji: m.title?.romaji || null,
    title_native: m.title?.native || null,
    overview: m.description ? m.description.replace(/<[^>]+>/g, "") : null,
    poster: m.coverImage?.extraLarge || m.coverImage?.large || null,
    poster_medium: m.coverImage?.medium || null,
    banner: m.bannerImage || null,
    color: m.coverImage?.color || null,
    rating: m.averageScore ? +(m.averageScore / 10).toFixed(1) : 0,
    votes: m.popularity || 0,
    year: m.seasonYear || m.startDate?.year || null,
    season: m.season || null,
    status: m.status || null,
    episodes: m.episodes || null,
    episode_duration: m.duration || null,
    genres: m.genres || [],
    tags: (m.tags || []).slice(0, 10).map((t) => ({ name: t.name, category: t.category })),
    studios: (m.studios?.nodes || []).map((s) => ({ id: s.id, name: s.name, is_animation_studio: s.isAnimationStudio })),
    source: m.source || null,
    format: m.format || null,
    country: m.countryOfOrigin || null,
    trailer: m.trailer?.site === "youtube" ? {
      youtube_key: m.trailer.id,
      url: `https://www.youtube.com/watch?v=${m.trailer.id}`,
      embed: `https://www.youtube.com/embed/${m.trailer.id}`,
    } : null,
    next_airing: m.nextAiringEpisode ? {
      episode: m.nextAiringEpisode.episode,
      airing_at: new Date(m.nextAiringEpisode.airingAt * 1000).toISOString(),
    } : null,
    type: "anime",
  };
}

async function anilistQuery(query, variables = {}) {
  const cacheKey = `anilist:${query.slice(0, 40)}:${JSON.stringify(variables)}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  const data = await httpPost(ANILIST_GQL, {
    body: { query, variables },
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
  });
  if (data?.errors) throw new Error(data.errors[0]?.message || "AniList error");
  await cacheSet(cacheKey, data, TTL_META);
  return data;
}

const ANIME_FIELDS = `
  id idMal
  title { romaji english native }
  description
  coverImage { extraLarge large medium color }
  bannerImage
  averageScore popularity
  season seasonYear
  status episodes duration
  genres
  tags { name category }
  studios { nodes { id name isAnimationStudio } }
  source format countryOfOrigin
  trailer { id site }
  nextAiringEpisode { episode airingAt }
  startDate { year month day }
  endDate { year month day }
`;

/* ─────────────────────────── CRYPTO HELPERS ─────────────────────────── */

function xorDecrypt(str, key) {
  let result = "";
  for (let i = 0; i < str.length; i++) {
    result += String.fromCharCode(str.charCodeAt(i) ^ key.charCodeAt(i % key.length));
  }
  return result;
}

function aesDecrypt(encoded, key, iv) {
  const keyBuf = Buffer.from(key, "utf8");
  const ivBuf = Buffer.from(iv, "utf8");
  const decipher = crypto.createDecipheriv("aes-256-cbc", keyBuf, ivBuf);
  let dec = decipher.update(encoded, "base64", "utf8");
  dec += decipher.final("utf8");
  return dec;
}

function rc4Decrypt(key, data) {
  const S = Array.from({ length: 256 }, (_, i) => i);
  let j = 0;
  for (let i = 0; i < 256; i++) {
    j = (j + S[i] + key.charCodeAt(i % key.length)) % 256;
    [S[i], S[j]] = [S[j], S[i]];
  }
  let i = 0; j = 0;
  return data.split("").map((c) => {
    i = (i + 1) % 256;
    j = (j + S[i]) % 256;
    [S[i], S[j]] = [S[j], S[i]];
    return String.fromCharCode(c.charCodeAt(0) ^ S[(S[i] + S[j]) % 256]);
  }).join("");
}

function base64Decode(str) {
  return Buffer.from(str, "base64").toString("utf8");
}

/* ─────────────────────────── STREAM HELPERS ─────────────────────────── */

const QUALITY_RANK = { "2160p": 5, "1080p": 4, "720p": 3, "480p": 2, "360p": 1 };

function rankQuality(q) {
  const s = String(q || "").toLowerCase();
  return QUALITY_RANK[s] || 0;
}

function dedupeStreams(streams) {
  const seen = new Set();
  const out = [];
  for (const s of streams) {
    if (!s?.url || seen.has(s.url)) continue;
    seen.add(s.url);
    out.push(s);
  }
  return out.sort((a, b) => rankQuality(b.quality) - rankQuality(a.quality));
}

function makeStream(url, quality, format, provider, server = null, headers = null) {
  return {
    url,
    quality: quality || "auto",
    format: format || (url.includes(".m3u8") ? "hls" : "mp4"),
    provider,
    server: server || null,
    headers: headers || {},
    validated: true,
  };
}

async function validateStreamUrl(url) {
  try {
    const resp = await gotScraping({ url, method: "HEAD", timeout: { request: 5000 }, followRedirects: true, https: { rejectUnauthorized: false } });
    return resp.statusCode >= 200 && resp.statusCode < 400;
  } catch { return false; }
}

/* ─────────────────────────── SCRAPER: FlixHQ ─────────────────────────── */

const FLIXHQ_BASE = "https://flixhq.to";

// Known RabbitStream/UpCloud decryption — key rotates, these are current stable
const RABBIT_KEY = "8b5fa756190740d5901c4a52bccc5a39";

async function flixhqSearch(title, type = "movie") {
  try {
    const html = await hybridGet(`${FLIXHQ_BASE}/search/${encodeURIComponent(title)}`, { referer: FLIXHQ_BASE });
    const $ = cheerio.load(html);
    const results = [];
    $(".film_list-wrap .flw-item").each((_, el) => {
      const $el = $(el);
      const link = $el.find(".film-name a").attr("href") || "";
      const name = $el.find(".film-name a").text().trim();
      const typeTag = $el.find(".fdi-type").text().trim().toLowerCase();
      if (type === "movie" && !link.includes("/movie/")) return;
      if (type === "tv" && !link.includes("/tv/")) return;
      const id = link.split("-").pop();
      if (id) results.push({ id, name, link: `${FLIXHQ_BASE}${link}` });
    });
    return results;
  } catch (e) {
    console.error(`[flixhq] search error: ${e.message}`);
    return [];
  }
}

async function flixhqGetServers(contentId, isTV = false, episodeId = null) {
  try {
    const url = isTV
      ? `${FLIXHQ_BASE}/ajax/v2/episode/servers/${episodeId}`
      : `${FLIXHQ_BASE}/ajax/movie/episodes/${contentId}`;
    const data = await httpGet(url, {
      json: false,
      referer: FLIXHQ_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest", "Accept": "application/json, text/plain, */*" },
    });
    let html;
    try { html = JSON.parse(data)?.html || data; } catch { html = data; }
    const $ = cheerio.load(html);
    const servers = [];
    $(".server-item, .nav-item").each((_, el) => {
      const $el = $(el);
      const id = $el.attr("data-id") || $el.find("a").attr("data-id");
      const name = $el.find("a").text().trim() || $el.text().trim();
      if (id) servers.push({ id, name: name.toLowerCase() });
    });
    return servers;
  } catch (e) {
    console.error(`[flixhq] getServers error: ${e.message}`);
    return [];
  }
}

async function flixhqGetEpisodeId(contentId, season, episode) {
  try {
    const seasonsHtml = await httpGet(`${FLIXHQ_BASE}/ajax/v2/tv/seasons/${contentId}`, {
      referer: FLIXHQ_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    let seasonsData;
    try { seasonsData = JSON.parse(seasonsHtml); } catch { seasonsData = { html: seasonsHtml }; }
    const $s = cheerio.load(seasonsData?.html || seasonsHtml);
    const seasonItems = [];
    $s(".ss-item").each((_, el) => seasonItems.push({ id: $s(el).attr("data-id"), num: parseInt($s(el).text().trim()) }));
    const targetSeason = seasonItems.find((s) => s.num === parseInt(season)) || seasonItems[parseInt(season) - 1];
    if (!targetSeason) return null;

    const epsHtml = await httpGet(`${FLIXHQ_BASE}/ajax/v2/season/episodes/${targetSeason.id}`, {
      referer: FLIXHQ_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    let epsData;
    try { epsData = JSON.parse(epsHtml); } catch { epsData = { html: epsHtml }; }
    const $e = cheerio.load(epsData?.html || epsHtml);
    const episodes = [];
    $e(".ep-item, .eps-item").each((_, el) => {
      episodes.push({ id: $e(el).attr("data-id"), num: parseInt($e(el).attr("data-number") || $e(el).attr("title") || "") });
    });
    const targetEp = episodes.find((e) => e.num === parseInt(episode)) || episodes[parseInt(episode) - 1];
    return targetEp?.id || null;
  } catch (e) {
    console.error(`[flixhq] getEpisodeId error: ${e.message}`);
    return null;
  }
}

async function flixhqExtractSource(serverId) {
  try {
    const data = await httpGet(`${FLIXHQ_BASE}/ajax/sources/${serverId}`, {
      json: true,
      referer: FLIXHQ_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    const embedUrl = data?.link || data?.url;
    if (!embedUrl) return [];

    // Handle RabbitStream
    if (embedUrl.includes("rabbitstream") || embedUrl.includes("rapid-cloud")) {
      return await extractRabbitStream(embedUrl, FLIXHQ_BASE);
    }
    // Handle UpCloud
    if (embedUrl.includes("upcloud") || embedUrl.includes("dood") || embedUrl.includes("vidcloud")) {
      return await extractUpCloud(embedUrl, FLIXHQ_BASE);
    }
    // Generic iframe source
    return await extractGenericEmbed(embedUrl, FLIXHQ_BASE);
  } catch (e) {
    console.error(`[flixhq] extractSource error: ${e.message}`);
    return [];
  }
}

async function extractRabbitStream(embedUrl, referer) {
  try {
    const parsed = new URL(embedUrl);
    const vid = parsed.pathname.split("/").pop().split("?")[0];
    const host = parsed.origin;

    const html = await hybridGet(embedUrl, { referer });
    // Extract getSources endpoint and keys
    const keyMatch = html.match(/getSources\.php[^"']*/);
    const e1Match = html.match(/var\s+e1\s*=\s*"([^"]+)"/);
    const e2Match = html.match(/var\s+e2\s*=\s*"([^"]+)"/);

    const sourcesUrl = `${host}/ajax/embed-6/getSources?id=${vid}`;
    const sourcesData = await httpGet(sourcesUrl, {
      json: true,
      referer: embedUrl,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    if (!sourcesData) return [];

    let sources = sourcesData.sources;
    // If sources is a string, it's encrypted — decrypt it
    if (typeof sources === "string") {
      // Try RC4 with known key
      try {
        const decrypted = rc4Decrypt(RABBIT_KEY, sources);
        sources = JSON.parse(decrypted);
      } catch {
        // Try AES with extracted keys
        if (e1Match && e2Match) {
          try {
            const dec = aesDecrypt(sources, e1Match[1].padEnd(32, "0").slice(0, 32), e2Match[1].padEnd(16, "0").slice(0, 16));
            sources = JSON.parse(dec);
          } catch { sources = []; }
        } else {
          sources = [];
        }
      }
    }

    const streams = [];
    for (const s of (sources || [])) {
      if (s.file || s.url) {
        streams.push(makeStream(s.file || s.url, s.label || "auto", "hls", "flixhq", "rabbitstream", { Referer: host, Origin: host }));
      }
    }

    // Also grab tracks (subtitles embedded)
    const tracks = sourcesData.tracks || [];
    if (streams.length > 0) streams[0].subtitles_embedded = tracks.filter((t) => t.kind === "captions").length > 0;

    return streams;
  } catch (e) {
    console.error(`[rabbitstream] error: ${e.message}`);
    return [];
  }
}

async function extractUpCloud(embedUrl, referer) {
  try {
    const html = await hybridGet(embedUrl, { referer });
    const $ = cheerio.load(html);

    // Look for sources in various formats
    const scriptContent = $("script").map((_, el) => $(el).html()).get().join("\n");
    const m3u8Match = scriptContent.match(/file:\s*["']([^"']+\.m3u8[^"']*)/i)
      || scriptContent.match(/src:\s*["']([^"']+\.m3u8[^"']*)/i);
    if (m3u8Match) {
      return [makeStream(m3u8Match[1], "auto", "hls", "flixhq", "upcloud", { Referer: new URL(embedUrl).origin })];
    }

    const mp4Match = scriptContent.match(/file:\s*["']([^"']+\.mp4[^"']*)/i);
    if (mp4Match) {
      return [makeStream(mp4Match[1], "auto", "mp4", "flixhq", "upcloud", { Referer: new URL(embedUrl).origin })];
    }
    return [];
  } catch (e) {
    console.error(`[upcloud] error: ${e.message}`);
    return [];
  }
}

async function extractGenericEmbed(embedUrl, referer) {
  try {
    const html = await hybridGet(embedUrl, { referer });
    const $ = cheerio.load(html);
    const scriptContent = $("script").map((_, el) => $(el).html()).get().join("\n");
    const m3u8 = scriptContent.match(/["']([^"']+\.m3u8[^"']*)/i);
    const mp4 = scriptContent.match(/["']([^"']+\.mp4[^"']*)/i);
    const streams = [];
    if (m3u8) streams.push(makeStream(m3u8[1], "auto", "hls", "flixhq", "embed", { Referer: new URL(embedUrl).origin }));
    else if (mp4) streams.push(makeStream(mp4[1], "auto", "mp4", "flixhq", "embed", { Referer: new URL(embedUrl).origin }));
    return streams;
  } catch { return []; }
}

async function scrapeFlixHQMovie(imdbId) {
  try {
    const title = imdbId; // We'll search by IMDB ID pattern (FlixHQ accepts tt IDs in search too)
    const results = await flixhqSearch(imdbId, "movie");
    if (!results.length) return [];
    const content = results[0];
    const servers = await flixhqGetServers(content.id, false);
    const streams = [];
    for (const server of servers.slice(0, 3)) {
      const s = await flixhqExtractSource(server.id);
      streams.push(...s);
      if (streams.length >= 3) break;
    }
    return streams;
  } catch (e) {
    console.error(`[flixhq:movie] ${e.message}`);
    return [];
  }
}

async function scrapeFlixHQTV(imdbId, season, episode) {
  try {
    const results = await flixhqSearch(imdbId, "tv");
    if (!results.length) return [];
    const content = results[0];
    const episodeId = await flixhqGetEpisodeId(content.id, season, episode);
    if (!episodeId) return [];
    const servers = await flixhqGetServers(content.id, true, episodeId);
    const streams = [];
    for (const server of servers.slice(0, 3)) {
      const s = await flixhqExtractSource(server.id);
      streams.push(...s);
      if (streams.length >= 3) break;
    }
    return streams;
  } catch (e) {
    console.error(`[flixhq:tv] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: VidSrc.to ─────────────────────────── */

const VIDSRCTO_BASE = "https://vidsrc.to";

async function scrapeVidsrcTo(imdbId, season = null, episode = null) {
  try {
    const url = season
      ? `${VIDSRCTO_BASE}/embed/tv/${imdbId}/${season}/${episode}`
      : `${VIDSRCTO_BASE}/embed/movie/${imdbId}`;

    const html = await hybridGet(url, { referer: VIDSRCTO_BASE });
    const $ = cheerio.load(html);

    // Extract datastore hash for API call
    const providerEls = [];
    $("[data-hash]").each((_, el) => { providerEls.push($(el).attr("data-hash")); });

    if (!providerEls.length) {
      // Try script-based extraction
      const scriptContent = $("script").map((_, el) => $(el).html()).get().join("\n");
      const hashMatch = scriptContent.match(/data-hash="([^"]+)"/);
      if (hashMatch) providerEls.push(hashMatch[1]);
    }

    const streams = [];
    for (const hash of providerEls.slice(0, 3)) {
      try {
        const srcData = await httpGet(`${VIDSRCTO_BASE}/ajax/embed/episode/${hash}/sources`, {
          json: true, referer: url, headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        for (const src of (srcData || [])) {
          const detail = await httpGet(`${VIDSRCTO_BASE}/ajax/embed/source/${src.id}`, {
            json: true, referer: url, headers: { "X-Requested-With": "XMLHttpRequest" },
          });
          if (detail?.url) {
            // XOR decode the URL if needed
            let streamUrl = detail.url;
            try {
              if (!streamUrl.startsWith("http")) {
                streamUrl = xorDecrypt(base64Decode(streamUrl), "WXrUARXb1aDLaZjI");
              }
            } catch {}
            if (streamUrl.startsWith("http")) {
              streams.push(makeStream(streamUrl, src.quality || "auto", streamUrl.includes(".m3u8") ? "hls" : "mp4", "vidsrc.to", src.title || null, { Referer: VIDSRCTO_BASE }));
            }
          }
        }
        if (streams.length > 0) break;
      } catch {}
    }
    return streams;
  } catch (e) {
    console.error(`[vidsrc.to] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: VidSrc.me ─────────────────────────── */

const VIDSRCME_BASE = "https://vidsrc.me";

async function scrapeVidsrcMe(imdbId, season = null, episode = null) {
  try {
    const url = season
      ? `${VIDSRCME_BASE}/embed/tv?imdb=${imdbId}&season=${season}&episode=${episode}`
      : `${VIDSRCME_BASE}/embed/movie?imdb=${imdbId}`;

    const html = await hybridGet(url, { referer: VIDSRCME_BASE });
    const $ = cheerio.load(html);

    // Find the srcrcs iframe
    const iframeSrc = $("iframe#player_iframe, iframe.vidplay").attr("src") || "";
    if (!iframeSrc) return [];

    const iframeUrl = iframeSrc.startsWith("//") ? `https:${iframeSrc}` : iframeSrc;
    const iframeHtml = await hybridGet(iframeUrl, { referer: url });
    const $2 = cheerio.load(iframeHtml);

    // Extract encoded source from data attribute
    const dataHash = $2("[data-hash]").attr("data-hash")
      || iframeHtml.match(/data-hash="([^"]+)"/)?.[1];
    if (!dataHash) return [];

    // Fetch sources
    const sourcesUrl = `${new URL(iframeUrl).origin}/ajax/embed/episode/${dataHash}/sources`;
    const sourcesData = await httpGet(sourcesUrl, {
      json: true, referer: iframeUrl, headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    const streams = [];
    for (const src of (sourcesData || [])) {
      try {
        const detail = await httpGet(`${new URL(iframeUrl).origin}/ajax/embed/source/${src.id}`, {
          json: true, referer: iframeUrl, headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        if (detail?.url) {
          let streamUrl = detail.url;
          try {
            if (!streamUrl.startsWith("http")) {
              // XOR with known key
              const decoded = base64Decode(streamUrl);
              streamUrl = xorDecrypt(decoded, "WXrUARXb1aDLaZjI");
            }
          } catch {}
          if (streamUrl.startsWith("http")) {
            streams.push(makeStream(streamUrl, src.quality || "auto", streamUrl.includes(".m3u8") ? "hls" : "mp4", "vidsrc.me", src.title || null, { Referer: VIDSRCME_BASE }));
          }
        }
      } catch {}
    }
    return streams;
  } catch (e) {
    console.error(`[vidsrc.me] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: Rive ─────────────────────────── */

const RIVE_BASE = "https://rivestream.live";

async function scrapeRive(tmdbId, type = "movie", season = null, episode = null) {
  try {
    const url = type === "movie"
      ? `${RIVE_BASE}/watch?type=movie&id=${tmdbId}`
      : `${RIVE_BASE}/watch?type=tv&id=${tmdbId}&season=${season}&episode=${episode}`;

    const html = await hybridGet(url, { referer: RIVE_BASE });
    const $ = cheerio.load(html);

    // Extract __NEXT_DATA__ JSON
    const nextDataRaw = $("#__NEXT_DATA__").html() || $("script#__NEXT_DATA__").html();
    if (!nextDataRaw) return [];

    let nextData;
    try { nextData = JSON.parse(nextDataRaw); } catch { return []; }

    // Navigate to sources in Next.js page props
    const pageProps = nextData?.props?.pageProps || {};
    const sources = pageProps?.sources || pageProps?.streams || pageProps?.data?.sources || [];

    if (Array.isArray(sources) && sources.length > 0) {
      return sources.map((s) => makeStream(s.url || s.file, s.quality || s.label || "auto", (s.url || s.file || "").includes(".m3u8") ? "hls" : "mp4", "rive", s.server || null, { Referer: RIVE_BASE, Origin: RIVE_BASE }));
    }

    // Fallback: try the API endpoint directly
    const apiUrl = type === "movie"
      ? `${RIVE_BASE}/api/backendfetch?requestID=movie-${tmdbId}`
      : `${RIVE_BASE}/api/backendfetch?requestID=tv-${tmdbId}-${season}-${episode}`;
    const apiData = await httpGet(apiUrl, {
      json: true, referer: url,
      headers: { "X-Requested-With": "XMLHttpRequest", "Accept": "application/json" },
    });

    const apiSources = apiData?.data?.sources || apiData?.sources || [];
    return apiSources.map((s) => makeStream(s.url || s.file, s.quality || "auto", (s.url || "").includes(".m3u8") ? "hls" : "mp4", "rive", null, { Referer: RIVE_BASE }));
  } catch (e) {
    console.error(`[rive] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: 2embed ─────────────────────────── */

const TWOEMBED_BASE = "https://www.2embed.cc";

async function scrape2Embed(imdbId, season = null, episode = null) {
  try {
    const url = season
      ? `${TWOEMBED_BASE}/embedtv/${imdbId}&s=${season}&e=${episode}`
      : `${TWOEMBED_BASE}/embed/${imdbId}`;

    const html = await hybridGet(url, { referer: TWOEMBED_BASE });
    const $ = cheerio.load(html);

    // Extract all iframes and find the video one
    const iframes = [];
    $("iframe").each((_, el) => {
      const src = $(el).attr("src") || $(el).attr("data-src") || "";
      if (src && (src.includes("stream") || src.includes("embed") || src.includes("vid") || src.includes("play"))) {
        iframes.push(src.startsWith("//") ? `https:${src}` : src.startsWith("http") ? src : `${TWOEMBED_BASE}${src}`);
      }
    });

    if (!iframes.length) {
      // Check scripts for source URLs
      const scripts = $("script").map((_, el) => $(el).html()).get().join("\n");
      const m3u8 = scripts.match(/["']([^"']+\.m3u8[^"']*)/i);
      if (m3u8) return [makeStream(m3u8[1], "auto", "hls", "2embed", null, { Referer: TWOEMBED_BASE })];
      return [];
    }

    const streams = [];
    for (const iframeSrc of iframes.slice(0, 3)) {
      try {
        const frameHtml = await hybridGet(iframeSrc, { referer: url });
        const $f = cheerio.load(frameHtml);
        const scripts = $f("script").map((_, el) => $f(el).html()).get().join("\n");
        const m3u8 = scripts.match(/["']([^"']+\.m3u8[^"']*)/i) || scripts.match(/file:\s*["']([^"']+\.m3u8[^"']*)/i);
        const mp4 = scripts.match(/file:\s*["']([^"']+\.mp4[^"']*)/i);
        const origin = new URL(iframeSrc).origin;
        if (m3u8) streams.push(makeStream(m3u8[1], "auto", "hls", "2embed", null, { Referer: origin, Origin: origin }));
        else if (mp4) streams.push(makeStream(mp4[1], "auto", "mp4", "2embed", null, { Referer: origin, Origin: origin }));
      } catch {}
      if (streams.length > 0) break;
    }
    return streams;
  } catch (e) {
    console.error(`[2embed] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: Zoro/Aniwatch (Anime) ─────────────────────────── */

const ZORO_BASE = "https://aniwatch.to";

// Known Zoro/Aniwatch AES key (stable, publicly documented)
const ZORO_KEY = "8z+sQGfJ0e7UYU0nJmJeKXXZdHHf0M8KdHHf0M8KBEE=";

async function zoroSearch(title) {
  try {
    const html = await hybridGet(`${ZORO_BASE}/search?keyword=${encodeURIComponent(title)}`, { referer: ZORO_BASE });
    const $ = cheerio.load(html);
    const results = [];
    $(".flw-item").each((_, el) => {
      const $el = $(el);
      const link = $el.find(".film-name a").attr("href") || "";
      const name = $el.find(".film-name a").text().trim();
      const id = link.split("-").pop().split("?")[0];
      if (id && name) results.push({ id, name, link: `${ZORO_BASE}${link}` });
    });
    return results;
  } catch (e) {
    console.error(`[zoro] search error: ${e.message}`);
    return [];
  }
}

async function zoroGetEpisodes(animeId) {
  try {
    const data = await httpGet(`${ZORO_BASE}/ajax/v2/episode/list/${animeId}`, {
      json: false, referer: `${ZORO_BASE}/watch/${animeId}`,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    let html;
    try { html = JSON.parse(data)?.html || data; } catch { html = data; }
    const $ = cheerio.load(html);
    const episodes = [];
    $(".ep-item").each((_, el) => {
      const $el = $(el);
      const id = $el.attr("data-id");
      const num = parseInt($el.attr("data-number") || "0", 10);
      const title = $el.attr("title") || $el.find(".ep-name").text().trim() || `Episode ${num}`;
      if (id) episodes.push({ id, number: num, title });
    });
    return episodes;
  } catch (e) {
    console.error(`[zoro] getEpisodes error: ${e.message}`);
    return [];
  }
}

async function zoroGetServers(episodeId) {
  try {
    const data = await httpGet(`${ZORO_BASE}/ajax/v2/episode/servers?episodeId=${episodeId}`, {
      json: false, referer: ZORO_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    let html;
    try { html = JSON.parse(data)?.html || data; } catch { html = data; }
    const $ = cheerio.load(html);
    const servers = [];
    $(".server-item").each((_, el) => {
      const $el = $(el);
      const id = $el.attr("data-id") || $el.attr("data-server-id");
      const name = $el.text().trim().toLowerCase();
      const type = $el.attr("data-type") || "sub";
      if (id) servers.push({ id, name, type });
    });
    return servers;
  } catch (e) {
    console.error(`[zoro] getServers error: ${e.message}`);
    return [];
  }
}

async function zoroExtractSource(serverId) {
  try {
    const data = await httpGet(`${ZORO_BASE}/ajax/v2/episode/sources?id=${serverId}`, {
      json: true, referer: ZORO_BASE,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    if (!data?.link) return [];
    const embedUrl = data.link;

    // Handle RabbitStream from Zoro
    if (embedUrl.includes("megacloud") || embedUrl.includes("rapid-cloud") || embedUrl.includes("rabbitstream")) {
      return await extractRabbitStream(embedUrl, ZORO_BASE);
    }

    // Generic extraction
    return await extractGenericEmbed(embedUrl, ZORO_BASE);
  } catch (e) {
    console.error(`[zoro] extractSource error: ${e.message}`);
    return [];
  }
}

async function scrapeZoroAnime(anilistId, episodeNum, subType = "sub") {
  try {
    // Get anime title from AniList first
    const aniData = await anilistQuery(`query($id:Int){Media(id:$id,type:ANIME){id title{romaji english}}}`, { id: anilistId });
    const title = aniData?.data?.Media?.title?.english || aniData?.data?.Media?.title?.romaji;
    if (!title) return [];

    const searchResults = await zoroSearch(title);
    if (!searchResults.length) return [];

    // Try top 2 results in case first is wrong
    for (const result of searchResults.slice(0, 2)) {
      const episodes = await zoroGetEpisodes(result.id);
      const targetEp = episodes.find((e) => e.number === episodeNum) || episodes[episodeNum - 1];
      if (!targetEp) continue;

      const servers = await zoroGetServers(targetEp.id);
      // Prefer the requested sub type
      const preferred = servers.filter((s) => s.type === subType);
      const fallback = servers.filter((s) => s.type !== subType);
      const orderedServers = [...preferred, ...fallback];

      for (const server of orderedServers.slice(0, 3)) {
        const streams = await zoroExtractSource(server.id);
        if (streams.length > 0) {
          return streams.map((s) => ({ ...s, sub_type: server.type, provider: "zoro/aniwatch" }));
        }
      }
    }
    return [];
  } catch (e) {
    console.error(`[zoro] anime scrape error: ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── SCRAPER: Gogoanime ─────────────────────────── */

const GOGO_BASE = "https://gogoanime3.co";
const GOGO_AJAX = "https://ajax.gogocdn.net";
// AES-256-CBC keys — ASCII strings used as utf8 Buffers (standard gogoanime scraper keys)
const GOGO_IV   = Buffer.from("3232363936313634", "utf8");   // 16 bytes
const GOGO_KEY  = Buffer.from("37383866643737623638663538663163", "utf8"); // 32 bytes
const GOGO_SECOND_KEY = Buffer.from("33363435363133343336333036343337", "utf8"); // 32 bytes

function gogoDecrypt(data, key) {
  const decipher = crypto.createDecipheriv("aes-256-cbc", key, GOGO_IV);
  let dec = decipher.update(data, "base64", "utf8");
  dec += decipher.final("utf8");
  return dec;
}

function gogoEncrypt(data, key) {
  const cipher = crypto.createCipheriv("aes-256-cbc", key, GOGO_IV);
  let enc = cipher.update(data, "utf8", "base64");
  enc += cipher.final("base64");
  return enc;
}

async function gogoSearch(title) {
  try {
    const html = await hybridGet(`${GOGO_BASE}/search.html?keyword=${encodeURIComponent(title)}`, { referer: GOGO_BASE });
    const $ = cheerio.load(html);
    const results = [];
    $(".items li").each((_, el) => {
      const $el = $(el);
      const link = $el.find(".name a").attr("href") || "";
      const name = $el.find(".name a").text().trim();
      if (link && name) results.push({ link: `${GOGO_BASE}${link}`, name });
    });
    return results;
  } catch (e) {
    console.error(`[gogo] search error: ${e.message}`);
    return [];
  }
}

async function gogoGetEpisodes(animeLink) {
  try {
    const html = await hybridGet(animeLink, { referer: GOGO_BASE });
    const $ = cheerio.load(html);
    const animeId = $("input#movie_id").val() || $("[data-id]").attr("data-id");
    const epStart = $("input#default_ep").val() || "0";
    const epEnd = $(".anime_video_body_episodes_r a").last().text() || "1";
    if (!animeId) return [];

    const data = await httpGet(`${GOGO_AJAX}/ajax/load-list-episode?ep_start=${epStart}&ep_end=${epEnd}&id=${animeId}`, {
      json: true, referer: animeLink, headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    if (!data?.html) return [];
    const $2 = cheerio.load(data.html);
    const episodes = [];
    $2("li").each((_, el) => {
      const $el = $2(el);
      const link = $el.find("a").attr("href")?.trim();
      const numText = $el.find(".name").text().trim();
      const num = parseInt(numText.replace(/\D/g, "")) || 0;
      if (link) episodes.push({ link: `${GOGO_BASE}${link}`, number: num });
    });
    return episodes.reverse();
  } catch (e) {
    console.error(`[gogo] getEpisodes error: ${e.message}`);
    return [];
  }
}

async function gogoExtractStream(episodeLink) {
  try {
    const html = await hybridGet(episodeLink, { referer: GOGO_BASE });
    const $ = cheerio.load(html);
    const iframeSrc = $("div.play-video iframe").attr("src") || $("iframe.iframe-embed").attr("src") || "";
    if (!iframeSrc) return [];

    const iframeUrl = iframeSrc.startsWith("//") ? `https:${iframeSrc}` : iframeSrc;
    const iframeHtml = await hybridGet(iframeUrl, { referer: episodeLink });
    const $2 = cheerio.load(iframeHtml);

    // Extract crypto params
    const cryptoScript = $2("script[data-name='episode']").attr("data-value");
    if (!cryptoScript) {
      // Fallback: look for direct HLS
      const scripts = $2("script").map((_, el) => $2(el).html()).get().join("\n");
      const m3u8 = scripts.match(/["']([^"']+\.m3u8[^"']*)/i);
      if (m3u8) return [makeStream(m3u8[1], "auto", "hls", "gogoanime", null, { Referer: new URL(iframeUrl).origin })];
      return [];
    }

    const decrypted = gogoDecrypt(cryptoScript, GOGO_KEY);
    const params = new URLSearchParams(decrypted);
    const id = params.get("id") || new URL(iframeUrl).searchParams.get("id");
    if (!id) return [];

    const encId = gogoEncrypt(id, GOGO_KEY);
    const token = gogoEncrypt(`${id}&alias=${id}`, GOGO_SECOND_KEY);

    const ajaxUrl = `${new URL(iframeUrl).origin}/encrypt-ajax.php?id=${encodeURIComponent(encId)}&alias=${encodeURIComponent(token)}`;
    const ajaxData = await httpGet(ajaxUrl, {
      json: true, referer: iframeUrl,
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    if (!ajaxData?.data) return [];
    const decSources = gogoDecrypt(ajaxData.data, GOGO_SECOND_KEY);
    let sources;
    try { sources = JSON.parse(decSources); } catch { return []; }

    const streams = [];
    for (const s of (sources?.source || sources?.sources || [])) {
      const file = s.file || s.url;
      if (file) {
        streams.push(makeStream(file, s.label || s.type || "auto", file.includes(".m3u8") ? "hls" : "mp4", "gogoanime", null, { Referer: new URL(iframeUrl).origin }));
      }
    }
    return streams;
  } catch (e) {
    console.error(`[gogo] extractStream error: ${e.message}`);
    return [];
  }
}

async function scrapeGogoAnime(anilistId, episodeNum) {
  try {
    const aniData = await anilistQuery(`query($id:Int){Media(id:$id,type:ANIME){id title{romaji english}}}`, { id: anilistId });
    const title = aniData?.data?.Media?.title?.english || aniData?.data?.Media?.title?.romaji;
    if (!title) return [];

    const results = await gogoSearch(title);
    if (!results.length) return [];

    for (const result of results.slice(0, 2)) {
      const episodes = await gogoGetEpisodes(result.link);
      const targetEp = episodes.find((e) => e.number === episodeNum) || episodes[episodeNum - 1];
      if (!targetEp) continue;
      const streams = await gogoExtractStream(targetEp.link);
      if (streams.length > 0) return streams;
    }
    return [];
  } catch (e) {
    console.error(`[gogo] anime scrape error: ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── STREAM AGGREGATOR ─────────────────────────── */

async function gatherMovieStreams(imdbId, tmdbId) {
  const cacheKey = `streams:movie:${imdbId}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const started = Date.now();
  const results = await Promise.allSettled([
    scrapeFlixHQMovie(imdbId),
    scrapeVidsrcTo(imdbId),
    scrapeVidsrcMe(imdbId),
    scrapeRive(tmdbId, "movie"),
    scrape2Embed(imdbId),
  ]);

  const all = results.flatMap((r) => r.status === "fulfilled" ? r.value : []);
  const deduped = dedupeStreams(all);
  const payload = {
    streams: deduped,
    scraped_at: new Date().toISOString(),
    scraper_stats: {
      providers_tried: 5,
      providers_succeeded: results.filter((r) => r.status === "fulfilled" && r.value.length > 0).length,
      total_ms: Date.now() - started,
    },
  };

  if (deduped.length > 0) await cacheSet(cacheKey, payload, TTL_STREAM);
  return payload;
}

async function gatherTVStreams(imdbId, tmdbId, season, episode) {
  const cacheKey = `streams:tv:${imdbId}:${season}:${episode}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const started = Date.now();
  const results = await Promise.allSettled([
    scrapeFlixHQTV(imdbId, season, episode),
    scrapeVidsrcTo(imdbId, season, episode),
    scrapeVidsrcMe(imdbId, season, episode),
    scrapeRive(tmdbId, "tv", season, episode),
    scrape2Embed(imdbId, season, episode),
  ]);

  const all = results.flatMap((r) => r.status === "fulfilled" ? r.value : []);
  const deduped = dedupeStreams(all);
  const payload = {
    streams: deduped,
    scraped_at: new Date().toISOString(),
    scraper_stats: {
      providers_tried: 5,
      providers_succeeded: results.filter((r) => r.status === "fulfilled" && r.value.length > 0).length,
      total_ms: Date.now() - started,
    },
  };

  if (deduped.length > 0) await cacheSet(cacheKey, payload, TTL_STREAM);
  return payload;
}

async function gatherAnimeStreams(anilistId, episodeNum, subType = "sub") {
  const cacheKey = `streams:anime:${anilistId}:${episodeNum}:${subType}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const started = Date.now();
  const results = await Promise.allSettled([
    scrapeZoroAnime(anilistId, episodeNum, subType),
    scrapeGogoAnime(anilistId, episodeNum),
  ]);

  const all = results.flatMap((r) => r.status === "fulfilled" ? r.value : []);
  const deduped = dedupeStreams(all);
  const payload = {
    streams: deduped,
    scraped_at: new Date().toISOString(),
    scraper_stats: {
      providers_tried: 2,
      providers_succeeded: results.filter((r) => r.status === "fulfilled" && r.value.length > 0).length,
      total_ms: Date.now() - started,
    },
  };

  if (deduped.length > 0) await cacheSet(cacheKey, payload, TTL_STREAM);
  return payload;
}

/* ─────────────────────────── SUBTITLE SCRAPER ─────────────────────────── */

async function scrapeSubtitles(imdbId, season = null, episode = null, lang = "en") {
  const cacheKey = `subs:${imdbId}:${season}:${episode}:${lang}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  try {
    // OpenSubtitles public search (no API key needed for basic search)
    const params = new URLSearchParams({
      imdb_id: imdbId.replace("tt", ""),
      languages: lang,
      ...(season ? { season_number: season, episode_number: episode } : {}),
    });
    const data = await httpGet(`https://rest.opensubtitles.org/search/${params.toString()}`, {
      json: true,
      headers: {
        "X-User-Agent": "TemporaryUserAgent",
        "Accept": "application/json",
      },
    });

    const subs = (Array.isArray(data) ? data : []).slice(0, 20).map((s) => ({
      id: s.IDSubtitleFile,
      url: s.SubDownloadLink,
      lang: s.SubLanguageID,
      lang_name: s.LanguageName,
      label: s.MovieReleaseName || s.LanguageName,
      format: s.SubFormat?.toLowerCase() || "srt",
      encoding: s.SubEncoding || "UTF-8",
      hi: s.SubHearingImpaired === "1",
      rating: parseFloat(s.SubRating) || 0,
      downloads: parseInt(s.SubDownloadsCnt) || 0,
    }));

    await cacheSet(cacheKey, subs, TTL_SUBS);
    return subs;
  } catch (e) {
    console.error(`[subtitles] ${e.message}`);
    return [];
  }
}

/* ─────────────────────────── ROUTES ─────────────────────────── */

// Helper to register route with consistent error handling
function route(method, path, handler) {
  app[method.toLowerCase()](path, async (req, reply) => {
    try {
      const q = { ...req.query, ...req.params };
      const result = await handler(q, req);
      return reply.send(ok(result));
    } catch (e) {
      stats.errors++;
      console.error(`[route ${method} ${path}] ${e.message}`);
      return reply.code(500).send(fail(e.message || "Internal server error"));
    }
  });
}

/* ── MOVIES ── */

route("GET", "/cn/v1/movie/search", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const cacheKey = `search:movie:${q}:${page}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  const r = await tmdb("/search/movie", { query: q, page, include_adult: false });
  const result = { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapMovie) };
  await cacheSet(cacheKey, result, TTL_SEARCH);
  return result;
});

route("GET", "/cn/v1/movie/details", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const cacheKey = `details:movie:${id}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  const r = await tmdb(`/movie/${id}`, {
    append_to_response: "credits,videos,recommendations,similar,images,keywords,external_ids,watch/providers,release_dates",
  });
  if (r.status_code === 34) throw new Error("Movie not found");
  const result = mapMovieFull(r);
  await cacheSet(cacheKey, result, TTL_META);
  return result;
});

route("GET", "/cn/v1/movie/stream", async ({ id, quality }) => {
  if (!id) throw new Error("Missing id");
  // Accept TMDB ID or IMDB ID
  let imdbId = String(id).startsWith("tt") ? id : await getImdbId(id, "movie");
  if (!imdbId) throw new Error("Could not resolve IMDB ID for this movie");
  const payload = await gatherMovieStreams(imdbId, id);
  let streams = payload.streams;
  if (quality) {
    const q = quality.toLowerCase();
    const matching = streams.filter((s) => String(s.quality).toLowerCase() === q);
    const rest = streams.filter((s) => String(s.quality).toLowerCase() !== q);
    streams = [...matching, ...rest];
  }
  return { id, imdb_id: imdbId, type: "movie", count: streams.length, streams, ...payload.scraper_stats, scraped_at: payload.scraped_at };
});

route("GET", "/cn/v1/movie/trending", async ({ page = 1, window = "week" }) => {
  const r = await tmdb(`/trending/movie/${window === "day" ? "day" : "week"}`, { page });
  return { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/popular", async ({ page = 1 }) => {
  const r = await tmdb("/movie/popular", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/top-rated", async ({ page = 1 }) => {
  const r = await tmdb("/movie/top_rated", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/new-releases", async ({ page = 1, region = "US" }) => {
  const r = await tmdb("/movie/now_playing", { page, region });
  return { page: r.page, total_pages: r.total_pages, dates: r.dates, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/upcoming", async ({ page = 1, region = "US" }) => {
  const r = await tmdb("/movie/upcoming", { page, region });
  return { page: r.page, total_pages: r.total_pages, dates: r.dates, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/featured", async () => {
  const r = await tmdb("/trending/movie/day", { page: 1 });
  const list = (r.results || []).map(mapMovie);
  return { featured: list[0] || null, more: list.slice(1, 10) };
});

route("GET", "/cn/v1/movie/by-genre", async ({ genre, page = 1, sort_by = "popularity.desc", year }) => {
  if (!genre) throw new Error("Missing genre");
  const g = await tmdb("/genre/movie/list");
  const found = (g.genres || []).find((x) => x.name.toLowerCase() === String(genre).toLowerCase() || String(x.id) === String(genre));
  if (!found) throw new Error("Unknown genre");
  const params = { with_genres: found.id, page, sort_by };
  if (year) params.primary_release_year = year;
  const r = await tmdb("/discover/movie", params);
  return { genre: { id: found.id, name: found.name }, page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/genres", async () => {
  const r = await tmdb("/genre/movie/list");
  return { genres: r.genres || [] };
});

route("GET", "/cn/v1/movie/recommendations", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/recommendations`, { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/cast", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/credits`);
  return {
    cast: (r.cast || []).map((c) => ({ id: c.id, name: c.name, character: c.character, order: c.order, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null, known_for: c.known_for_department })),
    crew: (r.crew || []).filter((c) => ["Director", "Writer", "Screenplay", "Producer"].includes(c.job))
      .map((c) => ({ id: c.id, name: c.name, job: c.job, department: c.department, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
  };
});

route("GET", "/cn/v1/movie/trailer", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/videos`);
  const videos = r.results || [];
  const trailer = videos.find((v) => v.site === "YouTube" && v.type === "Trailer") || videos.find((v) => v.site === "YouTube") || null;
  const teasers = videos.filter((v) => v.site === "YouTube" && v.type === "Teaser").slice(0, 3);
  return {
    trailer: trailer ? { youtube_key: trailer.key, url: `https://www.youtube.com/watch?v=${trailer.key}`, embed: `https://www.youtube.com/embed/${trailer.key}`, name: trailer.name } : null,
    teasers: teasers.map((t) => ({ youtube_key: t.key, url: `https://www.youtube.com/watch?v=${t.key}`, name: t.name })),
    all_videos: videos.map((v) => ({ key: v.key, name: v.name, type: v.type, site: v.site })),
  };
});

route("GET", "/cn/v1/movie/related", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/similar`, { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapMovie) };
});

route("GET", "/cn/v1/movie/collection", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/collection/${id}`);
  return {
    id: r.id, name: r.name, overview: r.overview,
    poster: r.poster_path ? `${IMG_ORIG}${r.poster_path}` : null,
    backdrop: r.backdrop_path ? `${IMG_ORIG}${r.backdrop_path}` : null,
    parts: (r.parts || []).sort((a, b) => (a.release_date || "").localeCompare(b.release_date || "")).map(mapMovie),
  };
});

route("GET", "/cn/v1/movie/images", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/images`);
  return {
    posters: (r.posters || []).slice(0, 20).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1, rating: i.vote_average })),
    backdrops: (r.backdrops || []).slice(0, 20).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, rating: i.vote_average })),
    logos: (r.logos || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1 })),
  };
});

route("GET", "/cn/v1/movie/reviews", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/reviews`, { page });
  return {
    page: r.page, total_pages: r.total_pages, total_results: r.total_results,
    results: (r.results || []).map((rv) => ({
      id: rv.id, author: rv.author, rating: rv.author_details?.rating || null,
      avatar: rv.author_details?.avatar_path ? (rv.author_details.avatar_path.startsWith("/https") ? rv.author_details.avatar_path.slice(1) : `${IMG_W500}${rv.author_details.avatar_path}`) : null,
      content: rv.content, created_at: rv.created_at, url: rv.url,
    })),
  };
});

route("GET", "/cn/v1/movie/keywords", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/keywords`);
  return { keywords: (r.keywords || []).map((k) => ({ id: k.id, name: k.name })) };
});

route("GET", "/cn/v1/movie/watch-providers", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/movie/${id}/watch/providers`);
  return { results: r.results || {} };
});

route("GET", "/cn/v1/movie/discover", async ({ page = 1, sort_by = "popularity.desc", genre, year, min_rating, language, with_cast, with_company }) => {
  const params = { page, sort_by };
  if (genre) params.with_genres = genre;
  if (year) params.primary_release_year = year;
  if (min_rating) params["vote_average.gte"] = min_rating;
  if (language) params.with_original_language = language;
  if (with_cast) params.with_cast = with_cast;
  if (with_company) params.with_companies = with_company;
  const r = await tmdb("/discover/movie", params);
  return { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapMovie) };
});

/* ── TV SHOWS ── */

route("GET", "/cn/v1/tv/search", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const cacheKey = `search:tv:${q}:${page}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  const r = await tmdb("/search/tv", { query: q, page, include_adult: false });
  const result = { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapTV) };
  await cacheSet(cacheKey, result, TTL_SEARCH);
  return result;
});

route("GET", "/cn/v1/tv/details", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const cacheKey = `details:tv:${id}`;
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;
  const r = await tmdb(`/tv/${id}`, {
    append_to_response: "credits,videos,recommendations,similar,images,keywords,external_ids,watch/providers,content_ratings",
  });
  if (r.status_code === 34) throw new Error("TV show not found");
  const result = mapTVFull(r);
  await cacheSet(cacheKey, result, TTL_META);
  return result;
});

route("GET", "/cn/v1/tv/stream", async ({ id, season = 1, episode = 1, quality }) => {
  if (!id) throw new Error("Missing id");
  let imdbId = String(id).startsWith("tt") ? id : await getImdbId(id, "tv");
  if (!imdbId) throw new Error("Could not resolve IMDB ID for this show");
  const payload = await gatherTVStreams(imdbId, id, parseInt(season), parseInt(episode));
  let streams = payload.streams;
  if (quality) {
    const q = quality.toLowerCase();
    streams = [...streams.filter((s) => String(s.quality).toLowerCase() === q), ...streams.filter((s) => String(s.quality).toLowerCase() !== q)];
  }
  return { id, imdb_id: imdbId, type: "tv", season: parseInt(season), episode: parseInt(episode), count: streams.length, streams, ...payload.scraper_stats, scraped_at: payload.scraped_at };
});

route("GET", "/cn/v1/tv/trending", async ({ page = 1, window = "week" }) => {
  const r = await tmdb(`/trending/tv/${window === "day" ? "day" : "week"}`, { page });
  return { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/popular", async ({ page = 1 }) => {
  const r = await tmdb("/tv/popular", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/top-rated", async ({ page = 1 }) => {
  const r = await tmdb("/tv/top_rated", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/on-air", async ({ page = 1 }) => {
  const r = await tmdb("/tv/on_the_air", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/airing-today", async ({ page = 1 }) => {
  const r = await tmdb("/tv/airing_today", { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/seasons", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}`);
  return {
    id: r.id, title: r.name,
    number_of_seasons: r.number_of_seasons,
    number_of_episodes: r.number_of_episodes,
    seasons: (r.seasons || []).map((s) => ({
      id: s.id, name: s.name, season_number: s.season_number, episode_count: s.episode_count,
      poster: s.poster_path ? `${IMG_W500}${s.poster_path}` : null,
      air_date: s.air_date, overview: s.overview,
    })),
  };
});

route("GET", "/cn/v1/tv/episodes", async ({ id, season = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/season/${season}`);
  if (r.status_code === 34) throw new Error("Season not found");
  return {
    id: r.id, show_id: parseInt(id), season_number: r.season_number, name: r.name, overview: r.overview,
    poster: r.poster_path ? `${IMG_W500}${r.poster_path}` : null,
    air_date: r.air_date,
    episodes: (r.episodes || []).map((e) => ({
      id: e.id, episode_number: e.episode_number, name: e.name, overview: e.overview,
      air_date: e.air_date, runtime: e.runtime, runtime_formatted: fmtRuntime(e.runtime),
      rating: e.vote_average ? +e.vote_average.toFixed(1) : 0, votes: e.vote_count,
      still: e.still_path ? `${IMG_W780}${e.still_path}` : null,
      crew: (e.crew || []).filter((c) => c.job === "Director").map((c) => ({ id: c.id, name: c.name, job: c.job })),
      guest_stars: (e.guest_stars || []).slice(0, 5).map((g) => ({ id: g.id, name: g.name, character: g.character, profile: g.profile_path ? `${IMG_W500}${g.profile_path}` : null })),
    })),
  };
});

route("GET", "/cn/v1/tv/episode", async ({ id, season, episode }) => {
  if (!id || !season || !episode) throw new Error("Missing id, season, or episode");
  const r = await tmdb(`/tv/${id}/season/${season}/episode/${episode}`, { append_to_response: "credits,images,videos" });
  if (r.status_code === 34) throw new Error("Episode not found");
  return {
    id: r.id, show_id: parseInt(id), season_number: r.season_number, episode_number: r.episode_number,
    name: r.name, overview: r.overview, air_date: r.air_date,
    runtime: r.runtime, runtime_formatted: fmtRuntime(r.runtime),
    rating: r.vote_average ? +r.vote_average.toFixed(1) : 0, votes: r.vote_count,
    still: r.still_path ? `${IMG_ORIG}${r.still_path}` : null,
    crew: (r.crew || []).map((c) => ({ id: c.id, name: c.name, job: c.job })),
    guest_stars: (r.guest_stars || []).map((g) => ({ id: g.id, name: g.name, character: g.character, profile: g.profile_path ? `${IMG_W500}${g.profile_path}` : null })),
    cast: (r.credits?.cast || []).map((c) => ({ id: c.id, name: c.name, character: c.character, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
    images: { stills: (r.images?.stills || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height })) },
    videos: (r.videos?.results || []).map((v) => ({ key: v.key, name: v.name, type: v.type, site: v.site })),
  };
});

route("GET", "/cn/v1/tv/by-genre", async ({ genre, page = 1, sort_by = "popularity.desc" }) => {
  if (!genre) throw new Error("Missing genre");
  const g = await tmdb("/genre/tv/list");
  const found = (g.genres || []).find((x) => x.name.toLowerCase() === String(genre).toLowerCase() || String(x.id) === String(genre));
  if (!found) throw new Error("Unknown genre");
  const r = await tmdb("/discover/tv", { with_genres: found.id, page, sort_by });
  return { genre: { id: found.id, name: found.name }, page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/genres", async () => {
  const r = await tmdb("/genre/tv/list");
  return { genres: r.genres || [] };
});

route("GET", "/cn/v1/tv/recommendations", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/recommendations`, { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/cast", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/credits`);
  return {
    cast: (r.cast || []).map((c) => ({ id: c.id, name: c.name, character: c.character, order: c.order, profile: c.profile_path ? `${IMG_W500}${c.profile_path}` : null })),
    crew: (r.crew || []).filter((c) => ["Creator", "Executive Producer"].includes(c.job)).map((c) => ({ id: c.id, name: c.name, job: c.job })),
  };
});

route("GET", "/cn/v1/tv/related", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/similar`, { page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map(mapTV) };
});

route("GET", "/cn/v1/tv/trailer", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/videos`);
  const videos = r.results || [];
  const trailer = videos.find((v) => v.site === "YouTube" && v.type === "Trailer") || videos.find((v) => v.site === "YouTube") || null;
  return {
    trailer: trailer ? { youtube_key: trailer.key, url: `https://www.youtube.com/watch?v=${trailer.key}`, embed: `https://www.youtube.com/embed/${trailer.key}`, name: trailer.name } : null,
    all_videos: videos.map((v) => ({ key: v.key, name: v.name, type: v.type, site: v.site })),
  };
});

route("GET", "/cn/v1/tv/images", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/images`);
  return {
    posters: (r.posters || []).slice(0, 20).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1 })),
    backdrops: (r.backdrops || []).slice(0, 20).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height })),
    logos: (r.logos || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height, lang: i.iso_639_1 })),
  };
});

route("GET", "/cn/v1/tv/reviews", async ({ id, page = 1 }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/reviews`, { page });
  return {
    page: r.page, total_pages: r.total_pages,
    results: (r.results || []).map((rv) => ({
      id: rv.id, author: rv.author, rating: rv.author_details?.rating || null,
      content: rv.content, created_at: rv.created_at, url: rv.url,
    })),
  };
});

route("GET", "/cn/v1/tv/keywords", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/keywords`);
  return { keywords: (r.results || []).map((k) => ({ id: k.id, name: k.name })) };
});

route("GET", "/cn/v1/tv/watch-providers", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/tv/${id}/watch/providers`);
  return { results: r.results || {} };
});

route("GET", "/cn/v1/tv/discover", async ({ page = 1, sort_by = "popularity.desc", genre, year, min_rating, language, network }) => {
  const params = { page, sort_by };
  if (genre) params.with_genres = genre;
  if (year) params.first_air_date_year = year;
  if (min_rating) params["vote_average.gte"] = min_rating;
  if (language) params.with_original_language = language;
  if (network) params.with_networks = network;
  const r = await tmdb("/discover/tv", params);
  return { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results: (r.results || []).map(mapTV) };
});

/* ── ANIME ── */

route("GET", "/cn/v1/anime/search", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const data = await anilistQuery(`
    query($search:String,$page:Int){Page(page:$page,perPage:20){
      pageInfo{total currentPage lastPage hasNextPage}
      media(search:$search,type:ANIME,sort:SEARCH_MATCH){${ANIME_FIELDS}}
    }}
  `, { search: q, page: parseInt(page) });
  const page_data = data?.data?.Page;
  return { page: page_data?.pageInfo?.currentPage, total_pages: page_data?.pageInfo?.lastPage, has_next: page_data?.pageInfo?.hasNextPage, results: (page_data?.media || []).map(mapAnime) };
});

route("GET", "/cn/v1/anime/details", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const data = await anilistQuery(`
    query($id:Int){Media(id:$id,type:ANIME){
      ${ANIME_FIELDS}
      relations{edges{relationType(version:2) node{id title{romaji english} coverImage{large} type format}}}
      characters{edges{role node{id name{full} image{large} description}}}
      staff{edges{role node{id name{full} image{large}}}}
      airingSchedule{nodes{episode airingAt}}
      recommendations{nodes{rating mediaRecommendation{id title{romaji english} coverImage{large}}}}
      externalLinks{url site type}
    }}
  `, { id: parseInt(id) });
  const m = data?.data?.Media;
  if (!m) throw new Error("Anime not found");
  return {
    ...mapAnime(m),
    relations: (m.relations?.edges || []).map((e) => ({ type: e.relationType, id: e.node.id, title: e.node.title?.english || e.node.title?.romaji, format: e.node.format, media_type: e.node.type, poster: e.node.coverImage?.large })),
    characters: (m.characters?.edges || []).slice(0, 20).map((e) => ({ id: e.node.id, name: e.node.name?.full, role: e.role, image: e.node.image?.large, description: e.node.description })),
    staff: (m.staff?.edges || []).slice(0, 15).map((e) => ({ id: e.node.id, name: e.node.name?.full, role: e.role, image: e.node.image?.large })),
    airing_schedule: (m.airingSchedule?.nodes || []).map((n) => ({ episode: n.episode, airing_at: new Date(n.airingAt * 1000).toISOString() })),
    recommendations: (m.recommendations?.nodes || []).slice(0, 12).map((n) => ({ rating: n.rating, id: n.mediaRecommendation?.id, title: n.mediaRecommendation?.title?.english || n.mediaRecommendation?.title?.romaji, poster: n.mediaRecommendation?.coverImage?.large })),
    external_links: (m.externalLinks || []).map((l) => ({ url: l.url, site: l.site, type: l.type })),
  };
});

route("GET", "/cn/v1/anime/stream", async ({ id, episode = 1, sub_type = "sub" }) => {
  if (!id) throw new Error("Missing id");
  const payload = await gatherAnimeStreams(parseInt(id), parseInt(episode), sub_type);
  return { id: parseInt(id), type: "anime", episode: parseInt(episode), sub_type, count: payload.streams.length, streams: payload.streams, ...payload.scraper_stats, scraped_at: payload.scraped_at };
});

route("GET", "/cn/v1/anime/trending", async ({ page = 1 }) => {
  const data = await anilistQuery(`
    query($page:Int){Page(page:$page,perPage:20){
      pageInfo{total currentPage lastPage}
      media(type:ANIME,sort:TRENDING_DESC){${ANIME_FIELDS}}
    }}
  `, { page: parseInt(page) });
  const p = data?.data?.Page;
  return { page: p?.pageInfo?.currentPage, total_pages: p?.pageInfo?.lastPage, results: (p?.media || []).map(mapAnime) };
});

route("GET", "/cn/v1/anime/popular", async ({ page = 1 }) => {
  const data = await anilistQuery(`
    query($page:Int){Page(page:$page,perPage:20){
      pageInfo{total currentPage lastPage}
      media(type:ANIME,sort:POPULARITY_DESC){${ANIME_FIELDS}}
    }}
  `, { page: parseInt(page) });
  const p = data?.data?.Page;
  return { page: p?.pageInfo?.currentPage, total_pages: p?.pageInfo?.lastPage, results: (p?.media || []).map(mapAnime) };
});

route("GET", "/cn/v1/anime/top-rated", async ({ page = 1 }) => {
  const data = await anilistQuery(`
    query($page:Int){Page(page:$page,perPage:20){
      pageInfo{total currentPage lastPage}
      media(type:ANIME,sort:SCORE_DESC){${ANIME_FIELDS}}
    }}
  `, { page: parseInt(page) });
  const p = data?.data?.Page;
  return { page: p?.pageInfo?.currentPage, total_pages: p?.pageInfo?.lastPage, results: (p?.media || []).map(mapAnime) };
});

route("GET", "/cn/v1/anime/season", async ({ season, year, page = 1 }) => {
  const currentYear = new Date().getFullYear();
  const seasons = ["WINTER", "SPRING", "SUMMER", "FALL"];
  const s = season ? season.toUpperCase() : null;
  if (s && !seasons.includes(s)) throw new Error("season must be WINTER, SPRING, SUMMER, or FALL");
  const data = await anilistQuery(`
    query($season:MediaSeason,$year:Int,$page:Int){Page(page:$page,perPage:20){
      pageInfo{total currentPage lastPage}
      media(type:ANIME,season:$season,seasonYear:$year,sort:POPULARITY_DESC){${ANIME_FIELDS}}
    }}
  `, { season: s || null, year: year ? parseInt(year) : currentYear, page: parseInt(page) });
  const p = data?.data?.Page;
  return { season: s, year: year || currentYear, page: p?.pageInfo?.currentPage, total_pages: p?.pageInfo?.lastPage, results: (p?.media || []).map(mapAnime) };
});

route("GET", "/cn/v1/anime/episodes", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  // Get episode list from Zoro
  const aniData = await anilistQuery(`query($id:Int){Media(id:$id,type:ANIME){id episodes title{romaji english} status}}`, { id: parseInt(id) });
  const anime = aniData?.data?.Media;
  if (!anime) throw new Error("Anime not found");

  const results = await zoroSearch(anime.title?.english || anime.title?.romaji || "");
  if (!results.length) {
    // Return basic episode list from AniList data
    const count = anime.episodes || 0;
    return {
      id: parseInt(id), title: anime.title?.english || anime.title?.romaji,
      total_episodes: count, source: "anilist",
      episodes: Array.from({ length: count }, (_, i) => ({ number: i + 1, title: `Episode ${i + 1}` })),
    };
  }

  const episodes = await zoroGetEpisodes(results[0].id);
  return {
    id: parseInt(id), title: anime.title?.english || anime.title?.romaji,
    total_episodes: episodes.length, source: "zoro",
    episodes: episodes.map((e) => ({ number: e.number, title: e.title, zoro_id: e.id })),
  };
});

route("GET", "/cn/v1/anime/related", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const data = await anilistQuery(`
    query($id:Int){Media(id:$id,type:ANIME){
      relations{edges{relationType(version:2) node{${ANIME_FIELDS}}}}
    }}
  `, { id: parseInt(id) });
  const edges = data?.data?.Media?.relations?.edges || [];
  return {
    relations: edges.map((e) => ({ type: e.relationType, ...mapAnime(e.node) })),
  };
});

/* ── SUBTITLES ── */

route("GET", "/cn/v1/subtitles", async ({ id, type = "movie", season, episode, lang = "en" }) => {
  if (!id) throw new Error("Missing id");
  let imdbId = String(id).startsWith("tt") ? id : null;
  if (!imdbId) {
    imdbId = await getImdbId(id, type === "movie" ? "movie" : "tv");
  }
  if (!imdbId) throw new Error("Could not resolve IMDB ID");
  const subs = await scrapeSubtitles(imdbId, season || null, episode || null, lang);
  return { id, imdb_id: imdbId, type, count: subs.length, subtitles: subs };
});

route("GET", "/cn/v1/subtitles/languages", async ({ id, type = "movie", season, episode }) => {
  if (!id) throw new Error("Missing id");
  let imdbId = String(id).startsWith("tt") ? id : await getImdbId(id, type === "movie" ? "movie" : "tv");
  if (!imdbId) throw new Error("Could not resolve IMDB ID");
  const subs = await scrapeSubtitles(imdbId, season || null, episode || null, null);
  const langs = {};
  for (const s of subs) {
    if (!s.lang) continue;
    if (!langs[s.lang]) langs[s.lang] = { code: s.lang, name: s.lang_name, count: 0 };
    langs[s.lang].count++;
  }
  return { id, imdb_id: imdbId, languages: Object.values(langs) };
});

/* ── SEARCH ── */

route("GET", "/cn/v1/search/multi", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const r = await tmdb("/search/multi", { query: q, page, include_adult: false });
  const results = (r.results || []).map((it) => {
    if (it.media_type === "movie") return mapMovie(it);
    if (it.media_type === "tv") return mapTV(it);
    return { id: it.id, name: it.name, type: "person", popularity: it.popularity, profile: it.profile_path ? `${IMG_W500}${it.profile_path}` : null, known_for: (it.known_for || []).slice(0, 3).map((k) => ({ id: k.id, title: k.title || k.name, type: k.media_type })) };
  });
  return { page: r.page, total_pages: r.total_pages, total_results: r.total_results, results };
});

route("GET", "/cn/v1/search/suggestions", async ({ q }) => {
  if (!q) throw new Error("Missing q");
  const r = await tmdb("/search/multi", { query: q, page: 1, include_adult: false });
  return {
    suggestions: (r.results || []).slice(0, 8).map((it) => ({
      id: it.id, title: it.title || it.name, type: it.media_type,
      year: ((it.release_date || it.first_air_date) || "").slice(0, 4),
      poster: it.poster_path ? `${IMG_W500}${it.poster_path}` : it.profile_path ? `${IMG_W500}${it.profile_path}` : null,
      rating: it.vote_average ? +it.vote_average.toFixed(1) : null,
    })),
  };
});

route("GET", "/cn/v1/search/keyword", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const r = await tmdb("/search/keyword", { query: q, page });
  return { page: r.page, total_pages: r.total_pages, results: r.results || [] };
});

route("GET", "/cn/v1/search/company", async ({ q, page = 1 }) => {
  if (!q) throw new Error("Missing q");
  const r = await tmdb("/search/company", { query: q, page });
  return { page: r.page, total_pages: r.total_pages, results: (r.results || []).map((c) => ({ id: c.id, name: c.name, logo: c.logo_path ? `${IMG_W500}${c.logo_path}` : null, country: c.origin_country })) };
});

/* ── PEOPLE ── */

route("GET", "/cn/v1/person/details", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/person/${id}`, { append_to_response: "external_ids,images" });
  if (r.status_code === 34) throw new Error("Person not found");
  return {
    id: r.id, name: r.name, biography: r.biography, birthday: r.birthday, deathday: r.deathday,
    place_of_birth: r.place_of_birth, gender: r.gender === 1 ? "female" : r.gender === 2 ? "male" : "unknown",
    popularity: r.popularity ? +r.popularity.toFixed(2) : 0,
    profile: r.profile_path ? `${IMG_W500}${r.profile_path}` : null,
    profile_hd: r.profile_path ? `${IMG_ORIG}${r.profile_path}` : null,
    known_for: r.known_for_department, adult: r.adult,
    also_known_as: r.also_known_as || [],
    external_ids: r.external_ids || null,
    images: (r.images?.profiles || []).slice(0, 10).map((i) => ({ url: `${IMG_ORIG}${i.file_path}`, width: i.width, height: i.height })),
  };
});

route("GET", "/cn/v1/person/filmography", async ({ id }) => {
  if (!id) throw new Error("Missing id");
  const r = await tmdb(`/person/${id}/combined_credits`);
  return {
    id: parseInt(id),
    cast: (r.cast || []).sort((a, b) => (b.vote_count || 0) - (a.vote_count || 0)).map((c) => ({
      id: c.id, title: c.title || c.name, type: c.media_type, character: c.character, episode_count: c.episode_count,
      year: ((c.release_date || c.first_air_date) || "").slice(0, 4),
      poster: c.poster_path ? `${IMG_W500}${c.poster_path}` : null,
      rating: c.vote_average ? +c.vote_average.toFixed(1) : 0,
    })),
    crew: (r.crew || []).sort((a, b) => (b.vote_count || 0) - (a.vote_count || 0)).map((c) => ({
      id: c.id, title: c.title || c.name, type: c.media_type, job: c.job, department: c.department,
      year: ((c.release_date || c.first_air_date) || "").slice(0, 4),
      poster: c.poster_path ? `${IMG_W500}${c.poster_path}` : null,
    })),
  };
});

route("GET", "/cn/v1/person/popular", async ({ page = 1 }) => {
  const r = await tmdb("/person/popular", { page });
  return {
    page: r.page, total_pages: r.total_pages,
    results: (r.results || []).map((p) => ({
      id: p.id, name: p.name, popularity: p.popularity,
      profile: p.profile_path ? `${IMG_W500}${p.profile_path}` : null,
      known_for_department: p.known_for_department,
      known_for: (p.known_for || []).slice(0, 3).map((k) => ({ id: k.id, title: k.title || k.name, type: k.media_type })),
    })),
  };
});

/* ── CONVERT ── */

route("GET", "/cn/v1/convert/imdb/:imdb_id", async ({ imdb_id }) => {
  if (!imdb_id) throw new Error("Missing imdb_id");
  const r = await tmdb("/find/" + imdb_id, { external_source: "imdb_id" });
  const movie = (r.movie_results || [])[0];
  const tv = (r.tv_results || [])[0];
  if (!movie && !tv) throw new Error("Not found");
  const result = movie || tv;
  const type = movie ? "movie" : "tv";
  return { imdb_id, tmdb_id: result.id, type, title: result.title || result.name, year: ((result.release_date || result.first_air_date) || "").slice(0, 4) };
});

route("GET", "/cn/v1/convert/tmdb/:tmdb_id", async ({ tmdb_id, type = "movie" }) => {
  if (!tmdb_id) throw new Error("Missing tmdb_id");
  const r = await tmdb(`/${type}/${tmdb_id}/external_ids`);
  return { tmdb_id: parseInt(tmdb_id), type, imdb_id: r.imdb_id || null, tvdb_id: r.tvdb_id || null, wikidata_id: r.wikidata_id || null };
});

/* ── KEYS ── */

app.get("/cn/v1/keys/validate", async (req, reply) => {
  return reply.send(ok({ valid: true, key: req.apiKey, tier: req.keyMeta?.tier, name: req.keyMeta?.name, rate_limit_per_minute: req.keyMeta?.rpm }));
});

app.get("/cn/v1/keys/usage", async (req, reply) => {
  const windowKey = `ratelimit:${req.apiKey}`;
  const window = (await cacheGet(windowKey)) || [];
  const now = Date.now();
  const current = window.filter((t) => now - t < 60_000).length;
  return reply.send(ok({
    key: req.apiKey, tier: req.keyMeta?.tier, name: req.keyMeta?.name,
    current_minute_requests: current,
    limit_per_minute: req.keyMeta?.rpm,
    remaining: Math.max(0, (req.keyMeta?.rpm || 0) - current),
  }));
});

/* ── SYSTEM ── */

app.get("/cn/v1/health", async (req, reply) => {
  return reply.send(ok({
    status: "healthy",
    uptime_ms: Date.now() - START_TIME,
    uptime_formatted: fmtRuntime(Math.floor((Date.now() - START_TIME) / 60000)),
    version: "1.0.0",
    playwright_ready: pool.ready,
    browser_count: PLAYWRIGHT_POOL_SIZE,
  }));
});

app.get("/cn/v1/status", async (req, reply) => {
  return reply.send(ok({
    service: "Cine API", version: "1.0.0",
    uptime_ms: Date.now() - START_TIME,
    registered_keys: Object.keys(API_KEYS).length,
    total_requests: stats.total,
    error_count: stats.errors,
    top_endpoints: Object.entries(stats.byEndpoint).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([ep, count]) => ({ endpoint: ep, requests: count })),
    playwright_ready: pool.ready,
    node_version: process.version,
  }));
});

app.delete("/cn/v1/system/cache/clear", async (req, reply) => {
  // Only allow master/enterprise keys to clear cache
  if (req.keyMeta?.tier !== "enterprise") {
    return reply.code(403).send(fail("Enterprise tier required"));
  }
  await cacheClear();
  return reply.send(ok({ cleared: true, message: "All cache cleared" }));
});

app.delete("/cn/v1/system/cache/streams", async (req, reply) => {
  if (req.keyMeta?.tier !== "enterprise") return reply.code(403).send(fail("Enterprise tier required"));
  // Selective clear not available with keyv directly — clear all
  await cacheClear();
  return reply.send(ok({ cleared: true }));
});

/* ── ROOT EXPLORER ── */

function buildSchema() {
  const P = (name, type = "string", req = false, example = "") => ({ name, type, req, example });
  return [
    { group: "Movies", method: "GET", path: "/cn/v1/movie/search", desc: "Search movies", params: [P("q","string",true,"inception"), P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/details", desc: "Full movie details", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/stream", desc: "Stream sources", params: [P("id","int",true,"27205"), P("quality","string",false,"1080p")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/trending", desc: "Trending this week", params: [P("page","int",false,"1"), P("window","string",false,"week")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/popular", desc: "Popular movies", params: [P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/top-rated", desc: "Top rated", params: [P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/new-releases", desc: "Now playing", params: [P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/upcoming", desc: "Upcoming releases", params: [P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/featured", desc: "Featured hero pick", params: [] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/by-genre", desc: "Filter by genre", params: [P("genre","string",true,"action"), P("page","int",false,"1")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/genres", desc: "List movie genres", params: [] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/recommendations", desc: "Recommendations", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/cast", desc: "Cast & crew", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/trailer", desc: "Trailer + videos", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/related", desc: "Similar movies", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/collection", desc: "Movie collection/saga", params: [P("id","int",true,"10")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/images", desc: "Posters & backdrops", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/reviews", desc: "User reviews", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/keywords", desc: "Movie keywords/tags", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/watch-providers", desc: "Streaming platforms", params: [P("id","int",true,"27205")] },
    { group: "Movies", method: "GET", path: "/cn/v1/movie/discover", desc: "Discover with filters", params: [P("genre","string",false,"28"), P("year","string",false,"2023"), P("min_rating","float",false,"7"), P("language","string",false,"en"), P("sort_by","string",false,"popularity.desc")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/search", desc: "Search shows", params: [P("q","string",true,"breaking bad"), P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/details", desc: "Full show details", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/stream", desc: "Episode stream", params: [P("id","int",true,"1396"), P("season","int",false,"1"), P("episode","int",false,"1"), P("quality","string",false,"1080p")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/trending", desc: "Trending shows", params: [P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/popular", desc: "Popular shows", params: [P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/top-rated", desc: "Top rated shows", params: [P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/on-air", desc: "Currently on air", params: [P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/airing-today", desc: "Airing today", params: [P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/seasons", desc: "Seasons list", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/episodes", desc: "Episodes in season", params: [P("id","int",true,"1396"), P("season","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/episode", desc: "Single episode detail", params: [P("id","int",true,"1396"), P("season","int",true,"1"), P("episode","int",true,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/by-genre", desc: "Filter by genre", params: [P("genre","string",true,"drama"), P("page","int",false,"1")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/genres", desc: "TV genres", params: [] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/recommendations", desc: "Recommendations", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/cast", desc: "Cast & crew", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/related", desc: "Similar shows", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/trailer", desc: "Trailer + videos", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/images", desc: "Posters & backdrops", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/reviews", desc: "User reviews", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/keywords", desc: "Show keywords/tags", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/watch-providers", desc: "Streaming platforms", params: [P("id","int",true,"1396")] },
    { group: "TV Shows", method: "GET", path: "/cn/v1/tv/discover", desc: "Discover with filters", params: [P("genre","string",false,"18"), P("year","string",false,"2023"), P("min_rating","float",false,"7"), P("network","string",false,"213")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/search", desc: "Search anime", params: [P("q","string",true,"attack on titan"), P("page","int",false,"1")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/details", desc: "Full anime details", params: [P("id","int",true,"16498")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/stream", desc: "Episode stream", params: [P("id","int",true,"16498"), P("episode","int",false,"1"), P("sub_type","string",false,"sub")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/trending", desc: "Trending anime", params: [P("page","int",false,"1")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/popular", desc: "Popular anime", params: [P("page","int",false,"1")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/top-rated", desc: "Top rated anime", params: [P("page","int",false,"1")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/season", desc: "Seasonal anime", params: [P("season","string",false,"SPRING"), P("year","int",false,"2024")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/episodes", desc: "Episode list", params: [P("id","int",true,"16498")] },
    { group: "Anime", method: "GET", path: "/cn/v1/anime/related", desc: "Related anime", params: [P("id","int",true,"16498")] },
    { group: "Subtitles", method: "GET", path: "/cn/v1/subtitles", desc: "Subtitle tracks", params: [P("id","int",true,"27205"), P("type","string",false,"movie"), P("lang","string",false,"en")] },
    { group: "Subtitles", method: "GET", path: "/cn/v1/subtitles/languages", desc: "Available languages", params: [P("id","int",true,"27205"), P("type","string",false,"movie")] },
    { group: "Search", method: "GET", path: "/cn/v1/search/multi", desc: "Multi search", params: [P("q","string",true,"matrix"), P("page","int",false,"1")] },
    { group: "Search", method: "GET", path: "/cn/v1/search/suggestions", desc: "Type-ahead", params: [P("q","string",true,"mat")] },
    { group: "Search", method: "GET", path: "/cn/v1/search/keyword", desc: "Search keywords", params: [P("q","string",true,"superhero")] },
    { group: "Search", method: "GET", path: "/cn/v1/search/company", desc: "Search companies", params: [P("q","string",true,"marvel")] },
    { group: "People", method: "GET", path: "/cn/v1/person/details", desc: "Person details", params: [P("id","int",true,"6193")] },
    { group: "People", method: "GET", path: "/cn/v1/person/filmography", desc: "Filmography", params: [P("id","int",true,"6193")] },
    { group: "People", method: "GET", path: "/cn/v1/person/popular", desc: "Popular people", params: [P("page","int",false,"1")] },
    { group: "Convert", method: "GET", path: "/cn/v1/convert/imdb/:imdb_id", desc: "IMDB → TMDB", params: [P("imdb_id","string",true,"tt1375666")] },
    { group: "Convert", method: "GET", path: "/cn/v1/convert/tmdb/:tmdb_id", desc: "TMDB → IMDB", params: [P("tmdb_id","int",true,"27205"), P("type","string",false,"movie")] },
    { group: "Keys", method: "GET", path: "/cn/v1/keys/validate", desc: "Validate key", params: [] },
    { group: "Keys", method: "GET", path: "/cn/v1/keys/usage", desc: "Usage stats", params: [] },
    { group: "System", method: "GET", path: "/cn/v1/health", desc: "Health check", params: [] },
    { group: "System", method: "GET", path: "/cn/v1/status", desc: "System status", params: [] },
    { group: "System", method: "DELETE", path: "/cn/v1/system/cache/clear", desc: "Clear all cache (enterprise)", params: [] },
  ];
}

const EXPLORER_HTML = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Cine API · Explorer</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap"/>
<style>
:root{--bg:#08090c;--panel:#10131a;--line:#1d2230;--text:#e5e7ef;--mute:#8089a0;--accent:#ff3b3b;--accent2:#ff7a3b;--ok:#22c55e;--warn:#f59e0b;--err:#ef4444}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
header{display:flex;align-items:center;justify-content:space-between;padding:18px 28px;border-bottom:1px solid var(--line);background:rgba(8,9,12,.9);backdrop-filter:blur(12px);position:sticky;top:0;z-index:10}
.brand{display:flex;align-items:center;gap:10px;font-weight:800;font-size:18px;letter-spacing:.5px}
.brand .dot{width:10px;height:10px;border-radius:3px;background:linear-gradient(135deg,var(--accent),var(--accent2));flex-shrink:0}
.tag{font-size:11px;color:var(--mute);padding:3px 8px;border:1px solid var(--line);border-radius:999px}
main{display:grid;grid-template-columns:220px 1fr;min-height:calc(100vh - 61px)}
aside{padding:20px 14px;border-right:1px solid var(--line);position:sticky;top:61px;height:calc(100vh - 61px);overflow:auto}
aside h4{font-size:10px;text-transform:uppercase;color:var(--mute);letter-spacing:1.4px;margin:16px 0 6px;padding:0 6px}
aside a{display:block;color:var(--mute);text-decoration:none;padding:5px 10px;border-radius:6px;font-size:12px;font-weight:500;transition:all .15s}
aside a:hover,aside a.active{background:rgba(255,59,59,.08);color:var(--accent)}
section{padding:28px 32px;max-width:1060px}
h1{font-size:26px;font-weight:800;margin:0 0 4px;background:linear-gradient(90deg,#fff 40%,var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.lead{color:var(--mute);margin:0 0 22px;font-size:13px}
.keybox{display:flex;gap:8px;margin-bottom:8px}
.keybox input{flex:1;padding:10px 13px;background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;outline:none;transition:border .15s}
.keybox input:focus{border-color:var(--accent)}
.keybox button{padding:10px 16px;background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border:0;border-radius:8px;font-weight:700;cursor:pointer;font-size:12px;white-space:nowrap}
#keyStatus{margin-bottom:22px;font-size:12px;color:var(--mute);min-height:16px}
.group{margin-bottom:28px}
.group-title{font-size:11px;text-transform:uppercase;letter-spacing:1.8px;color:var(--accent2);border-bottom:1px solid var(--line);padding-bottom:8px;margin-bottom:12px;font-weight:700}
.ep{background:var(--panel);border:1px solid var(--line);border-radius:10px;margin-bottom:10px;overflow:hidden;transition:border .15s}
.ep:hover{border-color:#2d3347}
.ep-head{display:flex;align-items:center;gap:10px;padding:12px 14px;cursor:pointer;user-select:none}
.method{font-size:10px;font-weight:800;padding:3px 7px;border-radius:4px;background:#0f1824;color:#7dd3fc;letter-spacing:.5px;white-space:nowrap}
.method.delete{color:#f87171;background:#1f0a0a}
.path{font-family:'JetBrains Mono',monospace;font-size:12px;color:#e2e8f0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.desc{font-size:11px;color:var(--mute);min-width:140px;text-align:right;flex-shrink:0}
.ep-body{display:none;border-top:1px solid var(--line);padding:14px;background:#0a0c12}
.ep.open .ep-body{display:block}
.params{display:grid;grid-template-columns:repeat(auto-fill,minmax(175px,1fr));gap:7px;margin-bottom:10px}
.param-wrap{display:flex;flex-direction:column;gap:3px}
.param-label{font-size:10px;color:var(--mute);font-family:'JetBrains Mono',monospace}
.param-label .req{color:var(--accent);margin-left:2px}
.params input{padding:7px 10px;background:#070810;border:1px solid var(--line);border-radius:6px;color:var(--text);font-size:12px;font-family:'JetBrains Mono',monospace;outline:none;width:100%;transition:border .15s}
.params input:focus{border-color:#3d4560}
.run{padding:7px 16px;background:var(--accent);color:#fff;border:0;border-radius:6px;font-weight:700;cursor:pointer;font-size:11px;letter-spacing:.3px}
.run:hover{background:#e03030}
pre{background:#000;border:1px solid #1a1d28;border-radius:7px;padding:12px;overflow:auto;font-size:11px;font-family:'JetBrains Mono',monospace;color:#a5b4fc;max-height:360px;margin-top:10px;line-height:1.55}
.res-meta{display:flex;align-items:center;gap:8px;margin-top:10px;margin-bottom:4px}
.status-badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:800;letter-spacing:.5px}
.s2{background:#052e16;color:#4ade80}.s4{background:#2d0d0d;color:#f87171}.s5{background:#2d1f00;color:#fb923c}
.timing{font-size:11px;color:var(--mute);font-family:'JetBrains Mono',monospace}
@media(max-width:720px){main{grid-template-columns:1fr}aside{display:none}section{padding:20px 16px}}
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:#2d3347;border-radius:4px}
</style></head>
<body>
<header>
  <div class="brand"><span class="dot"></span>Cine API</div>
  <div style="display:flex;gap:10px;align-items:center">
    <span class="tag">v1</span>
    <a href="https://docs.cine.dpdns.org" style="color:var(--mute);font-size:12px;text-decoration:none;font-weight:500">Docs →</a>
  </div>
</header>
<main>
<aside id="nav"></aside>
<section>
<h1>Cine API Explorer</h1>
<p class="lead">Live interactive reference. Enter your key, expand an endpoint, fill params, and run real requests.</p>
<div class="keybox">
  <input id="apiKey" placeholder="API key (e.g. cine-2026)" value="cine-2026" autocomplete="off"/>
  <button onclick="validateKey()">Validate</button>
</div>
<div id="keyStatus"></div>
<div id="endpoints"></div>
</section>
</main>
<script>
const EP=${JSON.stringify(buildSchema())};
const ENDP=document.getElementById('endpoints');
const NAV=document.getElementById('nav');
const groups={};
EP.forEach(e=>{(groups[e.group]=groups[e.group]||[]).push(e);});
let navHtml='';
for(const g in groups){
  navHtml+=\`<h4>\${g}</h4><a href="#g-\${g.replace(/ /g,'-')}">\${g} <span style="color:var(--mute);font-size:10px">(\${groups[g].length})</span></a>\`;
}
NAV.innerHTML=navHtml;
let allHtml='';
for(const g in groups){
  allHtml+=\`<div class="group" id="g-\${g.replace(/ /g,'-')}"><div class="group-title">\${g}</div>\`;
  groups[g].forEach((e,i)=>{
    const id=g.replace(/ /g,'_')+'-'+i;
    const paramsHtml=e.params.map(p=>\`<div class="param-wrap"><div class="param-label">\${p.name}\${p.req?'<span class="req">*</span>':''} <span style="opacity:.5">(\${p.type})</span></div><input data-k="\${p.name}" placeholder="\${p.example||p.name}" value="\${p.example||''}"/></div>\`).join('');
    const methodClass=e.method==='DELETE'?'method delete':'method';
    allHtml+=\`<div class="ep" id="ep-\${id}">
      <div class="ep-head" onclick="toggle('\${id}')">
        <span class="\${methodClass}">\${e.method}</span>
        <span class="path">\${e.path}</span>
        <span class="desc">\${e.desc}</span>
      </div>
      <div class="ep-body" id="body-\${id}">
        \${e.params.length?'<div class="params">'+paramsHtml+'</div>':''}
        <button class="run" onclick="run('\${id}','\${e.method}','\${e.path}')">Run</button>
        <div class="res-meta" id="meta-\${id}" style="display:none"></div>
        <pre id="out-\${id}">// Click Run to see response</pre>
      </div>
    </div>\`;
  });
  allHtml+='</div>';
}
ENDP.innerHTML=allHtml;

function toggle(id){
  const ep=document.getElementById('ep-'+id);
  ep.classList.toggle('open');
}

async function run(id,method,path){
  const body=document.getElementById('body-'+id);
  const out=document.getElementById('out-'+id);
  const meta=document.getElementById('meta-'+id);
  const params=new URLSearchParams();
  body.querySelectorAll('input').forEach(i=>{if(i.value&&i.dataset.k)params.set(i.dataset.k,i.value);});
  params.set('api',document.getElementById('apiKey').value);
  // Replace path params
  let url=path;
  params.forEach((v,k)=>{if(url.includes(':'+k)){url=url.replace(':'+k,encodeURIComponent(v));params.delete(k);}});
  url+='?'+params.toString();
  out.textContent='Loading…';
  meta.style.display='none';
  const t0=performance.now();
  try{
    const r=await fetch(url,{method});
    const j=await r.json();
    const ms=(performance.now()-t0).toFixed(0);
    const cls=r.status<300?'s2':r.status<500?'s4':'s5';
    meta.innerHTML=\`<span class="status-badge \${cls}">\${r.status}</span><span class="timing">\${ms}ms</span>\`;
    meta.style.display='flex';
    out.innerHTML=escapeHtml(JSON.stringify(j,null,2));
  }catch(e){
    meta.style.display='none';
    out.textContent='Error: '+e.message;
  }
}

function escapeHtml(s){return s.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

async function validateKey(){
  const key=document.getElementById('apiKey').value.trim();
  if(!key){document.getElementById('keyStatus').innerHTML='<span style="color:var(--err)">●</span> Enter a key';return;}
  const el=document.getElementById('keyStatus');
  el.textContent='Validating…';
  try{
    const r=await fetch('/cn/v1/keys/validate?api='+encodeURIComponent(key));
    const j=await r.json();
    if(j.success) el.innerHTML=\`<span style="color:var(--ok)">●</span> Valid · <b>\${j.data.name}</b> · \${j.data.tier} · \${j.data.rate_limit_per_minute} rpm\`;
    else el.innerHTML=\`<span style="color:var(--err)">●</span> \${j.error}\`;
  }catch(e){el.innerHTML='<span style="color:var(--err)">●</span> Validation failed';}
}

validateKey();
</script>
</body></html>`;

app.get("/", async (req, reply) => reply.type("text/html").send(EXPLORER_HTML));
app.get("/cn", async (req, reply) => reply.type("text/html").send(EXPLORER_HTML));
app.get("/cn/v1", async (req, reply) => reply.type("text/html").send(EXPLORER_HTML));
app.get("/favicon.ico", async (req, reply) => reply.code(204).send());

/* ─────────────────────────── STARTUP ─────────────────────────── */

async function start() {
  try {
    // Init Playwright pool
    await pool.init();

    // Start server
    await app.listen({ port: PORT, host: "0.0.0.0" });
    console.log(`\n  🎬  Cine API`);
    console.log(`  ↳  http://localhost:${PORT}`);
    console.log(`  ↳  ${Object.keys(API_KEYS).length} API keys registered`);
    console.log(`  ↳  ${PLAYWRIGHT_POOL_SIZE} Playwright browser(s) warm\n`);
  } catch (err) {
    console.error("Startup error:", err);
    process.exit(1);
  }
}

// Graceful shutdown
async function shutdown(signal) {
  console.log(`\n[${signal}] Shutting down gracefully...`);
  await pool.destroy();
  await app.close();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", (err) => {
  console.error("[uncaughtException]", err);
});
process.on("unhandledRejection", (reason) => {
  console.error("[unhandledRejection]", reason);
});

start();
