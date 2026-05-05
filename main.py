import asyncio
import base64
import hashlib
import html
import json
import logging
import os
import random
import re
import secrets
import string
import subprocess
import time
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import httpx
import psutil
from curl_cffi.requests import AsyncSession as CurlSession
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from jinja2 import DictLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cineapi")

TMDB_API_KEY = "1739012afb6a538588d51ce8e9bded3a"
OPENSUBS_API_KEY = "your_opensubtitles_api_key_here"
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID", "")
MASTER_API_KEY = os.getenv("MASTER_API_KEY", "cineapi-" + secrets.token_hex(16))
PORT = int(os.getenv("PORT", 8000))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/cineapi_downloads"))
DOWNLOAD_TTL = int(os.getenv("DOWNLOAD_TTL_HOURS", 2)) * 3600
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 3))

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280"
ANILIST_BASE = "https://graphql.anilist.co"
OPENSUBS_BASE = "https://api.opensubtitles.com/api/v1"

API_VERSION = "v1"
PROJECT_NAME = "CineAPI"
PROJECT_AUTHOR = "Jaden Afrix"
PROJECT_COMPANY = "SAGE"
PROJECT_VERSION = "2.0.0"

PUBLIC_PATHS = {"/", "/health", "/docs", "/redoc", "/openapi.json"}

CACHE_TTL_STREAM = 1800
CACHE_TTL_META = 86400
CACHE_TTL_SEARCH = 3600
CACHE_TTL_SOURCE = 900
CACHE_TTL_PROVIDER = 300

_stream_cache: dict[str, dict] = {}
_meta_cache: dict[str, dict] = {}
_search_cache: dict[str, dict] = {}
_source_cache: dict[str, dict] = {}
_provider_health_cache: dict[str, dict] = {}

_download_jobs: dict[str, dict] = {}
_active_downloads: int = 0

_request_stats: dict = {
    "total": 0,
    "today": 0,
    "errors_4xx": 0,
    "errors_5xx": 0,
    "by_endpoint": {},
    "by_ip": {},
    "response_times": {},
    "peak_rpm": 0,
    "rpm_window": [],
    "recent_errors": [],
    "start_of_day": time.time(),
}
_start_time = time.time()
_log_buffer: list[dict] = []

HEADERS_CHROME = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

HEADERS_AJAX = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
}

HEADERS_JSON = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

PLAYER_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{{ title }} — CineAPI Player</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/video.js/8.6.1/video-js.min.css"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/video.js/8.6.1/video.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/videojs-http-streaming/3.0.2/videojs-http-streaming.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#000;color:#fff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;height:100vh;overflow:hidden}
#player-container{position:relative;width:100vw;height:100vh}
.video-js{width:100%;height:100%}
#controls-bar{position:absolute;bottom:0;left:0;right:0;padding:12px 16px;background:linear-gradient(transparent,rgba(0,0,0,0.9));display:flex;align-items:center;gap:12px;z-index:10;opacity:0;transition:opacity 0.3s;flex-wrap:wrap}
#player-container:hover #controls-bar{opacity:1}
select.ctrl{background:rgba(255,255,255,0.1);color:#fff;border:1px solid rgba(255,255,255,0.2);border-radius:6px;padding:5px 10px;font-size:12px;cursor:pointer;backdrop-filter:blur(10px)}
select.ctrl option{background:#1a1a1a;color:#fff}
#title-bar{position:absolute;top:0;left:0;right:0;padding:16px;background:linear-gradient(rgba(0,0,0,0.7),transparent);font-size:14px;font-weight:600;letter-spacing:0.5px;z-index:10;opacity:0;transition:opacity 0.3s}
#player-container:hover #title-bar{opacity:1}
#branding{position:absolute;top:16px;right:16px;font-size:11px;color:rgba(255,255,255,0.4);z-index:10;letter-spacing:1px;text-transform:uppercase}
#error-overlay{display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);background:rgba(0,0,0,0.8);border:1px solid rgba(255,80,80,0.4);border-radius:12px;padding:24px 32px;text-align:center;z-index:20}
#error-overlay p{color:#ff6b6b;font-size:14px;margin-bottom:12px}
#error-overlay button{background:#fff;color:#000;border:none;border-radius:6px;padding:8px 16px;cursor:pointer;font-size:12px;font-weight:600}
.vjs-big-play-button{left:50%;top:50%;transform:translate(-50%,-50%);border-radius:50%;width:72px;height:72px;line-height:72px;border:2px solid rgba(255,255,255,0.8);background:rgba(0,0,0,0.5);backdrop-filter:blur(10px)}
.vjs-control-bar{background:linear-gradient(transparent,rgba(0,0,0,0.7))}
#status-badge{font-size:11px;color:rgba(255,255,255,0.5);padding:4px 8px;background:rgba(255,255,255,0.08);border-radius:4px}
</style>
</head>
<body>
<div id="player-container">
  <div id="title-bar">{{ title }}</div>
  <div id="branding">CineAPI · SAGE</div>
  <div id="error-overlay">
    <p id="error-msg">Stream failed. Trying next source...</p>
    <button onclick="tryNextSource()">Try Next Source</button>
  </div>
  <video id="cineapi-player" class="video-js vjs-default-skin vjs-big-play-centered" controls preload="auto" {% if poster %}poster="{{ poster }}"{% endif %}>
    {% if stream_url %}
    <source src="{{ stream_url }}" type="{{ 'application/x-mpegURL' if '.m3u8' in stream_url else 'video/mp4' }}"/>
    {% endif %}
    {% if subtitle_url %}
    <track kind="subtitles" src="{{ subtitle_url }}" srclang="en" label="English" default/>
    {% endif %}
  </video>
  <div id="controls-bar">
    <select class="ctrl" id="source-select" onchange="switchSource(this.value)">
      <option value="">— Source —</option>
    </select>
    <select class="ctrl" id="sub-select" onchange="switchSubtitle(this.value)">
      <option value="">— Subtitles —</option>
      <option value="off">Off</option>
    </select>
    <select class="ctrl" id="quality-select" onchange="switchQuality(this.value)">
      <option value="">— Quality —</option>
    </select>
    <span id="status-badge">Ready</span>
  </div>
</div>
<script>
const player = videojs('cineapi-player', {
  html5: { vhs: { overrideNative: true } },
  controls: true,
  autoplay: {{ 'true' if autoplay else 'false' }},
  preload: 'auto',
  fluid: false,
});
const streams = {{ streams_json }};
const subtitles = {{ subtitles_json }};
let currentSourceIdx = 0;
const sourceSelect = document.getElementById('source-select');
const subSelect = document.getElementById('sub-select');
const qualitySelect = document.getElementById('quality-select');
const statusBadge = document.getElementById('status-badge');
const errorOverlay = document.getElementById('error-overlay');
const errorMsg = document.getElementById('error-msg');
const qualityGroups = {};
streams.forEach((s, i) => {
  const q = (s.quality || 'auto').toLowerCase();
  if (!qualityGroups[q]) qualityGroups[q] = [];
  qualityGroups[q].push(i);
  const opt = document.createElement('option');
  opt.value = i;
  opt.textContent = (s.provider || 'Unknown') + ' · ' + (s.quality || 'auto') + ' · ' + (s.format || '').toUpperCase();
  if (i === 0) opt.selected = true;
  sourceSelect.appendChild(opt);
});
Object.keys(qualityGroups).sort().forEach(q => {
  const opt = document.createElement('option');
  opt.value = q;
  opt.textContent = q.toUpperCase();
  qualitySelect.appendChild(opt);
});
subtitles.forEach((s, i) => {
  const opt = document.createElement('option');
  opt.value = s.file_id || i;
  opt.textContent = (s.language || 'Unknown').toUpperCase() + ' — ' + (s.file_name || '').substring(0, 30);
  subSelect.appendChild(opt);
});
function setStatus(msg) { statusBadge.textContent = msg; }
function switchSource(idx) {
  if (idx === '') return;
  currentSourceIdx = parseInt(idx);
  const s = streams[currentSourceIdx];
  if (!s) return;
  errorOverlay.style.display = 'none';
  const type = s.format === 'm3u8' ? 'application/x-mpegURL' : 'video/mp4';
  player.src({ type, src: s.url });
  player.play();
  setStatus((s.provider || 'Unknown') + ' · ' + (s.quality || 'auto'));
}
function tryNextSource() {
  const next = currentSourceIdx + 1;
  if (next < streams.length) {
    currentSourceIdx = next;
    sourceSelect.value = next;
    switchSource(next);
  } else {
    errorMsg.textContent = 'All sources exhausted. No more streams available.';
  }
}
function switchQuality(q) {
  if (!q) return;
  const indices = qualityGroups[q];
  if (indices && indices.length > 0) {
    switchSource(indices[0]);
    sourceSelect.value = indices[0];
  }
}
function switchSubtitle(fileId) {
  if (!fileId || fileId === 'off') {
    const tracks = player.remoteTextTracks();
    for (let i = tracks.length - 1; i >= 0; i--) player.removeRemoteTextTrack(tracks[i]);
    return;
  }
  const url = '/api/v1/subtitles/proxy/' + fileId;
  const tracks = player.remoteTextTracks();
  for (let i = tracks.length - 1; i >= 0; i--) player.removeRemoteTextTrack(tracks[i]);
  player.addRemoteTextTrack({ kind: 'subtitles', src: url, srclang: 'en', label: 'Subtitle', default: true }, false);
}
player.on('error', function() {
  const err = player.error();
  errorMsg.textContent = 'Stream error: ' + (err ? err.message : 'Unknown') + '. Try next source?';
  errorOverlay.style.display = 'block';
  setStatus('Error — trying next...');
  setTimeout(tryNextSource, 2000);
});
player.on('playing', function() { setStatus('Playing · ' + (streams[currentSourceIdx] ? streams[currentSourceIdx].provider : '')); errorOverlay.style.display = 'none'; });
player.on('waiting', function() { setStatus('Buffering...'); });
player.on('ended', function() { setStatus('Ended'); });
</script>
</body>
</html>"""

_jinja_env = DictLoader({"player.html": PLAYER_TEMPLATE})
templates = Jinja2Templates(directory="/tmp")
templates.env.loader = _jinja_env


def cache_set(store: dict, key: str, value: Any, ttl: int):
    store[key] = {"data": value, "expires": time.time() + ttl}


def cache_get(store: dict, key: str) -> Optional[Any]:
    entry = store.get(key)
    if not entry:
        return None
    if time.time() > entry["expires"]:
        del store[key]
        return None
    return entry["data"]


def cache_cleanup_all():
    now = time.time()
    for store in [_stream_cache, _meta_cache, _search_cache, _source_cache, _provider_health_cache]:
        expired = [k for k, v in store.items() if now > v["expires"]]
        for k in expired:
            del store[k]


def validate_api_key(request: Request) -> bool:
    path = request.url.path
    if path in PUBLIC_PATHS or path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi"):
        return True
    key = request.query_params.get("api_key") or request.headers.get("X-API-Key") or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return key == MASTER_API_KEY


def track_request(request: Request, status_code: int, duration: float):
    global _request_stats
    now = time.time()
    path = request.url.path
    ip = request.client.host if request.client else "unknown"

    _request_stats["total"] += 1

    if now - _request_stats["start_of_day"] >= 86400:
        _request_stats["today"] = 0
        _request_stats["start_of_day"] = now
    _request_stats["today"] += 1

    if 400 <= status_code < 500:
        _request_stats["errors_4xx"] += 1
    elif status_code >= 500:
        _request_stats["errors_5xx"] += 1

    ep = _request_stats["by_endpoint"]
    ep[path] = ep.get(path, 0) + 1

    ip_stats = _request_stats["by_ip"]
    ip_stats[ip] = ip_stats.get(ip, 0) + 1

    rt = _request_stats["response_times"]
    if path not in rt:
        rt[path] = {"count": 0, "total_ms": 0, "avg_ms": 0}
    rt[path]["count"] += 1
    rt[path]["total_ms"] += duration * 1000
    rt[path]["avg_ms"] = rt[path]["total_ms"] / rt[path]["count"]

    window = _request_stats["rpm_window"]
    window.append(now)
    cutoff = now - 60
    _request_stats["rpm_window"] = [t for t in window if t > cutoff]
    current_rpm = len(_request_stats["rpm_window"])
    if current_rpm > _request_stats["peak_rpm"]:
        _request_stats["peak_rpm"] = current_rpm

    if status_code >= 400:
        err_entry = {"time": now, "endpoint": path, "status": status_code, "ip": ip}
        _request_stats["recent_errors"].append(err_entry)
        if len(_request_stats["recent_errors"]) > 100:
            _request_stats["recent_errors"] = _request_stats["recent_errors"][-100:]


async def fetch_html(url: str, headers: dict = None, timeout: int = 15) -> str:
    try:
        async with CurlSession(impersonate="chrome124") as session:
            resp = await session.get(url, headers=headers or HEADERS_CHROME, timeout=timeout)
            return resp.text
    except Exception as e:
        logger.warning(f"CurlSession failed for {url}: {e}")
    try:
        async with httpx.AsyncClient(headers=headers or HEADERS_CHROME, follow_redirects=True, timeout=timeout) as client:
            resp = await client.get(url)
            return resp.text
    except Exception as e:
        logger.warning(f"httpx fallback failed for {url}: {e}")
        return ""


async def fetch_json(url: str, headers: dict = None, timeout: int = 15) -> dict:
    try:
        async with CurlSession(impersonate="chrome124") as session:
            resp = await session.get(url, headers=headers or HEADERS_JSON, timeout=timeout)
            return resp.json()
    except Exception as e:
        logger.warning(f"CurlSession JSON failed for {url}: {e}")
    try:
        async with httpx.AsyncClient(headers=headers or HEADERS_JSON, follow_redirects=True, timeout=timeout) as client:
            resp = await client.get(url)
            return resp.json()
    except Exception as e:
        logger.warning(f"httpx JSON fallback failed for {url}: {e}")
        return {}


async def post_json(url: str, payload: dict, headers: dict = None, timeout: int = 15) -> dict:
    try:
        async with httpx.AsyncClient(headers=headers or HEADERS_JSON, follow_redirects=True, timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            return resp.json()
    except Exception as e:
        logger.warning(f"post_json failed for {url}: {e}")
        return {}


async def tmdb_get(endpoint: str, params: dict = None) -> dict:
    p = dict(params or {})
    p["api_key"] = TMDB_API_KEY
    url = f"{TMDB_BASE}{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=p)
            return resp.json()
    except Exception as e:
        logger.warning(f"TMDB request failed {endpoint}: {e}")
        return {}


def unpack_js(packed: str) -> str:
    try:
        import jsbeautifier
        return jsbeautifier.beautify(packed)
    except Exception:
        pass
    try:
        p_match = re.search(r"'([^']+)'", packed)
        a_match = re.search(r",(\d+),", packed)
        c_match = re.search(r",\d+,(\d+),", packed)
        k_match = re.search(r"'([^']+)'\.split\('\|'\)", packed)
        if not all([p_match, a_match, c_match, k_match]):
            return packed
        p = p_match.group(1)
        a = int(a_match.group(1))
        k = k_match.group(1).split("|")

        def base_n(num, base):
            chars = string.digits + string.ascii_letters
            result = ""
            while num:
                result = chars[num % base] + result
                num //= base
            return result or "0"

        for i, word in enumerate(k):
            if word:
                p = re.sub(r"\b" + base_n(i, a) + r"\b", word, p)
        return p
    except Exception as e:
        logger.warning(f"unpack_js failed: {e}")
        return packed


def validate_stream_url(url: str) -> bool:
    if not url:
        return False
    if not url.startswith(("http://", "https://")):
        return False
    if len(url) < 10:
        return False
    return True


def rank_streams(streams: list) -> list:
    quality_order = {"2160p": 0, "4k": 0, "1080p": 1, "720p": 2, "480p": 3, "360p": 4, "auto": 5}
    format_order = {"m3u8": 0, "mp4": 1}
    provider_order = {"HDRezka": 0, "FlixHQ": 1, "LookMovie": 2, "HiAnime/Zoro": 3, "GogoAnime": 4, "9Anime": 5, "SuperEmbed": 6, "AutoEmbed": 7, "SmashyStream": 8, "2Embed": 9, "EmbedSu": 10}

    def score(s):
        q = s.get("quality", "auto").lower()
        f = s.get("format", "mp4").lower()
        p = s.get("provider", "Unknown")
        return (quality_order.get(q, 5), format_order.get(f, 1), provider_order.get(p, 99))

    return sorted(streams, key=score)


def dedupe_streams(streams: list) -> list:
    seen = set()
    result = []
    for s in streams:
        url = s.get("url", "")
        if url and url not in seen and validate_stream_url(url):
            seen.add(url)
            result.append(s)
    return result


async def get_tmdb_movie_meta(imdb_id: str) -> dict:
    cache_key = f"tmdb_movie_{imdb_id}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    find_data = await tmdb_get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
    movie_results = find_data.get("movie_results", [])
    if not movie_results:
        return {}
    movie = movie_results[0]
    tmdb_id = movie["id"]
    detail = await tmdb_get(f"/movie/{tmdb_id}", {"append_to_response": "credits,videos,keywords,watch/providers,release_dates"})
    if not detail:
        return {}
    cast = []
    crew = []
    if "credits" in detail:
        cast = [{"name": m["name"], "character": m.get("character", ""), "profile": f"{TMDB_IMAGE_BASE}{m['profile_path']}" if m.get("profile_path") else None} for m in detail["credits"].get("cast", [])[:15]]
        crew = [{"name": m["name"], "job": m.get("job", ""), "department": m.get("department", "")} for m in detail["credits"].get("crew", []) if m.get("job") in ["Director", "Producer", "Screenplay", "Writer"]]
    trailers = []
    if "videos" in detail:
        for v in detail["videos"].get("results", []):
            if v.get("site") == "YouTube":
                trailers.append({"type": v.get("type"), "key": v.get("key"), "name": v.get("name"), "url": f"https://www.youtube.com/watch?v={v['key']}"})
    trailer = trailers[0]["url"] if trailers else None
    watch_providers = {}
    if "watch/providers" in detail:
        for country, pdata in detail["watch/providers"].get("results", {}).items():
            watch_providers[country] = {"streaming": [p["provider_name"] for p in pdata.get("flatrate", [])], "rent": [p["provider_name"] for p in pdata.get("rent", [])], "buy": [p["provider_name"] for p in pdata.get("buy", [])]}
    certifications = {}
    if "release_dates" in detail:
        for entry in detail["release_dates"].get("results", []):
            country = entry.get("iso_3166_1")
            for rd in entry.get("release_dates", []):
                cert = rd.get("certification")
                if cert:
                    certifications[country] = cert
                    break
    result = {
        "imdb_id": imdb_id,
        "tmdb_id": tmdb_id,
        "title": detail.get("title", ""),
        "original_title": detail.get("original_title", ""),
        "overview": detail.get("overview", ""),
        "tagline": detail.get("tagline", ""),
        "year": detail.get("release_date", "")[:4],
        "release_date": detail.get("release_date", ""),
        "runtime": detail.get("runtime"),
        "status": detail.get("status", ""),
        "rating": detail.get("vote_average", 0),
        "vote_count": detail.get("vote_count", 0),
        "popularity": detail.get("popularity", 0),
        "genres": [g["name"] for g in detail.get("genres", [])],
        "genre_ids": [g["id"] for g in detail.get("genres", [])],
        "keywords": [k["name"] for k in detail.get("keywords", {}).get("keywords", [])],
        "production_companies": [c["name"] for c in detail.get("production_companies", [])],
        "production_countries": [c["name"] for c in detail.get("production_countries", [])],
        "spoken_languages": [l["english_name"] for l in detail.get("spoken_languages", [])],
        "budget": detail.get("budget", 0),
        "revenue": detail.get("revenue", 0),
        "poster": f"{TMDB_IMAGE_BASE}{detail['poster_path']}" if detail.get("poster_path") else None,
        "backdrop": f"{TMDB_BACKDROP_BASE}{detail['backdrop_path']}" if detail.get("backdrop_path") else None,
        "trailer": trailer,
        "trailers": trailers,
        "cast": cast,
        "crew": crew,
        "watch_providers": watch_providers,
        "certifications": certifications,
        "collection": detail.get("belongs_to_collection"),
        "type": "movie",
    }
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def get_tmdb_tv_meta(imdb_id: str) -> dict:
    cache_key = f"tmdb_tv_{imdb_id}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    find_data = await tmdb_get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
    tv_results = find_data.get("tv_results", [])
    if not tv_results:
        return {}
    show = tv_results[0]
    tmdb_id = show["id"]
    detail = await tmdb_get(f"/tv/{tmdb_id}", {"append_to_response": "credits,videos,external_ids,watch/providers"})
    if not detail:
        return {}
    cast = []
    if "credits" in detail:
        cast = [{"name": m["name"], "character": m.get("character", ""), "profile": f"{TMDB_IMAGE_BASE}{m['profile_path']}" if m.get("profile_path") else None} for m in detail["credits"].get("cast", [])[:15]]
    trailers = []
    if "videos" in detail:
        for v in detail["videos"].get("results", []):
            if v.get("site") == "YouTube":
                trailers.append({"type": v.get("type"), "key": v.get("key"), "name": v.get("name"), "url": f"https://www.youtube.com/watch?v={v['key']}"})
    trailer = trailers[0]["url"] if trailers else None
    seasons_summary = []
    for s in detail.get("seasons", []):
        if s["season_number"] == 0:
            continue
        seasons_summary.append({"season_number": s["season_number"], "name": s.get("name", ""), "overview": s.get("overview", ""), "episode_count": s.get("episode_count", 0), "air_date": s.get("air_date", ""), "poster": f"{TMDB_IMAGE_BASE}{s['poster_path']}" if s.get("poster_path") else None})
    last_ep = detail.get("last_episode_to_air")
    next_ep = detail.get("next_episode_to_air")
    watch_providers = {}
    if "watch/providers" in detail:
        for country, pdata in detail["watch/providers"].get("results", {}).items():
            watch_providers[country] = {"streaming": [p["provider_name"] for p in pdata.get("flatrate", [])], "rent": [p["provider_name"] for p in pdata.get("rent", [])]}
    result = {
        "imdb_id": imdb_id,
        "tmdb_id": tmdb_id,
        "title": detail.get("name", ""),
        "original_title": detail.get("original_name", ""),
        "overview": detail.get("overview", ""),
        "tagline": detail.get("tagline", ""),
        "year": detail.get("first_air_date", "")[:4],
        "first_air_date": detail.get("first_air_date", ""),
        "last_air_date": detail.get("last_air_date", ""),
        "status": detail.get("status", ""),
        "rating": detail.get("vote_average", 0),
        "vote_count": detail.get("vote_count", 0),
        "popularity": detail.get("popularity", 0),
        "genres": [g["name"] for g in detail.get("genres", [])],
        "networks": [n["name"] for n in detail.get("networks", [])],
        "number_of_seasons": detail.get("number_of_seasons", 0),
        "number_of_episodes": detail.get("number_of_episodes", 0),
        "episode_run_time": detail.get("episode_run_time", []),
        "spoken_languages": [l["english_name"] for l in detail.get("spoken_languages", [])],
        "poster": f"{TMDB_IMAGE_BASE}{detail['poster_path']}" if detail.get("poster_path") else None,
        "backdrop": f"{TMDB_BACKDROP_BASE}{detail['backdrop_path']}" if detail.get("backdrop_path") else None,
        "trailer": trailer,
        "trailers": trailers,
        "cast": cast,
        "seasons": seasons_summary,
        "last_episode": last_ep,
        "next_episode": next_ep,
        "watch_providers": watch_providers,
        "type": "tv",
    }
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def get_tmdb_tv_season(tmdb_id: int, season: int) -> dict:
    cache_key = f"tmdb_season_{tmdb_id}_{season}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    detail = await tmdb_get(f"/tv/{tmdb_id}/season/{season}")
    if not detail:
        return {}
    episodes = []
    for ep in detail.get("episodes", []):
        episodes.append({"episode_number": ep["episode_number"], "name": ep.get("name", ""), "overview": ep.get("overview", ""), "air_date": ep.get("air_date", ""), "runtime": ep.get("runtime"), "still": f"{TMDB_IMAGE_BASE}{ep['still_path']}" if ep.get("still_path") else None, "rating": ep.get("vote_average", 0)})
    result = {"season_number": season, "name": detail.get("name", ""), "overview": detail.get("overview", ""), "air_date": detail.get("air_date", ""), "poster": f"{TMDB_IMAGE_BASE}{detail['poster_path']}" if detail.get("poster_path") else None, "episodes": episodes}
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def tmdb_search_movies(query: str, page: int = 1) -> list:
    data = await tmdb_get("/search/movie", {"query": query, "page": page, "include_adult": False})
    results = []
    for m in data.get("results", []):
        detail = await tmdb_get(f"/movie/{m['id']}", {"append_to_response": "external_ids"})
        imdb_id = detail.get("imdb_id") or detail.get("external_ids", {}).get("imdb_id")
        results.append({"imdb_id": imdb_id, "tmdb_id": m["id"], "title": m.get("title", ""), "overview": m.get("overview", ""), "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "type": "movie"})
    return results


async def tmdb_search_tv(query: str, page: int = 1) -> list:
    data = await tmdb_get("/search/tv", {"query": query, "page": page})
    results = []
    for s in data.get("results", []):
        ext = await tmdb_get(f"/tv/{s['id']}/external_ids")
        results.append({"imdb_id": ext.get("imdb_id"), "tmdb_id": s["id"], "title": s.get("name", ""), "overview": s.get("overview", ""), "year": s.get("first_air_date", "")[:4], "rating": s.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{s['poster_path']}" if s.get("poster_path") else None, "type": "tv"})
    return results


async def tmdb_trending(media_type: str = "all", time_window: str = "week") -> list:
    data = await tmdb_get(f"/trending/{media_type}/{time_window}")
    results = []
    for item in data.get("results", [])[:20]:
        media = item.get("media_type", media_type)
        if media == "movie":
            ext = await tmdb_get(f"/movie/{item['id']}/external_ids")
            results.append({"imdb_id": ext.get("imdb_id"), "tmdb_id": item["id"], "title": item.get("title", ""), "year": item.get("release_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "movie"})
        elif media == "tv":
            ext = await tmdb_get(f"/tv/{item['id']}/external_ids")
            results.append({"imdb_id": ext.get("imdb_id"), "tmdb_id": item["id"], "title": item.get("name", ""), "year": item.get("first_air_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "tv"})
    return results


async def tmdb_discover(media_type: str, genre: str = None, year: str = None, min_rating: float = None, sort_by: str = "popularity.desc", language: str = None, page: int = 1) -> list:
    params = {"sort_by": sort_by, "page": page, "include_adult": False}
    if genre:
        params["with_genres"] = genre
    if year:
        if media_type == "movie":
            params["primary_release_year"] = year
        else:
            params["first_air_date_year"] = year
    if min_rating:
        params["vote_average.gte"] = min_rating
    if language:
        params["with_original_language"] = language
    endpoint = f"/discover/{media_type}"
    data = await tmdb_get(endpoint, params)
    results = []
    for item in data.get("results", [])[:20]:
        if media_type == "movie":
            results.append({"tmdb_id": item["id"], "title": item.get("title", ""), "year": item.get("release_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "movie", "overview": item.get("overview", "")})
        else:
            results.append({"tmdb_id": item["id"], "title": item.get("name", ""), "year": item.get("first_air_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "tv", "overview": item.get("overview", "")})
    return results


async def tmdb_person(person_id: int) -> dict:
    cache_key = f"person_{person_id}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    detail = await tmdb_get(f"/person/{person_id}", {"append_to_response": "movie_credits,tv_credits,external_ids"})
    if not detail:
        return {}
    result = {
        "id": person_id,
        "name": detail.get("name"),
        "biography": detail.get("biography"),
        "birthday": detail.get("birthday"),
        "deathday": detail.get("deathday"),
        "place_of_birth": detail.get("place_of_birth"),
        "popularity": detail.get("popularity"),
        "profile": f"{TMDB_IMAGE_BASE}{detail['profile_path']}" if detail.get("profile_path") else None,
        "known_for_department": detail.get("known_for_department"),
        "movie_credits": [{"tmdb_id": m["id"], "title": m.get("title"), "character": m.get("character"), "year": m.get("release_date", "")[:4], "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in detail.get("movie_credits", {}).get("cast", [])[:20]],
        "tv_credits": [{"tmdb_id": m["id"], "title": m.get("name"), "character": m.get("character"), "year": m.get("first_air_date", "")[:4], "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in detail.get("tv_credits", {}).get("cast", [])[:20]],
    }
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def tmdb_collection(collection_id: int) -> dict:
    cache_key = f"collection_{collection_id}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    detail = await tmdb_get(f"/collection/{collection_id}")
    if not detail:
        return {}
    parts = []
    for p in detail.get("parts", []):
        ext = await tmdb_get(f"/movie/{p['id']}/external_ids")
        parts.append({"tmdb_id": p["id"], "imdb_id": ext.get("imdb_id"), "title": p.get("title"), "year": p.get("release_date", "")[:4], "poster": f"{TMDB_IMAGE_BASE}{p['poster_path']}" if p.get("poster_path") else None, "overview": p.get("overview")})
    result = {"id": collection_id, "name": detail.get("name"), "overview": detail.get("overview"), "poster": f"{TMDB_IMAGE_BASE}{detail['poster_path']}" if detail.get("poster_path") else None, "backdrop": f"{TMDB_BACKDROP_BASE}{detail['backdrop_path']}" if detail.get("backdrop_path") else None, "parts": sorted(parts, key=lambda x: x.get("year") or "")}
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


ANILIST_QUERY_SEARCH = """
query($search: String, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id idMal title { romaji english native }
      description episodes status averageScore popularity genres
      startDate { year month day } season seasonYear format duration
      studios { nodes { name } } coverImage { large extraLarge }
      bannerImage trailer { id site }
      nextAiringEpisode { airingAt episode }
    }
  }
}
"""

ANILIST_QUERY_BY_ID = """
query($id: Int) {
  Media(id: $id, type: ANIME) {
    id idMal title { romaji english native }
    description episodes status averageScore popularity genres
    tags { name rank } startDate { year month day } endDate { year month day }
    season seasonYear format duration
    studios { nodes { name isAnimationStudio } }
    coverImage { large extraLarge } bannerImage trailer { id site }
    nextAiringEpisode { airingAt episode }
    streamingEpisodes { title thumbnail url site }
    characters(sort: ROLE, perPage: 15) { nodes { name { full } image { large } } }
    staff(sort: RELEVANCE, perPage: 10) { nodes { name { full } primaryOccupations } }
    relations { edges { relationType node { id title { romaji english } type format coverImage { large } } } }
    recommendations(sort: RATING_DESC, perPage: 10) { nodes { mediaRecommendation { id title { romaji english } coverImage { large } averageScore } } }
  }
}
"""

ANILIST_QUERY_TRENDING = """
query($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: TRENDING_DESC, status_in: [RELEASING, FINISHED]) {
      id title { romaji english } episodes status averageScore format
      coverImage { large extraLarge } startDate { year } genres
    }
  }
}
"""

ANILIST_QUERY_POPULAR = """
query($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: POPULARITY_DESC) {
      id title { romaji english } episodes status averageScore format
      coverImage { large extraLarge } startDate { year } genres
    }
  }
}
"""

ANILIST_QUERY_SEASON = """
query($season: MediaSeason, $year: Int, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, season: $season, seasonYear: $year, sort: POPULARITY_DESC) {
      id title { romaji english } episodes status averageScore format
      coverImage { large extraLarge } startDate { year } genres
    }
  }
}
"""

ANILIST_QUERY_SCHEDULE = """
query($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    airingSchedules(notYetAired: false, sort: TIME_DESC) {
      airingAt episode
      media { id title { romaji english } coverImage { large } }
    }
  }
}
"""


async def anilist_request(query: str, variables: dict) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(ANILIST_BASE, json={"query": query, "variables": variables}, headers={"Content-Type": "application/json", "Accept": "application/json"})
            return resp.json()
    except Exception as e:
        logger.warning(f"AniList request failed: {e}")
        return {}


def parse_anilist_media(m: dict) -> dict:
    title = m.get("title", {})
    start = m.get("startDate", {})
    cover = m.get("coverImage", {})
    trailer = m.get("trailer", {})
    trailer_url = None
    if trailer and trailer.get("site") == "youtube":
        trailer_url = f"https://www.youtube.com/watch?v={trailer['id']}"
    studios = [s["name"] for s in m.get("studios", {}).get("nodes", [])]
    characters = [{"name": c["name"]["full"], "image": c["image"]["large"]} for c in m.get("characters", {}).get("nodes", [])] if m.get("characters") else []
    staff = [{"name": s["name"]["full"], "roles": s.get("primaryOccupations", [])} for s in m.get("staff", {}).get("nodes", [])] if m.get("staff") else []
    relations = []
    if m.get("relations"):
        for edge in m["relations"].get("edges", []):
            node = edge.get("node", {})
            relations.append({"id": node.get("id"), "title": node.get("title", {}).get("english") or node.get("title", {}).get("romaji"), "type": node.get("type"), "format": node.get("format"), "relation": edge.get("relationType"), "cover": node.get("coverImage", {}).get("large")})
    recommendations = []
    if m.get("recommendations"):
        for rec in m["recommendations"].get("nodes", []):
            mr = rec.get("mediaRecommendation", {})
            if mr:
                recommendations.append({"id": mr.get("id"), "title": mr.get("title", {}).get("english") or mr.get("title", {}).get("romaji"), "cover": mr.get("coverImage", {}).get("large"), "score": mr.get("averageScore")})
    return {
        "anilist_id": m.get("id"),
        "mal_id": m.get("idMal"),
        "title_english": title.get("english"),
        "title_romaji": title.get("romaji"),
        "title_native": title.get("native"),
        "description": re.sub(r"<[^>]+>", "", m.get("description") or ""),
        "episodes": m.get("episodes"),
        "status": m.get("status"),
        "score": m.get("averageScore"),
        "popularity": m.get("popularity"),
        "genres": m.get("genres", []),
        "tags": [{"name": t["name"], "rank": t["rank"]} for t in m.get("tags", [])[:10]] if m.get("tags") else [],
        "format": m.get("format"),
        "duration": m.get("duration"),
        "season": m.get("season"),
        "season_year": m.get("seasonYear"),
        "year": start.get("year"),
        "studios": studios,
        "cover": cover.get("extraLarge") or cover.get("large"),
        "banner": m.get("bannerImage"),
        "trailer": trailer_url,
        "next_airing": m.get("nextAiringEpisode"),
        "streaming_episodes": m.get("streamingEpisodes", []),
        "characters": characters,
        "staff": staff,
        "relations": relations,
        "recommendations": recommendations,
        "type": "anime",
    }


async def get_anilist_meta(anilist_id: int) -> dict:
    cache_key = f"anilist_{anilist_id}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    data = await anilist_request(ANILIST_QUERY_BY_ID, {"id": anilist_id})
    media = data.get("data", {}).get("Media")
    if not media:
        return {}
    result = parse_anilist_media(media)
    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def search_anilist(query: str, page: int = 1) -> list:
    data = await anilist_request(ANILIST_QUERY_SEARCH, {"search": query, "page": page, "perPage": 20})
    results = []
    for m in data.get("data", {}).get("Page", {}).get("media", []):
        title = m.get("title", {})
        cover = m.get("coverImage", {})
        start = m.get("startDate", {})
        results.append({"anilist_id": m.get("id"), "mal_id": m.get("idMal"), "title": title.get("english") or title.get("romaji"), "cover": cover.get("large"), "score": m.get("averageScore"), "episodes": m.get("episodes"), "status": m.get("status"), "format": m.get("format"), "year": start.get("year"), "genres": m.get("genres", []), "type": "anime"})
    return results


async def get_anilist_trending(page: int = 1) -> list:
    data = await anilist_request(ANILIST_QUERY_TRENDING, {"page": page, "perPage": 20})
    results = []
    for m in data.get("data", {}).get("Page", {}).get("media", []):
        title = m.get("title", {})
        cover = m.get("coverImage", {})
        results.append({"anilist_id": m.get("id"), "title": title.get("english") or title.get("romaji"), "cover": cover.get("extraLarge") or cover.get("large"), "score": m.get("averageScore"), "episodes": m.get("episodes"), "status": m.get("status"), "format": m.get("format"), "year": m.get("startDate", {}).get("year"), "genres": m.get("genres", []), "type": "anime"})
    return results


async def get_anilist_popular(page: int = 1) -> list:
    data = await anilist_request(ANILIST_QUERY_POPULAR, {"page": page, "perPage": 20})
    results = []
    for m in data.get("data", {}).get("Page", {}).get("media", []):
        title = m.get("title", {})
        cover = m.get("coverImage", {})
        results.append({"anilist_id": m.get("id"), "title": title.get("english") or title.get("romaji"), "cover": cover.get("extraLarge") or cover.get("large"), "score": m.get("averageScore"), "episodes": m.get("episodes"), "status": m.get("status"), "format": m.get("format"), "year": m.get("startDate", {}).get("year"), "genres": m.get("genres", []), "type": "anime"})
    return results


async def get_anilist_season(season: str, year: int, page: int = 1) -> list:
    data = await anilist_request(ANILIST_QUERY_SEASON, {"season": season.upper(), "year": year, "page": page, "perPage": 20})
    results = []
    for m in data.get("data", {}).get("Page", {}).get("media", []):
        title = m.get("title", {})
        cover = m.get("coverImage", {})
        results.append({"anilist_id": m.get("id"), "title": title.get("english") or title.get("romaji"), "cover": cover.get("extraLarge") or cover.get("large"), "score": m.get("averageScore"), "episodes": m.get("episodes"), "status": m.get("status"), "format": m.get("format"), "year": m.get("startDate", {}).get("year"), "genres": m.get("genres", []), "type": "anime"})
    return results


async def extract_flixhq_movie_id(imdb_id: str) -> Optional[str]:
    search_url = f"https://flixhq.to/search/{imdb_id}"
    html_content = await fetch_html(search_url)
    if not html_content:
        meta = await get_tmdb_movie_meta(imdb_id)
        title = meta.get("title", "")
        if not title:
            return None
        search_url = f"https://flixhq.to/search/{urllib.parse.quote(title)}"
        html_content = await fetch_html(search_url)
    matches = re.findall(r'href="(/movie/[^"]+)"', html_content)
    return matches[0] if matches else None


async def extract_flixhq_tv_id(imdb_id: str, season: int, episode: int) -> Optional[tuple]:
    meta = await get_tmdb_tv_meta(imdb_id)
    title = meta.get("title", "")
    if not title:
        return None
    search_url = f"https://flixhq.to/search/{urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    matches = re.findall(r'href="(/tv/[^"]+)"', html_content)
    if not matches:
        return None
    show_path = matches[0]
    show_url = f"https://flixhq.to{show_path}"
    show_html = await fetch_html(show_url)
    show_id_match = re.search(r'data-id="(\d+)"', show_html)
    if not show_id_match:
        return None
    show_id = show_id_match.group(1)
    seasons_url = f"https://flixhq.to/ajax/v2/tv/seasons/{show_id}"
    seasons_html = await fetch_html(seasons_url, headers=HEADERS_AJAX)
    season_ids = re.findall(r'data-id="(\d+)"[^>]*>.*?Season\s+(\d+)', seasons_html, re.DOTALL)
    target_season_id = None
    for sid, snum in season_ids:
        if int(snum) == season:
            target_season_id = sid
            break
    if not target_season_id:
        simple = re.findall(r'data-id="(\d+)"', seasons_html)
        if season <= len(simple):
            target_season_id = simple[season - 1]
    if not target_season_id:
        return None
    episodes_url = f"https://flixhq.to/ajax/v2/season/episodes/{target_season_id}"
    episodes_html = await fetch_html(episodes_url, headers=HEADERS_AJAX)
    episode_ids = re.findall(r'data-id="(\d+)"[^>]*>.*?Eps\s+(\d+)', episodes_html, re.DOTALL)
    target_episode_id = None
    for eid, enum in episode_ids:
        if int(enum) == episode:
            target_episode_id = eid
            break
    if not target_episode_id:
        ep_simple = re.findall(r'data-id="(\d+)"', episodes_html)
        if episode <= len(ep_simple):
            target_episode_id = ep_simple[episode - 1]
    if not target_episode_id:
        return None
    return show_id, target_episode_id


async def get_flixhq_servers(content_id: str, is_tv: bool = False, episode_id: str = None) -> list:
    if is_tv and episode_id:
        servers_url = f"https://flixhq.to/ajax/v2/episode/servers/{episode_id}"
    else:
        movie_id_match = re.search(r"-(\d+)$", content_id)
        if not movie_id_match:
            return []
        movie_id = movie_id_match.group(1)
        servers_url = f"https://flixhq.to/ajax/movie/episodes/{movie_id}"
    servers_html = await fetch_html(servers_url, headers=HEADERS_AJAX)
    servers = []
    server_matches = re.findall(r'data-id="(\d+)"[^>]*data-type="([^"]+)"[^>]*>.*?<span>([^<]+)</span>', servers_html, re.DOTALL)
    for sid, stype, sname in server_matches:
        servers.append({"id": sid, "type": stype, "name": sname.strip()})
    return servers


async def get_flixhq_source_url(server_id: str) -> Optional[str]:
    data = await fetch_json(f"https://flixhq.to/ajax/get_link/{server_id}", headers=HEADERS_AJAX)
    return data.get("link")


async def scrape_flixhq_movie(imdb_id: str) -> list:
    try:
        movie_path = await extract_flixhq_movie_id(imdb_id)
        if not movie_path:
            return []
        servers = await get_flixhq_servers(movie_path)
        streams = []
        for server in servers[:4]:
            source_link = await get_flixhq_source_url(server["id"])
            if source_link:
                extracted = await extract_stream_from_host(source_link, "https://flixhq.to")
                for stream in extracted:
                    stream["provider"] = "FlixHQ"
                    stream["server"] = server["name"]
                    streams.append(stream)
        return streams
    except Exception as e:
        logger.warning(f"scrape_flixhq_movie failed: {e}")
        return []


async def scrape_flixhq_tv(imdb_id: str, season: int, episode: int) -> list:
    try:
        result = await extract_flixhq_tv_id(imdb_id, season, episode)
        if not result:
            return []
        show_id, episode_id = result
        servers = await get_flixhq_servers(show_id, is_tv=True, episode_id=episode_id)
        streams = []
        for server in servers[:4]:
            source_link = await get_flixhq_source_url(server["id"])
            if source_link:
                extracted = await extract_stream_from_host(source_link, "https://flixhq.to")
                for stream in extracted:
                    stream["provider"] = "FlixHQ"
                    stream["server"] = server["name"]
                    streams.append(stream)
        return streams
    except Exception as e:
        logger.warning(f"scrape_flixhq_tv failed: {e}")
        return []


async def search_hdrezka(query: str) -> list:
    url = f"https://rezka.ag/search/?do=search&subaction=search&q={urllib.parse.quote(query)}"
    html_content = await fetch_html(url)
    results = []
    items = re.findall(r'<div class="b-content__inline_item"[^>]*>.*?<a href="([^"]+)"[^>]*>.*?<div class="b-content__inline_item-link">.*?<a[^>]*>([^<]+)</a>', html_content, re.DOTALL)
    for url_match, title in items:
        results.append({"url": url_match, "title": title.strip()})
    return results


async def get_hdrezka_translations(page_url: str) -> list:
    html_content = await fetch_html(page_url)
    translations = []
    trans_matches = re.findall(r'<li[^>]*data-translator_id="(\d+)"[^>]*data-id="(\d+)"[^>]*>([^<]+)</li>', html_content)
    for trans_id, content_id, trans_name in trans_matches:
        translations.append({"translator_id": trans_id, "content_id": content_id, "name": trans_name.strip()})
    if not translations:
        cid_match = re.search(r"initCDNMoviesEvents\((\d+),", html_content)
        if cid_match:
            translations.append({"translator_id": "0", "content_id": cid_match.group(1), "name": "Default"})
    return translations


async def get_hdrezka_stream(content_id: str, translator_id: str, season: int = None, episode: int = None) -> list:
    payload = {"id": content_id, "translator_id": translator_id, "is_camrip": "0", "is_ads": "0", "is_director": "0", "action": "get_movie" if not season else "get_stream"}
    if season and episode:
        payload["season"] = str(season)
        payload["episode"] = str(episode)
    headers = {**HEADERS_AJAX, "Referer": "https://rezka.ag/", "Origin": "https://rezka.ag"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post("https://rezka.ag/ajax/get_cdn_series/", data=payload, headers=headers)
            data = resp.json()
    except Exception as e:
        logger.warning(f"hdrezka stream request failed: {e}")
        return []
    if not data.get("success"):
        return []
    stream_str = data.get("url", "")
    streams = []
    for quality, url in re.findall(r'\[([^\]]+)\](https?://[^\s,]+\.m3u8[^\s,]*)', stream_str):
        streams.append({"url": url, "quality": quality.strip(), "format": "m3u8", "provider": "HDRezka"})
    if not streams:
        for quality, url in re.findall(r'\[([^\]]+)\](https?://[^\s,]+\.mp4[^\s,]*)', stream_str):
            streams.append({"url": url, "quality": quality.strip(), "format": "mp4", "provider": "HDRezka"})
    return streams


async def scrape_hdrezka_movie(imdb_id: str) -> list:
    try:
        meta = await get_tmdb_movie_meta(imdb_id)
        title = meta.get("title") or meta.get("original_title", "")
        if not title:
            return []
        results = await search_hdrezka(title)
        if not results:
            return []
        translations = await get_hdrezka_translations(results[0]["url"])
        all_streams = []
        for trans in translations[:2]:
            all_streams.extend(await get_hdrezka_stream(trans["content_id"], trans["translator_id"]))
        return all_streams
    except Exception as e:
        logger.warning(f"scrape_hdrezka_movie failed: {e}")
        return []


async def scrape_hdrezka_tv(imdb_id: str, season: int, episode: int) -> list:
    try:
        meta = await get_tmdb_tv_meta(imdb_id)
        title = meta.get("title") or meta.get("original_title", "")
        if not title:
            return []
        results = await search_hdrezka(title)
        if not results:
            return []
        translations = await get_hdrezka_translations(results[0]["url"])
        all_streams = []
        for trans in translations[:2]:
            all_streams.extend(await get_hdrezka_stream(trans["content_id"], trans["translator_id"], season, episode))
        return all_streams
    except Exception as e:
        logger.warning(f"scrape_hdrezka_tv failed: {e}")
        return []


async def search_lookmovie(title: str, is_tv: bool = False) -> Optional[dict]:
    content_type = "shows" if is_tv else "movies"
    url = f"https://lookmovie2.to/api/v1/{content_type}/search/?q={urllib.parse.quote(title)}"
    data = await fetch_json(url)
    results = data.get("result", [])
    return results[0] if results else None


async def get_lookmovie_stream(slug: str, is_tv: bool = False, season: int = None, episode: int = None) -> list:
    try:
        if is_tv:
            url = f"https://lookmovie2.to/shows/view/{slug}"
            html_content = await fetch_html(url)
            show_id_match = re.search(r'"show_storage":\s*\{[^}]*"id_show":\s*"?(\d+)"?', html_content)
            if not show_id_match:
                return []
            show_id = show_id_match.group(1)
            ep_data = await fetch_json(f"https://lookmovie2.to/api/v1/shows/episode-item/?season={season}&episode={episode}&id_show={show_id}")
            episode_id = ep_data.get("result", {}).get("id_episode")
            if not episode_id:
                return []
            stream_url = f"https://lookmovie2.to/api/v1/security/show-access/?id_episode={episode_id}&id_show={show_id}"
        else:
            url = f"https://lookmovie2.to/movies/view/{slug}"
            html_content = await fetch_html(url)
            movie_id_match = re.search(r'"movie_storage":\s*\{[^}]*"id_movie":\s*"?(\d+)"?', html_content)
            if not movie_id_match:
                return []
            movie_id = movie_id_match.group(1)
            stream_url = f"https://lookmovie2.to/api/v1/security/movie-access/?id_movie={movie_id}&token=1"
        access_data = await fetch_json(stream_url)
        result = access_data.get("result", {})
        streams = []
        for quality in ["1080p", "720p", "480p", "360p"]:
            sf = result.get(quality)
            if sf:
                streams.append({"url": sf, "quality": quality, "format": "m3u8" if ".m3u8" in sf else "mp4", "provider": "LookMovie"})
        return streams
    except Exception as e:
        logger.warning(f"get_lookmovie_stream failed: {e}")
        return []


async def scrape_lookmovie_movie(imdb_id: str) -> list:
    try:
        meta = await get_tmdb_movie_meta(imdb_id)
        title = meta.get("title", "")
        if not title:
            return []
        result = await search_lookmovie(title, is_tv=False)
        if not result:
            return []
        slug = result.get("slug")
        if not slug:
            return []
        return await get_lookmovie_stream(slug, is_tv=False)
    except Exception as e:
        logger.warning(f"scrape_lookmovie_movie failed: {e}")
        return []


async def scrape_lookmovie_tv(imdb_id: str, season: int, episode: int) -> list:
    try:
        meta = await get_tmdb_tv_meta(imdb_id)
        title = meta.get("title", "")
        if not title:
            return []
        result = await search_lookmovie(title, is_tv=True)
        if not result:
            return []
        slug = result.get("slug")
        if not slug:
            return []
        return await get_lookmovie_stream(slug, is_tv=True, season=season, episode=episode)
    except Exception as e:
        logger.warning(f"scrape_lookmovie_tv failed: {e}")
        return []


async def get_superembed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://multiembed.mov/directstream.php?video_id={imdb_id}" + (f"&s={season}&e={episode}" if season and episode else "")
        html_content = await fetch_html(url)
        streams = []
        for m in re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content):
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "provider": "SuperEmbed"})
        for m in re.findall(r'(https?://[^\s"\']+\.mp4[^\s"\']*)', html_content):
            streams.append({"url": m, "quality": "auto", "format": "mp4", "provider": "SuperEmbed"})
        return streams
    except Exception as e:
        logger.warning(f"get_superembed_sources failed: {e}")
        return []


async def get_embedsu_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://embed.su/embed/tv/{imdb_id}/{season}/{episode}" if season and episode else f"https://embed.su/embed/movie/{imdb_id}"
        html_content = await fetch_html(url)
        json_match = re.search(r'JSON\.parse\(atob\(["\']([\w+/=]+)["\']\)\)', html_content)
        if not json_match:
            return []
        decoded = base64.b64decode(json_match.group(1)).decode("utf-8")
        data = json.loads(decoded)
        streams = []
        for source in data.get("sources", []):
            url_val = source.get("file") or source.get("url")
            if url_val:
                streams.append({"url": url_val, "quality": source.get("label", "auto"), "format": "m3u8" if ".m3u8" in url_val else "mp4", "provider": "EmbedSu"})
        return streams
    except Exception as e:
        logger.warning(f"get_embedsu_sources failed: {e}")
        return []


async def get_autoembed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://autoembed.cc/embed/imdb/tv-{imdb_id}-{season}-{episode}" if season and episode else f"https://autoembed.cc/embed/imdb/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        for m in re.findall(r'"file"\s*:\s*"(https?://[^"]+\.m3u8[^"]*)"', html_content):
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "provider": "AutoEmbed"})
        for m in re.findall(r'"src"\s*:\s*"(https?://[^"]+)"', html_content):
            streams.append({"url": m, "quality": "auto", "format": "m3u8" if ".m3u8" in m else "mp4", "provider": "AutoEmbed"})
        return streams
    except Exception as e:
        logger.warning(f"get_autoembed_sources failed: {e}")
        return []


async def get_vidsrcpro_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://vidsrc.pro/embed/tv/{imdb_id}/{season}/{episode}" if season and episode else f"https://vidsrc.pro/embed/movie/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        for src in re.findall(r'(?:src|file)\s*[=:]\s*["\'](https?://[^"\']+)["\']', html_content):
            if any(x in src for x in [".m3u8", ".mp4", "stream", "playlist"]):
                streams.append({"url": src, "quality": "auto", "format": "m3u8" if ".m3u8" in src else "mp4", "provider": "VidSrcPro"})
        return streams
    except Exception as e:
        logger.warning(f"get_vidsrcpro_sources failed: {e}")
        return []


async def get_smashystream_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://player.smashy.stream/tv/{imdb_id}?s={season}&e={episode}" if season and episode else f"https://player.smashy.stream/movie/{imdb_id}"
        html_content = await fetch_html(url)
        return [{"url": m, "quality": "auto", "format": "m3u8", "provider": "SmashyStream"} for m in re.findall(r'"(https?://[^"]+\.m3u8[^"]*)"', html_content)]
    except Exception as e:
        logger.warning(f"get_smashystream_sources failed: {e}")
        return []


async def get_2embed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        url = f"https://www.2embed.cc/embedtv/{imdb_id}&s={season}&e={episode}" if season and episode else f"https://www.2embed.cc/embed/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        for iframe_src in re.findall(r'<iframe[^>]*src=["\'](https?://[^"\']+)["\']', html_content):
            if any(x in iframe_src for x in ["stream", "player", "embed"]) and "2embed" not in iframe_src:
                extracted = await extract_stream_from_host(iframe_src, url)
                for s in extracted:
                    s["provider"] = "2Embed"
                    streams.append(s)
        return streams
    except Exception as e:
        logger.warning(f"get_2embed_sources failed: {e}")
        return []


async def search_zoro(title: str) -> Optional[dict]:
    search_url = f"https://hianime.to/search?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'href="/([a-z0-9-]+-\d+)\?[^"]*"[^>]*>.*?<span[^>]*class="[^"]*film-name[^"]*"[^>]*>([^<]+)', html_content, re.DOTALL)
    if items:
        return {"id": items[0][0], "title": items[0][1].strip()}
    return None


async def get_zoro_episodes(anime_id: str) -> list:
    url = f"https://hianime.to/ajax/v2/episode/list/{anime_id.split('-')[-1]}"
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("html", "")
    episodes = []
    for ep_id, ep_num, ep_title in re.findall(r'data-id="(\d+)"[^>]*data-number="(\d+)"[^>]*title="([^"]*)"', html_content):
        episodes.append({"id": ep_id, "number": int(ep_num), "title": ep_title})
    return episodes


async def get_zoro_servers(episode_id: str) -> list:
    url = f"https://hianime.to/ajax/v2/episode/servers?episodeId={episode_id}"
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("html", "")
    servers = []
    for sid, stype, sname in re.findall(r'data-id="(\d+)"[^>]*data-type="([^"]+)"[^>]*>.*?<span>([^<]+)</span>', html_content, re.DOTALL):
        servers.append({"id": sid, "type": stype, "name": sname.strip()})
    return servers


async def get_zoro_source(server_id: str) -> Optional[str]:
    data = await fetch_json(f"https://hianime.to/ajax/v2/episode/sources?id={server_id}", headers=HEADERS_AJAX)
    return data.get("link")


async def scrape_zoro_anime(anilist_id: int, episode_num: int, sub_type: str = None) -> list:
    try:
        meta = await get_anilist_meta(anilist_id)
        title = meta.get("title_english") or meta.get("title_romaji", "")
        if not title:
            return []
        result = await search_zoro(title)
        if not result:
            return []
        episodes = await get_zoro_episodes(result["id"])
        target_ep = next((ep for ep in episodes if ep["number"] == episode_num), None)
        if not target_ep and episodes:
            target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        servers = await get_zoro_servers(target_ep["id"])
        if sub_type:
            servers = [s for s in servers if s.get("type", "").lower() == sub_type.lower()]
        streams = []
        for server in servers[:3]:
            source_link = await get_zoro_source(server["id"])
            if source_link:
                extracted = await extract_stream_from_host(source_link, "https://hianime.to")
                for s in extracted:
                    s["provider"] = "HiAnime/Zoro"
                    s["server"] = server["name"]
                    s["sub_type"] = server["type"]
                    streams.append(s)
        return streams
    except Exception as e:
        logger.warning(f"scrape_zoro_anime failed: {e}")
        return []


async def search_9anime(title: str) -> Optional[dict]:
    url = f"https://9anime.pl/filter?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(url)
    items = re.findall(r'href="/watch/([^"?]+)"[^>]*>.*?<span[^>]*>([^<]+)</span>', html_content, re.DOTALL)
    return {"id": items[0][0], "title": items[0][1].strip()} if items else None


async def get_9anime_episodes(anime_id: str) -> list:
    url = f"https://9anime.pl/ajax/episode/list/{anime_id}?vrf="
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("result", "")
    return [{"ids": ep_ids, "number": int(ep_num)} for ep_ids, ep_num in re.findall(r'data-ids="([^"]+)"[^>]*data-num="(\d+)"', html_content)]


async def get_9anime_source(episode_ids: str, anime_id: str) -> list:
    params_url = f"https://9anime.pl/ajax/episode/servers?episodeId={episode_ids}&vrf="
    servers_data = await fetch_json(params_url, headers=HEADERS_AJAX)
    result_html = servers_data.get("result", "")
    streams = []
    for link_id, server_name in re.findall(r'data-link-id="(\d+)"[^>]*data-name="([^"]+)"', result_html)[:3]:
        source_data = await fetch_json(f"https://9anime.pl/ajax/server/{link_id}?vrf=", headers=HEADERS_AJAX)
        embed_url = source_data.get("result", {}).get("url")
        if embed_url:
            extracted = await extract_stream_from_host(embed_url, "https://9anime.pl")
            for s in extracted:
                s["provider"] = "9Anime"
                s["server"] = server_name
                streams.append(s)
    return streams


async def scrape_9anime(anilist_id: int, episode_num: int) -> list:
    try:
        meta = await get_anilist_meta(anilist_id)
        title = meta.get("title_english") or meta.get("title_romaji", "")
        if not title:
            return []
        result = await search_9anime(title)
        if not result:
            return []
        episodes = await get_9anime_episodes(result["id"])
        target_ep = next((ep for ep in episodes if ep["number"] == episode_num), None)
        if not target_ep and episodes:
            target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        return await get_9anime_source(target_ep["ids"], result["id"])
    except Exception as e:
        logger.warning(f"scrape_9anime failed: {e}")
        return []


async def search_gogoanime(title: str) -> Optional[dict]:
    search_url = f"https://gogoanime3.cc/search.html?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'<div class="img">.*?<a href="([^"]+)" title="([^"]+)"', html_content, re.DOTALL)
    return {"path": items[0][0], "title": items[0][1]} if items else None


async def get_gogoanime_episodes(anime_path: str) -> list:
    url = f"https://gogoanime3.cc{anime_path}"
    html_content = await fetch_html(url)
    movie_id_match = re.search(r'<input[^>]*id="movie_id"[^>]*value="(\d+)"', html_content)
    last_ep_match = re.search(r'<input[^>]*id="ep_end"[^>]*value="(\d+)"', html_content)
    if not movie_id_match or not last_ep_match:
        return []
    movie_id = movie_id_match.group(1)
    last_ep = int(last_ep_match.group(1))
    ep_html = await fetch_html(f"https://ajax.gogocdn.net/ajax/load-list-episode?ep_start=1&ep_end={last_ep}&id={movie_id}")
    episodes = []
    for ep_path, ep_num in re.findall(r'<a href="([^"]+)".*?<div class="name">\s*EP\s*(\d+)', ep_html, re.DOTALL):
        episodes.append({"path": ep_path.strip(), "number": int(ep_num)})
    return sorted(episodes, key=lambda x: x["number"])


async def get_gogoanime_stream(episode_path: str) -> list:
    html_content = await fetch_html(f"https://gogoanime3.cc{episode_path}")
    iframe_match = re.search(r'<iframe[^>]*src="(https?://[^"]+)"[^>]*class="[^"]*player[^"]*"', html_content)
    if not iframe_match:
        iframe_match = re.search(r'<iframe[^>]*src="(https?://[^"]+(?:gogoanime|gogocdn|playtaku)[^"]*)"', html_content)
    if not iframe_match:
        return []
    embed_url = iframe_match.group(1)
    if embed_url.startswith("//"):
        embed_url = "https:" + embed_url
    return await extract_stream_from_host(embed_url, f"https://gogoanime3.cc{episode_path}")


async def scrape_gogoanime(anilist_id: int, episode_num: int) -> list:
    try:
        meta = await get_anilist_meta(anilist_id)
        title = meta.get("title_english") or meta.get("title_romaji", "")
        if not title:
            return []
        result = await search_gogoanime(title)
        if not result:
            return []
        episodes = await get_gogoanime_episodes(result["path"])
        target_ep = next((ep for ep in episodes if ep["number"] == episode_num), None)
        if not target_ep and episodes:
            target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        streams = await get_gogoanime_stream(target_ep["path"])
        for s in streams:
            s["provider"] = "GogoAnime"
        return streams
    except Exception as e:
        logger.warning(f"scrape_gogoanime failed: {e}")
        return []


async def search_kissasian(title: str) -> Optional[dict]:
    search_url = f"https://kissasian.sh/Search/SearchSuggest?type=Drama&keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'<a href="([^"]+)"[^>]*>([^<]+)</a>', html_content)
    return {"url": items[0][0], "title": items[0][1].strip()} if items else None


async def get_kissasian_episodes(drama_url: str) -> list:
    html_content = await fetch_html(drama_url)
    episodes = []
    for ep_url, ep_num in re.findall(r'<li[^>]*>.*?<a href="([^"]+)"[^>]*>.*?Episode\s+(\d+)', html_content, re.DOTALL):
        episodes.append({"url": ep_url, "number": int(ep_num)})
    return sorted(episodes, key=lambda x: x["number"])


async def get_kissasian_stream(episode_url: str) -> list:
    html_content = await fetch_html(episode_url)
    streams = []
    for iframe_src in re.findall(r'<iframe[^>]*src=["\'](https?://[^"\']+)["\']', html_content):
        if "kissasian" not in iframe_src:
            extracted = await extract_stream_from_host(iframe_src, episode_url)
            for s in extracted:
                s["provider"] = "KissAsian"
                streams.append(s)
    return streams


async def scrape_kissasian(imdb_id: str, season: int, episode: int) -> list:
    try:
        meta = await get_tmdb_tv_meta(imdb_id)
        title = meta.get("title", "")
        if not title:
            return []
        result = await search_kissasian(title)
        if not result:
            return []
        episodes = await get_kissasian_episodes(result["url"])
        target_ep = next((ep for ep in episodes if ep["number"] == episode), None)
        if not target_ep and episodes:
            overall = (season - 1) * 100 + episode
            target_ep = episodes[min(overall - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        return await get_kissasian_stream(target_ep["url"])
    except Exception as e:
        logger.warning(f"scrape_kissasian failed: {e}")
        return []


async def extract_filemoon(url: str, referer: str = "") -> list:
    try:
        headers = {**HEADERS_CHROME}
        if referer:
            headers["Referer"] = referer
        html_content = await fetch_html(url, headers=headers)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            unpacked = unpack_js(packed_match.group(0))
            m3u8_matches = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', unpacked)
            if m3u8_matches:
                return [{"url": m3u8_matches[0], "quality": "auto", "format": "m3u8", "host": "Filemoon"}]
        for pattern in [r'"file"\s*:\s*"(https?://[^"]+\.m3u8[^"]*)"', r'jwplayer\([^)]+\)\.setup\(\{.*?"file"\s*:\s*"([^"]+)"']:
            match = re.search(pattern, html_content, re.DOTALL)
            if match:
                fu = match.group(1)
                return [{"url": fu, "quality": "auto", "format": "m3u8" if ".m3u8" in fu else "mp4", "host": "Filemoon"}]
        return []
    except Exception as e:
        logger.warning(f"extract_filemoon failed: {e}")
        return []


async def extract_vidplay(url: str, referer: str = "") -> list:
    try:
        headers = {**HEADERS_CHROME}
        if referer:
            headers["Referer"] = referer
        html_content = await fetch_html(url, headers=headers)
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', html_content, re.DOTALL)
        if sources_match:
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
            label_matches = re.findall(r'"label"\s*:\s*"([^"]+)"', sources_match.group(1))
            return [{"url": fu, "quality": label_matches[i] if i < len(label_matches) else "auto", "format": "m3u8" if ".m3u8" in fu else "mp4", "host": "Vidplay"} for i, fu in enumerate(file_matches)]
        m3u8 = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content)
        return [{"url": m, "quality": "auto", "format": "m3u8", "host": "Vidplay"}] if m3u8 else []
    except Exception as e:
        logger.warning(f"extract_vidplay failed: {e}")
        return []


async def extract_streamtape(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        concat_match = re.search(r"document\.getElementById\('robotlink'\)\.innerHTML\s*=\s*([^<]+)<", html_content)
        if concat_match:
            parts = re.findall(r'"([^"]+)"', concat_match.group(1))
            if parts:
                stream_url = "https:" + "".join(parts)
                return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "StreamTape"}]
        direct = re.search(r'(https?://[^\s"\']+streamtape[^\s"\']+)', html_content)
        if direct:
            return [{"url": direct.group(1), "quality": "auto", "format": "mp4", "host": "StreamTape"}]
        return []
    except Exception as e:
        logger.warning(f"extract_streamtape failed: {e}")
        return []


async def extract_doodstream(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        pass_match = re.search(r"/pass_md5/([^'\"]+)", html_content)
        if not pass_match:
            return []
        pass_path = pass_match.group(0)
        base_match = re.match(r"(https?://[^/]+)", url)
        if not base_match:
            return []
        base = base_match.group(1)
        async with httpx.AsyncClient(headers={**HEADERS_CHROME, "Referer": url}, timeout=15) as client:
            resp = await client.get(f"{base}{pass_path}")
            token = resp.text.strip()
        if not token:
            return []
        rand_str = "".join(random.choices(string.ascii_letters + string.digits, k=10))
        final_url = f"{token}{rand_str}?token={pass_path.split('/')[-1]}&expiry={int(time.time()) + 3600}"
        return [{"url": final_url, "quality": "auto", "format": "mp4", "host": "DoodStream"}]
    except Exception as e:
        logger.warning(f"extract_doodstream failed: {e}")
        return []


async def extract_upstream(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            unpacked = unpack_js(packed_match.group(0))
            m3u8 = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', unpacked)
            if m3u8:
                return [{"url": m3u8[0], "quality": "auto", "format": "m3u8", "host": "Upstream"}]
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', html_content, re.DOTALL)
        if sources_match:
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
            if file_matches:
                return [{"url": file_matches[0], "quality": "auto", "format": "m3u8" if ".m3u8" in file_matches[0] else "mp4", "host": "Upstream"}]
        return []
    except Exception as e:
        logger.warning(f"extract_upstream failed: {e}")
        return []


async def extract_mixdrop(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        src = html_content
        if packed_match:
            src = unpack_js(packed_match.group(0))
        wurl_match = re.search(r'wurl\s*=\s*"([^"]+)"', src)
        if wurl_match:
            stream_url = wurl_match.group(1)
            if stream_url.startswith("//"):
                stream_url = "https:" + stream_url
            return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "MixDrop"}]
        return []
    except Exception as e:
        logger.warning(f"extract_mixdrop failed: {e}")
        return []


async def extract_mp4upload(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        for pattern in [r'"src"\s*:\s*"(https?://[^"]+\.mp4[^"]*)"', r'"file"\s*:\s*"(https?://[^"]+)"']:
            match = re.search(pattern, html_content)
            if match:
                fu = match.group(1)
                return [{"url": fu, "quality": "auto", "format": "m3u8" if ".m3u8" in fu else "mp4", "host": "Mp4Upload"}]
        return []
    except Exception as e:
        logger.warning(f"extract_mp4upload failed: {e}")
        return []


async def extract_voe(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        for pattern in [r"'hls'\s*:\s*'([^']+)'", r'"hls"\s*:\s*"([^"]+)"']:
            match = re.search(pattern, html_content)
            if match:
                m3u8_url = match.group(1)
                if not m3u8_url.startswith("http"):
                    try:
                        m3u8_url = base64.b64decode(m3u8_url).decode("utf-8")
                    except Exception:
                        pass
                return [{"url": m3u8_url, "quality": "auto", "format": "m3u8", "host": "VOE"}]
        return []
    except Exception as e:
        logger.warning(f"extract_voe failed: {e}")
        return []


async def extract_vidhide(url: str, referer: str = "") -> list:
    try:
        headers = {**HEADERS_CHROME, "Referer": referer or "https://flixhq.to"}
        html_content = await fetch_html(url, headers=headers)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        src = unpack_js(packed_match.group(0)) if packed_match else html_content
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', src, re.DOTALL)
        if sources_match:
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
            if file_matches:
                return [{"url": file_matches[0], "quality": "auto", "format": "m3u8" if ".m3u8" in file_matches[0] else "mp4", "host": "VidHide"}]
        return []
    except Exception as e:
        logger.warning(f"extract_vidhide failed: {e}")
        return []


async def extract_fembed(url: str, referer: str = "") -> list:
    try:
        vid_id_match = re.search(r"/(?:v|f)/([^/?]+)", url)
        if not vid_id_match:
            return []
        vid_id = vid_id_match.group(1)
        base = re.match(r"(https?://[^/]+)", url).group(1)
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(f"{base}/api/source/{vid_id}", headers={**HEADERS_AJAX, "Referer": url})
            data = resp.json()
        if not data.get("success"):
            return []
        return [{"url": src["file"], "quality": src.get("label", "auto"), "format": src.get("type", "mp4"), "host": "Fembed"} for src in data.get("data", [])]
    except Exception as e:
        logger.warning(f"extract_fembed failed: {e}")
        return []


async def extract_okru(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        data_match = re.search(r'data-options="([^"]+)"', html_content)
        if not data_match:
            return []
        data = json.loads(html.unescape(data_match.group(1)))
        flash_vars = data.get("flashvars", {})
        metadata_str = flash_vars.get("metadata") or flash_vars.get("videoSources")
        if not metadata_str:
            return []
        metadata = json.loads(metadata_str) if isinstance(metadata_str, str) else metadata_str
        videos = metadata.get("videos", metadata) if isinstance(metadata, dict) else metadata
        if isinstance(videos, list):
            return [{"url": v.get("url"), "quality": v.get("name", "auto"), "format": "mp4", "host": "OK.ru"} for v in videos if v.get("url")]
        return []
    except Exception as e:
        logger.warning(f"extract_okru failed: {e}")
        return []


async def extract_stream_from_host(url: str, referer: str = "") -> list:
    if not url:
        return []
    url_lower = url.lower()
    extractors = {
        ("filemoon", "moonplayer"): extract_filemoon,
        ("vidplay", "vidstream", "mcloud"): extract_vidplay,
        ("streamtape",): extract_streamtape,
        ("dood",): extract_doodstream,
        ("upstream", "upns"): extract_upstream,
        ("mixdrop",): extract_mixdrop,
        ("mp4upload",): extract_mp4upload,
        ("fembed", "layar.kita"): extract_fembed,
        ("ok.ru", "odnoklassniki"): extract_okru,
        ("voe.sx",): extract_voe,
        ("vidhide", "vid.icu"): extract_vidhide,
    }
    for keywords, extractor in extractors.items():
        if any(k in url_lower for k in keywords):
            return await extractor(url, referer)
    html_content = await fetch_html(url, headers={**HEADERS_CHROME, "Referer": referer})
    streams = []
    for m in re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content):
        streams.append({"url": m, "quality": "auto", "format": "m3u8", "host": "Unknown"})
    if not streams:
        for m in re.findall(r'(https?://[^\s"\']+\.mp4[^\s"\']*)', html_content):
            streams.append({"url": m, "quality": "auto", "format": "mp4", "host": "Unknown"})
    return streams


async def search_subtitles(imdb_id: str, season: int = None, episode: int = None, languages: str = "en") -> list:
    cache_key = f"subs_{imdb_id}_{season}_{episode}_{languages}"
    cached = cache_get(_meta_cache, cache_key)
    if cached:
        return cached
    try:
        params = {"imdb_id": imdb_id.replace("tt", ""), "languages": languages}
        if season:
            params["season_number"] = season
        if episode:
            params["episode_number"] = episode
        headers = {"Api-Key": OPENSUBS_API_KEY, "User-Agent": "CineAPI v2.0"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{OPENSUBS_BASE}/subtitles", params=params, headers=headers)
            data = resp.json()
        results = []
        for item in data.get("data", [])[:20]:
            attrs = item.get("attributes", {})
            files = attrs.get("files", [])
            if files:
                results.append({"id": item.get("id"), "language": attrs.get("language"), "file_name": attrs.get("release", ""), "upload_date": attrs.get("upload_date", ""), "downloads": attrs.get("download_count", 0), "rating": attrs.get("ratings", 0), "file_id": files[0].get("file_id"), "format": "srt"})
        cache_set(_meta_cache, cache_key, results, CACHE_TTL_META)
        return results
    except Exception as e:
        logger.warning(f"search_subtitles failed: {e}")
        return []


async def get_subtitle_download_url(file_id: int) -> Optional[str]:
    try:
        headers = {"Api-Key": OPENSUBS_API_KEY, "User-Agent": "CineAPI v2.0", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{OPENSUBS_BASE}/download", json={"file_id": file_id}, headers=headers)
            return resp.json().get("link")
    except Exception as e:
        logger.warning(f"get_subtitle_download_url failed: {e}")
        return None


async def convert_srt_to_vtt_content(srt_content: str) -> str:
    vtt = "WEBVTT\n\n"
    srt_content = srt_content.strip()
    blocks = re.split(r"\n\n+", srt_content)
    for block in blocks:
        lines = block.strip().split("\n")
        if len(lines) < 3:
            continue
        try:
            int(lines[0].strip())
        except ValueError:
            continue
        timestamp = lines[1].replace(",", ".")
        text = "\n".join(lines[2:])
        vtt += f"{timestamp}\n{text}\n\n"
    return vtt


async def resolve_movie_streams(imdb_id: str) -> list:
    cache_key = f"movie_streams_{imdb_id}"
    cached = cache_get(_stream_cache, cache_key)
    if cached:
        return cached
    tasks = [
        scrape_flixhq_movie(imdb_id),
        scrape_hdrezka_movie(imdb_id),
        scrape_lookmovie_movie(imdb_id),
        get_superembed_sources(imdb_id),
        get_embedsu_sources(imdb_id),
        get_autoembed_sources(imdb_id),
        get_smashystream_sources(imdb_id),
        get_2embed_sources(imdb_id),
        get_vidsrcpro_sources(imdb_id),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_streams = []
    for r in results:
        if isinstance(r, list):
            all_streams.extend(r)
    all_streams = dedupe_streams(rank_streams(all_streams))
    cache_set(_stream_cache, cache_key, all_streams, CACHE_TTL_STREAM)
    return all_streams


async def resolve_tv_streams(imdb_id: str, season: int, episode: int) -> list:
    cache_key = f"tv_streams_{imdb_id}_{season}_{episode}"
    cached = cache_get(_stream_cache, cache_key)
    if cached:
        return cached
    tasks = [
        scrape_flixhq_tv(imdb_id, season, episode),
        scrape_hdrezka_tv(imdb_id, season, episode),
        scrape_lookmovie_tv(imdb_id, season, episode),
        get_superembed_sources(imdb_id, season, episode),
        get_embedsu_sources(imdb_id, season, episode),
        get_autoembed_sources(imdb_id, season, episode),
        get_smashystream_sources(imdb_id, season, episode),
        get_2embed_sources(imdb_id, season, episode),
        get_vidsrcpro_sources(imdb_id, season, episode),
        scrape_kissasian(imdb_id, season, episode),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_streams = []
    for r in results:
        if isinstance(r, list):
            all_streams.extend(r)
    all_streams = dedupe_streams(rank_streams(all_streams))
    cache_set(_stream_cache, cache_key, all_streams, CACHE_TTL_STREAM)
    return all_streams


async def resolve_anime_streams(anilist_id: int, episode_num: int, sub_type: str = None) -> list:
    cache_key = f"anime_streams_{anilist_id}_{episode_num}_{sub_type}"
    cached = cache_get(_stream_cache, cache_key)
    if cached:
        return cached
    tasks = [
        scrape_zoro_anime(anilist_id, episode_num, sub_type),
        scrape_9anime(anilist_id, episode_num),
        scrape_gogoanime(anilist_id, episode_num),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_streams = []
    for r in results:
        if isinstance(r, list):
            all_streams.extend(r)
    all_streams = dedupe_streams(rank_streams(all_streams))
    cache_set(_stream_cache, cache_key, all_streams, CACHE_TTL_STREAM)
    return all_streams


def generate_job_id() -> str:
    return secrets.token_hex(12)


async def run_download_job(job_id: str, stream_url: str, output_path: Path, filename: str):
    global _active_downloads
    _active_downloads += 1
    job = _download_jobs[job_id]
    job["status"] = "downloading"
    job["started_at"] = time.time()
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg", "-y",
            "-headers", f"User-Agent: {HEADERS_CHROME['User-Agent']}\r\nReferer: https://google.com\r\n",
            "-i", stream_url,
            "-c", "copy",
            "-movflags", "+faststart",
            "-progress", "pipe:1",
            "-nostats",
            str(output_path)
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        job["pid"] = process.pid
        job["status"] = "processing"

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line = line.decode("utf-8", errors="ignore").strip()
            if "out_time_ms=" in line:
                try:
                    ms = int(line.split("=")[1])
                    job["progress_ms"] = ms
                except Exception:
                    pass
            if "speed=" in line:
                try:
                    job["speed"] = line.split("=")[1]
                except Exception:
                    pass

        await process.wait()
        if process.returncode == 0 and output_path.exists():
            job["status"] = "ready"
            job["file_path"] = str(output_path)
            job["file_size_mb"] = round(output_path.stat().st_size / (1024 * 1024), 2)
            job["completed_at"] = time.time()
            job["expires_at"] = time.time() + DOWNLOAD_TTL
        else:
            stderr_out = await process.stderr.read()
            job["status"] = "error"
            job["error"] = stderr_out.decode("utf-8", errors="ignore")[-500:]
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        logger.warning(f"Download job {job_id} failed: {e}")
    finally:
        _active_downloads -= 1
        job.pop("pid", None)


async def start_download(job_id: str, imdb_id: str = None, anilist_id: int = None, season: int = None, episode: int = None, quality: str = None, filename: str = None, is_anime: bool = False):
    job = _download_jobs.get(job_id)
    if not job:
        return

    try:
        if is_anime and anilist_id:
            streams = await resolve_anime_streams(anilist_id, episode or 1)
            meta = await get_anilist_meta(anilist_id)
            title = meta.get("title_english") or meta.get("title_romaji") or str(anilist_id)
            label = f"ep{episode}"
        elif season and episode:
            streams = await resolve_tv_streams(imdb_id, season, episode)
            meta = await get_tmdb_tv_meta(imdb_id)
            title = meta.get("title") or imdb_id
            label = f"S{season:02d}E{episode:02d}"
        else:
            streams = await resolve_movie_streams(imdb_id)
            meta = await get_tmdb_movie_meta(imdb_id)
            title = meta.get("title") or imdb_id
            label = meta.get("year") or ""

        if not streams:
            _download_jobs[job_id]["status"] = "error"
            _download_jobs[job_id]["error"] = "No streams found"
            return

        if quality:
            preferred = [s for s in streams if quality.lower() in s.get("quality", "").lower()]
            stream = preferred[0] if preferred else streams[0]
        else:
            stream = streams[0]

        safe_title = re.sub(r'[^\w\s-]', '', title).strip().replace(" ", "_")[:50]
        out_filename = filename or f"{safe_title}_{label}_{stream.get('quality', 'auto')}.mp4"
        out_filename = re.sub(r'[^\w._-]', '', out_filename)
        output_path = DOWNLOAD_DIR / out_filename

        _download_jobs[job_id]["title"] = title
        _download_jobs[job_id]["stream_url"] = stream["url"]
        _download_jobs[job_id]["quality"] = stream.get("quality", "auto")
        _download_jobs[job_id]["provider"] = stream.get("provider", "Unknown")
        _download_jobs[job_id]["filename"] = out_filename

        await run_download_job(job_id, stream["url"], output_path, out_filename)

    except Exception as e:
        _download_jobs[job_id]["status"] = "error"
        _download_jobs[job_id]["error"] = str(e)
        logger.warning(f"start_download failed for job {job_id}: {e}")


async def cleanup_downloads():
    now = time.time()
    to_delete = []
    for job_id, job in _download_jobs.items():
        expires = job.get("expires_at")
        if expires and now > expires:
            fp = job.get("file_path")
            if fp:
                try:
                    Path(fp).unlink(missing_ok=True)
                except Exception:
                    pass
            to_delete.append(job_id)
    for jid in to_delete:
        del _download_jobs[jid]


async def get_cloudflare_analytics(period: str = "24h") -> dict:
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare credentials not configured. Set CLOUDFLARE_API_TOKEN and CLOUDFLARE_ZONE_ID env vars."}
    periods = {"1h": "-60", "24h": "-1440", "7d": "-10080", "30d": "-43200"}
    minutes = periods.get(period, "-1440")
    headers = {"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"https://api.cloudflare.com/client/v4/zones/{CLOUDFLARE_ZONE_ID}/analytics/dashboard?since={minutes}&continuous=true", headers=headers)
            data = resp.json()
        if not data.get("success"):
            return {"error": "Cloudflare API error", "details": data.get("errors")}
        totals = data.get("result", {}).get("totals", {})
        requests_data = totals.get("requests", {})
        bandwidth_data = totals.get("bandwidth", {})
        threats_data = totals.get("threats", {})
        pageviews_data = totals.get("pageviews", {})
        return {
            "period": period,
            "requests": {"total": requests_data.get("all", 0), "cached": requests_data.get("cached", 0), "uncached": requests_data.get("uncached", 0), "cache_hit_rate": round(requests_data.get("cached", 0) / max(requests_data.get("all", 1), 1), 4)},
            "bandwidth": {"total_gb": round(bandwidth_data.get("all", 0) / (1024 ** 3), 3), "cached_gb": round(bandwidth_data.get("cached", 0) / (1024 ** 3), 3), "uncached_gb": round(bandwidth_data.get("uncached", 0) / (1024 ** 3), 3)},
            "threats": {"total": threats_data.get("all", 0), "type_breakdown": threats_data.get("type", {})},
            "pageviews": {"total": pageviews_data.get("all", 0), "search_engine": pageviews_data.get("search_engine", {})},
        }
    except Exception as e:
        logger.warning(f"Cloudflare analytics failed: {e}")
        return {"error": str(e)}


async def get_cloudflare_firewall(limit: int = 25) -> dict:
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare credentials not configured"}
    headers = {"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"https://api.cloudflare.com/client/v4/zones/{CLOUDFLARE_ZONE_ID}/security/events?per_page={limit}", headers=headers)
            data = resp.json()
        if not data.get("success"):
            return {"error": "Cloudflare API error", "details": data.get("errors")}
        events = []
        for ev in data.get("result", []):
            events.append({"timestamp": ev.get("occurred_at"), "action": ev.get("action"), "rule_id": ev.get("rule_id"), "source": ev.get("source"), "ip": ev.get("client_ip"), "country": ev.get("client_country_name"), "method": ev.get("request", {}).get("method"), "path": ev.get("request", {}).get("uri")})
        return {"events": events, "total": len(events)}
    except Exception as e:
        return {"error": str(e)}


async def check_provider_health() -> dict:
    providers = {
        "FlixHQ": "https://flixhq.to",
        "HDRezka": "https://rezka.ag",
        "LookMovie": "https://lookmovie2.to",
        "HiAnime": "https://hianime.to",
        "GogoAnime": "https://gogoanime3.cc",
        "9Anime": "https://9anime.pl",
        "KissAsian": "https://kissasian.sh",
        "SuperEmbed": "https://multiembed.mov",
        "AutoEmbed": "https://autoembed.cc",
        "2Embed": "https://www.2embed.cc",
        "VidSrcPro": "https://vidsrc.pro",
        "SmashyStream": "https://player.smashy.stream",
        "AniList": "https://graphql.anilist.co",
        "TMDB": "https://api.themoviedb.org",
    }
    results = {}

    async def ping_one(name, url):
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                resp = await client.head(url, headers=HEADERS_CHROME)
                latency = round((time.time() - start) * 1000)
                results[name] = {"status": "up" if resp.status_code < 500 else "degraded", "status_code": resp.status_code, "latency_ms": latency}
        except Exception as e:
            results[name] = {"status": "down", "latency_ms": None, "error": str(e)[:60]}

    await asyncio.gather(*[ping_one(name, url) for name, url in providers.items()])
    return results


def get_system_stats() -> dict:
    try:
        ram = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        disk = psutil.disk_usage("/")
        net = psutil.net_io_counters()
        download_dir_size = 0
        if DOWNLOAD_DIR.exists():
            download_dir_size = sum(f.stat().st_size for f in DOWNLOAD_DIR.rglob("*") if f.is_file())
        active_jobs = sum(1 for j in _download_jobs.values() if j["status"] in ("downloading", "processing", "queued"))
        ready_jobs = sum(1 for j in _download_jobs.values() if j["status"] == "ready")
        failed_jobs = sum(1 for j in _download_jobs.values() if j["status"] == "error")
        uptime = time.time() - _start_time
        now = time.time()
        rpm_window = [t for t in _request_stats["rpm_window"] if t > now - 60]
        return {
            "uptime_seconds": round(uptime),
            "uptime_human": f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m {int(uptime % 60)}s",
            "system": {
                "cpu_percent": cpu,
                "ram_used_mb": round(ram.used / (1024 ** 2), 1),
                "ram_total_mb": round(ram.total / (1024 ** 2), 1),
                "ram_available_mb": round(ram.available / (1024 ** 2), 1),
                "ram_percent": ram.percent,
                "disk_used_gb": round(disk.used / (1024 ** 3), 2),
                "disk_total_gb": round(disk.total / (1024 ** 3), 2),
                "disk_free_gb": round(disk.free / (1024 ** 3), 2),
                "disk_percent": disk.percent,
                "net_sent_mb": round(net.bytes_sent / (1024 ** 2), 2),
                "net_recv_mb": round(net.bytes_recv / (1024 ** 2), 2),
            },
            "requests": {
                "total": _request_stats["total"],
                "today": _request_stats["today"],
                "errors_4xx": _request_stats["errors_4xx"],
                "errors_5xx": _request_stats["errors_5xx"],
                "current_rpm": len(rpm_window),
                "peak_rpm": _request_stats["peak_rpm"],
                "top_endpoints": sorted(_request_stats["by_endpoint"].items(), key=lambda x: x[1], reverse=True)[:10],
                "top_ips": sorted(_request_stats["by_ip"].items(), key=lambda x: x[1], reverse=True)[:10],
            },
            "downloads": {
                "active_jobs": active_jobs,
                "queued_jobs": sum(1 for j in _download_jobs.values() if j["status"] == "queued"),
                "ready_jobs": ready_jobs,
                "failed_jobs": failed_jobs,
                "total_jobs": len(_download_jobs),
                "storage_used_mb": round(download_dir_size / (1024 ** 2), 2),
            },
            "cache": {
                "streams_cached": len(_stream_cache),
                "meta_cached": len(_meta_cache),
                "search_cached": len(_search_cache),
            },
            "api_version": API_VERSION,
            "project_version": PROJECT_VERSION,
        }
    except Exception as e:
        logger.warning(f"get_system_stats failed: {e}")
        return {"error": str(e)}


async def background_tasks_runner():
    while True:
        try:
            await asyncio.sleep(300)
            cache_cleanup_all()
            await cleanup_downloads()
            cached_health = await check_provider_health()
            cache_set(_provider_health_cache, "all_providers", cached_health, CACHE_TTL_PROVIDER)
        except Exception as e:
            logger.warning(f"Background task error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"{PROJECT_NAME} v{PROJECT_VERSION} by {PROJECT_AUTHOR} / {PROJECT_COMPANY} starting...")
    logger.info(f"Master API Key: {MASTER_API_KEY}")
    logger.info(f"Download directory: {DOWNLOAD_DIR}")
    task = asyncio.create_task(background_tasks_runner())
    yield
    task.cancel()
    cache_cleanup_all()
    logger.info(f"{PROJECT_NAME} shutting down.")


app = FastAPI(
    title=PROJECT_NAME,
    description=f"Streaming, Download & Source API — Built by {PROJECT_AUTHOR} under {PROJECT_COMPANY}",
    version=PROJECT_VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start = time.time()
    if not validate_api_key(request):
        duration = time.time() - start
        track_request(request, 403, duration)
        return JSONResponse(status_code=403, content={"status": "error", "detail": "Invalid or missing API key. Pass via ?api_key=, X-API-Key header, or Authorization: Bearer."})
    response = await call_next(request)
    duration = time.time() - start
    track_request(request, response.status_code, duration)
    response.headers["X-Response-Time"] = f"{round(duration * 1000)}ms"
    response.headers["X-Powered-By"] = f"{PROJECT_NAME}/{PROJECT_VERSION}"
    return response


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{PROJECT_NAME}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#050505;color:#fff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px}}
.logo{{font-size:52px;font-weight:900;letter-spacing:-2px;margin-bottom:6px}}
.tagline{{color:rgba(255,255,255,0.4);font-size:13px;margin-bottom:48px;letter-spacing:3px;text-transform:uppercase}}
.key-badge{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:10px 18px;font-family:monospace;font-size:13px;color:rgba(255,255,255,0.7);margin-bottom:40px;word-break:break-all;max-width:600px;text-align:center}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;max-width:1000px;width:100%;margin-bottom:48px}}
.card{{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:18px}}
.card h3{{font-size:11px;color:rgba(255,255,255,0.35);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px}}
.endpoint{{font-family:'Courier New',monospace;font-size:11px;color:#a8c7fa;padding:5px 9px;background:rgba(168,199,250,0.06);border-radius:5px;margin-bottom:5px;display:block}}
.footer{{color:rgba(255,255,255,0.2);font-size:12px;letter-spacing:1px}}
.footer span{{color:rgba(255,255,255,0.4)}}
</style>
</head>
<body>
<div class="logo">{PROJECT_NAME}</div>
<div class="tagline">Streaming · Download · Intelligence · v{PROJECT_VERSION}</div>
<div class="key-badge">🔑 Your API Key: Set MASTER_API_KEY env var (check server logs on first boot)</div>
<div class="grid">
  <div class="card"><h3>Movies</h3>
    <code class="endpoint">GET /api/v1/meta/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/stream/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/download/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /embed/movie/{{imdb_id}}</code>
  </div>
  <div class="card"><h3>TV Shows</h3>
    <code class="endpoint">GET /api/v1/meta/tv/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/stream/tv/{{imdb_id}}/{{s}}/{{e}}</code>
    <code class="endpoint">GET /api/v1/download/tv/{{imdb_id}}/{{s}}/{{e}}</code>
    <code class="endpoint">GET /embed/tv/{{imdb_id}}/{{s}}/{{e}}</code>
  </div>
  <div class="card"><h3>Anime</h3>
    <code class="endpoint">GET /api/v1/meta/anime/{{anilist_id}}</code>
    <code class="endpoint">GET /api/v1/stream/anime/{{anilist_id}}/{{ep}}</code>
    <code class="endpoint">GET /api/v1/download/anime/{{anilist_id}}/{{ep}}</code>
    <code class="endpoint">GET /embed/anime/{{anilist_id}}/{{ep}}</code>
  </div>
  <div class="card"><h3>Downloads</h3>
    <code class="endpoint">POST /api/v1/download/batch</code>
    <code class="endpoint">GET /api/v1/download/status/{{job_id}}</code>
    <code class="endpoint">GET /api/v1/download/file/{{job_id}}</code>
    <code class="endpoint">GET /api/v1/download/list</code>
  </div>
  <div class="card"><h3>Discover</h3>
    <code class="endpoint">GET /api/v1/search?q=&type=movie</code>
    <code class="endpoint">GET /api/v1/discover/movie</code>
    <code class="endpoint">GET /api/v1/trending</code>
    <code class="endpoint">GET /api/v1/popular</code>
  </div>
  <div class="card"><h3>Subtitles</h3>
    <code class="endpoint">GET /api/v1/subtitles/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/subtitles/proxy/{{file_id}}</code>
    <code class="endpoint">GET /api/v1/subtitles/vtt/{{file_id}}</code>
  </div>
  <div class="card"><h3>People & Collections</h3>
    <code class="endpoint">GET /api/v1/meta/person/{{person_id}}</code>
    <code class="endpoint">GET /api/v1/meta/collection/{{collection_id}}</code>
    <code class="endpoint">GET /api/v1/meta/tv/{{imdb_id}}/season/{{s}}</code>
  </div>
  <div class="card"><h3>System</h3>
    <code class="endpoint">GET /api/v1/system/stats</code>
    <code class="endpoint">GET /api/v1/system/cloudflare</code>
    <code class="endpoint">GET /api/v1/system/providers/health</code>
    <code class="endpoint">GET /api/v1/system/stats/requests</code>
  </div>
</div>
<div class="footer">Built by <span>{PROJECT_AUTHOR}</span> · <span>{PROJECT_COMPANY}</span> · {PROJECT_NAME} {PROJECT_VERSION}</div>
</body>
</html>""")


@app.get("/health")
async def health():
    return {"status": "ok", "project": PROJECT_NAME, "version": PROJECT_VERSION, "author": PROJECT_AUTHOR, "company": PROJECT_COMPANY, "uptime_seconds": round(time.time() - _start_time)}


@app.get("/api/v1/meta/movie/{imdb_id}")
async def meta_movie(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="imdb_id must be in format tt1234567")
    data = await get_tmdb_movie_meta(imdb_id)
    if not data:
        raise HTTPException(status_code=404, detail="Movie not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/tv/{imdb_id}")
async def meta_tv(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="imdb_id must be in format tt1234567")
    data = await get_tmdb_tv_meta(imdb_id)
    if not data:
        raise HTTPException(status_code=404, detail="TV show not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/tv/{imdb_id}/season/{season}")
async def meta_tv_season(imdb_id: str, season: int):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="imdb_id must be in format tt1234567")
    show = await get_tmdb_tv_meta(imdb_id)
    if not show:
        raise HTTPException(status_code=404, detail="TV show not found")
    data = await get_tmdb_tv_season(show["tmdb_id"], season)
    if not data:
        raise HTTPException(status_code=404, detail="Season not found")
    return {"status": "ok", "imdb_id": imdb_id, "data": data}


@app.get("/api/v1/meta/anime/{anilist_id}")
async def meta_anime(anilist_id: int):
    data = await get_anilist_meta(anilist_id)
    if not data:
        raise HTTPException(status_code=404, detail="Anime not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/person/{person_id}")
async def meta_person(person_id: int):
    data = await tmdb_person(person_id)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/collection/{collection_id}")
async def meta_collection(collection_id: int):
    data = await tmdb_collection(collection_id)
    if not data:
        raise HTTPException(status_code=404, detail="Collection not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/stream/movie/{imdb_id}")
async def stream_movie(imdb_id: str, provider: Optional[str] = None, quality: Optional[str] = None):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_movie_streams(imdb_id)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    if provider:
        streams = [s for s in streams if s.get("provider", "").lower() == provider.lower()]
    if quality:
        streams = [s for s in streams if quality.lower() in s.get("quality", "").lower()] or streams
    meta = await get_tmdb_movie_meta(imdb_id)
    return {"status": "ok", "imdb_id": imdb_id, "title": meta.get("title"), "year": meta.get("year"), "streams": streams, "total": len(streams)}


@app.get("/api/v1/stream/movie/{imdb_id}/best")
async def stream_movie_best(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_movie_streams(imdb_id)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    return {"status": "ok", "imdb_id": imdb_id, "stream": streams[0]}


@app.get("/api/v1/stream/movie/{imdb_id}/m3u8")
async def stream_movie_m3u8(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_movie_streams(imdb_id)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    meta = await get_tmdb_movie_meta(imdb_id)
    title = meta.get("title", imdb_id)
    m3u8_content = f"#EXTM3U\n#PLAYLIST:{title}\n"
    for s in streams:
        quality = s.get("quality", "auto")
        provider = s.get("provider", "Unknown")
        m3u8_content += f"#EXTINF:-1,{title} [{provider}] [{quality}]\n{s['url']}\n"
    return HTMLResponse(content=m3u8_content, media_type="application/x-mpegURL", headers={"Content-Disposition": f'attachment; filename="{imdb_id}.m3u8"'})


@app.get("/api/v1/stream/tv/{imdb_id}/{season}/{episode}")
async def stream_tv(imdb_id: str, season: int, episode: int, provider: Optional[str] = None, quality: Optional[str] = None):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_tv_streams(imdb_id, season, episode)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    if provider:
        streams = [s for s in streams if s.get("provider", "").lower() == provider.lower()]
    if quality:
        streams = [s for s in streams if quality.lower() in s.get("quality", "").lower()] or streams
    meta = await get_tmdb_tv_meta(imdb_id)
    return {"status": "ok", "imdb_id": imdb_id, "title": meta.get("title"), "season": season, "episode": episode, "streams": streams, "total": len(streams)}


@app.get("/api/v1/stream/tv/{imdb_id}/{season}/{episode}/best")
async def stream_tv_best(imdb_id: str, season: int, episode: int):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_tv_streams(imdb_id, season, episode)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    return {"status": "ok", "stream": streams[0]}


@app.get("/api/v1/stream/anime/{anilist_id}/{episode}")
async def stream_anime(anilist_id: int, episode: int, sub_type: Optional[str] = None):
    streams = await resolve_anime_streams(anilist_id, episode, sub_type)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    meta = await get_anilist_meta(anilist_id)
    return {"status": "ok", "anilist_id": anilist_id, "title": meta.get("title_english") or meta.get("title_romaji"), "episode": episode, "streams": streams, "total": len(streams)}


@app.get("/api/v1/stream/anime/{anilist_id}/{episode}/sub")
async def stream_anime_sub(anilist_id: int, episode: int):
    return await stream_anime(anilist_id, episode, sub_type="sub")


@app.get("/api/v1/stream/anime/{anilist_id}/{episode}/dub")
async def stream_anime_dub(anilist_id: int, episode: int):
    return await stream_anime(anilist_id, episode, sub_type="dub")


@app.post("/api/v1/stream/resolve")
async def stream_resolve(request: Request):
    body = await request.json()
    url = body.get("url")
    referer = body.get("referer", "")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    streams = await extract_stream_from_host(url, referer)
    return {"status": "ok", "url": url, "streams": streams, "total": len(streams)}


@app.get("/api/v1/sources/movie/{imdb_id}")
async def sources_movie(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_movie_streams(imdb_id)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        providers.setdefault(p, []).append(s)
    return {"status": "ok", "imdb_id": imdb_id, "providers": providers, "total": len(streams)}


@app.get("/api/v1/sources/tv/{imdb_id}/{season}/{episode}")
async def sources_tv(imdb_id: str, season: int, episode: int):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    streams = await resolve_tv_streams(imdb_id, season, episode)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        providers.setdefault(p, []).append(s)
    return {"status": "ok", "imdb_id": imdb_id, "season": season, "episode": episode, "providers": providers, "total": len(streams)}


@app.get("/api/v1/sources/anime/{anilist_id}/{episode}")
async def sources_anime(anilist_id: int, episode: int):
    streams = await resolve_anime_streams(anilist_id, episode)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        providers.setdefault(p, []).append(s)
    return {"status": "ok", "anilist_id": anilist_id, "episode": episode, "providers": providers, "total": len(streams)}


@app.get("/api/v1/search")
async def search(q: str = Query(..., min_length=1), type: str = Query("movie"), page: int = Query(1, ge=1)):
    cache_key = f"search_{hashlib.md5(q.encode()).hexdigest()}_{type}_{page}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "query": q, "type": type, "page": page, "results": cached, "total": len(cached)}
    if type == "movie":
        results = await tmdb_search_movies(q, page)
    elif type == "tv":
        results = await tmdb_search_tv(q, page)
    elif type == "anime":
        results = await search_anilist(q, page)
    elif type == "all":
        r1, r2, r3 = await asyncio.gather(tmdb_search_movies(q, page), tmdb_search_tv(q, page), search_anilist(q, page))
        results = r1[:5] + r2[:5] + r3[:5]
    else:
        raise HTTPException(status_code=400, detail="type must be: movie, tv, anime, all")
    cache_set(_search_cache, cache_key, results, CACHE_TTL_SEARCH)
    return {"status": "ok", "query": q, "type": type, "page": page, "results": results, "total": len(results)}


@app.get("/api/v1/trending")
async def trending(type: str = Query("all"), window: str = Query("week")):
    cache_key = f"trending_{type}_{window}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    data = await get_anilist_trending() if type == "anime" else await tmdb_trending(type, window)
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "type": type, "window": window, "data": data, "total": len(data)}


@app.get("/api/v1/popular")
async def popular(type: str = Query("movie")):
    cache_key = f"popular_{type}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    if type == "anime":
        data = await get_anilist_popular()
    elif type == "movie":
        raw = await tmdb_get("/movie/popular")
        data = [{"tmdb_id": m["id"], "title": m.get("title"), "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "type": "movie"} for m in raw.get("results", [])[:20]]
    elif type == "tv":
        raw = await tmdb_get("/tv/popular")
        data = [{"tmdb_id": m["id"], "title": m.get("name"), "year": m.get("first_air_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "type": "tv"} for m in raw.get("results", [])[:20]]
    else:
        data = []
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "type": type, "data": data, "total": len(data)}


@app.get("/api/v1/toprated/{type}")
async def toprated(type: str):
    cache_key = f"toprated_{type}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    if type == "movie":
        raw = await tmdb_get("/movie/top_rated")
        data = [{"tmdb_id": m["id"], "title": m.get("title"), "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in raw.get("results", [])[:20]]
    elif type == "tv":
        raw = await tmdb_get("/tv/top_rated")
        data = [{"tmdb_id": m["id"], "title": m.get("name"), "year": m.get("first_air_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in raw.get("results", [])[:20]]
    else:
        raise HTTPException(status_code=400, detail="type must be: movie, tv")
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "type": type, "data": data}


@app.get("/api/v1/upcoming/{type}")
async def upcoming(type: str):
    cache_key = f"upcoming_{type}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    if type == "movie":
        raw = await tmdb_get("/movie/upcoming")
        data = [{"tmdb_id": m["id"], "title": m.get("title"), "release_date": m.get("release_date"), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in raw.get("results", [])[:20]]
    elif type == "tv":
        raw = await tmdb_get("/tv/on_the_air")
        data = [{"tmdb_id": m["id"], "title": m.get("name"), "next_air_date": m.get("next_episode_to_air", {}).get("air_date") if m.get("next_episode_to_air") else None, "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in raw.get("results", [])[:20]]
    else:
        raise HTTPException(status_code=400, detail="type must be: movie, tv")
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "type": type, "data": data}


@app.get("/api/v1/nowplaying")
async def nowplaying():
    cache_key = "nowplaying"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    raw = await tmdb_get("/movie/now_playing")
    data = [{"tmdb_id": m["id"], "title": m.get("title"), "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None} for m in raw.get("results", [])[:20]]
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "data": data}


@app.get("/api/v1/discover/{media_type}")
async def discover(media_type: str, genre: Optional[str] = None, year: Optional[str] = None, min_rating: Optional[float] = None, sort_by: str = Query("popularity.desc"), language: Optional[str] = None, page: int = Query(1, ge=1)):
    if media_type not in ("movie", "tv"):
        raise HTTPException(status_code=400, detail="media_type must be: movie, tv")
    data = await tmdb_discover(media_type, genre, year, min_rating, sort_by, language, page)
    return {"status": "ok", "media_type": media_type, "data": data, "total": len(data)}


@app.get("/api/v1/discover/anime")
async def discover_anime(season: Optional[str] = None, year: Optional[int] = None, page: int = Query(1, ge=1)):
    valid_seasons = {"WINTER", "SPRING", "SUMMER", "FALL"}
    if season and season.upper() not in valid_seasons:
        raise HTTPException(status_code=400, detail=f"season must be one of: {', '.join(valid_seasons)}")
    import datetime
    current_year = datetime.datetime.now().year
    use_year = year or current_year
    use_season = season or "WINTER"
    data = await get_anilist_season(use_season, use_year, page)
    return {"status": "ok", "season": use_season, "year": use_year, "data": data, "total": len(data)}


@app.get("/api/v1/genres/{media_type}")
async def genres(media_type: str):
    if media_type not in ("movie", "tv"):
        raise HTTPException(status_code=400, detail="media_type must be: movie, tv")
    cache_key = f"genres_{media_type}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    data = await tmdb_get(f"/genre/{media_type}/list")
    genres_list = data.get("genres", [])
    cache_set(_search_cache, cache_key, genres_list, 86400)
    return {"status": "ok", "media_type": media_type, "data": genres_list}


@app.get("/api/v1/convert/imdb/{imdb_id}")
async def convert_imdb(imdb_id: str):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    find = await tmdb_get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
    movies = find.get("movie_results", [])
    tvs = find.get("tv_results", [])
    result = {}
    if movies:
        result = {"type": "movie", "imdb_id": imdb_id, "tmdb_id": movies[0]["id"], "title": movies[0].get("title")}
    elif tvs:
        result = {"type": "tv", "imdb_id": imdb_id, "tmdb_id": tvs[0]["id"], "title": tvs[0].get("name")}
    else:
        raise HTTPException(status_code=404, detail="ID not found in TMDB")
    return {"status": "ok", "data": result}


@app.get("/api/v1/convert/tmdb/{tmdb_id}")
async def convert_tmdb(tmdb_id: int, media_type: str = Query("movie")):
    if media_type not in ("movie", "tv"):
        raise HTTPException(status_code=400, detail="media_type must be: movie, tv")
    ext = await tmdb_get(f"/{media_type}/{tmdb_id}/external_ids")
    if not ext:
        raise HTTPException(status_code=404, detail="TMDB ID not found")
    return {"status": "ok", "tmdb_id": tmdb_id, "imdb_id": ext.get("imdb_id"), "tvdb_id": ext.get("tvdb_id")}


@app.get("/api/v1/subtitles/movie/{imdb_id}")
async def subtitles_movie(imdb_id: str, lang: str = Query("en")):
    results = await search_subtitles(imdb_id, languages=lang)
    return {"status": "ok", "imdb_id": imdb_id, "language": lang, "subtitles": results, "total": len(results)}


@app.get("/api/v1/subtitles/tv/{imdb_id}/{season}/{episode}")
async def subtitles_tv(imdb_id: str, season: int, episode: int, lang: str = Query("en")):
    results = await search_subtitles(imdb_id, season=season, episode=episode, languages=lang)
    return {"status": "ok", "imdb_id": imdb_id, "season": season, "episode": episode, "language": lang, "subtitles": results, "total": len(results)}


@app.get("/api/v1/subtitles/download/{file_id}")
async def subtitle_download(file_id: int):
    url = await get_subtitle_download_url(file_id)
    if not url:
        raise HTTPException(status_code=404, detail="Subtitle download link not available")
    return {"status": "ok", "file_id": file_id, "url": url}


@app.get("/api/v1/subtitles/proxy/{file_id}")
async def subtitle_proxy(file_id: int):
    url = await get_subtitle_download_url(file_id)
    if not url:
        raise HTTPException(status_code=404, detail="Subtitle not available")
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url)
            content = resp.text
        return HTMLResponse(content=content, media_type="text/plain", headers={"Access-Control-Allow-Origin": "*", "Content-Disposition": f"inline; filename=subtitle_{file_id}.srt"})
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch subtitle: {e}")


@app.get("/api/v1/subtitles/vtt/{file_id}")
async def subtitle_vtt(file_id: int):
    url = await get_subtitle_download_url(file_id)
    if not url:
        raise HTTPException(status_code=404, detail="Subtitle not available")
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url)
            srt_content = resp.text
        vtt_content = await convert_srt_to_vtt_content(srt_content)
        return HTMLResponse(content=vtt_content, media_type="text/vtt", headers={"Access-Control-Allow-Origin": "*", "Content-Disposition": f"inline; filename=subtitle_{file_id}.vtt"})
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to convert subtitle: {e}")


@app.get("/api/v1/download/movie/{imdb_id}")
async def download_movie(background_tasks: BackgroundTasks, imdb_id: str, quality: Optional[str] = None, filename: Optional[str] = None):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    if _active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        raise HTTPException(status_code=429, detail=f"Max concurrent downloads ({MAX_CONCURRENT_DOWNLOADS}) reached. Try again later.")
    job_id = generate_job_id()
    _download_jobs[job_id] = {"job_id": job_id, "status": "queued", "type": "movie", "imdb_id": imdb_id, "quality": quality, "created_at": time.time(), "progress_ms": 0}
    background_tasks.add_task(start_download, job_id, imdb_id=imdb_id, quality=quality, filename=filename)
    return {"status": "ok", "job_id": job_id, "message": "Download queued. Poll /api/v1/download/status/{job_id} for progress."}


@app.get("/api/v1/download/tv/{imdb_id}/{season}/{episode}")
async def download_tv(background_tasks: BackgroundTasks, imdb_id: str, season: int, episode: int, quality: Optional[str] = None, filename: Optional[str] = None):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    if _active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        raise HTTPException(status_code=429, detail=f"Max concurrent downloads ({MAX_CONCURRENT_DOWNLOADS}) reached.")
    job_id = generate_job_id()
    _download_jobs[job_id] = {"job_id": job_id, "status": "queued", "type": "tv", "imdb_id": imdb_id, "season": season, "episode": episode, "quality": quality, "created_at": time.time(), "progress_ms": 0}
    background_tasks.add_task(start_download, job_id, imdb_id=imdb_id, season=season, episode=episode, quality=quality, filename=filename)
    return {"status": "ok", "job_id": job_id, "message": "Download queued. Poll /api/v1/download/status/{job_id} for progress."}


@app.get("/api/v1/download/anime/{anilist_id}/{episode}")
async def download_anime(background_tasks: BackgroundTasks, anilist_id: int, episode: int, quality: Optional[str] = None, filename: Optional[str] = None):
    if _active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        raise HTTPException(status_code=429, detail=f"Max concurrent downloads ({MAX_CONCURRENT_DOWNLOADS}) reached.")
    job_id = generate_job_id()
    _download_jobs[job_id] = {"job_id": job_id, "status": "queued", "type": "anime", "anilist_id": anilist_id, "episode": episode, "quality": quality, "created_at": time.time(), "progress_ms": 0}
    background_tasks.add_task(start_download, job_id, anilist_id=anilist_id, episode=episode, quality=quality, filename=filename, is_anime=True)
    return {"status": "ok", "job_id": job_id, "message": "Download queued. Poll /api/v1/download/status/{job_id} for progress."}


@app.post("/api/v1/download/batch")
async def download_batch(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    items = body.get("items", [])
    if not items:
        raise HTTPException(status_code=400, detail="items array is required")
    if len(items) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 items per batch")
    job_ids = []
    for item in items:
        if _active_downloads >= MAX_CONCURRENT_DOWNLOADS:
            break
        job_id = generate_job_id()
        media_type = item.get("type", "movie")
        imdb_id = item.get("imdb_id")
        anilist_id = item.get("anilist_id")
        season = item.get("season")
        episode = item.get("episode")
        quality = item.get("quality")
        filename = item.get("filename")
        _download_jobs[job_id] = {"job_id": job_id, "status": "queued", "type": media_type, "created_at": time.time(), "progress_ms": 0}
        if media_type == "anime" and anilist_id:
            _download_jobs[job_id]["anilist_id"] = anilist_id
            background_tasks.add_task(start_download, job_id, anilist_id=anilist_id, episode=episode or 1, quality=quality, filename=filename, is_anime=True)
        elif imdb_id:
            _download_jobs[job_id]["imdb_id"] = imdb_id
            background_tasks.add_task(start_download, job_id, imdb_id=imdb_id, season=season, episode=episode, quality=quality, filename=filename)
        job_ids.append(job_id)
    return {"status": "ok", "job_ids": job_ids, "queued": len(job_ids), "message": "Poll each job_id at /api/v1/download/status/{job_id}"}


@app.get("/api/v1/download/status/{job_id}")
async def download_status(job_id: str):
    job = _download_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    result = dict(job)
    result.pop("file_path", None)
    result.pop("stream_url", None)
    result.pop("pid", None)
    expires = job.get("expires_at")
    if expires:
        result["expires_in_seconds"] = max(0, round(expires - time.time()))
    return {"status": "ok", "job": result}


@app.get("/api/v1/download/file/{job_id}")
async def download_file(job_id: str):
    job = _download_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "ready":
        raise HTTPException(status_code=400, detail=f"Job is not ready. Current status: {job['status']}")
    file_path = Path(job.get("file_path", ""))
    if not file_path.exists():
        raise HTTPException(status_code=410, detail="File has expired or been deleted")
    filename = job.get("filename", f"{job_id}.mp4")
    return FileResponse(path=str(file_path), media_type="video/mp4", filename=filename, headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.delete("/api/v1/download/cancel/{job_id}")
async def download_cancel(job_id: str):
    job = _download_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    pid = job.get("pid")
    if pid:
        try:
            import signal
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    fp = job.get("file_path")
    if fp:
        try:
            Path(fp).unlink(missing_ok=True)
        except Exception:
            pass
    del _download_jobs[job_id]
    return {"status": "ok", "message": f"Job {job_id} cancelled and removed"}


@app.get("/api/v1/download/list")
async def download_list(status: Optional[str] = None):
    jobs = list(_download_jobs.values())
    if status:
        jobs = [j for j in jobs if j.get("status") == status]
    safe_jobs = []
    for j in jobs:
        safe = dict(j)
        safe.pop("file_path", None)
        safe.pop("stream_url", None)
        safe.pop("pid", None)
        safe_jobs.append(safe)
    safe_jobs.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return {"status": "ok", "jobs": safe_jobs, "total": len(safe_jobs)}


@app.delete("/api/v1/download/cleanup")
async def download_cleanup_endpoint():
    await cleanup_downloads()
    completed = [j for j in _download_jobs.values() if j["status"] in ("ready", "error")]
    for j in completed:
        fp = j.get("file_path")
        if fp:
            try:
                Path(fp).unlink(missing_ok=True)
            except Exception:
                pass
        _download_jobs.pop(j["job_id"], None)
    return {"status": "ok", "message": "Cleanup complete", "removed": len(completed)}


@app.get("/api/v1/providers")
async def get_providers():
    cached = cache_get(_provider_health_cache, "all_providers")
    if cached:
        return {"status": "ok", "providers": cached}
    return {"status": "ok", "providers": {
        "FlixHQ": {"status": "unknown"}, "HDRezka": {"status": "unknown"}, "LookMovie": {"status": "unknown"},
        "HiAnime/Zoro": {"status": "unknown"}, "GogoAnime": {"status": "unknown"}, "9Anime": {"status": "unknown"},
        "KissAsian": {"status": "unknown"}, "SuperEmbed": {"status": "unknown"}, "AutoEmbed": {"status": "unknown"},
        "2Embed": {"status": "unknown"}, "VidSrcPro": {"status": "unknown"}, "SmashyStream": {"status": "unknown"},
    }, "note": "Call /api/v1/system/providers/health for live data"}


@app.get("/api/v1/system/stats")
async def system_stats():
    return {"status": "ok", "data": get_system_stats()}


@app.get("/api/v1/system/stats/requests")
async def system_stats_requests():
    now = time.time()
    rpm_window = [t for t in _request_stats["rpm_window"] if t > now - 60]
    return {
        "status": "ok",
        "data": {
            "total": _request_stats["total"],
            "today": _request_stats["today"],
            "errors_4xx": _request_stats["errors_4xx"],
            "errors_5xx": _request_stats["errors_5xx"],
            "current_rpm": len(rpm_window),
            "peak_rpm": _request_stats["peak_rpm"],
            "top_endpoints": sorted(_request_stats["by_endpoint"].items(), key=lambda x: x[1], reverse=True)[:20],
            "top_ips": sorted(_request_stats["by_ip"].items(), key=lambda x: x[1], reverse=True)[:20],
            "avg_response_times": {k: round(v["avg_ms"], 1) for k, v in _request_stats["response_times"].items()},
            "recent_errors": _request_stats["recent_errors"][-20:],
        }
    }


@app.get("/api/v1/system/cloudflare")
async def system_cloudflare(period: str = Query("24h")):
    data = await get_cloudflare_analytics(period)
    return {"status": "ok", "data": data}


@app.get("/api/v1/system/cloudflare/firewall")
async def system_cloudflare_firewall(limit: int = Query(25, ge=1, le=100)):
    data = await get_cloudflare_firewall(limit)
    return {"status": "ok", "data": data}


@app.get("/api/v1/system/providers/health")
async def system_providers_health():
    results = await check_provider_health()
    cache_set(_provider_health_cache, "all_providers", results, CACHE_TTL_PROVIDER)
    up = sum(1 for v in results.values() if v.get("status") == "up")
    down = sum(1 for v in results.values() if v.get("status") == "down")
    return {"status": "ok", "summary": {"up": up, "down": down, "total": len(results)}, "providers": results}


@app.get("/api/v1/system/providers/health/{provider_name}")
async def system_provider_health_single(provider_name: str):
    providers_map = {
        "flixhq": "https://flixhq.to", "hdrezka": "https://rezka.ag", "lookmovie": "https://lookmovie2.to",
        "hianime": "https://hianime.to", "gogoanime": "https://gogoanime3.cc", "9anime": "https://9anime.pl",
        "kissasian": "https://kissasian.sh", "tmdb": "https://api.themoviedb.org", "anilist": "https://graphql.anilist.co",
    }
    url = providers_map.get(provider_name.lower())
    if not url:
        raise HTTPException(status_code=404, detail=f"Provider '{provider_name}' not found")
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            resp = await client.head(url, headers=HEADERS_CHROME)
            latency = round((time.time() - start) * 1000)
            return {"status": "ok", "provider": provider_name, "result": {"status": "up" if resp.status_code < 500 else "degraded", "status_code": resp.status_code, "latency_ms": latency}}
    except Exception as e:
        return {"status": "ok", "provider": provider_name, "result": {"status": "down", "latency_ms": None, "error": str(e)[:100]}}


@app.delete("/api/v1/system/cache/clear")
async def cache_clear_all():
    for store in [_stream_cache, _meta_cache, _search_cache, _source_cache]:
        store.clear()
    return {"status": "ok", "message": "All caches cleared"}


@app.delete("/api/v1/system/cache/clear/{cache_type}")
async def cache_clear_type(cache_type: str):
    stores = {"streams": _stream_cache, "meta": _meta_cache, "search": _search_cache, "source": _source_cache}
    store = stores.get(cache_type)
    if not store:
        raise HTTPException(status_code=400, detail=f"cache_type must be one of: {', '.join(stores.keys())}")
    count = len(store)
    store.clear()
    return {"status": "ok", "message": f"Cache '{cache_type}' cleared", "entries_removed": count}


@app.get("/embed/movie/{imdb_id}", response_class=HTMLResponse)
async def embed_movie(request: Request, imdb_id: str, autoplay: bool = False, theme: str = "dark"):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    meta, streams = await asyncio.gather(get_tmdb_movie_meta(imdb_id), resolve_movie_streams(imdb_id))
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for {imdb_id}</p></body></html>", status_code=404)
    title = meta.get("title", imdb_id)
    poster = meta.get("backdrop") or meta.get("poster")
    subs = await search_subtitles(imdb_id)
    sub_url = f"/api/v1/subtitles/vtt/{subs[0]['file_id']}" if subs else None
    return templates.TemplateResponse("player.html", {"request": request, "title": title, "stream_url": streams[0]["url"], "subtitle_url": sub_url, "poster": poster, "streams_json": json.dumps(streams), "subtitles_json": json.dumps(subs), "autoplay": autoplay})


@app.get("/embed/tv/{imdb_id}/{season}/{episode}", response_class=HTMLResponse)
async def embed_tv(request: Request, imdb_id: str, season: int, episode: int, autoplay: bool = False):
    if not re.match(r"^tt\d+$", imdb_id):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    meta, streams = await asyncio.gather(get_tmdb_tv_meta(imdb_id), resolve_tv_streams(imdb_id, season, episode))
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for S{season:02d}E{episode:02d}</p></body></html>", status_code=404)
    show_title = meta.get("title", imdb_id)
    title = f"{show_title} — S{season:02d}E{episode:02d}"
    poster = meta.get("backdrop") or meta.get("poster")
    subs = await search_subtitles(imdb_id, season=season, episode=episode)
    sub_url = f"/api/v1/subtitles/vtt/{subs[0]['file_id']}" if subs else None
    return templates.TemplateResponse("player.html", {"request": request, "title": title, "stream_url": streams[0]["url"], "subtitle_url": sub_url, "poster": poster, "streams_json": json.dumps(streams), "subtitles_json": json.dumps(subs), "autoplay": autoplay})


@app.get("/embed/anime/{anilist_id}/{episode}", response_class=HTMLResponse)
async def embed_anime(request: Request, anilist_id: int, episode: int, autoplay: bool = False, sub_type: Optional[str] = None):
    meta, streams = await asyncio.gather(get_anilist_meta(anilist_id), resolve_anime_streams(anilist_id, episode, sub_type))
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for episode {episode}</p></body></html>", status_code=404)
    anime_title = meta.get("title_english") or meta.get("title_romaji", str(anilist_id))
    title = f"{anime_title} — Episode {episode}"
    poster = meta.get("banner") or meta.get("cover")
    return templates.TemplateResponse("player.html", {"request": request, "title": title, "stream_url": streams[0]["url"], "subtitle_url": None, "poster": poster, "streams_json": json.dumps(streams), "subtitles_json": json.dumps([]), "autoplay": autoplay})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)

