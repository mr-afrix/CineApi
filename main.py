import os
import re
import json
import time
import base64
import hashlib
import secrets
import asyncio
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from functools import wraps
from collections import defaultdict
from contextlib import asynccontextmanager

import httpx
import psutil
from fastapi import FastAPI, HTTPException, Query, Request, Response, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
from jinja2 import Template

TMDB_API_KEY = "04553f0caacbc229340b6213dc3f7676"
OPENSUBTITLES_API_KEY = "mij33pjc3Cj1RKuLCeMG4jlvMGCGqUci"
MASTER_API_KEY = "cine-v2"

CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "cfut_8IqQOP1i0684KUCTY262RkYXvtIvyr7ZedHhYqMu9779ba30")
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID", "04553f0caacbc229340b6213dc3f7676")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "157880c36aef91c51ecf7d54de5cc065")

REQUEST_TIMEOUT = 15.0
SCRAPER_TIMEOUT = 25.0
MAX_CONCURRENT_SCRAPERS = 20
CACHE_TTL = 3600
MAX_CACHE_SIZE = 10000
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280"

class Stats:
    def __init__(self):
        self.total_requests = 0
        self.requests_per_endpoint = defaultdict(int)
        self.requests_per_hour = defaultdict(int)
        self.scraper_success = defaultdict(int)
        self.scraper_failures = defaultdict(int)
        self.cache_hits = 0
        self.cache_misses = 0
        self.start_time = datetime.now()
        self.response_times = []
        self.active_downloads = 0
        self.completed_downloads = 0
        self.total_bytes_downloaded = 0

stats = Stats()

class Cache:
    def __init__(self, max_size: int = MAX_CACHE_SIZE, ttl: int = CACHE_TTL):
        self.data: Dict[str, Dict] = {}
        self.max_size = max_size
        self.ttl = ttl
        self.access_order: List[str] = []

    def get(self, key: str) -> Optional[Any]:
        if key in self.data:
            entry = self.data[key]
            if time.time() - entry["timestamp"] < self.ttl:
                stats.cache_hits += 1
                if key in self.access_order:
                    self.access_order.remove(key)
                self.access_order.append(key)
                return entry["value"]
            else:
                del self.data[key]
                if key in self.access_order:
                    self.access_order.remove(key)
        stats.cache_misses += 1
        return None

    def set(self, key: str, value: Any):
        if len(self.data) >= self.max_size:
            if self.access_order:
                oldest = self.access_order.pop(0)
                if oldest in self.data:
                    del self.data[oldest]
        self.data[key] = {"value": value, "timestamp": time.time()}
        if key in self.access_order:
            self.access_order.remove(key)
        self.access_order.append(key)

    def clear(self):
        self.data.clear()
        self.access_order.clear()

    def cleanup(self):
        now = time.time()
        expired = [k for k, v in self.data.items() if now - v["timestamp"] >= self.ttl]
        for key in expired:
            del self.data[key]
            if key in self.access_order:
                self.access_order.remove(key)

cache = Cache()

class RateLimiter:
    def __init__(self, max_requests: int = RATE_LIMIT_REQUESTS, window: int = RATE_LIMIT_WINDOW):
        self.max_requests = max_requests
        self.window = window
        self.requests: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, identifier: str) -> bool:
        now = time.time()
        self.requests[identifier] = [t for t in self.requests[identifier] if now - t < self.window]
        if len(self.requests[identifier]) >= self.max_requests:
            return False
        self.requests[identifier].append(now)
        return True

    def get_remaining(self, identifier: str) -> int:
        now = time.time()
        self.requests[identifier] = [t for t in self.requests[identifier] if now - t < self.window]
        return max(0, self.max_requests - len(self.requests[identifier]))

rate_limiter = RateLimiter()

class DownloadJob:
    def __init__(self, job_id: str, url: str, filename: str, quality: str = "auto"):
        self.job_id = job_id
        self.url = url
        self.filename = filename
        self.quality = quality
        self.status = "pending"
        self.progress = 0.0
        self.total_size = 0
        self.downloaded_size = 0
        self.speed = 0.0
        self.error = None
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None

download_queue: Dict[str, DownloadJob] = {}

class DownloadRequest(BaseModel):
    urls: List[str] = Field(..., min_length=1, max_length=50)
    quality: str = Field(default="auto")

class BatchDownloadRequest(BaseModel):
    items: List[Dict[str, Any]] = Field(..., min_length=1, max_length=100)

PLAYER_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }} - CineAPI Player</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0a0a; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #fff; min-height: 100vh; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .player-wrapper { position: relative; width: 100%; aspect-ratio: 16/9; background: #000; border-radius: 12px; overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,0.5); }
        video { width: 100%; height: 100%; }
        .controls { margin-top: 20px; display: flex; gap: 10px; flex-wrap: wrap; }
        .btn { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500; transition: all 0.2s; }
        .btn-primary { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #fff; }
        .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4); }
        .btn-secondary { background: #1a1a2e; color: #fff; border: 1px solid #333; }
        .btn-secondary:hover { background: #252542; }
        .info { margin-top: 30px; padding: 20px; background: #111; border-radius: 12px; }
        .info h1 { font-size: 24px; margin-bottom: 10px; }
        .info p { color: #888; line-height: 1.6; }
        .sources-list { margin-top: 20px; display: grid; gap: 10px; }
        .source-item { padding: 15px; background: #1a1a2e; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }
        .source-item:hover { background: #252542; }
    </style>
</head>
<body>
    <div class="container">
        <div class="player-wrapper">
            <video id="player" controls playsinline>
                {% if stream_url %}<source src="{{ stream_url }}" type="{{ stream_type }}">{% endif %}
                {% for subtitle in subtitles %}<track kind="subtitles" src="{{ subtitle.url }}" srclang="{{ subtitle.lang }}" label="{{ subtitle.label }}" {% if loop.first %}default{% endif %}>{% endfor %}
            </video>
        </div>
        <div class="controls">
            <button class="btn btn-primary" onclick="toggleFullscreen()">Fullscreen</button>
            <button class="btn btn-secondary" onclick="togglePiP()">Picture-in-Picture</button>
            <button class="btn btn-secondary" onclick="downloadVideo()">Download</button>
            {% if sources|length > 1 %}
            <select class="btn btn-secondary" onchange="changeSource(this.value)">
                {% for source in sources %}<option value="{{ source.url }}" {% if source.url == stream_url %}selected{% endif %}>{{ source.quality }} - {{ source.provider }}</option>{% endfor %}
            </select>
            {% endif %}
        </div>
        <div class="info">
            <h1>{{ title }}</h1>
            <p>{{ description }}</p>
            {% if episode_info %}<p style="margin-top: 10px; color: #667eea;">Season {{ episode_info.season }}, Episode {{ episode_info.episode }}</p>{% endif %}
        </div>
        {% if all_sources %}
        <div class="sources-list">
            <h3 style="margin-bottom: 15px;">Available Sources ({{ all_sources|length }})</h3>
            {% for source in all_sources %}<div class="source-item" onclick="changeSource('{{ source.url }}')"><span>{{ source.provider }}</span><span style="color: #667eea;">{{ source.quality }}</span></div>{% endfor %}
        </div>
        {% endif %}
    </div>
    <script>
        const video = document.getElementById('player');
        function toggleFullscreen() { if (document.fullscreenElement) { document.exitFullscreen(); } else { document.querySelector('.player-wrapper').requestFullscreen(); } }
        function togglePiP() { if (document.pictureInPictureElement) { document.exitPictureInPicture(); } else if (video.requestPictureInPicture) { video.requestPictureInPicture(); } }
        function downloadVideo() { const a = document.createElement('a'); a.href = video.currentSrc; a.download = '{{ title }}.mp4'; a.click(); }
        function changeSource(url) { const currentTime = video.currentTime; const wasPlaying = !video.paused; video.src = url; video.currentTime = currentTime; if (wasPlaying) video.play(); }
    </script>
</body>
</html>"""

DOCS_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CineAPI v2 - Documentation</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%); color: #fff; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; padding: 40px 20px; }
        header { text-align: center; margin-bottom: 60px; }
        h1 { font-size: 48px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 10px; }
        .subtitle { color: #888; font-size: 18px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 40px; }
        .stat-card { background: rgba(255,255,255,0.05); padding: 25px; border-radius: 12px; text-align: center; border: 1px solid rgba(255,255,255,0.1); }
        .stat-value { font-size: 36px; font-weight: 700; color: #667eea; }
        .stat-label { color: #888; margin-top: 5px; }
        .section { margin-bottom: 40px; }
        .section h2 { font-size: 24px; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 1px solid #333; }
        .endpoint { background: rgba(255,255,255,0.03); padding: 20px; border-radius: 8px; margin-bottom: 15px; border-left: 3px solid #667eea; }
        .endpoint-method { display: inline-block; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: 600; margin-right: 10px; }
        .get { background: #10b981; }
        .post { background: #3b82f6; }
        .delete { background: #ef4444; }
        .endpoint-path { font-family: monospace; font-size: 14px; color: #a5b4fc; }
        .endpoint-desc { color: #888; margin-top: 10px; font-size: 14px; }
        .badge { display: inline-block; padding: 2px 8px; background: rgba(102, 126, 234, 0.2); color: #667eea; border-radius: 4px; font-size: 11px; margin-left: 10px; }
        .providers-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 10px; margin-top: 20px; }
        .provider { padding: 10px; background: rgba(255,255,255,0.05); border-radius: 6px; text-align: center; font-size: 14px; }
        footer { text-align: center; padding: 40px; color: #666; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>CineAPI v2</h1>
            <p class="subtitle">The Ultimate Streaming API - Movies, TV Shows, Anime & More</p>
        </header>
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value">{{ total_requests }}</div><div class="stat-label">Total Requests</div></div>
            <div class="stat-card"><div class="stat-value">{{ total_providers }}</div><div class="stat-label">Providers</div></div>
            <div class="stat-card"><div class="stat-value">{{ total_extractors }}</div><div class="stat-label">Extractors</div></div>
            <div class="stat-card"><div class="stat-value">{{ uptime }}</div><div class="stat-label">Uptime</div></div>
        </div>
        <div class="section">
            <h2>Streaming Endpoints</h2>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/stream/movie/{imdb_id}</span><span class="badge">Multi-Source</span><p class="endpoint-desc">Get streaming sources for a movie by IMDB ID</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/stream/tv/{imdb_id}/{season}/{episode}</span><span class="badge">Multi-Source</span><p class="endpoint-desc">Get streaming sources for a TV episode</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/stream/anime/{mal_id}/{episode}</span><span class="badge">Anime</span><p class="endpoint-desc">Get streaming sources for anime by MAL ID</p></div>
        </div>
        <div class="section">
            <h2>Search & Discovery</h2>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/search/multi?query={query}</span><p class="endpoint-desc">Search movies, TV shows, and people</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/trending/{type}/{window}</span><p class="endpoint-desc">Get trending content</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/discover/{type}</span><p class="endpoint-desc">Discover with advanced filters</p></div>
        </div>
        <div class="section">
            <h2>Downloads</h2>
            <div class="endpoint"><span class="endpoint-method post">POST</span><span class="endpoint-path">/api/v1/download/start</span><span class="badge">Queue</span><p class="endpoint-desc">Start a download job</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/download/status/{job_id}</span><p class="endpoint-desc">Get download status</p></div>
            <div class="endpoint"><span class="endpoint-method post">POST</span><span class="endpoint-path">/api/v1/download/batch</span><span class="badge">Batch</span><p class="endpoint-desc">Start multiple downloads</p></div>
        </div>
        <div class="section">
            <h2>System & Monitoring</h2>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/system/stats</span><p class="endpoint-desc">Get system stats (CPU, RAM, Storage, Requests)</p></div>
            <div class="endpoint"><span class="endpoint-method get">GET</span><span class="endpoint-path">/api/v1/cloudflare/analytics</span><span class="badge">Cloudflare</span><p class="endpoint-desc">Get Cloudflare analytics</p></div>
        </div>
        <div class="section">
            <h2>Supported Providers</h2>
            <div class="providers-grid">{% for provider in providers %}<div class="provider">{{ provider }}</div>{% endfor %}</div>
        </div>
        <footer><p>CineAPI v2.0 - Built by SAGE</p></footer>
    </div>
</body>
</html>"""

player_template = Template(PLAYER_TEMPLATE)
docs_template = Template(DOCS_TEMPLATE)

HEADERS_CHROME = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

HEADERS_AJAX = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

class HttpClient:
    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
                follow_redirects=True,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
            )
        return self._client

    async def get(self, url: str, headers: Dict = None, params: Dict = None, timeout: float = None) -> Optional[httpx.Response]:
        try:
            client = await self.get_client()
            h = dict(HEADERS_CHROME)
            if headers:
                h.update(headers)
            return await client.get(url, headers=h, params=params, timeout=timeout or REQUEST_TIMEOUT)
        except Exception:
            return None

    async def post(self, url: str, data: Dict = None, json_data: Dict = None, headers: Dict = None, timeout: float = None) -> Optional[httpx.Response]:
        try:
            client = await self.get_client()
            h = dict(HEADERS_CHROME)
            if headers:
                h.update(headers)
            return await client.post(url, data=data, json=json_data, headers=h, timeout=timeout or REQUEST_TIMEOUT)
        except Exception:
            return None

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

http = HttpClient()

def decode_base64(data: str) -> str:
    try:
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.b64decode(data).decode("utf-8", errors="ignore")
    except Exception:
        return ""

def encode_base64(data: str) -> str:
    return base64.b64encode(data.encode()).decode()

def extract_m3u8_url(text: str) -> Optional[str]:
    patterns = [
        r'source["\']?\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        r'file["\']?\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        r'src["\']?\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        r'["\']([^"\']+\.m3u8[^"\']*)["\']',
        r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            url = match.group(1)
            if url.startswith("//"):
                url = "https:" + url
            return url
    return None

def extract_mp4_url(text: str) -> Optional[str]:
    patterns = [
        r'source["\']?\s*[:=]\s*["\']([^"\']+\.mp4[^"\']*)["\']',
        r'file["\']?\s*[:=]\s*["\']([^"\']+\.mp4[^"\']*)["\']',
        r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            url = match.group(1)
            if url.startswith("//"):
                url = "https:" + url
            return url
    return None

def unpack_js(packed: str) -> str:
    try:
        match = re.search(r"}\('(.+)',(\d+),(\d+),'([^']+)'\.split\('\|'\)", packed, re.DOTALL)
        if not match:
            return packed
        payload, radix, count, symtab = match.groups()
        radix = int(radix)
        symbols = symtab.split("|")
        def decode_base(value: str, base: int) -> int:
            result = 0
            for char in value:
                if char.isdigit():
                    digit = int(char)
                elif char.isalpha():
                    digit = ord(char.lower()) - 87
                    if char.isupper():
                        digit = ord(char) - 29
                else:
                    continue
                result = result * base + digit
            return result
        def replacer(match):
            word = match.group(0)
            index = decode_base(word, radix)
            if index < len(symbols) and symbols[index]:
                return symbols[index]
            return word
        return re.sub(r'\b\w+\b', replacer, payload)
    except Exception:
        return packed

def clean_title(title: str) -> str:
    if not title:
        return ""
    title = re.sub(r'[^\w\s-]', '', title.lower())
    return re.sub(r'\s+', '-', title.strip())

async def safe_request(coro, default=None):
    try:
        return await asyncio.wait_for(coro, timeout=SCRAPER_TIMEOUT)
    except Exception:
        return default

class TMDBClient:
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        cache_key = f"tmdb:{endpoint}:{json.dumps(params or {}, sort_keys=True)}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        p = {"api_key": TMDB_API_KEY}
        if params:
            p.update(params)
        response = await http.get(f"{TMDB_BASE}{endpoint}", params=p)
        if response and response.status_code == 200:
            data = response.json()
            cache.set(cache_key, data)
            return data
        return None

    async def search_multi(self, query: str, page: int = 1) -> Dict:
        return await self._request("/search/multi", {"query": query, "page": page}) or {"results": []}

    async def search_movie(self, query: str, page: int = 1, year: int = None) -> Dict:
        params = {"query": query, "page": page}
        if year:
            params["year"] = year
        return await self._request("/search/movie", params) or {"results": []}

    async def search_tv(self, query: str, page: int = 1, year: int = None) -> Dict:
        params = {"query": query, "page": page}
        if year:
            params["first_air_date_year"] = year
        return await self._request("/search/tv", params) or {"results": []}

    async def get_movie(self, movie_id: int) -> Optional[Dict]:
        return await self._request(f"/movie/{movie_id}", {"append_to_response": "credits,videos,images,external_ids,similar,recommendations"})

    async def get_tv(self, tv_id: int) -> Optional[Dict]:
        return await self._request(f"/tv/{tv_id}", {"append_to_response": "credits,videos,images,external_ids,similar,recommendations"})

    async def get_tv_season(self, tv_id: int, season: int) -> Optional[Dict]:
        return await self._request(f"/tv/{tv_id}/season/{season}")

    async def get_tv_episode(self, tv_id: int, season: int, episode: int) -> Optional[Dict]:
        return await self._request(f"/tv/{tv_id}/season/{season}/episode/{episode}")

    async def get_trending(self, media_type: str = "all", time_window: str = "day", page: int = 1) -> Dict:
        return await self._request(f"/trending/{media_type}/{time_window}", {"page": page}) or {"results": []}

    async def get_popular_movies(self, page: int = 1) -> Dict:
        return await self._request("/movie/popular", {"page": page}) or {"results": []}

    async def get_popular_tv(self, page: int = 1) -> Dict:
        return await self._request("/tv/popular", {"page": page}) or {"results": []}

    async def get_top_rated_movies(self, page: int = 1) -> Dict:
        return await self._request("/movie/top_rated", {"page": page}) or {"results": []}

    async def get_top_rated_tv(self, page: int = 1) -> Dict:
        return await self._request("/tv/top_rated", {"page": page}) or {"results": []}

    async def get_upcoming_movies(self, page: int = 1) -> Dict:
        return await self._request("/movie/upcoming", {"page": page}) or {"results": []}

    async def get_now_playing(self, page: int = 1) -> Dict:
        return await self._request("/movie/now_playing", {"page": page}) or {"results": []}

    async def get_on_the_air(self, page: int = 1) -> Dict:
        return await self._request("/tv/on_the_air", {"page": page}) or {"results": []}

    async def get_airing_today(self, page: int = 1) -> Dict:
        return await self._request("/tv/airing_today", {"page": page}) or {"results": []}

    async def discover_movie(self, params: Dict = None) -> Dict:
        return await self._request("/discover/movie", params) or {"results": []}

    async def discover_tv(self, params: Dict = None) -> Dict:
        return await self._request("/discover/tv", params) or {"results": []}

    async def get_genres(self, media_type: str = "movie") -> Dict:
        return await self._request(f"/genre/{media_type}/list") or {"genres": []}

    async def get_person(self, person_id: int) -> Optional[Dict]:
        return await self._request(f"/person/{person_id}", {"append_to_response": "combined_credits,images,external_ids"})

    async def get_collection(self, collection_id: int) -> Optional[Dict]:
        return await self._request(f"/collection/{collection_id}")

    async def get_similar(self, media_type: str, media_id: int, page: int = 1) -> Dict:
        return await self._request(f"/{media_type}/{media_id}/similar", {"page": page}) or {"results": []}

    async def get_recommendations(self, media_type: str, media_id: int, page: int = 1) -> Dict:
        return await self._request(f"/{media_type}/{media_id}/recommendations", {"page": page}) or {"results": []}

    async def get_reviews(self, media_type: str, media_id: int, page: int = 1) -> Dict:
        return await self._request(f"/{media_type}/{media_id}/reviews", {"page": page}) or {"results": []}

    async def find_by_external_id(self, external_id: str, source: str = "imdb_id") -> Optional[Dict]:
        return await self._request(f"/find/{external_id}", {"external_source": source})

    async def get_random_movie(self) -> Optional[Dict]:
        import random
        page = random.randint(1, 500)
        result = await self._request("/discover/movie", {"page": page, "sort_by": "popularity.desc"})
        if result and result.get("results"):
            return random.choice(result["results"])
        return None

    async def get_random_tv(self) -> Optional[Dict]:
        import random
        page = random.randint(1, 200)
        result = await self._request("/discover/tv", {"page": page, "sort_by": "popularity.desc"})
        if result and result.get("results"):
            return random.choice(result["results"])
        return None

tmdb = TMDBClient()

class SubtitleClient:
    BASE_URL = "https://api.opensubtitles.com/api/v1"

    async def search(self, imdb_id: str = None, tmdb_id: int = None, query: str = None, season: int = None, episode: int = None, languages: str = "en") -> List[Dict]:
        cache_key = f"subs:{imdb_id}:{tmdb_id}:{query}:{season}:{episode}:{languages}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        headers = {"Api-Key": OPENSUBTITLES_API_KEY, "Content-Type": "application/json", "User-Agent": "CineAPI v2.0"}
        params = {"languages": languages}
        if imdb_id:
            params["imdb_id"] = imdb_id.replace("tt", "")
        if tmdb_id:
            params["tmdb_id"] = tmdb_id
        if query:
            params["query"] = query
        if season:
            params["season_number"] = season
        if episode:
            params["episode_number"] = episode
        response = await http.get(f"{self.BASE_URL}/subtitles", headers=headers, params=params)
        if response and response.status_code == 200:
            data = response.json()
            subtitles = []
            for item in data.get("data", [])[:20]:
                attrs = item.get("attributes", {})
                files = attrs.get("files", [])
                if files:
                    subtitles.append({"id": item.get("id"), "language": attrs.get("language"), "release": attrs.get("release"), "download_count": attrs.get("download_count"), "file_id": files[0].get("file_id"), "file_name": files[0].get("file_name")})
            cache.set(cache_key, subtitles)
            return subtitles
        return []

    async def get_download_link(self, file_id: int) -> Optional[str]:
        headers = {"Api-Key": OPENSUBTITLES_API_KEY, "Content-Type": "application/json", "User-Agent": "CineAPI v2.0"}
        response = await http.post(f"{self.BASE_URL}/download", json_data={"file_id": file_id}, headers=headers)
        if response and response.status_code == 200:
            return response.json().get("link")
        return None

subtitles_client = SubtitleClient()

class CloudflareClient:
    BASE_URL = "https://api.cloudflare.com/client/v4"

    async def _request(self, endpoint: str) -> Optional[Dict]:
        if not CLOUDFLARE_API_TOKEN:
            return None
        headers = {"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}", "Content-Type": "application/json"}
        response = await http.get(f"{self.BASE_URL}{endpoint}", headers=headers)
        if response and response.status_code == 200:
            return response.json()
        return None

    async def get_zone_analytics(self, since: str = "-1440") -> Optional[Dict]:
        if not CLOUDFLARE_ZONE_ID:
            return None
        return await self._request(f"/zones/{CLOUDFLARE_ZONE_ID}/analytics/dashboard?since={since}&continuous=true")

    async def get_firewall_events(self, limit: int = 50) -> Optional[Dict]:
        if not CLOUDFLARE_ZONE_ID:
            return None
        return await self._request(f"/zones/{CLOUDFLARE_ZONE_ID}/security/events?per_page={limit}")

    async def get_bandwidth(self) -> Optional[Dict]:
        result = await self.get_zone_analytics()
        if result and result.get("success"):
            totals = result.get("result", {}).get("totals", {})
            bandwidth = totals.get("bandwidth", {})
            return {"total": bandwidth.get("all", 0), "cached": bandwidth.get("cached", 0), "uncached": bandwidth.get("uncached", 0)}
        return None

    async def get_threats(self) -> Optional[Dict]:
        result = await self.get_zone_analytics()
        if result and result.get("success"):
            totals = result.get("result", {}).get("totals", {})
            return {"threats": totals.get("threats", {}), "pageviews": totals.get("pageviews", {}), "uniques": totals.get("uniques", {})}
        return None

    async def get_top_countries(self) -> Optional[List]:
        result = await self.get_zone_analytics()
        if result and result.get("success"):
            return result.get("result", {}).get("totals", {}).get("requests", {}).get("country", [])
        return None

cloudflare = CloudflareClient()

class VidSrcExtractor:
    async def extract_movie(self, imdb_id: str, tmdb_id: int = None) -> List[Dict]:
        sources = []
        tasks = [self._vidsrc_to(imdb_id, "movie"), self._vidsrc_me(imdb_id, "movie"), self._vidsrc_xyz(imdb_id, "movie"), self._vidsrc_cc(imdb_id), self._vidsrc_icu(imdb_id, "movie"), self._vidsrc_pro(imdb_id, "movie"), self._vidsrc_nl(tmdb_id, "movie") if tmdb_id else asyncio.sleep(0)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                sources.extend(result)
        return sources

    async def extract_tv(self, imdb_id: str, season: int, episode: int, tmdb_id: int = None) -> List[Dict]:
        sources = []
        tasks = [self._vidsrc_to(imdb_id, "tv", season, episode), self._vidsrc_me(imdb_id, "tv", season, episode), self._vidsrc_xyz(imdb_id, "tv", season, episode), self._vidsrc_cc_tv(imdb_id, season, episode), self._vidsrc_icu(imdb_id, "tv", season, episode), self._vidsrc_pro(imdb_id, "tv", season, episode), self._vidsrc_nl(tmdb_id, "tv", season, episode) if tmdb_id else asyncio.sleep(0)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                sources.extend(result)
        return sources

    async def _vidsrc_to(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://vidsrc.to/embed/movie/{imdb_id}" if media_type == "movie" else f"https://vidsrc.to/embed/tv/{imdb_id}/{season}/{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            html = response.text
            sources = []
            source_matches = re.findall(r'data-id="([^"]+)"', html)
            for source_id in source_matches[:3]:
                ajax_url = f"https://vidsrc.to/ajax/embed/episode/{source_id}/sources"
                ajax_resp = await http.get(ajax_url, headers=HEADERS_AJAX)
                if ajax_resp and ajax_resp.status_code == 200:
                    try:
                        ajax_data = ajax_resp.json()
                        for src in ajax_data.get("result", []):
                            src_id = src.get("id", "")
                            src_url = f"https://vidsrc.to/ajax/embed/source/{src_id}"
                            src_resp = await http.get(src_url, headers=HEADERS_AJAX)
                            if src_resp and src_resp.status_code == 200:
                                src_data = src_resp.json()
                                encrypted_url = src_data.get("result", {}).get("url", "")
                                if encrypted_url:
                                    decoded = self._decrypt_url(encrypted_url)
                                    if decoded and ("m3u8" in decoded or "mp4" in decoded):
                                        sources.append({"url": decoded, "quality": "Auto", "provider": "VidSrc.to", "type": "m3u8" if "m3u8" in decoded else "mp4"})
                    except Exception:
                        continue
            stats.scraper_success["vidsrc_to"] += 1
            return sources
        except Exception:
            stats.scraper_failures["vidsrc_to"] += 1
            return []

    def _decrypt_url(self, encrypted: str) -> str:
        try:
            decoded = decode_base64(encrypted)
            key = "8z5Ag5wgagfsOuhz"
            result = ""
            for i, char in enumerate(decoded):
                result += chr(ord(char) ^ ord(key[i % len(key)]))
            if result.startswith("//"):
                result = "https:" + result
            return result
        except Exception:
            return ""

    async def _vidsrc_me(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://vidsrc.me/embed/movie?imdb={imdb_id}" if media_type == "movie" else f"https://vidsrc.me/embed/tv?imdb={imdb_id}&season={season}&episode={episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_me"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.me", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_me"] += 1
            return []

    async def _vidsrc_xyz(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://vidsrc.xyz/embed/movie/{imdb_id}" if media_type == "movie" else f"https://vidsrc.xyz/embed/tv/{imdb_id}/{season}-{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_xyz"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.xyz", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_xyz"] += 1
            return []

    async def _vidsrc_cc(self, imdb_id: str) -> List[Dict]:
        try:
            response = await http.get(f"https://vidsrc.cc/v2/embed/movie/{imdb_id}")
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_cc"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.cc", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_cc"] += 1
            return []

    async def _vidsrc_cc_tv(self, imdb_id: str, season: int, episode: int) -> List[Dict]:
        try:
            response = await http.get(f"https://vidsrc.cc/v2/embed/tv/{imdb_id}/{season}/{episode}")
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_cc"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.cc", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_cc"] += 1
            return []

    async def _vidsrc_icu(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://vidsrc.icu/embed/movie/{imdb_id}" if media_type == "movie" else f"https://vidsrc.icu/embed/tv/{imdb_id}/{season}/{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_icu"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.icu", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_icu"] += 1
            return []

    async def _vidsrc_pro(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://vidsrc.pro/embed/movie/{imdb_id}" if media_type == "movie" else f"https://vidsrc.pro/embed/tv/{imdb_id}/{season}/{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_pro"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.pro", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_pro"] += 1
            return []

    async def _vidsrc_nl(self, tmdb_id: int, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            if not tmdb_id:
                return []
            url = f"https://vidsrc.nl/embed/movie/{tmdb_id}" if media_type == "movie" else f"https://vidsrc.nl/embed/tv/{tmdb_id}/{season}/{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["vidsrc_nl"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "VidSrc.nl", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["vidsrc_nl"] += 1
            return []

vidsrc = VidSrcExtractor()

class SuperEmbedExtractor:
    async def extract_movie(self, imdb_id: str, tmdb_id: int = None) -> List[Dict]:
        sources = []
        tasks = [self._multiembed(imdb_id, "movie"), self._moviesapi(imdb_id, "movie"), self._twoembed(imdb_id, "movie"), self._autoembed(imdb_id, "movie"), self._embedsu(tmdb_id, "movie") if tmdb_id else asyncio.sleep(0)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                sources.extend(result)
        return sources

    async def extract_tv(self, imdb_id: str, season: int, episode: int, tmdb_id: int = None) -> List[Dict]:
        sources = []
        tasks = [self._multiembed(imdb_id, "tv", season, episode), self._moviesapi(imdb_id, "tv", season, episode), self._twoembed(imdb_id, "tv", season, episode), self._autoembed(imdb_id, "tv", season, episode), self._embedsu(tmdb_id, "tv", season, episode) if tmdb_id else asyncio.sleep(0)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                sources.extend(result)
        return sources

    async def _multiembed(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://multiembed.mov/?video_id={imdb_id}&tmdb=0" if media_type == "movie" else f"https://multiembed.mov/?video_id={imdb_id}&tmdb=0&s={season}&e={episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["multiembed"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "MultiEmbed", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["multiembed"] += 1
            return []

    async def _moviesapi(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://moviesapi.club/movie/{imdb_id}" if media_type == "movie" else f"https://moviesapi.club/tv/{imdb_id}-{season}-{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["moviesapi"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "MoviesAPI", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["moviesapi"] += 1
            return []

    async def _twoembed(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://2embed.cc/embed/{imdb_id}" if media_type == "movie" else f"https://2embed.cc/embedtv/{imdb_id}&s={season}&e={episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["twoembed"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "2Embed", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["twoembed"] += 1
            return []

    async def _autoembed(self, imdb_id: str, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            url = f"https://autoembed.co/movie/imdb/{imdb_id}" if media_type == "movie" else f"https://autoembed.co/tv/imdb/{imdb_id}-{season}-{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["autoembed"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "AutoEmbed", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["autoembed"] += 1
            return []

    async def _embedsu(self, tmdb_id: int, media_type: str, season: int = None, episode: int = None) -> List[Dict]:
        try:
            if not tmdb_id:
                return []
            url = f"https://embed.su/embed/movie/{tmdb_id}" if media_type == "movie" else f"https://embed.su/embed/tv/{tmdb_id}/{season}/{episode}"
            response = await http.get(url)
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["embedsu"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "Embed.su", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["embedsu"] += 1
            return []

superembed = SuperEmbedExtractor()

class FlixHQExtractor:
    BASE_URL = "https://flixhq.to"

    async def search(self, query: str) -> List[Dict]:
        try:
            response = await http.get(f"{self.BASE_URL}/search/{urllib.parse.quote(query)}")
            if not response or response.status_code != 200:
                return []
            results = []
            items = re.findall(r'<a[^>]*href="(/(?:movie|tv)/[^"]+)"[^>]*title="([^"]+)"', response.text)
            for link, title in items[:10]:
                results.append({"title": title, "url": f"{self.BASE_URL}{link}"})
            return results
        except Exception:
            return []

    async def extract_movie(self, imdb_id: str) -> List[Dict]:
        try:
            find_result = await tmdb.find_by_external_id(imdb_id)
            if not find_result or not find_result.get("movie_results"):
                return []
            movie = find_result["movie_results"][0]
            title = movie.get("title", "")
            year = movie.get("release_date", "")[:4]
            results = await self.search(f"{title} {year}")
            if not results:
                return []
            movie_url = results[0]["url"]
            response = await http.get(movie_url)
            if not response or response.status_code != 200:
                return []
            movie_id_match = re.search(r'data-id="([^"]+)"', response.text)
            if not movie_id_match:
                return []
            servers_resp = await http.get(f"{self.BASE_URL}/ajax/movie/episodes/{movie_id_match.group(1)}", headers=HEADERS_AJAX)
            if not servers_resp or servers_resp.status_code != 200:
                return []
            server_ids = re.findall(r'data-id="([^"]+)"', servers_resp.text)
            sources = []
            for server_id in server_ids[:3]:
                source_resp = await http.get(f"{self.BASE_URL}/ajax/sources/{server_id}", headers=HEADERS_AJAX)
                if source_resp and source_resp.status_code == 200:
                    try:
                        source_data = source_resp.json()
                        link = source_data.get("link", "")
                        if link:
                            iframe_resp = await http.get(link)
                            if iframe_resp:
                                m3u8_url = extract_m3u8_url(iframe_resp.text)
                                if m3u8_url:
                                    sources.append({"url": m3u8_url, "quality": "Auto", "provider": "FlixHQ", "type": "m3u8"})
                    except Exception:
                        continue
            stats.scraper_success["flixhq"] += 1
            return sources
        except Exception:
            stats.scraper_failures["flixhq"] += 1
            return []

    async def extract_tv(self, imdb_id: str, season: int, episode: int) -> List[Dict]:
        try:
            find_result = await tmdb.find_by_external_id(imdb_id)
            if not find_result or not find_result.get("tv_results"):
                return []
            show = find_result["tv_results"][0]
            title = show.get("name", "")
            results = await self.search(title)
            tv_url = None
            for result in results:
                if "/tv/" in result["url"]:
                    tv_url = result["url"]
                    break
            if not tv_url:
                return []
            response = await http.get(tv_url)
            if not response or response.status_code != 200:
                return []
            tv_id_match = re.search(r'data-id="([^"]+)"', response.text)
            if not tv_id_match:
                return []
            seasons_resp = await http.get(f"{self.BASE_URL}/ajax/tv/seasons/{tv_id_match.group(1)}", headers=HEADERS_AJAX)
            if not seasons_resp or seasons_resp.status_code != 200:
                return []
            season_ids = re.findall(r'data-id="([^"]+)"', seasons_resp.text)
            if season > len(season_ids):
                return []
            episodes_resp = await http.get(f"{self.BASE_URL}/ajax/tv/episodes/{season_ids[season - 1]}", headers=HEADERS_AJAX)
            if not episodes_resp or episodes_resp.status_code != 200:
                return []
            episode_ids = re.findall(r'data-id="([^"]+)"', episodes_resp.text)
            if episode > len(episode_ids):
                return []
            servers_resp = await http.get(f"{self.BASE_URL}/ajax/tv/servers/{episode_ids[episode - 1]}", headers=HEADERS_AJAX)
            if not servers_resp or servers_resp.status_code != 200:
                return []
            server_ids = re.findall(r'data-id="([^"]+)"', servers_resp.text)
            sources = []
            for server_id in server_ids[:3]:
                source_resp = await http.get(f"{self.BASE_URL}/ajax/sources/{server_id}", headers=HEADERS_AJAX)
                if source_resp and source_resp.status_code == 200:
                    try:
                        source_data = source_resp.json()
                        link = source_data.get("link", "")
                        if link:
                            iframe_resp = await http.get(link)
                            if iframe_resp:
                                m3u8_url = extract_m3u8_url(iframe_resp.text)
                                if m3u8_url:
                                    sources.append({"url": m3u8_url, "quality": "Auto", "provider": "FlixHQ", "type": "m3u8"})
                    except Exception:
                        continue
            stats.scraper_success["flixhq"] += 1
            return sources
        except Exception:
            stats.scraper_failures["flixhq"] += 1
            return []

flixhq = FlixHQExtractor()

class GoMoviesExtractor:
    async def extract_movie(self, imdb_id: str) -> List[Dict]:
        try:
            find_result = await tmdb.find_by_external_id(imdb_id)
            if not find_result or not find_result.get("movie_results"):
                return []
            movie = find_result["movie_results"][0]
            title = movie.get("title", "")
            year = movie.get("release_date", "")[:4]
            slug = clean_title(f"{title} {year}")
            response = await http.get(f"https://gomovies.sx/movie/{slug}")
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["gomovies"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "GoMovies", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["gomovies"] += 1
            return []

    async def extract_tv(self, imdb_id: str, season: int, episode: int) -> List[Dict]:
        try:
            find_result = await tmdb.find_by_external_id(imdb_id)
            if not find_result or not find_result.get("tv_results"):
                return []
            show = find_result["tv_results"][0]
            slug = clean_title(show.get("name", ""))
            response = await http.get(f"https://gomovies.sx/tv/{slug}/season-{season}/episode-{episode}")
            if not response or response.status_code != 200:
                return []
            m3u8_url = extract_m3u8_url(response.text)
            if m3u8_url:
                stats.scraper_success["gomovies"] += 1
                return [{"url": m3u8_url, "quality": "Auto", "provider": "GoMovies", "type": "m3u8"}]
            return []
        except Exception:
            stats.scraper_failures["gomovies"] += 1
            return []

gomovies = GoMoviesExtractor()

class AniwaveExtractor:
    BASE_URL = "https://aniwave.to"

    async def search(self, query: str) -> List[Dict]:
        try:
            response = await http.get(f"{self.BASE_URL}/filter?keyword={urllib.parse.quote(query)}")
            if not response or response.status_code != 200:
                return []
            results = []
            items = re.findall(r'<a[^>]*href="(/watch/[^"]+)"[^>]*title="([^"]+)"', response.text)
            for link, title in items[:10]:
                results.append({"title": title, "url": f"{self.BASE_URL}{link}"})
            return results
        except Exception:
            return []

    async def extract(self, mal_id: int, episode: int) -> List[Dict]:
        try:
            jikan_resp = await http.get(f"https://api.jikan.moe/v4/anime/{mal_id}")
            if not jikan_resp or jikan_resp.status_code != 200:
                return []
            data = jikan_resp.json()
            title = data.get("data", {}).get("title", "")
            if not title:
                return []
            search_results = await self.search(title)
            if not search_results:
                return []
            anime_url = search_results[0]["url"]
            response = await http.get(f"{anime_url}/ep-{episode}")
            if not response or response.status_code != 200:
                return []
            sources = []
            server_ids = re.findall(r'data-link-id="([^"]+)"', response.text)
            for server_id in server_ids[:3]:
                ajax_resp = await http.get(f"{self.BASE_URL}/ajax/server/{server_id}", headers=HEADERS_AJAX)
                if ajax_resp and ajax_resp.status_code == 200:
                    try:
                        ajax_data = ajax_resp.json()
                        url = ajax_data.get("result", {}).get("url", "")
                        if url:
                            decoded = decode_base64(url)
                            if decoded:
                                iframe_resp = await http.get(decoded)
                                if iframe_resp:
                                    m3u8_url = extract_m3u8_url(iframe_resp.text)
                                    if m3u8_url:
                                        sources.append({"url": m3u8_url, "quality": "Auto", "provider": "Aniwave", "type": "m3u8"})
                    except Exception:
                        continue
            stats.scraper_success["aniwave"] += 1
            return sources
        except Exception:
            stats.scraper_failures["aniwave"] += 1
            return []

aniwave = AniwaveExtractor()

class GogoAnimeExtractor:
    BASE_URL = "https://gogoanimehd.io"

    async def search(self, query: str) -> List[Dict]:
        try:
            response = await http.get(f"{self.BASE_URL}/search.html?keyword={urllib.parse.quote(query)}")
            if not response or response.status_code != 200:
                return []
            results = []
            items = re.findall(r'<a[^>]*href="(/category/[^"]+)"[^>]*title="([^"]+)"', response.text)
            for link, title in items[:10]:
                results.append({"title": title, "url": f"{self.BASE_URL}{link}"})
            return results
        except Exception:
            return []

    async def extract(self, mal_id: int, episode: int) -> List[Dict]:
        try:
            jikan_resp = await http.get(f"https://api.jikan.moe/v4/anime/{mal_id}")
            if not jikan_resp or jikan_resp.status_code != 200:
                return []
            data = jikan_resp.json()
            title = data.get("data", {}).get("title", "")
            if not title:
                return []
            search_results = await self.search(title)
            if not search_results:
                return []
            anime_slug = search_results[0]["url"].split("/category/")[-1]
            response = await http.get(f"{self.BASE_URL}/{anime_slug}-episode-{episode}")
            if not response or response.status_code != 200:
                return []
            sources = []
            servers = re.findall(r'data-video="([^"]+)"', response.text)
            for server_url in servers[:3]:
                if server_url.startswith("//"):
                    server_url = "https:" + server_url
                iframe_resp = await http.get(server_url)
                if iframe_resp:
                    m3u8_url = extract_m3u8_url(iframe_resp.text)
                    if m3u8_url:
                        sources.append({"url": m3u8_url, "quality": "Auto", "provider": "GogoAnime", "type": "m3u8"})
            stats.scraper_success["gogoanime"] += 1
            return sources
        except Exception:
            stats.scraper_failures["gogoanime"] += 1
            return []

gogoanime = GogoAnimeExtractor()

class ZoroExtractor:
    BASE_URL = "https://hianime.to"

    async def search(self, query: str) -> List[Dict]:
        try:
            response = await http.get(f"{self.BASE_URL}/search?keyword={urllib.parse.quote(query)}")
            if not response or response.status_code != 200:
                return []
            results = []
            items = re.findall(r'<a[^>]*href="(/watch/[^"]+)"[^>]*class="[^"]*"', response.text)
            titles = re.findall(r'<h3[^>]*class="film-name"[^>]*><a[^>]*>([^<]+)</a>', response.text)
            for i, link in enumerate(items[:10]):
                results.append({"title": titles[i] if i < len(titles) else "Unknown", "url": f"{self.BASE_URL}{link}"})
            return results
        except Exception:
            return []

    async def extract(self, mal_id: int, episode: int) -> List[Dict]:
        try:
            jikan_resp = await http.get(f"https://api.jikan.moe/v4/anime/{mal_id}")
            if not jikan_resp or jikan_resp.status_code != 200:
                return []
            data = jikan_resp.json()
            title = data.get("data", {}).get("title", "")
            if not title:
                return []
            search_results = await self.search(title)
            if not search_results:
                return []
            anime_id = search_results[0]["url"].split("/watch/")[-1].split("?")[0]
            servers_resp = await http.get(f"{self.BASE_URL}/ajax/v2/episode/servers?episodeId={anime_id}-episode-{episode}", headers=HEADERS_AJAX)
            if not servers_resp or servers_resp.status_code != 200:
                return []
            try:
                servers_html = servers_resp.json().get("html", "")
            except Exception:
                return []
            sources = []
            server_ids = re.findall(r'data-id="([^"]+)"', servers_html)
            for server_id in server_ids[:3]:
                source_resp = await http.get(f"{self.BASE_URL}/ajax/v2/episode/sources?id={server_id}", headers=HEADERS_AJAX)
                if source_resp and source_resp.status_code == 200:
                    try:
                        link = source_resp.json().get("link", "")
                        if link:
                            iframe_resp = await http.get(link)
                            if iframe_resp:
                                m3u8_url = extract_m3u8_url(iframe_resp.text)
                                if m3u8_url:
                                    sources.append({"url": m3u8_url, "quality": "Auto", "provider": "HiAnime", "type": "m3u8"})
                    except Exception:
                        continue
            stats.scraper_success["zoro"] += 1
            return sources
        except Exception:
            stats.scraper_failures["zoro"] += 1
            return []

zoro = ZoroExtractor()

class AnimePaheExtractor:
    BASE_URL = "https://animepahe.ru"

    async def search(self, query: str) -> List[Dict]:
        try:
            response = await http.get(f"{self.BASE_URL}/api?m=search&q={urllib.parse.quote(query)}")
            if not response or response.status_code != 200:
                return []
            data = response.json()
            results = []
            for item in data.get("data", [])[:10]:
                results.append({"title": item.get("title", ""), "session": item.get("session", ""), "episodes": item.get("episodes", 0)})
            return results
        except Exception:
            return []

    async def extract(self, mal_id: int, episode: int) -> List[Dict]:
        try:
            jikan_resp = await http.get(f"https://api.jikan.moe/v4/anime/{mal_id}")
            if not jikan_resp or jikan_resp.status_code != 200:
                return []
            data = jikan_resp.json()
            title = data.get("data", {}).get("title", "")
            if not title:
                return []
            search_results = await self.search(title)
            if not search_results:
                return []
            session = search_results[0].get("session")
            if not session:
                return []
            episodes_resp = await http.get(f"{self.BASE_URL}/api?m=release&id={session}&sort=episode_asc&page=1")
            if not episodes_resp or episodes_resp.status_code != 200:
                return []
            episodes_data = episodes_resp.json()
            episode_session = None
            for ep in episodes_data.get("data", []):
                if ep.get("episode") == episode:
                    episode_session = ep.get("session")
                    break
            if not episode_session:
                return []
            play_resp = await http.get(f"{self.BASE_URL}/play/{session}/{episode_session}")
            if not play_resp or play_resp.status_code != 200:
                return []
            sources = []
            kwik_links = re.findall(r'href="([^"]*kwik[^"]*)"', play_resp.text)
            for kwik_url in kwik_links[:3]:
                if kwik_url.startswith("//"):
                    kwik_url = "https:" + kwik_url
                kwik_resp = await http.get(kwik_url, headers={"Referer": self.BASE_URL})
                if kwik_resp:
                    m3u8_url = extract_m3u8_url(kwik_resp.text)
                    if m3u8_url:
                        sources.append({"url": m3u8_url, "quality": "Auto", "provider": "AnimePahe", "type": "m3u8"})
                        break
            stats.scraper_success["animepahe"] += 1
            return sources
        except Exception:
            stats.scraper_failures["animepahe"] += 1
            return []

animepahe = AnimePaheExtractor()

class HostExtractor:
    async def extract_filemoon(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            packed_match = re.search(r"(eval\(function\(p,a,c,k,e,d\).*?\))\s*</script>", response.text, re.DOTALL)
            if packed_match:
                unpacked = unpack_js(packed_match.group(1))
                return extract_m3u8_url(unpacked)
            return extract_m3u8_url(response.text)
        except Exception:
            return None

    async def extract_streamwish(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            packed_match = re.search(r"(eval\(function\(p,a,c,k,e,d\).*?\))\s*</script>", response.text, re.DOTALL)
            if packed_match:
                unpacked = unpack_js(packed_match.group(1))
                return extract_m3u8_url(unpacked)
            return extract_m3u8_url(response.text)
        except Exception:
            return None

    async def extract_vidhide(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            packed_match = re.search(r"(eval\(function\(p,a,c,k,e,d\).*?\))\s*</script>", response.text, re.DOTALL)
            if packed_match:
                unpacked = unpack_js(packed_match.group(1))
                return extract_m3u8_url(unpacked)
            return extract_m3u8_url(response.text)
        except Exception:
            return None

    async def extract_mixdrop(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            packed_match = re.search(r"(eval\(function\(p,a,c,k,e,d\).*?\))\s*</script>", response.text, re.DOTALL)
            if packed_match:
                unpacked = unpack_js(packed_match.group(1))
                video_match = re.search(r'wurl\s*=\s*"([^"]+)"', unpacked)
                if video_match:
                    video_url = video_match.group(1)
                    return "https:" + video_url if video_url.startswith("//") else video_url
            return None
        except Exception:
            return None

    async def extract_doodstream(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            pass_match = re.search(r"/pass_md5/([^'\"]+)", response.text)
            if not pass_match:
                return None
            pass_resp = await http.get(f"https://dood.re/pass_md5/{pass_match.group(1)}", headers={"Referer": url})
            if not pass_resp or pass_resp.status_code != 200:
                return None
            token = pass_resp.text
            chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            rand_str = "".join(secrets.choice(chars) for _ in range(10))
            return f"{token}{rand_str}?token={pass_match.group(1).split('/')[-1]}&expiry={int(time.time() * 1000)}"
        except Exception:
            return None

    async def extract_streamtape(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            link_match = re.search(r"getElementById\('robotlink'\)\.innerHTML\s*=\s*['\"]([^'\"]+)['\"]", response.text)
            if link_match:
                partial = link_match.group(1)
                token_match = re.search(r"\+ \('([^']+)'\)", response.text)
                if token_match:
                    return f"https://streamtape.com{partial}{token_match.group(1)}"
            return None
        except Exception:
            return None

    async def extract_mp4upload(self, url: str) -> Optional[str]:
        try:
            response = await http.get(url)
            if not response or response.status_code != 200:
                return None
            packed_match = re.search(r"(eval\(function\(p,a,c,k,e,d\).*?\))\s*</script>", response.text, re.DOTALL)
            if packed_match:
                unpacked = unpack_js(packed_match.group(1))
                return extract_mp4_url(unpacked)
            return extract_mp4_url(response.text)
        except Exception:
            return None

    async def auto_extract(self, url: str) -> Optional[str]:
        url_lower = url.lower()
        if "filemoon" in url_lower:
            return await self.extract_filemoon(url)
        elif "streamwish" in url_lower:
            return await self.extract_streamwish(url)
        elif "vidhide" in url_lower:
            return await self.extract_vidhide(url)
        elif "mixdrop" in url_lower:
            return await self.extract_mixdrop(url)
        elif "dood" in url_lower:
            return await self.extract_doodstream(url)
        elif "streamtape" in url_lower:
            return await self.extract_streamtape(url)
        elif "mp4upload" in url_lower:
            return await self.extract_mp4upload(url)
        else:
            response = await http.get(url)
            if response and response.status_code == 200:
                return extract_m3u8_url(response.text) or extract_mp4_url(response.text)
        return None

host_extractor = HostExtractor()

class MasterScraper:
    async def scrape_movie(self, imdb_id: str, tmdb_id: int = None) -> List[Dict]:
        cache_key = f"movie:{imdb_id}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        tasks = [safe_request(vidsrc.extract_movie(imdb_id, tmdb_id), []), safe_request(superembed.extract_movie(imdb_id, tmdb_id), []), safe_request(flixhq.extract_movie(imdb_id), []), safe_request(gomovies.extract_movie(imdb_id), [])]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_sources = []
        for result in results:
            if isinstance(result, list):
                all_sources.extend(result)
        seen_urls = set()
        unique_sources = []
        for source in all_sources:
            url = source.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_sources.append(source)
        if unique_sources:
            cache.set(cache_key, unique_sources)
        return unique_sources

    async def scrape_tv(self, imdb_id: str, season: int, episode: int, tmdb_id: int = None) -> List[Dict]:
        cache_key = f"tv:{imdb_id}:{season}:{episode}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        tasks = [safe_request(vidsrc.extract_tv(imdb_id, season, episode, tmdb_id), []), safe_request(superembed.extract_tv(imdb_id, season, episode, tmdb_id), []), safe_request(flixhq.extract_tv(imdb_id, season, episode), []), safe_request(gomovies.extract_tv(imdb_id, season, episode), [])]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_sources = []
        for result in results:
            if isinstance(result, list):
                all_sources.extend(result)
        seen_urls = set()
        unique_sources = []
        for source in all_sources:
            url = source.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_sources.append(source)
        if unique_sources:
            cache.set(cache_key, unique_sources)
        return unique_sources

    async def scrape_anime(self, mal_id: int, episode: int) -> List[Dict]:
        cache_key = f"anime:{mal_id}:{episode}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        tasks = [safe_request(aniwave.extract(mal_id, episode), []), safe_request(gogoanime.extract(mal_id, episode), []), safe_request(zoro.extract(mal_id, episode), []), safe_request(animepahe.extract(mal_id, episode), [])]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_sources = []
        for result in results:
            if isinstance(result, list):
                all_sources.extend(result)
        seen_urls = set()
        unique_sources = []
        for source in all_sources:
            url = source.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_sources.append(source)
        if unique_sources:
            cache.set(cache_key, unique_sources)
        return unique_sources

scraper = MasterScraper()

async def periodic_cleanup():
    while True:
        await asyncio.sleep(300)
        cache.cleanup()
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        old_hours = [h for h in stats.requests_per_hour.keys() if h < current_hour]
        for h in old_hours[:-24]:
            del stats.requests_per_hour[h]

@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_task = asyncio.create_task(periodic_cleanup())
    yield
    cleanup_task.cancel()
    await http.close()
    cache.clear()

app = FastAPI(title="CineAPI v2", description="The Ultimate Streaming API", version="2.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start_time = time.time()
    stats.total_requests += 1
    stats.requests_per_endpoint[request.url.path] += 1
    stats.requests_per_hour[datetime.now().strftime("%Y-%m-%d-%H")] += 1
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded", "retry_after": RATE_LIMIT_WINDOW})
    response = await call_next(request)
    process_time = time.time() - start_time
    stats.response_times.append(process_time)
    if len(stats.response_times) > 1000:
        stats.response_times = stats.response_times[-1000:]
    response.headers["X-Process-Time"] = str(round(process_time, 4))
    response.headers["X-Rate-Limit-Remaining"] = str(rate_limiter.get_remaining(client_ip))
    return response

@app.get("/")
async def root():
    uptime = datetime.now() - stats.start_time
    uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m"
    providers = ["VidSrc.to", "VidSrc.me", "VidSrc.xyz", "VidSrc.cc", "VidSrc.nl", "VidSrc.icu", "VidSrc.pro", "MultiEmbed", "MoviesAPI", "2Embed", "Embed.su", "AutoEmbed", "FlixHQ", "GoMovies", "Aniwave", "GogoAnime", "HiAnime", "AnimePahe"]
    return HTMLResponse(docs_template.render(total_requests=stats.total_requests, total_providers=len(providers), total_extractors=8, uptime=uptime_str, providers=providers))

@app.get("/api/v1/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(), "version": "2.0.0"}

@app.get("/api/v1/system/stats")
async def system_stats():
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    uptime = datetime.now() - stats.start_time
    avg_response_time = sum(stats.response_times) / len(stats.response_times) if stats.response_times else 0
    scraper_stats = {}
    all_scrapers = set(list(stats.scraper_success.keys()) + list(stats.scraper_failures.keys()))
    for s in all_scrapers:
        success = stats.scraper_success.get(s, 0)
        failures = stats.scraper_failures.get(s, 0)
        total = success + failures
        scraper_stats[s] = {"success": success, "failures": failures, "success_rate": round(success / total * 100, 2) if total > 0 else 0}
    return {"system": {"cpu_percent": cpu_percent, "memory": {"total": memory.total, "available": memory.available, "percent": memory.percent, "used": memory.used}, "disk": {"total": disk.total, "used": disk.used, "free": disk.free, "percent": disk.percent}}, "api": {"total_requests": stats.total_requests, "requests_per_endpoint": dict(stats.requests_per_endpoint), "requests_last_24h": dict(list(stats.requests_per_hour.items())[-24:]), "avg_response_time_ms": round(avg_response_time * 1000, 2), "uptime_seconds": uptime.total_seconds()}, "cache": {"size": len(cache.data), "max_size": cache.max_size, "hits": stats.cache_hits, "misses": stats.cache_misses, "hit_rate": round(stats.cache_hits / (stats.cache_hits + stats.cache_misses) * 100, 2) if (stats.cache_hits + stats.cache_misses) > 0 else 0}, "scrapers": scraper_stats, "downloads": {"active": stats.active_downloads, "completed": stats.completed_downloads, "total_bytes": stats.total_bytes_downloaded}}

@app.get("/api/v1/system/metrics")
async def system_metrics():
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    return {"cpu": cpu_percent, "memory_percent": memory.percent, "total_requests": stats.total_requests, "cache_hits": stats.cache_hits, "cache_misses": stats.cache_misses, "active_downloads": stats.active_downloads}

@app.get("/api/v1/cloudflare/analytics")
async def cloudflare_analytics():
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare not configured", "configured": False}
    analytics = await cloudflare.get_zone_analytics()
    return {"configured": True, "data": analytics.get("result", {}) if analytics else None}

@app.get("/api/v1/cloudflare/threats")
async def cloudflare_threats():
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare not configured", "configured": False}
    return {"configured": True, "data": await cloudflare.get_threats()}

@app.get("/api/v1/cloudflare/bandwidth")
async def cloudflare_bandwidth():
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare not configured", "configured": False}
    return {"configured": True, "data": await cloudflare.get_bandwidth()}

@app.get("/api/v1/cloudflare/countries")
async def cloudflare_countries():
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare not configured", "configured": False}
    return {"configured": True, "data": await cloudflare.get_top_countries()}

@app.get("/api/v1/cloudflare/firewall")
async def cloudflare_firewall():
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {"error": "Cloudflare not configured", "configured": False}
    events = await cloudflare.get_firewall_events()
    return {"configured": True, "data": events.get("result", []) if events else []}

@app.get("/api/v1/search/multi")
async def search_multi(query: str = Query(..., min_length=1, max_length=500), page: int = Query(1, ge=1, le=1000)):
    return await tmdb.search_multi(query, page)

@app.get("/api/v1/search/movie")
async def search_movie(query: str = Query(..., min_length=1), page: int = Query(1, ge=1), year: int = None):
    return await tmdb.search_movie(query, page, year)

@app.get("/api/v1/search/tv")
async def search_tv(query: str = Query(..., min_length=1), page: int = Query(1, ge=1), year: int = None):
    return await tmdb.search_tv(query, page, year)

@app.get("/api/v1/trending/{media_type}/{time_window}")
async def trending(media_type: str = "all", time_window: str = "day", page: int = Query(1, ge=1)):
    if media_type not in ["all", "movie", "tv", "person"]:
        raise HTTPException(400, "Invalid media_type")
    if time_window not in ["day", "week"]:
        raise HTTPException(400, "Invalid time_window")
    return await tmdb.get_trending(media_type, time_window, page)

@app.get("/api/v1/movie/popular")
async def popular_movies(page: int = Query(1, ge=1)):
    return await tmdb.get_popular_movies(page)

@app.get("/api/v1/tv/popular")
async def popular_tv(page: int = Query(1, ge=1)):
    return await tmdb.get_popular_tv(page)

@app.get("/api/v1/movie/top-rated")
async def top_rated_movies(page: int = Query(1, ge=1)):
    return await tmdb.get_top_rated_movies(page)

@app.get("/api/v1/tv/top-rated")
async def top_rated_tv(page: int = Query(1, ge=1)):
    return await tmdb.get_top_rated_tv(page)

@app.get("/api/v1/movie/upcoming")
async def upcoming_movies(page: int = Query(1, ge=1)):
    return await tmdb.get_upcoming_movies(page)

@app.get("/api/v1/movie/now-playing")
async def now_playing(page: int = Query(1, ge=1)):
    return await tmdb.get_now_playing(page)

@app.get("/api/v1/tv/on-the-air")
async def on_the_air(page: int = Query(1, ge=1)):
    return await tmdb.get_on_the_air(page)

@app.get("/api/v1/tv/airing-today")
async def airing_today(page: int = Query(1, ge=1)):
    return await tmdb.get_airing_today(page)

@app.get("/api/v1/discover/movie")
async def discover_movie(page: int = Query(1, ge=1), sort_by: str = "popularity.desc", with_genres: str = None, year: int = None, vote_average_gte: float = None, vote_average_lte: float = None, with_original_language: str = None):
    params = {"page": page, "sort_by": sort_by}
    if with_genres:
        params["with_genres"] = with_genres
    if year:
        params["year"] = year
    if vote_average_gte:
        params["vote_average.gte"] = vote_average_gte
    if vote_average_lte:
        params["vote_average.lte"] = vote_average_lte
    if with_original_language:
        params["with_original_language"] = with_original_language
    return await tmdb.discover_movie(params)

@app.get("/api/v1/discover/tv")
async def discover_tv(page: int = Query(1, ge=1), sort_by: str = "popularity.desc", with_genres: str = None, first_air_date_year: int = None, vote_average_gte: float = None, vote_average_lte: float = None, with_original_language: str = None):
    params = {"page": page, "sort_by": sort_by}
    if with_genres:
        params["with_genres"] = with_genres
    if first_air_date_year:
        params["first_air_date_year"] = first_air_date_year
    if vote_average_gte:
        params["vote_average.gte"] = vote_average_gte
    if vote_average_lte:
        params["vote_average.lte"] = vote_average_lte
    if with_original_language:
        params["with_original_language"] = with_original_language
    return await tmdb.discover_tv(params)

@app.get("/api/v1/genres/movie")
async def movie_genres():
    return await tmdb.get_genres("movie")

@app.get("/api/v1/genres/tv")
async def tv_genres():
    return await tmdb.get_genres("tv")

@app.get("/api/v1/movie/{movie_id}")
async def movie_details(movie_id: int):
    result = await tmdb.get_movie(movie_id)
    if not result:
        raise HTTPException(404, "Movie not found")
    return result

@app.get("/api/v1/tv/{tv_id}")
async def tv_details(tv_id: int):
    result = await tmdb.get_tv(tv_id)
    if not result:
        raise HTTPException(404, "TV show not found")
    return result

@app.get("/api/v1/tv/{tv_id}/season/{season}")
async def tv_season(tv_id: int, season: int):
    result = await tmdb.get_tv_season(tv_id, season)
    if not result:
        raise HTTPException(404, "Season not found")
    return result

@app.get("/api/v1/tv/{tv_id}/season/{season}/episode/{episode}")
async def tv_episode(tv_id: int, season: int, episode: int):
    result = await tmdb.get_tv_episode(tv_id, season, episode)
    if not result:
        raise HTTPException(404, "Episode not found")
    return result

@app.get("/api/v1/person/{person_id}")
async def person_details(person_id: int):
    result = await tmdb.get_person(person_id)
    if not result:
        raise HTTPException(404, "Person not found")
    return result

@app.get("/api/v1/collection/{collection_id}")
async def collection_details(collection_id: int):
    result = await tmdb.get_collection(collection_id)
    if not result:
        raise HTTPException(404, "Collection not found")
    return result

@app.get("/api/v1/{media_type}/{media_id}/similar")
async def similar(media_type: str, media_id: int, page: int = Query(1, ge=1)):
    if media_type not in ["movie", "tv"]:
        raise HTTPException(400, "Invalid media_type")
    return await tmdb.get_similar(media_type, media_id, page)

@app.get("/api/v1/{media_type}/{media_id}/recommendations")
async def recommendations(media_type: str, media_id: int, page: int = Query(1, ge=1)):
    if media_type not in ["movie", "tv"]:
        raise HTTPException(400, "Invalid media_type")
    return await tmdb.get_recommendations(media_type, media_id, page)

@app.get("/api/v1/{media_type}/{media_id}/reviews")
async def reviews(media_type: str, media_id: int, page: int = Query(1, ge=1)):
    if media_type not in ["movie", "tv"]:
        raise HTTPException(400, "Invalid media_type")
    return await tmdb.get_reviews(media_type, media_id, page)

@app.get("/api/v1/find/{external_id}")
async def find_by_external_id(external_id: str, source: str = "imdb_id"):
    result = await tmdb.find_by_external_id(external_id, source)
    if not result:
        raise HTTPException(404, "Not found")
    return result

@app.get("/api/v1/random/movie")
async def random_movie():
    result = await tmdb.get_random_movie()
    if not result:
        raise HTTPException(500, "Failed to get random movie")
    return result

@app.get("/api/v1/random/tv")
async def random_tv():
    result = await tmdb.get_random_tv()
    if not result:
        raise HTTPException(500, "Failed to get random TV show")
    return result

@app.get("/api/v1/stream/movie/{imdb_id}")
async def stream_movie(imdb_id: str, tmdb_id: int = None):
    if not imdb_id.startswith("tt"):
        imdb_id = f"tt{imdb_id}"
    sources = await scraper.scrape_movie(imdb_id, tmdb_id)
    return {"imdb_id": imdb_id, "tmdb_id": tmdb_id, "sources": sources, "count": len(sources)}

@app.get("/api/v1/stream/tv/{imdb_id}/{season}/{episode}")
async def stream_tv(imdb_id: str, season: int, episode: int, tmdb_id: int = None):
    if not imdb_id.startswith("tt"):
        imdb_id = f"tt{imdb_id}"
    sources = await scraper.scrape_tv(imdb_id, season, episode, tmdb_id)
    return {"imdb_id": imdb_id, "tmdb_id": tmdb_id, "season": season, "episode": episode, "sources": sources, "count": len(sources)}

@app.get("/api/v1/stream/anime/{mal_id}/{episode}")
async def stream_anime(mal_id: int, episode: int):
    sources = await scraper.scrape_anime(mal_id, episode)
    return {"mal_id": mal_id, "episode": episode, "sources": sources, "count": len(sources)}

@app.get("/api/v1/player/movie/{imdb_id}")
async def player_movie(imdb_id: str, tmdb_id: int = None):
    if not imdb_id.startswith("tt"):
        imdb_id = f"tt{imdb_id}"
    sources = await scraper.scrape_movie(imdb_id, tmdb_id)
    if not sources:
        return HTMLResponse("<html><body style='background:#0a0a0a;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;'><h1>No sources found</h1></body></html>")
    meta = None
    if tmdb_id:
        meta = await tmdb.get_movie(tmdb_id)
    elif imdb_id:
        find_result = await tmdb.find_by_external_id(imdb_id)
        if find_result and find_result.get("movie_results"):
            meta = find_result["movie_results"][0]
    title = meta.get("title", "Unknown") if meta else "Unknown"
    description = meta.get("overview", "") if meta else ""
    first_source = sources[0]
    stream_url = first_source.get("url", "")
    stream_type = "application/x-mpegURL" if first_source.get("type") == "m3u8" else "video/mp4"
    subs = await subtitles_client.search(imdb_id=imdb_id)
    subtitle_list = []
    for sub in subs[:5]:
        link = await subtitles_client.get_download_link(sub.get("file_id"))
        if link:
            subtitle_list.append({"url": link, "lang": sub.get("language", "en"), "label": sub.get("language", "English")})
    return HTMLResponse(player_template.render(title=title, description=description, stream_url=stream_url, stream_type=stream_type, sources=sources, all_sources=sources, subtitles=subtitle_list, episode_info=None))

@app.get("/api/v1/player/tv/{imdb_id}/{season}/{episode}")
async def player_tv(imdb_id: str, season: int, episode: int, tmdb_id: int = None):
    if not imdb_id.startswith("tt"):
        imdb_id = f"tt{imdb_id}"
    sources = await scraper.scrape_tv(imdb_id, season, episode, tmdb_id)
    if not sources:
        return HTMLResponse("<html><body style='background:#0a0a0a;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;'><h1>No sources found</h1></body></html>")
    meta = None
    if imdb_id:
        find_result = await tmdb.find_by_external_id(imdb_id)
        if find_result and find_result.get("tv_results"):
            meta = find_result["tv_results"][0]
    title = meta.get("name", "Unknown") if meta else "Unknown"
    description = meta.get("overview", "") if meta else ""
    first_source = sources[0]
    stream_url = first_source.get("url", "")
    stream_type = "application/x-mpegURL" if first_source.get("type") == "m3u8" else "video/mp4"
    subs = await subtitles_client.search(imdb_id=imdb_id, season=season, episode=episode)
    subtitle_list = []
    for sub in subs[:5]:
        link = await subtitles_client.get_download_link(sub.get("file_id"))
        if link:
            subtitle_list.append({"url": link, "lang": sub.get("language", "en"), "label": sub.get("language", "English")})
    return HTMLResponse(player_template.render(title=title, description=description, stream_url=stream_url, stream_type=stream_type, sources=sources, all_sources=sources, subtitles=subtitle_list, episode_info={"season": season, "episode": episode}))

@app.get("/api/v1/subtitles/search")
async def search_subtitles(imdb_id: str = None, tmdb_id: int = None, query: str = None, season: int = None, episode: int = None, languages: str = "en"):
    results = await subtitles_client.search(imdb_id, tmdb_id, query, season, episode, languages)
    return {"results": results, "count": len(results)}

@app.get("/api/v1/subtitles/download/{file_id}")
async def download_subtitle(file_id: int):
    link = await subtitles_client.get_download_link(file_id)
    if not link:
        raise HTTPException(404, "Subtitle not found")
    return {"download_url": link}

@app.get("/api/v1/extract/{host}")
async def extract_host(host: str, url: str = Query(...)):
    extractors = {"filemoon": host_extractor.extract_filemoon, "streamwish": host_extractor.extract_streamwish, "vidhide": host_extractor.extract_vidhide, "mixdrop": host_extractor.extract_mixdrop, "doodstream": host_extractor.extract_doodstream, "streamtape": host_extractor.extract_streamtape, "mp4upload": host_extractor.extract_mp4upload}
    if host not in extractors:
        raise HTTPException(400, f"Unknown host: {host}")
    result = await extractors[host](url)
    if not result:
        raise HTTPException(404, "Failed to extract video URL")
    return {"url": result, "host": host}

@app.get("/api/v1/extract/auto")
async def extract_auto(url: str = Query(...)):
    result = await host_extractor.auto_extract(url)
    if not result:
        raise HTTPException(404, "Failed to extract video URL")
    return {"url": result}

@app.post("/api/v1/download/start")
async def start_download(request: DownloadRequest, background_tasks: BackgroundTasks):
    jobs = []
    for url in request.urls:
        job_id = secrets.token_hex(16)
        job = DownloadJob(job_id, url, f"download_{job_id}.mp4", request.quality)
        download_queue[job_id] = job
        jobs.append({"job_id": job_id, "url": url, "status": "queued"})
        background_tasks.add_task(process_download, job_id)
    return {"jobs": jobs, "count": len(jobs)}

@app.post("/api/v1/download/batch")
async def batch_download(request: BatchDownloadRequest, background_tasks: BackgroundTasks):
    jobs = []
    for item in request.items:
        url = item.get("url")
        if not url:
            continue
        job_id = secrets.token_hex(16)
        job = DownloadJob(job_id, url, item.get("filename", f"download_{job_id}.mp4"), item.get("quality", "auto"))
        download_queue[job_id] = job
        jobs.append({"job_id": job_id, "url": url, "status": "queued"})
        background_tasks.add_task(process_download, job_id)
    return {"jobs": jobs, "count": len(jobs)}

@app.get("/api/v1/download/status/{job_id}")
async def download_status(job_id: str):
    if job_id not in download_queue:
        raise HTTPException(404, "Download job not found")
    job = download_queue[job_id]
    return {"job_id": job.job_id, "url": job.url, "filename": job.filename, "quality": job.quality, "status": job.status, "progress": job.progress, "total_size": job.total_size, "downloaded_size": job.downloaded_size, "speed": job.speed, "error": job.error, "created_at": job.created_at.isoformat(), "started_at": job.started_at.isoformat() if job.started_at else None, "completed_at": job.completed_at.isoformat() if job.completed_at else None}

@app.get("/api/v1/download/queue")
async def download_queue_list():
    return {"jobs": [{"job_id": job.job_id, "filename": job.filename, "status": job.status, "progress": job.progress} for job in download_queue.values()], "count": len(download_queue)}

@app.delete("/api/v1/download/{job_id}")
async def cancel_download(job_id: str):
    if job_id not in download_queue:
        raise HTTPException(404, "Download job not found")
    job = download_queue[job_id]
    if job.status in ["pending", "downloading"]:
        job.status = "cancelled"
    return {"status": job.status}

async def process_download(job_id: str):
    if job_id not in download_queue:
        return
    job = download_queue[job_id]
    job.status = "downloading"
    job.started_at = datetime.now()
    stats.active_downloads += 1
    try:
        client = await http.get_client()
        async with client.stream("GET", job.url, follow_redirects=True) as response:
            if response.status_code != 200:
                job.status = "failed"
                job.error = f"HTTP {response.status_code}"
                return
            total_size = int(response.headers.get("content-length", 0))
            job.total_size = total_size
            downloaded = 0
            start_time = time.time()
            async for chunk in response.aiter_bytes(8192):
                if job.status == "cancelled":
                    return
                downloaded += len(chunk)
                job.downloaded_size = downloaded
                if total_size > 0:
                    job.progress = round(downloaded / total_size * 100, 2)
                elapsed = time.time() - start_time
                if elapsed > 0:
                    job.speed = downloaded / elapsed
            job.status = "completed"
            job.progress = 100.0
            job.completed_at = datetime.now()
            stats.completed_downloads += 1
            stats.total_bytes_downloaded += downloaded
    except Exception as e:
        job.status = "failed"
        job.error = str(e)
    finally:
        stats.active_downloads -= 1

@app.get("/api/v1/anime/search")
async def anime_search(query: str = Query(..., min_length=1)):
    results = []
    aniwave_results = await aniwave.search(query)
    gogoanime_results = await gogoanime.search(query)
    zoro_results = await zoro.search(query)
    for r in aniwave_results:
        r["source"] = "aniwave"
        results.append(r)
    for r in gogoanime_results:
        r["source"] = "gogoanime"
        results.append(r)
    for r in zoro_results:
        r["source"] = "hianime"
        results.append(r)
    return {"results": results, "count": len(results)}

@app.get("/api/v1/providers")
async def list_providers():
    return {"movie_tv": [{"name": "VidSrc.to", "status": "active"}, {"name": "VidSrc.me", "status": "active"}, {"name": "VidSrc.xyz", "status": "active"}, {"name": "VidSrc.cc", "status": "active"}, {"name": "VidSrc.nl", "status": "active"}, {"name": "VidSrc.icu", "status": "active"}, {"name": "VidSrc.pro", "status": "active"}, {"name": "MultiEmbed", "status": "active"}, {"name": "MoviesAPI", "status": "active"}, {"name": "2Embed", "status": "active"}, {"name": "Embed.su", "status": "active"}, {"name": "AutoEmbed", "status": "active"}, {"name": "FlixHQ", "status": "active"}, {"name": "GoMovies", "status": "active"}], "anime": [{"name": "Aniwave", "status": "active"}, {"name": "GogoAnime", "status": "active"}, {"name": "HiAnime", "status": "active"}, {"name": "AnimePahe", "status": "active"}], "extractors": ["Filemoon", "Streamwish", "VidHide", "MixDrop", "DoodStream", "StreamTape", "MP4Upload"]}

@app.get("/api/v1/cache/stats")
async def cache_stats():
    return {"size": len(cache.data), "max_size": cache.max_size, "ttl": cache.ttl, "hits": stats.cache_hits, "misses": stats.cache_misses, "hit_rate": round(stats.cache_hits / (stats.cache_hits + stats.cache_misses) * 100, 2) if (stats.cache_hits + stats.cache_misses) > 0 else 0}

@app.post("/api/v1/cache/clear")
async def clear_cache(api_key: str = Header(None, alias="X-API-Key")):
    if api_key != MASTER_API_KEY:
        raise HTTPException(403, "Invalid API key")
    cache.clear()
    return {"status": "cleared"}

@app.get("/api/v1/proxy")
async def proxy_request(url: str = Query(...)):
    response = await http.get(url)
    if not response:
        raise HTTPException(502, "Failed to fetch URL")
    return Response(content=response.content, media_type=response.headers.get("content-type", "application/octet-stream"))

@app.get("/api/v1/m3u8/proxy")
async def m3u8_proxy(url: str = Query(...)):
    response = await http.get(url)
    if not response or response.status_code != 200:
        raise HTTPException(502, "Failed to fetch M3U8")
    return Response(content=response.content, media_type="application/vnd.apple.mpegurl")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
