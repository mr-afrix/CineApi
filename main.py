import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from curl_cffi.requests import AsyncSession as CurlSession
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cineapi")

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280"
ANILIST_BASE = "https://graphql.anilist.co"
OPENSUBS_API_KEY = os.getenv("OPENSUBS_API_KEY", "")
OPENSUBS_BASE = "https://api.opensubtitles.com/api/v1"
PORT = int(os.getenv("PORT", 8000))
API_VERSION = "v1"
PROJECT_NAME = "CineAPI"
PROJECT_AUTHOR = "Jaden Afrix"
PROJECT_COMPANY = "SAGE"
PROJECT_VERSION = "1.0.0"

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

_stream_cache: dict[str, dict] = {}
_meta_cache: dict[str, dict] = {}
_search_cache: dict[str, dict] = {}
_source_cache: dict[str, dict] = {}
CACHE_TTL_STREAM = 1800
CACHE_TTL_META = 86400
CACHE_TTL_SEARCH = 3600
CACHE_TTL_SOURCE = 900


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


def cache_cleanup():
    now = time.time()
    for store in [_stream_cache, _meta_cache, _search_cache, _source_cache]:
        expired = [k for k, v in store.items() if now > v["expires"]]
        for k in expired:
            del store[k]


async def fetch_html(url: str, headers: dict = None, timeout: int = 15) -> str:
    try:
        async with CurlSession(impersonate="chrome124") as session:
            resp = await session.get(url, headers=headers or HEADERS_CHROME, timeout=timeout)
            return resp.text
    except Exception:
        try:
            async with httpx.AsyncClient(headers=headers or HEADERS_CHROME, follow_redirects=True, timeout=timeout) as client:
                resp = await client.get(url)
                return resp.text
        except Exception:
            return ""


async def fetch_json(url: str, headers: dict = None, timeout: int = 15) -> dict:
    try:
        async with CurlSession(impersonate="chrome124") as session:
            resp = await session.get(url, headers=headers or HEADERS_JSON, timeout=timeout)
            return resp.json()
        return {}
    except Exception:
        try:
            async with httpx.AsyncClient(headers=headers or HEADERS_JSON, follow_redirects=True, timeout=timeout) as client:
                resp = await client.get(url)
                return resp.json()
        except Exception:
            return {}


async def post_json(url: str, payload: dict, headers: dict = None, timeout: int = 15) -> dict:
    try:
        async with httpx.AsyncClient(headers=headers or HEADERS_JSON, follow_redirects=True, timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            return resp.json()
    except Exception:
        return {}


async def fetch_with_referer(url: str, referer: str, timeout: int = 15) -> str:
    headers = {**HEADERS_CHROME, "Referer": referer}
    return await fetch_html(url, headers=headers, timeout=timeout)


async def tmdb_get(endpoint: str, params: dict = None) -> dict:
    if not params:
        params = {}
    params["api_key"] = TMDB_API_KEY
    url = f"{TMDB_BASE}{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            return resp.json()
    except Exception:
        return {}


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
    detail = await tmdb_get(f"/movie/{tmdb_id}", {"append_to_response": "credits,videos,similar,keywords"})
    if not detail:
        return {}

    cast = []
    crew = []
    if "credits" in detail:
        cast = [{"name": m["name"], "character": m.get("character", ""), "profile": f"{TMDB_IMAGE_BASE}{m['profile_path']}" if m.get("profile_path") else None} for m in detail["credits"].get("cast", [])[:15]]
        crew = [{"name": m["name"], "job": m.get("job", ""), "department": m.get("department", "")} for m in detail["credits"].get("crew", []) if m.get("job") in ["Director", "Producer", "Screenplay", "Writer"]]

    trailer = None
    if "videos" in detail:
        for v in detail["videos"].get("results", []):
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                trailer = f"https://www.youtube.com/watch?v={v['key']}"
                break

    similar = []
    if "similar" in detail:
        similar = [{"imdb_id": None, "title": m["title"], "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0)} for m in detail["similar"].get("results", [])[:10]]

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
        "production_companies": [c["name"] for c in detail.get("production_companies", [])],
        "production_countries": [c["name"] for c in detail.get("production_countries", [])],
        "spoken_languages": [l["english_name"] for l in detail.get("spoken_languages", [])],
        "budget": detail.get("budget", 0),
        "revenue": detail.get("revenue", 0),
        "poster": f"{TMDB_IMAGE_BASE}{detail['poster_path']}" if detail.get("poster_path") else None,
        "backdrop": f"{TMDB_BACKDROP_BASE}{detail['backdrop_path']}" if detail.get("backdrop_path") else None,
        "trailer": trailer,
        "cast": cast,
        "crew": crew,
        "similar": similar,
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
    detail = await tmdb_get(f"/tv/{tmdb_id}", {"append_to_response": "credits,videos,similar,keywords,external_ids"})
    if not detail:
        return {}

    seasons = []
    for s in detail.get("seasons", []):
        if s["season_number"] == 0:
            continue
        season_detail = await tmdb_get(f"/tv/{tmdb_id}/season/{s['season_number']}")
        episodes = []
        if season_detail:
            for ep in season_detail.get("episodes", []):
                episodes.append({
                    "episode_number": ep["episode_number"],
                    "name": ep.get("name", ""),
                    "overview": ep.get("overview", ""),
                    "air_date": ep.get("air_date", ""),
                    "runtime": ep.get("runtime"),
                    "still": f"{TMDB_IMAGE_BASE}{ep['still_path']}" if ep.get("still_path") else None,
                    "rating": ep.get("vote_average", 0),
                })
        seasons.append({
            "season_number": s["season_number"],
            "name": s.get("name", ""),
            "overview": s.get("overview", ""),
            "episode_count": s.get("episode_count", 0),
            "air_date": s.get("air_date", ""),
            "poster": f"{TMDB_IMAGE_BASE}{s['poster_path']}" if s.get("poster_path") else None,
            "episodes": episodes,
        })

    cast = []
    if "credits" in detail:
        cast = [{"name": m["name"], "character": m.get("character", ""), "profile": f"{TMDB_IMAGE_BASE}{m['profile_path']}" if m.get("profile_path") else None} for m in detail["credits"].get("cast", [])[:15]]

    trailer = None
    if "videos" in detail:
        for v in detail["videos"].get("results", []):
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                trailer = f"https://www.youtube.com/watch?v={v['key']}"
                break

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
        "cast": cast,
        "seasons": seasons,
        "type": "tv",
    }

    cache_set(_meta_cache, cache_key, result, CACHE_TTL_META)
    return result


async def tmdb_search_movies(query: str, page: int = 1) -> list:
    data = await tmdb_get("/search/movie", {"query": query, "page": page, "include_adult": False})
    results = []
    for m in data.get("results", []):
        ext = await tmdb_get(f"/movie/{m['id']}", {"append_to_response": "external_ids"})
        imdb_id = ext.get("external_ids", {}).get("imdb_id") or ext.get("imdb_id")
        results.append({
            "imdb_id": imdb_id,
            "tmdb_id": m["id"],
            "title": m.get("title", ""),
            "overview": m.get("overview", ""),
            "year": m.get("release_date", "")[:4],
            "rating": m.get("vote_average", 0),
            "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None,
            "type": "movie",
        })
    return results


async def tmdb_search_tv(query: str, page: int = 1) -> list:
    data = await tmdb_get("/search/tv", {"query": query, "page": page})
    results = []
    for s in data.get("results", []):
        ext = await tmdb_get(f"/tv/{s['id']}/external_ids")
        imdb_id = ext.get("imdb_id")
        results.append({
            "imdb_id": imdb_id,
            "tmdb_id": s["id"],
            "title": s.get("name", ""),
            "overview": s.get("overview", ""),
            "year": s.get("first_air_date", "")[:4],
            "rating": s.get("vote_average", 0),
            "poster": f"{TMDB_IMAGE_BASE}{s['poster_path']}" if s.get("poster_path") else None,
            "type": "tv",
        })
    return results


async def tmdb_trending(media_type: str = "all", time_window: str = "week") -> list:
    data = await tmdb_get(f"/trending/{media_type}/{time_window}")
    results = []
    for item in data.get("results", [])[:20]:
        media = item.get("media_type", media_type)
        if media == "movie":
            ext = await tmdb_get(f"/movie/{item['id']}/external_ids")
            imdb_id = ext.get("imdb_id")
            results.append({"imdb_id": imdb_id, "tmdb_id": item["id"], "title": item.get("title", ""), "year": item.get("release_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "movie"})
        elif media == "tv":
            ext = await tmdb_get(f"/tv/{item['id']}/external_ids")
            imdb_id = ext.get("imdb_id")
            results.append({"imdb_id": imdb_id, "tmdb_id": item["id"], "title": item.get("name", ""), "year": item.get("first_air_date", "")[:4], "rating": item.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{item['poster_path']}" if item.get("poster_path") else None, "type": "tv"})
    return results


ANILIST_QUERY_SEARCH = """
query($search: String, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id
      idMal
      title { romaji english native }
      description
      episodes
      status
      averageScore
      popularity
      genres
      startDate { year month day }
      endDate { year month day }
      season
      seasonYear
      format
      duration
      studios { nodes { name } }
      coverImage { large extraLarge }
      bannerImage
      trailer { id site }
      nextAiringEpisode { airingAt episode }
      relations { edges { relationType node { id title { romaji english } type format coverImage { large } } } }
    }
  }
}
"""

ANILIST_QUERY_BY_ID = """
query($id: Int) {
  Media(id: $id, type: ANIME) {
    id
    idMal
    title { romaji english native }
    description
    episodes
    status
    averageScore
    popularity
    genres
    tags { name rank }
    startDate { year month day }
    endDate { year month day }
    season
    seasonYear
    format
    duration
    studios { nodes { name isAnimationStudio } }
    coverImage { large extraLarge }
    bannerImage
    trailer { id site }
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
      id
      title { romaji english }
      episodes
      status
      averageScore
      format
      coverImage { large extraLarge }
      startDate { year }
      genres
    }
  }
}
"""

ANILIST_QUERY_POPULAR = """
query($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: POPULARITY_DESC) {
      id
      title { romaji english }
      episodes
      status
      averageScore
      format
      coverImage { large extraLarge }
      startDate { year }
      genres
    }
  }
}
"""


async def anilist_request(query: str, variables: dict) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(ANILIST_BASE, json={"query": query, "variables": variables}, headers={"Content-Type": "application/json", "Accept": "application/json"})
            return resp.json()
    except Exception:
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
        results.append({
            "anilist_id": m.get("id"),
            "mal_id": m.get("idMal"),
            "title": title.get("english") or title.get("romaji"),
            "cover": cover.get("large"),
            "score": m.get("averageScore"),
            "episodes": m.get("episodes"),
            "status": m.get("status"),
            "format": m.get("format"),
            "year": start.get("year"),
            "genres": m.get("genres", []),
            "type": "anime",
        })
    return results


async def get_anilist_trending(page: int = 1) -> list:
    data = await anilist_request(ANILIST_QUERY_TRENDING, {"page": page, "perPage": 20})
    results = []
    for m in data.get("data", {}).get("Page", {}).get("media", []):
        title = m.get("title", {})
        cover = m.get("coverImage", {})
        start = m.get("startDate", {})
        results.append({"anilist_id": m.get("id"), "title": title.get("english") or title.get("romaji"), "cover": cover.get("extraLarge") or cover.get("large"), "score": m.get("averageScore"), "episodes": m.get("episodes"), "status": m.get("status"), "format": m.get("format"), "year": start.get("year"), "genres": m.get("genres", []), "type": "anime"})
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
    if matches:
        return matches[0]
    return None


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
        season_ids_simple = re.findall(r'data-id="(\d+)"', seasons_html)
        if season <= len(season_ids_simple):
            target_season_id = season_ids_simple[season - 1]
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
        ep_ids_simple = re.findall(r'data-id="(\d+)"', episodes_html)
        if episode <= len(ep_ids_simple):
            target_episode_id = ep_ids_simple[episode - 1]
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
    source_url = f"https://flixhq.to/ajax/get_link/{server_id}"
    data = await fetch_json(source_url, headers=HEADERS_AJAX)
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
    except Exception:
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
    except Exception:
        return []


async def search_hdrezka(query: str) -> list:
    url = f"https://rezka.ag/search/?do=search&subaction=search&q={urllib.parse.quote(query)}"
    html_content = await fetch_html(url)
    results = []
    items = re.findall(r'<div class="b-content__inline_item"[^>]*>.*?<a href="([^"]+)"[^>]*>.*?<div class="b-content__inline_item-link">.*?<a[^>]*>([^<]+)</a>.*?<div[^>]*>([^<]*)</div>', html_content, re.DOTALL)
    for url_match, title, info in items:
        results.append({"url": url_match, "title": title.strip(), "info": info.strip()})
    return results


async def get_hdrezka_translations(page_url: str) -> list:
    html_content = await fetch_html(page_url)
    translations = []
    trans_matches = re.findall(r'<li[^>]*data-translator_id="(\d+)"[^>]*data-id="(\d+)"[^>]*>([^<]+)</li>', html_content)
    for trans_id, content_id, trans_name in trans_matches:
        translations.append({"translator_id": trans_id, "content_id": content_id, "name": trans_name.strip()})
    if not translations:
        content_id_match = re.search(r"initCDNMoviesEvents\((\d+),", html_content)
        if content_id_match:
            translations.append({"translator_id": "0", "content_id": content_id_match.group(1), "name": "Default"})
    return translations


async def get_hdrezka_stream(content_id: str, translator_id: str, season: int = None, episode: int = None) -> list:
    payload = {
        "id": content_id,
        "translator_id": translator_id,
        "is_camrip": "0",
        "is_ads": "0",
        "is_director": "0",
        "action": "get_movie" if not season else "get_stream",
    }
    if season and episode:
        payload["season"] = str(season)
        payload["episode"] = str(episode)
    headers = {**HEADERS_AJAX, "Referer": "https://rezka.ag/", "Origin": "https://rezka.ag"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post("https://rezka.ag/ajax/get_cdn_series/", data=payload, headers=headers)
            data = resp.json()
    except Exception:
        return []
    if not data.get("success"):
        return []
    stream_str = data.get("url", "")
    streams = []
    quality_pattern = re.findall(r'\[([^\]]+)\](https?://[^\s,]+\.m3u8[^\s,]*)', stream_str)
    for quality, url in quality_pattern:
        streams.append({"url": url, "quality": quality.strip(), "format": "m3u8", "provider": "HDRezka"})
    if not streams:
        mp4_pattern = re.findall(r'\[([^\]]+)\](https?://[^\s,]+\.mp4[^\s,]*)', stream_str)
        for quality, url in mp4_pattern:
            streams.append({"url": url, "quality": quality.strip(), "format": "mp4", "provider": "HDRezka"})
    return streams


async def scrape_hdrezka_movie(imdb_id: str) -> list:
    try:
        meta = await get_tmdb_movie_meta(imdb_id)
        title = meta.get("title", "") or meta.get("original_title", "")
        if not title:
            return []
        results = await search_hdrezka(title)
        if not results:
            return []
        page_url = results[0]["url"]
        translations = await get_hdrezka_translations(page_url)
        all_streams = []
        for trans in translations[:2]:
            streams = await get_hdrezka_stream(trans["content_id"], trans["translator_id"])
            all_streams.extend(streams)
        return all_streams
    except Exception:
        return []


async def scrape_hdrezka_tv(imdb_id: str, season: int, episode: int) -> list:
    try:
        meta = await get_tmdb_tv_meta(imdb_id)
        title = meta.get("title", "") or meta.get("original_title", "")
        if not title:
            return []
        results = await search_hdrezka(title)
        if not results:
            return []
        page_url = results[0]["url"]
        translations = await get_hdrezka_translations(page_url)
        all_streams = []
        for trans in translations[:2]:
            streams = await get_hdrezka_stream(trans["content_id"], trans["translator_id"], season, episode)
            all_streams.extend(streams)
        return all_streams
    except Exception:
        return []


async def search_lookmovie(title: str, is_tv: bool = False) -> Optional[dict]:
    content_type = "shows" if is_tv else "movies"
    url = f"https://lookmovie2.to/api/v1/{content_type}/search/?q={urllib.parse.quote(title)}"
    data = await fetch_json(url)
    results = data.get("result", [])
    if not results:
        return None
    return results[0]


async def get_lookmovie_stream(slug: str, is_tv: bool = False, season: int = None, episode: int = None) -> list:
    if is_tv:
        url = f"https://lookmovie2.to/shows/view/{slug}"
        html_content = await fetch_html(url)
        show_id_match = re.search(r'"show_storage":\s*\{[^}]*"id_show":\s*"?(\d+)"?', html_content)
        if not show_id_match:
            return []
        show_id = show_id_match.group(1)
        episodes_url = f"https://lookmovie2.to/api/v1/shows/episode-item/?season={season}&episode={episode}&id_show={show_id}"
        ep_data = await fetch_json(episodes_url)
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
        stream_file = result.get(quality)
        if stream_file:
            streams.append({"url": stream_file, "quality": quality, "format": "m3u8" if ".m3u8" in stream_file else "mp4", "provider": "LookMovie"})
    return streams


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
    except Exception:
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
    except Exception:
        return []


async def get_superembed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://multiembed.mov/directstream.php?video_id={imdb_id}&s={season}&e={episode}"
        else:
            url = f"https://multiembed.mov/directstream.php?video_id={imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        m3u8_matches = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content)
        for m in m3u8_matches:
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "provider": "SuperEmbed"})
        mp4_matches = re.findall(r'(https?://[^\s"\']+\.mp4[^\s"\']*)', html_content)
        for m in mp4_matches:
            streams.append({"url": m, "quality": "auto", "format": "mp4", "provider": "SuperEmbed"})
        return streams
    except Exception:
        return []


async def get_embedsu_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://embed.su/embed/tv/{imdb_id}/{season}/{episode}"
        else:
            url = f"https://embed.su/embed/movie/{imdb_id}"
        html_content = await fetch_html(url)
        json_match = re.search(r'JSON\.parse\(atob\(["\']([^"\']+)["\']\)\)', html_content)
        if not json_match:
            return []
        import base64
        decoded = base64.b64decode(json_match.group(1)).decode("utf-8")
        data = json.loads(decoded)
        streams = []
        for source in data.get("sources", []):
            url_val = source.get("file") or source.get("url")
            if url_val:
                streams.append({"url": url_val, "quality": source.get("label", "auto"), "format": "m3u8" if ".m3u8" in url_val else "mp4", "provider": "EmbedSu"})
        return streams
    except Exception:
        return []


async def get_autoembed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://autoembed.cc/embed/imdb/tv-{imdb_id}-{season}-{episode}"
        else:
            url = f"https://autoembed.cc/embed/imdb/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        m3u8_matches = re.findall(r'"file"\s*:\s*"(https?://[^"]+\.m3u8[^"]*)"', html_content)
        for m in m3u8_matches:
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "provider": "AutoEmbed"})
        source_matches = re.findall(r'"src"\s*:\s*"(https?://[^"]+)"', html_content)
        for m in source_matches:
            fmt = "m3u8" if ".m3u8" in m else "mp4"
            streams.append({"url": m, "quality": "auto", "format": fmt, "provider": "AutoEmbed"})
        return streams
    except Exception:
        return []


async def get_vidsrcpro_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://vidsrc.pro/embed/tv/{imdb_id}/{season}/{episode}"
        else:
            url = f"https://vidsrc.pro/embed/movie/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        src_matches = re.findall(r'src[cC]?\s*=\s*["\']([^"\']+)["\']', html_content)
        for src in src_matches:
            if any(x in src for x in [".m3u8", ".mp4", "stream", "playlist"]):
                fmt = "m3u8" if ".m3u8" in src else "mp4"
                streams.append({"url": src, "quality": "auto", "format": fmt, "provider": "VidSrcPro"})
        return streams
    except Exception:
        return []


async def get_smashystream_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://player.smashy.stream/tv/{imdb_id}?s={season}&e={episode}"
        else:
            url = f"https://player.smashy.stream/movie/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        m3u8_matches = re.findall(r'"(https?://[^"]+\.m3u8[^"]*)"', html_content)
        for m in m3u8_matches:
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "provider": "SmashyStream"})
        return streams
    except Exception:
        return []


async def get_2embed_sources(imdb_id: str, season: int = None, episode: int = None) -> list:
    try:
        if season and episode:
            url = f"https://www.2embed.cc/embedtv/{imdb_id}&s={season}&e={episode}"
        else:
            url = f"https://www.2embed.cc/embed/{imdb_id}"
        html_content = await fetch_html(url)
        streams = []
        iframe_matches = re.findall(r'<iframe[^>]*src=["\']([^"\']+)["\']', html_content)
        for iframe_src in iframe_matches:
            if any(x in iframe_src for x in ["stream", "player", "embed"]) and "2embed" not in iframe_src:
                extracted = await extract_stream_from_host(iframe_src, url)
                for s in extracted:
                    s["provider"] = "2Embed"
                    streams.append(s)
        return streams
    except Exception:
        return []


async def search_zoro(title: str) -> Optional[dict]:
    search_url = f"https://hianime.to/search?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'<a[^>]*href="/([^"?]+)\?[^"]*"[^>]*class="[^"]*film-name[^"]*"[^>]*>([^<]+)</a>', html_content)
    if not items:
        items = re.findall(r'href="/([a-z0-9-]+-\d+)\?[^"]*"[^>]*>.*?<span[^>]*class="[^"]*film-name[^"]*"[^>]*>([^<]+)', html_content, re.DOTALL)
    if items:
        return {"id": items[0][0], "title": items[0][1].strip()}
    return None


async def get_zoro_episodes(anime_id: str) -> list:
    url = f"https://hianime.to/ajax/v2/episode/list/{anime_id.split('-')[-1]}"
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("html", "")
    episodes = []
    ep_matches = re.findall(r'data-id="(\d+)"[^>]*data-number="(\d+)"[^>]*title="([^"]*)"', html_content)
    for ep_id, ep_num, ep_title in ep_matches:
        episodes.append({"id": ep_id, "number": int(ep_num), "title": ep_title})
    return episodes


async def get_zoro_servers(episode_id: str) -> list:
    url = f"https://hianime.to/ajax/v2/episode/servers?episodeId={episode_id}"
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("html", "")
    servers = []
    server_matches = re.findall(r'data-id="(\d+)"[^>]*data-type="([^"]+)"[^>]*>.*?<span>([^<]+)</span>', html_content, re.DOTALL)
    for sid, stype, sname in server_matches:
        servers.append({"id": sid, "type": stype, "name": sname.strip()})
    return servers


async def get_zoro_source(server_id: str) -> Optional[str]:
    url = f"https://hianime.to/ajax/v2/episode/sources?id={server_id}"
    data = await fetch_json(url, headers=HEADERS_AJAX)
    return data.get("link")


async def scrape_zoro_anime(anilist_id: int, episode_num: int) -> list:
    try:
        meta = await get_anilist_meta(anilist_id)
        title = meta.get("title_english") or meta.get("title_romaji", "")
        if not title:
            return []
        result = await search_zoro(title)
        if not result:
            return []
        episodes = await get_zoro_episodes(result["id"])
        target_ep = None
        for ep in episodes:
            if ep["number"] == episode_num:
                target_ep = ep
                break
        if not target_ep:
            if episodes:
                target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        servers = await get_zoro_servers(target_ep["id"])
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
    except Exception:
        return []


async def search_9anime(title: str) -> Optional[dict]:
    vrf_url = f"https://9anime.pl/filter?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(vrf_url)
    items = re.findall(r'href="/watch/([^"?]+)"[^>]*>.*?<span[^>]*>([^<]+)</span>', html_content, re.DOTALL)
    if items:
        return {"id": items[0][0], "title": items[0][1].strip()}
    return None


async def get_9anime_episodes(anime_id: str) -> list:
    url = f"https://9anime.pl/ajax/episode/list/{anime_id}?vrf="
    data = await fetch_json(url, headers=HEADERS_AJAX)
    html_content = data.get("result", "")
    episodes = []
    ep_matches = re.findall(r'data-ids="([^"]+)"[^>]*data-num="(\d+)"', html_content)
    for ep_ids, ep_num in ep_matches:
        episodes.append({"ids": ep_ids, "number": int(ep_num)})
    return episodes


async def get_9anime_source(episode_ids: str, server_id: str) -> list:
    url = f"https://9anime.pl/ajax/server/{server_id}?vrf="
    params_url = f"https://9anime.pl/ajax/episode/servers?episodeId={episode_ids}&vrf="
    servers_data = await fetch_json(params_url, headers=HEADERS_AJAX)
    result_html = servers_data.get("result", "")
    server_matches = re.findall(r'data-link-id="(\d+)"[^>]*data-name="([^"]+)"', result_html)
    streams = []
    for link_id, server_name in server_matches[:3]:
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
        target_ep = None
        for ep in episodes:
            if ep["number"] == episode_num:
                target_ep = ep
                break
        if not target_ep and episodes:
            target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        return await get_9anime_source(target_ep["ids"], result["id"])
    except Exception:
        return []


async def search_gogoanime(title: str) -> Optional[dict]:
    search_url = f"https://gogoanime3.cc/search.html?keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'<div class="img">.*?<a href="([^"]+)" title="([^"]+)"', html_content, re.DOTALL)
    if items:
        return {"path": items[0][0], "title": items[0][1]}
    return None


async def get_gogoanime_episodes(anime_path: str) -> list:
    url = f"https://gogoanime3.cc{anime_path}"
    html_content = await fetch_html(url)
    movie_id_match = re.search(r'<input[^>]*id="movie_id"[^>]*value="(\d+)"', html_content)
    last_ep_match = re.search(r'<input[^>]*id="ep_end"[^>]*value="(\d+)"', html_content)
    if not movie_id_match or not last_ep_match:
        return []
    movie_id = movie_id_match.group(1)
    last_ep = int(last_ep_match.group(1))
    ep_list_url = f"https://ajax.gogocdn.net/ajax/load-list-episode?ep_start=1&ep_end={last_ep}&id={movie_id}"
    ep_html = await fetch_html(ep_list_url)
    episodes = []
    ep_matches = re.findall(r'<a href="([^"]+)".*?<div class="name">\s*EP\s*(\d+)', ep_html, re.DOTALL)
    for ep_path, ep_num in ep_matches:
        episodes.append({"path": ep_path.strip(), "number": int(ep_num)})
    return sorted(episodes, key=lambda x: x["number"])


async def get_gogoanime_stream(episode_path: str) -> list:
    url = f"https://gogoanime3.cc{episode_path}"
    html_content = await fetch_html(url)
    iframe_match = re.search(r'<iframe[^>]*src="(https://[^"]+gogoanime[^"]*|https://[^"]+gogocdn[^"]*|https://[^"]+playtaku[^"]*)"', html_content)
    if not iframe_match:
        iframe_match = re.search(r'<iframe[^>]*src="([^"]+)"[^>]*class="[^"]*player[^"]*"', html_content)
    if not iframe_match:
        return []
    embed_url = iframe_match.group(1)
    if embed_url.startswith("//"):
        embed_url = "https:" + embed_url
    return await extract_stream_from_host(embed_url, url)


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
        target_ep = None
        for ep in episodes:
            if ep["number"] == episode_num:
                target_ep = ep
                break
        if not target_ep and episodes:
            target_ep = episodes[min(episode_num - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        streams = await get_gogoanime_stream(target_ep["path"])
        for s in streams:
            s["provider"] = "GogoAnime"
        return streams
    except Exception:
        return []


async def search_kissasian(title: str) -> Optional[dict]:
    search_url = f"https://kissasian.sh/Search/SearchSuggest?type=Drama&keyword={urllib.parse.quote(title)}"
    html_content = await fetch_html(search_url)
    items = re.findall(r'<a href="([^"]+)"[^>]*>([^<]+)</a>', html_content)
    if items:
        return {"url": items[0][0], "title": items[0][1].strip()}
    return None


async def get_kissasian_episodes(drama_url: str) -> list:
    html_content = await fetch_html(drama_url)
    episodes = []
    ep_matches = re.findall(r'<li[^>]*>.*?<a href="([^"]+)"[^>]*>.*?Episode\s+(\d+)', html_content, re.DOTALL)
    for ep_url, ep_num in ep_matches:
        episodes.append({"url": ep_url, "number": int(ep_num)})
    return sorted(episodes, key=lambda x: x["number"])


async def get_kissasian_stream(episode_url: str) -> list:
    html_content = await fetch_html(episode_url)
    streams = []
    iframe_matches = re.findall(r'<iframe[^>]*src=["\']([^"\']+)["\']', html_content)
    for iframe_src in iframe_matches:
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
        target_ep = None
        ep_num_overall = (season - 1) * 100 + episode
        for ep in episodes:
            if ep["number"] == episode:
                target_ep = ep
                break
        if not target_ep and episodes:
            target_ep = episodes[min(ep_num_overall - 1, len(episodes) - 1)]
        if not target_ep:
            return []
        return await get_kissasian_stream(target_ep["url"])
    except Exception:
        return []


async def extract_filemoon(url: str, referer: str = "") -> list:
    try:
        headers = {**HEADERS_CHROME}
        if referer:
            headers["Referer"] = referer
        html_content = await fetch_html(url, headers=headers)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            packed = packed_match.group(0)
            unpacked = unpack_js(packed)
            m3u8_matches = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', unpacked)
            if m3u8_matches:
                return [{"url": m3u8_matches[0], "quality": "auto", "format": "m3u8", "host": "Filemoon"}]
        m3u8_direct = re.findall(r'"file"\s*:\s*"(https?://[^"]+\.m3u8[^"]*)"', html_content)
        if m3u8_direct:
            return [{"url": m3u8_direct[0], "quality": "auto", "format": "m3u8", "host": "Filemoon"}]
        jwplayer_match = re.search(r'jwplayer\([^)]+\)\.setup\(\{.*?"file"\s*:\s*"([^"]+)"', html_content, re.DOTALL)
        if jwplayer_match:
            file_url = jwplayer_match.group(1)
            fmt = "m3u8" if ".m3u8" in file_url else "mp4"
            return [{"url": file_url, "quality": "auto", "format": fmt, "host": "Filemoon"}]
        return []
    except Exception:
        return []


async def extract_vidplay(url: str, referer: str = "") -> list:
    try:
        headers = {**HEADERS_CHROME}
        if referer:
            headers["Referer"] = referer
        html_content = await fetch_html(url, headers=headers)
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', html_content, re.DOTALL)
        if sources_match:
            sources_str = sources_match.group(1)
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_str)
            label_matches = re.findall(r'"label"\s*:\s*"([^"]+)"', sources_str)
            streams = []
            for i, file_url in enumerate(file_matches):
                label = label_matches[i] if i < len(label_matches) else "auto"
                fmt = "m3u8" if ".m3u8" in file_url else "mp4"
                streams.append({"url": file_url, "quality": label, "format": fmt, "host": "Vidplay"})
            return streams
        m3u8_matches = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content)
        if m3u8_matches:
            return [{"url": m3u8_matches[0], "quality": "auto", "format": "m3u8", "host": "Vidplay"}]
        return []
    except Exception:
        return []


async def extract_streamtape(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        token_match = re.search(r"getElementById\('norobotlink'\)\.innerHTML\s*=\s*'([^']+)'", html_content)
        if not token_match:
            token_match = re.search(r"\.innerHTML\s*=\s*([^;]+);", html_content)
        robotlink_match = re.search(r"robotlink['\"]?\s*\)\.innerHTML\s*=\s*['\"]?([^'\"<]+)", html_content)
        direct_match = re.search(r'(https?://[^\s"\']+streamtape[^\s"\']+)', html_content)
        if direct_match:
            stream_url = direct_match.group(1)
            if not stream_url.startswith("http"):
                stream_url = "https:" + stream_url
            return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "StreamTape"}]
        concat_match = re.search(r"document\.getElementById\('robotlink'\)\.innerHTML\s*=\s*([^<]+)<", html_content)
        if concat_match:
            js_expr = concat_match.group(1).strip()
            parts = re.findall(r'"([^"]+)"', js_expr)
            if parts:
                stream_url = "https:" + "".join(parts)
                return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "StreamTape"}]
        return []
    except Exception:
        return []


async def extract_doodstream(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        pass_match = re.search(r"/pass_md5/([^'\"]+)", html_content)
        if not pass_match:
            return []
        pass_path = pass_match.group(0)
        base_url = re.match(r"(https?://[^/]+)", url)
        if not base_url:
            return []
        base = base_url.group(1)
        token_url = f"{base}{pass_path}"
        async with httpx.AsyncClient(headers={**HEADERS_CHROME, "Referer": url}, timeout=15) as client:
            resp = await client.get(token_url)
            token = resp.text.strip()
        if not token:
            return []
        import random
        import string
        rand_str = "".join(random.choices(string.ascii_letters + string.digits, k=10))
        final_url = f"{token}{rand_str}?token={pass_path.split('/')[-1]}&expiry={int(time.time()) + 3600}"
        return [{"url": final_url, "quality": "auto", "format": "mp4", "host": "DoodStream"}]
    except Exception:
        return []


async def extract_upstream(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            unpacked = unpack_js(packed_match.group(0))
            m3u8_match = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', unpacked)
            if m3u8_match:
                return [{"url": m3u8_match[0], "quality": "auto", "format": "m3u8", "host": "Upstream"}]
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', html_content, re.DOTALL)
        if sources_match:
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
            if file_matches:
                return [{"url": file_matches[0], "quality": "auto", "format": "m3u8" if ".m3u8" in file_matches[0] else "mp4", "host": "Upstream"}]
        return []
    except Exception:
        return []


async def extract_mixdrop(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            unpacked = unpack_js(packed_match.group(0))
            wurl_match = re.search(r'wurl\s*=\s*"([^"]+)"', unpacked)
            if wurl_match:
                stream_url = wurl_match.group(1)
                if stream_url.startswith("//"):
                    stream_url = "https:" + stream_url
                return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "MixDrop"}]
        wurl_direct = re.search(r'wurl\s*=\s*"([^"]+)"', html_content)
        if wurl_direct:
            stream_url = wurl_direct.group(1)
            if stream_url.startswith("//"):
                stream_url = "https:" + stream_url
            return [{"url": stream_url, "quality": "auto", "format": "mp4", "host": "MixDrop"}]
        return []
    except Exception:
        return []


async def extract_mp4upload(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        src_match = re.search(r'"src"\s*:\s*"(https?://[^"]+\.mp4[^"]*)"', html_content)
        if src_match:
            return [{"url": src_match.group(1), "quality": "auto", "format": "mp4", "host": "Mp4Upload"}]
        file_match = re.search(r'"file"\s*:\s*"(https?://[^"]+)"', html_content)
        if file_match:
            fu = file_match.group(1)
            return [{"url": fu, "quality": "auto", "format": "m3u8" if ".m3u8" in fu else "mp4", "host": "Mp4Upload"}]
        return []
    except Exception:
        return []


async def extract_fembed(url: str, referer: str = "") -> list:
    try:
        vid_id_match = re.search(r"/(?:v|f)/([^/?]+)", url)
        if not vid_id_match:
            return []
        vid_id = vid_id_match.group(1)
        base = re.match(r"(https?://[^/]+)", url).group(1)
        api_url = f"{base}/api/source/{vid_id}"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(api_url, headers={**HEADERS_AJAX, "Referer": url})
            data = resp.json()
        if not data.get("success"):
            return []
        streams = []
        for source in data.get("data", []):
            streams.append({"url": source["file"], "quality": source.get("label", "auto"), "format": source.get("type", "mp4"), "host": "Fembed"})
        return streams
    except Exception:
        return []


async def extract_okru(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        data_match = re.search(r'data-options="([^"]+)"', html_content)
        if not data_match:
            return []
        data_str = html.unescape(data_match.group(1))
        data = json.loads(data_str)
        flash_vars = data.get("flashvars", {})
        metadata_str = flash_vars.get("metadata") or flash_vars.get("videoSources")
        if not metadata_str:
            return []
        metadata = json.loads(metadata_str) if isinstance(metadata_str, str) else metadata_str
        streams = []
        videos = metadata.get("videos", metadata) if isinstance(metadata, dict) else metadata
        if isinstance(videos, list):
            for video in videos:
                streams.append({"url": video.get("url"), "quality": video.get("name", "auto"), "format": "mp4", "host": "OK.ru"})
        return [s for s in streams if s["url"]]
    except Exception:
        return []


async def extract_voe(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url)
        m3u8_match = re.search(r"'hls'\s*:\s*'([^']+)'", html_content)
        if not m3u8_match:
            m3u8_match = re.search(r'"hls"\s*:\s*"([^"]+)"', html_content)
        if m3u8_match:
            m3u8_url = m3u8_match.group(1)
            import base64
            if not m3u8_url.startswith("http"):
                try:
                    m3u8_url = base64.b64decode(m3u8_url).decode("utf-8")
                except Exception:
                    pass
            return [{"url": m3u8_url, "quality": "auto", "format": "m3u8", "host": "VOE"}]
        return []
    except Exception:
        return []


async def extract_vidhide(url: str, referer: str = "") -> list:
    try:
        html_content = await fetch_html(url, headers={**HEADERS_CHROME, "Referer": referer or "https://flixhq.to"})
        packed_match = re.search(r"eval\(function\(p,a,c,k,e,d\).*?\)\)", html_content, re.DOTALL)
        if packed_match:
            unpacked = unpack_js(packed_match.group(0))
            sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', unpacked, re.DOTALL)
            if sources_match:
                file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
                if file_matches:
                    return [{"url": file_matches[0], "quality": "auto", "format": "m3u8" if ".m3u8" in file_matches[0] else "mp4", "host": "VidHide"}]
        sources_match = re.search(r'"sources"\s*:\s*\[(.*?)\]', html_content, re.DOTALL)
        if sources_match:
            file_matches = re.findall(r'"file"\s*:\s*"([^"]+)"', sources_match.group(1))
            if file_matches:
                return [{"url": file_matches[0], "quality": "auto", "format": "m3u8" if ".m3u8" in file_matches[0] else "mp4", "host": "VidHide"}]
        return []
    except Exception:
        return []


def _base_encode(n: int, base: int) -> str:
    """Convert integer n to a base-N string using 0-9a-z alphabet."""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return ""
    remainder = n % base
    char = chars[remainder] if remainder < len(chars) else str(remainder)
    return _base_encode(n // base, base) + char


def unpack_js(packed: str) -> str:
    try:
        p_match = re.search(r"'([^']+)'", packed)
        a_match = re.search(r",(\d+),", packed)
        c_match = re.search(r",\d+,(\d+),", packed)
        k_match = re.search(r"'([^']+)'\.split\('\\|'\)", packed)
        if not all([p_match, a_match, c_match, k_match]):
            return packed
        p = p_match.group(1)
        a = int(a_match.group(1))
        c = int(c_match.group(1))
        k = k_match.group(1).split("|")
        while c > 0:
            c -= 1
            if c < len(k) and k[c]:
                p = re.sub(r"\b" + _base_encode(c, a) + r"\b", k[c], p)
        return p
    except Exception:
        return packed


async def extract_stream_from_host(url: str, referer: str = "") -> list:
    if not url:
        return []
    url_lower = url.lower()
    if "filemoon" in url_lower or "moonplayer" in url_lower:
        return await extract_filemoon(url, referer)
    elif "vidplay" in url_lower or "vidstream" in url_lower or "mcloud" in url_lower:
        return await extract_vidplay(url, referer)
    elif "streamtape" in url_lower:
        return await extract_streamtape(url, referer)
    elif "dood" in url_lower:
        return await extract_doodstream(url, referer)
    elif "upstream" in url_lower or "upns" in url_lower:
        return await extract_upstream(url, referer)
    elif "mixdrop" in url_lower:
        return await extract_mixdrop(url, referer)
    elif "mp4upload" in url_lower:
        return await extract_mp4upload(url, referer)
    elif "fembed" in url_lower or "layar.kita" in url_lower:
        return await extract_fembed(url, referer)
    elif "ok.ru" in url_lower or "odnoklassniki" in url_lower:
        return await extract_okru(url, referer)
    elif "voe.sx" in url_lower:
        return await extract_voe(url, referer)
    elif "vidhide" in url_lower or "vid.icu" in url_lower:
        return await extract_vidhide(url, referer)
    else:
        html_content = await fetch_html(url, headers={**HEADERS_CHROME, "Referer": referer})
        streams = []
        m3u8_matches = re.findall(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', html_content)
        for m in m3u8_matches:
            streams.append({"url": m, "quality": "auto", "format": "m3u8", "host": "Unknown"})
        if not streams:
            mp4_matches = re.findall(r'(https?://[^\s"\']+\.mp4[^\s"\']*)', html_content)
            for m in mp4_matches:
                streams.append({"url": m, "quality": "auto", "format": "mp4", "host": "Unknown"})
        return streams


async def get_opensubs_token() -> Optional[str]:
    if not OPENSUBS_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{OPENSUBS_BASE}/login", json={"username": "", "password": ""}, headers={"Api-Key": OPENSUBS_API_KEY, "Content-Type": "application/json"})
            data = resp.json()
            return data.get("token")
    except Exception:
        return None


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
        headers = {"Api-Key": OPENSUBS_API_KEY or "temporarily", "User-Agent": "CineAPI v1.0"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{OPENSUBS_BASE}/subtitles", params=params, headers=headers)
            data = resp.json()
        results = []
        for item in data.get("data", [])[:20]:
            attrs = item.get("attributes", {})
            files = attrs.get("files", [])
            if files:
                results.append({
                    "id": item.get("id"),
                    "language": attrs.get("language"),
                    "file_name": attrs.get("release", ""),
                    "upload_date": attrs.get("upload_date", ""),
                    "downloads": attrs.get("download_count", 0),
                    "rating": attrs.get("ratings", 0),
                    "file_id": files[0].get("file_id"),
                    "format": "srt",
                })
        cache_set(_meta_cache, cache_key, results, CACHE_TTL_META)
        return results
    except Exception:
        return []


async def get_subtitle_download_url(file_id: int) -> Optional[str]:
    try:
        headers = {"Api-Key": OPENSUBS_API_KEY or "temporarily", "User-Agent": "CineAPI v1.0", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{OPENSUBS_BASE}/download", json={"file_id": file_id}, headers=headers)
            data = resp.json()
            return data.get("link")
    except Exception:
        return None


def rank_streams(streams: list) -> list:
    quality_order = {"2160p": 0, "4k": 0, "1080p": 1, "720p": 2, "480p": 3, "360p": 4, "auto": 5}
    format_order = {"m3u8": 0, "mp4": 1}
    provider_order = {"HDRezka": 0, "FlixHQ": 1, "LookMovie": 2, "HiAnime/Zoro": 3, "GogoAnime": 4, "9Anime": 5, "SuperEmbed": 6, "AutoEmbed": 7, "SmashyStream": 8, "2Embed": 9, "EmbedSu": 10}

    def score(s):
        q = s.get("quality", "auto").lower()
        f = s.get("format", "mp4").lower()
        p = s.get("provider", "Unknown")
        q_score = quality_order.get(q, 5)
        f_score = format_order.get(f, 1)
        p_score = provider_order.get(p, 99)
        return (q_score, f_score, p_score)

    return sorted(streams, key=score)


def dedupe_streams(streams: list) -> list:
    seen_urls = set()
    unique = []
    for s in streams:
        url = s.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(s)
    return unique


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


async def resolve_anime_streams(anilist_id: int, episode_num: int) -> list:
    cache_key = f"anime_streams_{anilist_id}_{episode_num}"
    cached = cache_get(_stream_cache, cache_key)
    if cached:
        return cached

    tasks = [
        scrape_zoro_anime(anilist_id, episode_num),
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


def build_player_html(title: str, stream_url: str, subtitle_url: str = None, poster: str = None, streams: list = None, subtitles: list = None) -> str:
    streams = streams or []
    subtitles = subtitles or []
    streams_json = json.dumps(streams)
    subtitles_json = json.dumps(subtitles)
    poster_style = f'background-image: url("{poster}");' if poster else "background: #0a0a0a;"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{html.escape(title)} — CineAPI Player</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/video.js/8.6.1/video-js.min.css"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/video.js/8.6.1/video.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/videojs-contrib-hls/5.15.0/videojs-contrib-hls.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#000;color:#fff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;height:100vh;overflow:hidden}}
#player-container{{position:relative;width:100vw;height:100vh}}
.video-js{{width:100%;height:100%}}
#controls-bar{{position:absolute;bottom:0;left:0;right:0;padding:12px 16px;background:linear-gradient(transparent,rgba(0,0,0,0.85));display:flex;align-items:center;gap:12px;z-index:10;opacity:0;transition:opacity 0.3s}}
#player-container:hover #controls-bar{{opacity:1}}
#source-select,#sub-select{{background:rgba(255,255,255,0.1);color:#fff;border:1px solid rgba(255,255,255,0.2);border-radius:6px;padding:5px 10px;font-size:12px;cursor:pointer;backdrop-filter:blur(10px)}}
#source-select option,#sub-select option{{background:#1a1a1a;color:#fff}}
#title-bar{{position:absolute;top:0;left:0;right:0;padding:16px;background:linear-gradient(rgba(0,0,0,0.7),transparent);font-size:14px;font-weight:600;letter-spacing:0.5px;z-index:10;opacity:0;transition:opacity 0.3s}}
#player-container:hover #title-bar{{opacity:1}}
#branding{{position:absolute;top:16px;right:16px;font-size:11px;color:rgba(255,255,255,0.4);z-index:10;letter-spacing:1px;text-transform:uppercase}}
.vjs-big-play-button{{left:50%;top:50%;transform:translate(-50%,-50%);border-radius:50%;width:72px;height:72px;line-height:72px;border:2px solid rgba(255,255,255,0.8);background:rgba(0,0,0,0.5);backdrop-filter:blur(10px)}}
.vjs-control-bar{{background:linear-gradient(transparent,rgba(0,0,0,0.7))}}
</style>
</head>
<body>
<div id="player-container">
  <div id="title-bar">{html.escape(title)}</div>
  <div id="branding">CineAPI · by SAGE</div>
  <video id="cineapi-player" class="video-js vjs-default-skin vjs-big-play-centered" controls preload="auto" {"data-poster='"+poster+"'" if poster else ""}>
    {"<source src='"+html.escape(stream_url)+"' type='"+(  "application/x-mpegURL" if ".m3u8" in stream_url else "video/mp4")+"'/>" if stream_url else ""}
    {"<track kind='subtitles' src='"+html.escape(subtitle_url)+"' srclang='en' label='English' default/>" if subtitle_url else ""}
  </video>
  <div id="controls-bar">
    <select id="source-select" onchange="switchSource(this.value)">
      <option value="">— Source —</option>
    </select>
    <select id="sub-select" onchange="switchSubtitle(this.value)">
      <option value="">— Subtitles —</option>
      <option value="off">Off</option>
    </select>
  </div>
</div>
<script>
const player = videojs('cineapi-player', {{
  html5: {{ hls: {{ overrideNative: true }} }},
  controls: true,
  autoplay: false,
  preload: 'auto',
  fluid: false,
}});
const streams = {streams_json};
const subtitles = {subtitles_json};
const sourceSelect = document.getElementById('source-select');
const subSelect = document.getElementById('sub-select');
streams.forEach((s, i) => {{
  const opt = document.createElement('option');
  opt.value = i;
  opt.textContent = (s.provider || 'Unknown') + ' · ' + (s.quality || 'auto') + ' · ' + (s.format || '').toUpperCase();
  if (i === 0) opt.selected = true;
  sourceSelect.appendChild(opt);
}});
subtitles.forEach((s, i) => {{
  const opt = document.createElement('option');
  opt.value = s.file_id || i;
  opt.textContent = (s.language || 'Unknown').toUpperCase() + ' — ' + (s.file_name || '').substring(0, 30);
  subSelect.appendChild(opt);
}});
function switchSource(idx) {{
  if (idx === '') return;
  const s = streams[parseInt(idx)];
  if (!s) return;
  const type = s.format === 'm3u8' ? 'application/x-mpegURL' : 'video/mp4';
  player.src({{ type: type, src: s.url }});
  player.play();
}}
function switchSubtitle(fileId) {{
  if (!fileId || fileId === 'off') return;
  const url = '/api/v1/subtitles/download/' + fileId;
  const tracks = player.remoteTextTracks();
  while (tracks.length > 0) player.removeRemoteTextTrack(tracks[0]);
  player.addRemoteTextTrack({{ kind: 'subtitles', src: url, srclang: 'en', label: 'Subtitle', default: true }}, false);
}}
</script>
</body>
</html>"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"{PROJECT_NAME} by {PROJECT_AUTHOR} / {PROJECT_COMPANY} starting...")
    yield
    cache_cleanup()
    logger.info(f"{PROJECT_NAME} shutting down.")


app = FastAPI(
    title=PROJECT_NAME,
    description=f"Streaming Embed & Source API — Built by {PROJECT_AUTHOR} under {PROJECT_COMPANY}",
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
.logo{{font-size:48px;font-weight:800;background:linear-gradient(135deg,#e50914,#ff6b35);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px;letter-spacing:-1px}}
.tagline{{color:rgba(255,255,255,0.5);font-size:14px;margin-bottom:48px;letter-spacing:2px;text-transform:uppercase}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;max-width:900px;width:100%;margin-bottom:48px}}
.card{{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:20px}}
.card h3{{font-size:13px;color:rgba(255,255,255,0.4);text-transform:uppercase;letter-spacing:1px;margin-bottom:12px}}
.endpoint{{font-family:'Courier New',monospace;font-size:12px;color:#e50914;padding:6px 10px;background:rgba(229,9,20,0.08);border-radius:6px;margin-bottom:6px;display:block}}
.footer{{color:rgba(255,255,255,0.2);font-size:12px;letter-spacing:1px}}
.footer span{{color:rgba(255,255,255,0.4)}}
</style>
</head>
<body>
<div class="logo">{PROJECT_NAME}</div>
<div class="tagline">Streaming API · v{PROJECT_VERSION}</div>
<div class="grid">
  <div class="card">
    <h3>Movies</h3>
    <code class="endpoint">GET /api/v1/meta/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/stream/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/sources/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /embed/movie/{{imdb_id}}</code>
  </div>
  <div class="card">
    <h3>TV Shows</h3>
    <code class="endpoint">GET /api/v1/meta/tv/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/stream/tv/{{imdb_id}}/{{s}}/{{e}}</code>
    <code class="endpoint">GET /api/v1/sources/tv/{{imdb_id}}/{{s}}/{{e}}</code>
    <code class="endpoint">GET /embed/tv/{{imdb_id}}/{{s}}/{{e}}</code>
  </div>
  <div class="card">
    <h3>Anime</h3>
    <code class="endpoint">GET /api/v1/meta/anime/{{anilist_id}}</code>
    <code class="endpoint">GET /api/v1/stream/anime/{{anilist_id}}/{{ep}}</code>
    <code class="endpoint">GET /api/v1/sources/anime/{{anilist_id}}/{{ep}}</code>
    <code class="endpoint">GET /embed/anime/{{anilist_id}}/{{ep}}</code>
  </div>
  <div class="card">
    <h3>Search & Discover</h3>
    <code class="endpoint">GET /api/v1/search?q={{query}}&type=movie</code>
    <code class="endpoint">GET /api/v1/search?q={{query}}&type=tv</code>
    <code class="endpoint">GET /api/v1/search?q={{query}}&type=anime</code>
    <code class="endpoint">GET /api/v1/trending</code>
  </div>
  <div class="card">
    <h3>Subtitles</h3>
    <code class="endpoint">GET /api/v1/subtitles/movie/{{imdb_id}}</code>
    <code class="endpoint">GET /api/v1/subtitles/tv/{{imdb_id}}/{{s}}/{{e}}</code>
    <code class="endpoint">GET /api/v1/subtitles/download/{{file_id}}</code>
  </div>
  <div class="card">
    <h3>System</h3>
    <code class="endpoint">GET /health</code>
    <code class="endpoint">GET /api/v1/providers</code>
    <code class="endpoint">GET /docs</code>
  </div>
</div>
<div class="footer">Built by <span>{PROJECT_AUTHOR}</span> · <span>{PROJECT_COMPANY}</span> · {PROJECT_NAME} {PROJECT_VERSION}</div>
</body>
</html>""")


@app.get("/health")
async def health():
    return {"status": "ok", "project": PROJECT_NAME, "version": PROJECT_VERSION, "author": PROJECT_AUTHOR, "company": PROJECT_COMPANY, "cache": {"streams": len(_stream_cache), "meta": len(_meta_cache), "search": len(_search_cache)}}


@app.get("/api/v1/providers")
async def get_providers():
    return {
        "status": "ok",
        "providers": [
            {"name": "FlixHQ", "type": ["movie", "tv"], "status": "active"},
            {"name": "HDRezka", "type": ["movie", "tv"], "status": "active"},
            {"name": "LookMovie", "type": ["movie", "tv"], "status": "active"},
            {"name": "SuperEmbed", "type": ["movie", "tv"], "status": "active"},
            {"name": "EmbedSu", "type": ["movie", "tv"], "status": "active"},
            {"name": "AutoEmbed", "type": ["movie", "tv"], "status": "active"},
            {"name": "SmashyStream", "type": ["movie", "tv"], "status": "active"},
            {"name": "2Embed", "type": ["movie", "tv"], "status": "active"},
            {"name": "VidSrcPro", "type": ["movie", "tv"], "status": "active"},
            {"name": "KissAsian", "type": ["tv"], "status": "active"},
            {"name": "HiAnime/Zoro", "type": ["anime"], "status": "active"},
            {"name": "9Anime", "type": ["anime"], "status": "active"},
            {"name": "GogoAnime", "type": ["anime"], "status": "active"},
        ],
        "hosts": ["Filemoon", "Vidplay", "StreamTape", "DoodStream", "Upstream", "MixDrop", "Mp4Upload", "Fembed", "OK.ru", "VOE", "VidHide"],
    }


@app.get("/api/v1/meta/movie/{imdb_id}")
async def meta_movie(imdb_id: str):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    data = await get_tmdb_movie_meta(imdb_id)
    if not data:
        raise HTTPException(status_code=404, detail="Movie not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/tv/{imdb_id}")
async def meta_tv(imdb_id: str):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    data = await get_tmdb_tv_meta(imdb_id)
    if not data:
        raise HTTPException(status_code=404, detail="TV show not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/meta/anime/{anilist_id}")
async def meta_anime(anilist_id: int):
    data = await get_anilist_meta(anilist_id)
    if not data:
        raise HTTPException(status_code=404, detail="Anime not found")
    return {"status": "ok", "data": data}


@app.get("/api/v1/stream/movie/{imdb_id}")
async def stream_movie(imdb_id: str):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    streams = await resolve_movie_streams(imdb_id)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    meta = await get_tmdb_movie_meta(imdb_id)
    return {"status": "ok", "imdb_id": imdb_id, "title": meta.get("title"), "year": meta.get("year"), "streams": streams, "total": len(streams)}


@app.get("/api/v1/stream/tv/{imdb_id}/{season}/{episode}")
async def stream_tv(imdb_id: str, season: int, episode: int):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    streams = await resolve_tv_streams(imdb_id, season, episode)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    meta = await get_tmdb_tv_meta(imdb_id)
    return {"status": "ok", "imdb_id": imdb_id, "title": meta.get("title"), "season": season, "episode": episode, "streams": streams, "total": len(streams)}


@app.get("/api/v1/stream/anime/{anilist_id}/{episode}")
async def stream_anime(anilist_id: int, episode: int):
    streams = await resolve_anime_streams(anilist_id, episode)
    if not streams:
        raise HTTPException(status_code=404, detail="No streams found")
    meta = await get_anilist_meta(anilist_id)
    return {"status": "ok", "anilist_id": anilist_id, "title": meta.get("title_english") or meta.get("title_romaji"), "episode": episode, "streams": streams, "total": len(streams)}


@app.get("/api/v1/sources/movie/{imdb_id}")
async def sources_movie(imdb_id: str):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    streams = await resolve_movie_streams(imdb_id)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        if p not in providers:
            providers[p] = []
        providers[p].append(s)
    return {"status": "ok", "imdb_id": imdb_id, "providers": providers, "total": len(streams)}


@app.get("/api/v1/sources/tv/{imdb_id}/{season}/{episode}")
async def sources_tv(imdb_id: str, season: int, episode: int):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="imdb_id must start with 'tt'")
    streams = await resolve_tv_streams(imdb_id, season, episode)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        if p not in providers:
            providers[p] = []
        providers[p].append(s)
    return {"status": "ok", "imdb_id": imdb_id, "season": season, "episode": episode, "providers": providers, "total": len(streams)}


@app.get("/api/v1/sources/anime/{anilist_id}/{episode}")
async def sources_anime(anilist_id: int, episode: int):
    streams = await resolve_anime_streams(anilist_id, episode)
    providers = {}
    for s in streams:
        p = s.get("provider", "Unknown")
        if p not in providers:
            providers[p] = []
        providers[p].append(s)
    return {"status": "ok", "anilist_id": anilist_id, "episode": episode, "providers": providers, "total": len(streams)}


@app.get("/api/v1/search")
async def search(q: str = Query(..., min_length=1), type: str = Query("movie"), page: int = Query(1, ge=1)):
    cache_key = f"search_{q}_{type}_{page}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "query": q, "type": type, "page": page, "results": cached, "total": len(cached)}
    results = []
    if type == "movie":
        results = await tmdb_search_movies(q, page)
    elif type == "tv":
        results = await tmdb_search_tv(q, page)
    elif type == "anime":
        results = await search_anilist(q, page)
    elif type == "all":
        movie_results, tv_results, anime_results = await asyncio.gather(
            tmdb_search_movies(q, page),
            tmdb_search_tv(q, page),
            search_anilist(q, page),
        )
        results = movie_results[:5] + tv_results[:5] + anime_results[:5]
    else:
        raise HTTPException(status_code=400, detail="type must be one of: movie, tv, anime, all")
    cache_set(_search_cache, cache_key, results, CACHE_TTL_SEARCH)
    return {"status": "ok", "query": q, "type": type, "page": page, "results": results, "total": len(results)}


@app.get("/api/v1/trending")
async def trending(type: str = Query("all"), window: str = Query("week")):
    cache_key = f"trending_{type}_{window}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    if type == "anime":
        data = await get_anilist_trending()
    else:
        data = await tmdb_trending(type, window)
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "data": data}


@app.get("/api/v1/popular")
async def popular(type: str = Query("movie")):
    cache_key = f"popular_{type}"
    cached = cache_get(_search_cache, cache_key)
    if cached:
        return {"status": "ok", "data": cached}
    if type == "anime":
        data = await get_anilist_trending(page=1)
    elif type == "movie":
        raw = await tmdb_get("/movie/popular")
        data = [{"tmdb_id": m["id"], "title": m.get("title"), "year": m.get("release_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "type": "movie"} for m in raw.get("results", [])[:20]]
    elif type == "tv":
        raw = await tmdb_get("/tv/popular")
        data = [{"tmdb_id": m["id"], "title": m.get("name"), "year": m.get("first_air_date", "")[:4], "rating": m.get("vote_average", 0), "poster": f"{TMDB_IMAGE_BASE}{m['poster_path']}" if m.get("poster_path") else None, "type": "tv"} for m in raw.get("results", [])[:20]]
    else:
        data = []
    cache_set(_search_cache, cache_key, data, CACHE_TTL_SEARCH)
    return {"status": "ok", "data": data}


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


@app.get("/embed/movie/{imdb_id}", response_class=HTMLResponse)
async def embed_movie(imdb_id: str):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    meta_task = get_tmdb_movie_meta(imdb_id)
    streams_task = resolve_movie_streams(imdb_id)
    meta, streams = await asyncio.gather(meta_task, streams_task)
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for {imdb_id}</p></body></html>", status_code=404)
    title = meta.get("title", imdb_id)
    poster = meta.get("backdrop") or meta.get("poster")
    best_stream = streams[0]
    subs = await search_subtitles(imdb_id)
    sub_url = None
    if subs:
        sub_url = f"/api/v1/subtitles/download/{subs[0]['file_id']}"
    return HTMLResponse(content=build_player_html(title, best_stream["url"], sub_url, poster, streams, subs))


@app.get("/embed/tv/{imdb_id}/{season}/{episode}", response_class=HTMLResponse)
async def embed_tv(imdb_id: str, season: int, episode: int):
    if not imdb_id.startswith("tt"):
        raise HTTPException(status_code=400, detail="Invalid IMDb ID")
    meta_task = get_tmdb_tv_meta(imdb_id)
    streams_task = resolve_tv_streams(imdb_id, season, episode)
    meta, streams = await asyncio.gather(meta_task, streams_task)
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for S{season:02d}E{episode:02d}</p></body></html>", status_code=404)
    show_title = meta.get("title", imdb_id)
    title = f"{show_title} — S{season:02d}E{episode:02d}"
    poster = meta.get("backdrop") or meta.get("poster")
    best_stream = streams[0]
    subs = await search_subtitles(imdb_id, season=season, episode=episode)
    sub_url = None
    if subs:
        sub_url = f"/api/v1/subtitles/download/{subs[0]['file_id']}"
    return HTMLResponse(content=build_player_html(title, best_stream["url"], sub_url, poster, streams, subs))


@app.get("/embed/anime/{anilist_id}/{episode}", response_class=HTMLResponse)
async def embed_anime(anilist_id: int, episode: int):
    meta_task = get_anilist_meta(anilist_id)
    streams_task = resolve_anime_streams(anilist_id, episode)
    meta, streams = await asyncio.gather(meta_task, streams_task)
    if not streams:
        return HTMLResponse(content=f"<html><body style='background:#000;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif'><p>No streams available for episode {episode}</p></body></html>", status_code=404)
    anime_title = meta.get("title_english") or meta.get("title_romaji", str(anilist_id))
    title = f"{anime_title} — Episode {episode}"
    poster = meta.get("banner") or meta.get("cover")
    best_stream = streams[0]
    return HTMLResponse(content=build_player_html(title, best_stream["url"], None, poster, streams, []))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
