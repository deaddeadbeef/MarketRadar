"""Offline X post search results → world-events-v1 transform.

Pure local transform: no network, no provider calls. Social sources always stay
research_only downstream (discovery brief usefulness gate).
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from catalyst_radar.discovery.mapper import DEFAULT_THEME_TICKERS, load_theme_ticker_map
from catalyst_radar.discovery.models import (
    WORLD_EVENTS_SCHEMA,
    clamp_score,
    normalize_tickers,
    parse_datetime,
)

X_POSTS_SCHEMA = "x-posts-v1"
DEFAULT_OUTPUT_PATH = Path("data/local/world_events.json")

_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5})\b")

# Keyword → theme keys aligned with DEFAULT_THEME_TICKERS.
_THEME_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "semiconductor",
        (
            "semiconductor",
            "semiconductors",
            "chip",
            "chips",
            "foundry",
            "wafer",
            "asml",
            "tsmc",
            "nvidia",
            "nvda",
            "gpu",
            "hbm",
            "dram",
            "fab",
        ),
    ),
    (
        "memory",
        ("memory", "dram", "nand", "hbm", "micron", "sk hynix", "skhynix"),
    ),
    (
        "ai_infrastructure",
        (
            "ai infrastructure",
            "ai infra",
            "datacenter",
            "data center",
            "gpu cluster",
            "inference",
            "training cluster",
            "ai demand",
        ),
    ),
    (
        "ai_interconnect",
        ("interconnect", "optical", "coherent", "photonics", "ethernet ai"),
    ),
    (
        "energy_security",
        (
            "energy",
            "oil",
            "crude",
            "opec",
            "hormuz",
            "brent",
            "wti",
            "refiner",
            "lng",
            "natural gas",
        ),
    ),
    (
        "shipping",
        ("shipping", "tanker", "freight", "red sea", "suez", "baltic dry", "container"),
    ),
    (
        "defense",
        ("defense", "defence", "missile", "pentagon", "military", "aerospace defense"),
    ),
    (
        "onshoring",
        ("onshoring", "reshoring", "friend-shoring", "friendshoring", "domestic supply"),
    ),
    (
        "datacenter_power",
        ("datacenter power", "grid power", "power demand", "electrical infrastructure"),
    ),
    (
        "tariffs",
        ("tariff", "tariffs", "trade war", "import duty", "customs duty"),
    ),
    (
        "china_export",
        ("china export", "chinese export", "overcapacity", "ev export", "solar export"),
    ),
    (
        "gold",
        ("gold", "bullion", "safe haven", "gld"),
    ),
)


def load_x_posts(path: str | Path) -> dict[str, Any]:
    """Load an x-posts-v1 JSON fixture (object with posts list, or bare list)."""
    file_path = Path(path)
    raw = json.loads(file_path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return {
            "schema_version": X_POSTS_SCHEMA,
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "query": None,
            "posts": raw,
        }
    if not isinstance(raw, Mapping):
        msg = "x posts file must be a JSON object or list"
        raise ValueError(msg)
    schema = str(raw.get("schema_version") or X_POSTS_SCHEMA)
    if schema != X_POSTS_SCHEMA:
        msg = f"unsupported x posts schema_version: {schema}"
        raise ValueError(msg)
    posts = raw.get("posts")
    if not isinstance(posts, list):
        msg = "x posts file must include a posts list"
        raise ValueError(msg)
    return {
        "schema_version": schema,
        "generated_at": raw.get("generated_at"),
        "query": raw.get("query"),
        "posts": posts,
        "source": raw.get("source"),
    }


def extract_cashtags(text: object) -> list[str]:
    """Extract $TICKER cashtags from free text (1–5 letter symbols)."""
    if text is None:
        return []
    found = _CASHTAG_RE.findall(str(text))
    return list(normalize_tickers(found))


def detect_themes(text: object, *, theme_map: Mapping[str, Sequence[str]] | None = None) -> list[str]:
    """Detect theme keys from post text using keyword map + optional ticker map keys."""
    blob = str(text or "").casefold()
    if not blob.strip():
        return []
    hits: list[str] = []
    for theme, keywords in _THEME_KEYWORDS:
        if any(keyword in blob for keyword in keywords):
            hits.append(theme)
    # Honor custom theme map keys as loose keyword matches when present.
    if theme_map:
        for theme in theme_map:
            key = str(theme).casefold().replace(" ", "_")
            if key in hits:
                continue
            needle = key.replace("_", " ")
            if needle and needle in blob:
                hits.append(key)
    return list(dict.fromkeys(hits))


def _engagement_numbers(engagement: object) -> tuple[float, float, float]:
    if not isinstance(engagement, Mapping):
        return 0.0, 0.0, 0.0
    likes = _as_float(engagement.get("likes") or engagement.get("like_count") or 0)
    views = _as_float(engagement.get("views") or engagement.get("view_count") or 0)
    reposts = _as_float(
        engagement.get("reposts")
        or engagement.get("retweets")
        or engagement.get("repost_count")
        or 0
    )
    return likes, views, reposts


def _as_float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def materiality_from_engagement(engagement: object) -> float:
    """Map likes/views into 0–1 materiality (clamped)."""
    likes, views, reposts = _engagement_numbers(engagement)
    # Log-ish saturation so viral posts approach 1 without needing extreme caps.
    like_score = min(1.0, likes / 500.0)
    view_score = min(1.0, views / 100_000.0)
    repost_score = min(1.0, reposts / 100.0)
    score = (like_score * 0.45) + (view_score * 0.40) + (repost_score * 0.15)
    if likes <= 0 and views <= 0 and reposts <= 0:
        score = 0.25
    return clamp_score(score, 0.25)


def source_quality_from_engagement(engagement: object) -> float:
    """Social source quality stays low (0.20–0.45)."""
    likes, views, _reposts = _engagement_numbers(engagement)
    base = 0.20
    bump = 0.0
    if likes >= 50 or views >= 10_000:
        bump += 0.10
    if likes >= 200 or views >= 50_000:
        bump += 0.10
    if likes >= 500 or views >= 200_000:
        bump += 0.05
    return clamp_score(base + bump, 0.20)


def _post_text(post: Mapping[str, Any]) -> str:
    for key in ("text", "full_text", "content", "body"):
        value = post.get(key)
        if value:
            return str(value)
    return ""


def _post_id(post: Mapping[str, Any], index: int) -> str:
    for key in ("id", "post_id", "tweet_id", "status_id"):
        value = post.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return f"post_{index}"


def _post_author(post: Mapping[str, Any]) -> str | None:
    for key in ("author", "username", "user", "handle"):
        value = post.get(key)
        if isinstance(value, Mapping):
            handle = value.get("username") or value.get("screen_name") or value.get("name")
            if handle:
                text = str(handle).strip()
                return text if text.startswith("@") else f"@{text}"
        if value is not None and str(value).strip():
            text = str(value).strip()
            return text if text.startswith("@") else f"@{text}"
    return None


def _post_url(post: Mapping[str, Any], post_id: str, author: str | None) -> str | None:
    url = post.get("url") or post.get("permalink")
    if url:
        return str(url)
    if author:
        handle = author.lstrip("@")
        return f"https://x.com/{handle}/status/{post_id}"
    return f"https://x.com/i/web/status/{post_id}"


def _post_published_at(post: Mapping[str, Any]) -> datetime | None:
    for key in ("published_at", "created_at", "timestamp"):
        if post.get(key) is not None and str(post.get(key)).strip():
            try:
                return parse_datetime(post.get(key), key)
            except ValueError:
                continue
    return None


def _direction_from_text(text: str) -> str:
    blob = text.casefold()
    bull = sum(
        1
        for token in ("bullish", "breakout", "surge", "rally", "upgrade", "buy")
        if token in blob
    )
    bear = sum(
        1
        for token in ("bearish", "crash", "selloff", "sell-off", "downgrade", "collapse")
        if token in blob
    )
    if bull > bear and bull > 0:
        return "bullish"
    if bear > bull and bear > 0:
        return "bearish"
    return "mixed"


def posts_to_world_events(
    posts_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    query: str | None = None,
    generated_at: datetime | str | None = None,
    theme_map: Mapping[str, Sequence[str]] | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    """Transform x-posts payload into world-events-v1 (offline, zero external calls)."""
    if isinstance(posts_payload, Sequence) and not isinstance(posts_payload, (str, bytes)):
        posts_raw: list[Any] = list(posts_payload)
        payload_query = query
        payload_generated = generated_at
        source_label = "x_search_offline"
    else:
        body = dict(posts_payload)  # type: ignore[arg-type]
        posts_raw = list(body.get("posts") or [])
        payload_query = query if query is not None else body.get("query")
        payload_generated = generated_at if generated_at is not None else body.get("generated_at")
        source_label = str(body.get("source") or "x_search_offline")

    themes_map = theme_map or DEFAULT_THEME_TICKERS
    if limit is not None and limit >= 0:
        posts_raw = posts_raw[:limit]

    # Group posts by primary theme (or general_market).
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, item in enumerate(posts_raw):
        if not isinstance(item, Mapping):
            continue
        post = dict(item)
        text = _post_text(post)
        cashtags = extract_cashtags(text)
        # Also accept explicit tickers field on the post.
        explicit = list(normalize_tickers(post.get("tickers") or post.get("symbols")))
        tickers = list(dict.fromkeys([*cashtags, *explicit]))
        themes = detect_themes(text, theme_map=themes_map)
        if post.get("themes"):
            for theme in post.get("themes") or []:
                key = str(theme).casefold().replace(" ", "_")
                if key and key not in themes:
                    themes.append(key)
        primary_theme = themes[0] if themes else "general_market"
        engagement = post.get("engagement") if isinstance(post.get("engagement"), Mapping) else {}
        if not engagement:
            engagement = {
                "likes": post.get("likes") or post.get("favorite_count") or 0,
                "views": post.get("views") or post.get("view_count") or 0,
                "reposts": post.get("reposts") or post.get("retweet_count") or 0,
            }
        post_id = _post_id(post, index)
        author = _post_author(post)
        published = _post_published_at(post)
        groups[primary_theme].append(
            {
                "post_id": post_id,
                "text": text,
                "tickers": tickers,
                "themes": themes or [primary_theme],
                "engagement": dict(engagement),
                "author": author,
                "url": _post_url(post, post_id, author),
                "published_at": published,
                "direction": _direction_from_text(text),
                "materiality": materiality_from_engagement(engagement),
                "source_quality": source_quality_from_engagement(engagement),
            }
        )

    if isinstance(payload_generated, datetime):
        generated = payload_generated
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=UTC)
        else:
            generated = generated.astimezone(UTC)
    elif payload_generated:
        generated = parse_datetime(payload_generated, "generated_at")
    else:
        generated = datetime.now(tz=UTC)

    events: list[dict[str, object]] = []
    for theme, posts in groups.items():
        all_tickers: list[str] = []
        all_themes: list[str] = []
        sources: list[dict[str, object]] = []
        materialities: list[float] = []
        qualities: list[float] = []
        directions: list[str] = []
        summaries: list[str] = []
        latest_available = generated

        for post in posts:
            for ticker in post["tickers"]:
                if ticker not in all_tickers:
                    all_tickers.append(ticker)
            for t in post["themes"]:
                if t not in all_themes:
                    all_themes.append(t)
            materialities.append(float(post["materiality"]))
            qualities.append(float(post["source_quality"]))
            directions.append(str(post["direction"]))
            text = str(post["text"] or "").strip()
            if text:
                summaries.append(text[:280])
            published = post["published_at"]
            if isinstance(published, datetime):
                if published > latest_available:
                    latest_available = published
                published_iso = published.isoformat()
            else:
                published_iso = None
            sources.append(
                {
                    "provider": "x",
                    "url": post["url"],
                    "author": post["author"],
                    "published_at": published_iso,
                    "engagement": post["engagement"],
                }
            )

        # Map theme defaults when cashtags missing.
        theme_defaults = list(normalize_tickers(themes_map.get(theme, ())))
        for fallback_theme in all_themes:
            for ticker in normalize_tickers(themes_map.get(fallback_theme, ())):
                if ticker not in theme_defaults:
                    theme_defaults.append(ticker)
        if not all_tickers:
            all_tickers = theme_defaults[:8]
        secondary = [t for t in theme_defaults if t not in all_tickers][:8]
        primary = all_tickers[:8]
        if len(all_tickers) > 8:
            secondary = list(dict.fromkeys([*all_tickers[8:], *secondary]))[:8]

        avg_materiality = (
            sum(materialities) / len(materialities) if materialities else 0.35
        )
        avg_quality = sum(qualities) / len(qualities) if qualities else 0.25
        # Keep social quality in the research_only band.
        avg_quality = min(0.45, max(0.20, avg_quality))

        bull = directions.count("bullish")
        bear = directions.count("bearish")
        if bull > bear and bull > 0:
            direction = "bullish"
        elif bear > bull and bear > 0:
            direction = "bearish"
        else:
            direction = "mixed"

        theme_label = theme.replace("_", " ")
        title = f"X social cluster: {theme_label} ({len(posts)} posts)"
        if payload_query:
            title = f"X social cluster [{payload_query}]: {theme_label}"
        summary = " ".join(summaries[:3]).strip()
        if not summary:
            summary = (
                f"Offline X transform grouped {len(posts)} posts under theme '{theme}'. "
                "Social-only; research decision support only."
            )

        event_id = f"evt_x_{theme}_{generated.strftime('%Y%m%d')}"
        events.append(
            {
                "id": event_id,
                "title": title,
                "summary": summary[:1200],
                "themes": all_themes or [theme],
                "tickers": primary,
                "secondary_tickers": secondary,
                "direction": direction,
                "materiality": round(clamp_score(avg_materiality, 0.35), 4),
                "source_quality": round(clamp_score(avg_quality, 0.25), 4),
                "source_category": "social",
                "sources": sources[:12],
                "available_at": latest_available.isoformat(),
            }
        )

    # Stable order: higher materiality first.
    events.sort(key=lambda row: float(row.get("materiality") or 0), reverse=True)

    query_part = f" query={payload_query}" if payload_query else ""
    return {
        "schema_version": WORLD_EVENTS_SCHEMA,
        "generated_at": generated.isoformat(),
        "source": f"{source_label}{query_part}".strip(),
        "events": events,
        "meta": {
            "transform": "x_posts_to_world_events",
            "x_posts_schema": X_POSTS_SCHEMA,
            "post_count": sum(len(v) for v in groups.values()),
            "event_count": len(events),
            "query": payload_query,
            "investment_advice": False,
            "external_calls_made": 0,
        },
    }


def posts_to_world_events_payload(
    posts_path: str | Path,
    *,
    query: str | None = None,
    generated_at: datetime | str | None = None,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    limit: int | None = None,
) -> dict[str, object]:
    """Load posts from disk and transform to world-events-v1."""
    loaded = load_x_posts(posts_path)
    theme_map = load_theme_ticker_map(theme_peers_path)
    return posts_to_world_events(
        loaded,
        query=query if query is not None else loaded.get("query"),  # type: ignore[arg-type]
        generated_at=generated_at if generated_at is not None else loaded.get("generated_at"),
        theme_map=theme_map,
        limit=limit,
    )


def write_world_events_from_x_posts(
    posts_path: str | Path,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    *,
    execute: bool = False,
    query: str | None = None,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    limit: int | None = None,
) -> dict[str, object]:
    """Preview or write world-events JSON produced from X posts (offline)."""
    payload = posts_to_world_events_payload(
        posts_path,
        query=query,
        theme_peers_path=theme_peers_path,
        limit=limit,
    )
    dest = Path(output_path)
    result: dict[str, object] = {
        "schema_version": "discovery-from-x-result-v1",
        "mode": "execute" if execute else "preview",
        "posts_path": str(posts_path),
        "output_path": str(dest),
        "world_events": payload,
        "event_count": len(payload.get("events") or []),  # type: ignore[arg-type]
        "external_calls_made": 0,
        "db_writes_made": 0,
        "investment_advice": False,
        "decision_support_only": True,
        "written": False,
    }
    if execute:
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Write clean world-events-v1 without transform meta for brief compatibility.
        clean = {
            "schema_version": payload["schema_version"],
            "generated_at": payload["generated_at"],
            "source": payload["source"],
            "events": payload["events"],
        }
        dest.write_text(json.dumps(clean, indent=2, sort_keys=False) + "\n", encoding="utf-8")
        result["written"] = True
        result["db_writes_made"] = 0
        result["file_writes_made"] = 1
    return result


__all__ = [
    "DEFAULT_OUTPUT_PATH",
    "X_POSTS_SCHEMA",
    "detect_themes",
    "extract_cashtags",
    "load_x_posts",
    "materiality_from_engagement",
    "posts_to_world_events",
    "posts_to_world_events_payload",
    "source_quality_from_engagement",
    "write_world_events_from_x_posts",
]
