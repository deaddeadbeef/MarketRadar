"""Offline x-posts-v1 → world-events-v1 transform. Zero provider calls."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.discovery.models import (
    WORLD_EVENTS_SCHEMA,
    clamp_score,
    normalize_themes,
    normalize_tickers,
    parse_datetime,
)

POSTS_SCHEMA = "x-posts-v1"
FROM_POSTS_SCHEMA = "discovery-from-posts-v1"
CASHTAG = re.compile(r"\$([A-Za-z]{1,6})\b")


def load_x_posts(path: str | Path) -> dict[str, object]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("posts file must be a JSON object")
    schema = str(raw.get("schema_version") or POSTS_SCHEMA)
    if schema != POSTS_SCHEMA:
        raise ValueError(f"unsupported posts schema_version: {schema}")
    posts = raw.get("posts")
    if not isinstance(posts, list) or not posts:
        raise ValueError("posts file must include a non-empty posts list")
    return {
        "schema_version": schema,
        "generated_at": parse_datetime(raw.get("generated_at"), "generated_at"),
        "source": str(raw.get("source") or "x_posts"),
        "posts": tuple(_parse_post(item, index) for index, item in enumerate(posts)),
    }


def build_world_events_from_posts(
    *,
    posts_path: str | Path,
    now: datetime | None = None,
) -> dict[str, object]:
    bundle = load_x_posts(posts_path)
    clock = now if now is not None else datetime.now(tz=UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)
    else:
        clock = clock.astimezone(UTC)

    groups: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for post in bundle["posts"]:
        groups[str(post["group_key"])].append(post)

    events: list[dict[str, object]] = []
    for group_key, posts in groups.items():
        tickers: list[str] = []
        themes: list[str] = []
        sources: list[dict[str, object]] = []
        directions: list[str] = []
        materiality_values: list[float] = []
        quality_values: list[float] = []
        available_at = posts[0]["published_at"]
        for post in posts:
            for ticker in post["tickers"]:
                if ticker not in tickers:
                    tickers.append(ticker)
            for theme in post["themes"]:
                if theme not in themes:
                    themes.append(theme)
            sources.append(post["source"])
            directions.append(str(post["direction"]))
            materiality_values.append(float(post["materiality"]))
            quality_values.append(float(post["source_quality"]))
            published = post["published_at"]
            if isinstance(published, datetime) and (
                not isinstance(available_at, datetime) or published < available_at
            ):
                available_at = published
        if not tickers and not themes:
            continue
        direction = _majority_direction(directions)
        # Title/summary come from the most material post, not a tweet mashup.
        lead = max(
            posts,
            key=lambda post: (float(post["materiality"]), len(str(post["title"]))),
        )
        lead_text = str(lead["text"] or "").strip()
        events.append(
            {
                "id": f"evt_{_slug(group_key)}",
                "title": str(lead["title"])[:180],
                "summary": (lead_text or str(lead["title"]))[:800],
                "themes": themes,
                "tickers": tickers[:8],
                "secondary_tickers": tickers[8:16],
                "direction": direction,
                "materiality": round(sum(materiality_values) / len(materiality_values), 3),
                "source_quality": round(sum(quality_values) / len(quality_values), 3),
                "source_category": "social",
                "sources": sources[:8],
                "available_at": (
                    available_at.isoformat()
                    if isinstance(available_at, datetime)
                    else clock.isoformat()
                ),
            }
        )

    payload = {
        "schema_version": WORLD_EVENTS_SCHEMA,
        "generated_at": clock.isoformat(),
        "source": f"from_posts:{bundle['source']}",
        "events": events,
    }
    return payload


def convert_posts_file(
    *,
    posts_path: str | Path,
    destination: str | Path,
    execute: bool = False,
    now: datetime | None = None,
) -> dict[str, object]:
    events = build_world_events_from_posts(posts_path=posts_path, now=now)
    dest = Path(destination)
    writes = 0
    if execute:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(events, indent=2), encoding="utf-8")
        writes = 1
    return {
        "schema_version": FROM_POSTS_SCHEMA,
        "status": "executed" if execute else "preview",
        "posts_path": str(posts_path),
        "destination": str(dest),
        "event_count": len(events.get("events") or []),
        "events_source": events.get("source"),
        "generated_at": events.get("generated_at"),
        "file_writes_made": writes,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "investment_advice": False,
        "world_events": events if not execute else None,
        "next_action": (
            f"Install completed. Run discovery-brief --events {dest}."
            if execute
            else f"Preview only. Re-run with --execute to write {dest}."
        ),
    }


def _parse_post(raw: object, index: int) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"posts[{index}] must be an object")
    text = str(raw.get("text") or raw.get("summary") or "").strip()
    title = str(raw.get("title") or text[:120] or f"Untitled post {index + 1}").strip()
    post_id = str(raw.get("id") or f"post_{index + 1}").strip()
    tickers = list(normalize_tickers(raw.get("tickers")))
    for match in CASHTAG.findall(text):
        symbol = match.upper()
        if symbol not in tickers:
            tickers.append(symbol)
    themes = list(normalize_themes(raw.get("themes")))
    group_key = str(raw.get("event_id") or (themes[0] if themes else post_id)).strip()
    published = parse_datetime(raw.get("published_at") or raw.get("available_at"), "published_at")
    engagement = raw.get("engagement") if isinstance(raw.get("engagement"), Mapping) else {}
    likes = _as_int(engagement.get("likes"))
    views = _as_int(engagement.get("views"))
    materiality = clamp_score(raw.get("materiality"), default=_materiality(likes, views))
    source_quality = clamp_score(raw.get("source_quality"), default=0.32)
    direction = str(raw.get("direction") or "mixed").casefold()
    if direction not in {"bullish", "bearish", "mixed"}:
        direction = "mixed"
    return {
        "id": post_id,
        "group_key": group_key,
        "title": title,
        "text": text,
        "tickers": tickers,
        "themes": themes,
        "direction": direction,
        "materiality": materiality,
        "source_quality": source_quality,
        "published_at": published,
        "source": {
            "provider": str(raw.get("provider") or "x"),
            "url": raw.get("url"),
            "author": raw.get("author"),
            "published_at": published.isoformat(),
            "engagement": {"likes": likes, "views": views},
        },
    }


def _materiality(likes: int, views: int) -> float:
    score = 0.28 + min(0.4, likes / 500.0) + min(0.2, views / 100_000.0)
    return max(0.2, min(0.85, score))


def _majority_direction(values: Sequence[str]) -> str:
    counts = {"bullish": 0, "bearish": 0, "mixed": 0}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    ranked = sorted(counts, key=lambda key: (-counts[key], key))
    return ranked[0] if counts[ranked[0]] else "mixed"


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    return text[:48] or "event"


def _as_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0
