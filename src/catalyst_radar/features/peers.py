from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from catalyst_radar.features.theme import ThemePeerConfig

PEER_FEATURE_VERSION = "peer-v1"


@dataclass(frozen=True)
class PeerReadthroughScore:
    ticker: str
    theme_id: str
    score: float
    peers: tuple[str, ...]
    source_theme_hits: tuple[Mapping[str, Any], ...]


def peer_readthrough_score(
    ticker: str,
    source_theme_hits: object,
    config: ThemePeerConfig,
) -> PeerReadthroughScore:
    normalized_ticker = str(ticker).upper()
    hits = _theme_hits(source_theme_hits)
    best_theme = ""
    best_score = 0.0
    best_peers: tuple[str, ...] = ()
    for hit in hits:
        theme_id = str(hit.get("theme_id", ""))
        theme = config.themes.get(theme_id)
        if theme is None:
            continue
        if normalized_ticker not in theme.tickers and normalized_ticker not in theme.peers:
            continue
        evidence_count = _finite_float(hit.get("count", 0.0))
        term_count = len(hit.get("terms", ())) if isinstance(hit.get("terms"), Sequence) else 0
        score = min(100.0, (evidence_count * 25.0) + (term_count * 10.0))
        if score > best_score:
            best_theme = theme_id
            best_score = score
            best_peers = tuple(peer for peer in theme.peers if peer != normalized_ticker)
    return PeerReadthroughScore(
        ticker=normalized_ticker,
        theme_id=best_theme,
        score=round(best_score, 2),
        peers=best_peers,
        source_theme_hits=hits,
    )


def _theme_hits(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _finite_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


__all__ = ["PEER_FEATURE_VERSION", "PeerReadthroughScore", "peer_readthrough_score"]
