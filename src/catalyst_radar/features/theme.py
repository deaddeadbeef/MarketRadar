from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from catalyst_radar.textint.models import TextFeature
from catalyst_radar.textint.ontology import _parse_yaml_subset

THEME_FEATURE_VERSION = "theme-v1"


@dataclass(frozen=True)
class ThemeDefinition:
    theme_id: str
    sectors: tuple[str, ...]
    industries: tuple[str, ...]
    tickers: tuple[str, ...]
    peers: tuple[str, ...]


@dataclass(frozen=True)
class ThemePeerConfig:
    themes: Mapping[str, ThemeDefinition]


def load_theme_peer_config(path: Path | str = Path("config/theme_peers.yaml")) -> ThemePeerConfig:
    parsed = _parse_yaml_subset(Path(path).read_text(encoding="utf-8"))
    raw_themes = parsed.get("themes")
    if not isinstance(raw_themes, Mapping) or not raw_themes:
        msg = "theme peer config must contain a non-empty themes mapping"
        raise ValueError(msg)
    themes = {
        str(theme_id): _theme_definition(str(theme_id), raw_theme)
        for theme_id, raw_theme in sorted(raw_themes.items())
    }
    return ThemePeerConfig(themes=themes)


def theme_for_security(
    ticker: str,
    sector: str,
    industry: str,
    metadata: object,
    config: ThemePeerConfig,
) -> str:
    normalized_ticker = str(ticker).upper()
    metadata_theme = _metadata_theme(metadata)
    if metadata_theme in config.themes:
        return metadata_theme
    for theme_id, theme in config.themes.items():
        if normalized_ticker in theme.tickers:
            return theme_id
    normalized_sector = str(sector).casefold()
    normalized_industry = str(industry).casefold()
    for theme_id, theme in config.themes.items():
        if normalized_industry in {value.casefold() for value in theme.industries}:
            return theme_id
        if normalized_sector in {value.casefold() for value in theme.sectors}:
            return theme_id
    return str(metadata_theme or industry or sector or "unknown")


def theme_velocity_score(text_feature: TextFeature | None, theme_id: str) -> float:
    if text_feature is None or not theme_id:
        return 0.0
    theme_count = 0.0
    for hit in _theme_hits(text_feature.theme_hits):
        if str(hit.get("theme_id")) == theme_id:
            theme_count += _finite_float(hit.get("count", 0.0))
    if theme_count <= 0:
        return 0.0
    score = (
        (text_feature.theme_match_score * 0.45)
        + (text_feature.local_narrative_score * 0.30)
        + (text_feature.novelty_score * 0.15)
        + min(10.0, theme_count * 5.0)
    )
    return round(_clamp(score, 0.0, 100.0), 2)


def _theme_definition(theme_id: str, value: object) -> ThemeDefinition:
    if not isinstance(value, Mapping):
        msg = f"theme {theme_id} must be a mapping"
        raise ValueError(msg)
    return ThemeDefinition(
        theme_id=theme_id,
        sectors=_string_tuple(value.get("sectors")),
        industries=_string_tuple(value.get("industries")),
        tickers=tuple(item.upper() for item in _string_tuple(value.get("tickers"))),
        peers=tuple(item.upper() for item in _string_tuple(value.get("peers"))),
    )


def _metadata_theme(metadata: object) -> str:
    if isinstance(metadata, Mapping):
        value = metadata.get("theme")
        if value:
            return str(value)
    return ""


def _theme_hits(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if isinstance(value, Sequence):
        return tuple(str(item).strip() for item in value if str(item).strip())
    msg = "theme config values must be strings or lists"
    raise ValueError(msg)


def _finite_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, _finite_float(value)))


__all__ = [
    "THEME_FEATURE_VERSION",
    "ThemeDefinition",
    "ThemePeerConfig",
    "load_theme_peer_config",
    "theme_for_security",
    "theme_velocity_score",
]
