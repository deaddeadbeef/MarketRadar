from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from catalyst_radar.discovery.models import WorldEvent, normalize_tickers
from catalyst_radar.discovery.yaml_util import parse_yaml_subset

# Built-in second-order maps for common world-event themes when theme_peers is thin.
DEFAULT_THEME_TICKERS: dict[str, tuple[str, ...]] = {
    "energy_security": ("XOM", "CVX", "COP", "SLB", "HAL", "OXY", "BKR"),
    "shipping": ("FRO", "STNG", "DAC", "ZIM", "SBLK", "GOGL"),
    "diesel": ("XOM", "CVX", "VLO", "MPC", "PSX"),
    "geopolitics": ("LMT", "RTX", "NOC", "XOM", "GLD"),
    "defense": ("LMT", "RTX", "NOC", "GD", "HII", "LHX"),
    "semiconductor": ("NVDA", "TSM", "ASML", "AMAT", "LRCX", "KLAC", "MU", "AVGO"),
    "memory": ("MU", "WDC", "STX", "SNDK"),
    "hbm": ("MU", "NVDA", "TSM", "ASML"),
    "ai_infrastructure": ("NVDA", "AVGO", "MRVL", "ANET", "SMCI", "DELL"),
    "ai_interconnect": ("COHR", "LITE", "CRDO", "CIEN", "GLW"),
    "consumer_electronics": ("AAPL", "HPQ", "DELL", "SONY"),
    "onshoring": ("CAT", "DE", "ETN", "EMR", "PWR"),
    "policy": ("XOM", "CVX", "SLB", "CAT"),
    "datacenter_power": ("ETN", "VRT", "PWR", "HUBB", "CEG"),
    "tariffs": ("CAT", "DE", "UPS", "FDX"),
    "china_export": ("TSLA", "F", "GM", "ALB"),
    "gold": ("GLD", "NEM", "AEM"),
}

# Mega-caps often already reflect world narratives; demote when ranking lag.
MEGA_CAP_TICKERS: frozenset[str] = frozenset(
    {
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "GOOGL",
        "GOOG",
        "META",
        "TSLA",
        "BRK.B",
        "BRK.A",
        "JPM",
        "V",
        "MA",
    }
)


def load_theme_ticker_map(theme_peers_path: str | Path | None = None) -> dict[str, tuple[str, ...]]:
    mapping = {key: values for key, values in DEFAULT_THEME_TICKERS.items()}
    if theme_peers_path is None:
        return mapping
    path = Path(theme_peers_path)
    if not path.is_file():
        return mapping
    payload = parse_yaml_subset(path.read_text(encoding="utf-8")) or {}
    themes = payload.get("themes") if isinstance(payload, Mapping) else None
    if not isinstance(themes, Mapping):
        return mapping
    for theme_name, theme_body in themes.items():
        key = str(theme_name).casefold().replace(" ", "_")
        if not isinstance(theme_body, Mapping):
            continue
        tickers = normalize_tickers(theme_body.get("tickers"))
        peers = normalize_tickers(theme_body.get("peers"))
        combined = tuple(dict.fromkeys([*tickers, *peers]))
        if combined:
            existing = mapping.get(key, ())
            mapping[key] = tuple(dict.fromkeys([*existing, *combined]))
    return mapping


def map_event_tickers(
    event: WorldEvent,
    *,
    theme_ticker_map: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, object]:
    theme_map = theme_ticker_map or DEFAULT_THEME_TICKERS
    primary = list(event.tickers)
    secondary = list(event.secondary_tickers)
    theme_hits: list[str] = []
    for theme in event.themes:
        mapped = tuple(str(item).upper() for item in theme_map.get(theme, ()))
        if not mapped:
            continue
        theme_hits.append(theme)
        for ticker in mapped:
            if ticker in primary or ticker in secondary:
                continue
            if len(primary) < 8:
                primary.append(ticker)
            else:
                secondary.append(ticker)
    primary_unique = list(dict.fromkeys(primary))
    secondary_unique = [t for t in dict.fromkeys(secondary) if t not in primary_unique]
    return {
        "event_id": event.id,
        "primary_tickers": primary_unique,
        "secondary_tickers": secondary_unique,
        "all_tickers": primary_unique + secondary_unique,
        "theme_hits": theme_hits,
    }
