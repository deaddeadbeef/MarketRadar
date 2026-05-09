from __future__ import annotations

from pathlib import Path

from catalyst_radar.connectors.base import MarketDataConnector
from catalyst_radar.connectors.market_data import CsvMarketDataConnector


class ConnectorRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, MarketDataConnector] = {}

    def register_connector(self, name: str, connector: MarketDataConnector) -> None:
        normalized = _normalize_name(name)
        self._connectors[normalized] = connector

    def get_connector(self, name: str) -> MarketDataConnector:
        normalized = _normalize_name(name)
        try:
            return self._connectors[normalized]
        except KeyError as exc:
            msg = f"connector is not registered: {name}"
            raise KeyError(msg) from exc

    def reset(self) -> None:
        self._connectors.clear()


_DEFAULT_REGISTRY = ConnectorRegistry()


def register_connector(name: str, connector: MarketDataConnector) -> None:
    _DEFAULT_REGISTRY.register_connector(name, connector)


def get_connector(name: str) -> MarketDataConnector:
    return _DEFAULT_REGISTRY.get_connector(name)


def reset_registry() -> None:
    _DEFAULT_REGISTRY.reset()


def default_csv_connector(
    securities_path: str | Path,
    daily_bars_path: str | Path,
    holdings_path: str | Path | None = None,
) -> CsvMarketDataConnector:
    return CsvMarketDataConnector(
        securities_path=securities_path,
        daily_bars_path=daily_bars_path,
        holdings_path=holdings_path,
    )


def _normalize_name(name: str) -> str:
    normalized = name.strip().lower()
    if not normalized:
        msg = "connector name must be non-empty"
        raise ValueError(msg)
    return normalized


__all__ = [
    "ConnectorRegistry",
    "default_csv_connector",
    "get_connector",
    "register_connector",
    "reset_registry",
]
