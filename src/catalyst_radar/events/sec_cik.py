from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.engine import Engine

from catalyst_radar.connectors.http import (
    HeaderInjectingTransport,
    HttpTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import securities

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass(frozen=True)
class SecCikMetadataRefreshResult:
    live: bool
    active_security_count: int
    missing_before_count: int
    matched_missing_count: int
    updated_count: int
    missing_after_count: int
    updated_tickers: tuple[str, ...]
    unmatched_tickers: tuple[str, ...]
    fetched_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "sec-cik-metadata-refresh-v1",
            "provider": "sec",
            "endpoint": "company-tickers",
            "live": self.live,
            "external_calls_made": 1 if self.live else 0,
            "active_security_count": self.active_security_count,
            "missing_before_count": self.missing_before_count,
            "matched_missing_count": self.matched_missing_count,
            "updated_count": self.updated_count,
            "missing_after_count": self.missing_after_count,
            "updated_tickers": list(self.updated_tickers),
            "unmatched_tickers": list(self.unmatched_tickers),
            "fetched_at": self.fetched_at.isoformat(),
            "next_action": self._next_action(),
        }

    def _next_action(self) -> str:
        if self.updated_count:
            return (
                "Recheck catalyst_events source batches; newly CIK-backed rows "
                "can now be planned for SEC ingestion."
            )
        if self.missing_after_count:
            return (
                "Some active tickers still lack SEC CIKs; they may be ETFs, "
                "preferred/share-class symbols, or unavailable in SEC company tickers."
            )
        return "All active securities already have SEC CIK metadata."


def refresh_sec_cik_metadata(
    engine: Engine,
    config: AppConfig,
    *,
    fixture_path: Path | None = None,
    transport: HttpTransport | None = None,
) -> SecCikMetadataRefreshResult:
    live = fixture_path is None
    if live and not config.sec_enable_live:
        msg = "live SEC CIK refresh requires CATALYST_SEC_ENABLE_LIVE=1"
        raise ValueError(msg)
    if live and not config.sec_user_agent_configured:
        msg = "CATALYST_SEC_USER_AGENT is required for live SEC CIK refresh"
        raise ValueError(msg)

    fetched_at = datetime.now(UTC).replace(microsecond=0)
    cik_by_ticker = _load_sec_company_tickers(
        config,
        fixture_path=fixture_path,
        transport=transport,
    )
    with engine.begin() as conn:
        active_rows = [
            row._mapping
            for row in conn.execute(
                select(securities.c.ticker, securities.c.metadata).where(
                    securities.c.is_active.is_(True)
                )
            )
        ]
        missing_rows = [
            row
            for row in active_rows
            if _metadata_cik(_as_mapping(row["metadata"])) is None
        ]
        updated: list[str] = []
        unmatched: list[str] = []
        for row in missing_rows:
            ticker = str(row["ticker"]).strip().upper()
            company = _lookup_company_ticker(cik_by_ticker, ticker)
            if company is None:
                unmatched.append(ticker)
                continue
            metadata = dict(_as_mapping(row["metadata"]))
            metadata.update(
                {
                    "cik": company["cik"],
                    "sec_company_name": company["name"],
                    "cik_source": "sec_company_tickers",
                    "cik_updated_at": fetched_at.isoformat(),
                }
            )
            conn.execute(
                update(securities)
                .where(securities.c.ticker == ticker)
                .values(metadata=thaw_json_value(metadata))
            )
            updated.append(ticker)

    return SecCikMetadataRefreshResult(
        live=live,
        active_security_count=len(active_rows),
        missing_before_count=len(missing_rows),
        matched_missing_count=len(updated),
        updated_count=len(updated),
        missing_after_count=len(unmatched),
        updated_tickers=tuple(updated[:10]),
        unmatched_tickers=tuple(unmatched[:10]),
        fetched_at=fetched_at,
    )


def _load_sec_company_tickers(
    config: AppConfig,
    *,
    fixture_path: Path | None,
    transport: HttpTransport | None,
) -> dict[str, dict[str, str]]:
    if fixture_path is not None:
        raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    else:
        active_transport = HeaderInjectingTransport(
            transport or UrlLibHttpTransport(),
            {"User-Agent": config.sec_user_agent or ""},
        )
        raw_payload = JsonHttpClient(
            active_transport,
            timeout_seconds=config.http_timeout_seconds,
        ).get_json(SEC_COMPANY_TICKERS_URL)
    records = _company_ticker_records(raw_payload)
    by_ticker: dict[str, dict[str, str]] = {}
    for record in records:
        ticker = str(record.get("ticker") or "").strip().upper()
        cik = str(record.get("cik_str") or record.get("cik") or "").strip()
        name = str(record.get("title") or record.get("name") or "").strip()
        if not ticker or not cik:
            continue
        payload = {"ticker": ticker, "cik": cik.zfill(10), "name": name}
        for key in _ticker_keys(ticker):
            by_ticker.setdefault(key, payload)
    return by_ticker


def _company_ticker_records(payload: object) -> Sequence[Mapping[str, object]]:
    if isinstance(payload, Mapping):
        values = payload.values()
    elif isinstance(payload, list | tuple):
        values = payload
    else:
        msg = "SEC company tickers payload must be a mapping or list"
        raise ValueError(msg)
    records = [value for value in values if isinstance(value, Mapping)]
    if not records:
        msg = "SEC company tickers payload did not contain company records"
        raise ValueError(msg)
    return records


def _lookup_company_ticker(
    cik_by_ticker: Mapping[str, Mapping[str, str]],
    ticker: str,
) -> Mapping[str, str] | None:
    for key in _ticker_keys(ticker):
        value = cik_by_ticker.get(key)
        if value is not None:
            return value
    return None


def _ticker_keys(ticker: str) -> tuple[str, ...]:
    normalized = str(ticker).strip().upper()
    keys = {
        normalized,
        normalized.replace(".", "-"),
        normalized.replace("-", "."),
    }
    return tuple(key for key in keys if key)


def _metadata_cik(metadata: Mapping[str, object]) -> str | None:
    for key in ("cik", "cik_str", "central_index_key"):
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().zfill(10)
    return None


def _as_mapping(value: Any) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "SEC_COMPANY_TICKERS_URL",
    "SecCikMetadataRefreshResult",
    "refresh_sec_cik_metadata",
]
