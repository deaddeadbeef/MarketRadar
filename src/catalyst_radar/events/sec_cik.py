from __future__ import annotations

import csv
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
SEC_CIK_OVERRIDE_TEMPLATE_COLUMNS = (
    "ticker",
    "cik",
    "sec_company_name",
    "security_type",
    "template_reason",
)


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


@dataclass(frozen=True)
class SecCikOverrideResult:
    requested_count: int
    updated_count: int
    skipped_count: int
    unmatched_count: int
    invalid_count: int
    updated_tickers: tuple[str, ...]
    skipped_tickers: tuple[str, ...]
    unmatched_tickers: tuple[str, ...]
    invalid_rows: tuple[str, ...]
    applied_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "sec-cik-override-import-v1",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "requested_count": self.requested_count,
            "updated_count": self.updated_count,
            "skipped_count": self.skipped_count,
            "unmatched_count": self.unmatched_count,
            "invalid_count": self.invalid_count,
            "updated_tickers": list(self.updated_tickers),
            "skipped_tickers": list(self.skipped_tickers),
            "unmatched_tickers": list(self.unmatched_tickers),
            "invalid_rows": list(self.invalid_rows),
            "applied_at": self.applied_at.isoformat(),
            "next_action": self._next_action(),
        }

    def _next_action(self) -> str:
        if self.updated_count:
            return (
                "Recheck catalyst_events source batches; manually CIK-backed rows "
                "can now be planned for SEC ingestion."
            )
        if self.invalid_count:
            return "Fix invalid ticker/CIK rows in the override CSV and import again."
        if self.unmatched_count:
            return "Override tickers were not active securities in the local database."
        return "No CIK metadata changed."


@dataclass(frozen=True)
class SecCikOverrideTemplateWriteResult:
    output_path: Path
    row_count: int
    generated_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "sec-cik-override-template-write-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "output_path": str(self.output_path),
            "row_count": self.row_count,
            "columns": list(SEC_CIK_OVERRIDE_TEMPLATE_COLUMNS),
            "generated_at": self.generated_at.isoformat(),
            "import_command": (
                "catalyst-radar ingest-sec cik-overrides "
                f"--csv {self.output_path}"
            ),
            "next_action": (
                "Fill cik and optional sec_company_name for each row, then import "
                "the completed CSV."
            ),
        }


def write_sec_cik_override_template_csv(
    output_path: str | Path,
    rows: Sequence[Mapping[str, object]],
    *,
    generated_at: datetime | None = None,
) -> SecCikOverrideTemplateWriteResult:
    path = Path(output_path)
    resolved_at = (generated_at or datetime.now(UTC)).astimezone(UTC).replace(
        microsecond=0
    )
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SEC_CIK_OVERRIDE_TEMPLATE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    column: str(row.get(column) or "")
                    for column in SEC_CIK_OVERRIDE_TEMPLATE_COLUMNS
                }
            )
    return SecCikOverrideTemplateWriteResult(
        output_path=path,
        row_count=len(rows),
        generated_at=resolved_at,
    )


def apply_sec_cik_overrides_csv(
    engine: Engine,
    path: Path,
    *,
    source: str = "manual_cik_override",
) -> SecCikOverrideResult:
    records = _read_cik_override_csv(path)
    return apply_sec_cik_overrides(engine, records, source=source)


def apply_sec_cik_overrides(
    engine: Engine,
    records: Sequence[Mapping[str, object]],
    *,
    source: str = "manual_cik_override",
) -> SecCikOverrideResult:
    applied_at = datetime.now(UTC).replace(microsecond=0)
    requested_count = len(records)
    parsed: list[dict[str, str]] = []
    invalid_rows: list[str] = []
    for index, record in enumerate(records, start=1):
        ticker = str(record.get("ticker") or "").strip().upper()
        cik = _normalize_cik(record.get("cik") or record.get("cik_str"))
        if not ticker or cik is None:
            invalid_rows.append(f"row {index}")
            continue
        company_name = str(
            record.get("sec_company_name")
            or record.get("company_name")
            or record.get("name")
            or ""
        ).strip()
        parsed.append({"ticker": ticker, "cik": cik, "company_name": company_name})

    updated: list[str] = []
    skipped: list[str] = []
    unmatched: list[str] = []
    with engine.begin() as conn:
        for record in parsed:
            row = conn.execute(
                select(securities.c.ticker, securities.c.metadata).where(
                    securities.c.is_active.is_(True),
                    securities.c.ticker == record["ticker"],
                )
            ).first()
            if row is None:
                unmatched.append(record["ticker"])
                continue
            metadata = dict(_as_mapping(row._mapping["metadata"]))
            if _metadata_cik(metadata) == record["cik"]:
                skipped.append(record["ticker"])
                continue
            metadata.update(
                {
                    "cik": record["cik"],
                    "cik_source": source,
                    "cik_updated_at": applied_at.isoformat(),
                }
            )
            if record["company_name"]:
                metadata["sec_company_name"] = record["company_name"]
            conn.execute(
                update(securities)
                .where(securities.c.ticker == record["ticker"])
                .values(metadata=thaw_json_value(metadata))
            )
            updated.append(record["ticker"])

    return SecCikOverrideResult(
        requested_count=requested_count,
        updated_count=len(updated),
        skipped_count=len(skipped),
        unmatched_count=len(unmatched),
        invalid_count=len(invalid_rows),
        updated_tickers=tuple(updated[:10]),
        skipped_tickers=tuple(skipped[:10]),
        unmatched_tickers=tuple(unmatched[:10]),
        invalid_rows=tuple(invalid_rows[:10]),
        applied_at=applied_at,
    )


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


def _normalize_cik(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if not raw.isdigit():
        return None
    return raw.zfill(10)


def _read_cik_override_csv(path: Path) -> list[dict[str, object]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            msg = "CIK override CSV must include ticker and cik columns"
            raise ValueError(msg)
        fieldnames = {field.strip().lower() for field in reader.fieldnames}
        if "ticker" not in fieldnames or not {"cik", "cik_str"} & fieldnames:
            msg = "CIK override CSV must include ticker and cik columns"
            raise ValueError(msg)
        return [dict(row) for row in reader]


def _as_mapping(value: Any) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "SEC_COMPANY_TICKERS_URL",
    "SEC_CIK_OVERRIDE_TEMPLATE_COLUMNS",
    "SecCikOverrideResult",
    "SecCikOverrideTemplateWriteResult",
    "SecCikMetadataRefreshResult",
    "apply_sec_cik_overrides",
    "apply_sec_cik_overrides_csv",
    "refresh_sec_cik_metadata",
    "write_sec_cik_override_template_csv",
]
