from __future__ import annotations

import csv
import math
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.engine import Engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv
from catalyst_radar.core.models import DailyBar
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars, securities

MANUAL_BAR_COLUMNS = (
    "ticker",
    "date",
    "security_type",
    "template_reason",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "adjusted",
    "provider",
    "source_ts",
    "available_at",
)
MANUAL_BAR_COMPANY_LIKE_TYPES = frozenset({"ADRC", "CS"})
MANUAL_BAR_NON_STOCK_TYPES = frozenset(
    {"ETF", "ETN", "ETS", "ETV", "FUND", "PFD", "RIGHT", "SP", "UNIT", "WARRANT"}
)


@dataclass(frozen=True)
class ManualBarsTemplateResult:
    output_path: Path
    expected_as_of: date
    active_security_count: int
    row_count: int
    existing_as_of_bar_count: int
    missing_as_of_bar_count: int
    missing_only: bool
    provider: str
    generated_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "manual-market-bars-template-v1",
            "status": "ready",
            "output_path": str(self.output_path),
            "expected_as_of": self.expected_as_of.isoformat(),
            "active_security_count": self.active_security_count,
            "row_count": self.row_count,
            "existing_as_of_bar_count": self.existing_as_of_bar_count,
            "missing_as_of_bar_count": self.missing_as_of_bar_count,
            "missing_only": self.missing_only,
            "template_scope": "missing_as_of_bars" if self.missing_only else "active_universe",
            "row_order": "stock_like_then_unknown_then_non_stock",
            "provider": self.provider,
            "generated_at": self.generated_at.isoformat(),
            "external_calls_made": 0,
            "next_action": (
                "Rows are sorted stock-like first. Fill open, high, low, close, "
                "volume, and vwap for every row, then preview the import before "
                "executing."
            ),
            "import_command": (
                "catalyst-radar market-bars import "
                f"--daily-bars {self.output_path} "
                f"--expected-as-of {self.expected_as_of.isoformat()}"
            ),
            "execute_command": (
                "catalyst-radar market-bars import "
                f"--daily-bars {self.output_path} "
                f"--expected-as-of {self.expected_as_of.isoformat()} --execute"
            ),
        }


@dataclass(frozen=True)
class ManualBarsImportResult:
    daily_bars_path: Path
    expected_as_of: date | None
    status: str
    row_count: int
    ticker_count: int
    latest_bar_date: date | None
    active_security_count: int
    existing_as_of_bar_count: int | None
    coverage_after_import_count: int | None
    bars_at_expected_as_of: int | None
    missing_expected_tickers: tuple[str, ...] = ()
    executed: bool = False
    bars: tuple[DailyBar, ...] = field(default=(), repr=False)

    def as_payload(self) -> dict[str, object]:
        missing_sample = list(self.missing_expected_tickers[:12])
        if self.status == "imported":
            next_action = "Run one plan-only radar smoke, then run a capped scan if intended."
        elif self.status == "ready":
            next_action = "Preview is ready; rerun with --execute to import these bars."
        elif self.status == "stale":
            next_action = "Provide a CSV whose latest date is at least expected_as_of."
        elif self.status == "incomplete":
            next_action = "Fill every active ticker for expected_as_of before importing."
        else:
            next_action = "Review the CSV before importing."
        return {
            "schema_version": "manual-market-bars-import-v1",
            "status": self.status,
            "daily_bars_path": str(self.daily_bars_path),
            "expected_as_of": (
                self.expected_as_of.isoformat()
                if self.expected_as_of is not None
                else None
            ),
            "row_count": self.row_count,
            "ticker_count": self.ticker_count,
            "latest_bar_date": (
                self.latest_bar_date.isoformat()
                if self.latest_bar_date is not None
                else None
            ),
            "active_security_count": self.active_security_count,
            "existing_as_of_bar_count": self.existing_as_of_bar_count,
            "coverage_after_import_count": self.coverage_after_import_count,
            "bars_at_expected_as_of": self.bars_at_expected_as_of,
            "missing_expected_count": len(self.missing_expected_tickers),
            "missing_expected_tickers": missing_sample,
            "missing_expected_more": max(
                0,
                len(self.missing_expected_tickers) - len(missing_sample),
            ),
            "executed": self.executed,
            "external_calls_made": 0,
            "next_action": next_action,
            "execute_command": (
                "catalyst-radar market-bars import "
                f"--daily-bars {self.daily_bars_path}"
                + (
                    f" --expected-as-of {self.expected_as_of.isoformat()}"
                    if self.expected_as_of is not None
                    else ""
                )
                + " --execute"
            ),
        }


def write_manual_market_bars_template(
    engine: Engine,
    *,
    output_path: str | Path,
    expected_as_of: date,
    provider: str = "manual_csv",
    generated_at: datetime | None = None,
    missing_only: bool = False,
) -> ManualBarsTemplateResult:
    active_rows = _active_security_rows(engine)
    if not active_rows:
        msg = "cannot build manual market-bar template: no active securities in database"
        raise ValueError(msg)
    active_tickers = tuple(row[0] for row in active_rows)
    existing = _bar_tickers_for_date(engine, expected_as_of)
    template_rows = [
        row
        for row in active_rows
        if not missing_only or row[0] not in existing
    ]
    template_rows = sorted(template_rows, key=_manual_bar_template_sort_key)
    resolved_at = _as_utc(generated_at or datetime.now(UTC))
    path = Path(output_path)
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        for ticker, security_type in template_rows:
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": expected_as_of.isoformat(),
                    "security_type": security_type,
                    "template_reason": (
                        "missing_as_of_bar" if ticker not in existing else "active_universe"
                    ),
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "vwap": "",
                    "adjusted": "true",
                    "provider": provider,
                    "source_ts": resolved_at.isoformat(),
                    "available_at": resolved_at.isoformat(),
                }
            )
    return ManualBarsTemplateResult(
        output_path=path,
        expected_as_of=expected_as_of,
        active_security_count=len(active_tickers),
        row_count=len(template_rows),
        existing_as_of_bar_count=len(existing & set(active_tickers)),
        missing_as_of_bar_count=len(set(active_tickers) - existing),
        missing_only=missing_only,
        provider=provider,
        generated_at=resolved_at,
    )


def preview_manual_market_bars_import(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
) -> ManualBarsImportResult:
    path = Path(daily_bars_path)
    bars = tuple(load_daily_bars_csv(path))
    if not bars:
        msg = f"daily bars CSV contains no rows: {path}"
        raise ValueError(msg)
    _validate_manual_bars(bars)
    active = set(_active_tickers(engine))
    if not active:
        msg = "cannot validate manual market bars: no active securities in database"
        raise ValueError(msg)
    latest = max(bar.date for bar in bars)
    tickers = {bar.ticker.upper() for bar in bars}
    bars_at_expected: int | None = None
    existing_at_expected: set[str] | None = None
    coverage_after_import: int | None = None
    missing: tuple[str, ...] = ()
    status = "ready"
    if expected_as_of is not None:
        if latest < expected_as_of:
            status = "stale"
        expected_tickers = {
            bar.ticker.upper() for bar in bars if bar.date == expected_as_of
        }
        bars_at_expected = len(expected_tickers)
        existing_at_expected = _bar_tickers_for_date(engine, expected_as_of) & active
        coverage_after = existing_at_expected | (expected_tickers & active)
        coverage_after_import = len(coverage_after)
        missing = tuple(sorted(active - coverage_after))
        if missing and status == "ready":
            status = "incomplete"
    return ManualBarsImportResult(
        daily_bars_path=path,
        expected_as_of=expected_as_of,
        status=status,
        row_count=len(bars),
        ticker_count=len(tickers),
        latest_bar_date=latest,
        active_security_count=len(active),
        existing_as_of_bar_count=(
            len(existing_at_expected) if existing_at_expected is not None else None
        ),
        coverage_after_import_count=coverage_after_import,
        bars_at_expected_as_of=bars_at_expected,
        missing_expected_tickers=missing,
        bars=bars,
    )


def import_manual_market_bars(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
    execute: bool = False,
) -> ManualBarsImportResult:
    preview = preview_manual_market_bars_import(
        engine,
        daily_bars_path=daily_bars_path,
        expected_as_of=expected_as_of,
    )
    if preview.status != "ready":
        return preview
    if not execute:
        return preview
    MarketRepository(engine).upsert_daily_bars(preview.bars)
    return ManualBarsImportResult(
        daily_bars_path=preview.daily_bars_path,
        expected_as_of=preview.expected_as_of,
        status="imported",
        row_count=preview.row_count,
        ticker_count=preview.ticker_count,
        latest_bar_date=preview.latest_bar_date,
        active_security_count=preview.active_security_count,
        existing_as_of_bar_count=preview.existing_as_of_bar_count,
        coverage_after_import_count=preview.coverage_after_import_count,
        bars_at_expected_as_of=preview.bars_at_expected_as_of,
        missing_expected_tickers=preview.missing_expected_tickers,
        executed=True,
        bars=preview.bars,
    )


def _active_tickers(engine: Engine) -> tuple[str, ...]:
    return tuple(
        security.ticker.upper()
        for security in MarketRepository(engine).list_active_securities()
    )


def _active_security_rows(engine: Engine) -> tuple[tuple[str, str], ...]:
    with engine.connect() as conn:
        rows = conn.execute(
            select(securities.c.ticker, securities.c.metadata)
            .where(securities.c.is_active.is_(True))
            .order_by(securities.c.ticker)
        ).all()
    values: list[tuple[str, str]] = []
    for row in rows:
        ticker = str(row._mapping["ticker"] or "").strip().upper()
        metadata = row._mapping["metadata"]
        if not isinstance(metadata, dict):
            metadata = {}
        security_type = str(metadata.get("type") or "").strip().upper()
        values.append((ticker, security_type))
    return tuple(values)


def _manual_bar_template_sort_key(row: tuple[str, str]) -> tuple[int, str]:
    ticker, security_type = row
    return (_manual_bar_security_type_priority(security_type), ticker)


def _manual_bar_security_type_priority(security_type: str) -> int:
    normalized = str(security_type or "").strip().upper() or "UNKNOWN"
    if normalized in MANUAL_BAR_COMPANY_LIKE_TYPES:
        return 0
    if normalized == "UNKNOWN":
        return 1
    if normalized in MANUAL_BAR_NON_STOCK_TYPES:
        return 2
    return 3


def _bar_tickers_for_date(engine: Engine, as_of_date: date) -> set[str]:
    with engine.connect() as conn:
        return {
            str(row._mapping["ticker"]).strip().upper()
            for row in conn.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == as_of_date)
            )
            if str(row._mapping["ticker"]).strip()
        }


def _validate_manual_bars(bars: tuple[DailyBar, ...]) -> None:
    for bar in bars:
        for field_name, value in (
            ("open", bar.open),
            ("high", bar.high),
            ("low", bar.low),
            ("close", bar.close),
            ("vwap", bar.vwap),
        ):
            if not math.isfinite(value):
                msg = (
                    "invalid manual market bar: "
                    f"{bar.ticker} {bar.date.isoformat()} has invalid {field_name}"
                )
                raise ValueError(msg)
        if bar.volume < 0:
            msg = (
                "invalid manual market bar: "
                f"{bar.ticker} {bar.date.isoformat()} has negative volume"
            )
            raise ValueError(msg)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
