from __future__ import annotations

import csv
import math
from collections import Counter
from collections.abc import Iterable
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
    "name",
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
MANUAL_BAR_REQUIRED_FILL_FIELDS = ("open", "high", "low", "close", "volume", "vwap")
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
    stocks_only: bool
    provider: str
    generated_at: datetime

    def as_payload(self) -> dict[str, object]:
        if self.stocks_only and self.missing_only:
            template_scope = "stock_like_missing_as_of_bars"
        elif self.stocks_only:
            template_scope = "stock_like_active_universe"
        elif self.missing_only:
            template_scope = "missing_as_of_bars"
        else:
            template_scope = "active_universe"
        stock_flag = " --stocks-only" if self.stocks_only else ""
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
            "stocks_only": self.stocks_only,
            "template_scope": template_scope,
            "template_columns": list(MANUAL_BAR_COLUMNS),
            "row_order": "stock_like_then_unknown_then_non_stock",
            "provider": self.provider,
            "generated_at": self.generated_at.isoformat(),
            "external_calls_made": 0,
            "next_action": (
                "Rows include security names and are sorted stock-like first. "
                "Fill open, high, low, close, volume, and vwap for every row, "
                "then preview the import before executing."
            ),
            "import_command": (
                "catalyst-radar market-bars import "
                f"--daily-bars {self.output_path} "
                f"--expected-as-of {self.expected_as_of.isoformat()}"
                f"{stock_flag}"
            ),
            "execute_command": (
                "catalyst-radar market-bars import "
                f"--daily-bars {self.output_path} "
                f"--expected-as-of {self.expected_as_of.isoformat()}"
                f"{stock_flag} --execute"
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
    stocks_only: bool = False
    missing_expected_tickers: tuple[str, ...] = ()
    executed: bool = False
    invalid_row_count: int = 0
    blank_required_count: int = 0
    blank_required_field_counts: tuple[tuple[str, int], ...] = ()
    invalid_numeric_count: int = 0
    invalid_examples: tuple[str, ...] = ()
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
        elif self.status == "invalid":
            next_action = (
                "Fix blank or invalid required fields, then preview again before "
                "running --execute."
            )
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
            "stocks_only": self.stocks_only,
            "coverage_scope": "stock_like" if self.stocks_only else "active_universe",
            "missing_expected_count": len(self.missing_expected_tickers),
            "missing_expected_tickers": missing_sample,
            "missing_expected_more": max(
                0,
                len(self.missing_expected_tickers) - len(missing_sample),
            ),
            "executed": self.executed,
            "invalid_row_count": self.invalid_row_count,
            "blank_required_count": self.blank_required_count,
            "blank_required_field_counts": {
                field_name: count
                for field_name, count in self.blank_required_field_counts
            },
            "invalid_numeric_count": self.invalid_numeric_count,
            "invalid_examples": list(self.invalid_examples[:6]),
            "invalid_more": max(0, len(self.invalid_examples) - 6),
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
                + (" --stocks-only" if self.stocks_only else "")
                + " --execute"
            ),
        }


@dataclass(frozen=True)
class ManualBarsRepairPlanResult:
    expected_as_of: date
    active_security_count: int
    existing_as_of_bar_count: int
    missing_as_of_bar_tickers: tuple[str, ...]
    missing_security_type_counts: tuple[tuple[str, int], ...]
    missing_with_local_history_tickers: tuple[str, ...]
    missing_without_local_history_tickers: tuple[str, ...]
    stocks_only: bool
    provider_key_configured: bool
    generated_at: datetime
    local_template_path: Path
    local_template_preview: dict[str, object] | None = None

    @property
    def missing_as_of_bar_count(self) -> int:
        return len(self.missing_as_of_bar_tickers)

    def as_payload(self) -> dict[str, object]:
        missing = self.missing_as_of_bar_count
        template_command = _manual_market_bars_template_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
        )
        import_preview_command = _manual_market_bars_import_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
            execute=False,
        )
        import_execute_command = _manual_market_bars_import_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
            execute=True,
        )
        provider_command = (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {self.expected_as_of.isoformat()} --confirm-external-call"
        )
        if missing <= 0:
            status = "ready"
            provider_fill_status = "not_needed"
            next_action = "As-of market bars already cover this scope."
        elif self.provider_key_configured:
            status = "attention"
            provider_fill_status = "ready_for_approval"
            next_action = (
                "Fill the manual CSV and preview the import, or explicitly approve "
                "the one-call Polygon/Massive grouped-daily fill."
            )
        else:
            status = "attention"
            provider_fill_status = "blocked"
            next_action = (
                "Fill the manual CSV and preview the import, or configure a real "
                "Polygon/Massive API key before using the provider fill command."
            )
        missing_sample = list(self.missing_as_of_bar_tickers[:12])
        return {
            "schema_version": "manual-market-bars-repair-plan-v1",
            "status": status,
            "expected_as_of": self.expected_as_of.isoformat(),
            "stocks_only": self.stocks_only,
            "coverage_scope": "stock_like" if self.stocks_only else "active_universe",
            "active_security_count": self.active_security_count,
            "existing_as_of_bar_count": self.existing_as_of_bar_count,
            "missing_as_of_bar_count": missing,
            "missing_as_of_bar_ticker_sample": missing_sample,
            "missing_as_of_bar_ticker_more": max(
                0,
                missing - len(missing_sample),
            ),
            "missing_security_type_counts": {
                security_type: count
                for security_type, count in self.missing_security_type_counts
            },
            "missing_with_local_history_count": len(
                self.missing_with_local_history_tickers
            ),
            "missing_with_local_history_sample": list(
                self.missing_with_local_history_tickers[:12]
            ),
            "missing_with_local_history_more": max(
                0,
                len(self.missing_with_local_history_tickers) - 12,
            ),
            "missing_without_local_history_count": len(
                self.missing_without_local_history_tickers
            ),
            "missing_without_local_history_sample": list(
                self.missing_without_local_history_tickers[:12]
            ),
            "missing_without_local_history_more": max(
                0,
                len(self.missing_without_local_history_tickers) - 12,
            ),
            "manual_template_command": template_command,
            "manual_import_preview_command": import_preview_command,
            "manual_import_execute_command": import_execute_command,
            "local_template_path": str(self.local_template_path),
            "local_template_exists": self.local_template_path.exists(),
            "local_template_preview": self.local_template_preview,
            "manual_template_api": "POST /api/radar/market-bars/template",
            "manual_import_api": "POST /api/radar/market-bars/import",
            "required_fill_fields": list(MANUAL_BAR_REQUIRED_FILL_FIELDS),
            "blank_required_field_counts_if_new_template": {
                field_name: missing for field_name in MANUAL_BAR_REQUIRED_FILL_FIELDS
            }
            if missing
            else {},
            "template_row_count": missing,
            "provider_fill_status": provider_fill_status,
            "provider": "polygon",
            "provider_label": "Polygon/Massive grouped daily",
            "provider_key_configured": self.provider_key_configured,
            "provider_fill_external_call_count": 1 if missing > 0 else 0,
            "provider_fill_command": provider_command if missing > 0 else None,
            "provider_fill_api": None,
            "external_calls_made": 0,
            "approval_boundary": (
                "This repair plan makes 0 provider calls. The provider command "
                "makes one Polygon/Massive grouped-daily request and must only be "
                "run after explicit operator approval."
            ),
            "write_boundary": (
                "Template generation writes a local CSV. Import preview makes no "
                "database writes. Import --execute writes local daily bars only."
            ),
            "generated_at": self.generated_at.isoformat(),
            "next_action": next_action,
        }


def write_manual_market_bars_template(
    engine: Engine,
    *,
    output_path: str | Path,
    expected_as_of: date,
    provider: str = "manual_csv",
    generated_at: datetime | None = None,
    missing_only: bool = False,
    stocks_only: bool = False,
) -> ManualBarsTemplateResult:
    active_rows = _active_security_rows(engine)
    if not active_rows:
        msg = "cannot build manual market-bar template: no active securities in database"
        raise ValueError(msg)
    scoped_rows = (
        tuple(row for row in active_rows if _manual_bar_is_stock_like(row[1]))
        if stocks_only
        else active_rows
    )
    if not scoped_rows:
        msg = "cannot build manual market-bar template: no matching active securities"
        raise ValueError(msg)
    active_tickers = tuple(row[0] for row in scoped_rows)
    existing = _bar_tickers_for_date(engine, expected_as_of)
    template_rows = [
        row
        for row in scoped_rows
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
        for ticker, security_type, name in template_rows:
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": expected_as_of.isoformat(),
                    "security_type": security_type,
                    "name": name,
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
        stocks_only=stocks_only,
        provider=provider,
        generated_at=resolved_at,
    )


def manual_market_bars_repair_plan(
    engine: Engine,
    *,
    expected_as_of: date,
    stocks_only: bool = False,
    provider_key_configured: bool = False,
    generated_at: datetime | None = None,
) -> ManualBarsRepairPlanResult:
    active_rows = _active_security_rows(engine)
    if not active_rows:
        msg = "cannot build manual market-bar repair plan: no active securities in database"
        raise ValueError(msg)
    scoped_rows = (
        tuple(row for row in active_rows if _manual_bar_is_stock_like(row[1]))
        if stocks_only
        else active_rows
    )
    if not scoped_rows:
        msg = "cannot build manual market-bar repair plan: no matching active securities"
        raise ValueError(msg)
    active_tickers = {ticker for ticker, _security_type, _name in scoped_rows}
    security_type_by_ticker = {
        ticker: security_type for ticker, security_type, _name in scoped_rows
    }
    existing = _bar_tickers_for_date(engine, expected_as_of)
    missing = tuple(sorted(active_tickers - existing))
    missing_security_type_counts = _security_type_counts(
        security_type_by_ticker.get(ticker, "") for ticker in missing
    )
    tickers_with_history = _bar_tickers_with_any_history(engine)
    missing_with_history = tuple(
        ticker for ticker in missing if ticker in tickers_with_history
    )
    missing_without_history = tuple(
        ticker for ticker in missing if ticker not in tickers_with_history
    )
    template_path = _manual_market_bars_template_path(
        expected_as_of,
        stocks_only=stocks_only,
    )
    local_template_preview: dict[str, object] | None = None
    if template_path.exists():
        try:
            local_template_preview = preview_manual_market_bars_import(
                engine,
                daily_bars_path=template_path,
                expected_as_of=expected_as_of,
                stocks_only=stocks_only,
            ).as_payload()
        except ValueError as exc:
            local_template_preview = {
                "schema_version": "manual-market-bars-local-template-preview-v1",
                "status": "invalid",
                "daily_bars_path": str(template_path),
                "expected_as_of": expected_as_of.isoformat(),
                "stocks_only": stocks_only,
                "error": str(exc),
                "external_calls_made": 0,
                "next_action": "Fix or regenerate the local manual market-bars template.",
            }
    return ManualBarsRepairPlanResult(
        expected_as_of=expected_as_of,
        active_security_count=len(active_tickers),
        existing_as_of_bar_count=len(existing & active_tickers),
        missing_as_of_bar_tickers=missing,
        missing_security_type_counts=missing_security_type_counts,
        missing_with_local_history_tickers=missing_with_history,
        missing_without_local_history_tickers=missing_without_history,
        stocks_only=stocks_only,
        provider_key_configured=provider_key_configured,
        local_template_path=template_path,
        local_template_preview=local_template_preview,
        generated_at=_as_utc(generated_at or datetime.now(UTC)),
    )


def preview_manual_market_bars_import(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
    stocks_only: bool = False,
) -> ManualBarsImportResult:
    path = Path(daily_bars_path)
    validation = _inspect_manual_bars_csv(path, expected_as_of=expected_as_of)
    active = set(_active_tickers(engine, stocks_only=stocks_only))
    if not active:
        msg = "cannot validate manual market bars: no active securities in database"
        raise ValueError(msg)
    if validation.row_count <= 0:
        msg = f"daily bars CSV contains no rows: {path}"
        raise ValueError(msg)
    if validation.invalid_row_count:
        existing_at_expected: set[str] | None = None
        coverage_after_import: int | None = None
        missing: tuple[str, ...] = ()
        if expected_as_of is not None:
            existing_at_expected = _bar_tickers_for_date(engine, expected_as_of) & active
            coverage_after = existing_at_expected | (
                validation.expected_as_of_tickers & active
            )
            coverage_after_import = len(coverage_after)
            missing = tuple(sorted(active - coverage_after))
        return ManualBarsImportResult(
            daily_bars_path=path,
            expected_as_of=expected_as_of,
            status="invalid",
            row_count=validation.row_count,
            ticker_count=len(validation.tickers),
            latest_bar_date=validation.latest_bar_date,
            active_security_count=len(active),
            existing_as_of_bar_count=(
                len(existing_at_expected) if existing_at_expected is not None else None
            ),
            coverage_after_import_count=coverage_after_import,
            bars_at_expected_as_of=(
                len(validation.expected_as_of_tickers)
                if expected_as_of is not None
                else None
            ),
            stocks_only=stocks_only,
            missing_expected_tickers=missing,
            invalid_row_count=validation.invalid_row_count,
            blank_required_count=validation.blank_required_count,
            blank_required_field_counts=validation.blank_required_field_counts,
            invalid_numeric_count=validation.invalid_numeric_count,
            invalid_examples=validation.invalid_examples,
        )
    try:
        bars = tuple(load_daily_bars_csv(path))
    except (TypeError, ValueError) as exc:
        return ManualBarsImportResult(
            daily_bars_path=path,
            expected_as_of=expected_as_of,
            status="invalid",
            row_count=validation.row_count,
            ticker_count=len(validation.tickers),
            latest_bar_date=validation.latest_bar_date,
            active_security_count=len(active),
            existing_as_of_bar_count=None,
            coverage_after_import_count=None,
            bars_at_expected_as_of=None,
            stocks_only=stocks_only,
            invalid_row_count=1,
            invalid_examples=(str(exc),),
        )
    _validate_manual_bars(bars)
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
        stocks_only=stocks_only,
        missing_expected_tickers=missing,
        bars=bars,
    )


def import_manual_market_bars(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
    execute: bool = False,
    stocks_only: bool = False,
) -> ManualBarsImportResult:
    preview = preview_manual_market_bars_import(
        engine,
        daily_bars_path=daily_bars_path,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
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
        stocks_only=preview.stocks_only,
        missing_expected_tickers=preview.missing_expected_tickers,
        executed=True,
        bars=preview.bars,
    )


def _active_tickers(engine: Engine, *, stocks_only: bool = False) -> tuple[str, ...]:
    if not stocks_only:
        return tuple(
            security.ticker.upper()
            for security in MarketRepository(engine).list_active_securities()
        )
    return tuple(
        row[0]
        for row in _active_security_rows(engine)
        if _manual_bar_is_stock_like(row[1])
    )


def _active_security_rows(engine: Engine) -> tuple[tuple[str, str, str], ...]:
    with engine.connect() as conn:
        rows = conn.execute(
            select(securities.c.ticker, securities.c.name, securities.c.metadata)
            .where(securities.c.is_active.is_(True))
            .order_by(securities.c.ticker)
        ).all()
    values: list[tuple[str, str, str]] = []
    for row in rows:
        ticker = str(row._mapping["ticker"] or "").strip().upper()
        name = str(row._mapping["name"] or "").strip()
        metadata = row._mapping["metadata"]
        if not isinstance(metadata, dict):
            metadata = {}
        security_type = str(metadata.get("type") or "").strip().upper()
        values.append((ticker, security_type, name))
    return tuple(values)


def _manual_bar_template_sort_key(row: tuple[str, str, str]) -> tuple[int, str]:
    ticker, security_type, _name = row
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


def _manual_bar_is_stock_like(security_type: str) -> bool:
    return str(security_type or "").strip().upper() in MANUAL_BAR_COMPANY_LIKE_TYPES


def _security_type_counts(
    security_types: Iterable[object],
) -> tuple[tuple[str, int], ...]:
    counts = Counter(_normalized_security_type(item) for item in security_types)
    return tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _normalized_security_type(security_type: object) -> str:
    return str(security_type or "").strip().upper() or "UNKNOWN"


def _bar_tickers_for_date(engine: Engine, as_of_date: date) -> set[str]:
    with engine.connect() as conn:
        return {
            str(row._mapping["ticker"]).strip().upper()
            for row in conn.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == as_of_date)
            )
            if str(row._mapping["ticker"]).strip()
        }


def _bar_tickers_with_any_history(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        return {
            str(row._mapping["ticker"]).strip().upper()
            for row in conn.execute(select(daily_bars.c.ticker).distinct())
            if str(row._mapping["ticker"]).strip()
        }


def _manual_market_bars_template_command(
    expected_as_of: date,
    *,
    stocks_only: bool,
) -> str:
    stocks_flag = " --stocks-only" if stocks_only else ""
    return (
        "catalyst-radar market-bars template "
        f"--expected-as-of {expected_as_of.isoformat()} "
        f"--out {_manual_market_bars_template_path(expected_as_of, stocks_only=stocks_only)} "
        f"--missing-only{stocks_flag}"
    )


def _manual_market_bars_import_command(
    expected_as_of: date,
    *,
    stocks_only: bool,
    execute: bool,
) -> str:
    stocks_flag = " --stocks-only" if stocks_only else ""
    execute_flag = " --execute" if execute else ""
    template_path = _manual_market_bars_template_path(
        expected_as_of,
        stocks_only=stocks_only,
    )
    return (
        "catalyst-radar market-bars import "
        f"--daily-bars {template_path} "
        f"--expected-as-of {expected_as_of.isoformat()}{stocks_flag}{execute_flag}"
    )


def _manual_market_bars_template_path(
    expected_as_of: date,
    *,
    stocks_only: bool,
) -> Path:
    filename_prefix = "manual-stock-bars" if stocks_only else "manual-bars"
    return Path("data") / "local" / f"{filename_prefix}-{expected_as_of.isoformat()}.csv"


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


@dataclass(frozen=True)
class _ManualBarsCsvValidation:
    row_count: int
    tickers: frozenset[str]
    latest_bar_date: date | None
    expected_as_of_tickers: frozenset[str]
    invalid_row_count: int
    blank_required_count: int
    blank_required_field_counts: tuple[tuple[str, int], ...]
    invalid_numeric_count: int
    invalid_examples: tuple[str, ...]


def _inspect_manual_bars_csv(
    path: Path,
    *,
    expected_as_of: date | None,
) -> _ManualBarsCsvValidation:
    required = {
        "ticker",
        "date",
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
    }
    numeric_fields = ("open", "high", "low", "close", "volume", "vwap")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or ())
        missing_columns = sorted(required - fieldnames)
        if missing_columns:
            msg = (
                "daily bars CSV is missing required column(s): "
                + ", ".join(missing_columns)
            )
            raise ValueError(msg)
        row_count = 0
        tickers: set[str] = set()
        latest_date: date | None = None
        expected_date_tickers: set[str] = set()
        invalid_rows: set[int] = set()
        blank_required_count = 0
        blank_required_field_counts: dict[str, int] = {}
        invalid_numeric_count = 0
        examples: list[str] = []
        for row_number, row in enumerate(reader, start=2):
            row_count += 1
            ticker = str(row.get("ticker") or "").strip().upper()
            date_text = str(row.get("date") or "").strip()
            if ticker:
                tickers.add(ticker)
            parsed_date: date | None = None
            if date_text:
                try:
                    parsed_date = date.fromisoformat(date_text)
                except ValueError:
                    invalid_rows.add(row_number)
                    _append_invalid_example(
                        examples,
                        row_number,
                        ticker,
                        date_text,
                        "invalid date",
                    )
                    continue
                latest_date = (
                    parsed_date
                    if latest_date is None or parsed_date > latest_date
                    else latest_date
                )
                if expected_as_of is not None and parsed_date == expected_as_of:
                    expected_date_tickers.add(ticker)
            blank_fields = [
                field for field in required if not str(row.get(field) or "").strip()
            ]
            if blank_fields:
                invalid_rows.add(row_number)
                blank_required_count += len(blank_fields)
                for field in blank_fields:
                    blank_required_field_counts[field] = (
                        blank_required_field_counts.get(field, 0) + 1
                    )
                _append_invalid_example(
                    examples,
                    row_number,
                    ticker,
                    date_text,
                    "blank " + ",".join(sorted(blank_fields)[:4]),
                )
                continue
            row_invalid_numeric = False
            for field in numeric_fields:
                value = str(row.get(field) or "").strip()
                try:
                    parsed = float(value)
                except ValueError:
                    row_invalid_numeric = True
                    invalid_numeric_count += 1
                    _append_invalid_example(
                        examples,
                        row_number,
                        ticker,
                        date_text,
                        f"invalid {field}",
                    )
                    continue
                if not math.isfinite(parsed):
                    row_invalid_numeric = True
                    invalid_numeric_count += 1
                    _append_invalid_example(
                        examples,
                        row_number,
                        ticker,
                        date_text,
                        f"invalid {field}",
                    )
                if field == "volume" and parsed < 0:
                    row_invalid_numeric = True
                    invalid_numeric_count += 1
                    _append_invalid_example(
                        examples,
                        row_number,
                        ticker,
                        date_text,
                        "negative volume",
                    )
            if row_invalid_numeric:
                invalid_rows.add(row_number)
    return _ManualBarsCsvValidation(
        row_count=row_count,
        tickers=frozenset(tickers),
        latest_bar_date=latest_date,
        expected_as_of_tickers=frozenset(expected_date_tickers),
        invalid_row_count=len(invalid_rows),
        blank_required_count=blank_required_count,
        blank_required_field_counts=tuple(
            (field, blank_required_field_counts[field])
            for field in MANUAL_BAR_COLUMNS
            if blank_required_field_counts.get(field, 0)
        ),
        invalid_numeric_count=invalid_numeric_count,
        invalid_examples=tuple(examples),
    )


def _append_invalid_example(
    examples: list[str],
    row_number: int,
    ticker: str,
    date_text: str,
    reason: str,
) -> None:
    if len(examples) >= 12:
        return
    label = ticker or "<blank ticker>"
    date_label = date_text or "<blank date>"
    examples.append(f"row {row_number} {label} {date_label}: {reason}")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
