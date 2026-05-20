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
MANUAL_BAR_CONTEXT_COLUMNS = ("name",)
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
    complete_required_row_count: int = 0
    partial_required_row_count: int = 0
    empty_required_row_count: int = 0
    invalid_examples: tuple[str, ...] = ()
    bars: tuple[DailyBar, ...] = field(default=(), repr=False)
    complete_rows_only: bool = False

    def as_payload(self) -> dict[str, object]:
        missing_sample = list(self.missing_expected_tickers[:12])
        if self.status == "partial_imported":
            next_action = (
                "Imported complete rows only; continue filling the CSV until "
                "full coverage is ready."
            )
        elif self.status == "imported":
            next_action = "Run one plan-only radar smoke, then run a capped scan if intended."
        elif self.status == "ready_partial":
            next_action = (
                "Complete-row preview is ready; rerun with --execute "
                "--complete-rows-only to import completed rows. Full coverage "
                "remains incomplete."
            )
        elif self.status == "ready":
            next_action = "Preview is ready; rerun with --execute to import these bars."
        elif self.status == "stale":
            next_action = "Provide a CSV whose latest date is at least expected_as_of."
        elif self.status == "incomplete":
            next_action = "Fill every active ticker for expected_as_of before importing."
        elif self.status == "no_complete_rows":
            next_action = (
                "Fill at least one complete OHLCV/VWAP row before using "
                "--complete-rows-only."
            )
        elif self.status == "invalid":
            if self.complete_rows_only:
                next_action = (
                    "Fix partial or invalid touched rows, or leave unfinished rows "
                    "fully empty before importing complete rows only."
                )
            else:
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
            "complete_rows_only": self.complete_rows_only,
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
            "fill_progress": {
                "complete_rows": self.complete_required_row_count,
                "partial_rows": self.partial_required_row_count,
                "empty_rows": self.empty_required_row_count,
                "filled_rows": (
                    self.complete_required_row_count
                    + self.partial_required_row_count
                ),
            },
            "complete_required_row_count": self.complete_required_row_count,
            "partial_required_row_count": self.partial_required_row_count,
            "empty_required_row_count": self.empty_required_row_count,
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
                + (" --complete-rows-only" if self.complete_rows_only else "")
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
    provider_health_status: str | None = None
    provider_health_reason: str | None = None
    provider_health_checked_at: datetime | None = None

    @property
    def missing_as_of_bar_count(self) -> int:
        return len(self.missing_as_of_bar_tickers)

    def as_payload(self) -> dict[str, object]:
        missing = self.missing_as_of_bar_count
        template_command = _manual_market_bars_template_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
        )
        regenerate_template_command = _manual_market_bars_template_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
            overwrite=True,
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
        incremental_import_preview_command = _manual_market_bars_import_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
            execute=False,
            complete_rows_only=True,
        )
        incremental_import_execute_command = _manual_market_bars_import_command(
            self.expected_as_of,
            stocks_only=self.stocks_only,
            execute=True,
            complete_rows_only=True,
        )
        provider_command = (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {self.expected_as_of.isoformat()} --confirm-external-call"
        )
        provider_health = _manual_bar_provider_health_payload(
            status=self.provider_health_status,
            reason=self.provider_health_reason,
            checked_at=self.provider_health_checked_at,
        )
        provider_health_gate = manual_bar_provider_health_gate(
            status=self.provider_health_status,
            reason=self.provider_health_reason,
            checked_at=self.provider_health_checked_at,
            target_as_of=self.expected_as_of,
            generated_at=self.generated_at,
        )
        provider_health_blocks_fill = bool(
            provider_health_gate.get("blocks_provider_fill"),
        )
        provider_health_warning = (
            str(provider_health_gate.get("warning") or "").strip() or None
        )
        if missing <= 0:
            status = "ready"
            provider_fill_status = "not_needed"
            next_action = "As-of market bars already cover this scope."
        elif provider_health_blocks_fill:
            status = "attention"
            provider_fill_status = "blocked_by_provider_health"
            next_action = (
                "Fill the manual CSV, or fix the Polygon/Massive provider health "
                "before requesting a grouped-daily fill."
            )
        elif self.provider_key_configured:
            status = "attention"
            provider_fill_status = (
                "ready_for_approval_with_health_warning"
                if provider_health_warning
                else "ready_for_approval"
            )
            if provider_health_warning:
                next_action = (
                    "Fill the manual CSV, or explicitly approve the one-call "
                    "Polygon/Massive grouped-daily fill after reviewing the stored "
                    "provider-health warning."
                )
            else:
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
        local_template_exists = self.local_template_path.exists()
        local_template_schema = _manual_bar_template_schema_payload(
            self.local_template_path,
        )
        operator_step = _manual_market_bars_operator_step(
            missing=missing,
            local_template_exists=local_template_exists,
            local_template_path=self.local_template_path,
            local_template_preview=self.local_template_preview,
            local_template_schema=local_template_schema,
            template_command=template_command,
            regenerate_template_command=regenerate_template_command,
            import_preview_command=import_preview_command,
            import_execute_command=import_execute_command,
            incremental_import_preview_command=incremental_import_preview_command,
            incremental_import_execute_command=incremental_import_execute_command,
        )
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
            "manual_template_regenerate_command": regenerate_template_command,
            "manual_import_preview_command": import_preview_command,
            "manual_import_execute_command": import_execute_command,
            "manual_incremental_import_preview_command": (
                incremental_import_preview_command
            ),
            "manual_incremental_import_execute_command": (
                incremental_import_execute_command
            ),
            "local_template_path": str(self.local_template_path),
            "local_template_exists": local_template_exists,
            "local_template_schema": local_template_schema,
            "local_template_preview": self.local_template_preview,
            "operator_step": operator_step,
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
            "provider_health": provider_health,
            "provider_health_blocks_fill": provider_health_blocks_fill,
            "provider_health_warning": provider_health_warning,
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


def _manual_market_bars_operator_step(
    *,
    missing: int,
    local_template_exists: bool,
    local_template_path: Path,
    local_template_preview: dict[str, object] | None,
    local_template_schema: dict[str, object],
    template_command: str,
    regenerate_template_command: str,
    import_preview_command: str,
    import_execute_command: str,
    incremental_import_preview_command: str,
    incremental_import_execute_command: str,
) -> dict[str, object]:
    if missing <= 0:
        return {
            "status": "ready",
            "kind": "rerun_audit",
            "action": "Market bars cover this scope. Rerun the priced-in audit.",
            "command": "catalyst-radar priced-in-audit --all --json",
            "after_manual_command": None,
            "manual_step": False,
            "external_calls_made": 0,
        }
    if not local_template_exists:
        return {
            "status": "needs_template",
            "kind": "generate_template",
            "action": "Generate the DB-backed missing-bar CSV for the full scope.",
            "command": template_command,
            "after_manual_command": import_preview_command,
            "manual_step": False,
            "external_calls_made": 0,
        }
    if not isinstance(local_template_preview, dict):
        return {
            "status": "needs_preview",
            "kind": "preview_template",
            "action": "Preview the local missing-bar CSV before importing.",
            "command": import_preview_command,
            "after_manual_command": None,
            "manual_step": False,
            "external_calls_made": 0,
        }

    preview_status = str(local_template_preview.get("status") or "").strip().lower()
    progress = local_template_preview.get("fill_progress")
    if not isinstance(progress, dict):
        progress = {}
    complete_rows = _int_payload_value(progress.get("complete_rows"))
    partial_rows = _int_payload_value(progress.get("partial_rows"))
    empty_rows = _int_payload_value(progress.get("empty_rows"))
    invalid_rows = _int_payload_value(local_template_preview.get("invalid_row_count"))
    filled_rows = complete_rows + partial_rows

    missing_context_columns = local_template_schema.get("missing_context_columns")
    if (
        isinstance(missing_context_columns, list)
        and missing_context_columns
        and filled_rows <= 0
    ):
        missing_label = ", ".join(str(item) for item in missing_context_columns)
        return {
            "status": "stale_template_schema",
            "kind": "regenerate_blank_template",
            "action": (
                f"Regenerate the blank local CSV so it includes {missing_label}; "
                "then fill the named rows."
            ),
            "command": regenerate_template_command,
            "after_manual_command": incremental_import_preview_command,
            "manual_step": False,
            "external_calls_made": 0,
        }

    if preview_status == "ready":
        return {
            "status": "ready_to_import",
            "kind": "execute_import",
            "action": "Import the complete full-scope market-bar CSV.",
            "command": local_template_preview.get("execute_command")
            or import_execute_command,
            "after_manual_command": None,
            "manual_step": False,
            "external_calls_made": 0,
        }
    if preview_status == "ready_partial":
        return {
            "status": "ready_to_import_complete_rows",
            "kind": "execute_incremental_import",
            "action": "Import the completed rows, then keep filling the remaining rows.",
            "command": local_template_preview.get("execute_command")
            or incremental_import_execute_command,
            "after_manual_command": None,
            "manual_step": False,
            "external_calls_made": 0,
        }
    if preview_status == "partial_imported":
        return {
            "status": "continue_manual_fill",
            "kind": "fill_remaining_rows",
            "action": "Continue filling the remaining missing-bar rows.",
            "command": None,
            "after_manual_command": incremental_import_preview_command,
            "manual_step": True,
            "external_calls_made": 0,
        }
    if partial_rows > 0:
        return {
            "status": "fix_partial_rows",
            "kind": "finish_or_clear_partial_rows",
            "action": (
                "Finish or clear partial OHLCV/VWAP rows in "
                f"{local_template_path}; partial rows cannot be imported."
            ),
            "command": import_preview_command,
            "after_manual_command": incremental_import_preview_command,
            "manual_step": True,
            "external_calls_made": 0,
        }
    if complete_rows <= 0 and empty_rows > 0:
        return {
            "status": "manual_fill_required",
            "kind": "fill_first_complete_rows",
            "action": (
                "Fill at least one complete OHLCV/VWAP row in "
                f"{local_template_path}; blank rows can wait."
            ),
            "command": None,
            "after_manual_command": incremental_import_preview_command,
            "manual_step": True,
            "external_calls_made": 0,
        }
    if invalid_rows > 0:
        return {
            "status": "fix_invalid_rows",
            "kind": "fix_csv_values",
            "action": "Fix invalid manual CSV values, then preview again.",
            "command": import_preview_command,
            "after_manual_command": incremental_import_preview_command,
            "manual_step": True,
            "external_calls_made": 0,
        }
    return {
        "status": "review_csv",
        "kind": "review_template",
        "action": "Review the manual CSV state before importing.",
        "command": import_preview_command,
        "after_manual_command": incremental_import_preview_command,
        "manual_step": False,
        "external_calls_made": 0,
    }


def _int_payload_value(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _manual_bar_provider_health_payload(
    *,
    status: str | None,
    reason: str | None,
    checked_at: datetime | None,
) -> dict[str, object] | None:
    if not status and not reason and checked_at is None:
        return None
    return {
        "provider": "polygon",
        "status": status,
        "reason": reason,
        "checked_at": checked_at.isoformat() if checked_at is not None else None,
    }


def manual_bar_provider_health_gate(
    *,
    status: str | None,
    reason: str | None,
    checked_at: datetime | None,
    target_as_of: date,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    normalized_status = str(status or "").strip().lower()
    if normalized_status != "down":
        return {
            "schema_version": "manual-market-bars-provider-health-gate-v1",
            "status": "clear",
            "blocks_provider_fill": False,
            "warning": None,
            "external_calls_made": 0,
        }
    resolved_at = _as_utc(generated_at or datetime.now(UTC))
    checked = _as_utc(checked_at) if checked_at is not None else None
    if _manual_bar_provider_health_is_stale_eod_denial(
        reason=reason,
        checked_at=checked,
        target_as_of=target_as_of,
        generated_at=resolved_at,
    ):
        return {
            "schema_version": "manual-market-bars-provider-health-gate-v1",
            "status": "warning",
            "blocks_provider_fill": False,
            "warning": (
                "Stored Polygon/Massive health is a stale same-day EOD denial; "
                "the target date is historical, so the grouped-daily command can "
                "be reviewed for explicit one-call approval."
            ),
            "external_calls_made": 0,
        }
    return {
        "schema_version": "manual-market-bars-provider-health-gate-v1",
        "status": "blocked",
        "blocks_provider_fill": True,
        "warning": None,
        "external_calls_made": 0,
    }


def _manual_bar_provider_health_is_stale_eod_denial(
    *,
    reason: str | None,
    checked_at: datetime | None,
    target_as_of: date,
    generated_at: datetime,
) -> bool:
    reason_text = str(reason or "").lower()
    if "before end of day" not in reason_text and "today's data" not in reason_text:
        return False
    current_date = generated_at.date()
    if target_as_of >= current_date:
        return False
    if checked_at is None:
        return True
    return target_as_of <= checked_at.date() or checked_at.date() < current_date


def write_manual_market_bars_template(
    engine: Engine,
    *,
    output_path: str | Path,
    expected_as_of: date,
    provider: str = "manual_csv",
    generated_at: datetime | None = None,
    missing_only: bool = False,
    stocks_only: bool = False,
    overwrite: bool = False,
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
    if path.exists() and not overwrite:
        filled_rows = _filled_required_row_count(path)
        if filled_rows:
            msg = (
                "refusing to overwrite manual market-bar template with "
                f"{filled_rows} row(s) containing filled OHLCV/VWAP values: {path}; "
                "rerun with --overwrite only after backing up or confirming the "
                "filled values are no longer needed"
            )
            raise ValueError(msg)
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
    provider_health_status: str | None = None,
    provider_health_reason: str | None = None,
    provider_health_checked_at: datetime | None = None,
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
        provider_health_status=provider_health_status,
        provider_health_reason=provider_health_reason,
        provider_health_checked_at=provider_health_checked_at,
        generated_at=_as_utc(generated_at or datetime.now(UTC)),
    )


def preview_manual_market_bars_import(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
    stocks_only: bool = False,
    complete_rows_only: bool = False,
) -> ManualBarsImportResult:
    path = Path(daily_bars_path)
    validation = _inspect_manual_bars_csv(
        path,
        expected_as_of=expected_as_of,
        complete_rows_only=complete_rows_only,
    )
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
            complete_required_row_count=validation.complete_required_row_count,
            partial_required_row_count=validation.partial_required_row_count,
            empty_required_row_count=validation.empty_required_row_count,
            invalid_examples=validation.invalid_examples,
            complete_rows_only=complete_rows_only,
        )
    try:
        bars = (
            _load_complete_manual_bars_csv(path)
            if complete_rows_only
            else tuple(load_daily_bars_csv(path))
        )
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
            complete_required_row_count=validation.complete_required_row_count,
            partial_required_row_count=validation.partial_required_row_count,
            empty_required_row_count=validation.empty_required_row_count,
            invalid_examples=(str(exc),),
            complete_rows_only=complete_rows_only,
        )
    if not bars:
        existing_at_expected: set[str] | None = None
        coverage_after_import: int | None = None
        missing: tuple[str, ...] = ()
        if expected_as_of is not None:
            existing_at_expected = _bar_tickers_for_date(engine, expected_as_of) & active
            coverage_after_import = len(existing_at_expected)
            missing = tuple(sorted(active - existing_at_expected))
        return ManualBarsImportResult(
            daily_bars_path=path,
            expected_as_of=expected_as_of,
            status="no_complete_rows" if complete_rows_only else "invalid",
            row_count=0,
            ticker_count=0,
            latest_bar_date=validation.latest_bar_date,
            active_security_count=len(active),
            existing_as_of_bar_count=(
                len(existing_at_expected) if existing_at_expected is not None else None
            ),
            coverage_after_import_count=coverage_after_import,
            bars_at_expected_as_of=0 if expected_as_of is not None else None,
            stocks_only=stocks_only,
            missing_expected_tickers=missing,
            complete_required_row_count=validation.complete_required_row_count,
            partial_required_row_count=validation.partial_required_row_count,
            empty_required_row_count=validation.empty_required_row_count,
            invalid_examples=("no complete manual market-bar rows were found",),
            complete_rows_only=complete_rows_only,
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
            status = "ready_partial" if complete_rows_only else "incomplete"
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
        complete_required_row_count=validation.complete_required_row_count,
        partial_required_row_count=validation.partial_required_row_count,
        empty_required_row_count=validation.empty_required_row_count,
        complete_rows_only=complete_rows_only,
        bars=bars,
    )


def import_manual_market_bars(
    engine: Engine,
    *,
    daily_bars_path: str | Path,
    expected_as_of: date | None = None,
    execute: bool = False,
    stocks_only: bool = False,
    complete_rows_only: bool = False,
) -> ManualBarsImportResult:
    preview = preview_manual_market_bars_import(
        engine,
        daily_bars_path=daily_bars_path,
        expected_as_of=expected_as_of,
        stocks_only=stocks_only,
        complete_rows_only=complete_rows_only,
    )
    if preview.status not in {"ready", "ready_partial"}:
        return preview
    if not execute:
        return preview
    MarketRepository(engine).upsert_daily_bars(preview.bars)
    return ManualBarsImportResult(
        daily_bars_path=preview.daily_bars_path,
        expected_as_of=preview.expected_as_of,
        status="partial_imported" if preview.status == "ready_partial" else "imported",
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
        complete_required_row_count=preview.complete_required_row_count,
        partial_required_row_count=preview.partial_required_row_count,
        empty_required_row_count=preview.empty_required_row_count,
        complete_rows_only=preview.complete_rows_only,
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


def _filled_required_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        count = 0
        for row in reader:
            if any(
                str(row.get(field_name) or "").strip()
                for field_name in MANUAL_BAR_REQUIRED_FILL_FIELDS
            ):
                count += 1
        return count


def _load_complete_manual_bars_csv(path: Path) -> tuple[DailyBar, ...]:
    bars: list[DailyBar] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not all(
                str(row.get(field_name) or "").strip()
                for field_name in MANUAL_BAR_REQUIRED_FILL_FIELDS
            ):
                continue
            bars.append(
                DailyBar(
                    ticker=str(row["ticker"]).strip().upper(),
                    date=date.fromisoformat(str(row["date"]).strip()),
                    open=float(str(row["open"]).strip()),
                    high=float(str(row["high"]).strip()),
                    low=float(str(row["low"]).strip()),
                    close=float(str(row["close"]).strip()),
                    volume=int(float(str(row["volume"]).strip())),
                    vwap=float(str(row["vwap"]).strip()),
                    adjusted=_manual_bar_bool(row["adjusted"]),
                    provider=str(row["provider"]).strip(),
                    source_ts=_manual_bar_datetime(row["source_ts"]),
                    available_at=_manual_bar_datetime(row["available_at"]),
                )
            )
    return tuple(bars)


def _manual_bar_bool(value: object) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for adjusted: {value!r}")


def _manual_bar_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _manual_market_bars_template_command(
    expected_as_of: date,
    *,
    stocks_only: bool,
    overwrite: bool = False,
) -> str:
    stocks_flag = " --stocks-only" if stocks_only else ""
    overwrite_flag = " --overwrite" if overwrite else ""
    return (
        "catalyst-radar market-bars template "
        f"--expected-as-of {expected_as_of.isoformat()} "
        f"--out {_manual_market_bars_template_path(expected_as_of, stocks_only=stocks_only)} "
        f"--missing-only{stocks_flag}{overwrite_flag}"
    )


def _manual_market_bars_import_command(
    expected_as_of: date,
    *,
    stocks_only: bool,
    execute: bool,
    complete_rows_only: bool = False,
) -> str:
    stocks_flag = " --stocks-only" if stocks_only else ""
    complete_rows_flag = " --complete-rows-only" if complete_rows_only else ""
    execute_flag = " --execute" if execute else ""
    template_path = _manual_market_bars_template_path(
        expected_as_of,
        stocks_only=stocks_only,
    )
    return (
        "catalyst-radar market-bars import "
        f"--daily-bars {template_path} "
        f"--expected-as-of {expected_as_of.isoformat()}"
        f"{stocks_flag}{complete_rows_flag}{execute_flag}"
    )


def _manual_market_bars_template_path(
    expected_as_of: date,
    *,
    stocks_only: bool,
) -> Path:
    filename_prefix = "manual-stock-bars" if stocks_only else "manual-bars"
    return Path("data") / "local" / f"{filename_prefix}-{expected_as_of.isoformat()}.csv"


def _manual_bar_template_schema_payload(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "schema_version": "manual-market-bars-template-schema-v1",
            "status": "missing",
            "path": str(path),
            "columns": [],
            "template_columns": list(MANUAL_BAR_COLUMNS),
            "missing_template_columns": list(MANUAL_BAR_COLUMNS),
            "missing_context_columns": list(MANUAL_BAR_CONTEXT_COLUMNS),
            "external_calls_made": 0,
        }
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = tuple(
            str(column).strip()
            for column in (reader.fieldnames or ())
            if str(column).strip()
        )
    missing_template_columns = [
        column for column in MANUAL_BAR_COLUMNS if column not in columns
    ]
    missing_context_columns = [
        column for column in MANUAL_BAR_CONTEXT_COLUMNS if column not in columns
    ]
    if missing_context_columns:
        status = "stale_context_columns"
    elif missing_template_columns:
        status = "missing_template_columns"
    else:
        status = "current"
    return {
        "schema_version": "manual-market-bars-template-schema-v1",
        "status": status,
        "path": str(path),
        "columns": list(columns),
        "template_columns": list(MANUAL_BAR_COLUMNS),
        "missing_template_columns": missing_template_columns,
        "missing_context_columns": missing_context_columns,
        "external_calls_made": 0,
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
    complete_required_row_count: int
    partial_required_row_count: int
    empty_required_row_count: int
    invalid_examples: tuple[str, ...]


def _inspect_manual_bars_csv(
    path: Path,
    *,
    expected_as_of: date | None,
    complete_rows_only: bool = False,
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
        complete_required_row_count = 0
        partial_required_row_count = 0
        empty_required_row_count = 0
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
            filled_required_fields = [
                field
                for field in MANUAL_BAR_REQUIRED_FILL_FIELDS
                if str(row.get(field) or "").strip()
            ]
            is_complete_required = (
                len(filled_required_fields) == len(MANUAL_BAR_REQUIRED_FILL_FIELDS)
            )
            is_empty_required = not filled_required_fields
            if (
                expected_as_of is not None
                and parsed_date == expected_as_of
                and (not complete_rows_only or is_complete_required)
            ):
                expected_date_tickers.add(ticker)
            if is_complete_required:
                complete_required_row_count += 1
            elif filled_required_fields:
                partial_required_row_count += 1
            else:
                empty_required_row_count += 1
            if complete_rows_only and is_empty_required:
                continue
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
        complete_required_row_count=complete_required_row_count,
        partial_required_row_count=partial_required_row_count,
        empty_required_row_count=empty_required_row_count,
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
