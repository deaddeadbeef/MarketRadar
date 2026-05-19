from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.core.immutability import thaw_json_value

OPTIONS_PROVIDER_NAME = "options_fixture"
OPTIONS_LICENSE_TAG = "options-fixture"
OPTIONS_RETENTION_POLICY = "local-fixture-retain"
OPTIONS_FIXTURE_TEMPLATE_RESULT_FIELDS = (
    "ticker",
    "call_volume",
    "put_volume",
    "call_open_interest",
    "put_open_interest",
    "iv_percentile",
    "skew",
)
OPTIONS_FIXTURE_NUMERIC_FIELDS = tuple(
    field for field in OPTIONS_FIXTURE_TEMPLATE_RESULT_FIELDS if field != "ticker"
)


@dataclass(frozen=True)
class OptionsFixtureTemplateWriteResult:
    output_path: Path
    row_count: int
    generated_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "options-fixture-template-write-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "output_path": str(self.output_path),
            "row_count": self.row_count,
            "generated_at": self.generated_at.isoformat(),
            "import_command": f"catalyst-radar ingest-options --fixture {self.output_path}",
            "next_action": (
                "Fill the aggregate option fields for each ticker, then import "
                "the completed point-in-time fixture."
            ),
        }


@dataclass(frozen=True)
class OptionsFixtureValidationResult:
    path: Path
    status: str
    row_count: int
    valid_row_count: int
    invalid_row_count: int
    blank_required_count: int
    invalid_numeric_count: int
    missing_field_count: int
    duplicate_ticker_count: int
    generated_at: datetime
    as_of: str | None
    provider: str | None
    errors: tuple[str, ...]

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "options-fixture-validation-v1",
            "status": self.status,
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "path": str(self.path),
            "row_count": self.row_count,
            "valid_row_count": self.valid_row_count,
            "invalid_row_count": self.invalid_row_count,
            "blank_required_count": self.blank_required_count,
            "invalid_numeric_count": self.invalid_numeric_count,
            "missing_field_count": self.missing_field_count,
            "duplicate_ticker_count": self.duplicate_ticker_count,
            "as_of": self.as_of,
            "fixture_provider": self.provider,
            "generated_at": self.generated_at.isoformat(),
            "errors": list(self.errors),
            "import_command": (
                f"catalyst-radar ingest-options --fixture {self.path}"
                if self.status == "ready"
                else None
            ),
            "next_action": (
                "Import the validated point-in-time options fixture."
                if self.status == "ready"
                else "Fix blank or invalid option fields, then validate again before import."
            ),
        }


def write_options_fixture_template_json(
    output_path: str | Path,
    fixture: Mapping[str, object],
    *,
    generated_at: datetime | None = None,
) -> OptionsFixtureTemplateWriteResult:
    payload = dict(_mapping(fixture, "fixture"))
    results = payload.get("results")
    if not isinstance(results, list):
        msg = "options fixture template results must be a list"
        raise ValueError(msg)

    path = Path(output_path)
    resolved_at = (generated_at or datetime.now(UTC)).astimezone(UTC).replace(
        microsecond=0
    )
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return OptionsFixtureTemplateWriteResult(
        output_path=path,
        row_count=len(results),
        generated_at=resolved_at,
    )


def validate_options_fixture_json(
    fixture_path: str | Path,
    *,
    expected_as_of: date | datetime | str | None = None,
    max_errors: int = 20,
) -> OptionsFixtureValidationResult:
    path = Path(fixture_path)
    checked_at = datetime.now(UTC).replace(microsecond=0)
    errors: list[str] = []
    payload: Mapping[str, Any] = {}
    if not path.exists():
        errors.append(f"fixture file does not exist: {path}")
        return _options_fixture_validation_result(
            path=path,
            checked_at=checked_at,
            errors=errors,
            max_errors=max_errors,
        )
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw_payload = json.load(handle)
        payload = _mapping(raw_payload, "fixture")
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        errors.append(f"fixture could not be read: {exc}")
        return _options_fixture_validation_result(
            path=path,
            checked_at=checked_at,
            errors=errors,
            max_errors=max_errors,
        )

    as_of_text = _header_datetime_text(payload, "as_of", errors)
    _header_datetime_text(payload, "source_ts", errors)
    _header_datetime_text(payload, "available_at", errors)
    expected_date = _expected_as_of_date(expected_as_of)
    if expected_date is not None and as_of_text:
        as_of = _parse_datetime(as_of_text, "as_of")
        if as_of.date() != expected_date:
            errors.append(
                f"header as_of date {as_of.date().isoformat()} does not match "
                f"expected {expected_date.isoformat()}"
            )

    results = payload.get("results")
    if not isinstance(results, list):
        errors.append("results must be a list")
        results = []

    seen_tickers: set[str] = set()
    valid_rows = 0
    blank_required_count = 0
    invalid_numeric_count = 0
    missing_field_count = 0
    duplicate_ticker_count = 0
    for index, result in enumerate(results, start=1):
        row_errors: list[str] = []
        if not isinstance(result, Mapping):
            row_errors.append("row must be a mapping")
            _append_limited(errors, f"row {index}: {'; '.join(row_errors)}", max_errors)
            continue
        ticker = str(result.get("ticker") or "").strip().upper()
        if not ticker:
            blank_required_count += 1
            row_errors.append("ticker is blank")
        elif ticker in seen_tickers:
            duplicate_ticker_count += 1
            row_errors.append(f"duplicate ticker {ticker}")
        else:
            seen_tickers.add(ticker)
        for field in OPTIONS_FIXTURE_NUMERIC_FIELDS:
            if field not in result:
                missing_field_count += 1
                row_errors.append(f"{field} is missing")
                continue
            value = result.get(field)
            if str(value).strip() == "":
                blank_required_count += 1
                row_errors.append(f"{field} is blank")
                continue
            parsed = _finite_number(value)
            if parsed is None:
                invalid_numeric_count += 1
                row_errors.append(f"{field} is not a finite number")
                continue
            if field != "skew" and parsed < 0:
                invalid_numeric_count += 1
                row_errors.append(f"{field} must be nonnegative")
            if field == "iv_percentile" and not 0 <= parsed <= 1:
                invalid_numeric_count += 1
                row_errors.append("iv_percentile must be between 0 and 1")
        if row_errors:
            label = ticker or f"#{index}"
            _append_limited(errors, f"row {index} {label}: {'; '.join(row_errors)}", max_errors)
        else:
            valid_rows += 1

    return _options_fixture_validation_result(
        path=path,
        checked_at=checked_at,
        errors=errors,
        row_count=len(results),
        valid_row_count=valid_rows,
        blank_required_count=blank_required_count,
        invalid_numeric_count=invalid_numeric_count,
        missing_field_count=missing_field_count,
        duplicate_ticker_count=duplicate_ticker_count,
        as_of=as_of_text,
        provider=str(payload.get("provider") or "") or None,
        max_errors=max_errors,
    )


class OptionsAggregateConnector:
    def __init__(
        self,
        *,
        fixture_path: str | Path,
        provider: str = OPTIONS_PROVIDER_NAME,
    ) -> None:
        self.fixture_path = Path(fixture_path)
        self.provider = provider

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        payload = self._load_payload()
        header = _fixture_header(payload)
        provider = str(header.get("provider") or self.provider)
        source_ts = _parse_datetime(header["source_ts"], "source_ts")
        available_at = _parse_datetime(header["available_at"], "available_at")
        as_of = _parse_datetime(header["as_of"], "as_of")
        request_hash = _hash_payload(
            {
                "provider": request.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "fixture_path": str(self.fixture_path),
            }
        )
        results = payload.get("results")
        if not isinstance(results, list):
            msg = "options fixture results must be a list"
            raise ValueError(msg)

        records: list[RawRecord] = []
        for index, result in enumerate(results, start=1):
            row = dict(_mapping(result, "result"))
            row_source_ts = _parse_datetime(row.get("source_ts", source_ts), "source_ts")
            row_available_at = _parse_datetime(
                row.get("available_at", available_at),
                "available_at",
            )
            row_as_of = _parse_datetime(row.get("as_of", as_of), "as_of")
            raw_payload = {
                "source": provider,
                "kind": ConnectorRecordKind.OPTION_FEATURE.value,
                "path": str(self.fixture_path),
                "row_number": index,
                "header": {
                    "as_of": as_of.isoformat(),
                    "source_ts": source_ts.isoformat(),
                    "available_at": available_at.isoformat(),
                    "provider": provider,
                },
                "record": {
                    **row,
                    "as_of": row_as_of.isoformat(),
                    "source_ts": row_source_ts.isoformat(),
                    "available_at": row_available_at.isoformat(),
                    "provider": provider,
                },
            }
            records.append(
                RawRecord(
                    provider=provider,
                    kind=ConnectorRecordKind.OPTION_FEATURE,
                    request_hash=request_hash,
                    payload_hash=_hash_payload(raw_payload),
                    payload=raw_payload,
                    source_ts=row_source_ts,
                    fetched_at=max(request.requested_at, row_source_ts),
                    available_at=row_available_at,
                    license_tag=OPTIONS_LICENSE_TAG,
                    retention_policy=OPTIONS_RETENTION_POLICY,
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            if record.kind != ConnectorRecordKind.OPTION_FEATURE:
                continue
            payload = _mapping(record.payload.get("record"), "record")
            ticker = str(payload["ticker"]).upper()
            as_of = _parse_datetime(payload["as_of"], "as_of")
            option_payload = {
                "ticker": ticker,
                "as_of": as_of.isoformat(),
                "provider": str(payload.get("provider") or record.provider),
                "call_volume": float(payload["call_volume"]),
                "put_volume": float(payload["put_volume"]),
                "call_open_interest": float(payload["call_open_interest"]),
                "put_open_interest": float(payload["put_open_interest"]),
                "iv_percentile": float(payload["iv_percentile"]),
                "skew": float(payload["skew"]),
                "source_ts": record.source_ts.isoformat(),
                "available_at": record.available_at.isoformat(),
                "payload": {
                    "fixture_path": str(self.fixture_path),
                    "raw_record": thaw_json_value(payload),
                },
            }
            normalized.append(
                NormalizedRecord(
                    provider=record.provider,
                    kind=ConnectorRecordKind.OPTION_FEATURE,
                    identity=f"{ticker}:{as_of.isoformat()}",
                    payload=option_payload,
                    source_ts=record.source_ts,
                    available_at=record.available_at,
                    raw_payload_hash=record.payload_hash,
                )
            )
        return normalized

    def healthcheck(self) -> ConnectorHealth:
        if self.fixture_path.exists():
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.HEALTHY,
                checked_at=datetime.now(UTC),
                reason="options fixture path is readable",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime.now(UTC),
            reason=f"missing options fixture path: {self.fixture_path}",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        return ProviderCostEstimate(
            provider=request.provider,
            request_count=1,
            estimated_cost_usd=0.0,
        )

    def _load_payload(self) -> Mapping[str, Any]:
        with self.fixture_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return _mapping(payload, "fixture")


def _fixture_header(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for field in ("as_of", "source_ts", "available_at"):
        if field not in payload:
            msg = f"options fixture missing header field: {field}"
            raise ValueError(msg)
    return payload


def _options_fixture_validation_result(
    *,
    path: Path,
    checked_at: datetime,
    errors: Sequence[str],
    row_count: int = 0,
    valid_row_count: int = 0,
    blank_required_count: int = 0,
    invalid_numeric_count: int = 0,
    missing_field_count: int = 0,
    duplicate_ticker_count: int = 0,
    as_of: str | None = None,
    provider: str | None = None,
    max_errors: int = 20,
) -> OptionsFixtureValidationResult:
    visible_errors = tuple(list(errors)[: max(1, max_errors)])
    invalid_row_count = max(0, row_count - valid_row_count)
    status = "ready" if row_count > 0 and not errors else "invalid"
    return OptionsFixtureValidationResult(
        path=path,
        status=status,
        row_count=row_count,
        valid_row_count=valid_row_count,
        invalid_row_count=invalid_row_count,
        blank_required_count=blank_required_count,
        invalid_numeric_count=invalid_numeric_count,
        missing_field_count=missing_field_count,
        duplicate_ticker_count=duplicate_ticker_count,
        generated_at=checked_at,
        as_of=as_of,
        provider=provider,
        errors=visible_errors,
    )


def _header_datetime_text(
    payload: Mapping[str, Any],
    field_name: str,
    errors: list[str],
) -> str | None:
    if field_name not in payload:
        errors.append(f"header {field_name} is missing")
        return None
    try:
        return _parse_datetime(payload[field_name], field_name).isoformat()
    except ValueError as exc:
        errors.append(str(exc))
        return None


def _expected_as_of_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def _finite_number(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _append_limited(values: list[str], value: str, limit: int) -> None:
    if len(values) < max(1, limit):
        values.append(value)


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise ValueError(msg)
    return value


def _parse_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


__all__ = [
    "OPTIONS_LICENSE_TAG",
    "OPTIONS_FIXTURE_TEMPLATE_RESULT_FIELDS",
    "OPTIONS_FIXTURE_NUMERIC_FIELDS",
    "OPTIONS_PROVIDER_NAME",
    "OPTIONS_RETENTION_POLICY",
    "OptionsFixtureValidationResult",
    "OptionsFixtureTemplateWriteResult",
    "OptionsAggregateConnector",
    "validate_options_fixture_json",
    "write_options_fixture_template_json",
]
