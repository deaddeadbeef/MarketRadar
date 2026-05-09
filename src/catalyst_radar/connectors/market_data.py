from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.connectors.csv_market import _to_bool
from catalyst_radar.core.immutability import freeze_mapping, thaw_json_value
from catalyst_radar.core.models import DataQualitySeverity

CSV_PROVIDER_NAME = "csv"
CSV_LICENSE_TAG = "local-csv-fixture"
CSV_RETENTION_POLICY = "retain-local-fixture"


@dataclass(frozen=True)
class RejectedPayload:
    provider: str
    kind: ConnectorRecordKind
    payload: Mapping[str, Any]
    reason: str
    severity: DataQualitySeverity = DataQualitySeverity.ERROR
    fail_closed_action: str = "reject-payload"

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))

    @property
    def affected_tickers(self) -> tuple[str, ...]:
        record = self.payload.get("record")
        if not isinstance(record, Mapping):
            return ()
        ticker = record.get("ticker")
        if _is_missing(ticker):
            return ()
        return (str(ticker).upper(),)


class CsvMarketDataConnector:
    def __init__(
        self,
        securities_path: str | Path,
        daily_bars_path: str | Path,
        holdings_path: str | Path | None = None,
        *,
        provider: str = CSV_PROVIDER_NAME,
        license_tag: str = CSV_LICENSE_TAG,
        retention_policy: str = CSV_RETENTION_POLICY,
    ) -> None:
        self.provider = provider
        self.securities_path = Path(securities_path)
        self.daily_bars_path = Path(daily_bars_path)
        self.holdings_path = Path(holdings_path) if holdings_path is not None else None
        self.license_tag = license_tag
        self.retention_policy = retention_policy
        self._rejected_payloads: list[RejectedPayload] = []

    @property
    def rejected_payloads(self) -> tuple[RejectedPayload, ...]:
        return tuple(self._rejected_payloads)

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        self._rejected_payloads = []
        fetched_at = datetime.now(UTC)
        request_hash = _hash_payload(
            {
                "provider": request.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "paths": self._configured_paths_payload(),
            }
        )
        records: list[RawRecord] = []
        records.extend(
            self._fetch_path(
                kind=ConnectorRecordKind.SECURITY,
                path=self.securities_path,
                request_hash=request_hash,
                fetched_at=fetched_at,
                timestamp_fields=("updated_at",),
            )
        )
        records.extend(
            self._fetch_path(
                kind=ConnectorRecordKind.DAILY_BAR,
                path=self.daily_bars_path,
                request_hash=request_hash,
                fetched_at=fetched_at,
                timestamp_fields=("source_ts", "available_at"),
            )
        )
        if self.holdings_path is not None and self.holdings_path.exists():
            records.extend(
                self._fetch_path(
                    kind=ConnectorRecordKind.HOLDING,
                    path=self.holdings_path,
                    request_hash=request_hash,
                    fetched_at=fetched_at,
                    timestamp_fields=("as_of",),
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            payload = _record_payload(record)
            if record.kind == ConnectorRecordKind.SECURITY:
                identity = str(payload["ticker"]).upper()
                normalized_payload = _normalize_security_payload(payload)
            elif record.kind == ConnectorRecordKind.DAILY_BAR:
                identity = f"{str(payload['ticker']).upper()}:{payload['date']}"
                normalized_payload = _normalize_daily_bar_payload(payload)
            elif record.kind == ConnectorRecordKind.HOLDING:
                identity = f"{str(payload['ticker']).upper()}:{payload['as_of']}"
                normalized_payload = _normalize_holding_payload(payload)
            else:
                continue
            normalized.append(
                NormalizedRecord(
                    provider=record.provider,
                    kind=record.kind,
                    identity=identity,
                    payload=normalized_payload,
                    source_ts=record.source_ts,
                    available_at=record.available_at,
                    raw_payload_hash=record.payload_hash,
                )
            )
        return normalized

    def healthcheck(self) -> ConnectorHealth:
        missing_required = [
            str(path)
            for path in (self.securities_path, self.daily_bars_path)
            if not path.exists()
        ]
        if missing_required:
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason=f"missing required csv path(s): {', '.join(missing_required)}",
            )
        if self.holdings_path is not None and not self.holdings_path.exists():
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.DEGRADED,
                checked_at=datetime.now(UTC),
                reason=f"missing optional holdings csv path: {self.holdings_path}",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime.now(UTC),
            reason="configured csv paths are readable",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        return ProviderCostEstimate(
            provider=self.provider,
            request_count=len(self._configured_paths_payload()),
            estimated_cost_usd=0.0,
        )

    def _fetch_path(
        self,
        *,
        kind: ConnectorRecordKind,
        path: Path,
        request_hash: str,
        fetched_at: datetime,
        timestamp_fields: tuple[str, ...],
    ) -> list[RawRecord]:
        rows: list[RawRecord] = []
        frame = pd.read_csv(path)
        for index, record in enumerate(frame.to_dict(orient="records"), start=2):
            clean_record = _clean_record(record)
            missing = [field for field in timestamp_fields if _is_missing(clean_record.get(field))]
            raw_payload = _raw_payload(kind, path, index, clean_record)
            if missing:
                self._reject(
                    kind,
                    raw_payload,
                    f"missing mandatory timestamp field(s): {', '.join(missing)}",
                )
                continue
            try:
                source_ts, available_at = _timestamps_for_kind(kind, clean_record)
            except (TypeError, ValueError) as exc:
                self._reject(kind, raw_payload, f"invalid mandatory timestamp field: {exc}")
                continue
            if source_ts > fetched_at:
                self._reject(kind, raw_payload, "source_ts is later than actual fetch time")
                continue
            try:
                _validate_required_values(kind, clean_record)
            except (TypeError, ValueError) as exc:
                self._reject(kind, raw_payload, str(exc))
                continue
            try:
                rows.append(
                    RawRecord(
                        provider=self.provider,
                        kind=kind,
                        request_hash=request_hash,
                        payload_hash=_hash_payload(raw_payload),
                        payload=raw_payload,
                        source_ts=source_ts,
                        fetched_at=fetched_at,
                        available_at=available_at,
                        license_tag=self.license_tag,
                        retention_policy=self.retention_policy,
                    )
                )
            except ValueError as exc:
                self._reject(kind, raw_payload, str(exc))
        return rows

    def _reject(self, kind: ConnectorRecordKind, payload: Mapping[str, Any], reason: str) -> None:
        severity = DataQualitySeverity.ERROR
        fail_closed_action = "reject-payload"
        if kind == ConnectorRecordKind.DAILY_BAR and "available_at" in reason:
            severity = DataQualitySeverity.CRITICAL
            fail_closed_action = "abort-ingest"
        self._rejected_payloads.append(
            RejectedPayload(
                provider=self.provider,
                kind=kind,
                payload=payload,
                reason=reason,
                severity=severity,
                fail_closed_action=fail_closed_action,
            )
        )

    def _configured_paths_payload(self) -> list[str]:
        paths = [self.securities_path, self.daily_bars_path]
        if self.holdings_path is not None:
            paths.append(self.holdings_path)
        return [str(path) for path in paths]


def _timestamps_for_kind(
    kind: ConnectorRecordKind,
    record: Mapping[str, Any],
) -> tuple[datetime, datetime]:
    if kind == ConnectorRecordKind.SECURITY:
        updated_at = _to_strict_utc_datetime(record["updated_at"], "updated_at")
        return updated_at, updated_at
    if kind == ConnectorRecordKind.DAILY_BAR:
        return (
            _to_strict_utc_datetime(record["source_ts"], "source_ts"),
            _to_strict_utc_datetime(record["available_at"], "available_at"),
        )
    if kind == ConnectorRecordKind.HOLDING:
        as_of = _to_strict_utc_datetime(record["as_of"], "as_of")
        return as_of, as_of
    msg = f"unsupported csv record kind: {kind}"
    raise ValueError(msg)


def _record_payload(record: RawRecord) -> Mapping[str, Any]:
    payload = record.payload.get("record")
    if not isinstance(payload, Mapping):
        msg = "raw csv payload must contain a record mapping"
        raise ValueError(msg)
    return payload


def _normalize_security_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": str(record["ticker"]).upper(),
        "name": str(record["name"]),
        "exchange": str(record["exchange"]),
        "sector": str(record["sector"]),
        "industry": str(record["industry"]),
        "market_cap": float(record["market_cap"]),
        "avg_dollar_volume_20d": float(record["avg_dollar_volume_20d"]),
        "has_options": _to_bool(record["has_options"], "has_options"),
        "is_active": _to_bool(record["is_active"], "is_active"),
        "updated_at": _to_strict_utc_datetime(record["updated_at"], "updated_at").isoformat(),
    }


def _normalize_daily_bar_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": str(record["ticker"]).upper(),
        "date": pd.Timestamp(record["date"]).date().isoformat(),
        "open": float(record["open"]),
        "high": float(record["high"]),
        "low": float(record["low"]),
        "close": float(record["close"]),
        "volume": int(record["volume"]),
        "vwap": float(record["vwap"]),
        "adjusted": _to_bool(record["adjusted"], "adjusted"),
        "provider": str(record["provider"]),
        "source_ts": _to_strict_utc_datetime(record["source_ts"], "source_ts").isoformat(),
        "available_at": _to_strict_utc_datetime(
            record["available_at"], "available_at"
        ).isoformat(),
    }


def _normalize_holding_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": str(record["ticker"]).upper(),
        "shares": float(record["shares"]),
        "market_value": float(record["market_value"]),
        "sector": str(record["sector"]),
        "theme": str(record["theme"]),
        "as_of": _to_strict_utc_datetime(record["as_of"], "as_of").isoformat(),
        "portfolio_value": _optional_float(record, "portfolio_value"),
        "cash": _optional_float(record, "cash"),
    }


def _validate_required_values(kind: ConnectorRecordKind, record: Mapping[str, Any]) -> None:
    if kind == ConnectorRecordKind.SECURITY:
        _require_fields(
            record,
            (
                "ticker",
                "name",
                "exchange",
                "sector",
                "industry",
                "market_cap",
                "avg_dollar_volume_20d",
                "has_options",
                "is_active",
            ),
        )
        float(record["market_cap"])
        float(record["avg_dollar_volume_20d"])
        _to_bool(record["has_options"], "has_options")
        _to_bool(record["is_active"], "is_active")
        return
    if kind == ConnectorRecordKind.DAILY_BAR:
        _require_fields(
            record,
            (
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
            ),
        )
        pd.Timestamp(record["date"]).date()
        float(record["open"])
        float(record["high"])
        float(record["low"])
        float(record["close"])
        int(record["volume"])
        float(record["vwap"])
        _to_bool(record["adjusted"], "adjusted")
        return
    if kind == ConnectorRecordKind.HOLDING:
        _require_fields(record, ("ticker", "shares", "market_value", "sector", "theme"))
        float(record["shares"])
        float(record["market_value"])
        _optional_float(record, "portfolio_value")
        _optional_float(record, "cash")
        return
    msg = f"unsupported csv record kind: {kind}"
    raise ValueError(msg)


def _require_fields(record: Mapping[str, Any], fields: tuple[str, ...]) -> None:
    missing = [field for field in fields if _is_missing(record.get(field))]
    if missing:
        msg = f"missing mandatory field(s): {', '.join(missing)}"
        raise ValueError(msg)


def _raw_payload(
    kind: ConnectorRecordKind,
    path: Path,
    row_number: int,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source": "csv",
        "kind": kind.value,
        "path": str(path),
        "row_number": row_number,
        "record": dict(record),
    }


def _clean_record(record: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in record.items():
        if _is_missing(value):
            cleaned[str(key)] = None
        else:
            cleaned[str(key)] = value
    return cleaned


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _optional_float(record: Mapping[str, Any], field: str) -> float:
    value = record.get(field)
    if _is_missing(value):
        return 0.0
    return float(value)


def _to_strict_utc_datetime(value: object, field: str) -> datetime:
    parsed = pd.Timestamp(value).to_pydatetime()
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"{field} must include timezone information"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


__all__ = [
    "CSV_LICENSE_TAG",
    "CSV_PROVIDER_NAME",
    "CSV_RETENTION_POLICY",
    "CsvMarketDataConnector",
    "RejectedPayload",
]
