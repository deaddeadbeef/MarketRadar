from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode

from sqlalchemy import select

from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars


def preview_polygon_grouped_daily_fixture(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    date_value: date,
    fixture_path: Path,
) -> dict[str, object]:
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=JsonHttpClient(
            transport=_grouped_daily_fixture_transport(
                config=config,
                date_value=date_value,
                fixture_path=fixture_path,
            ),
            timeout_seconds=config.http_timeout_seconds,
        ),
        base_url=config.polygon_base_url,
        availability_policy=config.provider_availability_policy,
        ticker_page_delay_seconds=config.polygon_ticker_page_delay_seconds,
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=PolygonEndpoint.GROUPED_DAILY.value,
        params={
            "date": date_value.isoformat(),
            "adjusted": True,
            "include_otc": False,
        },
        requested_at=datetime.now(UTC),
    )
    raw_records = connector.fetch(request)
    rejections = tuple(getattr(connector, "rejected_payloads", ()))
    normalized_records = connector.normalize(raw_records)
    daily_bar_records = [
        record
        for record in normalized_records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    target_date = date_value.isoformat()
    date_mismatches = [
        str(record.payload.get("ticker") or record.identity)
        for record in daily_bar_records
        if str(record.payload.get("date") or "") != target_date
    ]
    target_daily_bar_records = [
        record
        for record in daily_bar_records
        if str(record.payload.get("date") or "") == target_date
    ]
    fixture_tickers = {
        str(record.payload.get("ticker") or "").upper()
        for record in target_daily_bar_records
        if str(record.payload.get("ticker") or "").strip()
    }
    abort_rejection_count = _provider_abort_rejection_count(rejections)
    coverage = _polygon_fixture_coverage_payload(
        market_repo=market_repo,
        date_value=date_value,
        fixture_tickers=fixture_tickers,
    )
    if abort_rejection_count or date_mismatches or not target_daily_bar_records:
        status = "invalid"
    elif rejections:
        status = "ready_with_rejections"
    else:
        status = "ready"
    return {
        "schema_version": "polygon-grouped-daily-fixture-preview-v1",
        "status": status,
        "provider": "polygon",
        "date": target_date,
        "fixture_path": str(fixture_path),
        "requested_count": len(raw_records) + len(rejections),
        "raw_count": len(raw_records),
        "normalized_count": len(normalized_records),
        "daily_bar_count": len(target_daily_bar_records),
        "rejected_count": len(rejections),
        "abort_rejection_count": abort_rejection_count,
        "date_mismatch_count": len(date_mismatches),
        "date_mismatch_sample": date_mismatches[:12],
        "coverage": coverage,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "import_command": (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {target_date} --fixture {fixture_path}"
        ),
        "next_action": _polygon_fixture_preview_next_action(
            status=status,
            coverage=coverage,
        ),
    }


def _grouped_daily_fixture_transport(
    *,
    config: AppConfig,
    date_value: date,
    fixture_path: Path,
) -> FakeHttpTransport:
    url = _polygon_grouped_daily_url(
        config=config,
        date_value=date_value,
        api_key="fixture-key",
    )
    return FakeHttpTransport(
        {
            url: HttpResponse(
                status_code=200,
                url=url,
                headers={"content-type": "application/json"},
                body=fixture_path.read_bytes(),
            ),
        },
    )


def _polygon_grouped_daily_url(
    *,
    config: AppConfig,
    date_value: date,
    api_key: str,
) -> str:
    query = urlencode(
        {
            "adjusted": "true",
            "include_otc": "false",
            "apiKey": api_key,
        },
    )
    base_url = config.polygon_base_url.rstrip("/")
    return f"{base_url}/v2/aggs/grouped/locale/us/market/stocks/{date_value.isoformat()}?{query}"


def _polygon_fixture_coverage_payload(
    *,
    market_repo: MarketRepository,
    date_value: date,
    fixture_tickers: set[str],
) -> dict[str, object]:
    active_securities = market_repo.list_active_securities()
    active_tickers = {security.ticker.upper() for security in active_securities}
    stock_like_tickers = {
        security.ticker.upper()
        for security in active_securities
        if _is_stock_like_security(security.metadata)
    }
    with market_repo.engine.connect() as conn:
        existing_tickers = {
            str(row.ticker).upper()
            for row in conn.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == date_value),
            )
        }
    missing_before = active_tickers - existing_tickers
    fixture_active = fixture_tickers & active_tickers
    covered_missing = fixture_active & missing_before
    missing_after = missing_before - covered_missing

    stock_missing_before = stock_like_tickers - existing_tickers
    stock_fixture_active = fixture_tickers & stock_like_tickers
    stock_covered_missing = stock_fixture_active & stock_missing_before
    stock_missing_after = stock_missing_before - stock_covered_missing
    return {
        "active_security_count": len(active_tickers),
        "existing_as_of_bar_count": len(active_tickers & existing_tickers),
        "missing_before_count": len(missing_before),
        "fixture_as_of_bar_count": len(fixture_tickers),
        "fixture_active_match_count": len(fixture_active),
        "missing_covered_by_fixture_count": len(covered_missing),
        "missing_after_import_count": len(missing_after),
        "missing_covered_sample": sorted(covered_missing)[:12],
        "fixture_outside_active_count": len(fixture_tickers - active_tickers),
        "stock_like_active_count": len(stock_like_tickers),
        "stock_like_existing_as_of_bar_count": len(
            stock_like_tickers & existing_tickers,
        ),
        "stock_like_missing_before_count": len(stock_missing_before),
        "stock_like_covered_by_fixture_count": len(stock_covered_missing),
        "stock_like_missing_after_import_count": len(stock_missing_after),
        "stock_like_covered_sample": sorted(stock_covered_missing)[:12],
    }


def _is_stock_like_security(metadata: Mapping[str, object]) -> bool:
    security_type = str(metadata.get("type") or metadata.get("security_type") or "")
    return security_type.strip().upper() in {"CS", "ADRC"}


def _provider_abort_rejection_count(rejections: Sequence[object]) -> int:
    count = 0
    for rejected in rejections:
        severity = getattr(rejected, "severity", "")
        severity_value = getattr(severity, "value", severity)
        if (
            str(severity_value) == "critical"
            or str(getattr(rejected, "fail_closed_action", "")) == "abort-ingest"
        ):
            count += 1
    return count


def _polygon_fixture_preview_next_action(
    *,
    status: str,
    coverage: Mapping[str, object],
) -> str:
    if status == "invalid":
        return "Fix the saved grouped-daily JSON file, then rerun --validate-only."
    covered = int(coverage.get("missing_covered_by_fixture_count") or 0)
    missing_after = int(coverage.get("missing_after_import_count") or 0)
    active_count = int(coverage.get("active_security_count") or 0)
    if active_count <= 0:
        return "Preview is valid; import only after the active universe exists locally."
    if covered <= 0:
        return (
            "Preview is valid but covers no currently missing active tickers; "
            "check date and universe before importing."
        )
    if missing_after <= 0:
        return "Preview covers the missing active bars; run the import command if intended."
    return (
        "Preview covers some missing bars; run the import command if intended, "
        "then repair the remaining gaps."
    )


__all__ = ["preview_polygon_grouped_daily_fixture"]
