from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode

from sqlalchemy import select

from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.redaction import redact_text, redact_url
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars


def preview_polygon_grouped_daily_fixture(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    date_value: date,
    fixture_path: Path,
) -> dict[str, object]:
    connector, request, _, _ = build_polygon_grouped_daily_fixture_ingest(
        config=config,
        date_value=date_value,
        fixture_path=fixture_path,
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


def ingest_polygon_grouped_daily_fixture(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    date_value: date,
    fixture_path: Path,
) -> ProviderIngestResult:
    connector, request, metadata, job_type = build_polygon_grouped_daily_fixture_ingest(
        config=config,
        date_value=date_value,
        fixture_path=fixture_path,
    )
    return ingest_provider_records(
        connector=connector,
        request=request,
        market_repo=market_repo,
        provider_repo=provider_repo,
        job_type=job_type,
        metadata=metadata,
    )


def capture_polygon_grouped_daily_response(
    *,
    config: AppConfig,
    date_value: date,
    output_path: Path,
    fixture_path: Path | None = None,
    confirm_external_call: bool = False,
) -> dict[str, object]:
    if fixture_path is None and not config.polygon_api_key_configured:
        raise ValueError("missing CATALYST_POLYGON_API_KEY")
    if fixture_path is None and not confirm_external_call:
        raise PermissionError(
            "polygon grouped-daily response capture requires confirm_external_call=true",
        )
    api_key = "fixture-key" if fixture_path is not None else config.polygon_api_key
    url = _polygon_grouped_daily_url(
        config=config,
        date_value=date_value,
        api_key=api_key,
    )
    transport = (
        FakeHttpTransport(
            {
                url: HttpResponse(
                    status_code=200,
                    url=url,
                    headers={"content-type": "application/json"},
                    body=fixture_path.read_bytes(),
                ),
            },
        )
        if fixture_path is not None
        else UrlLibHttpTransport()
    )
    response = transport.get(
        url,
        headers={},
        timeout_seconds=config.http_timeout_seconds,
    )
    if response.status_code < 200 or response.status_code >= 300:
        detail = redact_text(response.body.decode("utf-8", errors="replace").strip())
        msg = f"HTTP {response.status_code} from {redact_url(response.url)}"
        if detail:
            msg = f"{msg}; detail={detail}"
        raise RuntimeError(msg)
    try:
        json.loads(response.body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        msg = f"invalid JSON from {redact_url(response.url)}"
        raise RuntimeError(msg) from exc
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.body)
    target_date = date_value.isoformat()
    return {
        "schema_version": "polygon-grouped-daily-response-capture-v1",
        "status": "ready",
        "provider": "polygon",
        "date": target_date,
        "source": "fixture" if fixture_path is not None else "live_provider",
        "url": redact_url(response.url),
        "output_path": str(output_path),
        "bytes_written": len(response.body),
        "status_code": response.status_code,
        "external_calls_made": 0 if fixture_path is not None else 1,
        "db_writes_made": 0,
        "validate_command": (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {target_date} --fixture {output_path} --validate-only"
        ),
        "import_command": (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {target_date} --fixture {output_path}"
        ),
        "next_action": (
            "Run the validate command, then import only if the preview covers "
            "the missing market bars."
        ),
    }

def capture_polygon_grouped_daily_response_with_preview(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    date_value: date,
    output_path: Path,
    fixture_path: Path | None = None,
    confirm_external_call: bool = False,
) -> dict[str, object]:
    payload = capture_polygon_grouped_daily_response(
        config=config,
        date_value=date_value,
        output_path=output_path,
        fixture_path=fixture_path,
        confirm_external_call=confirm_external_call,
    )
    preview = preview_polygon_grouped_daily_fixture(
        config=config,
        market_repo=market_repo,
        date_value=date_value,
        fixture_path=output_path,
    )
    return {
        **payload,
        "post_capture_preview": preview,
        "post_capture_external_calls_made": int(
            preview.get("external_calls_made") or 0,
        ),
        "post_capture_db_writes_made": int(preview.get("db_writes_made") or 0),
        "next_action": _polygon_capture_with_preview_next_action(preview),
    }

def build_polygon_grouped_daily_fixture_ingest(
    *,
    config: AppConfig,
    date_value: date,
    fixture_path: Path,
) -> tuple[PolygonMarketDataConnector, ConnectorRequest, dict[str, object], str]:
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
    metadata: dict[str, object] = {
        "provider": "polygon",
        "endpoint": PolygonEndpoint.GROUPED_DAILY.value,
        "date": date_value.isoformat(),
        "fixture": str(fixture_path),
        "availability_policy": config.provider_availability_policy,
    }
    return connector, request, metadata, PolygonEndpoint.GROUPED_DAILY.value


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

def _polygon_capture_with_preview_next_action(
    preview: Mapping[str, object],
) -> str:
    status = str(preview.get("status") or "")
    coverage = preview.get("coverage")
    if not isinstance(coverage, Mapping):
        coverage = {}
    if status == "invalid":
        return (
            "Saved response was captured, but preview is invalid; fix or "
            "recapture before importing."
        )
    covered = int(coverage.get("missing_covered_by_fixture_count") or 0)
    missing_after = int(coverage.get("missing_after_import_count") or 0)
    active_count = int(coverage.get("active_security_count") or 0)
    if active_count <= 0:
        return "Saved response is valid; import only after the active universe exists locally."
    if missing_after <= 0:
        return "Saved response covers the missing active bars; import the saved file if intended."
    if covered:
        return (
            "Saved response covers some missing bars; import if intended, "
            "then repair the remaining gaps."
        )
    return (
        "Saved response is valid but covers no current missing bars; check date "
        "and universe before importing."
    )

__all__ = [
    "build_polygon_grouped_daily_fixture_ingest",
    "capture_polygon_grouped_daily_response",
    "capture_polygon_grouped_daily_response_with_preview",
    "ingest_polygon_grouped_daily_fixture",
    "preview_polygon_grouped_daily_fixture",
]
