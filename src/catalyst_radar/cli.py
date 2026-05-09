from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv

from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.universe.builder import UniverseBuilder
from catalyst_radar.universe.filters import UniverseFilterConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalyst-radar")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")

    ingest = subparsers.add_parser("ingest-csv")
    ingest.add_argument("--securities", type=Path, required=True)
    ingest.add_argument("--daily-bars", type=Path, required=True)
    ingest.add_argument("--holdings", type=Path)

    polygon = subparsers.add_parser("ingest-polygon")
    polygon_sub = polygon.add_subparsers(dest="polygon_command", required=True)
    grouped = polygon_sub.add_parser("grouped-daily")
    grouped.add_argument("--date", type=date.fromisoformat, required=True)
    grouped.add_argument("--fixture", type=Path)
    tickers = polygon_sub.add_parser("tickers")
    tickers.add_argument("--fixture", type=Path)

    scan = subparsers.add_parser("scan")
    scan.add_argument("--as-of", type=date.fromisoformat, required=True)
    scan.add_argument("--available-at", type=_parse_aware_datetime)
    scan.add_argument("--universe")

    build_universe = subparsers.add_parser("build-universe")
    build_universe.add_argument("--name")
    build_universe.add_argument("--provider")
    build_universe.add_argument("--as-of", type=date.fromisoformat, required=True)
    build_universe.add_argument("--available-at", type=_parse_aware_datetime)

    provider_health = subparsers.add_parser("provider-health")
    provider_health.add_argument("--provider", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(".env.local")
    args = build_parser().parse_args(argv)
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)

    if args.command == "init-db":
        create_schema(engine)
        print("initialized database")
        return 0

    if args.command == "ingest-csv":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        connector = CsvMarketDataConnector(
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )
        return _ingest_csv_provider(
            connector=connector,
            market_repo=market_repo,
            provider_repo=provider_repo,
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )

    if args.command == "ingest-polygon":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        return _ingest_polygon_provider(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            polygon_command=args.polygon_command,
            date_value=args.date if hasattr(args, "date") else None,
            fixture_path=args.fixture,
        )

    if args.command == "provider-health":
        create_schema(engine)
        provider_repo = ProviderRepository(engine)
        health = provider_repo.latest_health(args.provider)
        if health is None:
            print(f"provider={args.provider} status=unknown")
            return 1
        print(f"provider={health.provider} status={health.status.value}")
        return 0

    if args.command == "scan":
        create_schema(engine)
        repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        available_at = args.available_at or _scan_timestamp(args.as_of)
        universe_tickers = _universe_tickers_for_scan(
            provider_repo=provider_repo,
            universe_name=args.universe,
            as_of=args.as_of,
            available_at=available_at,
        )
        if args.universe is not None and universe_tickers is None:
            print(f"universe not found: {args.universe}", file=sys.stderr)
            return 1
        results = run_scan(
            repo,
            as_of=args.as_of,
            available_at=available_at,
            universe_tickers=universe_tickers,
        )
        for result in results:
            repo.save_scan_result(result.candidate, result.policy)
        print(f"scanned candidates={len(results)}")
        return 0

    if args.command == "build-universe":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        as_of_dt = _scan_timestamp(args.as_of)
        available_at = args.available_at or as_of_dt
        builder = UniverseBuilder(
            market_repo=market_repo,
            provider_repo=provider_repo,
            config=UniverseFilterConfig(
                min_price=config.universe_min_price,
                min_avg_dollar_volume=config.universe_min_avg_dollar_volume,
                require_sector=config.universe_require_sector,
                include_etfs=config.universe_include_etfs,
                include_adrs=config.universe_include_adrs,
            ),
            name=args.name or config.universe_name,
            provider=args.provider or config.market_provider,
        )
        snapshot = builder.build(as_of=args.as_of, available_at=available_at)
        print(
            f"built universe={snapshot.name} members={snapshot.member_count} "
            f"excluded={snapshot.excluded_count}"
        )
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


def _ingest_csv_provider(
    *,
    connector: CsvMarketDataConnector,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    securities_path: Path,
    daily_bars_path: Path,
    holdings_path: Path | None,
) -> int:
    metadata = {
        "securities": str(securities_path),
        "daily_bars": str(daily_bars_path),
        "holdings": str(holdings_path) if holdings_path is not None else None,
    }
    request = ConnectorRequest(
        provider=connector.provider,
        endpoint="csv_ingest",
        params=metadata,
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="csv_ingest",
            metadata=metadata,
        )
    except ProviderIngestError as exc:
        print(f"csv ingest failed: {exc}", file=sys.stderr)
        return 1

    message = (
        f"ingested securities={result.security_count} daily_bars={result.daily_bar_count}"
    )
    if holdings_path is not None:
        message = f"{message} holdings={result.holding_count}"
    print(message)
    return 0


def _ingest_polygon_provider(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    polygon_command: str,
    date_value: date | None,
    fixture_path: Path | None,
) -> int:
    try:
        connector, request, metadata, job_type = _build_polygon_ingest(
            config=config,
            polygon_command=polygon_command,
            date_value=date_value,
            fixture_path=fixture_path,
        )
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type=job_type,
            metadata=metadata,
        )
    except (ProviderIngestError, ValueError) as exc:
        print(f"polygon ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _build_polygon_ingest(
    *,
    config: AppConfig,
    polygon_command: str,
    date_value: date | None,
    fixture_path: Path | None,
) -> tuple[PolygonMarketDataConnector, ConnectorRequest, dict[str, object], str]:
    if polygon_command == "grouped-daily":
        if date_value is None:
            msg = "grouped-daily requires --date"
            raise ValueError(msg)
        endpoint = PolygonEndpoint.GROUPED_DAILY
        params = {
            "date": date_value.isoformat(),
            "adjusted": True,
            "include_otc": False,
        }
        first_url = _polygon_grouped_daily_url(
            config=config,
            date_value=date_value,
            api_key=config.polygon_api_key,
        )
        metadata: dict[str, object] = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "date": date_value.isoformat(),
            "fixture": str(fixture_path) if fixture_path is not None else None,
        }
    elif polygon_command == "tickers":
        endpoint = PolygonEndpoint.TICKERS
        params = {"market": "stocks", "active": True, "limit": 1000}
        first_url = _polygon_tickers_url(config=config, api_key=config.polygon_api_key)
        metadata = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "fixture": str(fixture_path) if fixture_path is not None else None,
        }
    else:
        msg = f"unsupported polygon command: {polygon_command}"
        raise ValueError(msg)

    transport = (
        _fixture_transport(first_url=first_url, fixture_path=fixture_path)
        if fixture_path is not None
        else UrlLibHttpTransport()
    )
    connector = PolygonMarketDataConnector(
        api_key=config.polygon_api_key,
        client=JsonHttpClient(
            transport=transport,
            timeout_seconds=config.http_timeout_seconds,
        ),
        base_url=config.polygon_base_url,
        availability_policy=config.provider_availability_policy,
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=endpoint.value,
        params=params,
        requested_at=datetime.now(UTC),
    )
    return connector, request, metadata, endpoint.value


def _fixture_transport(*, first_url: str, fixture_path: Path) -> FakeHttpTransport:
    responses = {first_url: _fixture_response(first_url, fixture_path)}
    payload = _read_fixture_payload(fixture_path)
    next_url = payload.get("next_url")
    current_path = fixture_path
    while next_url:
        if not isinstance(next_url, str):
            msg = "polygon fixture next_url must be a string"
            raise ValueError(msg)
        current_path = _next_fixture_path(current_path)
        if not current_path.exists():
            msg = f"missing polygon fixture page for {next_url}: {current_path}"
            raise ValueError(msg)
        responses[next_url] = _fixture_response(next_url, current_path)
        payload = _read_fixture_payload(current_path)
        next_url = payload.get("next_url")
    return FakeHttpTransport(responses)


def _fixture_response(url: str, fixture_path: Path) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        url=url,
        headers={"content-type": "application/json"},
        body=fixture_path.read_bytes(),
    )


def _read_fixture_payload(fixture_path: Path) -> dict[str, object]:
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"polygon fixture must contain a JSON object: {fixture_path}"
        raise ValueError(msg)
    return payload


def _next_fixture_path(fixture_path: Path) -> Path:
    prefix, separator, suffix = fixture_path.stem.rpartition("_")
    if suffix.isdigit():
        return fixture_path.with_name(
            f"{prefix}{separator}{int(suffix) + 1}{fixture_path.suffix}"
        )
    return fixture_path.with_name(f"{fixture_path.stem}_2{fixture_path.suffix}")


def _polygon_grouped_daily_url(
    *,
    config: AppConfig,
    date_value: date,
    api_key: str | None,
) -> str:
    query = urlencode(
        {
            "adjusted": "true",
            "include_otc": "false",
            "apiKey": api_key or "",
        }
    )
    base_url = config.polygon_base_url.rstrip("/")
    return f"{base_url}/v2/aggs/grouped/locale/us/market/stocks/{date_value.isoformat()}?{query}"


def _polygon_tickers_url(*, config: AppConfig, api_key: str | None) -> str:
    query = urlencode(
        {
            "market": "stocks",
            "active": "true",
            "limit": "1000",
            "apiKey": api_key or "",
        }
    )
    return f"{config.polygon_base_url.rstrip('/')}/v3/reference/tickers?{query}"


def _print_provider_result(result: ProviderIngestResult) -> None:
    print(
        f"ingested provider={result.provider} raw={result.raw_count} "
        f"normalized={result.normalized_count} securities={result.security_count} "
        f"daily_bars={result.daily_bar_count} rejected={result.rejected_count}"
    )


def _universe_tickers_for_scan(
    *,
    provider_repo: ProviderRepository,
    universe_name: str | None,
    as_of: date,
    available_at: datetime,
) -> set[str] | None:
    if universe_name is None:
        return None
    as_of_dt = _scan_timestamp(as_of)
    snapshot = provider_repo.latest_universe_snapshot(
        name=universe_name,
        as_of=as_of_dt,
        available_at=available_at,
    )
    if snapshot is None:
        return None
    return {row.ticker for row in provider_repo.list_universe_member_rows(snapshot.id)}


def _scan_timestamp(value: date) -> datetime:
    return datetime.combine(value, time(21), tzinfo=UTC)


def _parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = "--available-at must include timezone information"
        raise argparse.ArgumentTypeError(msg)
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
