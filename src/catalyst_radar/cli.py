from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv

from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    HttpTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.options import OptionsAggregateConnector
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.pipeline import run_text_pipeline
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
    tickers.add_argument("--date", type=date.fromisoformat)

    sec = subparsers.add_parser("ingest-sec")
    sec_sub = sec.add_subparsers(dest="sec_command", required=True)
    submissions = sec_sub.add_parser("submissions")
    submissions.add_argument("--ticker", required=True)
    submissions.add_argument("--cik", required=True)
    submissions.add_argument("--fixture", type=Path)

    news = subparsers.add_parser("ingest-news")
    news.add_argument("--fixture", type=Path, required=True)

    earnings = subparsers.add_parser("ingest-earnings")
    earnings.add_argument("--fixture", type=Path, required=True)

    options = subparsers.add_parser("ingest-options")
    options.add_argument("--fixture", type=Path, required=True)

    events = subparsers.add_parser("events")
    events.add_argument("--ticker", required=True)
    events.add_argument("--as-of", type=date.fromisoformat, required=True)
    events.add_argument("--available-at", type=_parse_aware_datetime)
    events.add_argument("--limit", type=int, default=20)

    run_textint = subparsers.add_parser("run-textint")
    run_textint.add_argument("--as-of", type=date.fromisoformat, required=True)
    run_textint.add_argument("--available-at", type=_parse_aware_datetime)
    run_textint.add_argument("--ontology", type=Path, default=Path("config/themes.yaml"))
    run_textint.add_argument("--ticker")

    text_features = subparsers.add_parser("text-features")
    text_features.add_argument("--ticker", required=True)
    text_features.add_argument("--as-of", type=date.fromisoformat, required=True)
    text_features.add_argument("--available-at", type=_parse_aware_datetime)

    scan = subparsers.add_parser("scan")
    scan.add_argument("--as-of", type=date.fromisoformat, required=True)
    scan.add_argument("--available-at", type=_parse_aware_datetime)
    scan.add_argument("--provider")
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

    if args.command == "ingest-sec":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_sec_provider(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            sec_command=args.sec_command,
            ticker=args.ticker,
            cik=args.cik,
            fixture_path=args.fixture,
        )

    if args.command == "ingest-news":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_news_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            fixture_path=args.fixture,
        )

    if args.command == "ingest-earnings":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_earnings_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            fixture_path=args.fixture,
        )

    if args.command == "ingest-options":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        feature_repo = FeatureRepository(engine)
        return _ingest_options_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            feature_repo=feature_repo,
            fixture_path=args.fixture,
        )

    if args.command == "events":
        create_schema(engine)
        event_repo = EventRepository(engine)
        as_of = datetime.combine(args.as_of, time.max, tzinfo=UTC)
        available_at = args.available_at or datetime.now(UTC)
        for event in event_repo.list_events_for_ticker(
            args.ticker,
            as_of=as_of,
            available_at=available_at,
            limit=args.limit,
        ):
            print(
                f"{event.ticker} {event.available_at.isoformat()} "
                f"{event.event_type.value} materiality={event.materiality:.2f} "
                f"quality={event.source_quality:.2f} source={event.source} "
                f"title={event.title}"
            )
        return 0

    if args.command == "provider-health":
        create_schema(engine)
        provider_repo = ProviderRepository(engine)
        health = provider_repo.latest_health(args.provider)
        if health is None:
            print(f"provider={args.provider} status=unknown")
            return 1
        print(f"provider={health.provider} status={health.status.value}")
        return 0

    if args.command == "run-textint":
        create_schema(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        tickers = [args.ticker] if args.ticker else None
        result = run_text_pipeline(
            event_repo,
            text_repo,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
            ontology_path=args.ontology,
            tickers=tickers,
        )
        print(f"processed text_features={result.feature_count} snippets={result.snippet_count}")
        return 0

    if args.command == "text-features":
        create_schema(engine)
        text_repo = TextRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        features = text_repo.latest_text_features_by_ticker(
            [args.ticker],
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
        )
        feature = features.get(args.ticker.upper())
        if feature is None:
            print(f"text feature not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        print(
            f"{feature.ticker} local_narrative={feature.local_narrative_score:.2f} "
            f"novelty={feature.novelty_score:.2f} "
            f"snippets={len(feature.selected_snippet_ids)}"
        )
        return 0

    if args.command == "scan":
        create_schema(engine)
        repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        feature_repo = FeatureRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        universe_tickers = _universe_tickers_for_scan(
            provider_repo=provider_repo,
            universe_name=args.universe,
            as_of=args.as_of,
            available_at=available_at,
        )
        if args.universe is not None and universe_tickers is None:
            print(f"universe not found: {args.universe}", file=sys.stderr)
            return 1
        scan_provider = args.provider
        if args.universe is not None:
            snapshot = _universe_snapshot_for_scan(
                provider_repo=provider_repo,
                universe_name=args.universe,
                as_of=args.as_of,
                available_at=available_at,
            )
            scan_provider = snapshot.provider if snapshot is not None else scan_provider
        results = run_scan(
            repo,
            as_of=args.as_of,
            available_at=available_at,
            provider=scan_provider,
            universe_tickers=universe_tickers,
            config=config,
            event_repo=event_repo,
            text_repo=text_repo,
            feature_repo=feature_repo,
        )
        for result in results:
            repo.save_scan_result(result.candidate, result.policy)
        print(f"scanned candidates={len(results)}")
        return 0

    if args.command == "build-universe":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
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


def _ingest_sec_provider(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    sec_command: str,
    ticker: str,
    cik: str,
    fixture_path: Path | None,
) -> int:
    if sec_command != "submissions":
        print(f"sec ingest failed: unsupported sec command: {sec_command}", file=sys.stderr)
        return 1
    if fixture_path is None and not config.sec_enable_live:
        print(
            "sec ingest failed: live SEC ingest requires CATALYST_SEC_ENABLE_LIVE=1",
            file=sys.stderr,
        )
        return 1
    if fixture_path is None and not config.sec_user_agent:
        print(
            "sec ingest failed: CATALYST_SEC_USER_AGENT is required for live SEC ingest",
            file=sys.stderr,
        )
        return 1

    transport: HttpTransport | None = None
    if fixture_path is None:
        transport = _HeaderInjectingTransport(
            UrlLibHttpTransport(),
            {"User-Agent": config.sec_user_agent or ""},
        )
    connector = SecSubmissionsConnector(
        fixture_path=fixture_path,
        client=(
            JsonHttpClient(
                transport=transport,
                timeout_seconds=config.http_timeout_seconds,
            )
            if transport is not None
            else None
        ),
        base_url=config.sec_base_url,
    )
    metadata = {
        "provider": "sec",
        "endpoint": "submissions",
        "ticker": ticker.upper(),
        "cik": cik,
        "fixture": str(fixture_path) if fixture_path is not None else None,
        "live": fixture_path is None,
    }
    request = ConnectorRequest(
        provider="sec",
        endpoint="submissions",
        params={"ticker": ticker.upper(), "cik": cik},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="sec_submissions",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"sec ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_news_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    fixture_path: Path,
) -> int:
    connector = NewsJsonConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "news_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="news_fixture",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"news ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_earnings_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    fixture_path: Path,
) -> int:
    connector = EarningsCalendarConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "earnings_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="earnings_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="earnings_fixture",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"earnings ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_options_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    feature_repo: FeatureRepository,
    fixture_path: Path,
) -> int:
    connector = OptionsAggregateConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "options_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="options_fixture",
            metadata=metadata,
            feature_repo=feature_repo,
        )
    except ProviderIngestError as exc:
        print(f"options ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_options_provider_result(result)
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
            api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
        )
        metadata: dict[str, object] = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "date": date_value.isoformat(),
            "fixture": str(fixture_path) if fixture_path is not None else None,
            "availability_policy": config.provider_availability_policy,
        }
    elif polygon_command == "tickers":
        endpoint = PolygonEndpoint.TICKERS
        params = {"market": "stocks", "active": True, "limit": 1000}
        if date_value is not None:
            params["date"] = date_value.isoformat()
        first_url = _polygon_tickers_url(
            config=config,
            api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
            date_value=date_value,
        )
        metadata = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "date": date_value.isoformat() if date_value is not None else None,
            "fixture": str(fixture_path) if fixture_path is not None else None,
            "availability_policy": config.provider_availability_policy,
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
        api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
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
        response_url = _fixture_next_url(str(next_url))
        responses[response_url] = _fixture_response(response_url, current_path)
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


def _polygon_tickers_url(
    *,
    config: AppConfig,
    api_key: str | None,
    date_value: date | None = None,
) -> str:
    params: dict[str, str] = {
        "market": "stocks",
        "active": "true",
        "limit": "1000",
    }
    if date_value is not None:
        params["date"] = date_value.isoformat()
    params["apiKey"] = api_key or ""
    query = urlencode(params)
    return f"{config.polygon_base_url.rstrip('/')}/v3/reference/tickers?{query}"


def _fixture_next_url(url: str) -> str:
    separator = "&" if "?" in url else "?"
    if "apiKey=" in url:
        return url
    return f"{url}{separator}apiKey=fixture-key"


class _HeaderInjectingTransport:
    def __init__(
        self,
        transport: HttpTransport,
        headers: Mapping[str, str],
    ) -> None:
        self.transport = transport
        self.headers = dict(headers)

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        merged_headers: dict[str, str] = {**self.headers, **dict(headers)}
        return self.transport.get(
            url,
            headers=merged_headers,
            timeout_seconds=timeout_seconds,
        )


def _print_provider_result(result: ProviderIngestResult) -> None:
    print(
        f"ingested provider={result.provider} raw={result.raw_count} "
        f"normalized={result.normalized_count} securities={result.security_count} "
        f"daily_bars={result.daily_bar_count} holdings={result.holding_count} "
        f"events={result.event_count} rejected={result.rejected_count}"
    )


def _print_options_provider_result(result: ProviderIngestResult) -> None:
    print(
        f"ingested provider={result.provider} raw={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"option_features={result.option_feature_count} rejected={result.rejected_count}"
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
    snapshot = _universe_snapshot_for_scan(
        provider_repo=provider_repo,
        universe_name=universe_name,
        as_of=as_of,
        available_at=available_at,
    )
    if snapshot is None:
        return None
    return {row.ticker for row in provider_repo.list_universe_member_rows(snapshot.id)}


def _universe_snapshot_for_scan(
    *,
    provider_repo: ProviderRepository,
    universe_name: str,
    as_of: date,
    available_at: datetime,
):
    as_of_dt = _scan_timestamp(as_of)
    return provider_repo.latest_universe_snapshot(
        name=universe_name,
        as_of=as_of_dt,
        available_at=available_at,
    )


def _polygon_api_key(*, config: AppConfig, fixture_path: Path | None) -> str | None:
    if fixture_path is not None:
        return "fixture-key"
    return config.polygon_api_key


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
