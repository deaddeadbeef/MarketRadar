from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.options import OptionsAggregateConnector
from catalyst_radar.connectors.provider_ingest import ProviderIngestError, ingest_provider_records
from catalyst_radar.features.options import OptionFeatureInput
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import option_features


def test_options_connector_emits_raw_and_normalized_option_feature_records() -> None:
    connector = OptionsAggregateConnector(
        fixture_path="tests/fixtures/options/options_summary_2026-05-08.json"
    )
    request = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": "tests/fixtures/options/options_summary_2026-05-08.json"},
        requested_at=datetime(2026, 5, 8, 21, 5, tzinfo=UTC),
    )

    raw_records = connector.fetch(request)
    normalized = connector.normalize(raw_records)

    assert [record.kind for record in raw_records] == [ConnectorRecordKind.OPTION_FEATURE]
    assert [record.kind for record in normalized] == [ConnectorRecordKind.OPTION_FEATURE]
    assert normalized[0].identity == "AAA:2026-05-08T21:00:00+00:00"
    assert normalized[0].payload["ticker"] == "AAA"
    assert normalized[0].payload["call_volume"] == 12_000.0


def test_ingest_provider_records_fails_closed_without_feature_repository() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    connector = OptionsAggregateConnector(
        fixture_path="tests/fixtures/options/options_summary_2026-05-08.json"
    )
    request = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": "tests/fixtures/options/options_summary_2026-05-08.json"},
        requested_at=datetime(2026, 5, 8, 21, 5, tzinfo=UTC),
    )

    with pytest.raises(ProviderIngestError, match="feature repository required"):
        ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=MarketRepository(engine),
            provider_repo=ProviderRepository(engine),
            job_type="options_fixture",
            metadata={"fixture": "tests/fixtures/options/options_summary_2026-05-08.json"},
        )


def test_ingest_options_cli_persists_option_feature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'options.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "ingest-options",
            "--fixture",
            "tests/fixtures/options/options_summary_2026-05-08.json",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "ingested provider=options_fixture raw=1 normalized=1 "
        "option_features=1 rejected=0\n"
    )
    assert captured.err == ""

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        row = conn.execute(select(option_features)).one()
    assert row.ticker == "AAA"
    assert row.call_volume == 12_000


def test_upsert_option_features_replaces_by_deterministic_id() -> None:
    repo = _repo()
    as_of = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
    original = _option_input(as_of=as_of, call_volume=100, put_volume=50)
    replacement = _option_input(as_of=as_of, call_volume=900, put_volume=100)

    assert repo.upsert_option_features([original]) == 1
    assert repo.upsert_option_features([replacement]) == 1

    with repo.engine.connect() as conn:
        count = conn.execute(select(func.count()).select_from(option_features)).scalar_one()
        row = conn.execute(select(option_features)).one()

    assert count == 1
    assert row.call_volume == 900
    assert row.put_volume == 100
    assert row.abnormality_score > 0


def test_latest_option_features_by_ticker_respects_as_of_and_available_at() -> None:
    repo = _repo()
    may_7 = datetime(2026, 5, 7, 21, 0, tzinfo=UTC)
    may_8 = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
    repo.upsert_option_features(
        [
            _option_input(as_of=may_7, call_volume=500, available_at=may_7),
            _option_input(as_of=may_8, call_volume=1_500, available_at=may_8),
        ]
    )

    latest = repo.latest_option_features_by_ticker(
        ["aaa"],
        as_of=may_8,
        available_at=may_8 + timedelta(minutes=1),
    )
    earlier = repo.latest_option_features_by_ticker(
        ["AAA"],
        as_of=may_7,
        available_at=may_8 + timedelta(minutes=1),
    )

    assert latest["AAA"].as_of == may_8
    assert latest["AAA"].call_volume == 1_500
    assert earlier["AAA"].as_of == may_7
    assert earlier["AAA"].call_volume == 500


def test_future_available_option_rows_are_ignored() -> None:
    repo = _repo()
    as_of = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
    repo.upsert_option_features(
        [
            _option_input(
                as_of=as_of,
                source_ts=as_of - timedelta(minutes=15),
                available_at=as_of + timedelta(days=1),
            )
        ]
    )

    latest = repo.latest_option_features_by_ticker(
        ["AAA"],
        as_of=as_of,
        available_at=as_of + timedelta(minutes=1),
    )

    assert latest == {}


def _repo() -> FeatureRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return FeatureRepository(engine)


def _option_input(
    *,
    ticker: str = "AAA",
    as_of: datetime = datetime(2026, 5, 8, 21, 0, tzinfo=UTC),
    provider: str = "options_fixture",
    call_volume: float = 1_000.0,
    put_volume: float = 500.0,
    call_open_interest: float = 5_000.0,
    put_open_interest: float = 3_000.0,
    iv_percentile: float = 0.65,
    skew: float = 0.2,
    source_ts: datetime | None = None,
    available_at: datetime | None = None,
) -> OptionFeatureInput:
    resolved_source_ts = source_ts or as_of - timedelta(minutes=15)
    return OptionFeatureInput(
        ticker=ticker,
        as_of=as_of,
        provider=provider,
        call_volume=call_volume,
        put_volume=put_volume,
        call_open_interest=call_open_interest,
        put_open_interest=put_open_interest,
        iv_percentile=iv_percentile,
        skew=skew,
        source_ts=resolved_source_ts,
        available_at=available_at or as_of,
        payload={"fixture": True},
    )
