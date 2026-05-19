from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.brokers.models import BrokerMarketSnapshot, broker_market_snapshot_id
from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.options import (
    OptionsAggregateConnector,
    validate_options_fixture_json,
    write_options_fixture_template_json,
)
from catalyst_radar.connectors.provider_ingest import ProviderIngestError, ingest_provider_records
from catalyst_radar.features.options import OptionFeatureInput
from catalyst_radar.storage.broker_repositories import BrokerRepository
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


def test_write_options_fixture_template_json_writes_importable_shape(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "point-in-time-options-2026-05-10.json"
    fixture = {
        "as_of": "2026-05-10T21:00:00+00:00",
        "source_ts": "2026-05-10T21:00:00+00:00",
        "available_at": "2026-05-10T21:00:00+00:00",
        "provider": "options_fixture",
        "results": [
            {
                "ticker": "MSFT",
                "call_volume": "",
                "put_volume": "",
                "call_open_interest": "",
                "put_open_interest": "",
                "iv_percentile": "",
                "skew": "",
            }
        ],
    }

    result = write_options_fixture_template_json(output_path, fixture)

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert result.row_count == 1
    assert result.as_payload()["external_calls_made"] == 0
    assert written["as_of"] == "2026-05-10T21:00:00+00:00"
    assert written["results"][0]["ticker"] == "MSFT"
    assert "call_volume" in written["results"][0]


def test_validate_options_fixture_json_rejects_blank_template_rows(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "point-in-time-options-2026-05-10.json"
    write_options_fixture_template_json(
        output_path,
        {
            "as_of": "2026-05-10T21:00:00+00:00",
            "source_ts": "2026-05-10T21:00:00+00:00",
            "available_at": "2026-05-10T21:00:00+00:00",
            "provider": "options_fixture",
            "results": [
                {
                    "ticker": "MSFT",
                    "call_volume": "",
                    "put_volume": "",
                    "call_open_interest": "",
                    "put_open_interest": "",
                    "iv_percentile": "",
                    "skew": "",
                }
            ],
        },
    )

    result = validate_options_fixture_json(
        output_path,
        expected_as_of=datetime(2026, 5, 10, tzinfo=UTC).date(),
    ).as_payload()

    assert result["status"] == "invalid"
    assert result["external_calls_made"] == 0
    assert result["row_count"] == 1
    assert result["valid_row_count"] == 0
    assert result["blank_required_count"] == 6
    assert result["import_command"] is None
    assert "call_volume is blank" in result["errors"][0]


def test_validate_options_fixture_json_accepts_filled_rows(tmp_path: Path) -> None:
    output_path = tmp_path / "point-in-time-options-2026-05-10.json"
    output_path.write_text(
        json.dumps(
            {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [
                    {
                        "ticker": "MSFT",
                        "call_volume": 1200,
                        "put_volume": 400,
                        "call_open_interest": 5000,
                        "put_open_interest": 3000,
                        "iv_percentile": 0.62,
                        "skew": 0.15,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = validate_options_fixture_json(
        output_path,
        expected_as_of=datetime(2026, 5, 10, tzinfo=UTC).date(),
    ).as_payload()

    assert result["status"] == "ready"
    assert result["valid_row_count"] == 1
    assert result["invalid_row_count"] == 0
    assert result["import_command"] == (
        f"catalyst-radar ingest-options --fixture {output_path}"
    )


def test_ingest_options_validate_only_cli_reports_invalid_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'options-validate.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    output_path = tmp_path / "point-in-time-options-2026-05-10.json"
    output_path.write_text(
        json.dumps(
            {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [{"ticker": "MSFT", "call_volume": ""}],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "ingest-options",
            "--fixture",
            str(output_path),
            "--validate-only",
            "--expected-as-of",
            "2026-05-10",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.err == ""
    assert "options_fixture_validation status=invalid" in captured.out
    assert "external_calls=0" in captured.out
    assert "missing_fields=5" in captured.out
    assert "next_action=Fix blank or invalid option fields" in captured.out


def test_ingest_options_fixture_template_cli_writes_gap_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'options-template.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    output_path = tmp_path / "point-in-time-options-2026-05-10.json"

    def fake_payload(*_args, **kwargs):
        assert kwargs["stocks_only"] is True
        return {
            "schema_version": "options-fixture-template-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "source": "options",
            "stocks_only": True,
            "source_gap_rows": 1,
            "row_count": 1,
            "target_as_of": "2026-05-10T21:00:00+00:00",
            "target_date": "2026-05-10",
            "columns": [
                "ticker",
                "call_volume",
                "put_volume",
                "call_open_interest",
                "put_open_interest",
                "iv_percentile",
                "skew",
            ],
            "fixture": {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [
                    {
                        "ticker": "MSFT",
                        "call_volume": "",
                        "put_volume": "",
                        "call_open_interest": "",
                        "put_open_interest": "",
                        "iv_percentile": "",
                        "skew": "",
                    }
                ],
            },
            "sample_tickers": ["MSFT"],
            "api": "GET /api/radar/options/fixture-template?stocks_only=true",
            "boundary": "Template/export is zero-call.",
            "next_action": "Fill the aggregate option fields.",
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.options_fixture_template_payload",
        fake_payload,
    )

    exit_code = main(
        [
            "ingest-options",
            "--fixture-template",
            "--out",
            str(output_path),
            "--stocks-only",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert "options_fixture_template status=ready" in captured.out
    assert "external_calls=0" in captured.out
    assert f"import_command=catalyst-radar ingest-options --fixture {output_path}" in (
        captured.out
    )
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["results"][0]["ticker"] == "MSFT"


def test_ingest_options_cli_promotes_stored_schwab_market_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'schwab-options.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    BrokerRepository(engine).upsert_market_snapshots(
        [
            BrokerMarketSnapshot(
                id=broker_market_snapshot_id("GLW", now),
                ticker="GLW",
                as_of=now,
                raw_payload={
                    "options": {
                        "callExpDateMap": {
                            "2026-06-19:38": {
                                "12.5": [
                                    {
                                        "totalVolume": 900,
                                        "openInterest": 1200,
                                        "volatility": 42.0,
                                    }
                                ]
                            }
                        },
                        "putExpDateMap": {
                            "2026-06-19:38": {
                                "10.0": [
                                    {
                                        "totalVolume": 300,
                                        "openInterest": 800,
                                        "volatility": 39.0,
                                    }
                                ]
                            }
                        },
                    }
                },
                created_at=now,
            )
        ]
    )

    exit_code = main(["ingest-options", "--from-schwab-market", "--ticker", "GLW"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "ingested provider=schwab_option_chain raw=1 normalized=1 "
        "option_features=1 rejected=0\n"
    )
    assert captured.err == ""
    with engine.connect() as conn:
        row = conn.execute(select(option_features)).one()
    assert row.ticker == "GLW"
    assert row.provider == "schwab_option_chain"
    assert row.call_volume == 900
    assert row.put_volume == 300
    assert row.iv_percentile == 0.405


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
