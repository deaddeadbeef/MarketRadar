from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

from catalyst_radar.cli import main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import Security
from catalyst_radar.events.sec_cik import (
    apply_sec_cik_overrides,
    refresh_sec_cik_metadata,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import securities


def test_refresh_sec_cik_metadata_updates_missing_active_security_metadata(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    repo = MarketRepository(engine)
    repo.upsert_securities(
        [
            _security("AAPL"),
            _security("MSFT", metadata={"cik": "0000789019"}),
            _security("BRK.A"),
            _security("MISS"),
        ]
    )

    result = refresh_sec_cik_metadata(
        engine,
        AppConfig(),
        fixture_path=Path("tests/fixtures/sec/company_tickers.json"),
    )
    payload = result.as_payload()

    assert payload["schema_version"] == "sec-cik-metadata-refresh-v1"
    assert payload["live"] is False
    assert payload["external_calls_made"] == 0
    assert payload["active_security_count"] == 4
    assert payload["missing_before_count"] == 3
    assert payload["matched_missing_count"] == 2
    assert payload["updated_count"] == 2
    assert payload["missing_after_count"] == 1
    assert payload["updated_tickers"] == ["AAPL", "BRK.A"]
    assert payload["unmatched_tickers"] == ["MISS"]

    metadata = _security_metadata(engine)
    assert metadata["AAPL"]["cik"] == "0000320193"
    assert metadata["AAPL"]["sec_company_name"] == "Apple Inc."
    assert metadata["AAPL"]["cik_source"] == "sec_company_tickers"
    assert metadata["MSFT"]["cik"] == "0000789019"
    assert metadata["BRK.A"]["cik"] == "0001067983"
    assert "cik" not in metadata["MISS"]


def test_refresh_sec_cik_metadata_live_mode_fails_closed_without_enable(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)

    with pytest.raises(ValueError, match="CATALYST_SEC_ENABLE_LIVE=1"):
        refresh_sec_cik_metadata(
            engine,
            AppConfig(sec_enable_live=False, sec_user_agent="MarketRadar test@example.com"),
        )


def test_ingest_sec_company_tickers_cli_updates_cik_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'sec-cik-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    MarketRepository(engine).upsert_securities([_security("AAPL"), _security("MISS")])

    exit_code = main(
        [
            "ingest-sec",
            "company-tickers",
            "--fixture",
            "tests/fixtures/sec/company_tickers.json",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert "refreshed_sec_cik_metadata provider=sec live=False" in captured.out
    assert "active=2 missing_before=2 matched=1 updated=1 missing_after=1" in (
        captured.out
    )
    assert "updated_examples=AAPL" in captured.out
    assert "unmatched_examples=MISS" in captured.out
    assert _security_metadata(engine)["AAPL"]["cik"] == "0000320193"


def test_apply_sec_cik_overrides_updates_metadata_without_provider_calls(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    MarketRepository(engine).upsert_securities(
        [_security("AAPL"), _security("MSFT", metadata={"cik": "0000789019"})]
    )

    result = apply_sec_cik_overrides(
        engine,
        [
            {"ticker": "AAPL", "cik": "320193", "sec_company_name": "Apple Inc."},
            {"ticker": "MSFT", "cik": "0000789019"},
            {"ticker": "MISS", "cik": "123456"},
            {"ticker": "BAD", "cik": "not-a-cik"},
        ],
    )
    payload = result.as_payload()

    assert payload["schema_version"] == "sec-cik-override-import-v1"
    assert payload["live"] is False
    assert payload["external_calls_made"] == 0
    assert payload["requested_count"] == 4
    assert payload["updated_count"] == 1
    assert payload["skipped_count"] == 1
    assert payload["unmatched_count"] == 1
    assert payload["invalid_count"] == 1
    assert payload["updated_tickers"] == ["AAPL"]
    assert payload["skipped_tickers"] == ["MSFT"]
    assert payload["unmatched_tickers"] == ["MISS"]
    assert payload["invalid_rows"] == ["row 4"]

    metadata = _security_metadata(engine)
    assert metadata["AAPL"]["cik"] == "0000320193"
    assert metadata["AAPL"]["cik_source"] == "manual_cik_override"
    assert metadata["AAPL"]["sec_company_name"] == "Apple Inc."
    assert metadata["MSFT"]["cik"] == "0000789019"


def test_ingest_sec_cik_overrides_cli_imports_local_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'sec-cik-overrides.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    MarketRepository(engine).upsert_securities(
        [_security("AAPL"), _security("MSFT", metadata={"cik": "0000789019"})]
    )
    overrides = tmp_path / "cik-overrides.csv"
    overrides.write_text(
        "ticker,cik,sec_company_name\n"
        "AAPL,320193,Apple Inc.\n"
        "MSFT,0000789019,Microsoft Corp.\n"
        "MISS,123456,Missing Co.\n",
        encoding="utf-8",
    )

    exit_code = main(["ingest-sec", "cik-overrides", "--csv", str(overrides)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert "imported_sec_cik_overrides provider=manual live=False" in captured.out
    assert "requested=3 updated=1 skipped=1 unmatched=1 invalid=0 external_calls=0" in (
        captured.out
    )
    assert "updated_examples=AAPL" in captured.out
    assert "skipped_examples=MSFT" in captured.out
    assert "unmatched_examples=MISS" in captured.out
    assert _security_metadata(engine)["AAPL"]["cik"] == "0000320193"


def _engine(tmp_path: Path):
    engine = create_engine(
        f"sqlite:///{(tmp_path / 'sec-cik.db').as_posix()}",
        future=True,
    )
    create_schema(engine)
    return engine


def _security(ticker: str, *, metadata: dict[str, object] | None = None) -> Security:
    return Security(
        ticker=ticker,
        name=f"{ticker} Inc.",
        exchange="XNYS",
        sector="Technology",
        industry="Software",
        market_cap=10_000_000_000,
        avg_dollar_volume_20d=50_000_000,
        has_options=True,
        is_active=True,
        updated_at=datetime(2026, 5, 18, tzinfo=UTC),
        metadata=metadata or {},
    )


def _security_metadata(engine) -> dict[str, dict[str, object]]:
    with engine.connect() as conn:
        return {
            str(row.ticker): dict(row._mapping["metadata"] or {})
            for row in conn.execute(select(securities.c.ticker, securities.c.metadata))
        }
