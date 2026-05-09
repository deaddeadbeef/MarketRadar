from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.cli import main
from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


def test_scan_with_universe_uses_snapshot_members_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_fixture_market(database_url)
    engine = create_engine(database_url, future=True)
    provider_repo = ProviderRepository(engine)
    as_of_dt = datetime(2026, 5, 8, 21, tzinfo=UTC)
    provider_repo.save_universe_snapshot(
        name="liquid-us",
        as_of=as_of_dt,
        provider="fixture",
        source_ts=as_of_dt,
        available_at=as_of_dt,
        members=[{"ticker": "AAA", "reason": "eligible", "rank": 1}],
    )

    exit_code = main(["scan", "--as-of", "2026-05-08", "--universe", "liquid-us"])

    assert exit_code == 0
    assert capsys.readouterr().out == "scanned candidates=1\n"


def test_scan_with_missing_universe_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_fixture_market(database_url)

    exit_code = main(["scan", "--as-of", "2026-05-08", "--universe", "missing"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "universe not found: missing\n"


def test_build_universe_command_persists_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_UNIVERSE_MIN_PRICE", "5")
    monkeypatch.setenv("CATALYST_UNIVERSE_MIN_AVG_DOLLAR_VOLUME", "10000000")
    _seed_fixture_market(database_url)

    exit_code = main(
        [
            "build-universe",
            "--name",
            "liquid-us",
            "--provider",
            "fixture",
            "--as-of",
            "2026-05-08",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == "built universe=liquid-us members=2 excluded=4\n"


def _seed_fixture_market(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))


def _database_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'catalyst_radar.db').as_posix()}"
