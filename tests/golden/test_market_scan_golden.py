from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.cli import main
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


def test_polygon_fixture_universe_scan_is_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'catalyst_radar.db').as_posix()}"
    available_at = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    monkeypatch.setenv("CATALYST_MARKET_PROVIDER", "polygon")

    assert main(["init-db"]) == 0
    assert (
        main(
            [
                "ingest-polygon",
                "tickers",
                "--date",
                "2026-05-08",
                "--fixture",
                "tests/fixtures/polygon/tickers_page_1.json",
            ]
        )
        == 0
    )
    for day in ("2026-05-07", "2026-05-08"):
        assert (
            main(
                [
                    "ingest-polygon",
                    "grouped-daily",
                    "--date",
                    day,
                    "--fixture",
                    f"tests/fixtures/polygon/grouped_daily_{day}.json",
                ]
            )
            == 0
        )
    assert (
        main(
            [
                "build-universe",
                "--name",
                "liquid-us",
                "--provider",
                "polygon",
                "--as-of",
                "2026-05-08",
                "--available-at",
                available_at.isoformat(),
            ]
        )
        == 0
    )
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    market_repo = MarketRepository(engine)
    provider_repo = ProviderRepository(engine)
    snapshot = provider_repo.latest_universe_snapshot(
        name="liquid-us",
        as_of=datetime(2026, 5, 8, 21, tzinfo=UTC),
        available_at=available_at,
    )
    assert snapshot is not None
    universe_tickers = set(provider_repo.list_universe_members(snapshot.id))

    assert (
        main(
            [
                "scan",
                "--as-of",
                "2026-05-08",
                "--available-at",
                available_at.isoformat(),
                "--universe",
                "liquid-us",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "scanned candidates=2" in captured.out

    results = run_scan(
        market_repo,
        as_of=date(2026, 5, 8),
        available_at=available_at,
        provider=snapshot.provider,
        universe_tickers=universe_tickers,
    )

    assert [result.ticker for result in results] == ["AAPL", "MSFT"]
    assert results[0].candidate.final_score > results[1].candidate.final_score
