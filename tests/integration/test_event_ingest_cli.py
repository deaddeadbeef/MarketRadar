from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import (
    events,
    normalized_provider_records,
    raw_provider_records,
)


def test_ingest_sec_submissions_persists_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "ingest-sec",
            "submissions",
            "--ticker",
            "MSFT",
            "--cik",
            "0000789019",
            "--fixture",
            "tests/fixtures/sec/submissions_msft.json",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "ingested provider=sec raw=2 normalized=2 securities=0 "
        "daily_bars=0 holdings=0 events=2 rejected=0\n"
    )
    assert captured.err == ""

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(raw_provider_records)) == 2
        assert conn.scalar(select(func.count()).select_from(normalized_provider_records)) == 2
        assert conn.scalar(select(func.count()).select_from(events)) == 2

    rows = EventRepository(engine).list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 23, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )
    assert [row.event_type.value for row in rows] == ["guidance", "sec_filing"]


def test_sec_fixture_availability_is_deterministic_for_historical_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setattr(
        "catalyst_radar.cli.datetime",
        _FixedDateTime,
    )

    assert (
        main(
            [
                "ingest-sec",
                "submissions",
                "--ticker",
                "MSFT",
                "--cik",
                "0000789019",
                "--fixture",
                "tests/fixtures/sec/submissions_msft.json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    rows = EventRepository(engine).list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 23, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert [row.event_type.value for row in rows] == ["guidance", "sec_filing"]


def test_ingest_news_and_events_command_filter_future_available_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    ingest_exit = main(
        ["ingest-news", "--fixture", "tests/fixtures/news/ticker_news_msft.json"]
    )
    ingest_captured = capsys.readouterr()

    assert ingest_exit == 0
    assert ingest_captured.out == (
        "ingested provider=news_fixture raw=2 normalized=2 securities=0 "
        "daily_bars=0 holdings=0 events=2 rejected=0\n"
    )
    assert ingest_captured.err == ""

    events_exit = main(
        [
            "events",
            "--ticker",
            "MSFT",
            "--as-of",
            "2026-05-10",
            "--available-at",
            "2026-05-10T12:34:00Z",
        ]
    )
    events_captured = capsys.readouterr()

    assert events_exit == 0
    assert "MSFT 2026-05-10T12:31:00+00:00 news " in events_captured.out
    assert "Sponsored Stocks Daily" in events_captured.out
    assert "Reuters" not in events_captured.out
    assert events_captured.err == ""


def test_ingest_earnings_persists_upcoming_earnings_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        ["ingest-earnings", "--fixture", "tests/fixtures/earnings/calendar_msft.json"]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "ingested provider=earnings_fixture raw=1 normalized=1 securities=0 "
        "daily_bars=0 holdings=0 events=1 rejected=0\n"
    )
    assert captured.err == ""

    engine = create_engine(database_url, future=True)
    rows = EventRepository(engine).list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 23, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0].event_type.value == "earnings"
    assert rows[0].payload["event_risk"] == "upcoming_earnings"


def test_ingest_sec_live_mode_fails_closed_without_enable_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CATALYST_DATABASE_URL", _database_url(tmp_path))
    monkeypatch.delenv("CATALYST_SEC_ENABLE_LIVE", raising=False)

    exit_code = main(
        ["ingest-sec", "submissions", "--ticker", "MSFT", "--cik", "0000789019"]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "CATALYST_SEC_ENABLE_LIVE=1" in captured.err


def test_ingest_sec_live_mode_fails_closed_without_user_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CATALYST_DATABASE_URL", _database_url(tmp_path))
    monkeypatch.setenv("CATALYST_SEC_ENABLE_LIVE", "1")
    monkeypatch.delenv("CATALYST_SEC_USER_AGENT", raising=False)

    exit_code = main(
        ["ingest-sec", "submissions", "--ticker", "MSFT", "--cik", "0000789019"]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "CATALYST_SEC_USER_AGENT is required" in captured.err


def _database_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'events.db').as_posix()}"


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        value = datetime(2026, 5, 11, 0, tzinfo=UTC)
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)
