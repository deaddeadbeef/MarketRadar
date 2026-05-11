from __future__ import annotations

import json
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


def test_ingest_sec_ipo_s1_persists_public_document_analysis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "ingest-sec",
            "ipo-s1",
            "--ticker",
            "ACME",
            "--cik",
            "0002000001",
            "--fixture",
            "tests/fixtures/sec/submissions_acme_s1.json",
            "--document-fixture",
            "tests/fixtures/sec/acme_s1.htm",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "ingested provider=sec raw=1 normalized=1 securities=0 "
        "daily_bars=0 holdings=0 events=1 rejected=0\n"
    )
    assert captured.err == ""

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(raw_provider_records)) == 1
        assert conn.scalar(select(func.count()).select_from(normalized_provider_records)) == 1
        assert conn.scalar(select(func.count()).select_from(events)) == 1
        raw_payload = conn.scalar(select(raw_provider_records.c.payload))
    assert "We are offering 12,500,000 shares" in raw_payload["record"]["document_text"]

    rows = EventRepository(engine).list_events_for_ticker(
        "ACME",
        as_of=datetime(2026, 5, 10, 23, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
    )
    assert len(rows) == 1
    assert rows[0].event_type.value == "financing"
    assert rows[0].payload["ipo_analysis"]["proposed_ticker"] == "ACME"

    assert (
        main(
            [
                "ipo-s1-analysis",
                "--ticker",
                "ACME",
                "--as-of",
                "2026-05-10",
                "--available-at",
                "2026-05-10T13:00:00Z",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ticker"] == "ACME"
    assert payload["event_type"] == "financing"
    assert payload["form_type"] == "S-1"
    assert payload["analysis"]["shares_offered"] == 12_500_000
    assert payload["analysis"]["price_range_low"] == 17.0
    assert payload["analysis"]["price_range_high"] == 19.0
    assert payload["analysis"]["estimated_gross_proceeds"] == 225_000_000.0
    assert "history_of_losses" in payload["analysis"]["risk_flags"]


def test_ingest_sec_ipo_s1_live_mode_fails_closed_without_enable_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CATALYST_DATABASE_URL", _database_url(tmp_path))
    monkeypatch.delenv("CATALYST_SEC_ENABLE_LIVE", raising=False)

    exit_code = main(["ingest-sec", "ipo-s1", "--ticker", "ACME", "--cik", "0002000001"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert "CATALYST_SEC_ENABLE_LIVE=1" in captured.err


def _database_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'ipo-s1.db').as_posix()}"
