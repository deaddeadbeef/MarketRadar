from __future__ import annotations

import json

from catalyst_radar.cli import main


def test_assert_shadow_ready_cli_fails_closed_without_calls_or_writes(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-ready.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(["assert-shadow-ready", "--json"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "shadow-readiness-v1"
    assert payload["status"] == "setup_required"
    assert payload["ready"] is False
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["call_boundary"]["assert_external_calls_required"] == 0
    assert payload["call_boundary"]["assert_db_writes_required"] == 0
    assert {row["code"] for row in payload["blockers"]} >= {
        "active_universe",
        "latest_market_bars",
        "validation_ready",
    }
