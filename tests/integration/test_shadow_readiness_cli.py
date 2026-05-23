from __future__ import annotations

import json
from datetime import UTC, datetime

import catalyst_radar.cli as cli_module
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
    assert payload["first_blocker"] == "universe"
    assert payload["first_gap_count"] == 0
    assert payload["canonical_next_command"] is None
    check_codes = {row["code"] for row in payload["checks"]}
    assert check_codes >= {
        "candidate_state_pipeline",
        "value_ledger_table",
        "outcome_tracking_table",
        "llm_real_mode_disabled",
    }
    checks = {row["code"]: row for row in payload["checks"]}
    assert checks["candidate_state_pipeline"]["status"] == "blocked"
    assert checks["value_ledger_table"]["status"] == "ready"
    assert checks["outcome_tracking_table"]["status"] == "ready"
    assert checks["llm_real_mode_disabled"]["status"] == "ready"
    assert checks["llm_real_mode_disabled"]["metric"] == {
        "enable_premium_llm": False,
        "llm_provider": "none",
    }
    assert {row["code"] for row in payload["blockers"]} >= {
        "active_universe",
        "latest_market_bars",
        "candidate_state_pipeline",
        "validation_ready",
    }


def test_assert_shadow_ready_cli_passes_available_at_cutoff(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-cutoff.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    captured: dict[str, datetime | None] = {}

    def fake_shadow_readiness_payload(
        _engine,
        _config,
        *,
        available_at=None,
    ) -> dict[str, object]:
        captured["available_at"] = available_at
        return {
            "schema_version": "shadow-readiness-v1",
            "status": "setup_required",
            "available_at": available_at.isoformat() if available_at else None,
            "ready": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    monkeypatch.setattr(
        cli_module,
        "shadow_readiness_payload",
        fake_shadow_readiness_payload,
    )

    exit_code = main(
        [
            "assert-shadow-ready",
            "--available-at",
            "2026-05-23T16:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 1
    assert captured["available_at"] == datetime(2026, 5, 23, 16, tzinfo=UTC)
    payload = json.loads(capsys.readouterr().out)
    assert payload["available_at"] == "2026-05-23T16:00:00+00:00"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_sample_csv_ingest_reaches_market_bar_ready_state(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'sample-ready.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "ingest-csv",
                "--securities",
                "data/sample/securities.csv",
                "--daily-bars",
                "data/sample/daily_bars.csv",
                "--holdings",
                "data/sample/holdings.csv",
            ]
        )
        == 0
    )
    assert "daily_bars=48" in capsys.readouterr().out

    exit_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-08",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ready"
    assert payload["first_blocker"] is None
    assert payload["active_security_count"] == 8
    assert payload["existing_as_of_bar_count"] == 8
    assert payload["missing_as_of_bar_count"] == 0
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_assert_investable_readiness_cli_fails_closed_stricter_than_shadow(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'investable-ready.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        ["assert-investable-readiness", "--month", "2026-05", "--json"]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "investable-readiness-v1"
    assert payload["status"] == "blocked"
    assert payload["ready"] is False
    assert payload["decision_support_only"] is True
    assert payload["investment_advice"] is False
    assert payload["highest_allowed_action_state"] == "EligibleForManualBuyReview"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["call_boundary"]["assert_external_calls_required"] == 0
    assert payload["call_boundary"]["assert_db_writes_required"] == 0
    assert payload["first_blocker"] == "shadow_gate_ready"
    assert payload["first_gap_count"] == 0
    assert payload["canonical_next_command"] == "catalyst-radar assert-shadow-ready --json"
    blocker_codes = {row["code"] for row in payload["blockers"]}
    assert "shadow_gate_ready" in blocker_codes
    assert "thirty_valid_full_shadow_days" in blocker_codes
    assert "monthly_value_report" in blocker_codes
    assert "monthly_value_threshold" in blocker_codes


def test_assert_shadow_ready_blocks_when_premium_llm_real_mode_enabled(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-llm.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "true")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "openai")

    exit_code = main(["assert-shadow-ready", "--json"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    checks = {row["code"]: row for row in payload["checks"]}
    assert checks["llm_real_mode_disabled"]["status"] == "blocked"
    assert checks["llm_real_mode_disabled"]["metric"] == {
        "enable_premium_llm": True,
        "llm_provider": "openai",
    }
    assert "llm_real_mode_disabled" in {
        row["code"] for row in payload["blockers"]
    }
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
