from __future__ import annotations

import json

from catalyst_radar.cli import main


def test_assert_trial_ready_blocks_empty_database_without_calls_or_writes(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trial-empty.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "assert-trial-ready",
            "--available-at",
            "2026-05-23T12:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "trial-readiness-v1"
    assert payload["status"] == "setup_required"
    assert payload["safe_to_try_read_only"] is False
    assert payload["ready_for_shadow_mode"] is False
    assert payload["ready_for_investment_decision"] is False
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["first_blocker"] == "read_only_scan_surface"
    assert payload["minimum_features_required"]["read_only_priced_in_answer"] is False
    assert payload["minimum_features_required"]["zero_hidden_calls_or_writes"] is True


def test_assert_trial_ready_allows_read_only_demo_without_claiming_investment_ready(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trial-demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    exit_code = main(
        [
            "assert-trial-ready",
            "--available-at",
            "2026-05-23T12:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "trial-readiness-v1"
    assert payload["status"] == "safe_read_only"
    assert payload["safe_to_try_read_only"] is True
    assert payload["ready_for_investment_decision"] is False
    assert payload["highest_allowed_use"] == "read_only_research"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["canonical_next_command"] == "catalyst-radar dashboard-tui"
    assert payload["minimum_features_required"] == {
        "alerts_dry_run": True,
        "broker_orders_disabled": True,
        "canonical_next_step": True,
        "read_only_priced_in_answer": True,
        "real_llm_disabled": True,
        "value_report_available": True,
        "zero_hidden_calls_or_writes": True,
    }
    assert "Any command with --execute" in payload["blocked_until_explicit_approval"]


def test_assert_trial_ready_blocks_when_real_llm_mode_is_enabled(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trial-llm.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "true")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "openai")

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    exit_code = main(
        [
            "assert-trial-ready",
            "--available-at",
            "2026-05-23T12:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["safe_to_try_read_only"] is False
    assert payload["first_blocker"] == "llm_real_mode_disabled"
    assert payload["minimum_features_required"]["real_llm_disabled"] is False
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
