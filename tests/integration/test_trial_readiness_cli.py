from __future__ import annotations

import json
from datetime import UTC, date, datetime

from sqlalchemy import create_engine

import catalyst_radar.cli as cli_module
from catalyst_radar.cli import main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.dashboard.data import trial_readiness_payload
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


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
    product_gate = payload["minimum_useful_product"]
    assert product_gate["schema_version"] == "trial-minimum-useful-product-v1"
    assert product_gate["ready"] is False
    assert product_gate["status"] == "blocked"
    assert product_gate["highest_allowed_use"] == "safe_browsing_only"
    assert product_gate["first_blocker"] == "read_only_scan_surface"
    assert product_gate["external_calls_made"] == 0
    assert product_gate["db_writes_made"] == 0


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
    product_gate = payload["minimum_useful_product"]
    assert product_gate["schema_version"] == "trial-minimum-useful-product-v1"
    assert product_gate["ready"] is False
    assert product_gate["status"] == "blocked"
    assert product_gate["highest_allowed_use"] == "safe_browsing_only"
    assert product_gate["canonical_next_action"] == product_gate["next_action"]
    assert product_gate["canonical_next_command"] == product_gate["next_command"]
    assert product_gate["minimum_features_required"]["safe_read_only_gate"] is True
    assert product_gate["minimum_features_required"]["zero_hidden_calls_or_writes"] is True
    assert (
        product_gate["minimum_features_required"][
            "trusted_full_market_priced_in_answer"
        ]
        is False
    )
    assert "Any command with --execute" in payload["blocked_until_explicit_approval"]


def test_assert_trial_ready_minimum_product_mode_blocks_safe_browsing_only(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trial-demo-strict.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    exit_code = main(
        [
            "assert-trial-ready",
            "--available-at",
            "2026-05-23T12:00:00+00:00",
            "--minimum-product",
            "--json",
        ]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["safe_to_try_read_only"] is True
    assert payload["minimum_useful_product"]["ready"] is False
    assert payload["minimum_useful_product"]["status"] == "blocked"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_assert_trial_ready_minimum_product_mode_passes_only_when_gate_ready(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trial-ready-strict.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_trial_readiness_payload(*_args, **_kwargs) -> dict[str, object]:
        return {
            "schema_version": "trial-readiness-v1",
            "status": "safe_read_only",
            "safe_to_try_read_only": True,
            "minimum_useful_product": {
                "schema_version": "trial-minimum-useful-product-v1",
                "status": "ready",
                "ready": True,
                "external_calls_made": 0,
                "db_writes_made": 0,
            },
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    monkeypatch.setattr(
        cli_module,
        "trial_readiness_payload",
        fake_trial_readiness_payload,
    )

    exit_code = main(
        [
            "assert-trial-ready",
            "--available-at",
            "2026-05-23T12:00:00+00:00",
            "--minimum-product",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["minimum_useful_product"]["ready"] is True
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


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
    product_gate = payload["minimum_useful_product"]
    assert product_gate["ready"] is False
    assert product_gate["first_blocker"] == "llm_real_mode_disabled"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_trial_minimum_product_gate_preserves_priced_in_blocker_command() -> None:
    payload = trial_readiness_payload(
        create_engine("sqlite:///:memory:"),
        AppConfig(),
        available_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
        priced_in_answer={
            "schema_version": "priced-in-answer-v1",
            "status": "blocked",
            "counts": {"total_rows": 5},
            "first_blocker": "market_bars",
            "canonical_next_action": "Review residual market-bar rows.",
            "canonical_next_command": (
                "catalyst-radar market-bars residual-review "
                "--expected-as-of 2026-05-15"
            ),
            "full_market_trust_gate": {
                "trusted_full_market_answer": False,
                "first_blocker": "market_bars",
            },
            "full_scan": {
                "mode": "selected_universe",
                "active_securities": 12669,
                "scanned_rows": 2429,
                "ranked_rows": 2429,
            },
            "scan_scope": {"mode": "selected_universe", "total_rows": 2429},
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        shadow_readiness={
            "schema_version": "shadow-readiness-v1",
            "status": "setup_required",
            "ready": False,
            "first_blocker": "market_bars",
            "checks": [],
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        value_report={
            "schema_version": "monthly-value-report-v1",
            "verdict": "insufficient_evidence",
            "first_blocker": "candidate_ledger_coverage",
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
    )

    assert payload["safe_to_try_read_only"] is True
    product_gate = payload["minimum_useful_product"]
    assert product_gate["ready"] is False
    assert product_gate["first_blocker"] == "market_bars"
    assert product_gate["canonical_next_action"] == "Review residual market-bar rows."
    assert product_gate["canonical_next_command"] == (
        "catalyst-radar market-bars residual-review --expected-as-of 2026-05-15"
    )
    assert product_gate["next_command"] == product_gate["canonical_next_command"]
    assert product_gate["external_calls_made"] == 0
    assert product_gate["db_writes_made"] == 0


def test_trial_minimum_product_gate_surfaces_market_bar_approval_packet_without_writes(
    tmp_path,
) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'trial-approval.db').as_posix()}")
    create_schema(engine)
    market_repo = MarketRepository(engine)
    market_repo.upsert_securities(
        [
            _security("AAPL", active=True, market_cap=3_000_000_000, avg_volume=50_000_000),
            _security("AACO", active=True, market_cap=0, avg_volume=0),
        ]
    )
    market_repo.upsert_daily_bars([_daily_bar("AAPL", date(2026, 5, 8))])

    payload = trial_readiness_payload(
        engine,
        AppConfig(),
        available_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
        priced_in_answer={
            "schema_version": "priced-in-answer-v1",
            "status": "blocked",
            "counts": {"total_rows": 2},
            "first_blocker": "market_bars",
            "canonical_next_action": "Review residual market-bar rows.",
            "canonical_next_command": (
                "catalyst-radar market-bars residual-review "
                "--expected-as-of 2026-05-08"
            ),
            "full_market_trust_gate": {
                "trusted_full_market_answer": False,
                "first_blocker": "market_bars",
                "blocker_detail": {
                    "schema_version": "priced-in-market-bar-blocker-detail-v1",
                    "source": "market_bars",
                    "expected_as_of": "2026-05-08",
                    "missing_as_of_bar": 1,
                    "stocks_only": False,
                },
            },
            "full_scan": {
                "mode": "full_scan",
                "active_securities": 2,
                "scanned_rows": 1,
                "ranked_rows": 2,
            },
            "scan_scope": {"mode": "full_scan", "total_rows": 2},
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        shadow_readiness={
            "schema_version": "shadow-readiness-v1",
            "status": "setup_required",
            "ready": False,
            "first_blocker": "market_bars",
            "checks": [],
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        value_report={
            "schema_version": "monthly-value-report-v1",
            "verdict": "insufficient_evidence",
            "first_blocker": "candidate_ledger_coverage",
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
    )

    product_gate = payload["minimum_useful_product"]
    approval = product_gate["approval_required_unblock"]
    assert product_gate["ready"] is False
    assert product_gate["first_blocker"] == "market_bars"
    assert approval["schema_version"] == "trial-minimum-product-approval-required-v1"
    assert approval["approval_required"] is True
    assert approval["status"] == "ready_to_execute"
    assert approval["expected_missing_count"] == 1
    assert approval["expected_eligible_count"] == 1
    assert approval["db_writes_required_to_execute"] == 1
    assert approval["projected_market_bar_gate_cleared"] is True
    assert approval["execute_would_clear_market_bar_gate"] is True
    assert "--expect-missing-count 1" in approval["approval_command"]
    assert "--expect-eligible-count 1" in approval["approval_command"]
    assert approval["approval_command"].endswith("--execute --json")
    assert approval["external_calls_made"] == 0
    assert approval["db_writes_made"] == 0
    assert product_gate["external_calls_made"] == 0
    assert product_gate["db_writes_made"] == 0


def test_trial_readiness_marks_minimum_useful_product_ready_only_after_trusted_answer() -> None:
    payload = trial_readiness_payload(
        create_engine("sqlite:///:memory:"),
        AppConfig(),
        available_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
        priced_in_answer={
            "schema_version": "priced-in-answer-v1",
            "status": "ready",
            "answer": "Full-market priced-in answer is ready for research review.",
            "counts": {"total_rows": 25},
            "full_market_trust_gate": {
                "trusted_full_market_answer": True,
            },
            "full_scan": {
                "mode": "full_scan",
                "active_securities": 25,
                "scanned_rows": 25,
                "ranked_rows": 25,
            },
            "scan_scope": {"mode": "full_scan"},
            "canonical_next_action": "Open the dashboard.",
            "canonical_next_command": "catalyst-radar dashboard-tui",
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        shadow_readiness={
            "schema_version": "shadow-readiness-v1",
            "status": "blocked",
            "ready": False,
            "first_blocker": "validation_ready",
            "canonical_next_action": "Run validation replay before shadow mode.",
            "canonical_next_command": "catalyst-radar validation-report --latest --json",
            "checks": [],
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        value_report={
            "schema_version": "monthly-value-report-v1",
            "verdict": "insufficient_evidence",
            "first_blocker": "minimum_useful_evidence",
            "canonical_next_action": "Keep collecting value evidence.",
            "canonical_next_command": "catalyst-radar value-report --month 2026-05 --json",
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
    )

    assert payload["safe_to_try_read_only"] is True
    product_gate = payload["minimum_useful_product"]
    assert product_gate["ready"] is True
    assert product_gate["status"] == "ready"
    assert product_gate["highest_allowed_use"] == "read_only_decision_support"
    assert product_gate["first_blocker"] is None
    assert product_gate["canonical_next_action"] == product_gate["next_action"]
    assert product_gate["canonical_next_command"] == product_gate["next_command"]
    assert product_gate["canonical_next_command"] == "catalyst-radar dashboard-tui"
    assert product_gate["minimum_features_required"] == {
        "safe_read_only_gate": True,
        "shadow_gate_visible": True,
        "trusted_full_market_priced_in_answer": True,
        "value_report_visible": True,
        "zero_hidden_calls_or_writes": True,
    }


def _security(
    ticker: str,
    *,
    active: bool,
    market_cap: float,
    avg_volume: float,
) -> Security:
    return Security(
        ticker=ticker,
        name=f"{ticker} Inc.",
        exchange="NASDAQ",
        sector="Technology" if market_cap else "Unknown",
        industry="Software" if market_cap else "Unknown",
        market_cap=market_cap,
        avg_dollar_volume_20d=avg_volume,
        has_options=market_cap > 0,
        is_active=active,
        updated_at=datetime(2026, 5, 8, 20, tzinfo=UTC),
        metadata={"type": "CS"},
    )


def _daily_bar(ticker: str, bar_date: date) -> DailyBar:
    return DailyBar(
        ticker=ticker,
        date=bar_date,
        open=100,
        high=101,
        low=99,
        close=100,
        volume=1_000_000,
        vwap=100,
        adjusted=True,
        provider="test",
        source_ts=datetime(2026, 5, 8, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
    )
