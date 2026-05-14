from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, insert, update

from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerMarketSnapshot,
    BrokerPosition,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_market_snapshot_id,
    broker_position_id,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.data import (
    actionability_breakdown_payload,
    activation_summary_payload,
    agent_review_summary_payload,
    candidate_decision_labels_payload,
    candidate_delta_payload,
    candidate_rows_with_market_context,
    data_source_coverage_payload,
    investment_readiness_payload,
    live_activation_plan_payload,
    live_data_activation_contract_payload,
    load_alert_detail,
    load_alert_rows,
    load_broker_summary,
    load_candidate_rows,
    load_cost_summary,
    load_ipo_s1_rows,
    load_ops_health,
    load_radar_run_summary,
    load_theme_rows,
    load_ticker_detail,
    load_validation_summary,
    operator_work_queue_payload,
    opportunity_focus_payload,
    provider_preflight_payload,
    radar_discovery_snapshot_payload,
    radar_readiness_payload,
    radar_research_shortlist_payload,
    radar_run_call_plan_payload,
    radar_run_cooldown_payload,
    radar_run_default_scope_payload,
    readiness_checklist_payload,
    research_shortlist_payload,
    telemetry_tape_payload,
    universe_coverage_payload,
)
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    candidate_states,
    daily_bars,
    decision_cards,
    events,
    job_locks,
    job_runs,
    paper_trades,
    provider_health,
    securities,
    signal_features,
    text_snippets,
    useful_alert_labels,
    user_feedback,
    validation_results,
    validation_runs,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
EARLIER_AS_OF = AS_OF - timedelta(days=1)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
NEXT_REVIEW_AT = datetime(2026, 5, 12, 21, tzinfo=UTC)
FUTURE_AT = AVAILABLE_AT + timedelta(days=30)


@pytest.fixture(autouse=True)
def _isolate_llm_config_env(monkeypatch) -> None:
    for key in (
        "CATALYST_ENABLE_PREMIUM_LLM",
        "CATALYST_LLM_DAILY_BUDGET_USD",
        "CATALYST_LLM_MONTHLY_BUDGET_USD",
        "CATALYST_LLM_TASK_DAILY_CAPS",
    ):
        monkeypatch.delenv(key, raising=False)


def test_load_candidate_rows_returns_latest_state_per_ticker(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_candidate_rows(engine)

    assert [row["id"] for row in rows] == ["state-msft-latest", "state-aapl-latest"]
    assert [row["ticker"] for row in rows] == ["MSFT", "AAPL"]
    msft_brief = rows[0]["research_brief"]
    assert msft_brief["focus"] == "Research now"
    assert msft_brief["why_now"] == "MSFT guidance raised"
    assert msft_brief["supporting_evidence"] == "MSFT guidance raised"
    assert msft_brief["risk_or_gap"] == "Valuation stretched"
    assert msft_brief["decision_card_status"] == "available: card-msft-latest"
    assert msft_brief["audit"]["provider_license_policy"]["license_tags"] == [
        "local-csv-fixture"
    ]
    aapl_brief = rows[1]["research_brief"]
    assert aapl_brief["focus"] == "Research now"
    assert aapl_brief["why_now"] == "breakout setup with score 76.0"
    assert aapl_brief["decision_card_status"] == (
        "not generated; candidate is not in manual-buy-review state"
    )


def test_opportunity_focus_payload_promotes_research_briefs(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_candidate_rows(engine)
    focus = opportunity_focus_payload(rows, limit=1)

    assert focus == [
        {
            "rank": 1,
            "ticker": "MSFT",
            "focus": "Research now",
            "state": "Warning",
            "score": 88.0,
            "why_now": "MSFT guidance raised",
            "top_catalyst": "MSFT guidance raised",
            "evidence": "MSFT guidance raised",
            "risk_or_gap": "Valuation stretched",
            "next_step": "Review the Decision Card before any trade action.",
            "card": "card-msft-latest",
        }
    ]


def test_candidate_delta_payload_summarizes_latest_run_changes(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with engine.begin() as conn:
        rows = [
            _candidate_state_row(
                id="state-msft-prior",
                ticker="MSFT",
                as_of=EARLIER_AS_OF,
                state=ActionState.ADD_TO_WATCHLIST.value,
                final_score=70.0,
                created_at=AVAILABLE_AT - timedelta(days=1),
            ),
            _candidate_state_row(
                id="state-msft-current",
                ticker="MSFT",
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                final_score=88.0,
                created_at=AVAILABLE_AT,
            ),
            _candidate_state_row(
                id="state-aaa-prior",
                ticker="AAA",
                as_of=EARLIER_AS_OF,
                state=ActionState.BLOCKED.value,
                final_score=82.0,
                created_at=AVAILABLE_AT - timedelta(days=1),
            ),
            _candidate_state_row(
                id="state-aaa-current",
                ticker="AAA",
                as_of=AS_OF,
                state=ActionState.BLOCKED.value,
                final_score=83.0,
                created_at=AVAILABLE_AT,
            ),
            _candidate_state_row(
                id="state-nvda-current",
                ticker="NVDA",
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                final_score=91.0,
                created_at=AVAILABLE_AT,
            ),
            _candidate_state_row(
                id="state-ibm-stale",
                ticker="IBM",
                as_of=EARLIER_AS_OF,
                state=ActionState.WARNING.value,
                final_score=72.0,
                created_at=AVAILABLE_AT - timedelta(days=1),
            ),
        ]
        rows[1]["hard_blocks"] = ["risk_hard_block"]
        rows[2]["hard_blocks"] = ["data_stale"]
        rows[3]["hard_blocks"] = ["liquidity_hard_block"]
        conn.execute(insert(candidate_states), rows)

    payload = candidate_delta_payload(
        engine,
        radar_run_summary={
            "as_of": AS_OF.date().isoformat(),
            "decision_available_at": AVAILABLE_AT.isoformat(),
        },
        score_move_threshold=5.0,
    )

    assert payload["schema_version"] == "candidate-delta-v1"
    assert payload["status"] == "changed"
    assert payload["summary"] == {
        "current_run_candidates": 3,
        "stale_context_candidates": 1,
        "changed_candidates": 3,
        "new_candidates": 1,
        "state_changes": 1,
        "score_moves": 1,
        "blocker_changes": 2,
    }
    rows_by_ticker = {str(row["ticker"]): row for row in payload["rows"]}
    assert rows_by_ticker["NVDA"]["change_type"] == "new_candidate"
    assert rows_by_ticker["MSFT"]["change_type"] == "state_changed"
    assert rows_by_ticker["MSFT"]["previous_state"] == ActionState.ADD_TO_WATCHLIST.value
    assert rows_by_ticker["MSFT"]["current_state"] == ActionState.WARNING.value
    assert rows_by_ticker["MSFT"]["score_change"] == 18.0
    assert rows_by_ticker["MSFT"]["state_changed"] is True
    assert rows_by_ticker["MSFT"]["score_moved"] is True
    assert rows_by_ticker["MSFT"]["blocker_changed"] is True
    assert rows_by_ticker["MSFT"]["blockers_added"] == ["risk_hard_block"]
    assert rows_by_ticker["AAA"]["change_type"] == "blocker_changed"
    assert rows_by_ticker["AAA"]["blocker_changed"] is True
    assert rows_by_ticker["AAA"]["blockers_added"] == ["liquidity_hard_block"]
    assert rows_by_ticker["AAA"]["blockers_removed"] == ["data_stale"]


def test_candidate_delta_payload_reports_no_current_run_candidates(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="state-msft-stale",
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=70.0,
                    created_at=AVAILABLE_AT - timedelta(days=1),
                )
            ],
        )

    payload = candidate_delta_payload(
        engine,
        radar_run_summary={
            "as_of": AS_OF.date().isoformat(),
            "decision_available_at": AVAILABLE_AT.isoformat(),
        },
    )

    assert payload["status"] == "no_current_candidates"
    assert payload["summary"]["current_run_candidates"] == 0
    assert payload["summary"]["stale_context_candidates"] == 1
    assert payload["rows"] == []


def test_candidate_delta_payload_counts_run_candidates_created_after_decision_cutoff(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    early_decision_at = AS_OF - timedelta(hours=2)
    finished_at = early_decision_at + timedelta(seconds=8)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="state-msft-prior",
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=70.0,
                    created_at=early_decision_at - timedelta(days=1),
                ),
                _candidate_state_row(
                    id="state-msft-current",
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    created_at=early_decision_at + timedelta(seconds=3),
                ),
            ],
        )

    payload = candidate_delta_payload(
        engine,
        radar_run_summary={
            "as_of": AS_OF.date().isoformat(),
            "decision_available_at": early_decision_at.isoformat(),
            "finished_at": finished_at.isoformat(),
        },
        score_move_threshold=5.0,
    )

    assert payload["status"] == "changed"
    assert payload["summary"]["current_run_candidates"] == 1
    assert payload["summary"]["state_changes"] == 1
    assert payload["rows"][0]["ticker"] == "MSFT"


def test_actionability_breakdown_payload_explains_current_queue() -> None:
    rows = [
        {
            "ticker": "MSFT",
            "state": ActionState.WARNING.value,
            "final_score": 88.0,
            "decision_card_id": "card-msft",
            "research_brief": {
                "focus": "Research now",
                "risk_or_gap": "Valuation stretched",
                "next_step": "Review the Decision Card before any trade action.",
            },
        },
        {
            "ticker": "AAPL",
            "state": ActionState.BLOCKED.value,
            "final_score": 92.0,
            "research_brief": {
                "focus": "Blocked",
                "risk_or_gap": "Hard policy block",
                "next_step": "Do not escalate until hard blocks clear.",
            },
        },
        {
            "ticker": "NVDA",
            "state": ActionState.BLOCKED.value,
            "final_score": 91.0,
            "research_brief": {
                "focus": "Blocked",
                "risk_or_gap": "Hard policy block",
                "next_step": "Do not escalate until hard blocks clear.",
            },
        },
    ]

    payload = actionability_breakdown_payload(rows)

    assert payload["status"] == "research"
    assert payload["total_candidates"] == 3
    assert payload["counts"] == [
        {"bucket": "Research now", "count": 1},
        {"bucket": "Blocked or risk review", "count": 2},
    ]
    assert payload["top_blockers"][0] == {
        "risk_or_gap": "Hard policy block",
        "count": 2,
        "sample_tickers": "AAPL, NVDA",
    }
    assert payload["next_actions"][0]["ticker"] == "MSFT"
    assert payload["next_actions"][0]["card"] == "card-msft"


def test_candidate_decision_labels_mark_research_only_rows() -> None:
    rows = candidate_decision_labels_payload(
        [
            {
                "ticker": "AAA",
                "state": ActionState.BLOCKED.value,
                "decision_card_id": "",
            },
            {
                "ticker": "BBB",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "decision_card_id": "",
            },
            {
                "ticker": "CCC",
                "state": ActionState.WARNING.value,
                "decision_card_id": "",
            },
        ],
        {"decision_mode": "research_only", "next_action": "Configure live sources."},
    )

    assert rows[0]["decision_status"] == "blocked"
    assert rows[0]["decision_next_step"] == "Clear hard blocks before escalation."
    assert rows[1]["decision_status"] == "missing_card"
    assert rows[1]["decision_next_step"] == "Build a Decision Card first."
    assert rows[2]["decision_status"] == "research_only"
    assert rows[2]["decision_next_step"] == "Configure live sources."


def test_candidate_decision_labels_mark_manual_buy_review_rows() -> None:
    rows = candidate_decision_labels_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "decision_card_id": "card-msft",
            },
            {
                "ticker": "AAPL",
                "state": ActionState.WARNING.value,
                "decision_card_id": "",
            },
        ],
        {"decision_mode": "manual_buy_review"},
    )

    assert rows[0]["decision_status"] == "manual_buy_review"
    assert rows[0]["decision_next_step"] == "Review card, exposure, and hard blocks."
    assert rows[1]["decision_status"] == "research_only"
    assert rows[1]["decision_next_step"] == "Not in manual buy-review state."


def test_candidate_rows_with_market_context_attaches_latest_schwab_snapshot() -> None:
    rows = candidate_rows_with_market_context(
        [{"ticker": "MSFT", "final_score": 88.0}, {"ticker": "AAPL", "final_score": 76.0}],
        [
            {
                "ticker": "MSFT",
                "as_of": (AVAILABLE_AT - timedelta(minutes=5)).isoformat(),
                "last_price": 340.0,
                "day_change_percent": 1.2,
                "relative_volume": 1.1,
                "option_call_put_ratio": 0.8,
                "created_at": (AVAILABLE_AT - timedelta(minutes=5)).isoformat(),
            },
            {
                "ticker": "MSFT",
                "as_of": AVAILABLE_AT.isoformat(),
                "last_price": 345.5,
                "day_change_percent": 2.4,
                "relative_volume": 1.9,
                "option_call_put_ratio": 2.1,
                "created_at": AVAILABLE_AT.isoformat(),
            },
        ],
    )

    assert rows[0]["ticker"] == "MSFT"
    assert rows[0]["schwab_context_status"] == "available"
    assert rows[0]["schwab_last_price"] == 345.5
    assert rows[0]["schwab_day_change_percent"] == 2.4
    assert rows[0]["schwab_relative_volume"] == 1.9
    assert rows[0]["schwab_option_call_put_ratio"] == 2.1
    assert rows[1]["ticker"] == "AAPL"
    assert rows[1]["schwab_context_status"] == "missing"
    assert rows[1]["schwab_last_price"] is None


def test_research_shortlist_prioritizes_review_and_research_rows() -> None:
    rows = research_shortlist_payload(
        [
            {
                "ticker": "AAA",
                "state": ActionState.BLOCKED.value,
                "final_score": 100.0,
                "decision_card_id": "",
                "research_brief": {
                    "risk_or_gap": "Hard block",
                    "next_step": "Clear hard block.",
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                            "external_export_allowed": False,
                        }
                    },
                },
            },
            {
                "ticker": "BBB",
                "state": ActionState.WARNING.value,
                "final_score": 84.0,
                "decision_card_id": "",
                "research_brief": {
                    "why_now": "New catalyst",
                    "top_catalyst": "Revenue guide raised",
                    "risk_or_gap": "Needs primary-source review",
                    "next_step": "Open source filing.",
                },
            },
            {
                "ticker": "CCC",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "final_score": 91.0,
                "decision_card_id": "card-ccc",
                "research_brief": {
                    "why_now": "Validated catalyst",
                    "top_catalyst": "Contract award",
                    "next_step": "Review card.",
                },
            },
        ],
        {"decision_mode": "manual_buy_review", "manual_buy_review_ready": True},
    )

    assert rows["status"] == "manual_review"
    assert rows["safe_to_make_investment_decision"] is True
    assert [row["ticker"] for row in rows["rows"]] == ["CCC", "BBB", "AAA"]
    assert rows["rows"][0]["priority"] == "manual_review"
    assert rows["rows"][1]["priority"] == "research_now"
    assert rows["rows"][2]["audit"]["provider_license_policy"]["license_tags"] == [
        "local-csv-fixture"
    ]


def test_research_shortlist_payload_surfaces_schwab_market_context() -> None:
    rows = research_shortlist_payload(
        [
            {
                "ticker": "BBB",
                "state": ActionState.WARNING.value,
                "final_score": 84.0,
                "decision_card_id": "",
                "research_brief": {
                    "why_now": "New catalyst",
                    "top_catalyst": "Revenue guide raised",
                    "risk_or_gap": "Needs primary-source review",
                    "next_step": "Open source filing.",
                },
            }
        ],
        {"decision_mode": "research_only"},
        market_context=[
            {
                "ticker": "BBB",
                "as_of": AVAILABLE_AT.isoformat(),
                "last_price": 42.25,
                "day_change_percent": 3.1,
                "relative_volume": 2.3,
                "price_trend_5d_percent": 6.5,
                "option_call_put_ratio": 1.7,
                "created_at": AVAILABLE_AT.isoformat(),
            }
        ],
    )

    row = rows["rows"][0]
    assert row["ticker"] == "BBB"
    assert row["schwab_context_status"] == "available"
    assert row["schwab_last_price"] == 42.25
    assert row["schwab_day_change_percent"] == 3.1
    assert row["schwab_relative_volume"] == 2.3
    assert row["schwab_price_trend_5d_percent"] == 6.5
    assert row["schwab_option_call_put_ratio"] == 1.7


def test_actionability_breakdown_payload_flags_buy_review_ready() -> None:
    payload = actionability_breakdown_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "final_score": 96.0,
                "decision_card_id": "card-msft",
                "research_brief": {
                    "risk_or_gap": "Position size needs review",
                    "next_step": "Review the Decision Card before any trade action.",
                },
            }
        ]
    )

    assert payload["status"] == "ready"
    assert payload["counts"] == [{"bucket": "Buy-review ready", "count": 1}]
    assert "ready for manual buy review" in str(payload["headline"])


def test_investment_readiness_payload_blocks_fixture_candidates() -> None:
    actionability = actionability_breakdown_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "final_score": 96.0,
                "decision_card_id": "card-msft",
            }
        ]
    )
    readiness = investment_readiness_payload(
        {
            "status": "fixture",
            "source_modes": {"market": "fixture", "events": "fixture"},
            "freshness": {"latest_bars_older_than_as_of": False},
            "yield": {"candidate_packets": 1, "decision_cards": 1},
            "blockers": [
                {
                    "code": "fixture_market_data",
                    "finding": "Market data is still fixture-backed.",
                    "next_action": "Configure Polygon before relying on broad US-market discovery.",
                }
            ],
        },
        actionability,
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "decision_card_id": "card-msft",
            }
        ],
    )

    assert readiness["status"] == "research_only"
    assert readiness["decision_mode"] == "research_only"
    assert readiness["manual_buy_review_ready"] is False
    assert "research-only" in str(readiness["headline"])
    assert "fixture_market_data" in str(readiness["evidence"])
    assert readiness["next_action"] == (
        "Configure Polygon before relying on broad US-market discovery."
    )


def test_investment_readiness_payload_allows_live_buy_review() -> None:
    actionability = actionability_breakdown_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "final_score": 96.0,
                "decision_card_id": "card-msft",
            }
        ]
    )
    readiness = investment_readiness_payload(
        {
            "status": "ready",
            "source_modes": {"market": "live", "events": "live"},
            "freshness": {"latest_bars_older_than_as_of": False},
            "yield": {"candidate_packets": 1, "decision_cards": 1},
            "blockers": [],
        },
        actionability,
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "decision_card_id": "card-msft",
            }
        ],
    )

    assert readiness["status"] == "ready"
    assert readiness["decision_mode"] == "manual_buy_review"
    assert readiness["manual_buy_review_ready"] is True
    assert "1 candidate" in str(readiness["headline"])


def test_investment_readiness_payload_requires_buy_review_card() -> None:
    actionability = actionability_breakdown_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "final_score": 96.0,
                "decision_card_id": "",
            }
        ]
    )
    readiness = investment_readiness_payload(
        {
            "status": "ready",
            "source_modes": {"market": "live", "events": "live"},
            "freshness": {"latest_bars_older_than_as_of": False},
            "yield": {"candidate_packets": 1, "decision_cards": 1},
            "blockers": [],
        },
        actionability,
        [
            {
                "ticker": "MSFT",
                "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
                "decision_card_id": "",
            }
        ],
    )

    assert readiness["status"] == "research_only"
    assert readiness["decision_mode"] == "research_only"
    assert readiness["manual_buy_review_ready"] is False
    assert "missing Decision Cards" in str(readiness["headline"])


def test_investment_readiness_payload_keeps_live_research_out_of_buy_review() -> None:
    actionability = actionability_breakdown_payload(
        [
            {
                "ticker": "MSFT",
                "state": ActionState.WARNING.value,
                "final_score": 88.0,
                "decision_card_id": "",
            }
        ]
    )
    readiness = investment_readiness_payload(
        {
            "status": "ready",
            "source_modes": {"market": "live", "events": "live"},
            "freshness": {"latest_bars_older_than_as_of": False},
            "yield": {"candidate_packets": 1, "decision_cards": 0},
            "blockers": [],
        },
        actionability,
        [
            {
                "ticker": "MSFT",
                "state": ActionState.WARNING.value,
                "decision_card_id": "",
            }
        ],
    )

    assert readiness["status"] == "research_only"
    assert readiness["decision_mode"] == "research_only"
    assert readiness["manual_buy_review_ready"] is False
    assert "need research" in str(readiness["headline"])


def test_radar_readiness_payload_summarizes_operator_decision_gate(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": ["MSFT", "AAPL"],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=1),
                    metadata={**metadata, "result_status": "success"},
                    requested_count=2,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=2),
                    metadata={**metadata, "result_status": "success"},
                    requested_count=1,
                    raw_count=1,
                    normalized_count=1,
                ),
                _job_run_row(
                    "decision-cards",
                    job_type="decision_cards",
                    status="skipped",
                    started_at=AVAILABLE_AT + timedelta(seconds=3),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "no_manual_buy_review_inputs",
                    },
                ),
            ],
        )

    payload = radar_readiness_payload(engine, AppConfig.from_env({}))

    assert payload["schema_version"] == "radar-readiness-v1"
    assert payload["status"] == "research_only"
    assert payload["decision_mode"] == "research_only"
    assert payload["safe_to_make_investment_decision"] is False
    assert payload["latest_run_cutoff"] == AVAILABLE_AT.isoformat()
    assert payload["run_path"] == {
        "required_total": 2,
        "required_complete": 2,
        "blocking_count": 0,
        "expected_gate_count": 1,
    }
    assert payload["radar_run"]["provider"] == "csv"
    assert payload["live_activation_plan"]["status"] == "blocked"
    assert payload["investment_readiness"]["manual_buy_review_ready"] is False
    assert payload["candidate_delta"]["schema_version"] == "candidate-delta-v1"
    assert payload["candidate_delta"]["summary"]["current_run_candidates"] == 2
    assert payload["operator_work_queue"]["schema_version"] == "operator-work-queue-v1"
    assert payload["operator_work_queue"]["safe_to_make_investment_decision"] is False
    assert payload["candidate_decision_labels"][0]["ticker"] == "MSFT"
    assert payload["candidate_decision_labels"][0]["decision_status"] == "research_only"
    assert payload["candidate_decision_labels"][0]["next_step"]
    assert payload["candidate_decision_labels"][0]["audit"]["provider_license_policy"][
        "external_export_allowed"
    ] is False


def test_radar_readiness_candidate_delta_uses_artifact_cutoff(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    early_decision_at = AS_OF - timedelta(hours=2)
    metadata = {
        "as_of": AS_OF.date().isoformat(),
        "decision_available_at": early_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": ["MSFT"],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="state-msft-prior",
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=70.0,
                    created_at=early_decision_at + timedelta(seconds=2),
                ),
                _candidate_state_row(
                    id="state-msft-current",
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    created_at=early_decision_at + timedelta(seconds=3),
                ),
            ],
        )
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "delta-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=early_decision_at + timedelta(seconds=1),
                    metadata={**metadata, "result_status": "success"},
                    requested_count=1,
                    raw_count=1,
                    normalized_count=1,
                ),
                _job_run_row(
                    "delta-scoring",
                    job_type="scoring_policy",
                    status="success",
                    started_at=early_decision_at + timedelta(seconds=2),
                    metadata={**metadata, "result_status": "success"},
                    requested_count=1,
                    raw_count=1,
                    normalized_count=1,
                ),
            ],
        )

    payload = radar_readiness_payload(engine, AppConfig.from_env({}))

    delta = payload["candidate_delta"]
    assert delta["summary"]["current_run_candidates"] == 1
    assert delta["summary"]["state_changes"] == 1
    assert delta["rows"][0]["ticker"] == "MSFT"
    assert delta["rows"][0]["change_type"] == "state_changed"


def test_radar_readiness_candidate_delta_keeps_stale_context_without_current_rows(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    stale_run_cutoff = AVAILABLE_AT + timedelta(days=4)
    metadata = {
        "as_of": stale_run_cutoff.date().isoformat(),
        "decision_available_at": stale_run_cutoff.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-msft-stale-readiness",
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "stale-readiness-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=stale_run_cutoff,
                    metadata={**metadata, "result_status": "success"},
                    requested_count=1,
                    raw_count=0,
                    normalized_count=0,
                ),
                _job_run_row(
                    "stale-readiness-scoring",
                    job_type="scoring_policy",
                    status="success",
                    started_at=stale_run_cutoff + timedelta(seconds=1),
                    metadata={**metadata, "result_status": "success"},
                    requested_count=0,
                    raw_count=0,
                    normalized_count=0,
                ),
            ],
        )

    payload = radar_readiness_payload(engine, AppConfig.from_env({}))

    delta = payload["candidate_delta"]
    assert delta["status"] == "no_current_candidates"
    assert delta["summary"]["current_run_candidates"] == 0
    assert delta["summary"]["stale_context_candidates"] == 1


def test_radar_readiness_candidate_delta_treats_candidates_without_run_as_context(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    payload = radar_readiness_payload(engine, AppConfig.from_env({}))

    delta = payload["candidate_delta"]
    assert delta["status"] == "no_current_candidates"
    assert delta["summary"]["current_run_candidates"] == 0
    assert delta["summary"]["stale_context_candidates"] == 2


def test_radar_research_shortlist_payload_uses_latest_candidates(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    payload = radar_research_shortlist_payload(engine, AppConfig.from_env({}), limit=2)

    assert payload["schema_version"] == "research-shortlist-v1"
    assert payload["status"] in {"research", "monitor"}
    assert payload["count"] == 2
    assert payload["radar_status"] == "unknown"
    assert payload["rows"][0]["ticker"] == "MSFT"
    assert payload["rows"][0]["why_now"] == "MSFT guidance raised"
    assert payload["rows"][0]["decision_status"] == "research_only"


def test_operator_work_queue_prioritizes_setup_blockers_and_candidate_context(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
    )
    radar_summary = load_radar_run_summary(engine)
    ops_health = load_ops_health(engine, now=AVAILABLE_AT)
    broker_summary = load_broker_summary(engine)
    discovery = radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=radar_summary,
        ops_health=ops_health,
    )
    candidates = load_candidate_rows(engine, available_at=AVAILABLE_AT)

    payload = operator_work_queue_payload(
        config,
        radar_run_summary=radar_summary,
        broker_summary=broker_summary,
        discovery_snapshot=discovery,
        candidate_rows=candidates,
    )

    assert payload["schema_version"] == "operator-work-queue-v1"
    assert payload["status"] == "blocked"
    assert payload["counts"]["blocking"] >= 2
    assert payload["investment_mode"] == "research_only"
    assert payload["safe_to_make_investment_decision"] is False
    assert [row["area"] for row in payload["rows"][:2]] == [
        "Live market scan",
        "Catalyst feed",
    ]
    assert payload["rows"][0]["priority"] == "must_fix"
    assert "fresh US-market coverage" in str(payload["rows"][0]["item"])
    assert any(row.get("area") == "Candidate" for row in payload["rows"])


def test_data_source_coverage_payload_marks_fixture_and_read_only_modes() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
        schwab_order_submission_enabled=False,
        schwab_sync_min_interval_seconds=900,
    )

    rows = data_source_coverage_payload(
        config,
        broker_summary={
            "snapshot": {
                "connection_status": "connected",
                "account_count": 1,
                "position_count": 4,
            },
            "exposure": {"broker_data_stale": False},
            "rate_limit_config": {"portfolio_sync_min_interval_seconds": 900},
        },
    )

    by_layer = {str(row["layer"]): row for row in rows}
    assert by_layer["Market data"]["mode"] == "fixture"
    assert by_layer["News/events"]["mode"] == "fixture"
    assert by_layer["Schwab portfolio"]["mode"] == "read_only_connected"
    assert "read_only=true" in str(by_layer["Schwab portfolio"]["guardrail"])
    assert by_layer["LLM review"]["mode"] == "disabled"
    assert by_layer["Order submission"]["mode"] == "disabled"

    stale_rows = data_source_coverage_payload(
        config,
        broker_summary={
            "snapshot": {"connection_status": "connected"},
            "exposure": {"broker_data_stale": True},
        },
    )

    stale_by_layer = {str(row["layer"]): row for row in stale_rows}
    assert stale_by_layer["Schwab portfolio"]["mode"] == "stale_read_only_connected"


def test_provider_preflight_payload_reports_fixture_no_live_calls() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
        schwab_sync_min_interval_seconds=900,
        schwab_market_sync_min_interval_seconds=300,
        schwab_market_sync_max_tickers=5,
    )

    rows = provider_preflight_payload(config)

    by_layer = {str(row["layer"]): row for row in rows}
    assert by_layer["Market data"]["status"] == "fixture"
    assert by_layer["Market data"]["call_budget"] == "0 live calls"
    assert by_layer["News/events"]["status"] == "fixture"
    assert by_layer["News/events"]["call_budget"] == "0 live calls"
    assert by_layer["LLM review"]["status"] == "optional"
    assert str(by_layer["LLM review"]["call_budget"]).startswith("0 LLM calls")
    assert "CATALYST_ENABLE_PREMIUM_LLM=1" in str(
        by_layer["LLM review"]["call_budget"]
    )
    assert "OPENAI_API_KEY" in str(by_layer["LLM review"]["call_budget"])
    assert "portfolio sync min 900s" in str(by_layer["Schwab portfolio"]["call_budget"])


def test_activation_summary_payload_calls_out_fixture_mode() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
    )
    run_summary = {
        "step_count": 10,
        "blocking_step_count": 0,
        "expected_gate_count": 4,
        "outcome_category_counts": {"completed": 6, "expected_gate": 4},
    }

    summary = activation_summary_payload(config, radar_run_summary=run_summary)

    assert summary["status"] == "fixture"
    assert "not a live US-market scan" in str(summary["headline"])
    assert "CATALYST_POLYGON_API_KEY" in str(summary["next_action"])
    assert "required_path=6/6" in str(summary["evidence"])


def test_activation_summary_payload_blocks_missing_live_credentials() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key=None,
        daily_event_provider="sec",
        sec_enable_live=False,
        sec_user_agent=None,
    )

    summary = activation_summary_payload(config, radar_run_summary={"steps": []})

    assert summary["status"] == "blocked"
    assert "blocked" in str(summary["headline"]).lower()
    assert "Live market scan" in str(summary["detail"])
    assert "CATALYST_POLYGON_API_KEY" in str(summary["next_action"])


def test_activation_summary_payload_reports_ready_live_inputs() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="fixture-key",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="MarketRadar test@example.com",
    )
    run_summary = {
        "step_count": 10,
        "blocking_step_count": 0,
        "expected_gate_count": 4,
        "outcome_category_counts": {"completed": 6, "expected_gate": 4},
        "steps": [
            _run_step("daily_bar_ingest", "success", requested=43, raw=43, normalized=43),
            _run_step("event_ingest", "success", requested=1, raw=1, normalized=1),
            _run_step("local_text_triage", "success", requested=1, raw=1, normalized=1),
            _run_step("feature_scan", "success", requested=6, raw=3, normalized=3),
            _run_step("scoring_policy", "success", requested=3, raw=3, normalized=3),
            _run_step("candidate_packets", "success", requested=2, raw=2, normalized=2),
            _run_step("decision_cards", "skipped", reason="no_manual_buy_review_inputs"),
            _run_step("llm_review", "skipped", reason="llm_disabled"),
            _run_step("digest", "skipped", reason="no_alerts"),
            _run_step(
                "validation_update",
                "skipped",
                reason="outcome_available_at_not_supplied",
            ),
        ],
    }

    summary = activation_summary_payload(config, radar_run_summary=run_summary)

    assert summary["status"] == "ready"
    assert "Live radar inputs are ready" in str(summary["headline"])
    assert "market=polygon/live" in str(summary["evidence"])
    assert "events=sec/live" in str(summary["evidence"])


def test_live_activation_plan_payload_separates_optional_gates_from_blockers() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
    )
    run_summary = {
        "step_count": 10,
        "required_step_count": 6,
        "required_completed_count": 6,
        "blocking_step_count": 0,
        "expected_gate_count": 4,
        "outcome_category_counts": {"completed": 6, "expected_gate": 4},
    }

    plan = live_activation_plan_payload(config, radar_run_summary=run_summary)

    assert plan["status"] == "blocked"
    assert "run_path=6/6" in str(plan["evidence"])
    assert "optional_expected_gates=4" in str(plan["evidence"])
    assert "CATALYST_POLYGON_API_KEY" in plan["missing_env"]
    by_area = {str(row["area"]): row for row in plan["tasks"]}
    assert by_area["Required run path"]["status"] == "ready"
    assert by_area["Required run path"]["current_state"] == "6/6 completed"
    assert by_area["Live market data"]["status"] == "blocked"
    assert "CATALYST_DAILY_MARKET_PROVIDER=polygon" in str(
        by_area["Live market data"]["missing_env"]
    )
    assert by_area["Agentic LLM review"]["status"] == "optional_setup"
    assert "CATALYST_ENABLE_PREMIUM_LLM=1" in str(
        by_area["Agentic LLM review"]["missing_env"]
    )
    assert "CATALYST_LLM_PROVIDER=openai" in str(
        by_area["Agentic LLM review"]["missing_env"]
    )
    assert "OPENAI_API_KEY" in str(by_area["Agentic LLM review"]["missing_env"])
    assert "CATALYST_LLM_SKEPTIC_MODEL" in str(
        by_area["Agentic LLM review"]["missing_env"]
    )
    assert "CATALYST_LLM_INPUT_COST_PER_1M" in str(
        by_area["Agentic LLM review"]["missing_env"]
    )
    assert "CATALYST_LLM_DAILY_BUDGET_USD" in str(
        by_area["Agentic LLM review"]["missing_env"]
    )
    assert "OPENAI_API_KEY" not in plan["missing_env"]


def test_live_activation_plan_payload_never_leaks_configured_secrets() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="polygon-secret-value",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="Secret User Agent",
        enable_premium_llm=True,
        llm_provider="openai",
        llm_evidence_model="evidence-model",
        llm_skeptic_model="skeptic-model",
        llm_decision_card_model="card-model",
        llm_daily_budget_usd=1.0,
        llm_monthly_budget_usd=10.0,
        openai_api_key="sk-secret-value",
    )
    run_summary = {
        "step_count": 6,
        "required_step_count": 6,
        "required_completed_count": 6,
        "blocking_step_count": 0,
        "expected_gate_count": 0,
        "outcome_category_counts": {"completed": 6},
    }

    plan = live_activation_plan_payload(config, radar_run_summary=run_summary)

    rendered = str(plan)
    assert plan["status"] == "ready"
    assert "polygon-secret-value" not in rendered
    assert "sk-secret-value" not in rendered
    assert "Secret User Agent" not in rendered


def test_live_activation_plan_marks_agentic_review_ready_when_capped() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="polygon-secret-value",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="MarketRadar test@example.com",
        enable_premium_llm=True,
        llm_provider="openai",
        llm_skeptic_model="skeptic-model",
        llm_input_cost_per_1m=1.0,
        llm_cached_input_cost_per_1m=0.1,
        llm_output_cost_per_1m=2.0,
        llm_pricing_updated_at="2026-05-14",
        llm_daily_budget_usd=1.0,
        llm_monthly_budget_usd=20.0,
        llm_task_daily_caps={"skeptic_review": 3},
        openai_api_key="sk-secret-value",
    )
    run_summary = {
        "step_count": 6,
        "required_step_count": 6,
        "required_completed_count": 6,
        "blocking_step_count": 0,
        "expected_gate_count": 0,
        "outcome_category_counts": {"completed": 6},
    }

    plan = live_activation_plan_payload(config, radar_run_summary=run_summary)

    by_area = {str(row["area"]): row for row in plan["tasks"]}
    assert by_area["Agentic LLM review"]["status"] == "ready"
    assert by_area["Agentic LLM review"]["missing_env"] == ""
    assert "sk-secret-value" not in str(plan)
    assert "polygon-secret-value" not in str(plan)


def test_live_data_activation_contract_gives_exact_safe_next_steps() -> None:
    contract = live_data_activation_contract_payload(
        AppConfig(
            daily_market_provider="csv",
            daily_event_provider="news_fixture",
            polygon_tickers_max_pages=1,
            sec_daily_max_tickers=5,
            radar_run_min_interval_seconds=300,
        ),
        radar_run_summary={"steps": []},
    )

    assert contract["schema_version"] == "live-data-activation-contract-v1"
    assert contract["status"] == "blocked"
    assert contract["read_only"] is True
    assert contract["makes_external_calls"] is False
    assert "CATALYST_POLYGON_API_KEY" in contract["missing_env"]
    assert "CATALYST_SEC_USER_AGENT" in contract["missing_env"]
    assert contract["call_budget_if_activated"] == [
        {
            "operation": "read this activation contract",
            "max_external_calls": 0,
            "provider": "none",
        },
        {
            "operation": "seed universe once",
            "max_external_calls": 1,
            "provider": "polygon",
        },
        {
            "operation": "run one radar cycle",
            "max_external_calls": 6,
            "provider": "polygon + sec",
        },
    ]
    assert [row["step"] for row in contract["operator_steps"]] == [1, 2, 3, 4, 5, 6]
    assert "runs/call-plan" in str(contract["operator_steps"][3]["command"])
    assert "runs" in str(contract["operator_steps"][4]["command"])
    env_template = {str(row["name"]): row for row in contract["env_template"]}
    assert env_template["CATALYST_ENABLE_PREMIUM_LLM"]["value_template"] == "1"
    assert env_template["CATALYST_LLM_PROVIDER"]["value_template"] == "openai"
    assert "skeptic_review" in str(
        env_template["CATALYST_LLM_TASK_DAILY_CAPS"]["value_template"]
    )
    assert env_template["OPENAI_API_KEY"]["secret"] is True
    assert env_template["OPENAI_API_KEY"]["current"] == "missing"


def test_live_data_activation_contract_never_leaks_configured_secrets() -> None:
    contract = live_data_activation_contract_payload(
        AppConfig(
            daily_market_provider="polygon",
            market_provider="polygon",
            polygon_api_key="polygon-secret-value",
            daily_event_provider="sec",
            sec_enable_live=True,
            sec_user_agent="Secret User Agent",
            polygon_tickers_max_pages=2,
            sec_daily_max_tickers=3,
            radar_run_min_interval_seconds=600,
        ),
        radar_run_summary={"steps": []},
    )

    rendered = str(contract)
    assert contract["status"] == "ready"
    assert contract["missing_env"] == []
    assert "polygon-secret-value" not in rendered
    assert "Secret User Agent" not in rendered
    secret_rows = {
        str(row["name"]): row
        for row in contract["env_template"]
        if bool(row.get("secret"))
    }
    assert secret_rows["CATALYST_POLYGON_API_KEY"]["current"] == "set"
    assert secret_rows["CATALYST_SEC_USER_AGENT"]["current"] == "set"
    assert secret_rows["OPENAI_API_KEY"]["current"] == "missing"
    assert contract["call_budget_if_activated"][1]["max_external_calls"] == 2
    assert contract["call_budget_if_activated"][2]["max_external_calls"] == 4


def test_telemetry_tape_payload_summarizes_recent_radar_events() -> None:
    payload = telemetry_tape_payload(
        {
            "telemetry": {
                "event_count": 4,
                "latest_event_at": "2026-05-10T12:00:00+00:00",
                "status_counts": {"blocked": 1, "skipped": 2, "success": 1},
                "events": [
                    {
                        "event_type": "telemetry.radar_run.lock_contention",
                        "status": "blocked",
                        "reason": "lock_held",
                        "artifact_type": "radar_run",
                        "artifact_id": "radar-run-api-very-long-id",
                        "occurred_at": "2026-05-10T12:00:00+00:00",
                        "metadata": {
                            "provider": None,
                            "universe": "liquid-us",
                        },
                    },
                    {
                        "event_type": "telemetry.radar_run.completed",
                        "status": "success",
                        "artifact_type": "radar_run",
                        "artifact_id": "radar-run-api-completed",
                        "occurred_at": "2026-05-10T11:59:00+00:00",
                        "metadata": {
                            "daily_status": "success",
                            "step_counts": {"success": 6, "skipped": 4},
                            "blocked_steps": [],
                            "expected_gate_steps": [{"step": "llm_review"}],
                        },
                    },
                    {
                        "event_type": "telemetry.radar_run.step_finished",
                        "status": "skipped",
                        "reason": "no_manual_buy_review_inputs",
                        "artifact_type": "job_run",
                        "artifact_id": "decision-card-job-run",
                        "occurred_at": "2026-05-10T11:58:00+00:00",
                        "metadata": {
                            "step": "decision_cards",
                            "result_status": "skipped",
                            "result_reason": "no_manual_buy_review_inputs",
                            "outcome_category": "expected_gate",
                            "outcome_label": "Expected gate",
                            "operator_action": (
                                "No action required unless you want this optional gate to run."
                            ),
                            "blocks_reliance": False,
                        },
                    },
                    {
                        "event_type": "telemetry.radar_run.step_finished",
                        "status": "skipped",
                        "reason": "llm_disabled",
                        "artifact_type": "job_run",
                        "artifact_id": "legacy-llm-job-run",
                        "occurred_at": "2026-05-10T11:57:00+00:00",
                        "metadata": {
                            "step": "llm_review",
                            "outcome_category": "expected_gate",
                            "outcome_label": "Expected gate",
                        },
                    },
                ],
            }
        }
    )

    assert payload["status"] == "attention"
    assert payload["event_count"] == 4
    assert payload["events"][0]["event"] == "radar_run.lock_contention"
    assert payload["events"][0]["artifact"] == "radar_run:radar-run-api-very-long"
    assert payload["events"][0]["summary"] == "provider=default; universe=liquid-us"
    assert payload["events"][1]["summary"] == (
        "daily_status=success; steps=skipped=4, success=6; blocked=0; expected_gates=1"
    )
    assert payload["events"][2]["event"] == "radar_run.step_finished"
    assert payload["events"][2]["status"] == "expected_gate"
    assert payload["events"][2]["raw_status"] == "skipped"
    assert payload["events"][2]["step"] == "decision_cards"
    assert payload["events"][2]["outcome"] == "Expected gate"
    assert payload["events"][2]["blocks_reliance"] == "no"
    assert payload["events"][2]["summary"] == (
        "step=decision_cards; outcome=Expected gate; category=expected_gate; "
        "raw_status=skipped; reason=no_manual_buy_review_inputs; "
        "trigger=At least one candidate must pass policy into manual buy review.; "
        "action=No action required unless you want this optional gate to run."
    )
    assert payload["events"][3]["status"] == "expected_gate"
    assert payload["events"][3]["raw_status"] == "skipped"
    assert payload["events"][3]["summary"] == (
        "step=llm_review; outcome=Expected gate; category=expected_gate; "
        "raw_status=skipped; reason=llm_disabled; "
        "trigger=Request LLM dry-run review after candidate packets exist.; "
        "action=No action required unless you want this optional gate to run."
    )


def test_telemetry_tape_payload_refreshes_stale_step_metadata() -> None:
    payload = telemetry_tape_payload(
        {
            "telemetry": {
                "event_count": 1,
                "latest_event_at": "2026-05-10T11:58:00+00:00",
                "status_counts": {"skipped": 1},
                "events": [
                    {
                        "event_type": "telemetry.radar_run.step_finished",
                        "status": "skipped",
                        "reason": "degraded_mode_blocks_llm_review",
                        "artifact_type": "job_run",
                        "artifact_id": "legacy-stale-llm-job-run",
                        "occurred_at": "2026-05-10T11:58:00+00:00",
                        "metadata": {
                            "step": "llm_review",
                            "result_status": "skipped",
                            "result_reason": "degraded_mode_blocks_llm_review",
                            "outcome_category": "expected_gate",
                            "outcome_label": "Expected gate",
                            "operator_action": "Old non-blocking action.",
                            "blocks_reliance": False,
                        },
                    },
                ],
            }
        }
    )

    assert payload["events"][0]["status"] == "blocked_input"
    assert payload["events"][0]["outcome"] == "Blocked input"
    assert payload["events"][0]["blocks_reliance"] == "yes"
    assert payload["events"][0]["summary"] == (
        "step=llm_review; outcome=Blocked input; category=blocked_input; "
        "raw_status=skipped; reason=degraded_mode_blocks_llm_review; "
        "action=Resolve the upstream data/provider issue before relying on this run."
    )


def test_telemetry_tape_payload_summarizes_step_started_events() -> None:
    payload = telemetry_tape_payload(
        {
            "telemetry": {
                "event_count": 1,
                "latest_event_at": "2026-05-10T20:58:00+00:00",
                "status_counts": {"started": 1},
                "events": [
                    {
                        "event_type": "telemetry.radar_run.step_started",
                        "status": "started",
                        "artifact_type": "job_run",
                        "artifact_id": "job-started-1",
                        "occurred_at": "2026-05-10T20:58:00+00:00",
                        "metadata": {
                            "step": "daily_bar_ingest",
                            "job_id": "job-started-1",
                            "provider": "polygon",
                            "universe": "liquid-us",
                        },
                    },
                ],
            }
        }
    )

    assert payload["status"] == "ready"
    assert payload["events"] == [
        {
            "occurred_at": "2026-05-10T20:58:00+00:00",
            "event": "radar_run.step_started",
            "status": "started",
            "reason": "",
            "artifact": "job_run:job-started-1",
            "summary": (
                "step=daily_bar_ingest; job_id=job-started-1; "
                "provider=polygon; universe=liquid-us"
            ),
        }
    ]


def test_telemetry_tape_payload_handles_empty_telemetry() -> None:
    payload = telemetry_tape_payload({"telemetry": {"events": []}})

    assert payload["status"] == "empty"
    assert payload["events"] == []


def test_radar_run_cooldown_payload_reports_ready_without_active_lock(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    now = AVAILABLE_AT

    payload = radar_run_cooldown_payload(
        engine,
        AppConfig(radar_run_min_interval_seconds=300),
        now=now,
    )

    assert payload["status"] == "ready"
    assert payload["allowed"] is True
    assert payload["retry_after_seconds"] == 0
    assert payload["reset_at"] is None
    assert payload["min_interval_seconds"] == 300


def test_radar_run_cooldown_payload_reports_active_lock(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    now = AVAILABLE_AT
    reset_at = now + timedelta(seconds=125)
    with engine.begin() as conn:
        conn.execute(
            insert(job_locks).values(
                lock_name="manual_radar_run_cooldown",
                owner="api-radar-run-cooldown:test",
                acquired_at=now - timedelta(seconds=10),
                heartbeat_at=now - timedelta(seconds=10),
                expires_at=reset_at,
                metadata={"operation": "manual_radar_run"},
            )
        )

    payload = radar_run_cooldown_payload(
        engine,
        AppConfig(radar_run_min_interval_seconds=300),
        now=now,
    )

    assert payload["status"] == "cooldown"
    assert payload["allowed"] is False
    assert payload["retry_after_seconds"] == 125
    assert payload["reset_at"] == reset_at.isoformat()
    assert "lock=active" in str(payload["evidence"])


def test_radar_run_cooldown_payload_ignores_expired_lock(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    now = AVAILABLE_AT
    expired_at = now - timedelta(seconds=5)
    with engine.begin() as conn:
        conn.execute(
            insert(job_locks).values(
                lock_name="manual_radar_run_cooldown",
                owner="api-radar-run-cooldown:test",
                acquired_at=now - timedelta(minutes=5),
                heartbeat_at=now - timedelta(minutes=5),
                expires_at=expired_at,
                metadata={"operation": "manual_radar_run"},
            )
        )

    payload = radar_run_cooldown_payload(
        engine,
        AppConfig(radar_run_min_interval_seconds=300),
        now=now,
    )

    assert payload["status"] == "ready"
    assert payload["allowed"] is True
    assert payload["retry_after_seconds"] == 0
    assert payload["reset_at"] == expired_at.isoformat()
    assert "lock=inactive" in str(payload["evidence"])


def test_universe_coverage_payload_warns_on_thin_sample_universe() -> None:
    config = AppConfig(scan_batch_size=500, polygon_tickers_max_pages=1)
    health = {
        "database": {
            "active_security_count": 6,
            "active_security_with_daily_bar_count": 6,
            "latest_daily_bar_date": "2026-05-10",
        }
    }

    summary = universe_coverage_payload(config, health)

    assert summary["status"] == "thin"
    assert "6 active securities" in str(summary["headline"])
    assert "not broad US-market discovery" in str(summary["detail"])
    assert "--max-pages 1" in str(summary["next_action"])


def test_universe_coverage_payload_reports_ready_covered_universe() -> None:
    config = AppConfig(scan_batch_size=500)
    health = {
        "database": {
            "active_security_count": 500,
            "active_security_with_daily_bar_count": 500,
            "latest_daily_bar_date": "2026-05-10",
        }
    }

    summary = universe_coverage_payload(config, health)

    assert summary["status"] == "ready"
    assert "500 active securities" in str(summary["headline"])
    assert "active=500" in str(summary["evidence"])


def test_provider_preflight_payload_reports_live_provider_call_budgets() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="fixture-key",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="MarketRadar test@example.com",
        sec_daily_max_tickers=3,
    )

    rows = provider_preflight_payload(config)

    by_layer = {str(row["layer"]): row for row in rows}
    assert by_layer["Market data"]["status"] == "ready"
    assert by_layer["Market data"]["call_budget"] == (
        "1 grouped-daily request per radar run"
    )
    assert "No ticker-by-ticker price polling" in str(by_layer["Market data"]["guardrail"])
    assert "manual run cooldown=300s" in str(by_layer["Market data"]["guardrail"])
    assert "ticker reference seed cap=1 page(s)" in str(
        by_layer["Market data"]["guardrail"]
    )
    assert by_layer["News/events"]["status"] == "ready"
    assert by_layer["News/events"]["call_budget"] == (
        "up to 3 SEC submissions requests per radar run"
    )


def test_provider_preflight_payload_flags_sec_cik_target_gap() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="fixture-key",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="MarketRadar test@example.com",
        sec_daily_max_tickers=2,
    )
    radar_run_summary = {
        "steps": [
            _run_step("event_ingest", "skipped", reason="no_sec_cik_targets"),
        ]
    }

    rows = provider_preflight_payload(config, radar_run_summary=radar_run_summary)

    event = next(row for row in rows if row["layer"] == "News/events")
    assert event["status"] == "attention"
    assert "up to 2 SEC submissions requests" in str(event["call_budget"])
    assert "Seed active securities with CIKs" in str(event["next_action"])


def test_agent_review_summary_payload_surfaces_dry_run_reviewed_tickers() -> None:
    payload = agent_review_summary_payload(
        {
            "as_of": "2026-05-08",
            "steps": [
                _run_step(
                    "llm_review",
                    "success",
                    requested=1,
                    raw=0,
                    normalized=1,
                    reason="dry_run_only",
                )
                | {
                    "payload": {
                        "dry_run": True,
                        "review_task": "skeptic_review",
                        "reviewed_packet_count": 1,
                        "reviewed_tickers": ["AAA"],
                    }
                },
                _run_step(
                    "decision_cards",
                    "skipped",
                    reason="no_manual_buy_review_inputs",
                )
                | {
                    "category": "expected_gate",
                    "trigger_condition": (
                        "At least one candidate must pass policy into manual buy review."
                    ),
                },
            ],
        },
        [
            {
                "ticker": "AAA",
                "state": "Warning",
                "final_score": 88.0,
                "setup_type": "breakout",
                "decision_next_step": "Open source evidence before escalation.",
                "research_brief": {
                    "why_now": "AAA volume breakout",
                    "supporting_evidence": "AAA S-1 catalyst",
                    "risk_or_gap": "Customer concentration",
                    "next_step": "Review packet evidence.",
                },
            }
        ],
    )

    assert payload["schema_version"] == "agent-review-summary-v1"
    assert payload["status"] == "dry_run_reviewed"
    assert payload["mode"] == "dry_run"
    assert payload["review_task"] == "skeptic_review"
    assert payload["reviewed_tickers"] == ["AAA"]
    assert payload["reviewed_packet_count"] == 1
    assert payload["reviewed_candidates"] == [
        {
            "ticker": "AAA",
            "state": "Warning",
            "score": 88.0,
            "setup": "breakout",
            "why_now": "AAA volume breakout",
            "evidence": "AAA S-1 catalyst",
            "risk_or_gap": "Customer concentration",
            "next_step": "Open source evidence before escalation.",
        }
    ]
    assert payload["remaining_expected_gates"][0]["step"] == "decision_cards"
    assert "AAA" in str(payload["headline"])


def test_agent_review_summary_payload_explains_no_review_inputs() -> None:
    payload = agent_review_summary_payload(
        {
            "steps": [
                _run_step(
                    "llm_review",
                    "skipped",
                    requested=3,
                    raw=0,
                    normalized=0,
                    reason="no_llm_review_inputs",
                )
                | {
                    "category": "expected_gate",
                    "payload": {
                        "candidate_packet_count": 3,
                        "candidate_packet_state_counts": {"Blocked": 3},
                        "eligible_states": [
                            "EligibleForManualBuyReview",
                            "ThesisWeakening",
                            "Warning",
                        ],
                    },
                    "trigger_condition": (
                        "At least one Warning or manual-review candidate packet must exist."
                    ),
                },
            ],
        }
    )

    assert payload["status"] == "no_review_inputs"
    assert payload["mode"] == "not_run"
    assert payload["requested_count"] == 3
    assert payload["reviewed_packet_count"] == 0
    assert payload["candidate_packet_state_counts"] == {"Blocked": 3}
    assert "Warning" in str(payload["next_action"])


def test_radar_run_call_plan_reports_local_fixture_no_external_calls(
    tmp_path: Path,
) -> None:
    payload = radar_run_call_plan_payload(_engine(tmp_path), AppConfig.from_env({}))

    assert payload["status"] == "local_or_dry_run_only"
    assert payload["will_call_external_providers"] is False
    assert payload["max_external_call_count"] == 0
    by_layer = {str(row["layer"]): row for row in payload["rows"]}
    assert by_layer["Market data"]["status"] == "local_only"
    assert by_layer["News/events"]["status"] == "local_only"
    assert by_layer["LLM review"]["external_call_count_max"] == 0
    assert by_layer["Schwab"]["status"] == "not_called"


def test_radar_run_default_scope_uses_latest_local_bar_date(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    now = datetime(2026, 5, 14, 12, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(daily_bars),
            [
                {
                    "ticker": "AAA",
                    "date": datetime(2026, 5, 7, tzinfo=UTC).date(),
                    "provider": "csv",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "volume": 1_000_000,
                    "vwap": 10.3,
                    "adjusted": True,
                    "source_ts": now,
                    "available_at": now,
                },
                {
                    "ticker": "AAA",
                    "date": datetime(2026, 5, 8, tzinfo=UTC).date(),
                    "provider": "csv",
                    "open": 10.5,
                    "high": 12.0,
                    "low": 10.0,
                    "close": 11.7,
                    "volume": 1_500_000,
                    "vwap": 11.4,
                    "adjusted": True,
                    "source_ts": now,
                    "available_at": now,
                },
            ],
        )

    payload = radar_run_default_scope_payload(
        engine,
        AppConfig(daily_market_provider="csv"),
        now=now,
    )

    assert payload["status"] == "suggested"
    assert payload["scope"] == {"as_of": "2026-05-08"}
    assert payload["latest_daily_bar_date"] == "2026-05-08"
    assert "latest local daily bar" in str(payload["headline"])
    assert "No external calls" in str(payload["detail"])


def test_radar_run_default_scope_leaves_live_market_runs_current_date(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)

    payload = radar_run_default_scope_payload(
        engine,
        AppConfig(daily_market_provider="polygon", polygon_api_key="fixture-key"),
        now=datetime(2026, 5, 14, 12, tzinfo=UTC),
    )

    assert payload["status"] == "current_default"
    assert payload["scope"] == {}
    assert payload["market_mode"] == "live"


def test_radar_run_call_plan_caps_polygon_and_sec_calls(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_active_security_for_call_plan(engine, "MSFT", cik="789")
    _insert_active_security_for_call_plan(engine, "NVDA", cik="123")
    _insert_active_security_for_call_plan(engine, "AAPL", cik=None)
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key="fixture-key",
        daily_event_provider="sec",
        sec_enable_live=True,
        sec_user_agent="MarketRadar test@example.com",
        sec_daily_max_tickers=1,
    )

    payload = radar_run_call_plan_payload(
        engine,
        config,
        tickers=["MSFT", "NVDA", "MSFT"],
        run_llm=True,
        llm_dry_run=True,
    )

    assert payload["status"] == "live_calls_planned"
    assert payload["will_call_external_providers"] is True
    assert payload["max_external_call_count"] == 2
    assert payload["scope"]["tickers"] == ["MSFT", "NVDA"]
    by_layer = {str(row["layer"]): row for row in payload["rows"]}
    assert by_layer["Market data"]["external_call_count_max"] == 1
    assert by_layer["News/events"]["external_call_count_max"] == 1
    assert "this scope has 1 target" in str(by_layer["News/events"]["detail"])
    assert by_layer["LLM review"]["status"] == "dry_run"
    assert by_layer["Schwab"]["external_call_count_max"] == 0


def test_radar_run_call_plan_blocks_missing_live_credentials(
    tmp_path: Path,
) -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key=None,
        daily_event_provider="sec",
        sec_enable_live=False,
        sec_user_agent=None,
    )

    payload = radar_run_call_plan_payload(_engine(tmp_path), config)

    assert payload["status"] == "blocked"
    assert payload["max_external_call_count"] == 0
    by_layer = {str(row["layer"]): row for row in payload["rows"]}
    assert by_layer["Market data"]["status"] == "blocked"
    assert "CATALYST_POLYGON_API_KEY" in str(by_layer["Market data"]["detail"])
    assert by_layer["News/events"]["status"] == "blocked"
    assert "CATALYST_SEC_ENABLE_LIVE=1" in str(by_layer["News/events"]["detail"])


def test_provider_preflight_blocks_openai_when_key_missing() -> None:
    config = AppConfig(
        enable_premium_llm=True,
        llm_provider="openai",
        openai_api_key=None,
    )

    coverage = data_source_coverage_payload(config)
    preflight = provider_preflight_payload(config)
    readiness = readiness_checklist_payload(
        config,
        radar_run_summary={
            "steps": [_run_step("llm_review", "skipped", reason="llm_disabled")]
        },
    )

    llm_coverage = next(row for row in coverage if row["layer"] == "LLM review")
    llm_preflight = next(row for row in preflight if row["layer"] == "LLM review")
    llm_readiness = next(row for row in readiness if row["area"] == "LLM review")
    assert llm_coverage["mode"] == "missing_credentials"
    assert llm_preflight["status"] == "blocked"
    assert "OPENAI_API_KEY" in str(llm_preflight["call_budget"])
    assert llm_readiness["status"] == "blocked"


def test_provider_preflight_blocks_openai_when_pricing_or_budget_missing() -> None:
    config = AppConfig(
        enable_premium_llm=True,
        llm_provider="openai",
        openai_api_key="sk-test",
        llm_skeptic_model="skeptic-model",
        llm_daily_budget_usd=0.0,
        llm_monthly_budget_usd=0.0,
    )

    preflight = provider_preflight_payload(config)
    readiness = readiness_checklist_payload(
        config,
        radar_run_summary={
            "steps": [_run_step("llm_review", "skipped", reason="llm_disabled")]
        },
    )

    llm_preflight = next(row for row in preflight if row["layer"] == "LLM review")
    llm_readiness = next(row for row in readiness if row["area"] == "LLM review")
    assert llm_preflight["status"] == "blocked"
    assert "CATALYST_LLM_INPUT_COST_PER_1M" in str(llm_preflight["call_budget"])
    assert "CATALYST_LLM_DAILY_BUDGET_USD" in str(llm_preflight["call_budget"])
    assert llm_readiness["status"] == "blocked"


def test_readiness_checklist_blocks_polygon_without_api_key() -> None:
    config = AppConfig(
        daily_market_provider="polygon",
        polygon_api_key=None,
        daily_event_provider="news_fixture",
    )

    rows = readiness_checklist_payload(config, radar_run_summary={"steps": []})
    coverage = data_source_coverage_payload(config)

    market = next(row for row in rows if row["area"] == "Live market scan")
    coverage_market = next(row for row in coverage if row["layer"] == "Market data")
    assert coverage_market["mode"] == "missing_credentials"
    assert market["status"] == "blocked"
    assert "API key is missing" in str(market["finding"])
    assert "grouped daily" in str(market["evidence"])


def test_readiness_checklist_blocks_sec_without_live_settings() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="sec",
        sec_enable_live=False,
        sec_user_agent=None,
        sec_daily_max_tickers=3,
    )

    rows = readiness_checklist_payload(config, radar_run_summary={"steps": []})
    coverage = data_source_coverage_payload(config)

    event = next(row for row in rows if row["area"] == "Catalyst feed")
    coverage_event = next(row for row in coverage if row["layer"] == "News/events")
    assert coverage_event["mode"] == "missing_credentials"
    assert "max_tickers=3" in str(coverage_event["guardrail"])
    assert event["status"] == "blocked"
    assert "SEC catalyst ingestion" in str(event["finding"])


def test_readiness_checklist_payload_separates_blockers_from_expected_gates() -> None:
    config = AppConfig(
        daily_market_provider="csv",
        daily_event_provider="news_fixture",
        enable_premium_llm=False,
        llm_provider="none",
        schwab_order_submission_enabled=False,
        schwab_sync_min_interval_seconds=900,
    )
    run_summary = {
        "steps": [
            _run_step("daily_bar_ingest", "success", requested=43, raw=43, normalized=43),
            _run_step("event_ingest", "success", requested=1, raw=1, normalized=1),
            _run_step("local_text_triage", "success", requested=1, raw=1, normalized=1),
            _run_step("feature_scan", "success", requested=6, raw=3, normalized=3),
            _run_step("scoring_policy", "success", requested=3, raw=3, normalized=3),
            _run_step("candidate_packets", "success", requested=2, raw=2, normalized=2),
            _run_step(
                "decision_cards",
                "skipped",
                reason="no_manual_buy_review_inputs",
            ),
            _run_step("llm_review", "skipped", reason="llm_disabled"),
            _run_step("digest", "skipped", reason="no_alerts"),
            _run_step(
                "validation_update",
                "skipped",
                reason="outcome_available_at_not_supplied",
            ),
        ]
    }

    rows = readiness_checklist_payload(
        config,
        radar_run_summary=run_summary,
        broker_summary={
            "snapshot": {
                "connection_status": "connected",
                "account_count": 1,
                "position_count": 0,
            },
            "exposure": {"broker_data_stale": True},
            "rate_limit_config": {"portfolio_sync_min_interval_seconds": 900},
        },
    )

    by_area = {str(row["area"]): row for row in rows}
    assert by_area["Live market scan"]["status"] == "blocked"
    assert "fixture" in str(by_area["Live market scan"]["finding"])
    assert by_area["Catalyst feed"]["status"] == "blocked"
    assert "fixture" in str(by_area["Catalyst feed"]["finding"])
    assert by_area["Research loop"]["status"] == "ready"
    assert by_area["Decision Cards"]["status"] == "optional"
    assert "manual buy-review" in str(by_area["Decision Cards"]["finding"])
    assert "outcome=Expected gate" in str(by_area["Decision Cards"]["evidence"])
    assert "decision_cards: skipped" not in str(by_area["Decision Cards"]["evidence"])
    assert by_area["LLM review"]["status"] == "optional"
    assert by_area["Portfolio context"]["status"] == "attention"
    assert by_area["Alerting"]["status"] == "optional"
    assert "digest: skipped" not in str(by_area["Alerting"]["evidence"])
    assert by_area["Outcome validation"]["status"] == "optional"
    assert "validation_update: skipped" not in str(
        by_area["Outcome validation"]["evidence"]
    )
    assert by_area["Order safety"]["status"] == "safe"
    joined = " ".join(str(value) for row in rows for value in row.values())
    assert "CLIENT_SECRET" not in joined


def test_load_candidate_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-msft-future",
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )
        conn.execute(
            insert(candidate_packets).values(
                id="packet-msft-future",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_state_id="state-msft-latest",
                state=ActionState.WARNING.value,
                final_score=88.0,
                schema_version="candidate-packet-v1",
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"supporting_evidence": [{"title": "Future packet"}]},
                created_at=FUTURE_AT,
            )
        )

    rows = load_candidate_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    msft = next(row for row in rows if row["ticker"] == "MSFT")
    assert msft["id"] == "state-msft-latest"
    assert msft["candidate_packet_id"] == "packet-msft-latest"


def test_load_ticker_detail_returns_candidate_packet_card_events_and_validation(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    detail = load_ticker_detail(engine, "msft")

    assert detail is not None
    assert detail["ticker"] == "MSFT"
    assert detail["manual_review_only"] is True
    assert detail["latest_candidate"]["id"] == "state-msft-latest"
    assert detail["latest_candidate"]["candidate_packet_id"] == "packet-msft-latest"
    assert detail["latest_candidate"]["decision_card_id"] == "card-msft-latest"
    assert [row["id"] for row in detail["state_history"]] == [
        "state-msft-latest",
        "state-msft-earlier",
    ]
    assert detail["features"]["feature_version"] == "score-v4-options-theme"
    assert detail["events"][0]["id"] == "event-msft"
    assert detail["snippets"][0]["id"] == "snippet-msft"
    assert "event-msft-future" not in {row["id"] for row in detail["events"]}
    assert "snippet-msft-future" not in {row["id"] for row in detail["snippets"]}
    assert detail["candidate_packet"]["payload"]["supporting_evidence"][0]["title"] == (
        "MSFT guidance raised"
    )
    assert detail["decision_card"]["payload"]["disclaimer"] == "Manual review only."
    assert detail["setup_plan"]["setup_type"] == "breakout"
    assert detail["portfolio_impact"]["proposed_notional"] == 2080.0
    assert detail["validation_results"][0]["id"] == "validation-result-msft"
    assert "validation-result-msft-future" not in {
        row["id"] for row in detail["validation_results"]
    }
    assert detail["paper_trades"][0]["id"] == "paper-msft"
    assert "paper-msft-future" not in {row["id"] for row in detail["paper_trades"]}


def test_load_ticker_detail_respects_candidate_state_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-msft-future",
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )

    detail = load_ticker_detail(
        engine,
        "MSFT",
        available_at=AVAILABLE_AT + timedelta(minutes=1),
    )

    assert detail is not None
    assert detail["latest_candidate"]["id"] == "state-msft-latest"


def test_load_theme_rows_groups_candidate_themes(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    rows = load_theme_rows(engine)

    ai_row = next(row for row in rows if row["theme"] == "ai_infrastructure")
    assert ai_row["candidate_count"] == 2
    assert ai_row["avg_score"] == 82.0
    assert ai_row["top_tickers"] == ["MSFT", "AAPL"]
    assert ai_row["states"] == {"Warning": 2}
    assert ai_row["latest_as_of"] == AS_OF


def test_load_theme_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                _candidate_state_row(
                    id="state-aapl-future",
                    ticker="AAPL",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    created_at=AVAILABLE_AT,
                )
            )
        )
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    ticker="AAPL",
                    as_of=FUTURE_AT,
                    state=ActionState.WARNING.value,
                    final_score=99.0,
                    theme="future_theme",
                )
            )
        )

    rows = load_theme_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    assert {row["theme"] for row in rows} == {"ai_infrastructure"}


def test_load_validation_summary_returns_latest_run_report_and_paper_trades(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-old-same-ticker-validation",
                artifact_type="decision_card",
                artifact_id="old-card-msft-not-in-latest-run",
                ticker="MSFT",
                label="useful",
                notes="same ticker but unrelated to latest validation run",
                created_at=AVAILABLE_AT - timedelta(hours=1),
            )
        )

    summary = load_validation_summary(engine)

    assert summary["latest_run"]["id"] == "validation-run-latest"
    assert summary["report"]["candidate_count"] == 1
    assert summary["report"]["precision"]["target_20d_25"] == 1.0
    assert summary["report"]["useful_alert_rate"] == 1.0
    assert summary["report"]["leakage_failure_count"] == 0
    assert [row["id"] for row in summary["paper_trades"]] == ["paper-msft"]
    assert [row["id"] for row in summary["useful_labels"]] == ["label-useful-msft"]


def test_load_alert_rows_returns_latest_alerts_with_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)

    rows = load_alert_rows(engine, available_at=AVAILABLE_AT + timedelta(minutes=30))

    assert [row["id"] for row in rows] == ["alert-msft-dry-run", "alert-msft-planned"]
    assert rows[0]["ticker"] == "MSFT"
    assert rows[0]["route"] == "warning_digest"
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["feedback_label"] == "acted"
    assert rows[0]["feedback_notes"] == "latest feedback"
    assert rows[0]["score_trigger"] == 88.0


def test_load_alert_detail_returns_payload_and_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)

    detail = load_alert_detail(engine, "alert-msft-planned", available_at=AVAILABLE_AT)

    assert detail is not None
    assert detail["id"] == "alert-msft-planned"
    assert detail["payload"]["evidence"][0]["artifact_id"] == "event-msft"
    assert detail["feedback_label"] == "useful"
    assert detail["feedback_notes"] == "worth review"
    assert detail["feedback_id"] == "feedback-alert-msft-planned"


def test_load_alert_rows_respects_available_at_cutoff(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    future_available_at = datetime.now(UTC).replace(microsecond=0) + timedelta(days=1)
    _insert_alert_fixture(engine, available_at=future_available_at)

    default_rows = load_alert_rows(engine)
    rows = load_alert_rows(
        engine,
        available_at=future_available_at + timedelta(minutes=10),
        ticker="msft",
        status="planned",
        route="immediate_manual_review",
    )

    assert default_rows == []
    assert [row["id"] for row in rows] == ["alert-msft-planned"]
    assert rows[0]["feedback_label"] == "useful"
    assert (
        load_alert_detail(engine, "alert-msft-dry-run", available_at=future_available_at)
        is None
    )


def test_load_ipo_s1_rows_returns_visible_analysis_and_filters_future_rows(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    visible_at = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
    future_at = datetime.now(UTC).replace(microsecond=0) + timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(
            insert(events),
            [
                _ipo_event_row(
                    id="event-acme-s1",
                    ticker="ACME",
                    available_at=visible_at,
                    source_ts=visible_at,
                ),
                _ipo_event_row(
                    id="event-futr-s1",
                    ticker="FUTR",
                    available_at=future_at,
                    source_ts=future_at,
                ),
            ],
        )

    rows = load_ipo_s1_rows(engine)
    filtered = load_ipo_s1_rows(engine, ticker="acme")
    future_visible = load_ipo_s1_rows(engine, available_at=future_at + timedelta(minutes=1))

    assert [row["id"] for row in rows] == ["event-acme-s1"]
    assert [row["ticker"] for row in filtered] == ["ACME"]
    assert rows[0]["form_type"] == "S-1"
    assert rows[0]["proposed_ticker"] == "ACME"
    assert rows[0]["shares_offered"] == 12_500_000
    assert rows[0]["price_range_low"] == 17.0
    assert rows[0]["estimated_gross_proceeds"] == 225_000_000.0
    assert rows[0]["risk_flags"] == ["history_of_losses", "emerging_growth_company"]
    assert [row["id"] for row in future_visible] == ["event-futr-s1", "event-acme-s1"]


def test_load_cost_summary_defaults_to_zero_and_counts_useful_alerts(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-old-same-ticker",
                artifact_type="decision_card",
                artifact_id="old-card-msft-not-in-latest-run",
                ticker="MSFT",
                label="useful",
                notes="same ticker but unrelated to latest validation run",
                created_at=AVAILABLE_AT - timedelta(hours=1),
            )
        )

    summary = load_cost_summary(engine)

    assert summary["source"] == "budget_ledger"
    assert summary["total_actual_cost_usd"] == 0.0
    assert summary["total_estimated_cost_usd"] == 0.0
    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["useful_alert_count"] == 1
    assert summary["cost_per_useful_alert"] == 0.0
    assert summary["attempt_count"] == 0
    assert summary["status_counts"] == {}
    assert summary["rows"] == []


def test_load_cost_summary_counts_useful_alert_feedback(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_alert_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(useful_alert_labels),
            [
                {
                    "id": "label-alert-msft-useful",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-planned",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "alert mapped to validation result by candidate ids",
                    "created_at": AVAILABLE_AT,
                },
                {
                    "id": "label-alert-aapl-useful",
                    "artifact_type": "alert",
                    "artifact_id": "alert-aapl-unmatched",
                    "ticker": "AAPL",
                    "label": "useful",
                    "notes": "not part of latest validation result rows",
                    "created_at": AVAILABLE_AT,
                },
            ],
        )

    summary = load_cost_summary(engine)

    assert summary["useful_alert_count"] == 2
    assert {row["id"] for row in summary["useful_labels"]} == {
        "label-useful-msft",
        "label-alert-msft-useful",
    }


def test_load_cost_summary_uses_budget_ledger_rows(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    _insert_budget_ledger_fixture(engine, available_at=AVAILABLE_AT)

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT + timedelta(minutes=1))

    assert summary["source"] == "budget_ledger"
    assert summary["currency"] == "USD"
    assert summary["total_actual_cost_usd"] == 0.19
    assert summary["total_estimated_cost_usd"] == 0.22
    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["attempt_count"] == 2
    assert summary["status_counts"] == {"completed": 1, "skipped": 1}
    assert summary["useful_alert_count"] == 1
    assert summary["cost_per_useful_alert"] == 0.19
    assert summary["by_task"][0]["task"] == "mid_review"
    assert summary["by_model"][0]["model"] == "model-review"
    assert [row["status"] for row in summary["rows"]] == ["skipped", "completed"]
    assert summary["caps"] == {
        "premium_llm_enabled": False,
        "daily_budget_usd": 0.0,
        "monthly_budget_usd": 0.0,
        "task_daily_caps": {},
    }


def test_load_cost_summary_keeps_validation_cost_separate(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            update(validation_runs)
            .where(validation_runs.c.id == "validation-run-latest")
            .values(metrics={"total_cost_usd": 7.25})
        )

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT)

    assert summary["total_actual_cost_usd"] == 0.0
    assert summary["total_estimated_cost_usd"] == 0.0
    assert summary["validation_total_cost_usd"] == 7.25
    assert summary["cost_per_useful_alert"] == 0.0


def test_load_cost_summary_validation_context_respects_requested_cutoff(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    with engine.begin() as conn:
        conn.execute(
            update(validation_runs)
            .where(validation_runs.c.id == "validation-run-latest")
            .values(metrics={"total_cost_usd": 7.25})
        )

    summary = load_cost_summary(engine, available_at=AVAILABLE_AT - timedelta(minutes=1))

    assert summary["validation_total_cost_usd"] == 0.0
    assert summary["useful_alert_count"] == 0
    assert summary["useful_labels"] == []


def test_load_cost_summary_hides_future_ledger_rows_by_default(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    visible_at = datetime.now(UTC) - timedelta(hours=1)
    future_at = datetime.now(UTC) + timedelta(days=1)
    _insert_budget_ledger_fixture(engine, available_at=visible_at, future_at=future_at)

    summary = load_cost_summary(engine)

    assert summary["attempt_count"] == 1
    assert summary["total_actual_cost_usd"] == 0.19
    assert summary["status_counts"] == {"completed": 1}
    assert [row["id"] for row in summary["rows"]] == [
        budget_ledger_id(
            task=LLMTaskName.MID_REVIEW.value,
            ticker="msft",
            candidate_packet_id="candidate-packet-MSFT",
            status=LLMCallStatus.COMPLETED.value,
            available_at=visible_at,
            prompt_version="evidence_review_v1",
        )
    ]


def test_load_ops_health_reports_provider_status_and_database(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)

    health = load_ops_health(engine, now=AVAILABLE_AT)

    assert health["database"]["status"] == "ok"
    assert health["database"]["candidate_state_count"] == 3
    assert [row["provider"] for row in health["providers"]] == ["news", "polygon"]
    assert health["providers"][0]["status"] == "stale"
    assert health["stale_data"]["detected"] is True
    assert health["stale_data"]["providers"] == ["news"]
    assert health["jobs"][0]["id"] == "job-ingest"
    assert "provider_banners" in health
    assert "degraded_mode" in health
    assert "metrics" in health
    assert "score_drift" in health


def test_load_radar_run_summary_returns_latest_daily_step_group(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    old_decision_at = AVAILABLE_AT - timedelta(days=1)
    latest_decision_at = AVAILABLE_AT
    old_metadata = {
        "as_of": "2026-05-09",
        "decision_available_at": old_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "old-universe",
        "tickers": ["AAPL"],
    }
    latest_metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": ["MSFT", "NVDA"],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "old-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=old_decision_at,
                    metadata=old_metadata,
                ),
                _job_run_row(
                    "latest-scoring",
                    job_type="scoring_policy",
                    status="failed",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata=latest_metadata,
                    error_summary="policy input missing",
                ),
                _job_run_row(
                    "latest-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata=latest_metadata,
                    requested_count=4,
                    raw_count=3,
                    normalized_count=3,
                ),
                _job_run_row(
                    "provider-job",
                    job_type="provider_ingest",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=3),
                    metadata={"ignored": True},
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "partial_success"
    assert summary["as_of"] == "2026-05-10"
    assert summary["decision_available_at"] == latest_decision_at.isoformat()
    assert summary["provider"] == "csv"
    assert summary["universe"] == "liquid-us"
    assert summary["tickers"] == ["MSFT", "NVDA"]
    assert summary["step_count"] == 2
    assert summary["status_counts"] == {"failed": 1, "success": 1}
    assert summary["requested_count"] == 4
    assert [row["step"] for row in summary["steps"]] == ["feature_scan", "scoring_policy"]
    assert summary["steps"][1]["error_summary"] == "policy input missing"


def test_load_radar_run_summary_marks_limited_analysis_skips_partial(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    latest_decision_at = AVAILABLE_AT
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata={
                        **metadata,
                        "result_status": "success",
                        "result_reason": None,
                        "result_payload": {"scan_result_count": 2},
                    },
                    requested_count=3,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "degraded_mode_blocks_high_state_work",
                        "result_payload": {"degraded_mode": {"enabled": True}},
                    },
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "partial_success"
    assert summary["status_counts"] == {"skipped": 1, "success": 1}
    assert summary["outcome_category_counts"] == {
        "blocked_input": 1,
        "completed": 1,
    }
    assert summary["blocking_step_count"] == 1
    assert summary["expected_gate_count"] == 0
    assert summary["steps"][1]["reason"] == "degraded_mode_blocks_high_state_work"
    assert summary["steps"][1]["category"] == "blocked_input"
    assert summary["steps"][1]["blocks_reliance"] is True
    assert summary["steps"][1]["payload"] == {"degraded_mode": {"enabled": True}}


def test_load_radar_run_summary_refreshes_stale_blocking_metadata(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    latest_decision_at = AVAILABLE_AT
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
        "result_status": "skipped",
        "result_reason": "degraded_mode_blocks_llm_review",
        "result_payload": {"degraded_mode": {"enabled": True}},
        "outcome_category": "expected_gate",
        "outcome_label": "Expected gate",
        "outcome_meaning": "Old non-blocking explanation.",
        "operator_action": "Old non-blocking action.",
        "blocks_reliance": False,
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata={
                        **metadata,
                        "result_status": "success",
                        "result_reason": None,
                        "result_payload": {"scan_result_count": 2},
                    },
                    requested_count=3,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "llm-review",
                    job_type="llm_review",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata=metadata,
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "partial_success"
    assert summary["outcome_category_counts"] == {
        "blocked_input": 1,
        "completed": 1,
    }
    assert summary["blocking_step_count"] == 1
    assert summary["expected_gate_count"] == 0
    assert summary["steps"][1]["reason"] == "degraded_mode_blocks_llm_review"
    assert summary["steps"][1]["category"] == "blocked_input"
    assert summary["steps"][1]["label"] == "Blocked input"
    assert summary["steps"][1]["blocks_reliance"] is True
    assert summary["steps"][1]["meaning"] == (
        "Degraded mode blocked LLM review because current data is not trusted."
    )


def test_load_radar_run_summary_classifies_expected_gate_skips_success(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    latest_decision_at = AVAILABLE_AT
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata={
                        **metadata,
                        "result_status": "success",
                        "result_reason": None,
                        "result_payload": {"candidate_packet_count": 2},
                    },
                    requested_count=2,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "decision-cards",
                    job_type="decision_cards",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "no_manual_buy_review_inputs",
                        "result_payload": {},
                    },
                ),
                _job_run_row(
                    "llm-review",
                    job_type="llm_review",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=3),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "llm_disabled",
                        "result_payload": {},
                    },
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status"] == "success"
    assert summary["status_counts"] == {"skipped": 2, "success": 1}
    assert summary["outcome_category_counts"] == {
        "completed": 1,
        "expected_gate": 2,
    }
    assert summary["blocking_step_count"] == 0
    assert summary["expected_gate_count"] == 2
    assert summary["required_step_count"] == 1
    assert summary["required_completed_count"] == 1
    assert summary["required_incomplete_count"] == 0
    assert summary["optional_expected_gate_count"] == 2
    assert summary["optional_expected_gate_count"] == summary["status_counts"]["skipped"]
    assert summary["action_needed_count"] == 0
    assert summary["run_path_status"] == "complete"
    assert [row["category"] for row in summary["steps"]] == [
        "completed",
        "expected_gate",
        "expected_gate",
    ]
    assert summary["steps"][1]["trigger_condition"] == (
        "At least one candidate must pass policy into manual buy review."
    )
    assert summary["steps"][2]["trigger_condition"] == (
        "Request LLM dry-run review after candidate packets exist."
    )


def test_load_radar_run_summary_refreshes_stale_skip_explanations(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
        "result_status": "skipped",
        "result_reason": "no_alerts",
        "result_payload": {},
        "outcome_category": "expected_gate",
        "outcome_label": "Expected gate",
        "outcome_meaning": "No alert candidates were generated.",
        "operator_action": "Old action copy.",
        "blocks_reliance": False,
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "digest",
                    job_type="digest",
                    status="skipped",
                    started_at=AVAILABLE_AT + timedelta(seconds=1),
                    metadata=metadata,
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["steps"][0]["reason"] == "no_alerts"
    assert summary["steps"][0]["meaning"] == (
        "No existing alerts were available for the digest step."
    )
    assert summary["steps"][0]["operator_action"] == (
        "No action required unless you want this optional gate to run."
    )
    assert summary["steps"][0]["trigger_condition"] == (
        "Alert planning must produce at least one digest alert."
    )


def test_load_radar_run_summary_keeps_not_ready_skips_out_of_expected_gates(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    latest_decision_at = AVAILABLE_AT
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": None,
        "universe": None,
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "event-ingest",
                    job_type="event_ingest",
                    status="success",
                    started_at=latest_decision_at + timedelta(seconds=1),
                    metadata={
                        **metadata,
                        "result_status": "success",
                        "result_reason": None,
                        "result_payload": {"event_count": 0},
                    },
                ),
                _job_run_row(
                    "local-text-triage",
                    job_type="local_text_triage",
                    status="skipped",
                    started_at=latest_decision_at + timedelta(seconds=2),
                    metadata={
                        **metadata,
                        "result_status": "skipped",
                        "result_reason": "no_text_inputs",
                        "result_payload": {},
                    },
                ),
            ],
        )

    summary = load_radar_run_summary(engine)

    assert summary["status_counts"] == {"skipped": 1, "success": 1}
    assert summary["outcome_category_counts"] == {
        "completed": 1,
        "not_ready": 1,
    }
    assert summary["expected_gate_count"] == 0
    assert summary["optional_expected_gate_count"] == 0
    assert summary["required_step_count"] == 2
    assert summary["required_completed_count"] == 1
    assert summary["required_incomplete_count"] == 1
    assert summary["action_needed_count"] == 0
    assert summary["run_path_status"] == "incomplete"
    assert summary["steps"][1]["category"] == "not_ready"
    assert summary["steps"][1]["blocks_reliance"] is False


def test_radar_discovery_snapshot_labels_fixture_thin_run(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            update(candidate_packets)
            .where(candidate_packets.c.id == "packet-msft-latest")
            .values(
                available_at=SOURCE_TS,
                created_at=AVAILABLE_AT + timedelta(seconds=3),
            )
        )
        conn.execute(
            update(decision_cards)
            .where(decision_cards.c.id == "card-msft-latest")
            .values(available_at=AVAILABLE_AT + timedelta(seconds=5))
        )
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "daily-bars",
                    job_type="daily_bar_ingest",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=1),
                    metadata=metadata,
                    requested_count=6,
                    raw_count=6,
                    normalized_count=6,
                ),
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=2),
                    metadata=metadata,
                    requested_count=6,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=3),
                    metadata=metadata,
                    requested_count=2,
                    raw_count=1,
                    normalized_count=1,
                ),
                _job_run_row(
                    "decision-cards",
                    job_type="decision_cards",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=4),
                    metadata=metadata,
                    requested_count=1,
                    raw_count=1,
                    normalized_count=1,
                ),
            ],
        )
    summary = load_radar_run_summary(engine)

    snapshot = radar_discovery_snapshot_payload(
        engine,
        AppConfig(
            daily_market_provider="polygon",
            polygon_api_key="fixture-key",
            daily_event_provider="news_fixture",
            scan_batch_size=500,
        ),
        radar_run_summary=summary,
        ops_health={
            "database": {
                "active_security_count": 6,
                "active_security_with_daily_bar_count": 6,
                "latest_daily_bar_date": "2026-05-10",
            }
        },
    )

    assert snapshot["status"] == "fixture"
    assert snapshot["source_modes"] == {
        "market": "fixture",
        "market_provider": "csv",
        "events": "fixture",
        "event_provider": "news_fixture",
    }
    assert snapshot["yield"] == {
        "requested_securities": 6,
        "scanned_securities": 2,
        "candidate_states": 2,
        "candidate_packets": 1,
        "decision_cards": 1,
    }
    blocker_codes = {str(row["code"]) for row in snapshot["blockers"]}
    assert {"fixture_market_data", "fixture_events", "thin_universe"} <= blocker_codes
    assert snapshot["freshness"]["latest_bars_older_than_as_of"] is False
    assert snapshot["top_discoveries"][0]["ticker"] == "MSFT"
    assert snapshot["top_discoveries"][0]["packet"] == "packet-msft-latest"
    assert snapshot["top_discoveries"][0]["card"] == "card-msft-latest"


def test_radar_discovery_snapshot_counts_run_candidates_when_cutoff_precedes_as_of(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    early_decision_at = AS_OF - timedelta(hours=2)
    metadata = {
        "as_of": AS_OF.date().isoformat(),
        "decision_available_at": early_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            update(candidate_states)
            .where(candidate_states.c.id.in_(["state-msft-latest", "state-aapl-latest"]))
            .values(created_at=early_decision_at)
        )
        conn.execute(
            update(candidate_packets)
            .where(candidate_packets.c.id == "packet-msft-latest")
            .values(
                available_at=early_decision_at,
                created_at=early_decision_at + timedelta(seconds=3),
            )
        )
        conn.execute(
            update(decision_cards)
            .where(decision_cards.c.id == "card-msft-latest")
            .values(
                available_at=early_decision_at,
                created_at=early_decision_at + timedelta(seconds=4),
            )
        )
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "same-day-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=early_decision_at + timedelta(seconds=1),
                    metadata=metadata,
                    requested_count=6,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "same-day-scoring",
                    job_type="scoring_policy",
                    status="success",
                    started_at=early_decision_at + timedelta(seconds=2),
                    metadata=metadata,
                    requested_count=2,
                    raw_count=2,
                    normalized_count=2,
                ),
                _job_run_row(
                    "same-day-candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=early_decision_at + timedelta(seconds=3),
                    metadata=metadata,
                    requested_count=2,
                    raw_count=1,
                    normalized_count=1,
                ),
            ],
        )
    summary = load_radar_run_summary(engine)

    snapshot = radar_discovery_snapshot_payload(
        engine,
        AppConfig(
            daily_market_provider="csv",
            daily_event_provider="news_fixture",
            scan_batch_size=500,
        ),
        radar_run_summary=summary,
        ops_health={
            "database": {
                "active_security_count": 6,
                "active_security_with_daily_bar_count": 6,
                "latest_daily_bar_date": AS_OF.date().isoformat(),
            }
        },
    )

    assert snapshot["yield"]["candidate_states"] == 2
    assert snapshot["yield"]["candidate_packets"] == 1
    assert snapshot["latest_candidate_context"]["stale_relative_to_run"] is False
    assert snapshot["latest_candidate_context"]["latest_candidate_as_of"] == AS_OF.isoformat()
    assert (
        snapshot["latest_candidate_context"]["latest_candidate_session_date"]
        == AS_OF.date().isoformat()
    )
    assert snapshot["freshness"]["latest_candidate_session_date"] == AS_OF.date().isoformat()
    assert snapshot["top_discoveries"][0]["ticker"] == "MSFT"


def test_radar_discovery_snapshot_flags_stale_bars_and_empty_packets(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "outcome_available_at": None,
        "provider": "polygon",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "daily-bars",
                    job_type="daily_bar_ingest",
                    status="success",
                    started_at=AVAILABLE_AT,
                    metadata=metadata,
                    requested_count=500,
                    raw_count=500,
                    normalized_count=500,
                ),
                _job_run_row(
                    "feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=1),
                    metadata=metadata,
                    requested_count=500,
                    raw_count=0,
                    normalized_count=0,
                ),
                _job_run_row(
                    "candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=AVAILABLE_AT + timedelta(seconds=2),
                    metadata=metadata,
                    requested_count=0,
                    raw_count=0,
                    normalized_count=0,
                ),
            ],
        )
    summary = load_radar_run_summary(engine)

    snapshot = radar_discovery_snapshot_payload(
        engine,
        AppConfig(
            daily_market_provider="polygon",
            polygon_api_key="fixture-key",
            daily_event_provider="sec",
            sec_enable_live=True,
            sec_user_agent="MarketRadar test@example.com",
            scan_batch_size=500,
        ),
        radar_run_summary=summary,
        ops_health={
            "database": {
                "active_security_count": 500,
                "active_security_with_daily_bar_count": 500,
                "latest_daily_bar_date": "2026-05-08",
            }
        },
    )

    assert snapshot["status"] == "attention"
    assert snapshot["source_modes"]["market"] == "live"
    assert snapshot["source_modes"]["events"] == "live"
    assert snapshot["yield"]["candidate_packets"] == 0
    assert snapshot["top_discoveries"] == []
    assert snapshot["freshness"]["latest_bars_older_than_as_of"] is True
    blocker_codes = {str(row["code"]) for row in snapshot["blockers"]}
    assert blocker_codes == {"stale_daily_bars", "no_candidate_packets"}


def test_radar_discovery_snapshot_exposes_stale_candidate_context(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    stale_run_cutoff = AVAILABLE_AT + timedelta(days=4)
    metadata = {
        "as_of": stale_run_cutoff.date().isoformat(),
        "decision_available_at": stale_run_cutoff.isoformat(),
        "outcome_available_at": None,
        "provider": "csv",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "stale-daily-bars",
                    job_type="daily_bar_ingest",
                    status="success",
                    started_at=stale_run_cutoff,
                    metadata=metadata,
                    requested_count=43,
                    raw_count=43,
                    normalized_count=43,
                ),
                _job_run_row(
                    "stale-feature-scan",
                    job_type="feature_scan",
                    status="success",
                    started_at=stale_run_cutoff + timedelta(seconds=1),
                    metadata=metadata,
                    requested_count=6,
                    raw_count=3,
                    normalized_count=3,
                ),
                _job_run_row(
                    "stale-candidate-packets",
                    job_type="candidate_packets",
                    status="success",
                    started_at=stale_run_cutoff + timedelta(seconds=2),
                    metadata=metadata,
                    requested_count=3,
                    raw_count=3,
                    normalized_count=3,
                ),
            ],
        )
    summary = load_radar_run_summary(engine)

    snapshot = radar_discovery_snapshot_payload(
        engine,
        AppConfig(
            daily_market_provider="csv",
            daily_event_provider="news_fixture",
            scan_batch_size=500,
        ),
        radar_run_summary=summary,
        ops_health={
            "database": {
                "active_security_count": 6,
                "active_security_with_daily_bar_count": 6,
                "latest_daily_bar_date": AS_OF.date().isoformat(),
            }
        },
    )

    assert snapshot["yield"]["candidate_states"] == 0
    assert snapshot["freshness"]["latest_candidate_as_of"] == AS_OF.isoformat()
    assert snapshot["freshness"]["latest_candidate_session_date"] == AS_OF.date().isoformat()
    assert snapshot["freshness"]["latest_candidate_age_days"] == 4
    context = snapshot["latest_candidate_context"]
    assert context["candidate_states"] == 2
    assert context["latest_candidate_as_of"] == AS_OF.isoformat()
    assert context["latest_candidate_session_date"] == AS_OF.date().isoformat()
    assert context["latest_candidate_age_days"] == 4
    assert context["stale_relative_to_run"] is True
    assert context["top_candidates"][0]["ticker"] == "MSFT"


def test_radar_discovery_snapshot_ignores_old_packets_without_latest_packet_step(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_dashboard_fixture(engine)
    latest_decision_at = AVAILABLE_AT + timedelta(hours=1)
    metadata = {
        "as_of": "2026-05-10",
        "decision_available_at": latest_decision_at.isoformat(),
        "outcome_available_at": None,
        "provider": "polygon",
        "universe": "liquid-us",
        "tickers": [],
    }
    with engine.begin() as conn:
        conn.execute(
            insert(job_runs),
            [
                _job_run_row(
                    "latest-feature-scan-only",
                    job_type="feature_scan",
                    status="success",
                    started_at=latest_decision_at,
                    metadata=metadata,
                    requested_count=500,
                    raw_count=2,
                    normalized_count=2,
                ),
            ],
        )
    summary = load_radar_run_summary(engine)

    snapshot = radar_discovery_snapshot_payload(
        engine,
        AppConfig(
            daily_market_provider="polygon",
            polygon_api_key="fixture-key",
            daily_event_provider="sec",
            sec_enable_live=True,
            sec_user_agent="MarketRadar test@example.com",
            scan_batch_size=500,
        ),
        radar_run_summary=summary,
        ops_health={
            "database": {
                "active_security_count": 500,
                "active_security_with_daily_bar_count": 500,
                "latest_daily_bar_date": "2026-05-10",
            }
        },
    )

    assert snapshot["yield"]["candidate_states"] == 2
    assert snapshot["yield"]["candidate_packets"] == 0
    assert snapshot["top_discoveries"] == []
    assert {str(row["code"]) for row in snapshot["blockers"]} == {
        "no_candidate_packets"
    }


def test_load_broker_summary_returns_portfolio_context(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    repo = BrokerRepository(engine)
    now = datetime.now(UTC).replace(microsecond=0)
    connection_id = broker_connection_id()
    account_id = broker_account_id("schwab", "account-hash-123")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            last_successful_sync_at=now,
            metadata={"mode": "read_only"},
        )
    )
    repo.upsert_accounts(
        [
            BrokerAccount(
                id=account_id,
                connection_id=connection_id,
                broker="schwab",
                broker_account_id="12345678",
                account_hash="account-hash-123",
                created_at=now,
                updated_at=now,
                display_name="MARGIN ending 5678",
            )
        ]
    )
    repo.upsert_balance_snapshots(
        [
            BrokerBalanceSnapshot(
                id=broker_balance_snapshot_id(account_id, now),
                account_id=account_id,
                as_of=now,
                cash=50000.0,
                buying_power=100000.0,
                liquidation_value=250000.0,
                equity=250000.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    repo.upsert_positions(
        [
            BrokerPosition(
                id=broker_position_id(account_id, "GLW", now),
                account_id=account_id,
                as_of=now,
                ticker="GLW",
                quantity=100,
                market_value=9500.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    repo.upsert_market_snapshots(
        [
            BrokerMarketSnapshot(
                id=broker_market_snapshot_id("GLW", now),
                ticker="GLW",
                as_of=now,
                last_price=95.0,
                day_change_percent=1.5,
                relative_volume=1.8,
                option_call_put_ratio=2.2,
                raw_payload={},
                created_at=now,
            )
        ]
    )

    summary = load_broker_summary(engine)

    assert summary["snapshot"]["connection_status"] == "connected"
    assert summary["snapshot"]["account_count"] == 1
    assert summary["positions"][0]["ticker"] == "GLW"
    assert summary["market_context"][0]["ticker"] == "GLW"
    assert summary["market_context"][0]["last_price"] == 95.0
    assert summary["balances"][0]["cash"] == 50000.0
    assert summary["exposure"]["broker_data_stale"] is False
    assert summary["exposure"]["exposure_before"]["single_name"] == {"GLW": 0.038}
    assert summary["rate_limit_config"]["portfolio_sync_min_interval_seconds"] == 900
    assert summary["rate_limits"][0]["operation"] == "portfolio_sync"


def _engine(tmp_path: Path) -> Engine:
    engine = create_engine(f"sqlite:///{(tmp_path / 'dashboard.db').as_posix()}", future=True)
    create_schema(engine)
    return engine


def _insert_active_security_for_call_plan(
    engine: Engine,
    ticker: str,
    *,
    cik: str | None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(securities).values(
                ticker=ticker,
                name=ticker,
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=100_000_000_000.0,
                avg_dollar_volume_20d=1_000_000_000.0,
                has_options=True,
                is_active=True,
                updated_at=AVAILABLE_AT,
                metadata={"cik": cik} if cik is not None else {},
            )
        )


def _run_step(
    step: str,
    status: str,
    *,
    requested: int = 0,
    raw: int = 0,
    normalized: int = 0,
    reason: str | None = None,
) -> dict[str, object]:
    return {
        "step": step,
        "status": status,
        "requested_count": requested,
        "raw_count": raw,
        "normalized_count": normalized,
        "reason": reason,
    }


def _insert_dashboard_fixture(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states),
            [
                _candidate_state_row(
                    id="state-msft-earlier",
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=74.0,
                ),
                _candidate_state_row(
                    id="state-msft-latest",
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                ),
                _candidate_state_row(
                    id="state-aapl-latest",
                    ticker="AAPL",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=76.0,
                ),
            ],
        )
        conn.execute(
            insert(signal_features),
            [
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=EARLIER_AS_OF,
                    state=ActionState.ADD_TO_WATCHLIST.value,
                    final_score=74.0,
                    theme="ai_infrastructure",
                ),
                _signal_feature_row(
                    ticker="MSFT",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=88.0,
                    theme="ai_infrastructure",
                ),
                _signal_feature_row(
                    ticker="AAPL",
                    as_of=AS_OF,
                    state=ActionState.WARNING.value,
                    final_score=76.0,
                    theme="ai_infrastructure",
                ),
            ],
        )
        conn.execute(
            insert(candidate_packets).values(
                id="packet-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_state_id="state-msft-latest",
                state=ActionState.WARNING.value,
                final_score=88.0,
                schema_version="candidate-packet-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload=_candidate_packet_payload(),
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(decision_cards).values(
                id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_packet_id="packet-msft-latest",
                action_state=ActionState.WARNING.value,
                setup_type="breakout",
                final_score=88.0,
                schema_version="decision-card-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                next_review_at=NEXT_REVIEW_AT,
                user_decision=None,
                payload=_decision_card_payload(),
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(events).values(
                id="event-msft",
                ticker="MSFT",
                event_type="guidance",
                provider="news_fixture",
                source="Reuters",
                source_category="reputable_news",
                source_url="https://news.example.com/msft",
                title="MSFT guidance raised",
                body_hash="body-msft",
                dedupe_key="MSFT:event-msft",
                source_quality=0.9,
                materiality=0.85,
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"summary": "Raised cloud guidance."},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(events).values(
                id="event-msft-future",
                ticker="MSFT",
                event_type="guidance",
                provider="news_fixture",
                source="Reuters",
                source_category="reputable_news",
                source_url="https://news.example.com/msft-future",
                title="MSFT future guidance",
                body_hash="body-msft-future",
                dedupe_key="MSFT:event-msft-future",
                source_quality=0.9,
                materiality=0.9,
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"summary": "Future row must not leak into decision-time detail."},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(text_snippets).values(
                id="snippet-msft",
                ticker="MSFT",
                event_id="event-msft",
                snippet_hash="snippet-hash",
                section="body",
                text="Azure demand improved.",
                source="Reuters",
                source_url="https://news.example.com/msft",
                source_quality=0.9,
                event_type="guidance",
                materiality=0.8,
                ontology_hits=[{"theme": "ai_infrastructure"}],
                sentiment=0.7,
                embedding=[0.1, 0.2],
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"rank": 1},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(text_snippets).values(
                id="snippet-msft-future",
                ticker="MSFT",
                event_id="event-msft-future",
                snippet_hash="snippet-hash-future",
                section="body",
                text="Future detail should be hidden.",
                source="Reuters",
                source_url="https://news.example.com/msft-future",
                source_quality=0.9,
                event_type="guidance",
                materiality=0.9,
                ontology_hits=[{"theme": "ai_infrastructure"}],
                sentiment=0.7,
                embedding=[0.1, 0.2],
                source_ts=FUTURE_AT,
                available_at=FUTURE_AT,
                payload={"rank": 1},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(validation_runs),
            [
                _validation_run_row("validation-run-earlier", EARLIER_AS_OF),
                _validation_run_row("validation-run-latest", AS_OF),
                _validation_run_row(
                    "validation-run-failed",
                    AS_OF + timedelta(days=1),
                    status="failed",
                ),
            ],
        )
        conn.execute(
            insert(validation_results).values(
                id="validation-result-msft",
                run_id="validation-run-latest",
                ticker="MSFT",
                as_of=AS_OF,
                available_at=AVAILABLE_AT,
                state=ActionState.WARNING.value,
                final_score=88.0,
                candidate_state_id="state-msft-latest",
                candidate_packet_id="packet-msft-latest",
                decision_card_id="card-msft-latest",
                baseline=None,
                labels={"target_20d_25": True},
                leakage_flags=[],
                payload={"audit": {"external_calls": False}},
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(validation_results).values(
                id="validation-result-msft-future",
                run_id="validation-run-latest",
                ticker="MSFT",
                as_of=AS_OF,
                available_at=FUTURE_AT,
                state=ActionState.WARNING.value,
                final_score=88.0,
                candidate_state_id="state-msft-latest",
                candidate_packet_id="packet-msft-latest",
                decision_card_id="card-msft-latest",
                baseline=None,
                labels={"target_20d_25": False},
                leakage_flags=[],
                payload={"audit": {"external_calls": False}},
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(paper_trades).values(
                id="paper-msft",
                decision_card_id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                decision="approved",
                state="open",
                entry_price=101.0,
                entry_at=AVAILABLE_AT,
                invalidation_price=94.0,
                shares=20.0,
                notional=2080.0,
                max_loss=200.0,
                outcome_labels={"target_20d_25": True},
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={"no_execution": True},
                created_at=AVAILABLE_AT,
                updated_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(paper_trades).values(
                id="paper-msft-future",
                decision_card_id="card-msft-latest",
                ticker="MSFT",
                as_of=AS_OF,
                decision="approved",
                state="closed",
                entry_price=101.0,
                entry_at=AVAILABLE_AT,
                invalidation_price=94.0,
                shares=20.0,
                notional=2080.0,
                max_loss=200.0,
                outcome_labels={"target_20d_25": False},
                source_ts=SOURCE_TS,
                available_at=FUTURE_AT,
                payload={"no_execution": True},
                created_at=FUTURE_AT,
                updated_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-msft",
                artifact_type="decision_card",
                artifact_id="card-msft-latest",
                ticker="MSFT",
                label="useful",
                notes="worth review",
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(useful_alert_labels).values(
                id="label-useful-future-msft",
                artifact_type="decision_card",
                artifact_id="card-msft-latest",
                ticker="MSFT",
                label="useful",
                notes="future label should not change latest-run cost summary",
                created_at=FUTURE_AT,
            )
        )
        conn.execute(
            insert(provider_health),
            [
                {
                    "id": "provider-polygon",
                    "provider": "polygon",
                    "status": "healthy",
                    "checked_at": AVAILABLE_AT,
                    "reason": "ok",
                    "latency_ms": 123.0,
                },
                {
                    "id": "provider-news",
                    "provider": "news",
                    "status": "stale",
                    "checked_at": AVAILABLE_AT,
                    "reason": "last record exceeded freshness window",
                    "latency_ms": None,
                },
            ],
        )
        conn.execute(
            insert(job_runs).values(
                id="job-ingest",
                job_type="provider_ingest",
                provider="polygon",
                status="success",
                started_at=SOURCE_TS,
                finished_at=AVAILABLE_AT,
                requested_count=1,
                raw_count=1,
                normalized_count=1,
                error_summary=None,
                metadata={"dry_run": True},
            )
        )


def _job_run_row(
    job_id: str,
    *,
    job_type: str,
    status: str,
    started_at: datetime,
    metadata: dict[str, object],
    requested_count: int = 0,
    raw_count: int = 0,
    normalized_count: int = 0,
    error_summary: str | None = None,
) -> dict[str, object]:
    return {
        "id": job_id,
        "job_type": job_type,
        "provider": metadata.get("provider"),
        "status": status,
        "started_at": started_at,
        "finished_at": started_at + timedelta(seconds=1),
        "requested_count": requested_count,
        "raw_count": raw_count,
        "normalized_count": normalized_count,
        "error_summary": error_summary,
        "metadata": metadata,
    }


def _insert_alert_fixture(engine: Engine, *, available_at: datetime = AVAILABLE_AT) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(alerts),
            [
                _alert_row(
                    id="alert-msft-planned",
                    route="immediate_manual_review",
                    status="planned",
                    priority="high",
                    available_at=available_at,
                    created_at=available_at,
                ),
                _alert_row(
                    id="alert-msft-dry-run",
                    route="warning_digest",
                    status="dry_run",
                    priority="critical",
                    available_at=available_at + timedelta(minutes=20),
                    created_at=available_at + timedelta(minutes=20),
                    dedupe_key="alert-dedupe-v1:MSFT:warning_digest:Warning:score:88",
                ),
            ],
        )
        conn.execute(
            insert(user_feedback),
            [
                {
                    "id": "feedback-alert-msft-planned",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-planned",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "worth review",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-planned"},
                    "created_at": available_at,
                },
                {
                    "id": "feedback-alert-msft-dry-run-old",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-dry-run",
                    "ticker": "MSFT",
                    "label": "useful",
                    "notes": "old feedback",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-dry-run"},
                    "created_at": available_at + timedelta(minutes=21),
                },
                {
                    "id": "feedback-alert-msft-dry-run-latest",
                    "artifact_type": "alert",
                    "artifact_id": "alert-msft-dry-run",
                    "ticker": "MSFT",
                    "label": "acted",
                    "notes": "latest feedback",
                    "source": "dashboard",
                    "payload": {"alert_id": "alert-msft-dry-run"},
                    "created_at": available_at + timedelta(minutes=22),
                },
            ],
        )


def _insert_budget_ledger_fixture(
    engine: Engine,
    *,
    available_at: datetime,
    future_at: datetime | None = None,
) -> None:
    repo = BudgetLedgerRepository(engine)
    repo.upsert_entry(
        BudgetLedgerEntry(
            **_budget_ledger_entry_kwargs(
                ticker="msft",
                status=LLMCallStatus.COMPLETED,
                available_at=available_at,
                estimated_cost=0.22,
                actual_cost=0.19,
            )
        )
    )
    repo.upsert_entry(
        BudgetLedgerEntry(
            **_budget_ledger_entry_kwargs(
                ticker="aapl",
                status=LLMCallStatus.SKIPPED,
                available_at=future_at or available_at + timedelta(seconds=30),
                estimated_cost=0.0,
                actual_cost=0.0,
                skip_reason=LLMSkipReason.PREMIUM_LLM_DISABLED,
            )
        )
    )


def _budget_ledger_entry_kwargs(
    *,
    ticker: str,
    status: LLMCallStatus,
    available_at: datetime,
    estimated_cost: float,
    actual_cost: float,
    skip_reason: LLMSkipReason | None = None,
) -> dict[str, object]:
    return {
        "id": budget_ledger_id(
            task=LLMTaskName.MID_REVIEW.value,
            ticker=ticker,
            candidate_packet_id=f"candidate-packet-{ticker.upper()}",
            status=status.value,
            available_at=available_at,
            prompt_version="evidence_review_v1",
        ),
        "ts": available_at - timedelta(minutes=5),
        "available_at": available_at,
        "ticker": ticker,
        "candidate_state_id": f"candidate-state-{ticker.upper()}",
        "candidate_packet_id": f"candidate-packet-{ticker.upper()}",
        "decision_card_id": f"decision-card-{ticker.upper()}",
        "task": LLMTaskName.MID_REVIEW,
        "model": "model-review",
        "provider": "openai",
        "status": status,
        "skip_reason": skip_reason,
        "token_usage": TokenUsage(
            input_tokens=1_000,
            cached_input_tokens=100,
            output_tokens=250,
        ),
        "tool_calls": [{"name": "evidence_review", "arguments": {"ticker": ticker}}],
        "estimated_cost": estimated_cost,
        "actual_cost": actual_cost,
        "currency": "USD",
        "candidate_state": "Warning",
        "prompt_version": "evidence_review_v1",
        "schema_version": "evidence-review-v1",
        "outcome_label": "reviewed",
        "payload": {"ticker": ticker, "status": status.value},
        "created_at": available_at,
    }


def _alert_row(
    *,
    id: str,
    route: str,
    status: str,
    priority: str,
    available_at: datetime,
    created_at: datetime,
    dedupe_key: str = (
        "alert-dedupe-v1:MSFT:immediate_manual_review:Warning:"
        "state_transition:AddToWatchlist->Warning"
    ),
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": "MSFT",
        "as_of": AS_OF,
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "candidate_state_id": "state-msft-latest",
        "candidate_packet_id": "packet-msft-latest",
        "decision_card_id": "card-msft-latest",
        "action_state": ActionState.WARNING.value,
        "route": route,
        "channel": "dashboard",
        "priority": priority,
        "status": status,
        "dedupe_key": dedupe_key,
        "trigger_kind": "state_transition",
        "trigger_fingerprint": "AddToWatchlist->Warning",
        "title": "MSFT alert review",
        "summary": "MSFT candidate has new review evidence.",
        "feedback_url": "/api/alerts/feedback/alert-msft-planned",
        "payload": {
            "score": 88.0,
            "evidence": [{"kind": "event", "artifact_id": "event-msft"}],
        },
        "created_at": created_at,
        "sent_at": None,
    }


def _ipo_event_row(
    *,
    id: str,
    ticker: str,
    available_at: datetime,
    source_ts: datetime,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "event_type": "financing",
        "provider": "sec",
        "source": "SEC EDGAR",
        "source_category": "primary_source",
        "source_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
        "title": f"{ticker} S-1 IPO registration statement",
        "body_hash": f"body-{id}",
        "dedupe_key": f"{ticker}:{id}",
        "source_quality": 1.0,
        "materiality": 0.9,
        "source_ts": source_ts,
        "available_at": available_at,
        "payload": {
            "form_type": "S-1",
            "filing_date": source_ts.date().isoformat(),
            "accession_number": f"000-{id}",
            "document_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
            "document_text_hash": f"hash-{id}",
            "summary": f"{ticker} proposed IPO terms.",
            "ipo_analysis": {
                "analysis_version": "ipo-s1-analysis-v1",
                "company_name": f"{ticker} Robotics, Inc.",
                "form_type": "S-1",
                "source_url": f"https://www.sec.gov/Archives/{ticker.lower()}-s1.htm",
                "proposed_ticker": ticker,
                "exchange": "Nasdaq Global Select Market",
                "shares_offered": 12_500_000,
                "price_range_low": 17.0,
                "price_range_high": 19.0,
                "price_range_midpoint": 18.0,
                "estimated_gross_proceeds": 225_000_000.0,
                "underwriters": ["Morgan Stanley & Co. LLC"],
                "use_of_proceeds_summary": "working capital and research and development",
                "risk_flags": ["history_of_losses", "emerging_growth_company"],
                "sections_found": ["prospectus summary", "risk factors"],
            },
        },
        "created_at": available_at,
    }


def _candidate_state_row(
    *,
    id: str,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
    created_at: datetime = AVAILABLE_AT,
) -> dict[str, object]:
    return {
        "id": id,
        "ticker": ticker,
        "as_of": as_of,
        "state": state,
        "previous_state": None,
        "final_score": final_score,
        "score_delta_5d": 4.0,
        "hard_blocks": [],
        "transition_reasons": ["score_requires_manual_review"],
        "feature_version": "score-v4-options-theme",
        "policy_version": "policy-v2-events",
        "created_at": created_at,
    }


def _signal_feature_row(
    *,
    ticker: str,
    as_of: datetime,
    state: str,
    final_score: float,
    theme: str,
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "as_of": as_of,
        "feature_version": "score-v4-options-theme",
        "price_strength": 82.0,
        "volume_score": 74.0,
        "liquidity_score": 91.0,
        "risk_penalty": 4.0,
        "portfolio_penalty": 1.0,
        "final_score": final_score,
        "payload": {
            "candidate": {
                "ticker": ticker,
                "as_of": as_of.isoformat(),
                "features": {
                    "ticker": ticker,
                    "as_of": as_of.isoformat(),
                    "feature_version": "score-v4-options-theme",
                },
                "final_score": final_score,
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
                "metadata": {
                    "source_ts": SOURCE_TS.isoformat(),
                    "available_at": AVAILABLE_AT.isoformat(),
                    "setup_type": "breakout",
                    "candidate_theme": theme,
                    "theme_hits": [{"theme_id": theme, "count": 2}],
                    "portfolio_impact": {
                        "proposed_notional": 2080.0,
                        "max_loss": 200.0,
                        "hard_blocks": [],
                    },
                },
            },
            "policy": {
                "state": state,
                "hard_blocks": [],
                "reasons": ["score_requires_manual_review"],
                "missing_trade_plan": [],
                "policy_version": "policy-v2-events",
            },
        },
    }


def _candidate_packet_payload() -> dict[str, object]:
    return {
        "identity": {"ticker": "MSFT", "as_of": AS_OF.isoformat()},
        "scores": {"final": 88.0},
        "trade_plan": {
            "entry_zone": [100.0, 104.0],
            "invalidation_price": 94.0,
            "reward_risk": 2.7,
        },
        "setup_plan": {"setup_type": "breakout", "review_focus": "volume confirmation"},
        "portfolio_impact": {
            "proposed_notional": 2080.0,
            "max_loss": 200.0,
            "hard_blocks": [],
        },
        "supporting_evidence": [
            {
                "kind": "event",
                "title": "MSFT guidance raised",
                "source_id": "event-msft",
                "source_url": "https://news.example.com/msft",
                "strength": 0.8,
            }
        ],
        "disconfirming_evidence": [
            {
                "kind": "risk",
                "title": "Valuation stretched",
                "computed_feature_id": "signal_features:MSFT:risk",
                "strength": 0.4,
            }
        ],
        "hard_blocks": [],
        "audit": {
            "source_ts": SOURCE_TS.isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
            "provider_license_policy": {
                "license_tags": ["local-csv-fixture"],
                "metadata_complete": True,
                "prompt_allowed": True,
                "external_export_allowed": False,
                "attribution_required": False,
                "policies": [],
            },
        },
    }


def _decision_card_payload() -> dict[str, object]:
    return {
        "disclaimer": "Manual review only.",
        "manual_review_only": True,
        "trade_plan": {
            "entry_zone": [100.0, 104.0],
            "invalidation_price": 94.0,
            "reward_risk": 2.7,
        },
        "setup_plan": {"setup_type": "breakout", "next_step": "review evidence"},
        "portfolio_impact": {
            "proposed_notional": 2080.0,
            "max_loss": 200.0,
            "hard_blocks": [],
        },
    }


def _validation_run_row(
    id: str,
    as_of: datetime,
    *,
    status: str = "success",
) -> dict[str, object]:
    return {
        "id": id,
        "run_type": "point_in_time_replay",
        "as_of_start": as_of,
        "as_of_end": as_of,
        "decision_available_at": AVAILABLE_AT,
        "status": status,
        "config": {"states": [ActionState.WARNING.value]},
        "metrics": {},
        "started_at": as_of,
        "finished_at": as_of + timedelta(minutes=5),
        "created_at": as_of + timedelta(minutes=10),
    }
