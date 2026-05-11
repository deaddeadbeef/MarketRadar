from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.alerts.models import AlertStatus
from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import (
    alerts,
    budget_ledger,
    candidate_packets,
    candidate_states,
    decision_cards,
    events,
    paper_trades,
    signal_features,
    useful_alert_labels,
    user_feedback,
    validation_results,
    validation_runs,
)

DEMO_TICKER = "ACME"
DEMO_CIK = "0002000001"
DEMO_AS_OF = datetime(2026, 5, 10, 21, 0, tzinfo=UTC)
DEMO_SOURCE_TS = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
DEMO_AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)
DEMO_NEXT_REVIEW_AT = datetime(2026, 5, 12, 21, 0, tzinfo=UTC)
DEMO_FEATURE_VERSION = "demo-score-v1"
DEMO_POLICY_VERSION = "demo-policy-v1"


@dataclass(frozen=True)
class DashboardDemoSeedResult:
    ticker: str
    sec_result: ProviderIngestResult
    event_id: str | None
    candidate_state_id: str
    alert_id: str
    validation_run_id: str
    budget_ledger_id: str


def default_sec_fixture_path() -> Path:
    return _repo_root() / "tests" / "fixtures" / "sec" / "submissions_acme_s1.json"


def default_sec_document_fixture_path() -> Path:
    return _repo_root() / "tests" / "fixtures" / "sec" / "acme_s1.htm"


def seed_dashboard_demo(
    engine: Engine,
    *,
    ticker: str = DEMO_TICKER,
    cik: str = DEMO_CIK,
    sec_fixture_path: Path | None = None,
    document_fixture_path: Path | None = None,
) -> DashboardDemoSeedResult:
    symbol = ticker.strip().upper()
    if not symbol:
        msg = "ticker must not be blank"
        raise ValueError(msg)

    sec_fixture = sec_fixture_path or default_sec_fixture_path()
    document_fixture = document_fixture_path or default_sec_document_fixture_path()
    _require_readable_fixture(sec_fixture)
    _require_readable_fixture(document_fixture)

    sec_result = _ingest_demo_sec_s1(
        engine,
        ticker=symbol,
        cik=cik,
        sec_fixture_path=sec_fixture,
        document_fixture_path=document_fixture,
    )
    event_row = _latest_ipo_event(engine, symbol)
    event_id = str(event_row["id"]) if event_row is not None else None
    event_title = str(event_row["title"]) if event_row is not None else f"{symbol} S-1"
    event_url = str(event_row["source_url"]) if event_row is not None else None

    ids = _demo_ids(symbol)
    _replace_demo_rows(
        engine,
        symbol=symbol,
        ids=ids,
        event_id=event_id,
        event_title=event_title,
        event_url=event_url,
    )
    budget_id = _replace_demo_budget_row(engine, symbol=symbol, ids=ids)
    return DashboardDemoSeedResult(
        ticker=symbol,
        sec_result=sec_result,
        event_id=event_id,
        candidate_state_id=ids["state"],
        alert_id=ids["alert"],
        validation_run_id=ids["validation_run"],
        budget_ledger_id=budget_id,
    )


def _ingest_demo_sec_s1(
    engine: Engine,
    *,
    ticker: str,
    cik: str,
    sec_fixture_path: Path,
    document_fixture_path: Path,
) -> ProviderIngestResult:
    connector = SecSubmissionsConnector(
        fixture_path=sec_fixture_path,
        document_fixture_path=document_fixture_path,
    )
    return ingest_provider_records(
        connector=connector,
        request=ConnectorRequest(
            provider="sec",
            endpoint="ipo-s1",
            params={"ticker": ticker, "cik": cik},
            requested_at=DEMO_AVAILABLE_AT,
        ),
        market_repo=MarketRepository(engine),
        provider_repo=ProviderRepository(engine),
        job_type="demo_sec_ipo_s1",
        metadata={
            "provider": "sec",
            "endpoint": "ipo-s1",
            "ticker": ticker,
            "fixture": str(sec_fixture_path),
            "document_fixture": str(document_fixture_path),
            "demo": True,
        },
        event_repo=EventRepository(engine),
    )


def _replace_demo_rows(
    engine: Engine,
    *,
    symbol: str,
    ids: dict[str, str],
    event_id: str | None,
    event_title: str,
    event_url: str | None,
) -> None:
    with engine.begin() as conn:
        for table, row_id in (
            (user_feedback, ids["alert_feedback"]),
            (useful_alert_labels, ids["useful_label"]),
            (paper_trades, ids["paper_trade"]),
            (validation_results, ids["validation_result"]),
            (validation_runs, ids["validation_run"]),
            (alerts, ids["alert"]),
            (decision_cards, ids["card"]),
            (candidate_packets, ids["packet"]),
            (candidate_states, ids["state"]),
        ):
            conn.execute(delete(table).where(table.c.id == row_id))
        conn.execute(
            delete(signal_features).where(
                signal_features.c.ticker == symbol,
                signal_features.c.as_of == DEMO_AS_OF,
                signal_features.c.feature_version == DEMO_FEATURE_VERSION,
            )
        )

        conn.execute(insert(candidate_states).values(_candidate_state_row(symbol, ids)))
        conn.execute(
            insert(signal_features).values(
                _signal_feature_row(
                    symbol=symbol,
                    event_id=event_id,
                    event_title=event_title,
                    event_url=event_url,
                )
            )
        )
        conn.execute(
            insert(candidate_packets).values(
                _candidate_packet_row(
                    symbol=symbol,
                    ids=ids,
                    event_id=event_id,
                    event_title=event_title,
                    event_url=event_url,
                )
            )
        )
        conn.execute(insert(decision_cards).values(_decision_card_row(symbol, ids)))
        conn.execute(
            insert(alerts).values(
                _alert_row(
                    symbol=symbol,
                    ids=ids,
                    event_id=event_id,
                    event_title=event_title,
                )
            )
        )
        conn.execute(insert(user_feedback).values(_alert_feedback_row(symbol, ids)))
        conn.execute(insert(validation_runs).values(_validation_run_row(ids)))
        conn.execute(insert(validation_results).values(_validation_result_row(symbol, ids)))
        conn.execute(insert(useful_alert_labels).values(_useful_label_row(symbol, ids)))
        conn.execute(insert(paper_trades).values(_paper_trade_row(symbol, ids)))


def _replace_demo_budget_row(
    engine: Engine,
    *,
    symbol: str,
    ids: dict[str, str],
) -> str:
    ledger_id = budget_ledger_id(
        task=LLMTaskName.MID_REVIEW.value,
        ticker=symbol,
        candidate_packet_id=ids["packet"],
        status=LLMCallStatus.COMPLETED.value,
        available_at=DEMO_AVAILABLE_AT,
        prompt_version="demo-review-v1",
    )
    with engine.begin() as conn:
        conn.execute(delete(budget_ledger).where(budget_ledger.c.id == ledger_id))
    BudgetLedgerRepository(engine).upsert_entry(
        BudgetLedgerEntry(
            id=ledger_id,
            ts=DEMO_AVAILABLE_AT - timedelta(minutes=2),
            available_at=DEMO_AVAILABLE_AT,
            ticker=symbol,
            candidate_state_id=ids["state"],
            candidate_packet_id=ids["packet"],
            decision_card_id=ids["card"],
            task=LLMTaskName.MID_REVIEW,
            model="gpt-4.1-mini-demo",
            provider="openai",
            status=LLMCallStatus.COMPLETED,
            token_usage=TokenUsage(
                input_tokens=1_800,
                cached_input_tokens=400,
                output_tokens=320,
            ),
            tool_calls=(),
            estimated_cost=0.04,
            actual_cost=0.03,
            candidate_state=ActionState.WARNING.value,
            prompt_version="demo-review-v1",
            schema_version="demo-review-v1",
            outcome_label="reviewed",
            payload={"demo": True, "ticker": symbol, "decision": "manual_review_only"},
            created_at=DEMO_AVAILABLE_AT,
        )
    )
    return ledger_id


def _candidate_state_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["state"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "state": ActionState.WARNING.value,
        "previous_state": ActionState.ADD_TO_WATCHLIST.value,
        "final_score": 83.0,
        "score_delta_5d": 7.0,
        "hard_blocks": [],
        "transition_reasons": ["ipo_s1_primary_source_requires_manual_review"],
        "feature_version": DEMO_FEATURE_VERSION,
        "policy_version": DEMO_POLICY_VERSION,
        "created_at": DEMO_AVAILABLE_AT,
    }


def _signal_feature_row(
    *,
    symbol: str,
    event_id: str | None,
    event_title: str,
    event_url: str | None,
) -> dict[str, object]:
    return {
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "feature_version": DEMO_FEATURE_VERSION,
        "price_strength": 76.0,
        "volume_score": 69.0,
        "liquidity_score": 82.0,
        "risk_penalty": 6.0,
        "portfolio_penalty": 2.0,
        "final_score": 83.0,
        "payload": {
            "candidate": {
                "ticker": symbol,
                "as_of": DEMO_AS_OF.isoformat(),
                "final_score": 83.0,
                "entry_zone": [17.0, 19.0],
                "invalidation_price": 15.5,
                "metadata": {
                    "source_ts": DEMO_SOURCE_TS.isoformat(),
                    "available_at": DEMO_AVAILABLE_AT.isoformat(),
                    "setup_type": "ipo_primary_source_review",
                    "candidate_theme": "automation_and_robotics",
                    "theme_hits": [{"theme_id": "automation_and_robotics", "count": 1}],
                    "material_event_count": 1,
                    "top_event_type": "financing",
                    "top_event_title": event_title,
                    "top_event_source": "SEC EDGAR",
                    "top_event_source_url": event_url,
                    "top_event_source_quality": 1.0,
                    "top_event_materiality": 0.9,
                    "portfolio_impact": {
                        "proposed_notional": 2500.0,
                        "max_loss": 350.0,
                        "hard_blocks": [],
                    },
                },
            },
            "policy": {
                "state": ActionState.WARNING.value,
                "hard_blocks": [],
                "reasons": ["ipo_s1_primary_source_requires_manual_review"],
                "missing_trade_plan": [],
                "policy_version": DEMO_POLICY_VERSION,
            },
        },
    }


def _candidate_packet_row(
    *,
    symbol: str,
    ids: dict[str, str],
    event_id: str | None,
    event_title: str,
    event_url: str | None,
) -> dict[str, object]:
    return {
        "id": ids["packet"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "candidate_state_id": ids["state"],
        "state": ActionState.WARNING.value,
        "final_score": 83.0,
        "schema_version": "candidate-packet-demo-v1",
        "source_ts": DEMO_SOURCE_TS,
        "available_at": DEMO_AVAILABLE_AT,
        "payload": {
            "identity": {"ticker": symbol, "as_of": DEMO_AS_OF.isoformat()},
            "scores": {"final": 83.0},
            "trade_plan": {
                "entry_zone": [17.0, 19.0],
                "invalidation_price": 15.5,
                "reward_risk": 2.2,
            },
            "setup_plan": {
                "setup_type": "ipo_primary_source_review",
                "review_focus": "S-1 terms, risk factors, use of proceeds",
            },
            "portfolio_impact": {
                "proposed_notional": 2500.0,
                "max_loss": 350.0,
                "hard_blocks": [],
            },
            "supporting_evidence": [
                {
                    "kind": "event",
                    "title": event_title,
                    "source_id": event_id,
                    "source_url": event_url,
                    "strength": 0.9,
                }
            ],
            "disconfirming_evidence": [
                {
                    "kind": "risk",
                    "title": "S-1 risk factors include losses and emerging-growth-company status",
                    "computed_feature_id": "demo:s1-risk-flags",
                    "strength": 0.55,
                }
            ],
            "hard_blocks": [],
            "audit": {
                "source_ts": DEMO_SOURCE_TS.isoformat(),
                "available_at": DEMO_AVAILABLE_AT.isoformat(),
            },
        },
        "created_at": DEMO_AVAILABLE_AT,
    }


def _decision_card_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["card"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "candidate_packet_id": ids["packet"],
        "action_state": ActionState.WARNING.value,
        "setup_type": "ipo_primary_source_review",
        "final_score": 83.0,
        "schema_version": "decision-card-demo-v1",
        "source_ts": DEMO_SOURCE_TS,
        "available_at": DEMO_AVAILABLE_AT,
        "next_review_at": DEMO_NEXT_REVIEW_AT,
        "user_decision": None,
        "payload": {
            "disclaimer": "Manual review only.",
            "manual_review_only": True,
            "setup_plan": {
                "setup_type": "ipo_primary_source_review",
                "next_step": "Review S-1 terms against IPO watchlist criteria",
            },
            "trade_plan": {
                "entry_zone": [17.0, 19.0],
                "invalidation_price": 15.5,
                "reward_risk": 2.2,
            },
            "portfolio_impact": {
                "proposed_notional": 2500.0,
                "max_loss": 350.0,
                "hard_blocks": [],
            },
        },
        "created_at": DEMO_AVAILABLE_AT,
    }


def _alert_row(
    *,
    symbol: str,
    ids: dict[str, str],
    event_id: str | None,
    event_title: str,
) -> dict[str, object]:
    return {
        "id": ids["alert"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "source_ts": DEMO_SOURCE_TS,
        "available_at": DEMO_AVAILABLE_AT,
        "candidate_state_id": ids["state"],
        "candidate_packet_id": ids["packet"],
        "decision_card_id": ids["card"],
        "action_state": ActionState.WARNING.value,
        "route": "immediate_manual_review",
        "channel": "dashboard",
        "priority": "high",
        "status": AlertStatus.PLANNED.value,
        "dedupe_key": f"demo-alert:{symbol}:ipo-s1",
        "trigger_kind": "event",
        "trigger_fingerprint": f"{symbol}:ipo-s1",
        "title": f"{symbol} IPO S-1 review",
        "summary": "Primary SEC filing has extracted offering terms and risk flags.",
        "feedback_url": f"/api/alerts/{ids['alert']}/feedback",
        "payload": {
            "score": 83.0,
            "evidence": [{"kind": "event", "artifact_id": event_id, "title": event_title}],
            "demo": True,
        },
        "created_at": DEMO_AVAILABLE_AT,
        "sent_at": None,
    }


def _alert_feedback_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["alert_feedback"],
        "artifact_type": "alert",
        "artifact_id": ids["alert"],
        "ticker": symbol,
        "label": "useful",
        "notes": "Demo alert maps SEC S-1 analysis to dashboard review.",
        "source": "dashboard",
        "payload": {"demo": True, "alert_id": ids["alert"]},
        "created_at": DEMO_AVAILABLE_AT,
    }


def _validation_run_row(ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["validation_run"],
        "run_type": "demo_point_in_time_replay",
        "as_of_start": DEMO_AS_OF,
        "as_of_end": DEMO_AS_OF,
        "decision_available_at": DEMO_AVAILABLE_AT,
        "status": "success",
        "config": {"demo": True, "states": [ActionState.WARNING.value]},
        "metrics": {"total_cost_usd": 0.03},
        "started_at": DEMO_AVAILABLE_AT - timedelta(minutes=5),
        "finished_at": DEMO_AVAILABLE_AT,
        "created_at": DEMO_AVAILABLE_AT,
    }


def _validation_result_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["validation_result"],
        "run_id": ids["validation_run"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "available_at": DEMO_AVAILABLE_AT,
        "state": ActionState.WARNING.value,
        "final_score": 83.0,
        "candidate_state_id": ids["state"],
        "candidate_packet_id": ids["packet"],
        "decision_card_id": ids["card"],
        "baseline": None,
        "labels": {"target_20d_25": True},
        "leakage_flags": [],
        "payload": {"demo": True, "audit": {"external_calls": False}},
        "created_at": DEMO_AVAILABLE_AT,
    }


def _useful_label_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["useful_label"],
        "artifact_type": "decision_card",
        "artifact_id": ids["card"],
        "ticker": symbol,
        "label": "useful",
        "notes": "Demo review artifact is useful for dashboard smoke testing.",
        "created_at": DEMO_AVAILABLE_AT,
    }


def _paper_trade_row(symbol: str, ids: dict[str, str]) -> dict[str, object]:
    return {
        "id": ids["paper_trade"],
        "decision_card_id": ids["card"],
        "ticker": symbol,
        "as_of": DEMO_AS_OF,
        "decision": "approved",
        "state": "open",
        "entry_price": 18.0,
        "entry_at": DEMO_AVAILABLE_AT,
        "invalidation_price": 15.5,
        "shares": 100.0,
        "notional": 1800.0,
        "max_loss": 250.0,
        "outcome_labels": {"target_20d_25": True},
        "source_ts": DEMO_SOURCE_TS,
        "available_at": DEMO_AVAILABLE_AT,
        "payload": {"demo": True, "no_execution": True},
        "created_at": DEMO_AVAILABLE_AT,
        "updated_at": DEMO_AVAILABLE_AT,
    }


def _latest_ipo_event(engine: Engine, symbol: str) -> dict[str, object] | None:
    with engine.connect() as conn:
        row = conn.execute(
            select(events)
            .where(events.c.ticker == symbol, events.c.provider == "sec")
            .order_by(events.c.available_at.desc(), events.c.created_at.desc())
            .limit(1)
        ).first()
    return dict(row._mapping) if row is not None else None


def _demo_ids(symbol: str) -> dict[str, str]:
    key = symbol.lower()
    return {
        "state": f"demo-state-{key}",
        "packet": f"demo-packet-{key}",
        "card": f"demo-card-{key}",
        "alert": f"demo-alert-{key}",
        "alert_feedback": f"demo-alert-feedback-{key}",
        "validation_run": f"demo-validation-run-{key}",
        "validation_result": f"demo-validation-result-{key}",
        "useful_label": f"demo-useful-label-{key}",
        "paper_trade": f"demo-paper-trade-{key}",
    }


def _require_readable_fixture(path: Path) -> None:
    if not path.is_file():
        msg = f"demo fixture is not readable: {path}"
        raise FileNotFoundError(msg)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


__all__ = [
    "DashboardDemoSeedResult",
    "default_sec_document_fixture_path",
    "default_sec_fixture_path",
    "seed_dashboard_demo",
]
