from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from catalyst_radar.core.models import ActionState
from catalyst_radar.pipeline.candidate_packet import (
    build_candidate_packet,
    canonical_packet_json,
    packet_payload,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 12, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 13, tzinfo=UTC)


def test_candidate_packet_replay_is_deterministic_and_matches_golden_digest() -> None:
    first = _packet()
    second = _packet()

    canonical = canonical_packet_json(first)

    assert first.id == (
        "candidate-packet-v1:MSFT:2026-05-10T21:00:00+00:00:"
        "EligibleForManualBuyReview:2026-05-10T13:00:00+00:00"
    )
    assert canonical == canonical_packet_json(second)
    assert hashlib.sha256(canonical.encode()).hexdigest() == (
        "7b3621a6086085cc228e289db6c9dacbe3c88d57ae0c32d3b2241282fa58945f"
    )

    payload = packet_payload(first)
    assert payload["identity"] == {
        "ticker": "MSFT",
        "as_of": "2026-05-10T21:00:00+00:00",
        "state": "EligibleForManualBuyReview",
        "candidate_state_id": "state-msft",
        "schema_version": "candidate-packet-v1",
    }
    assert payload["disconfirming_evidence"][0]["kind"] == "evidence_gap"
    assert payload["audit"]["score_recomputed"] is False
    assert payload["audit"]["llm_calls"] is False


def _packet():
    return build_candidate_packet(
        candidate_state={
            "id": "state-msft",
            "ticker": "MSFT",
            "as_of": AS_OF,
            "state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
            "final_score": 88.0,
            "score_delta_5d": 4.1,
            "hard_blocks": [],
            "transition_reasons": ["all_buy_review_gates_passed"],
            "feature_version": "score-v4-options-theme",
            "policy_version": "policy-v2-events",
            "created_at": AVAILABLE_AT,
        },
        signal_features_payload=_signal_payload(),
        portfolio_row={
            "id": "portfolio-impact-1",
            "ticker": "MSFT",
            "as_of": AS_OF,
            "setup_type": "breakout",
            "payload": {"portfolio_impact": _portfolio_impact()},
            "source_ts": SOURCE_TS,
            "available_at": AVAILABLE_AT,
        },
        events=[
            {
                "id": "event-1",
                "source_id": "event-1",
                "event_type": "guidance",
                "title": "Cloud revenue guidance raised",
                "source": "company_release",
                "source_url": "https://example.com/msft-guidance",
                "source_quality": 0.95,
                "materiality": 0.90,
                "source_ts": SOURCE_TS.isoformat(),
                "available_at": AVAILABLE_AT.isoformat(),
            }
        ],
        snippets=[
            {
                "id": "snippet-1",
                "event_id": "event-1",
                "section": "management commentary",
                "text": (
                    "Management cited accelerating cloud demand and durable AI "
                    "infrastructure demand."
                ),
                "source": "company_release",
                "source_url": "https://example.com/msft-guidance",
                "source_quality": 0.90,
                "materiality": 0.80,
                "sentiment": 0.60,
                "source_ts": SOURCE_TS.isoformat(),
                "available_at": AVAILABLE_AT.isoformat(),
            }
        ],
        requested_available_at=AVAILABLE_AT,
    )


def _signal_payload() -> dict[str, object]:
    metadata = {
        "setup_type": "breakout",
        "target_price": 125.0,
        "pillar_scores": {
            "price_strength": 86.0,
            "relative_strength": 81.0,
            "volume_liquidity": 72.0,
            "trend_quality": 66.0,
        },
        "event_bonus": 4.8,
        "local_narrative_score": 72.0,
        "local_narrative_bonus": 4.3,
        "source_quality_score": 87.0,
        "theme_match_score": 74.0,
        "theme_hits": [{"theme_id": "ai_infrastructure", "terms": ["cloud", "ai"]}],
        "selected_snippet_ids": ["snippet-1"],
        "options_flow_score": 68.0,
        "options_bonus": 2.7,
        "options_risk_score": 20.0,
        "options_risk_penalty": 0.8,
        "call_put_ratio": 2.4,
        "iv_percentile": 0.55,
        "sector_rotation_score": 62.0,
        "theme_velocity_score": 58.0,
        "peer_readthrough_score": 35.0,
        "sector_theme_bonus": 4.2,
        "position_size": {
            "shares": 40,
            "notional": 4160.0,
            "position_pct": 0.0416,
            "risk_amount": 400.0,
            "is_capped": False,
        },
        "portfolio_impact": _portfolio_impact(),
        "source_ts": SOURCE_TS.isoformat(),
        "available_at": AVAILABLE_AT.isoformat(),
    }
    return {
        "ticker": "MSFT",
        "as_of": AS_OF,
        "feature_version": "score-v4-options-theme",
        "final_score": 88.0,
        "payload": {
            "candidate": {
                "ticker": "MSFT",
                "as_of": AS_OF.isoformat(),
                "features": {
                    "ticker": "MSFT",
                    "as_of": AS_OF.isoformat(),
                    "feature_version": "score-v4-options-theme",
                    "ret_20d": 0.13,
                    "rs_20_sector": 83.0,
                },
                "final_score": 88.0,
                "strong_pillars": 3,
                "risk_penalty": 8.0,
                "portfolio_penalty": 1.0,
                "data_stale": False,
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
                "reward_risk": 2.7,
                "metadata": metadata,
            },
            "policy": {
                "state": "EligibleForManualBuyReview",
                "hard_blocks": [],
                "reasons": ["all_buy_review_gates_passed"],
                "missing_trade_plan": [],
                "policy_version": "policy-v2-events",
            },
        },
    }


def _portfolio_impact() -> dict[str, object]:
    return {
        "ticker": "MSFT",
        "proposed_notional": 4160.0,
        "max_loss": 400.0,
        "single_name_before_pct": 0.05,
        "single_name_after_pct": 0.09,
        "sector_before_pct": 0.22,
        "sector_after_pct": 0.26,
        "theme_before_pct": 0.12,
        "theme_after_pct": 0.16,
        "correlated_before_pct": 0.18,
        "correlated_after_pct": 0.22,
        "portfolio_penalty": 1.0,
        "hard_blocks": [],
    }
