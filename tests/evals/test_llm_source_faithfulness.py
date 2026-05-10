from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.agents.evidence import (
    build_agent_evidence_packet,
    source_faithfulness_violations,
)
from catalyst_radar.core.models import ActionState
from catalyst_radar.pipeline.candidate_packet import CandidatePacket, EvidenceItem

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_eval_accepts_source_linked_skeptic_review() -> None:
    view = build_agent_evidence_packet(_candidate())

    skeptic_review = {
        "ticker": "MSFT",
        "claims": [
            {
                "claim": "Cloud guidance improved.",
                "source_id": "event-msft",
            }
        ],
        "bear_case": [
            {
                "claim": "Risk penalty remains elevated.",
                "computed_feature_id": "feature-risk-msft",
            }
        ],
    }

    assert source_faithfulness_violations(skeptic_review, view) == []


def test_eval_rejects_hallucinated_source_id() -> None:
    view = build_agent_evidence_packet(_candidate())

    skeptic_review = {
        "claims": [
            {
                "claim": "A nonexistent report confirms the thesis.",
                "source_id": "press-release-unknown",
            }
        ]
    }

    assert source_faithfulness_violations(skeptic_review, view) == [
        "claims[0].source_id is not in allowed_reference_ids: press-release-unknown"
    ]


def test_eval_rejects_decision_card_draft_without_references() -> None:
    view = build_agent_evidence_packet(_candidate())

    decision_card_draft = {
        "summary": {
            "supporting_points": [
                {"claim": "The setup is attractive."},
            ],
            "risks": [
                {"claim": "Risk is manageable."},
            ],
        }
    }

    assert source_faithfulness_violations(decision_card_draft, view) == [
        "summary.supporting_points[0] must include source_id or computed_feature_id",
        "summary.risks[0] must include source_id or computed_feature_id",
    ]


def _candidate() -> CandidatePacket:
    return CandidatePacket(
        id="packet-msft",
        ticker="MSFT",
        as_of=AS_OF,
        candidate_state_id="state-msft",
        state=ActionState.WARNING,
        final_score=82.5,
        supporting_evidence=(
            EvidenceItem(
                kind="event",
                title="Cloud guidance raised",
                summary="Company raised cloud revenue guidance.",
                polarity="supporting",
                strength=0.9,
                source_id="event-msft",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
            ),
        ),
        disconfirming_evidence=(
            EvidenceItem(
                kind="computed_feature",
                title="Risk penalty elevated",
                summary="Risk penalty is above the review threshold.",
                polarity="disconfirming",
                strength=0.7,
                computed_feature_id="feature-risk-msft",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
            ),
        ),
        conflicts=(),
        hard_blocks=(),
        payload={},
        source_ts=SOURCE_TS,
        available_at=AVAILABLE_AT,
    )
