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


def test_build_agent_evidence_packet_collects_allowed_references() -> None:
    view = build_agent_evidence_packet(_candidate())

    assert view["schema_version"] == "agent-evidence-packet-v1"
    assert view["candidate_packet_id"] == "packet-msft"
    assert view["ticker"] == "MSFT"
    assert view["as_of"] == AS_OF.isoformat()
    assert view["available_at"] == AVAILABLE_AT.isoformat()
    assert view["state"] == ActionState.WARNING.value
    assert view["final_score"] == 82.5
    assert "event-msft" in view["allowed_reference_ids"]
    assert "https://example.test/msft-guidance" in view["allowed_reference_ids"]
    assert "conflict-msft" in view["allowed_reference_ids"]
    assert "feature-risk-msft" in view["allowed_computed_feature_ids"]
    assert view["supporting_evidence"][0]["ref"] == "supporting_evidence[0]"
    assert view["disconfirming_evidence"][0]["ref"] == "disconfirming_evidence[0]"
    assert view["supporting_evidence"][0]["source_id"] == "event-msft"
    assert view["supporting_evidence"][0]["source_url"] == "https://example.test/msft-guidance"
    assert view["disconfirming_evidence"][0]["computed_feature_id"] == "feature-risk-msft"
    assert view["conflicts"] == [{"kind": "source_conflict", "source_id": "conflict-msft"}]
    assert view["hard_blocks"] == ["risk_hard_block"]
    assert view["no_trade_execution"] is True


def test_source_faithfulness_accepts_known_source_or_feature_ids() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {
            "claims": [{"claim": "Guidance improved.", "source_id": "event-msft"}],
            "analysis": {
                "bear_case": [
                    {
                        "claim": "Risk penalty is elevated.",
                        "computed_feature_id": "feature-risk-msft",
                    }
                ],
                "supporting_points": [
                    {
                        "claim": "The source URL is permitted as a reference.",
                        "source_id": "https://example.test/msft-guidance",
                    }
                ],
            },
        },
        view,
    )

    assert violations == []


def test_source_faithfulness_rejects_unknown_references() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {
            "claims": [{"claim": "Unsupported source.", "source_id": "event-unknown"}],
            "risks": [
                {
                    "claim": "Unsupported feature.",
                    "computed_feature_id": "feature-unknown",
                }
            ],
            "unresolved_conflicts": [
                {
                    "claim": "Unsupported conflict source.",
                    "source_id": "conflict-unknown",
                }
            ],
        },
        view,
    )

    assert violations == [
        "claims[0].source_id is not in allowed_reference_ids: event-unknown",
        (
            "risks[0].computed_feature_id is not in allowed_computed_feature_ids: "
            "feature-unknown"
        ),
        (
            "unresolved_conflicts[0].source_id is not in allowed_reference_ids: "
            "conflict-unknown"
        ),
    ]


def test_source_faithfulness_rejects_unlinked_claims() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {"claims": [{"claim": "Unsupported claim."}]},
        view,
    )

    assert violations == ["claims[0] must include source_id or computed_feature_id"]


def test_source_faithfulness_rejects_non_string_references() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {
            "claims": [{"claim": "Numeric source.", "source_id": 123}],
            "supporting_points": [
                {
                    "claim": "Nullable source uses a feature.",
                    "source_id": None,
                    "computed_feature_id": "feature-risk-msft",
                }
            ],
            "risks": [{"claim": "Numeric feature.", "computed_feature_id": 456}],
        },
        view,
    )

    assert violations == [
        "claims[0].source_id must be a string",
        "risks[0].computed_feature_id must be a string",
    ]


def test_source_faithfulness_ignores_plain_string_bear_case_items() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {
            "bear_case": [
                "Phase 12 evidence-review payloads may contain plain bear-case strings.",
                {
                    "claim": "Risk penalty is elevated.",
                    "computed_feature_id": "feature-risk-msft",
                },
            ],
        },
        view,
    )

    assert violations == []


def test_source_faithfulness_rejects_non_list_source_link_fields() -> None:
    view = build_agent_evidence_packet(_candidate())

    violations = source_faithfulness_violations(
        {
            "claims": {"claim": "Not a list."},
            "supporting_points": "Not a list.",
            "risks": {"claim": "Not a list."},
            "bear_case": {"claim": "Not a list."},
            "unresolved_conflicts": {"claim": "Not a list."},
        },
        view,
    )

    assert violations == [
        "claims must be a list",
        "supporting_points must be a list",
        "risks must be a list",
        "bear_case must be a list",
        "unresolved_conflicts must be a list",
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
                source_url="https://example.test/msft-guidance",
                source_quality=0.95,
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
        conflicts=({"kind": "source_conflict", "source_id": "conflict-msft"},),
        hard_blocks=("risk_hard_block",),
        payload={},
        source_ts=SOURCE_TS,
        available_at=AVAILABLE_AT,
    )
