from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

import pytest

from catalyst_radar.agents.evidence import build_agent_evidence_packet
from catalyst_radar.agents.schemas import (
    AgentSchemaError,
    validate_decision_card_draft_output,
    validate_evidence_review_output,
    validate_skeptic_review_output,
)
from catalyst_radar.core.models import ActionState
from catalyst_radar.pipeline.candidate_packet import CandidatePacket, EvidenceItem

AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 8, 21, 5, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 8, 20, 45, tzinfo=UTC)


def test_validates_source_linked_evidence_review_output() -> None:
    payload = _valid_payload()

    validated = validate_evidence_review_output(
        payload,
        ticker="MSFT",
        as_of=AS_OF,
        evidence_packet=_evidence_packet(),
    )

    assert validated["ticker"] == "MSFT"
    assert validated["as_of"] == AS_OF.isoformat()
    assert validated["claims"][0]["source_id"] == "event-msft"


def test_rejects_claim_without_source_or_computed_feature() -> None:
    payload = _valid_payload()
    del payload["claims"][0]["source_id"]

    with pytest.raises(AgentSchemaError, match="source_id or computed_feature_id"):
        _validate_evidence_review(payload)


def test_rejects_evidence_review_unknown_source_reference() -> None:
    payload = _valid_payload()
    payload["claims"][0]["source_id"] = "event-unknown"

    with pytest.raises(AgentSchemaError, match="allowed_reference_ids"):
        _validate_evidence_review(payload)


def test_rejects_evidence_review_plain_bear_case_string() -> None:
    payload = _valid_payload()
    payload["bear_case"] = ["Plain bear-case text is not source-linked."]

    with pytest.raises(AgentSchemaError, match="bear_case\\[0\\] must be a mapping"):
        _validate_evidence_review(payload)


def test_rejects_wrong_ticker() -> None:
    payload = _valid_payload(ticker="AAPL")

    with pytest.raises(AgentSchemaError, match="ticker"):
        _validate_evidence_review(payload)


def test_rejects_non_json_object() -> None:
    with pytest.raises(AgentSchemaError, match="mapping"):
        _validate_evidence_review(["not", "an", "object"])  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["claim", "evidence_type", "uncertainty_notes"])
def test_rejects_blank_required_claim_text(field: str) -> None:
    payload = _valid_payload()
    payload["claims"][0][field] = " "

    with pytest.raises(AgentSchemaError, match=field):
        _validate_evidence_review(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("claim", 123),
        ("evidence_type", ["filing"]),
        ("uncertainty_notes", {"note": "weak"}),
    ],
)
def test_rejects_non_string_required_claim_text(field: str, value: object) -> None:
    payload = _valid_payload()
    payload["claims"][0][field] = value

    with pytest.raises(AgentSchemaError, match=f"{field}.*string"):
        _validate_evidence_review(payload)


def test_rejects_non_string_claim_source_reference() -> None:
    payload = _valid_payload()
    payload["claims"][0]["source_id"] = 123

    with pytest.raises(AgentSchemaError, match="source_id.*string"):
        _validate_evidence_review(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_quality", -0.1),
        ("source_quality", 1.1),
        ("confidence", -0.1),
        ("confidence", 1.1),
        ("sentiment", -1.1),
        ("sentiment", 1.1),
    ],
)
def test_rejects_out_of_range_claim_numbers(field: str, value: float) -> None:
    payload = _valid_payload()
    payload["claims"][0][field] = value

    with pytest.raises(AgentSchemaError, match=field):
        _validate_evidence_review(payload)


def test_validates_source_linked_skeptic_review_output() -> None:
    payload = _valid_skeptic_payload()

    validated = validate_skeptic_review_output(
        payload,
        ticker="MSFT",
        as_of=AS_OF,
        evidence_packet=_evidence_packet(),
    )

    assert validated["schema_version"] == "skeptic-review-v1"
    assert validated["ticker"] == "MSFT"
    assert validated["as_of"] == AS_OF.isoformat()
    assert validated["bear_case"][0]["computed_feature_id"] == "feature-risk-msft"
    assert validated["bear_case"][0]["confidence"] == 0.74


def test_rejects_skeptic_review_unknown_source_reference() -> None:
    payload = _valid_skeptic_payload()
    payload["bear_case"][0]["source_id"] = "event-unknown"
    del payload["bear_case"][0]["computed_feature_id"]

    with pytest.raises(AgentSchemaError, match="allowed_reference_ids"):
        validate_skeptic_review_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_skeptic_review_forbidden_execution_language() -> None:
    payload = _valid_skeptic_payload()
    payload["manual_review_notes"] = "Human reviewer should buy now."

    with pytest.raises(AgentSchemaError, match="forbidden execution wording"):
        validate_skeptic_review_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ticker", "AAPL"),
        ("as_of", "2026-05-08T22:00:00+00:00"),
        ("schema_version", "evidence-review-v1"),
    ],
)
def test_rejects_skeptic_review_envelope_mismatch(
    field: str,
    value: object,
) -> None:
    payload = _valid_skeptic_payload()
    payload[field] = value

    with pytest.raises(AgentSchemaError, match=field):
        validate_skeptic_review_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_skeptic_review_out_of_range_confidence() -> None:
    payload = _valid_skeptic_payload()
    payload["bear_case"][0]["confidence"] = 1.1

    with pytest.raises(AgentSchemaError, match="confidence"):
        validate_skeptic_review_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_validates_source_linked_decision_card_draft_output() -> None:
    payload = _valid_decision_card_draft_payload()

    validated = validate_decision_card_draft_output(
        payload,
        ticker="MSFT",
        as_of=AS_OF,
        evidence_packet=_evidence_packet(),
    )

    assert validated["schema_version"] == "decision-card-v1"
    assert validated["ticker"] == "MSFT"
    assert validated["as_of"] == AS_OF.isoformat()
    assert validated["supporting_points"][0]["source_id"] == "event-msft"
    assert validated["risks"][0]["computed_feature_id"] == "feature-risk-msft"
    assert validated["manual_review_only"] is True


def test_rejects_decision_card_draft_that_is_not_manual_review_only() -> None:
    payload = _valid_decision_card_draft_payload()
    payload["manual_review_only"] = False

    with pytest.raises(AgentSchemaError, match="manual_review_only"):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_decision_card_draft_unknown_feature_reference() -> None:
    payload = _valid_decision_card_draft_payload()
    payload["risks"][0]["computed_feature_id"] = "feature-unknown"

    with pytest.raises(AgentSchemaError, match="allowed_computed_feature_ids"):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_decision_card_draft_forbidden_execution_language() -> None:
    payload = _valid_decision_card_draft_payload()
    payload["summary"] = "Manual reviewer should place order after review."

    with pytest.raises(AgentSchemaError, match="forbidden execution wording"):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ticker", "AAPL"),
        ("as_of", "2026-05-08T22:00:00+00:00"),
        ("schema_version", "skeptic-review-v1"),
    ],
)
def test_rejects_decision_card_draft_envelope_mismatch(
    field: str,
    value: object,
) -> None:
    payload = _valid_decision_card_draft_payload()
    payload[field] = value

    with pytest.raises(AgentSchemaError, match=field):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_decision_card_draft_out_of_range_confidence() -> None:
    payload = _valid_decision_card_draft_payload()
    payload["risks"][0]["confidence"] = -0.1

    with pytest.raises(AgentSchemaError, match="confidence"):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def test_rejects_decision_card_draft_non_string_narrative_text() -> None:
    payload = _valid_decision_card_draft_payload()
    payload["supporting_points"][0]["text"] = 123

    with pytest.raises(AgentSchemaError, match="text.*string"):
        validate_decision_card_draft_output(
            payload,
            ticker="MSFT",
            as_of=AS_OF,
            evidence_packet=_evidence_packet(),
        )


def _valid_payload(*, ticker: str = "MSFT") -> dict[str, object]:
    return {
        "ticker": ticker,
        "as_of": AS_OF.isoformat(),
        "claims": [
            {
                "claim": "Revenue guide was raised.",
                "source_id": "event-msft",
                "source_quality": 0.9,
                "evidence_type": "filing",
                "sentiment": 0.6,
                "confidence": 0.8,
                "uncertainty_notes": "Needs follow-up on margin pressure.",
            }
        ],
        "bear_case": [
            {
                "claim": "Risk penalty remains elevated.",
                "computed_feature_id": "feature-risk-msft",
                "confidence": 0.6,
            }
        ],
        "unresolved_conflicts": [
            {
                "claim": "Source conflict remains unresolved.",
                "source_id": "event-msft",
                "confidence": 0.4,
            }
        ],
        "recommended_policy_downgrade": False,
    }


def _valid_skeptic_payload(*, ticker: str = "MSFT") -> dict[str, object]:
    return {
        "ticker": ticker,
        "as_of": AS_OF.isoformat(),
        "schema_version": "skeptic-review-v1",
        "bear_case": [
            {
                "claim": "Risk penalty remains elevated.",
                "computed_feature_id": "feature-risk-msft",
                "severity": "medium",
                "confidence": 0.74,
                "why_it_matters": "It can reduce margin of safety.",
            }
        ],
        "missing_evidence": ["No updated margin guidance was present."],
        "contradictions": [],
        "recommended_policy_downgrade": False,
        "manual_review_notes": "Human reviewer should inspect valuation.",
    }


def _valid_decision_card_draft_payload(*, ticker: str = "MSFT") -> dict[str, object]:
    return {
        "ticker": ticker,
        "as_of": AS_OF.isoformat(),
        "schema_version": "decision-card-v1",
        "summary": "Manual-review setup with evidence-backed catalyst.",
        "supporting_points": [
            {
                "text": "Cloud guidance was raised.",
                "source_id": "event-msft",
            }
        ],
        "risks": [
            {
                "text": "Risk penalty remains non-zero.",
                "computed_feature_id": "feature-risk-msft",
            }
        ],
        "questions_for_human": ["Is catalyst durability confirmed?"],
        "manual_review_only": True,
    }


def _evidence_packet() -> dict[str, object]:
    return dict(build_agent_evidence_packet(_candidate()))


def _validate_evidence_review(payload: object) -> Mapping[str, object]:
    return validate_evidence_review_output(
        payload,  # type: ignore[arg-type]
        ticker="MSFT",
        as_of=AS_OF,
        evidence_packet=_evidence_packet(),
    )


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
