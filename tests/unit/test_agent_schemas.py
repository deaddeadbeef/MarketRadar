from __future__ import annotations

from datetime import UTC, datetime

import pytest

from catalyst_radar.agents.schemas import (
    AgentSchemaError,
    validate_evidence_review_output,
)

AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)


def test_validates_source_linked_evidence_review_output() -> None:
    payload = _valid_payload()

    validated = validate_evidence_review_output(
        payload,
        ticker="MSFT",
        as_of=AS_OF,
    )

    assert validated["ticker"] == "MSFT"
    assert validated["as_of"] == AS_OF.isoformat()
    assert validated["claims"][0]["source_id"] == "event-msft"


def test_rejects_claim_without_source_or_computed_feature() -> None:
    payload = _valid_payload()
    del payload["claims"][0]["source_id"]

    with pytest.raises(AgentSchemaError, match="source_id or computed_feature_id"):
        validate_evidence_review_output(payload, ticker="MSFT", as_of=AS_OF)


def test_rejects_wrong_ticker() -> None:
    payload = _valid_payload(ticker="AAPL")

    with pytest.raises(AgentSchemaError, match="ticker"):
        validate_evidence_review_output(payload, ticker="MSFT", as_of=AS_OF)


def test_rejects_non_json_object() -> None:
    with pytest.raises(AgentSchemaError, match="mapping"):
        validate_evidence_review_output(["not", "an", "object"], ticker="MSFT", as_of=AS_OF)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["claim", "evidence_type", "uncertainty_notes"])
def test_rejects_blank_required_claim_text(field: str) -> None:
    payload = _valid_payload()
    payload["claims"][0][field] = " "

    with pytest.raises(AgentSchemaError, match=field):
        validate_evidence_review_output(payload, ticker="MSFT", as_of=AS_OF)


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
        validate_evidence_review_output(payload, ticker="MSFT", as_of=AS_OF)


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
        "bear_case": ["Valuation is extended."],
        "unresolved_conflicts": [],
        "recommended_policy_downgrade": False,
    }
