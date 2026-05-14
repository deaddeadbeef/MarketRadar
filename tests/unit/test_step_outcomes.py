from catalyst_radar.jobs.step_outcomes import classify_step_outcome


def test_expected_gate_outcomes_explain_trigger_condition() -> None:
    classification = classify_step_outcome(
        "skipped",
        "no_manual_buy_review_inputs",
    )

    assert classification.category == "expected_gate"
    assert classification.blocks_reliance is False
    assert classification.trigger_condition == (
        "At least one candidate must pass policy into manual buy review."
    )
    assert classification.as_metadata()["trigger_condition"] == (
        "At least one candidate must pass policy into manual buy review."
    )


def test_non_gate_outcomes_do_not_report_gate_trigger() -> None:
    classification = classify_step_outcome("success", None)

    assert classification.category == "completed"
    assert classification.trigger_condition is None
    assert classification.as_metadata()["trigger_condition"] is None
