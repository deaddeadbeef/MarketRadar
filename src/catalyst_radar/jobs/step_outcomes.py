from __future__ import annotations

from dataclasses import dataclass

BLOCKING_SKIP_REASONS = frozenset(
    {
        "blocked_by_failed_dependency:daily_bar_ingest",
        "blocked_by_failed_dependency:event_ingest",
        "blocked_by_failed_dependency:feature_scan",
        "blocked_by_failed_dependency:scoring_policy",
        "blocked_by_failed_dependency:candidate_packets",
        "degraded_mode_blocks_high_state_work",
        "degraded_mode_blocks_decision_cards",
        "degraded_mode_blocks_llm_review",
        "no_scheduled_provider_input",
        "scheduled_provider_not_supported",
        "scheduled_event_provider_not_supported",
        "no_scheduled_event_provider",
    }
)
EXPECTED_GATE_REASONS = frozenset(
    {
        "llm_disabled",
        "no_alerts",
        "no_llm_review_inputs",
        "no_manual_buy_review_inputs",
        "outcome_available_at_not_supplied",
    }
)
NOT_READY_REASONS = frozenset(
    {
        "no_active_securities",
        "no_candidate_inputs",
        "no_candidate_packets",
        "no_current_scan_results",
        "no_feature_inputs",
        "no_sec_cik_targets",
        "no_text_inputs",
        "no_warning_or_higher_candidates",
    }
)

SKIP_EXPLANATIONS = {
    "blocked_by_failed_dependency:daily_bar_ingest": (
        "Daily market ingestion did not complete, so this dependent step did not run."
    ),
    "blocked_by_failed_dependency:event_ingest": (
        "Event ingestion did not complete, so text triage did not run."
    ),
    "blocked_by_failed_dependency:feature_scan": (
        "Feature scanning did not complete, so this dependent step did not run."
    ),
    "blocked_by_failed_dependency:scoring_policy": (
        "Scoring did not complete, so candidate packets were not built."
    ),
    "blocked_by_failed_dependency:candidate_packets": (
        "Candidate packets did not complete, so this dependent step did not run."
    ),
    "degraded_mode_blocks_high_state_work": (
        "Degraded mode blocked high-state research work because current data is not trusted."
    ),
    "degraded_mode_blocks_decision_cards": (
        "Degraded mode blocked Decision Cards because current data is not trusted."
    ),
    "degraded_mode_blocks_llm_review": (
        "Degraded mode blocked LLM review because current data is not trusted."
    ),
    "llm_disabled": "LLM review was not requested for this run.",
    "no_active_securities": "No active securities were available for scanning.",
    "no_alerts": "No existing alerts were available for the digest step.",
    "no_candidate_inputs": "No scan results were available for scoring.",
    "no_candidate_packets": "No candidate packets were available for Decision Cards.",
    "no_current_scan_results": "No current scan results were available for packet building.",
    "no_feature_inputs": "No signal inputs were available for feature scanning.",
    "no_llm_review_inputs": "There were no Decision Cards for LLM review.",
    "no_manual_buy_review_inputs": "No candidate crossed the manual buy-review gate.",
    "no_scheduled_event_provider": "No news/event provider was scheduled for this run.",
    "no_scheduled_provider_input": "No market-data provider was scheduled for this run.",
    "no_sec_cik_targets": "No active securities had CIK metadata for SEC submission checks.",
    "no_text_inputs": "No text or news inputs were available to triage.",
    "no_warning_or_higher_candidates": "No candidates crossed the warning threshold.",
    "outcome_available_at_not_supplied": (
        "Outcome validation needs a later outcome cutoff and is expected to skip "
        "during live triage."
    ),
    "scheduled_event_provider_not_supported": (
        "The scheduled event provider is not wired into dashboard runs yet."
    ),
    "scheduled_provider_not_supported": (
        "The scheduled market provider is not wired into dashboard runs yet."
    ),
}

OPERATOR_ACTIONS = {
    "blocked_input": "Resolve the upstream data/provider issue before relying on this run.",
    "expected_gate": "No action required unless you want this optional gate to run.",
    "failed": "Inspect the step error and rerun after fixing it.",
    "not_ready": "Add the missing input or tune the scan thresholds if this is too conservative.",
}


@dataclass(frozen=True)
class StepOutcomeClassification:
    category: str
    label: str
    meaning: str | None = None
    operator_action: str | None = None
    blocks_reliance: bool = False

    def as_metadata(self) -> dict[str, object]:
        return {
            "outcome_category": self.category,
            "outcome_label": self.label,
            "outcome_meaning": self.meaning,
            "operator_action": self.operator_action,
            "blocks_reliance": self.blocks_reliance,
        }


def classify_step_outcome(
    status: str | None,
    reason: str | None,
) -> StepOutcomeClassification:
    status_text = (status or "").strip().lower()
    reason_text = (reason or "").strip()
    meaning = SKIP_EXPLANATIONS.get(reason_text)
    if status_text == "success":
        return StepOutcomeClassification(category="completed", label="Completed")
    if status_text == "failed":
        return StepOutcomeClassification(
            category="failed",
            label="Failed",
            meaning=meaning,
            operator_action=OPERATOR_ACTIONS["failed"],
            blocks_reliance=True,
        )
    if status_text == "running":
        return StepOutcomeClassification(category="running", label="Running")
    if reason_text in BLOCKING_SKIP_REASONS:
        return StepOutcomeClassification(
            category="blocked_input",
            label="Blocked input",
            meaning=meaning,
            operator_action=OPERATOR_ACTIONS["blocked_input"],
            blocks_reliance=True,
        )
    if reason_text in EXPECTED_GATE_REASONS:
        return StepOutcomeClassification(
            category="expected_gate",
            label="Expected gate",
            meaning=meaning,
            operator_action=OPERATOR_ACTIONS["expected_gate"],
        )
    if reason_text in NOT_READY_REASONS:
        return StepOutcomeClassification(
            category="not_ready",
            label="Not ready",
            meaning=meaning,
            operator_action=OPERATOR_ACTIONS["not_ready"],
        )
    if status_text == "skipped":
        return StepOutcomeClassification(
            category="needs_review",
            label="Needs review",
            meaning=meaning,
            operator_action="Review the raw reason and upstream step telemetry.",
            blocks_reliance=True,
        )
    return StepOutcomeClassification(
        category=status_text or "unknown",
        label=(status_text or "unknown").replace("_", " ").title(),
        meaning=meaning,
    )


__all__ = [
    "BLOCKING_SKIP_REASONS",
    "EXPECTED_GATE_REASONS",
    "NOT_READY_REASONS",
    "SKIP_EXPLANATIONS",
    "StepOutcomeClassification",
    "classify_step_outcome",
]
