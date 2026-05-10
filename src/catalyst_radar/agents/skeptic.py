from __future__ import annotations

from datetime import datetime

from catalyst_radar.agents.router import LLMReviewResult, LLMRouter
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.pipeline.candidate_packet import CandidatePacket


def run_skeptic_review(
    *,
    router: LLMRouter,
    candidate: CandidatePacket,
    available_at: datetime,
    dry_run: bool = False,
) -> LLMReviewResult:
    return router.review_candidate(
        task=DEFAULT_TASKS["skeptic_review"],
        candidate=candidate,
        available_at=available_at,
        dry_run=dry_run,
    )


__all__ = ["run_skeptic_review"]
