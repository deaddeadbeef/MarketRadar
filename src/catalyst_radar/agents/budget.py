from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from catalyst_radar.agents.models import LLMSkipReason, LLMTaskName, TokenUsage
from catalyst_radar.agents.tasks import LLMTask
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository


@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: LLMSkipReason | None
    estimated_cost: float
    daily_spend: float
    monthly_spend: float
    task_daily_count: int


class BudgetController:
    def __init__(
        self,
        *,
        config: AppConfig,
        ledger_repo: BudgetLedgerRepository,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.ledger_repo = ledger_repo
        self.now = now or (lambda: datetime.now(UTC))

    def estimate_cost(self, usage: TokenUsage) -> float:
        if not self._has_pricing():
            return 0.0
        input_cost = self.config.llm_input_cost_per_1m or 0.0
        cached_cost = self.config.llm_cached_input_cost_per_1m or 0.0
        output_cost = self.config.llm_output_cost_per_1m or 0.0
        return round(
            (
                (usage.input_tokens * input_cost)
                + (usage.cached_input_tokens * cached_cost)
                + (usage.output_tokens * output_cost)
            )
            / 1_000_000,
            10,
        )

    def allow_llm_call(
        self,
        *,
        task: LLMTask,
        ticker: str | None,
        candidate_state: ActionState | str,
        final_score: float,
        estimated_usage: TokenUsage,
        available_at: datetime,
    ) -> BudgetDecision:
        del ticker, available_at

        if not self.config.enable_premium_llm:
            return self._blocked(LLMSkipReason.PREMIUM_LLM_DISABLED)

        if task.manual_only:
            return self._blocked(LLMSkipReason.MANUAL_TASK_REQUIRES_OPERATOR)

        state = ActionState(candidate_state)
        if state not in task.eligible_states:
            return self._blocked(LLMSkipReason.CANDIDATE_STATE_NOT_ELIGIBLE)

        if not self._model_for_task(task):
            return self._blocked(LLMSkipReason.MODEL_NOT_CONFIGURED)

        if not self._has_pricing():
            return self._blocked(LLMSkipReason.PRICING_MISSING)

        estimated_cost = self.estimate_cost(estimated_usage)
        if self._pricing_is_stale():
            return self._blocked(
                LLMSkipReason.PRICING_STALE,
                estimated_cost=estimated_cost,
            )

        day_start, day_end = self._day_window()
        month_start, month_end = self._month_window()
        task_daily_count = self.ledger_repo.task_count_between(
            task=task.name.value,
            start=day_start,
            end=day_end,
        )
        daily_cap = self.config.llm_task_daily_caps.get(
            task.name.value,
            task.default_daily_cap,
        )
        if task_daily_count >= daily_cap:
            return self._blocked(
                LLMSkipReason.TASK_DAILY_CAP_EXCEEDED,
                estimated_cost=estimated_cost,
                task_daily_count=task_daily_count,
            )

        daily_spend = self.ledger_repo.spend_between(start=day_start, end=day_end)
        if (
            self.config.llm_daily_budget_usd <= 0
            or daily_spend + estimated_cost > self.config.llm_daily_budget_usd
        ):
            return self._blocked(
                LLMSkipReason.DAILY_BUDGET_EXCEEDED,
                estimated_cost=estimated_cost,
                daily_spend=daily_spend,
                task_daily_count=task_daily_count,
            )

        monthly_spend = self.ledger_repo.spend_between(start=month_start, end=month_end)
        if (
            self.config.llm_monthly_budget_usd <= 0
            or monthly_spend + estimated_cost > self.config.llm_monthly_budget_usd
        ):
            return self._blocked(
                LLMSkipReason.MONTHLY_BUDGET_EXCEEDED,
                estimated_cost=estimated_cost,
                daily_spend=daily_spend,
                monthly_spend=monthly_spend,
                task_daily_count=task_daily_count,
            )

        soft_cap = (
            self.config.llm_monthly_budget_usd * self.config.llm_monthly_soft_cap_pct
        )
        if (
            task.name == LLMTaskName.GPT55_DECISION_CARD
            and final_score < 90
            and monthly_spend + estimated_cost > soft_cap
        ):
            return self._blocked(
                LLMSkipReason.MONTHLY_SOFT_CAP_REQUIRES_HIGH_SCORE,
                estimated_cost=estimated_cost,
                daily_spend=daily_spend,
                monthly_spend=monthly_spend,
                task_daily_count=task_daily_count,
            )

        return BudgetDecision(
            allowed=True,
            reason=None,
            estimated_cost=estimated_cost,
            daily_spend=daily_spend,
            monthly_spend=monthly_spend,
            task_daily_count=task_daily_count,
        )

    def _blocked(
        self,
        reason: LLMSkipReason,
        *,
        estimated_cost: float = 0.0,
        daily_spend: float = 0.0,
        monthly_spend: float = 0.0,
        task_daily_count: int = 0,
    ) -> BudgetDecision:
        return BudgetDecision(
            allowed=False,
            reason=reason,
            estimated_cost=estimated_cost,
            daily_spend=daily_spend,
            monthly_spend=monthly_spend,
            task_daily_count=task_daily_count,
        )

    def _has_pricing(self) -> bool:
        return (
            self.config.llm_input_cost_per_1m is not None
            and self.config.llm_cached_input_cost_per_1m is not None
            and self.config.llm_output_cost_per_1m is not None
        )

    def _pricing_is_stale(self) -> bool:
        raw = self.config.llm_pricing_updated_at
        if raw is None:
            return True
        try:
            updated_at = date.fromisoformat(raw)
        except ValueError:
            return True
        age = self._current_date() - updated_at
        return age.days > self.config.llm_pricing_stale_after_days

    def _model_for_task(self, task: LLMTask) -> str | None:
        value = getattr(self.config, task.model_config_key)
        return value if value else None

    def _day_window(self) -> tuple[datetime, datetime]:
        current_date = self._current_date()
        start = datetime.combine(current_date, time.min, tzinfo=UTC)
        return start, start + timedelta(days=1)

    def _month_window(self) -> tuple[datetime, datetime]:
        current_date = self._current_date()
        start = datetime(current_date.year, current_date.month, 1, tzinfo=UTC)
        if current_date.month == 12:
            end = datetime(current_date.year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(current_date.year, current_date.month + 1, 1, tzinfo=UTC)
        return start, end

    def _current_date(self) -> date:
        current = self.now()
        if current.tzinfo is None or current.utcoffset() is None:
            current = current.replace(tzinfo=UTC)
        return current.astimezone(UTC).date()


__all__ = ["BudgetController", "BudgetDecision"]
