from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, time, timedelta
from typing import Any

from pydantic import BaseModel, Field

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMTaskName,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.redaction import redact_text, redact_value
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository

SCHEMA_VERSION = "market-radar-agent-brief-v1"
SNAPSHOT_SCHEMA_VERSION = "market-radar-agent-snapshot-v1"
AGENT_SDK_MAX_TURNS = 6
AGENT_BRIEF_PROMPT_VERSION = "agent-brief-real-v1"
AGENT_BRIEF_DEFAULT_DAILY_CAP = 1
DEFAULT_AGENT_MAX_OPENAI_CALLS = 3
AGENT_BRIEF_BASE_ESTIMATED_USAGE = TokenUsage(input_tokens=20_000, output_tokens=4_000)
NON_OPENAI_ASSISTANT_DEPENDENCY_KEY = "co" + "pilot_dependency"

ALLOWED_OPERATIONS = [
    "Read the redacted dashboard snapshot supplied by MarketRadar.",
    "Reason over readiness, candidates, alerts, broker context, and call-plan rows.",
    "Produce manual-review next actions and risk checks for a human operator.",
    "Use OpenAI Agents SDK specialist agents only when real mode is explicitly enabled.",
]

BLOCKED_OPERATIONS = [
    "No Polygon/Massive, SEC, Schwab, broker, shell, filesystem, or web tools.",
    "No order submission, order modification, or autonomous trading.",
    "No hidden provider refresh; stale data must be reported as stale.",
    "No investment advice beyond manual research triage.",
    "No raw provider payload export or secret echoing.",
]


class SecurityCheck(BaseModel):
    name: str
    status: str
    detail: str


class AgentContribution(BaseModel):
    agent: str
    role: str
    summary: str
    confidence: str = "medium"


class MarketRadarAgentBrief(BaseModel):
    schema_version: str = SCHEMA_VERSION
    mode: str
    status: str
    decision_boundary: str
    runtime: dict[str, object] = Field(default_factory=dict)
    real_results: dict[str, object] = Field(default_factory=dict)
    credit_gate: dict[str, object] = Field(default_factory=dict)
    agents: list[AgentContribution] = Field(default_factory=list)
    insights: list[str] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    security_checks: list[SecurityCheck] = Field(default_factory=list)
    allowed_operations: list[str] = Field(default_factory=list)
    blocked_operations: list[str] = Field(default_factory=list)
    external_calls_made: dict[str, int] = Field(default_factory=dict)


def agent_sdk_gate_payload(config: AppConfig) -> dict[str, object]:
    """Return the explicit gate that must pass before any Agents SDK call."""
    missing: list[str] = []
    if not config.enable_agent_sdk:
        missing.append("CATALYST_ENABLE_AGENT_SDK=true")
    if not config.enable_premium_llm:
        missing.append("CATALYST_ENABLE_PREMIUM_LLM=true")
    if str(config.llm_provider or "").strip().lower() != "openai":
        missing.append("CATALYST_LLM_PROVIDER=openai")
    if not config.agent_sdk_model:
        missing.append("CATALYST_AGENT_SDK_MODEL")
    if not config.openai_api_key:
        missing.append("OPENAI_API_KEY")

    missing = list(dict.fromkeys(missing))
    ready = not missing
    return {
        "schema_version": "agent-sdk-real-mode-gate-v1",
        "status": "ready" if ready else "blocked",
        "headline": (
            "OpenAI Agents SDK real mode is configured."
            if ready
            else "OpenAI Agents SDK real mode is blocked until explicit gates are set."
        ),
        "next_action": (
            "Run a real agent brief only after reviewing the redacted snapshot."
            if ready
            else "Set the listed gates, then rerun agent-brief --real."
        ),
        "missing_env": missing,
        "provider": str(config.llm_provider or "none"),
        "agent_sdk_enabled": config.enable_agent_sdk,
        "premium_llm_enabled": config.enable_premium_llm,
        "model_configured": bool(config.agent_sdk_model),
        "openai_key_configured": bool(config.openai_api_key),
        "max_turns": AGENT_SDK_MAX_TURNS,
        "tool_surface": "specialist_agents_only",
    }


def agent_sdk_credit_gate_payload(
    config: AppConfig,
    *,
    ledger_repo: BudgetLedgerRepository | None,
    max_openai_calls: int = DEFAULT_AGENT_MAX_OPENAI_CALLS,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return the credit/budget gate for explicit real Agents SDK execution."""
    checked_at = _aware_utc(now or datetime.now(UTC))
    max_calls = _positive_int(max_openai_calls, DEFAULT_AGENT_MAX_OPENAI_CALLS)
    estimated_usage = _agent_estimated_usage(max_calls)
    estimated_cost = 0.0
    daily_spend = 0.0
    monthly_spend = 0.0
    task_daily_count = 0
    missing: list[str] = []

    if ledger_repo is None:
        missing.append("budget ledger")
    if not _has_llm_pricing(config):
        missing.extend(
            [
                "CATALYST_LLM_INPUT_COST_PER_1M",
                "CATALYST_LLM_CACHED_INPUT_COST_PER_1M",
                "CATALYST_LLM_OUTPUT_COST_PER_1M",
            ]
        )
    if config.llm_pricing_updated_at is None:
        missing.append("CATALYST_LLM_PRICING_UPDATED_AT")
    elif _pricing_is_stale(config, checked_at):
        missing.append("fresh CATALYST_LLM_PRICING_UPDATED_AT")
    if config.llm_daily_budget_usd <= 0:
        missing.append("CATALYST_LLM_DAILY_BUDGET_USD>0")
    if config.llm_monthly_budget_usd <= 0:
        missing.append("CATALYST_LLM_MONTHLY_BUDGET_USD>0")

    if ledger_repo is not None:
        controller = BudgetController(config=config, ledger_repo=ledger_repo)
        estimated_cost = controller.estimate_cost(estimated_usage)
        day_start, day_end = _day_window(checked_at)
        month_start, month_end = _month_window(checked_at)
        daily_spend = ledger_repo.spend_between(start=day_start, end=day_end)
        monthly_spend = ledger_repo.spend_between(start=month_start, end=month_end)
        task_daily_count = ledger_repo.task_count_between(
            task=LLMTaskName.AGENT_BRIEF.value,
            start=day_start,
            end=day_end,
        )

    daily_cap = int(
        config.llm_task_daily_caps.get(
            LLMTaskName.AGENT_BRIEF.value,
            AGENT_BRIEF_DEFAULT_DAILY_CAP,
        )
    )
    if daily_cap <= 0:
        missing.append("CATALYST_LLM_TASK_DAILY_CAPS agent_brief>0")
    elif task_daily_count >= daily_cap:
        missing.append("agent_brief daily cap remaining")
    if (
        config.llm_daily_budget_usd > 0
        and daily_spend + estimated_cost > config.llm_daily_budget_usd
    ):
        missing.append("daily OpenAI budget remaining")
    if (
        config.llm_monthly_budget_usd > 0
        and monthly_spend + estimated_cost > config.llm_monthly_budget_usd
    ):
        missing.append("monthly OpenAI budget remaining")

    missing = list(dict.fromkeys(missing))
    ready = not missing
    return {
        "schema_version": "agent-sdk-credit-gate-v1",
        "status": "ready" if ready else "blocked",
        "headline": (
            "OpenAI spend gate is ready for one explicit agent execution."
            if ready
            else "OpenAI spend gate blocks execution until pricing and budgets are explicit."
        ),
        "next_action": (
            "Add --execute only when the estimated cost and daily/monthly budgets "
            "match your intent."
            if ready
            else "Fill the missing env/budget values, then rerun the preview."
        ),
        "missing": missing,
        "estimated_usage": _token_usage_payload(estimated_usage),
        "estimated_cost_usd": estimated_cost,
        "daily_budget_usd": config.llm_daily_budget_usd,
        "daily_spend_usd": daily_spend,
        "monthly_budget_usd": config.llm_monthly_budget_usd,
        "monthly_spend_usd": monthly_spend,
        "task_daily_cap": daily_cap,
        "task_daily_count": task_daily_count,
        "max_openai_calls": max_calls,
        "pricing_updated_at": config.llm_pricing_updated_at,
        "checked_at": checked_at.isoformat(),
    }


def real_results_gate_payload(snapshot: Mapping[str, object]) -> dict[str, object]:
    """Return whether the supplied snapshot is backed by real scan rows."""
    real_results = _mapping(snapshot.get("real_results"))
    missing = _texts(real_results.get("missing"))
    status = _text(real_results.get("status"))
    ready = status == "ready" and not missing
    if not status:
        missing.append("real_results snapshot marker")
    missing = list(dict.fromkeys(missing))
    return {
        "schema_version": "agent-real-results-gate-v1",
        "status": "ready" if ready else "blocked",
        "headline": (
            _text(real_results.get("headline"))
            or (
                "Real scan results are ready for agent review."
                if ready
                else "Run a real scan before executing an agent brief."
            )
        ),
        "next_action": (
            _text(real_results.get("next_action"))
            or (
                "Review the scan rows, then execute the agent brief if desired."
                if ready
                else "Run the full scan/import flow before using real Agents SDK mode."
            )
        ),
        "missing": missing,
        "source": _text(real_results.get("source")) or "unknown",
        "row_count": int(_number(real_results.get("row_count"))),
        "latest_run_id": _text(real_results.get("latest_run_id")) or None,
        "latest_run_status": _text(real_results.get("latest_run_status")) or None,
        "as_of": _text(real_results.get("as_of")) or None,
        "cutoff": _text(real_results.get("cutoff")) or None,
    }


def redacted_operator_snapshot(payload: Mapping[str, object]) -> dict[str, object]:
    """Shrink the dashboard snapshot to fields safe enough for model input."""
    source = redact_value(dict(payload))
    snapshot = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "controls": _copy_keys(
            _mapping(source.get("controls")),
            ("ticker", "available_at", "alert_status", "alert_route"),
        ),
        "runtime": _runtime_context(_mapping(source.get("runtime_context"))),
        "readiness": _readiness_context(_mapping(source.get("readiness"))),
        "real_results": _real_results_context(_mapping(source.get("real_results"))),
        "operator_next_step": _copy_keys(
            _mapping(source.get("operator_next_step")),
            ("status", "priority", "area", "item", "ticker", "action", "evidence", "source"),
        ),
        "operator_work_queue": _work_queue_context(
            _mapping(source.get("operator_work_queue"))
        ),
        "call_plan": _call_plan_context(_mapping(source.get("call_plan"))),
        "priced_in": _priced_in_context(
            _mapping(source.get("priced_in_queue")),
            _mapping(source.get("priced_in_source_coverage")),
            _mapping(source.get("priced_in_source_workflow")),
            _mapping(source.get("priced_in_preflight")),
            _mapping(source.get("priced_in_answer")),
            _mapping(source.get("priced_in_audit")),
        ),
        "candidates": _candidates_context(_mapping(source.get("candidates"))),
        "alerts": _alerts_context(_mapping(source.get("alerts"))),
        "broker": _broker_context(_mapping(source.get("broker"))),
        "ops": _ops_context(_mapping(source.get("ops_health"))),
        "telemetry": _telemetry_context(_mapping(source.get("telemetry"))),
        "external_calls_made": int(_number(source.get("external_calls_made"))),
    }
    return _strip_sensitive_keys(redact_value(snapshot))


def deterministic_agent_brief(
    snapshot: Mapping[str, object],
    gate: Mapping[str, object],
    *,
    mode: str = "dry_run",
    operator_goal: str | None = None,
    issue: str | None = None,
) -> dict[str, object]:
    """Return the no-model multi-agent brief used by tests and default CLI runs."""
    status = mode if mode in {"blocked", "preview"} else "dry_run"
    readiness = _mapping(snapshot.get("readiness"))
    real_results = _mapping(snapshot.get("real_results"))
    credit_gate = _mapping(gate.get("credit_gate"))
    call_plan = _mapping(snapshot.get("call_plan"))
    work_queue = _mapping(snapshot.get("operator_work_queue"))
    priced_in = _mapping(snapshot.get("priced_in"))
    candidates = _rows(_mapping(snapshot.get("candidates")).get("rows"))
    alerts = _rows(_mapping(snapshot.get("alerts")).get("rows"))
    next_step = _mapping(snapshot.get("operator_next_step"))
    max_provider_calls = int(_number(call_plan.get("max_external_call_count")))
    recommended_unblock_actions = _priced_in_recommended_unblock_actions(priced_in)
    answer_context = _mapping(priced_in.get("answer"))
    setup_blocker = _mapping(priced_in.get("setup_blocker"))
    setup_actions = [
        _text(setup_blocker.get("action")),
        _text(setup_blocker.get("command")),
    ]
    has_current_blocker_action = bool(
        recommended_unblock_actions or any(setup_actions)
    )
    source_workflow_actions = (
        []
        if has_current_blocker_action
        else [
            _text(_mapping(priced_in.get("source_workflow")).get("coverage_first_action")),
            _text(_mapping(priced_in.get("source_workflow")).get("coverage_first_command")),
            _text(_mapping(priced_in.get("source_workflow")).get("decision_shortcut_action")),
            _text(_mapping(priced_in.get("source_workflow")).get("decision_shortcut_command")),
        ]
    )

    agents = [
        AgentContribution(
            agent="Data Sentinel",
            role="Data freshness and provider-call boundary",
            summary=_data_sentinel_summary(readiness, call_plan),
            confidence="high",
        ),
        AgentContribution(
            agent="Catalyst Analyst",
            role="Candidate and alert triage",
            summary=_catalyst_summary(candidates, alerts),
            confidence="medium",
        ),
        AgentContribution(
            agent="Risk Officer",
            role="Actionability, portfolio, and order-safety checks",
            summary=_risk_summary(snapshot),
            confidence="high",
        ),
        AgentContribution(
            agent="Operator",
            role="Human next action",
            summary=_operator_summary(
                (
                    {"action": recommended_unblock_actions[0]}
                    if recommended_unblock_actions
                    else {"action": setup_actions[0]}
                    if setup_actions[0]
                    else next_step
                ),
                operator_goal,
            ),
            confidence="high",
        ),
    ]

    insights = _dedupe(
        [
            _priced_in_answer_insight(priced_in),
            _priced_in_recommended_unblock_insight(priced_in),
            _priced_in_unblock_options_insight(priced_in),
            None if has_current_blocker_action else _status_insight(readiness),
            _call_plan_insight(call_plan),
            _priced_in_insight(priced_in),
            _priced_in_evidence_plan_insight(priced_in),
            _priced_in_source_workflow_insight(priced_in),
            None if has_current_blocker_action else _top_queue_insight(work_queue),
            *[_candidate_insight(row) for row in candidates[:3]],
            *[_alert_insight(row) for row in alerts[:2]],
            issue,
        ]
    )
    next_actions = _dedupe(
        [
            *recommended_unblock_actions,
            *(
                []
                if has_current_blocker_action and not recommended_unblock_actions
                else _priced_in_unblock_option_actions(priced_in)
            ),
            *setup_actions,
            None if has_current_blocker_action else _text(next_step.get("action")),
            _text(answer_context.get("next_action")),
            _text(answer_context.get("next_command")),
            _text(_mapping(priced_in.get("evidence_plan")).get("next_action")),
            _text(_mapping(priced_in.get("evidence_plan")).get("next_command")),
            *source_workflow_actions,
            _text(priced_in.get("next_action")),
            None if has_current_blocker_action else _text(work_queue.get("next_action")),
            _text(call_plan.get("next_action")) if max_provider_calls else None,
            "Keep order submission disabled; use broker context as read-only evidence.",
        ]
    )

    checks = _base_security_checks(snapshot, gate, mode=mode)
    if issue:
        checks.append(
            SecurityCheck(
                name="Agent runtime",
                status="blocked",
                detail=redact_text(issue),
            )
        )

    brief = MarketRadarAgentBrief(
        mode=mode,
        status=status,
        decision_boundary=_decision_boundary(snapshot),
        runtime=_agent_runtime_payload(gate, mode=mode),
        real_results=dict(real_results),
        credit_gate=dict(credit_gate),
        agents=agents,
        insights=insights,
        next_actions=next_actions,
        security_checks=checks,
        allowed_operations=list(ALLOWED_OPERATIONS),
        blocked_operations=list(BLOCKED_OPERATIONS),
        external_calls_made={"openai": 0, "market_data": 0, "broker": 0},
    )
    return _model_dump(brief)


def run_market_radar_agents(
    payload: Mapping[str, object],
    config: AppConfig,
    *,
    real: bool = False,
    operator_goal: str | None = None,
    execute: bool = False,
    max_openai_calls: int = DEFAULT_AGENT_MAX_OPENAI_CALLS,
    ledger_repo: BudgetLedgerRepository | None = None,
) -> dict[str, object]:
    gate = agent_sdk_gate_payload(config)
    snapshot = redacted_operator_snapshot(payload)
    real_results_gate = real_results_gate_payload(snapshot)
    credit_gate = agent_sdk_credit_gate_payload(
        config,
        ledger_repo=ledger_repo,
        max_openai_calls=max_openai_calls,
    )
    gate = {
        **gate,
        "real_results_gate": real_results_gate,
        "credit_gate": credit_gate,
    }
    if not real:
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="dry_run",
            operator_goal=operator_goal,
        )
    if not execute:
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="preview",
            operator_goal=operator_goal,
            issue=(
                "Preview only: this made 0 OpenAI calls. Add --execute after "
                "reviewing real results and the credit gate."
            ),
        )
    block_reason = _agent_execute_block_reason(gate)
    if block_reason:
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="blocked",
            operator_goal=operator_goal,
            issue=block_reason,
        )
    if gate["status"] != "ready":
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="blocked",
            operator_goal=operator_goal,
        )
    try:
        brief = _run_agent_sdk_real(
            snapshot,
            gate,
            config,
            operator_goal=operator_goal,
            max_openai_calls=max_openai_calls,
        )
        _record_agent_ledger_entry(
            snapshot=snapshot,
            gate=gate,
            brief=brief,
            config=config,
            ledger_repo=ledger_repo,
            status=LLMCallStatus.COMPLETED,
        )
        return brief
    except Exception as exc:  # pragma: no cover - exercised through integration boundary.
        brief = deterministic_agent_brief(
            snapshot,
            gate,
            mode="blocked",
            operator_goal=operator_goal,
            issue=f"OpenAI Agents SDK run failed closed: {exc}",
        )
        _record_agent_ledger_entry(
            snapshot=snapshot,
            gate=gate,
            brief=brief,
            config=config,
            ledger_repo=ledger_repo,
            status=LLMCallStatus.FAILED,
        )
        return brief


def _run_agent_sdk_real(
    snapshot: Mapping[str, object],
    gate: Mapping[str, object],
    config: AppConfig,
    *,
    operator_goal: str | None = None,
    max_openai_calls: int = DEFAULT_AGENT_MAX_OPENAI_CALLS,
) -> dict[str, object]:
    try:
        from agents import Agent, Runner
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on local env.
        raise RuntimeError("openai-agents package is not installed") from exc

    model = config.agent_sdk_model
    data_agent = Agent(
        name="Data Sentinel",
        model=model,
        instructions=(
            "You inspect only the supplied redacted MarketRadar snapshot. "
            "Report data freshness, stale bars, provider-call budgets, and missing inputs. "
            "Do not ask for or call external data."
        ),
    )
    catalyst_agent = Agent(
        name="Catalyst Analyst",
        model=model,
        instructions=(
            "You inspect candidates and alerts from the supplied snapshot. "
            "Identify what is worth human research and what evidence is missing. "
            "Do not recommend trades."
        ),
    )
    risk_agent = Agent(
        name="Risk Officer",
        model=model,
        instructions=(
            "You enforce MarketRadar safety boundaries. Check hard blocks, portfolio context, "
            "order safety, and whether the output stays manual-review only."
        ),
    )

    operator_agent = Agent(
        name="MarketRadar Operator",
        model=model,
        output_type=MarketRadarAgentBrief,
        instructions=(
            "You are the MarketRadar manager agent. You may use only the specialist agents "
            "provided as tools. You have no tools for Polygon/Massive, SEC, Schwab, shell, "
            "files, web browsing, or order submission. Use the supplied redacted snapshot as "
            "the only market evidence. Return a structured brief for a human operator. "
            "Do not provide investment advice, do not say buy/sell/hold, and do not imply "
            "that stale or blocked data is actionable."
        ),
        tools=[
            data_agent.as_tool(
                tool_name="data_sentinel",
                tool_description="Review data freshness, readiness, and provider-call budget.",
            ),
            catalyst_agent.as_tool(
                tool_name="catalyst_analyst",
                tool_description="Review candidate and alert triage from the redacted snapshot.",
            ),
            risk_agent.as_tool(
                tool_name="risk_officer",
                tool_description="Review actionability, portfolio context, and order safety.",
            ),
        ],
    )
    prompt = json.dumps(
        {
            "operator_goal": operator_goal or "Tell me what matters and what I should do next.",
            "security_contract": {
                "allowed_operations": ALLOWED_OPERATIONS,
                "blocked_operations": BLOCKED_OPERATIONS,
                "max_agent_turns": min(AGENT_SDK_MAX_TURNS, max_openai_calls),
                "external_market_calls_allowed": False,
                "broker_calls_allowed": False,
            },
            "gate": gate,
            "snapshot": snapshot,
        },
        sort_keys=True,
        default=str,
    )
    result = Runner.run_sync(
        operator_agent,
        prompt,
        max_turns=min(AGENT_SDK_MAX_TURNS, max_openai_calls),
    )
    brief = _coerce_brief(result.final_output)
    openai_calls = max(1, len(getattr(result, "raw_responses", []) or []))
    payload = _model_dump(brief)
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "mode": "real",
            "status": "completed",
            "decision_boundary": _decision_boundary(snapshot),
            "runtime": _agent_runtime_payload(gate, mode="real"),
            "real_results": dict(_mapping(snapshot.get("real_results"))),
            "credit_gate": dict(_mapping(gate.get("credit_gate"))),
            "allowed_operations": list(ALLOWED_OPERATIONS),
            "blocked_operations": list(BLOCKED_OPERATIONS),
            "external_calls_made": {
                "openai": openai_calls,
                "market_data": 0,
                "broker": 0,
            },
        }
    )
    checks = [
        SecurityCheck(**row)
        if isinstance(row, Mapping)
        else row
        for row in payload.get("security_checks", [])
    ]
    checks.extend(_base_security_checks(snapshot, gate, mode="real"))
    payload["security_checks"] = [_model_dump(row) for row in checks]
    return _model_dump(MarketRadarAgentBrief.model_validate(payload))


def _coerce_brief(value: object) -> MarketRadarAgentBrief:
    if isinstance(value, MarketRadarAgentBrief):
        return value
    if isinstance(value, Mapping):
        return MarketRadarAgentBrief.model_validate(value)
    if isinstance(value, str):
        return MarketRadarAgentBrief.model_validate_json(value)
    raise TypeError(f"Unsupported Agents SDK final output: {type(value).__name__}")


def _agent_runtime_payload(gate: Mapping[str, object], *, mode: str) -> dict[str, object]:
    credit_gate = _mapping(gate.get("credit_gate"))
    real_results_gate = _mapping(gate.get("real_results_gate"))
    max_turns = int(
        _number(credit_gate.get("max_openai_calls"))
        or _number(gate.get("max_turns"))
        or AGENT_SDK_MAX_TURNS
    )
    return {
        "schema_version": "market-radar-agent-runtime-v1",
        "orchestrator": "openai_agents_sdk",
        "provider": "openai",
        "mode": mode,
        "real_mode_gate_status": str(gate.get("status") or "unknown"),
        "real_results_gate_status": str(real_results_gate.get("status") or "unknown"),
        "credit_gate_status": str(credit_gate.get("status") or "unknown"),
        "execute_required": mode == "preview",
        "tool_surface": str(gate.get("tool_surface") or "specialist_agents_only"),
        NON_OPENAI_ASSISTANT_DEPENDENCY_KEY: "absent",
        "external_market_tools": False,
        "broker_tools": False,
        "shell_tools": False,
        "filesystem_tools": False,
        "web_tools": False,
        "max_turns": min(AGENT_SDK_MAX_TURNS, max_turns),
    }


def _agent_execute_block_reason(gate: Mapping[str, object]) -> str | None:
    reasons: list[str] = []
    if gate.get("status") != "ready":
        missing_env = ", ".join(_texts(gate.get("missing_env"))) or "unknown"
        reasons.append(f"OpenAI runtime gate blocked: {missing_env}")
    real_results_gate = _mapping(gate.get("real_results_gate"))
    if real_results_gate.get("status") != "ready":
        missing = ", ".join(_texts(real_results_gate.get("missing"))) or "real scan rows"
        reasons.append(f"Real results gate blocked: {missing}")
    credit_gate = _mapping(gate.get("credit_gate"))
    if credit_gate.get("status") != "ready":
        missing = ", ".join(_texts(credit_gate.get("missing"))) or "budget/pricing"
        reasons.append(f"OpenAI credit gate blocked: {missing}")
    return "; ".join(reasons) if reasons else None


def _record_agent_ledger_entry(
    *,
    snapshot: Mapping[str, object],
    gate: Mapping[str, object],
    brief: Mapping[str, object],
    config: AppConfig,
    ledger_repo: BudgetLedgerRepository | None,
    status: LLMCallStatus,
) -> None:
    if ledger_repo is None:
        return
    now = datetime.now(UTC)
    available_at = _snapshot_available_at(snapshot, now)
    credit_gate = _mapping(gate.get("credit_gate"))
    estimated_usage = _agent_estimated_usage(
        int(_number(credit_gate.get("max_openai_calls"))) or DEFAULT_AGENT_MAX_OPENAI_CALLS
    )
    estimated_cost = float(_number(credit_gate.get("estimated_cost_usd")))
    openai_calls = int(_number(_mapping(brief.get("external_calls_made")).get("openai")))
    actual_cost = estimated_cost if openai_calls > 0 or status == LLMCallStatus.FAILED else 0.0
    ticker = _text(_mapping(snapshot.get("controls")).get("ticker")) or None
    entry = BudgetLedgerEntry(
        id=budget_ledger_id(
            task=LLMTaskName.AGENT_BRIEF.value,
            ticker=ticker,
            candidate_packet_id=None,
            status=status.value,
            available_at=available_at,
            prompt_version=AGENT_BRIEF_PROMPT_VERSION,
            attempted_at=now,
        ),
        ts=now,
        available_at=available_at,
        task=LLMTaskName.AGENT_BRIEF,
        status=status,
        estimated_cost=estimated_cost,
        actual_cost=actual_cost,
        ticker=ticker,
        model=config.agent_sdk_model,
        provider="openai",
        token_usage=estimated_usage,
        tool_calls=[
            {"name": "data_sentinel", "type": "specialist_agent"},
            {"name": "catalyst_analyst", "type": "specialist_agent"},
            {"name": "risk_officer", "type": "specialist_agent"},
        ],
        prompt_version=AGENT_BRIEF_PROMPT_VERSION,
        schema_version=SCHEMA_VERSION,
        payload={
            "snapshot_hash": _snapshot_hash(snapshot),
            "brief_status": brief.get("status"),
            "openai_calls": openai_calls,
            "market_data_calls": int(
                _number(_mapping(brief.get("external_calls_made")).get("market_data"))
            ),
            "broker_calls": int(
                _number(_mapping(brief.get("external_calls_made")).get("broker"))
            ),
            "real_results_gate": gate.get("real_results_gate"),
            "credit_gate": credit_gate,
        },
    )
    ledger_repo.upsert_entry(entry)


def _agent_estimated_usage(max_openai_calls: int) -> TokenUsage:
    multiplier = _positive_int(max_openai_calls, DEFAULT_AGENT_MAX_OPENAI_CALLS)
    return TokenUsage(
        input_tokens=AGENT_BRIEF_BASE_ESTIMATED_USAGE.input_tokens * multiplier,
        cached_input_tokens=AGENT_BRIEF_BASE_ESTIMATED_USAGE.cached_input_tokens
        * multiplier,
        output_tokens=AGENT_BRIEF_BASE_ESTIMATED_USAGE.output_tokens * multiplier,
    )


def _token_usage_payload(usage: TokenUsage) -> dict[str, int]:
    return {
        "input_tokens": usage.input_tokens,
        "cached_input_tokens": usage.cached_input_tokens,
        "output_tokens": usage.output_tokens,
    }


def _has_llm_pricing(config: AppConfig) -> bool:
    return (
        config.llm_input_cost_per_1m is not None
        and config.llm_cached_input_cost_per_1m is not None
        and config.llm_output_cost_per_1m is not None
    )


def _pricing_is_stale(config: AppConfig, now: datetime) -> bool:
    if not config.llm_pricing_updated_at:
        return True
    try:
        updated = datetime.fromisoformat(config.llm_pricing_updated_at)
    except ValueError:
        return True
    if updated.tzinfo is None or updated.utcoffset() is None:
        updated = updated.replace(tzinfo=UTC)
    return now.astimezone(UTC) - updated.astimezone(UTC) > timedelta(
        days=config.llm_pricing_stale_after_days
    )


def _day_window(now: datetime) -> tuple[datetime, datetime]:
    start = datetime.combine(now.astimezone(UTC).date(), time.min, tzinfo=UTC)
    return start, start + timedelta(days=1)


def _month_window(now: datetime) -> tuple[datetime, datetime]:
    now = now.astimezone(UTC)
    start = datetime(now.year, now.month, 1, tzinfo=UTC)
    if now.month == 12:
        end = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
        end = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    return start, end


def _snapshot_available_at(snapshot: Mapping[str, object], fallback: datetime) -> datetime:
    controls = _mapping(snapshot.get("controls"))
    value = _text(controls.get("available_at")) or _text(
        _mapping(snapshot.get("real_results")).get("cutoff")
    )
    if not value:
        return fallback
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return fallback
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _snapshot_hash(snapshot: Mapping[str, object]) -> str:
    canonical = json.dumps(snapshot, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _runtime_context(row: Mapping[str, object]) -> dict[str, object]:
    return _copy_keys(
        row,
        (
            "environment",
            "daily_market_provider",
            "daily_event_provider",
            "latest_run_as_of",
            "latest_run_cutoff",
            "polygon_key_configured",
            "sec_live_enabled",
            "sec_user_agent_configured",
            "schwab_credentials_configured",
            "openai_key_configured",
        ),
    )


def _readiness_context(row: Mapping[str, object]) -> dict[str, object]:
    usefulness = _mapping(row.get("market_radar_usefulness"))
    return {
        **_copy_keys(
            row,
            (
                "schema_version",
                "status",
                "safe_to_make_investment_decision",
                "headline",
                "next_action",
            ),
        ),
        "usefulness": _copy_keys(
            usefulness,
            ("status", "headline", "next_action", "decision_mode"),
        ),
        "checklist": [
            _copy_keys(item, ("area", "status", "finding", "next_action", "evidence"))
            for item in _rows(row.get("readiness_checklist"))[:10]
        ],
    }


def _real_results_context(row: Mapping[str, object]) -> dict[str, object]:
    return _copy_keys(
        row,
        (
            "schema_version",
            "status",
            "headline",
            "next_action",
            "source",
            "row_count",
            "latest_run_id",
            "latest_run_status",
            "as_of",
            "cutoff",
            "missing",
        ),
    )


def _work_queue_context(row: Mapping[str, object]) -> dict[str, object]:
    return {
        **_copy_keys(
            row,
            (
                "schema_version",
                "status",
                "headline",
                "next_action",
                "investment_mode",
                "safe_to_make_investment_decision",
            ),
        ),
        "counts": _mapping(row.get("counts")),
        "rows": [
            _copy_keys(
                item,
                (
                    "priority",
                    "area",
                    "item",
                    "status",
                    "ticker",
                    "next_action",
                    "evidence",
                    "source",
                ),
            )
            for item in _rows(row.get("rows"))[:8]
        ],
    }


def _call_plan_context(row: Mapping[str, object]) -> dict[str, object]:
    return {
        **_copy_keys(
            row,
            (
                "schema_version",
                "status",
                "headline",
                "next_action",
                "will_call_external_providers",
                "max_external_call_count",
            ),
        ),
        "guardrails": _mapping(row.get("guardrails")),
        "rows": [
            _copy_keys(
                item,
                (
                    "layer",
                    "provider",
                    "endpoint",
                    "status",
                    "external_call_count_max",
                    "detail",
                    "next_action",
                ),
            )
            for item in _rows(row.get("rows"))[:10]
        ],
    }


def _priced_in_context(
    queue: Mapping[str, object],
    source_coverage: Mapping[str, object],
    source_workflow: Mapping[str, object],
    preflight: Mapping[str, object],
    answer: Mapping[str, object],
    audit: Mapping[str, object],
) -> dict[str, object]:
    coverage = source_coverage or _mapping(queue.get("source_coverage"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    first_blocker = _text(trust_gate.get("first_blocker"))
    setup_blocker = _mapping(trust_gate.get("setup_blocker"))
    market_bar_unblock_options = (
        _market_bar_unblock_options_context(audit)
        if first_blocker in {"", "market_bars"}
        else []
    )
    recommended_unblock_action = _market_bar_recommended_unblock_context(answer)
    return {
        **_copy_keys(
            queue,
            (
                "schema_version",
                "status",
                "headline",
                "next_action",
                "total_count",
                "returned_count",
                "count",
                "has_more",
                "offset",
            ),
        ),
        "filters": _copy_keys(
            _mapping(queue.get("filters")),
            (
                "status",
                "usefulness",
                "source_gap",
                "decision_gap",
                "limit",
                "offset",
                "available_at",
            ),
        ),
        "scan": _copy_keys(
            _mapping(queue.get("scan")),
            (
                "requested_securities",
                "scanned_securities",
                "scanned_candidate_states",
                "candidate_states",
                "candidate_packets",
                "decision_cards",
            ),
        ),
        "status_counts": _mapping(queue.get("status_counts")),
        "usefulness_counts": _mapping(queue.get("usefulness_counts")),
        "preflight": _copy_keys(
            preflight,
            ("status", "headline", "next_action", "scan_status"),
        ),
        "answer": _priced_in_answer_context(answer),
        **(
            {"setup_blocker": _copy_keys(
                setup_blocker,
                ("schema_version", "area", "status", "action", "command", "api"),
            )}
            if setup_blocker
            else {}
        ),
        "evidence_plan": _priced_in_evidence_plan_context(
            _mapping(preflight.get("evidence_plan"))
        ),
        "source_workflow": _priced_in_source_workflow_context(source_workflow),
        **(
            {"recommended_unblock_action": recommended_unblock_action}
            if recommended_unblock_action
            else {}
        ),
        **(
            {"market_bar_unblock_options": market_bar_unblock_options}
            if market_bar_unblock_options
            else {}
        ),
        "source_coverage": {
            **_copy_keys(
                coverage,
                ("schema_version", "row_count", "summary", "weak_sources"),
            ),
            "actions": [
                _copy_keys(
                    item,
                    (
                        "source",
                        "status",
                        "coverage_pct",
                        "gap_count",
                        "next_action",
                        "batch_plan_command",
                        "full_scan_gap_review_command",
                        "full_scan_export_command",
                    ),
                )
                for item in _rows(coverage.get("actions"))
                if _text(item.get("status")) not in {"ready", "not_applicable"}
            ][:8],
        },
        "rows": [_priced_in_row_context(item) for item in _rows(queue.get("rows"))[:8]],
    }


def _market_bar_recommended_unblock_context(answer):
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    recommended = _mapping(trust_gate.get("recommended_action"))
    if not recommended:
        blocker_detail = _mapping(trust_gate.get("blocker_detail"))
        recommended = _mapping(blocker_detail.get("recommended_action"))
    if not recommended:
        return {}
    return _copy_keys(
        recommended,
        (
            "schema_version",
            "kind",
            "label",
            "status",
            "reason",
            "command",
            "cli_command",
            "tui_command",
            "api",
            "request_body",
            "confirm_request_body",
            "approval_required",
            "external_calls_required",
            "db_writes_required",
            "external_calls_made",
        ),
    )


def _market_bar_unblock_options_context(audit: Mapping[str, object]) -> list[dict[str, object]]:
    repair = _mapping(_mapping(audit.get("market_bars")).get("repair"))
    if not repair:
        return []
    options: list[dict[str, object]] = []
    manual_command = _text(
        repair.get("dashboard_manual_template_command") or repair.get("template_command")
    )
    manual_preview = _text(
        repair.get("dashboard_manual_import_preview_command")
        or repair.get("import_preview_command")
    )
    if manual_command:
        options.append(
            {
                "kind": "manual_csv",
                "status": "available",
                "external_calls_required": 0,
                "db_writes_during_step": 0,
                "command": manual_command,
                "preview_command": manual_preview or None,
            }
        )

    provider_plan = _mapping(repair.get("provider_fill_plan"))
    packet = _mapping(provider_plan.get("provider_saved_file_capture_approval_packet"))
    if packet:
        options.append(
            {
                "kind": "saved_provider_capture",
                "status": _text(packet.get("status")) or "unknown",
                "approval_required": bool(packet.get("approval_required")),
                "external_calls_required": int(
                    _number(packet.get("external_calls_if_approved"))
                ),
                "db_writes_during_step": int(
                    _number(packet.get("db_writes_during_capture"))
                ),
                "command": _text(packet.get("tui_confirm_command"))
                or _text(packet.get("capture_cli_command")),
                "cli_command": _text(packet.get("capture_cli_command")),
                "tui_command": _text(packet.get("tui_confirm_command")),
                "question": _text(packet.get("question")),
            }
        )
        for step in _rows(packet.get("post_capture_zero_call_steps")):
            step_name = _text(step.get("step"))
            if step_name not in {"validate_saved_file", "preview_import"}:
                continue
            options.append(
                {
                    "kind": step_name,
                    "status": _text(packet.get("saved_file_status")) or "unknown",
                    "external_calls_required": int(_number(step.get("external_calls_made"))),
                    "db_writes_during_step": int(_number(step.get("db_writes_made"))),
                    "command": _text(step.get("tui_command"))
                    or _text(step.get("cli_command")),
                    "cli_command": _text(step.get("cli_command")),
                    "tui_command": _text(step.get("tui_command")),
                }
            )
    return options[:5]


def _priced_in_source_workflow_context(workflow: Mapping[str, object]) -> dict[str, object]:
    if not workflow:
        return {}
    return {
        **_copy_keys(
            workflow,
            (
                "schema_version",
                "status",
                "headline",
                "next_action",
                "next_command",
                "coverage_first_action",
                "coverage_first_command",
                "decision_shortcut_action",
                "decision_shortcut_command",
                "priority_scope",
                "decision_priority_scope",
                "overview_command",
                "overview_api",
                "external_calls_made",
            ),
        ),
        "steps": [
            _copy_keys(
                step,
                (
                    "priority",
                    "source",
                    "status",
                    "action",
                    "command",
                    "api",
                    "decision_useful_gap_rows",
                    "research_useful_gap_rows",
                    "actionable_gap_rows",
                    "priority_sample_tickers",
                ),
            )
            for step in _rows(workflow.get("steps"))[:8]
        ],
    }


def _priced_in_answer_context(answer: Mapping[str, object]) -> dict[str, object]:
    if not answer:
        return {}
    return {
        **_copy_keys(
            answer,
            (
                "schema_version",
                "status",
                "decision_ready",
                "question",
                "answer",
                "headline",
                "next_action",
                "next_command",
                "external_calls_made",
            ),
        ),
        "counts": _mapping(answer.get("counts")),
        "decision_readiness": _safe_value(answer.get("decision_readiness") or {}),
        "trust_blockers": _safe_value(answer.get("trust_blockers") or []),
    }


def _priced_in_evidence_plan_context(plan: Mapping[str, object]) -> dict[str, object]:
    if not plan:
        return {}
    return {
        **_copy_keys(
            plan,
            (
                "schema_version",
                "status",
                "headline",
                "next_action",
                "next_command",
                "external_calls_made",
            ),
        ),
        "steps": [
            _copy_keys(
                step,
                (
                    "priority",
                    "area",
                    "status",
                    "depends_on",
                    "action",
                    "command",
                    "api",
                ),
            )
            for step in _rows(plan.get("steps"))[:8]
        ],
    }


def _priced_in_row_context(row: Mapping[str, object]) -> dict[str, object]:
    usefulness = _mapping(row.get("usefulness"))
    data_sources = _mapping(row.get("data_sources"))
    return {
        **_copy_keys(
            row,
            (
                "ticker",
                "priced_in_status",
                "priced_in_direction",
                "emotion_reaction_gap",
                "emotion_score",
                "reaction_score",
                "priced_in_score",
                "score",
                "blocked",
                "next_step",
            ),
        ),
        "usefulness": _copy_keys(
            usefulness,
            ("status", "label", "decision_ready", "missing_for_decision", "next_action"),
        ),
        "data_sources": _copy_keys(
            data_sources,
            ("summary", "available", "missing", "stale"),
        ),
    }


def _candidates_context(row: Mapping[str, object]) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    for item in _rows(row.get("rows"))[:8]:
        research_brief = _mapping(item.get("research_brief"))
        candidates.append(
            {
                **_copy_keys(
                    item,
                    (
                        "ticker",
                        "state",
                        "final_score",
                        "score_delta_5d",
                        "setup_type",
                        "candidate_theme",
                        "hard_blocks",
                        "transition_reasons",
                        "material_event_count",
                        "supporting_evidence_count",
                        "disconfirming_evidence_count",
                        "decision_card_id",
                        "candidate_packet_id",
                        "top_event_title",
                        "top_event_source",
                        "top_event_type",
                        "top_event_materiality",
                    ),
                ),
                "research_brief": _copy_keys(
                    research_brief,
                    ("external_export_blocked", "attribution_required", "license_tags"),
                ),
            }
        )
    return {"count": int(_number(row.get("count"))), "rows": candidates}


def _alerts_context(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "count": int(_number(row.get("count"))),
        "rows": [
            _copy_keys(
                item,
                (
                    "id",
                    "ticker",
                    "title",
                    "summary",
                    "status",
                    "route",
                    "priority",
                    "channel",
                    "action_state",
                    "trigger_kind",
                    "feedback_label",
                ),
            )
            for item in _rows(row.get("rows"))[:8]
        ],
    }


def _broker_context(row: Mapping[str, object]) -> dict[str, object]:
    snapshot = _mapping(row.get("snapshot"))
    exposure = _mapping(row.get("exposure"))
    return {
        "snapshot": _copy_keys(
            snapshot,
            (
                "broker",
                "connection_status",
                "account_count",
                "position_count",
                "open_order_count",
                "last_successful_sync_at",
            ),
        ),
        "exposure": _copy_keys(
            exposure,
            (
                "broker_connected",
                "broker_data_stale",
                "read_only",
                "order_submission_enabled",
                "order_submission_available",
                "hard_blocks",
                "portfolio_equity",
                "cash",
                "buying_power",
            ),
        ),
        "rate_limits": [
            _copy_keys(
                item,
                ("operation", "allowed", "min_interval_seconds", "retry_after_seconds"),
            )
            for item in _rows(row.get("rate_limits"))[:5]
        ],
        "counts": {
            "positions": len(_rows(row.get("positions"))),
            "open_orders": len(_rows(row.get("open_orders"))),
            "opportunity_actions": len(_rows(row.get("opportunity_actions"))),
            "triggers": len(_rows(row.get("triggers"))),
            "order_tickets": len(_rows(row.get("order_tickets"))),
        },
    }


def _ops_context(row: Mapping[str, object]) -> dict[str, object]:
    database = _mapping(row.get("database"))
    return {
        "database": _copy_keys(
            database,
            (
                "latest_daily_bar_date",
                "active_security_count",
                "active_security_with_latest_bar_count",
                "candidate_state_count",
                "alert_count",
            ),
        ),
        "providers": [
            _copy_keys(
                item,
                ("provider", "status", "last_successful_at", "rejected_count", "incident_count"),
            )
            for item in _rows(row.get("providers"))[:8]
        ],
    }


def _telemetry_context(row: Mapping[str, object]) -> dict[str, object]:
    return _copy_keys(
        row,
        ("status", "headline", "next_action", "attention_count", "event_count", "latest_event_at"),
    )


def _base_security_checks(
    snapshot: Mapping[str, object],
    gate: Mapping[str, object],
    *,
    mode: str,
) -> list[SecurityCheck]:
    call_plan = _mapping(snapshot.get("call_plan"))
    broker = _mapping(snapshot.get("broker"))
    exposure = _mapping(broker.get("exposure"))
    real_results_gate = _mapping(gate.get("real_results_gate"))
    credit_gate = _mapping(gate.get("credit_gate"))
    return [
        SecurityCheck(
            name="Snapshot boundary",
            status="pass",
            detail="Agents receive an allowlisted, redacted operator snapshot only.",
        ),
        SecurityCheck(
            name="Provider calls",
            status="pass",
            detail=(
                f"Browsing may show {int(_number(call_plan.get('max_external_call_count')))} "
                "planned provider calls, but agent-brief itself makes none."
            ),
        ),
        SecurityCheck(
            name="Broker actions",
            status="pass",
            detail=(
                "Broker context is read-only; order submission enabled="
                f"{bool(exposure.get('order_submission_enabled'))}."
            ),
        ),
        SecurityCheck(
            name="Agent runtime",
            status="pass",
            detail=(
                "orchestrator=openai_agents_sdk; provider=openai; "
                "co" "pilot_dependency=absent; tools=specialist_agents_only."
            ),
        ),
        SecurityCheck(
            name="OpenAI real-mode gate",
            status="pass" if mode == "real" and gate.get("status") == "ready" else "blocked"
            if mode == "blocked"
            else "pass",
            detail=(
                f"status={gate.get('status')}; "
                f"missing={', '.join(_texts(gate.get('missing_env'))) or 'none'}"
            ),
        ),
        SecurityCheck(
            name="Real results gate",
            status=(
                "pass"
                if real_results_gate.get("status") == "ready"
                else "blocked"
            ),
            detail=(
                f"status={real_results_gate.get('status') or 'unknown'}; "
                f"rows={int(_number(real_results_gate.get('row_count')))}; "
                f"missing={', '.join(_texts(real_results_gate.get('missing'))) or 'none'}"
            ),
        ),
        SecurityCheck(
            name="OpenAI credit gate",
            status=(
                "pass"
                if credit_gate.get("status") == "ready"
                else "blocked"
                if mode in {"preview", "blocked", "real"}
                else "pass"
            ),
            detail=(
                f"status={credit_gate.get('status') or 'unknown'}; "
                f"estimated_cost_usd={credit_gate.get('estimated_cost_usd', 0)}; "
                f"daily={credit_gate.get('daily_spend_usd', 0)}/"
                f"{credit_gate.get('daily_budget_usd', 0)}; "
                f"monthly={credit_gate.get('monthly_spend_usd', 0)}/"
                f"{credit_gate.get('monthly_budget_usd', 0)}; "
                f"missing={', '.join(_texts(credit_gate.get('missing'))) or 'none'}"
            ),
        ),
        SecurityCheck(
            name="Decision boundary",
            status="pass",
            detail=_decision_boundary(snapshot),
        ),
    ]


def _decision_boundary(snapshot: Mapping[str, object]) -> str:
    readiness = _mapping(snapshot.get("readiness"))
    if bool(readiness.get("safe_to_make_investment_decision")):
        return "manual_review_ready; still no autonomous trading or investment advice"
    status = _text(readiness.get("status")) or "unknown"
    usefulness = _mapping(readiness.get("usefulness"))
    useful_status = _text(usefulness.get("status")) or status
    return f"{useful_status}; research/manual triage only"


def _data_sentinel_summary(
    readiness: Mapping[str, object],
    call_plan: Mapping[str, object],
) -> str:
    status = _text(readiness.get("status")) or "unknown"
    calls = int(_number(call_plan.get("max_external_call_count")))
    return (
        f"Readiness is {status}; the viewed call plan allows up to {calls} provider "
        "call(s), but this agent brief made none."
    )


def _catalyst_summary(
    candidates: Sequence[Mapping[str, object]],
    alerts: Sequence[Mapping[str, object]],
) -> str:
    if not candidates and not alerts:
        return "No candidate or alert rows are currently available for triage."
    top = candidates[0] if candidates else {}
    if top:
        return (
            f"Top candidate is {_text(top.get('ticker')) or 'n/a'} in "
            f"{_text(top.get('state')) or 'unknown'} state; alerts queued={len(alerts)}."
        )
    return f"No candidate rows; alerts queued={len(alerts)}."


def _risk_summary(snapshot: Mapping[str, object]) -> str:
    broker = _mapping(snapshot.get("broker"))
    exposure = _mapping(broker.get("exposure"))
    read_only = bool(exposure.get("read_only"))
    order_enabled = bool(exposure.get("order_submission_enabled"))
    return f"Broker read_only={read_only}; order_submission_enabled={order_enabled}."


def _operator_summary(
    next_step: Mapping[str, object],
    operator_goal: str | None,
) -> str:
    action = _text(next_step.get("action")) or "Review readiness and candidates."
    prefix = f"Goal: {operator_goal}. " if operator_goal else ""
    return f"{prefix}Next human action: {action}"


def _status_insight(readiness: Mapping[str, object]) -> str:
    status = _text(readiness.get("status")) or "unknown"
    next_action = _text(readiness.get("next_action")) or "Review readiness details."
    return f"Readiness is {status}: {next_action}"


def _call_plan_insight(call_plan: Mapping[str, object]) -> str:
    status = _text(call_plan.get("status")) or "unknown"
    max_calls = int(_number(call_plan.get("max_external_call_count")))
    return f"Run call plan is {status}; max provider calls shown by the plan is {max_calls}."


def _priced_in_insight(priced_in: Mapping[str, object]) -> str | None:
    if not priced_in:
        return None
    total = int(_number(priced_in.get("total_count")))
    returned = int(_number(priced_in.get("returned_count") or priced_in.get("count")))
    status = _text(priced_in.get("status")) or "unknown"
    headline = _text(priced_in.get("headline"))
    coverage = _mapping(priced_in.get("source_coverage"))
    weak_sources = ", ".join(_texts(coverage.get("weak_sources"))) or "none"
    if headline:
        return (
            f"Priced-in queue is {status}: {headline}; visible rows={returned}, "
            f"total rows={total}, weak sources={weak_sources}."
        )
    return (
        f"Priced-in queue is {status}; visible rows={returned}, "
        f"total rows={total}, weak sources={weak_sources}."
    )


def _priced_in_answer_insight(priced_in: Mapping[str, object]) -> str | None:
    answer = _mapping(priced_in.get("answer"))
    if not answer:
        return None
    status = _text(answer.get("status")) or "unknown"
    decision_ready = bool(answer.get("decision_ready"))
    answer_text = _text(answer.get("answer")) or _text(answer.get("headline"))
    readiness = _mapping(answer.get("decision_readiness"))
    recommended_gap = _mapping(readiness.get("recommended_gap"))
    gap_piece = (
        f"; blocker={_text(recommended_gap.get('gap'))}"
        if recommended_gap.get("gap")
        else ""
    )
    next_action = _text(answer.get("next_action"))
    next_piece = f"; next={next_action}" if next_action else ""
    return (
        f"Priced-in answer is {status}; decision_ready={str(decision_ready).lower()}; "
        f"{answer_text}{gap_piece}{next_piece}."
    )


def _priced_in_evidence_plan_insight(priced_in: Mapping[str, object]) -> str | None:
    plan = _mapping(priced_in.get("evidence_plan"))
    if not plan:
        return None
    steps = _rows(plan.get("steps"))
    next_action = _text(plan.get("next_action")) or "review evidence plan"
    next_command = _text(plan.get("next_command"))
    command_text = f"; command={next_command}" if next_command else ""
    return (
        f"Priced-in evidence plan is {_text(plan.get('status')) or 'unknown'}; "
        f"steps={len(steps)}; next={next_action}{command_text}."
    )


def _priced_in_source_workflow_insight(priced_in: Mapping[str, object]) -> str | None:
    workflow = _mapping(priced_in.get("source_workflow"))
    if not workflow:
        return None
    coverage_action = _text(workflow.get("coverage_first_action"))
    coverage_command = _text(workflow.get("coverage_first_command"))
    decision_action = _text(workflow.get("decision_shortcut_action"))
    decision_command = _text(workflow.get("decision_shortcut_command"))
    pieces = []
    if coverage_action:
        pieces.append(f"coverage-first={coverage_action}")
    if coverage_command:
        pieces.append(f"coverage-command={coverage_command}")
    if decision_action:
        pieces.append(f"decision-shortcut={decision_action}")
    if decision_command:
        pieces.append(f"decision-command={decision_command}")
    if not pieces:
        pieces.append(_text(workflow.get("next_action")) or "review source workflow")
    return (
        f"Priced-in source workflow is {_text(workflow.get('status')) or 'unknown'}; "
        f"{'; '.join(pieces)}."
    )


def _priced_in_recommended_unblock_insight(priced_in):
    action = _mapping(priced_in.get("recommended_unblock_action"))
    if not action:
        return None
    kind = _text(action.get("kind")) or "action"
    status = _text(action.get("status")) or "unknown"
    calls = int(_number(action.get("external_calls_required")))
    writes = int(_number(action.get("db_writes_required")))
    command = _text(
        action.get("cli_command") or action.get("command") or action.get("tui_command")
    )
    reason = _text(action.get("reason"))
    pieces = [f"{kind} status={status}", f"calls={calls}", f"db_writes={writes}"]
    if command:
        pieces.append(f"command={command}")
    if reason:
        pieces.append(f"reason={reason}")
    return "Recommended market-bar unblock: " + "; ".join(pieces) + "."


def _priced_in_unblock_options_insight(priced_in: Mapping[str, object]) -> str | None:
    options = _rows(priced_in.get("market_bar_unblock_options"))
    if not options:
        return None
    pieces = []
    for option in options[:4]:
        kind = _text(option.get("kind")) or "option"
        status = _text(option.get("status")) or "unknown"
        calls = int(_number(option.get("external_calls_required")))
        command = _text(option.get("cli_command") or option.get("command"))
        command_piece = f" command={command}" if command else ""
        pieces.append(f"{kind} status={status} calls={calls}{command_piece}")
    return "Market-bar unblock options: " + "; ".join(pieces) + "."


def _priced_in_recommended_unblock_actions(priced_in):
    action = _mapping(priced_in.get("recommended_unblock_action"))
    if not action:
        return []
    kind = _text(action.get("kind")) or "action"
    command = _text(
        action.get("cli_command") or action.get("command") or action.get("tui_command")
    )
    if not command:
        return []
    calls = int(_number(action.get("external_calls_required")))
    writes = int(_number(action.get("db_writes_required")))
    if bool(action.get("approval_required")):
        return [
            f"Review recommended {kind}: approve {command} only if {calls} "
            f"market-data call(s) and {writes} DB write(s) match your intent."
        ]
    return [
        f"Run recommended {kind}: {command}; external calls={calls}; "
        f"DB writes={writes}."
    ]


def _priced_in_unblock_option_actions(priced_in: Mapping[str, object]) -> list[str]:
    actions: list[str] = []
    recommended = _mapping(priced_in.get("recommended_unblock_action"))
    recommended_command = _text(
        recommended.get("cli_command")
        or recommended.get("command")
        or recommended.get("tui_command")
    )

    for option in _rows(priced_in.get("market_bar_unblock_options"))[:4]:
        kind = _text(option.get("kind"))
        calls = int(_number(option.get("external_calls_required")))
        writes = int(_number(option.get("db_writes_during_step")))
        command = _text(option.get("cli_command") or option.get("command"))
        if not kind or not command:
            continue
        if recommended_command and command == recommended_command:
            continue
        if kind == "saved_provider_capture":
            actions.append(
                f"Approve {command} only if one market-data call and {writes} "
                "DB writes during capture match your intent."
            )
            continue
        actions.append(f"Use {command} for {kind}; external calls={calls}.")
    return actions


def _top_queue_insight(work_queue: Mapping[str, object]) -> str | None:
    rows = _rows(work_queue.get("rows"))
    if not rows:
        return _text(work_queue.get("headline")) or None
    top = rows[0]
    return (
        f"Top queue item: {_text(top.get('area')) or 'Work'} - "
        f"{_text(top.get('item')) or 'review'}; next={_text(top.get('next_action')) or 'n/a'}."
    )


def _candidate_insight(row: Mapping[str, object]) -> str:
    blocks = ", ".join(_texts(row.get("hard_blocks"))) or "none"
    return (
        f"{_text(row.get('ticker')) or 'n/a'} candidate is "
        f"{_text(row.get('state')) or 'unknown'} score={row.get('final_score')}; "
        f"hard_blocks={blocks}."
    )


def _alert_insight(row: Mapping[str, object]) -> str:
    return (
        f"Alert {_text(row.get('ticker')) or 'n/a'} is "
        f"{_text(row.get('status')) or 'unknown'} via {_text(row.get('route')) or 'n/a'}."
    )


def _copy_keys(row: Mapping[str, object], keys: Sequence[str]) -> dict[str, object]:
    return {
        key: _safe_value(row.get(key))
        for key in keys
        if key in row and row.get(key) is not None
    }


def _safe_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _strip_sensitive_keys({str(key): _safe_value(item) for key, item in value.items()})
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_safe_value(item) for item in list(value)[:20]]
    if isinstance(value, str):
        return redact_text(_truncate(value))
    return value


def _strip_sensitive_keys(value: object) -> Any:
    if isinstance(value, Mapping):
        safe: dict[str, object] = {}
        for key, item in value.items():
            key_text = str(key)
            if _is_sensitive_key(key_text):
                continue
            safe[key_text] = _strip_sensitive_keys(item)
        return safe
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_strip_sensitive_keys(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def _is_sensitive_key(key: str) -> bool:
    normalized = key.lower()
    return (
        normalized in {"payload", "raw_payload", "before_payload", "after_payload", "headers"}
        or normalized in {"authorization", "access_token", "refresh_token", "api_key", "apikey"}
        or normalized.endswith("_secret")
        or normalized.endswith("_token")
        or normalized.endswith("_api_key")
    )


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _text(value: object) -> str:
    if value is None:
        return ""
    return redact_text(str(value).strip())


def _texts(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return []
    return [_text(item) for item in value if _text(item)]


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _positive_int(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _truncate(value: str, limit: int = 360) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _dedupe(values: Sequence[str | None]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def _model_dump(model: BaseModel) -> dict[str, object]:
    return model.model_dump()


__all__ = [
    "MarketRadarAgentBrief",
    "agent_sdk_credit_gate_payload",
    "agent_sdk_gate_payload",
    "deterministic_agent_brief",
    "real_results_gate_payload",
    "redacted_operator_snapshot",
    "run_market_radar_agents",
]
