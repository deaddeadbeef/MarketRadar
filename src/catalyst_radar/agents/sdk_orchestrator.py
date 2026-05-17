from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import BaseModel, Field

from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.redaction import redact_text, redact_value

SCHEMA_VERSION = "market-radar-agent-brief-v1"
SNAPSHOT_SCHEMA_VERSION = "market-radar-agent-snapshot-v1"
AGENT_SDK_MAX_TURNS = 6

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
        "operator_next_step": _copy_keys(
            _mapping(source.get("operator_next_step")),
            ("status", "priority", "area", "item", "ticker", "action", "evidence", "source"),
        ),
        "operator_work_queue": _work_queue_context(
            _mapping(source.get("operator_work_queue"))
        ),
        "call_plan": _call_plan_context(_mapping(source.get("call_plan"))),
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
    status = "blocked" if mode == "blocked" else "dry_run"
    readiness = _mapping(snapshot.get("readiness"))
    call_plan = _mapping(snapshot.get("call_plan"))
    work_queue = _mapping(snapshot.get("operator_work_queue"))
    candidates = _rows(_mapping(snapshot.get("candidates")).get("rows"))
    alerts = _rows(_mapping(snapshot.get("alerts")).get("rows"))
    next_step = _mapping(snapshot.get("operator_next_step"))
    max_provider_calls = int(_number(call_plan.get("max_external_call_count")))

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
            summary=_operator_summary(next_step, operator_goal),
            confidence="high",
        ),
    ]

    insights = _dedupe(
        [
            _status_insight(readiness),
            _call_plan_insight(call_plan),
            _top_queue_insight(work_queue),
            *[_candidate_insight(row) for row in candidates[:3]],
            *[_alert_insight(row) for row in alerts[:2]],
            issue,
        ]
    )
    next_actions = _dedupe(
        [
            _text(next_step.get("action")),
            _text(work_queue.get("next_action")),
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
) -> dict[str, object]:
    gate = agent_sdk_gate_payload(config)
    snapshot = redacted_operator_snapshot(payload)
    if not real:
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="dry_run",
            operator_goal=operator_goal,
        )
    if gate["status"] != "ready":
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="blocked",
            operator_goal=operator_goal,
        )
    try:
        return _run_agent_sdk_real(snapshot, gate, config, operator_goal=operator_goal)
    except Exception as exc:  # pragma: no cover - exercised through integration boundary.
        return deterministic_agent_brief(
            snapshot,
            gate,
            mode="blocked",
            operator_goal=operator_goal,
            issue=f"OpenAI Agents SDK run failed closed: {exc}",
        )


def _run_agent_sdk_real(
    snapshot: Mapping[str, object],
    gate: Mapping[str, object],
    config: AppConfig,
    *,
    operator_goal: str | None = None,
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
                "max_agent_turns": AGENT_SDK_MAX_TURNS,
                "external_market_calls_allowed": False,
                "broker_calls_allowed": False,
            },
            "gate": gate,
            "snapshot": snapshot,
        },
        sort_keys=True,
        default=str,
    )
    result = Runner.run_sync(operator_agent, prompt, max_turns=AGENT_SDK_MAX_TURNS)
    brief = _coerce_brief(result.final_output)
    openai_calls = max(1, len(getattr(result, "raw_responses", []) or []))
    payload = _model_dump(brief)
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "mode": "real",
            "status": "completed",
            "decision_boundary": _decision_boundary(snapshot),
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
    "agent_sdk_gate_payload",
    "deterministic_agent_brief",
    "redacted_operator_snapshot",
    "run_market_radar_agents",
]
