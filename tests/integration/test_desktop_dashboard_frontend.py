from __future__ import annotations

import json
import re
from pathlib import Path


def test_tauri_dashboard_static_shell_exposes_initial_navigation_contract() -> None:
    source = Path("apps/radar-desktop/frontend/index.html").read_text(
        encoding="utf-8",
    )

    assert 'data-testid="dashboard-page"' in source
    assert 'data-current-page="overview"' in source
    assert 'data-current-nav-page="overview"' in source
    assert 'data-testid="automation-state"' in source
    assert "page=overview nav=overview status=loading provider_calls=0" in source
    assert 'data-testid="filter-state"' in source
    assert "ticker=all scan_mode=all stocks_only=false limit=50 offset=0" in source
    assert 'data-testid="command-state"' in source
    assert 'last_command=none page=overview nav=overview provider_calls=0' in source
    assert "<title>MarketRadar Trading Workbench</title>" in source
    assert 'data-testid="platform-state"' in source
    assert "primary_tool=market-radar live_trading_enabled=false" in source
    assert 'data-testid="automation-json"' in source
    match = re.search(
        r'<pre id="automation-json"[^>]*>(?P<payload>.*?)</pre>',
        source,
    )
    assert match is not None
    automation_payload = json.loads(match.group("payload"))
    assert automation_payload["contract_version"] == "market-radar-desktop-automation-v1"
    assert automation_payload["page"] == "overview"
    assert automation_payload["provider_calls"] == 0
    assert automation_payload["filters"]["scan_mode"] == "all"
    assert automation_payload["filters"]["stocks_only"] is False


def test_tauri_trading_workbench_shell_exposes_platform_tools() -> None:
    html = Path("apps/radar-desktop/frontend/index.html").read_text(
        encoding="utf-8",
    )
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )
    styles = Path("apps/radar-desktop/frontend/styles.css").read_text(
        encoding="utf-8",
    )
    rust_source = Path("apps/radar-desktop/src/main.rs").read_text(
        encoding="utf-8",
    )
    model_source = Path("crates/radar-tui/src/model.rs").read_text(
        encoding="utf-8",
    )
    tauri_config = Path("apps/radar-desktop/tauri.conf.json").read_text(
        encoding="utf-8",
    )

    assert "MarketRadar Trading Workbench" in html
    assert "MarketRadar Trading Workbench" in tauri_config
    assert "const TRADING_WORKBENCH_TITLE" in rust_source
    assert "app_name: TRADING_WORKBENCH_TITLE" in rust_source
    assert 'schema_version: "trading-platform-manifest-v1"' in rust_source
    assert 'primary_tool: "market-radar"' in rust_source
    assert "live_trading_enabled: false" in rust_source
    assert 'broker_order_submission: "disabled"' in rust_source

    for page in (
        "Portfolio",
        "MarketRadar",
        "TradePlanner",
        "RiskDesk",
        "PaperTrading",
        "Backtest",
        "Journal",
    ):
        assert f"Page::{page}" in model_source

    for text in (
        "fallbackPlatformModules",
        "function renderTradingWorkbenchOverview",
        "function tradingWorkbenchSnapshot",
        "function workbenchOperatorState",
        "function workbenchDecisionBrief",
        "function workbenchScenarioMatrix",
        "function workbenchScenarioMatrixForPage",
        "function workbenchRiskEnvelope",
        "function workbenchRiskEnvelopeForPage",
        "function workbenchTradeRunbook",
        "function workbenchTradeRunbookForPage",
        "function workbenchActionBus",
        "function workbenchWorkflowMap",
        "function workbenchWorkflowStagesForPage",
        "function workbenchPriorityQueue",
        "function workbenchPriorityItemsForPage",
        "function workbenchSupervisionGates",
        "function workbenchSupervisionGatesForPage",
        "function workbenchActionsForPage",
        "function tradingWorkbenchModule",
        "trading_workbench",
        "function compactMapping",
        'data-testid="trading-workbench-overview"',
        "function renderWorkbenchOperatorState",
        "function operatorStateSummary",
        'data-testid="workbench-operator-state"',
        'data-testid="operator-state-card"',
        'data-testid="operator-state-next-action"',
        "data-operator-status",
        "data-operator-active-module",
        "function renderWorkbenchDecisionBrief",
        "function decisionBriefSummary",
        'data-testid="workbench-decision-brief"',
        'data-testid="decision-brief-source"',
        'data-testid="decision-brief-setup"',
        'data-testid="decision-brief-risk"',
        'data-testid="decision-brief-next-action"',
        'data-testid="decision-brief-evidence-row"',
        "data-decision-brief-status",
        "data-decision-brief-ticker",
        "function renderWorkbenchScenarioMatrix",
        "function scenarioMatrixSummary",
        'data-testid="workbench-scenario-matrix"',
        'data-testid="workbench-scenario-row"',
        "data-scenario-matrix-status",
        "data-scenario-kind",
        "data-scenario-status",
        "function renderWorkbenchRiskEnvelope",
        "function riskEnvelopeSummary",
        'data-testid="workbench-risk-envelope"',
        'data-testid="workbench-risk-check"',
        "data-risk-envelope-status",
        "data-risk-check-status",
        "data-risk-check-scope",
        "function renderWorkbenchTradeRunbook",
        "function tradeRunbookSummary",
        'data-testid="workbench-trade-runbook"',
        'data-testid="workbench-runbook-step"',
        "data-runbook-status",
        "data-runbook-step-status",
        "data-runbook-step-kind",
        "function renderWorkbenchWorkflowMap",
        "function workflowMapSummary",
        'data-testid="workbench-workflow-map"',
        'data-testid="workbench-workflow-stage"',
        "data-workflow-status",
        "data-active-stage",
        "data-stage-status",
        "function renderWorkbenchPriorityQueue",
        "function priorityItemControl",
        "function priorityQueueSummary",
        'data-testid="workbench-priority-queue"',
        'data-testid="workbench-priority-item"',
        "data-priority-queue-status",
        "data-priority-item-status",
        "data-priority-item-kind",
        "function renderWorkbenchSupervisionGates",
        "function supervisionGateSummary",
        'data-testid="workbench-supervision-gates"',
        'data-testid="workbench-supervision-gate"',
        "data-supervision-gate-status",
        "data-supervision-gate-kind",
        "function renderWorkbenchActionBus",
        "function actionBusSummary",
        "function renderWorkbenchActionControl",
        'data-testid="workbench-action-bus"',
        'data-testid="workbench-action-row"',
        'data-testid="workbench-action-command"',
        'data-testid="workbench-action-page"',
        "data-workbench-action-kind",
        "data-workbench-action-command",
        "data-workbench-action-page",
        "data-workbench-local-write",
        'data-testid="platform-tool-card"',
        'data-tool="${escapeHtml(module.key)}"',
        "function renderPlatformModulePage",
        "function renderWorkbenchModuleData",
        "function renderWorkbenchActivePlan",
        "function activePlanBlock",
        "function renderWorkbenchTradeLifecycle",
        "function renderWorkbenchTradeSetups",
        "function renderWorkbenchSizingRows",
        "function renderWorkbenchPaperIntents",
        "function renderWorkbenchOrderIntents",
        "function renderWorkbenchRiskApprovals",
        "function renderWorkbenchRiskActionControls",
        "function renderWorkbenchAgentCapabilities",
        "function renderWorkbenchAgentContributions",
        "function renderWorkbenchAgentActions",
        "function renderWorkbenchAgentActionControls",
        "function renderWorkbenchAgentInsights",
        "function renderWorkbenchAgentSecurityChecks",
        "function renderWorkbenchRiskBlocks",
        "function renderWorkbenchReadinessChecks",
        "function renderWorkbenchAlerts",
        "function renderWorkbenchMarketTriggers",
        "function renderWorkbenchOpportunityActions",
        "function renderWorkbenchIpoRows",
        "function renderWorkbenchThemeRows",
        "function renderWorkbenchBudgetRows",
        "function renderWorkbenchValueEconomicsRows",
        "function renderWorkbenchOpsProviders",
        "function renderWorkbenchOpsJobs",
        "function renderWorkbenchCallPlanRows",
        "function renderWorkbenchTelemetryEvents",
        "function renderWorkbenchTelemetryCoverage",
        "function workbenchModuleRows",
        "function renderWorkbenchModuleRows",
        "function renderWorkbenchPaperTrades",
        "function renderWorkbenchOrderTickets",
        "function renderWorkbenchExecutionAudit",
        "function renderWorkbenchTicketAudit",
        "function renderWorkbenchJournalLedger",
        "function renderWorkbenchJournalOutcomes",
        "function renderWorkbenchValidationResults",
        "function renderWorkbenchUsefulLabels",
        "function renderWorkbenchPortfolioPositions",
        "function renderWorkbenchPortfolioBalances",
        "function renderWorkbenchPortfolioExposure",
        "function renderWorkbenchPortfolioOpenOrders",
        "function renderWorkbenchPortfolioActionControls",
        "function reviewPageButton",
        "function renderWorkbenchFeatureRows",
        "broker: () => renderPlatformModulePage('broker', snapshot)",
        "run: () => renderPlatformModulePage('run', snapshot)",
        "validation: () => renderPlatformModulePage('validation', snapshot)",
        "features: () => renderPlatformModulePage('features', snapshot)",
        "telemetry: () => renderPlatformModulePage('telemetry', snapshot)",
        'data-testid="platform-module-page"',
        'data-testid="platform-module-data"',
        'data-testid="platform-module-metrics"',
        'data-testid="platform-module-sources"',
        'data-testid="platform-module-row"',
        'data-testid="workbench-paper-trades"',
        'data-testid="workbench-paper-trade-row"',
        'data-testid="workbench-order-tickets"',
        'data-testid="workbench-order-ticket-row"',
        'data-testid="workbench-execution-audit"',
        'data-testid="workbench-execution-audit-row"',
        'data-testid="workbench-ticket-audit"',
        'data-testid="workbench-ticket-audit-row"',
        'data-testid="workbench-journal-ledger"',
        'data-testid="workbench-journal-ledger-row"',
        'data-testid="workbench-journal-outcomes"',
        'data-testid="workbench-journal-outcome-row"',
        'data-testid="workbench-validation-results"',
        'data-testid="workbench-validation-result-row"',
        'data-testid="workbench-useful-labels"',
        'data-testid="workbench-useful-label-row"',
        'data-testid="workbench-portfolio-positions"',
        'data-testid="workbench-portfolio-position-row"',
        "workbench-portfolio-review-page",
        "workbench-portfolio-risk-page",
        'data-testid="workbench-portfolio-balances"',
        'data-testid="workbench-portfolio-balance-row"',
        'data-testid="workbench-portfolio-exposure"',
        'data-testid="workbench-portfolio-exposure-row"',
        'data-testid="workbench-portfolio-open-orders"',
        'data-testid="workbench-portfolio-open-order-row"',
        "workbench-portfolio-broker-page",
        'data-testid="workbench-feature-inventory"',
        'data-testid="workbench-feature-row"',
        'data-testid="workbench-active-plan"',
        'data-testid="workbench-trade-lifecycle"',
        'data-testid="workbench-trade-lifecycle-row"',
        "workbench-lifecycle-show-ledger",
        "workbench-lifecycle-show-outcome",
        "workbench-lifecycle-preview-outcome",
        "workbench-lifecycle-update-outcome",
        'data-testid="workbench-plan-controls"',
        'data-testid="workbench-paper-actions"',
        'data-testid="workbench-paper-preview"',
        'data-testid="workbench-paper-record"',
        'data-testid="workbench-ticket-preview"',
        'data-testid="workbench-ticket-record"',
        'data-testid="workbench-trade-setups"',
        'data-testid="workbench-trade-setup-row"',
        'data-testid="workbench-trade-sizing"',
        'data-testid="workbench-trade-sizing-row"',
        'data-testid="workbench-paper-intents"',
        'data-testid="workbench-paper-intent-row"',
        'data-testid="workbench-order-intents"',
        'data-testid="workbench-order-intent-row"',
        'data-testid="workbench-risk-approvals"',
        'data-testid="workbench-risk-approval-row"',
        'data-testid="workbench-risk-paper-preview"',
        "workbench-risk-review-page",
        'data-testid="workbench-agent-capabilities"',
        'data-testid="workbench-agent-capability-row"',
        'data-testid="workbench-agent-contributions"',
        'data-testid="workbench-agent-contribution-row"',
        'data-testid="workbench-agent-actions"',
        'data-testid="workbench-agent-action-row"',
        'data-testid="workbench-agent-preview-action"',
        'data-testid="workbench-agent-insights"',
        'data-testid="workbench-agent-insight-row"',
        'data-testid="workbench-agent-security-checks"',
        'data-testid="workbench-agent-security-check-row"',
        'data-testid="workbench-risk-blocks"',
        'data-testid="workbench-risk-block-row"',
        'data-testid="workbench-readiness-checks"',
        'data-testid="workbench-readiness-check-row"',
        'data-testid="workbench-alerts"',
        'data-testid="workbench-alert-row"',
        'data-testid="workbench-market-triggers"',
        'data-testid="workbench-market-trigger-row"',
        'data-testid="workbench-opportunity-actions"',
        'data-testid="workbench-opportunity-action-row"',
        'data-testid="workbench-ipo-s1"',
        'data-testid="workbench-ipo-s1-row"',
        'data-testid="workbench-themes"',
        'data-testid="workbench-theme-row"',
        'data-testid="workbench-budget-ledger"',
        'data-testid="workbench-budget-ledger-row"',
        'data-testid="workbench-value-economics"',
        'data-testid="workbench-value-economics-row"',
        'data-testid="workbench-ops-providers"',
        'data-testid="workbench-ops-provider-row"',
        'data-testid="workbench-ops-jobs"',
        'data-testid="workbench-ops-job-row"',
        'data-testid="workbench-call-plan"',
        'data-testid="workbench-call-plan-row"',
        'data-testid="workbench-telemetry-events"',
        'data-testid="workbench-telemetry-event-row"',
        'data-testid="workbench-telemetry-coverage"',
        'data-testid="workbench-telemetry-coverage-row"',
        "strategy_proposal",
        "risk_approval",
        "order_intent",
        "execution_controls",
        "capability_map",
        "agent_contributions",
        "agent_actions",
        "agent_insights",
        "security_checks",
        "risk_blocks",
        "readiness_checks",
        "opportunity_actions",
        "balances",
        "exposure_rows",
        "open_order_checks",
        "Agent capability map",
        "Agent contribution brief",
        "Agent proposed human actions",
        "Agent insight brief",
        "Agent security checks",
        "Risk desk active plan blocks",
        "Risk readiness checks",
        "Workbench alerts",
        "Saved market triggers",
        "Saved opportunity actions",
        "IPO/S-1 filings",
        "Workbench theme clusters",
        "Workbench budget ledger",
        "Workbench value economics",
        "Ops provider health",
        "Ops recent jobs",
        "Ops call plan",
        "Telemetry audit events",
        "Telemetry coverage domains",
        "recommended_paper_decision",
        "paper_decision",
        "order_ticket",
        "paper_decision_preview_command",
        "paper_decision_execute_command",
        "trade_setup_rows",
        "sizing_rows",
        "paper_intent_rows",
        "order_intent_rows",
        "risk_approval_rows",
        "trade_lifecycle_rows",
        "Trade lifecycle",
        "Trade planner setup proposal",
        "Trade planner sizing proposal",
        "Trade planner paper intent",
        "Trade planner order intent",
        "Risk desk approval gates",
        "function bindWorkbenchPaperControls",
        "function runWorkbenchPaperDecision",
        "`paper-decision ${resolvedMode}`",
        "function bindWorkbenchTicketControls",
        "function runWorkbenchOrderTicket",
        "`order-ticket ${resolvedMode}`",
        "function renderWorkbenchLifecycleActions",
        "function bindWorkbenchLifecycleControls",
        "function runWorkbenchLifecycleCommand",
        "data-lifecycle-command",
        "ledger_show_command",
        "outcome_show_command",
        "outcome_preview_command",
        "outcome_update_command",
        "primary_command",
        "function bindWorkbenchAgentControls",
        "function runWorkbenchAgentCommand",
        "data-agent-command",
        "agent_preview_command",
        "agent_execute_boundary_command",
        "function bindWorkbenchReviewControls",
        "function runWorkbenchReviewPage",
        "function runWorkbenchRiskCommand",
        "function bindWorkbenchActionBusControls",
        "function dispatchWorkbenchAction",
        "function commandHasExecuteToken",
        "data-review-page",
        "data-risk-command",
        "portfolio_review_command",
        "risk_review_command",
        "broker_review_command",
        "paper_preview_command",
        "live_boundary_command",
        "Record Paper Decision",
        "Preview Ticket",
        "Save Ticket",
        "Paper trade ledger",
        "no execution",
        "Blocked workbench order tickets",
        "Paper execution audit",
        "Broker ticket audit",
        "Journal value ledger entries",
        "Journal value outcomes",
        "Backtest validation results",
        "Validation useful labels",
        "Portfolio positions",
        "Portfolio account balances",
        "Portfolio exposure summary",
        "Portfolio open order boundary",
        "Workbench feature inventory",
        "Hard Blocks",
        "Provider Calls",
        "Broker Order",
        "manual_review_required",
        "candidates: () => renderPlatformModulePage('candidates', snapshot)",
        "review: () => renderPlatformModulePage('review', snapshot)",
        "readiness: () => renderPlatformModulePage('readiness', snapshot)",
        "alerts: () => renderPlatformModulePage('alerts', snapshot)",
        "agent: () => renderPlatformModulePage('agent', snapshot)",
        "ipo: () => renderPlatformModulePage('ipo', snapshot)",
        "themes: () => renderPlatformModulePage('themes', snapshot)",
        "validation: () => renderPlatformModulePage('validation', snapshot)",
        "features: () => renderPlatformModulePage('features', snapshot)",
        "costs: () => renderPlatformModulePage('costs', snapshot)",
        "ops: () => renderPlatformModulePage('ops', snapshot)",
        "telemetry: () => renderPlatformModulePage('telemetry', snapshot)",
        "ipo-s1",
        'data-testid="live-trading-disabled"',
        "function bindPlatformToolCards",
        "platform: {",
        "primary_tool: platformManifest().primary_tool",
        "live_trading_enabled: Boolean(platformBoundary().live_trading_enabled)",
        "action_count: Number(workbenchActionBus(snapshot)?.metrics?.action_count || 0)",
        "workflow_status: compact(workbenchWorkflowMap(snapshot)?.status, 'unknown')",
        "active_stage_id: compact(workbenchWorkflowMap(snapshot)?.active_stage_id, 'none')",
        "stage_count: Number(workbenchWorkflowMap(snapshot)?.stage_count || 0)",
        "priority_queue_status: compact(workbenchPriorityQueue(snapshot)?.status, 'unknown')",
        "primary_priority_item_id: compact("
        "workbenchPriorityQueue(snapshot)?.primary_item_id, 'none')",
        "priority_item_count: Number(workbenchPriorityQueue(snapshot)?.metrics?.item_count || 0)",
        "supervision_status: compact(workbenchSupervisionGates(snapshot)?.status, 'unknown')",
        "primary_supervision_gate_id: compact("
        "workbenchSupervisionGates(snapshot)?.primary_gate_id, 'none')",
        "approval_required_count: Number("
        "workbenchSupervisionGates(snapshot)?.metrics?.approval_required_count || 0)",
        "armed_local_write: compact(state.pendingLocalWrite?.command, 'none')",
        "operator_status: compact(workbenchOperatorState(snapshot)?.status, 'unknown')",
        "operator_active_module: compact("
        "workbenchOperatorState(snapshot)?.active_module, 'none')",
        "operator_active_blocker: compact("
        "workbenchOperatorState(snapshot)?.primary_blocker, 'none')",
        "operator_next_command: compact("
        "workbenchOperatorState(snapshot)?.primary_next_action?.command, 'none')",
        "function workbenchExecutionSandbox",
        "function workbenchExecutionSandboxForPage",
        "function renderWorkbenchExecutionSandbox",
        "function executionSandboxLaneControl",
        "function executionSandboxSummary",
        'data-testid="workbench-execution-sandbox"',
        'data-testid="execution-sandbox-lane"',
        'data-execution-sandbox-status="${escapeHtml(sandbox.status || \'unknown\')}"',
        'data-execution-lane-status="${escapeHtml(lane.status || \'unknown\')}"',
        'data-execution-lane-kind="${escapeHtml(lane.lane_kind || \'unknown\')}"',
        "execution_sandbox_status: compact("
        "workbenchExecutionSandbox(snapshot)?.status, 'unknown')",
        "execution_sandbox_active_lane_id: compact("
        "workbenchExecutionSandbox(snapshot)?.active_lane_id, 'none')",
        "execution_sandbox_preview_count: Number("
        "workbenchExecutionSandbox(snapshot)?.metrics?.preview_lane_count || 0)",
        "execution_sandbox_disabled_count: Number("
        "workbenchExecutionSandbox(snapshot)?.metrics?.disabled_lane_count || 0)",
        "decision_brief_status: compact(workbenchDecisionBrief(snapshot)?.status, 'unknown')",
        "decision_brief_ticker: compact(workbenchDecisionBrief(snapshot)?.ticker, 'none')",
        "decision_brief_source_tool: compact("
        "workbenchDecisionBrief(snapshot)?.source_tool, 'market-radar')",
        "decision_brief_next_command: compact("
        "workbenchDecisionBrief(snapshot)?.next_action?.command, 'none')",
        "scenario_matrix_status: compact(workbenchScenarioMatrix(snapshot)?.status, 'unknown')",
        "scenario_matrix_ticker: compact(workbenchScenarioMatrix(snapshot)?.ticker, 'none')",
        "scenario_count: Number(workbenchScenarioMatrix(snapshot)?.metrics?.scenario_count || 0)",
        "scenario_reward_risk: compact("
        "workbenchScenarioMatrix(snapshot)?.metrics?.risk_reward, 'none')",
        "function workbenchPortfolioImpact",
        "function workbenchPortfolioImpactForPage",
        "function renderWorkbenchPortfolioImpact",
        "function portfolioImpactSummary",
        'data-testid="workbench-portfolio-impact-preview"',
        'data-testid="portfolio-impact-exposure"',
        'data-testid="portfolio-impact-check"',
        "data-portfolio-impact-status=\"${escapeHtml(preview.status || 'unknown')}\"",
        "data-portfolio-impact-exposure-status=\"${escapeHtml(row.status || 'unknown')}\"",
        "data-portfolio-impact-check-status=\"${escapeHtml(check.status || 'unknown')}\"",
        "portfolio_impact_status: compact("
        "workbenchPortfolioImpact(snapshot)?.status, 'unknown')",
        "portfolio_impact_ticker: compact("
        "workbenchPortfolioImpact(snapshot)?.ticker, 'none')",
        "portfolio_impact_proposed_notional: compact("
        "workbenchPortfolioImpact(snapshot)?.impact?.proposed_notional, 'none')",
        "portfolio_impact_block_count: Number("
        "workbenchPortfolioImpact(snapshot)?.blockers?.length || 0)",
        "function workbenchPositionSizing",
        "function workbenchPositionSizingForPage",
        "function renderWorkbenchPositionSizing",
        "function positionSizingSummary",
        'data-testid="workbench-position-sizing"',
        'data-testid="position-sizing-check"',
        'data-position-sizing-status="${escapeHtml(sizing.status || \'unknown\')}"',
        'data-position-sizing-check-status="${escapeHtml(check.status || \'unknown\')}"',
        'data-position-sizing-check-scope="${escapeHtml(check.scope || \'unknown\')}"',
        "position_sizing_status: compact("
        "workbenchPositionSizing(snapshot)?.status, 'unknown')",
        "position_sizing_ticker: compact("
        "workbenchPositionSizing(snapshot)?.ticker, 'none')",
        "position_sizing_suggested_shares: Number("
        "workbenchPositionSizing(snapshot)?.recommendation?.suggested_quantity || 0)",
        "position_sizing_risk_budget: compact("
        "workbenchPositionSizing(snapshot)?.recommendation?.risk_budget, 'none')",
        "function workbenchCapitalAllocation",
        "function workbenchCapitalAllocationForPage",
        "function renderWorkbenchCapitalAllocation",
        "function capitalAllocationSummary",
        "${renderWorkbenchCapitalAllocation(snapshot, 'overview')}",
        "${renderWorkbenchCapitalAllocation(snapshot, pageKey)}",
        'data-testid="workbench-capital-allocation"',
        'data-testid="capital-allocation-check"',
        "data-capital-allocation-status=\"${escapeHtml(allocation.status || 'unknown')}\"",
        "data-capital-allocation-check-status=\"${escapeHtml(check.status || 'unknown')}\"",
        "data-capital-allocation-check-scope=\"${escapeHtml(check.scope || 'unknown')}\"",
        "capital_allocation_status: compact(",
        "capital_allocation_ticker: compact(",
        "capital_allocation_suggested_notional: compact(",
        "capital_allocation_buying_power_usage_pct: compact(",
        "capital_allocation_blocked_check_count: Number(",
        "capital_allocation_allowed: Boolean(",
        "function workbenchOrderTicketDraft",
        "function workbenchOrderTicketDraftForPage",
        "function renderWorkbenchOrderTicketDraft",
        "function orderTicketDraftSummary",
        'data-testid="workbench-order-ticket-draft"',
        'data-testid="order-ticket-draft-check"',
        "data-order-ticket-draft-status=\"${escapeHtml(draft.status || 'unknown')}\"",
        "data-order-ticket-draft-check-status=\"${escapeHtml(check.status || 'unknown')}\"",
        "data-order-ticket-draft-check-scope=\"${escapeHtml(check.scope || 'unknown')}\"",
        "order_ticket_draft_status: compact("
        "workbenchOrderTicketDraft(snapshot)?.status, 'unknown')",
        "order_ticket_draft_ticker: compact("
        "workbenchOrderTicketDraft(snapshot)?.ticker, 'none')",
        "order_ticket_draft_suggested_shares: Number("
        "workbenchOrderTicketDraft(snapshot)?.ticket?.suggested_quantity || 0)",
        "order_ticket_draft_preview_command: compact("
        "workbenchOrderTicketDraft(snapshot)?.commands?.preview, 'none')",
        "function workbenchPaperTradePreview",
        "function workbenchPaperTradePreviewForPage",
        "function renderWorkbenchPaperTradePreview",
        "function paperTradePreviewSummary",
        'data-testid="workbench-paper-trade-preview"',
        'data-testid="paper-trade-preview-check"',
        "data-paper-trade-preview-status=\"${escapeHtml(preview.status || 'unknown')}\"",
        "data-paper-trade-check-status=\"${escapeHtml(check.status || 'unknown')}\"",
        "data-paper-trade-check-scope=\"${escapeHtml(check.scope || 'unknown')}\"",
        "paper_trade_preview_status: compact("
        "workbenchPaperTradePreview(snapshot)?.status, 'unknown')",
        "paper_trade_preview_ticker: compact("
        "workbenchPaperTradePreview(snapshot)?.ticker, 'none')",
        "paper_trade_preview_decision: compact("
        "workbenchPaperTradePreview(snapshot)?.paper_decision?.decision, 'none')",
        "paper_trade_preview_suggested_quantity: Number("
        "workbenchPaperTradePreview(snapshot)?.paper_decision?.suggested_quantity || 0)",
        "paper_trade_preview_block_count: Number("
        "workbenchPaperTradePreview(snapshot)?.blockers?.length || 0)",
        "function workbenchPretradeCompliance",
        "function workbenchPretradeComplianceForPage",
        "function renderWorkbenchPretradeCompliance",
        "function pretradeComplianceSummary",
        "${renderWorkbenchPretradeCompliance(snapshot, 'overview')}",
        "${renderWorkbenchPretradeCompliance(snapshot, pageKey)}",
        'data-testid="workbench-pretrade-compliance"',
        'data-testid="pretrade-compliance-check"',
        "data-pretrade-compliance-status=\"${escapeHtml(compliance.status || 'unknown')}\"",
        "data-pretrade-compliance-check-status=\"${escapeHtml(check.status || 'unknown')}\"",
        "data-pretrade-compliance-check-scope=\"${escapeHtml(check.scope || 'unknown')}\"",
        "pretrade_compliance_status: compact(",
        "pretrade_compliance_ticker: compact(",
        "pretrade_compliance_primary_blocker: compact(",
        "pretrade_compliance_blocked_check_count: Number(",
        "pretrade_compliance_approval_required_count: Number(",
        "pretrade_compliance_ready: Boolean(",
        "function workbenchLearningLoop",
        "function workbenchLearningLoopForPage",
        "function renderWorkbenchLearningLoop",
        "function learningLoopSummary",
        "${renderWorkbenchLearningLoop(snapshot, 'overview')}",
        "${renderWorkbenchLearningLoop(snapshot, pageKey)}",
        'data-testid="workbench-learning-loop"',
        'data-testid="learning-loop-card"',
        "data-learning-loop-status=\"${escapeHtml(loop.status || 'unknown')}\"",
        "data-learning-loop-card-status=\"${escapeHtml(card.status || 'unknown')}\"",
        "data-learning-loop-card-module=\"${escapeHtml(card.module || 'unknown')}\"",
        "learning_loop_status: compact("
        "workbenchLearningLoop(snapshot)?.status, 'unknown')",
        "learning_loop_ticker: compact("
        "workbenchLearningLoop(snapshot)?.ticker, 'none')",
        "learning_loop_stage: compact("
        "workbenchLearningLoop(snapshot)?.learning_stage, 'unlinked')",
        "learning_loop_validation_result_id: compact("
        "workbenchLearningLoop(snapshot)?.validation_state?.validation_result_id, 'none')",
        "learning_loop_outcome_id: compact("
        "workbenchLearningLoop(snapshot)?.journal_state?.outcome_id, 'none')",
        "learning_loop_blocked_card_count: Number("
        "workbenchLearningLoop(snapshot)?.metrics?.blocked_card_count || 0)",
        "function workbenchStrategyReview",
        "function workbenchStrategyReviewForPage",
        "function renderWorkbenchStrategyReview",
        "function strategyReviewSummary",
        "${renderWorkbenchStrategyReview(snapshot, 'overview')}",
        "${renderWorkbenchStrategyReview(snapshot, pageKey)}",
        'data-testid="workbench-strategy-review"',
        'data-testid="strategy-review-hypothesis"',
        "data-strategy-review-status=\"${escapeHtml(review.status || 'unknown')}\"",
        "data-strategy-hypothesis-status=\"${escapeHtml(row.status || 'unknown')}\"",
        "data-strategy-hypothesis-driver=\"${escapeHtml(row.driver || 'unknown')}\"",
        "strategy_review_status: compact("
        "workbenchStrategyReview(snapshot)?.status, 'unknown')",
        "strategy_review_ticker: compact("
        "workbenchStrategyReview(snapshot)?.ticker, 'none')",
        "strategy_review_stage: compact("
        "workbenchStrategyReview(snapshot)?.strategy_stage, 'unlinked')",
        "strategy_review_hypothesis_count: Number("
        "workbenchStrategyReview(snapshot)?.metrics?.hypothesis_count || 0)",
        "strategy_review_blocked_hypothesis_count: Number("
        "workbenchStrategyReview(snapshot)?.metrics?.blocked_hypothesis_count || 0)",
        "strategy_update_allowed: Boolean("
        "workbenchStrategyReview(snapshot)?.strategy_update_allowed)",
        "function workbenchTradeMonitor",
        "function workbenchTradeMonitorForPage",
        "function renderWorkbenchTradeMonitor",
        "function tradeMonitorSummary",
        "${renderWorkbenchTradeMonitor(snapshot, 'overview')}",
        "${renderWorkbenchTradeMonitor(snapshot, pageKey)}",
        'data-testid="workbench-trade-monitor"',
        'data-testid="trade-monitor-watch-item"',
        "data-trade-monitor-status=\"${escapeHtml(monitor.status || 'unknown')}\"",
        "data-trade-monitor-item-status=\"${escapeHtml(item.status || 'unknown')}\"",
        "data-trade-monitor-item-scope=\"${escapeHtml(item.scope || 'unknown')}\"",
        "trade_monitor_status: compact("
        "workbenchTradeMonitor(snapshot)?.status, 'unknown')",
        "trade_monitor_ticker: compact("
        "workbenchTradeMonitor(snapshot)?.ticker, 'none')",
        "trade_monitor_stage: compact("
        "workbenchTradeMonitor(snapshot)?.monitor_stage, 'unlinked')",
        "trade_monitor_active_trade_count: Number("
        "workbenchTradeMonitor(snapshot)?.metrics?.active_paper_trade_count || 0)",
        "trade_monitor_blocker_count: Number("
        "workbenchTradeMonitor(snapshot)?.metrics?.blocked_watch_item_count || 0)",
        "trade_monitor_open_order_count: Number("
        "workbenchTradeMonitor(snapshot)?.metrics?.open_order_count || 0)",
        "trade_monitor_primary_trigger_id: compact("
        "workbenchTradeMonitor(snapshot)?.alert_watch?.primary_trigger_id, 'none')",
        "trade_monitor_exit_update_allowed: Boolean("
        "workbenchTradeMonitor(snapshot)?.exit_update_allowed)",
        "risk_envelope_status: compact(workbenchRiskEnvelope(snapshot)?.status, 'unknown')",
        "risk_envelope_ticker: compact(workbenchRiskEnvelope(snapshot)?.ticker, 'none')",
        "risk_sizing_status: compact("
        "workbenchRiskEnvelope(snapshot)?.sizing_context?.sizing_status, 'unknown')",
        "risk_block_count: Number(workbenchRiskEnvelope(snapshot)?.blockers?.length || 0)",
        "risk_max_loss: compact("
        "workbenchRiskEnvelope(snapshot)?.sizing_context?.estimated_max_loss, 'none')",
        "runbook_status: compact(workbenchTradeRunbook(snapshot)?.status, 'unknown')",
        "runbook_active_step_id: compact("
        "workbenchTradeRunbook(snapshot)?.active_step_id, 'none')",
        "runbook_step_count: Number("
        "workbenchTradeRunbook(snapshot)?.metrics?.step_count || 0)",
        "runbook_blocked_step_count: Number("
        "workbenchTradeRunbook(snapshot)?.metrics?.blocked_step_count || 0)",
        "pendingLocalWrite: null",
        "function localWriteArmKey",
        "function clearPendingLocalWrite",
        "requires_arm_before_run",
        "Click again to confirm",
    ):
        assert text in source

    for alias in (
        "['portfolio', 'portfolio']",
        "['market-radar', 'market-radar']",
        "['candidate-review', 'candidates']",
        "['candidate-review', 'Candidate Review', 'candidates'",
        "['decision-review', 'review']",
        "['decision-review', 'Decision Review', 'review'",
        "['evidence-gaps', 'readiness']",
        "['evidence-gaps', 'Evidence Gaps', 'readiness'",
        "['safe-run', 'Safe Run', 'run'",
        "['trade-planner', 'trade-planner']",
        "['risk-desk', 'risk-desk']",
        "['paper-trading', 'paper-trading']",
        "['broker-desk', 'broker']",
        "['backtest', 'backtest']",
        "['validation', 'validation']",
        "['journal', 'journal']",
        "['agent-cockpit', 'agent']",
        "['features', 'Features', 'features'",
        "['costs', 'costs']",
        "['9', 'telemetry']",
        "['t', 'telemetry']",
        "['telemetry', 'Telemetry', 'telemetry'",
        "['8', 'ops']",
        "['ops', 'Ops', 'ops'",
    ):
        assert alias in source

    for tool in (
        "platform-tool-market-radar",
        "platform-tool-candidate-review",
        "platform-tool-decision-review",
        "platform-tool-evidence-gaps",
        "platform-tool-safe-run",
        "platform-tool-trade-planner",
        "platform-tool-risk-desk",
        "platform-tool-paper-trading",
        "platform-tool-agent-cockpit",
        "platform-tool-ipo-s1",
        "platform-tool-themes",
        "platform-tool-validation",
        "platform-tool-features",
        "platform-tool-costs",
        "platform-tool-ops",
        "platform-tool-telemetry",
    ):
        assert tool in rust_source

    assert ".platform-tools" in styles
    assert ".platform-tool-card" in styles
    assert ".workbench-operator-state" in styles
    assert ".operator-state-grid" in styles
    assert ".operator-state-cards" in styles
    assert ".workbench-execution-sandbox" in styles
    assert ".execution-sandbox-preview" in styles
    assert ".workbench-decision-brief" in styles
    assert ".decision-brief-grid" in styles
    assert ".decision-brief-evidence" in styles
    assert ".workbench-scenario-matrix" in styles
    assert ".scenario-matrix-preview" in styles
    assert ".workbench-portfolio-impact-preview" in styles
    assert ".portfolio-impact-preview" in styles
    assert ".portfolio-impact-check-preview" in styles
    assert ".workbench-position-sizing" in styles
    assert ".position-sizing-preview" in styles
    assert ".workbench-capital-allocation" in styles
    assert ".capital-allocation-preview" in styles
    assert ".workbench-order-ticket-draft" in styles
    assert ".order-ticket-draft-preview" in styles
    assert ".workbench-paper-trade-preview" in styles
    assert ".paper-trade-preview-checks" in styles
    assert ".workbench-pretrade-compliance" in styles
    assert ".pretrade-compliance-preview" in styles
    assert ".workbench-learning-loop" in styles
    assert ".learning-loop-preview" in styles
    assert ".workbench-strategy-review" in styles
    assert ".strategy-review-preview" in styles
    assert ".workbench-trade-monitor" in styles
    assert ".trade-monitor-preview" in styles
    assert ".workbench-risk-envelope" in styles
    assert ".risk-envelope-preview" in styles
    assert ".workbench-trade-runbook" in styles
    assert ".trade-runbook-preview" in styles
    assert ".workbench-workflow-map" in styles
    assert ".workflow-map-preview" in styles
    assert ".workbench-priority-queue" in styles
    assert ".priority-queue-preview" in styles
    assert ".workbench-supervision-gates" in styles
    assert ".supervision-gate-preview" in styles
    assert ".workbench-action-bus" in styles
    assert ".action-bus-preview" in styles
    assert ".workbench-action-button" in styles
    assert ".workbench-boundary-code" in styles
    assert ".platform-boundary" in styles
    assert ".module-page" in styles
    assert ".trade-lifecycle-preview" in styles
    assert ".trade-setup-preview" in styles
    assert ".trade-sizing-preview" in styles
    assert ".paper-intent-preview" in styles
    assert ".order-intent-preview" in styles
    assert ".risk-approval-preview" in styles
    assert ".risk-action-row" in styles
    assert ".portfolio-action-row" in styles
    assert ".review-action-button" in styles
    assert ".paper-trade-preview" in styles
    assert ".order-ticket-preview" in styles
    assert ".execution-audit-preview" in styles
    assert ".ticket-audit-preview" in styles
    assert ".journal-ledger-preview" in styles
    assert ".journal-outcome-preview" in styles
    assert ".validation-result-preview" in styles
    assert ".useful-label-preview" in styles
    assert ".feature-inventory-preview" in styles
    assert ".portfolio-position-preview" in styles
    assert ".portfolio-balance-preview" in styles
    assert ".portfolio-exposure-preview" in styles
    assert ".portfolio-open-order-preview" in styles
    assert ".agent-capability-preview" in styles
    assert ".agent-contribution-preview" in styles
    assert ".agent-action-preview" in styles
    assert ".agent-action-command-row" in styles
    assert ".agent-action-command-button" in styles
    assert ".agent-insight-preview" in styles
    assert ".agent-security-preview" in styles
    assert ".risk-block-preview" in styles
    assert ".risk-readiness-preview" in styles
    assert ".alert-preview" in styles
    assert ".market-trigger-preview" in styles
    assert ".opportunity-action-preview" in styles
    assert ".ipo-s1-preview" in styles
    assert ".theme-preview" in styles
    assert ".budget-ledger-preview" in styles
    assert ".value-economics-preview" in styles
    assert ".ops-provider-preview" in styles
    assert ".ops-job-preview" in styles
    assert ".call-plan-preview" in styles
    assert ".telemetry-event-preview" in styles
    assert ".telemetry-coverage-preview" in styles


def test_tauri_dashboard_loading_state_is_not_blank() -> None:
    html = Path("apps/radar-desktop/frontend/index.html").read_text(
        encoding="utf-8",
    )
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )
    tui_source = Path("crates/radar-tui/src/ui.rs").read_text(
        encoding="utf-8",
    )
    rust_source = Path("apps/radar-desktop/src/main.rs").read_text(
        encoding="utf-8",
    )

    assert "render_loading_state_is_a_real_dashboard_not_a_blank_box" in tui_source
    assert "Loading market snapshot" in tui_source
    assert "ZERO PROVIDER CALLS" in tui_source
    assert "ATTENTION QUEUE" in tui_source
    assert "DECISION STATUS" in tui_source

    for text in (
        'data-testid="loading-dashboard"',
        'data-testid="loading-metric-strip"',
        'data-testid="loading-preview-queue"',
        "Loading market snapshot",
        "Rendering remains local and makes zero provider calls.",
        "[loading]",
        "resolving dashboard contract",
        "zero-call read",
    ):
        assert text in html

    assert "function renderLoadingDashboard()" in source
    assert "if (!state.snapshot) renderLoadingDashboard();" in source
    assert "function loadingMetric(label, value)" in source
    assert "setText('#next-action', 'Loading local snapshot.');" in source
    assert "setText('#provider-calls', 'provider_calls=0');" in source
    assert "data-testid=\"loading-dashboard\"" in source
    assert "data-testid=\"loading-preview-queue\"" in source
    assert "loading-dashboard" in rust_source
    assert "loading-preview-queue" in rust_source


def test_tauri_dashboard_right_rail_matches_rust_tui_metadata() -> None:
    html = Path("apps/radar-desktop/frontend/index.html").read_text(
        encoding="utf-8",
    )
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )
    styles = Path("apps/radar-desktop/frontend/styles.css").read_text(
        encoding="utf-8",
    )
    tui_source = Path("crates/radar-tui/src/ui.rs").read_text(
        encoding="utf-8",
    )

    assert "fn render_keys" in tui_source
    assert "fn render_snapshot_meta" in tui_source
    assert "q / Esc     quit" in tui_source
    assert "r / F5      refresh" in tui_source
    assert 'Span::styled("Source: "' in tui_source
    assert 'Span::styled("Refresh: "' in tui_source
    assert 'Span::styled("Page: "' in tui_source

    for test_id in (
        'data-testid="keys-panel"',
        'data-testid="keys-list"',
        'data-testid="snapshot-panel"',
        'data-testid="snapshot-source"',
        'data-testid="snapshot-refresh"',
        'data-testid="snapshot-page"',
        'data-testid="snapshot-mode"',
    ):
        assert test_id in html

    assert "function renderKeys()" in source
    assert "renderKeys();" in source
    assert "['q / Esc', 'quit']" in source
    assert "['r / F5', 'refresh']" in source
    assert "['Tab/Arrows', 'next/prev']" in source
    assert "function renderSnapshotMeta(snapshot, pageInfo)" in source
    assert "friendlySource(state.config?.source_label || 'pending')" in source
    assert "setText('#snapshot-refresh', state.lastRefresh ? '0s ago' : 'pending');" in source
    assert "setText('#snapshot-page', navigationPageKey(state.page));" in source
    assert (
        "setText('#snapshot-mode', compact(snapshot.snapshot_mode, 'snapshot pending'));"
        in source
    )
    assert "data-current-page" in source
    assert "data-current-mode" in source
    assert "data-page-label" in source
    assert "value.startsWith('command ') ? 'local snapshot command' : value" in source
    assert ".keys kbd" in styles
    assert ".rail-kv" in styles


def test_tauri_dashboard_json_command_targets_focusable_output() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert 'id="snapshot-json-output"' in source
    assert 'data-testid="snapshot-json-output"' in source
    assert 'tabindex="0"' in source
    assert 'role="textbox"' in source
    assert 'aria-readonly="true"' in source
    assert "const shouldFocusCommand = await applyCommand(raw);" in source
    assert "if (shouldFocusCommand !== false) input.focus();" in source
    assert "qs('#snapshot-json-output')?.focus?.()" in source
    assert "return false;" in source


def test_tauri_dashboard_quit_command_closes_native_window() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )
    rust_source = Path("apps/radar-desktop/src/main.rs").read_text(
        encoding="utf-8",
    )

    assert "['q', 'quit', 'exit'].includes(command)" in source
    assert "setCommandStatus('Closing MarketRadar Trading Workbench.');" in source
    assert "await closeDashboardWindow();" in source
    assert "await invoke('close_dashboard_window');" in source
    assert "fn close_dashboard_window(app: AppHandle) -> Result<(), String>" in rust_source
    assert ".get_webview_window(\"main\")" in rust_source
    assert "close_dashboard_window" in rust_source


def test_tauri_dashboard_escape_focuses_command_from_form_controls() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    escape_handler = source.index("if (event.key === 'Escape')")
    form_control_guard = source.index("if (isFormControlTarget(event.target))")

    assert escape_handler < form_control_guard
    assert "function isFormControlTarget(target)" in source
    assert "target instanceof HTMLInputElement" in source
    assert "qs('#command-input').focus();" in source
    assert "setCommandStatus('Command box focused.');" in source


def test_tauri_dashboard_global_keys_match_rust_tui() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )
    rust_source = Path("apps/radar-desktop/src/main.rs").read_text(
        encoding="utf-8",
    )
    tui_source = Path("crates/radar-tui/src/main.rs").read_text(
        encoding="utf-8",
    )

    assert "Some('n') => return KeyAction::NextPage" in tui_source
    assert "Some('p') => return KeyAction::PreviousPage" in tui_source
    assert "KeyCode::Tab | KeyCode::Right | KeyCode::Down | KeyCode::PageDown" in tui_source
    assert "KeyCode::BackTab | KeyCode::Left | KeyCode::Up | KeyCode::PageUp" in tui_source
    assert "'q' => KeyAction::Quit" in tui_source
    assert "'r' => KeyAction::Refresh" in tui_source
    assert "'j' => KeyAction::NextPage" in tui_source
    assert "'k' => KeyAction::PreviousPage" in tui_source
    assert "event.ctrlKey && event.key.toLowerCase() === 'n'" in source
    assert "event.ctrlKey && event.key.toLowerCase() === 'p'" in source
    assert "event.key === 'Tab' && !shouldPreserveNativeTab(event)" in source
    assert "stepPage(event.shiftKey ? -1 : 1);" in source
    assert "const commandModifier = event.ctrlKey || event.altKey || event.metaKey;" in source
    assert "if (plainKey === 'q')" in source
    assert "if (plainKey === 'r')" in source
    assert "if (plainKey === 'j')" in source
    assert "if (plainKey === 'k')" in source
    assert "setCommandStatus('Closing MarketRadar Trading Workbench.');" in source
    assert "closeDashboardWindow();" in source
    assert "setCommandStatus('Refreshed.');" in source
    assert "refreshSnapshot();" in source
    assert "stepPage(1);" in source
    assert "stepPage(-1);" in source
    assert "Ctrl+N moves forward; Ctrl+P moves backward" in rust_source
    assert "ArrowRight/ArrowDown/Tab/J moves forward" in rust_source
    assert "ArrowLeft/ArrowUp/Shift+Tab/K moves backward" in rust_source
    assert "F5 or R refreshes the local snapshot" in rust_source
    assert "Q closes the native desktop window" in rust_source


def test_tauri_dashboard_exposes_cli_command_reference_families() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "data-testid=\"command-reference\"" in source
    assert "const fallbackCommandReference = [" in source
    assert "function commandReference()" in source
    assert "state.config?.automation?.command_box_commands" in source
    assert "item.safety" in source
    assert "item.route" in source
    assert "commandReference().map" in source
    assert "function catalogLabel(value)" in source
    assert "<th>Safety</th><th>Route</th>" in source
    assert 'data-testid="command-reference-row"' in source
    assert 'data-command="${escapeHtml(command)}"' in source
    assert 'data-safety="${escapeHtml(safety)}"' in source
    assert 'data-route="${escapeHtml(route)}"' in source
    assert 'tabindex="0"' in source
    assert "Safety ${catalogLabel(safety)}. Route ${catalogLabel(route)}." in source
    assert "class=\"command-route\"" in source
    assert "['themes', 'themes']" in source
    assert "['validation', 'validation']" in source
    assert "['v', 'costs']" in source
    assert "run: () => renderPlatformModulePage('run', snapshot)" in source
    assert "themes: () => renderPlatformModulePage('themes', snapshot)" in source
    assert "validation: () => renderPlatformModulePage('validation', snapshot)" in source
    assert "costs: () => renderPlatformModulePage('costs', snapshot)" in source
    assert "ops: () => renderPlatformModulePage('ops', snapshot)" in source
    assert "telemetry: () => renderPlatformModulePage('telemetry', snapshot)" in source
    assert "renderCosts" in source
    assert "const powershellCommandPrefixes = new Set" in source
    assert "'market-bars'" in source
    assert "'priced-in-queue'" in source
    assert "PowerShell command, not a dashboard command." in source
    assert "Run this in a normal PowerShell prompt" in source
    for command in (
        "themes / validation / costs / features",
        "export full / export current",
        "batch SOURCE / batch SOURCE all / batch SOURCE execute 3",
        "catalyst-radar COMMAND",
        "bars manual template/import",
        "options template/validate/import",
        "cik template/validate/import",
        "order-ticket preview / record",
        "ledger coverage / record",
        "outcome coverage / update",
    ):
        assert command in source


def test_tauri_dashboard_batch_commands_are_first_class_safe_ops_commands() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    batch_handler = source.index(
        "['batch', 'batches', 'source-batch', 'source-batches'].includes(command)",
    )
    guarded_handler = source.index("const guardedMessage = guardedCommandMessage")

    assert batch_handler < guarded_handler
    assert "function parseSourceBatchCommand(value)" in source
    assert "function normalizeSourceName(value)" in source
    assert "sourceAliases = new Map" in source
    assert "['events', 'catalyst_events']" in source
    assert "['broker', 'broker_context']" in source
    assert "function sourceBatchCommandMessage(value)" in source
    assert "function sourceWorkflowStep(source)" in source
    assert "priced_in_source_workflow" in source
    assert "catalyst-radar priced-in-source-batches --source ${source} --all" in source
    assert "--execute-batches ${parsed.maxBatches}" in source
    assert "--execute-next" in source
    assert "batch all is plan-only" in source
    assert "No provider calls made in the desktop app" in source
    assert "provider_calls=0 in the desktop app" in source


def test_tauri_dashboard_gap_filters_reject_unsupported_values() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "const allowedSourceGaps = new Set" in source
    assert "const allowedDecisionGaps = new Set" in source
    assert "const decisionGapAliases = new Map" in source
    assert "function validatedListFilter(value, aliases, allowed, commandLabel)" in source
    assert "function normalizeFilterName(value, aliases)" in source
    assert "Unsupported ${commandLabel} value" in source
    assert "No calls made; filter unchanged" in source
    assert "validatedListFilter(value, sourceAliases, allowedSourceGaps, 'source-gap')" in source
    assert (
        "validatedListFilter(value, decisionGapAliases, allowedDecisionGaps, "
        "'decision-gap')"
    ) in source
    assert "if (sourceGap.error)" in source
    assert "if (decisionGap.error)" in source
    assert "['candidate_packet', 'candidate_packet']" in source
    assert "['broker', 'broker_context']" in source


def test_tauri_dashboard_numeric_and_time_commands_reject_invalid_values() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "function isPositiveIntegerText(value)" in source
    assert "function parseAvailableAtCommand(value)" in source
    assert "function isIsoDateTimeText(value)" in source
    assert "if (!isPositiveIntegerText(value))" in source
    assert "Usage: offset ROW." in source
    assert "Usage: limit 1-200." in source
    assert "const availableAt = parseAvailableAtCommand(value);" in source
    assert "if (availableAt.error)" in source
    assert "Invalid timestamp. No calls made; filter unchanged." in source
    assert "state.availableAt = availableAt.value;" in source


def test_tauri_dashboard_next_command_stops_at_scan_end() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "function paginationStateFromSnapshot()" in source
    assert "priced_in_queue" in source
    assert "total_count" in source
    assert "queueFilters?.limit" in source
    assert "const pagination = paginationStateFromSnapshot();" in source
    assert "const nextOffset = pagination.offset + Math.max(1, pagination.limit);" in source
    assert "if (pagination.total && nextOffset >= pagination.total)" in source
    assert "Already at the end of the current scan filter." in source
    assert "state.scanOffset = nextOffset;" in source


def test_tauri_dashboard_clear_filters_preserves_row_limit() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "const preservedLimit = qs('#filter-limit').value || '50';" in source
    assert "qs('#filter-limit').value = preservedLimit;" in source
    assert "state.sourceGap = [];" in source
    assert "state.decisionGap = [];" in source
    assert "state.usefulness = null;" in source
    assert "state.scanOffset = 0;" in source
    assert "qs('#filter-limit').value = '50';" not in source


def test_tauri_dashboard_optional_filters_clear_case_insensitively() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "function isOptionalClearValue(value, includeAny = false)" in source
    assert "function normalizeOptionalFilterValue(value)" in source
    assert "state.usefulness = normalizeOptionalFilterValue(value);" in source
    assert "isOptionalClearValue(normalized, true)" in source
    assert "state.alertStatus = isOptionalClearValue(value) ? null : value;" in source
    assert "state.alertRoute = isOptionalClearValue(value) ? null : value;" in source
    assert "['', 'all', 'any', 'none']" in source
    assert "['', 'all', 'none']" in source


def test_tauri_dashboard_ready_command_exposes_filter_state_for_automation() -> None:
    html = Path("apps/radar-desktop/frontend/index.html").read_text(
        encoding="utf-8",
    )
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert 'data-testid="filter-state"' in html
    assert "function updateFilterState()" in source
    assert "function updateCommandState()" in source
    assert "function updateAutomationJson(" in source
    assert "function automationFilterState()" in source
    assert "state.lastCommand = raw || 'refresh';" in source
    assert "['last_command', state.lastCommand || 'none']" in source
    assert "['page', state.page || 'overview']" in source
    assert "['nav', navigationPageKey(state.page || 'overview')]" in source
    assert "setText(\n    '#command-state'," in source
    assert "setText('#automation-json', JSON.stringify(payload));" in source
    assert "contract_version: state.config?.automation?.contract_version" in source
    assert "filters: automationFilterState()" in source
    assert "last_command: state.lastCommand || 'none'" in source
    assert "provider_calls: Number.isFinite(providerCalls) ? providerCalls : 0" in source
    assert "['scan_mode', filterState.scan_mode]" in source
    assert "['usefulness', filterState.usefulness]" in source
    assert "setText('#filter-state'" in source
    assert "updateFilterState();" in source
    assert "updateCommandState();" in source
    assert "updateAutomationJson();" in source
    assert (
        "['d', 'ready', 'decision', 'decision-ready', 'decision_ready'].includes(command)"
        in source
    )
    assert "setCommandStatus('Decision-ready review filter.');" in source
    assert "await setPage('review');" in source
    assert "Apply decision-useful, full universe, mismatch, and stock-only scan filters." in source
    ready_branch = source.index(
        "['d', 'ready', 'decision', 'decision-ready', 'decision_ready'].includes(command)",
    )
    generic_page_branch = source.index("const page = pageFromCommand(raw);")
    assert ready_branch < generic_page_branch


def test_tauri_dashboard_run_execute_uses_backend_command() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "async function handleRunCommand(value)" in source
    assert "command === 'run'" in source
    assert "await handleRunCommand(value);" in source
    assert "invoke('execute_dashboard_command'" in source
    assert "command: 'run execute'" in source
    assert "function radarRunResultMessage(result)" in source
    assert "Radar run finished: status=" in source
    execute_set = source.split("const executeClassCommands = new Set([", 1)[1].split(
        "]);",
        1,
    )[0]
    assert "'run execute'" not in execute_set


def test_tauri_dashboard_local_commands_use_backend_command() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    backend_handler = source.index("if (backendCommandWords.has(command))")
    guarded_handler = source.index("const guardedMessage = guardedCommandMessage")
    execute_set = source.split("const executeClassCommands = new Set([", 1)[1].split(
        "]);",
        1,
    )[0]

    assert "const backendCommandWords = new Set" in source
    for command in (
        "'action'",
        "'eval-triggers'",
        "'feedback'",
        "'ledger'",
        "'outcome'",
        "'order-ticket'",
        "'order_ticket'",
        "'paper-decision'",
        "'ticket'",
        "'trigger'",
        "'value-ledger'",
    ):
        assert command in source
        assert command not in execute_set
    assert backend_handler < guarded_handler
    assert "await handleBackendDashboardCommand(raw);" in source
    assert "async function handleBackendDashboardCommand(raw)" in source
    assert "command: raw" in source
    assert "function applyBackendDashboardResult(result)" in source
    assert "function applyBackendDashboardFilters(filters)" in source
    assert "function dashboardCommandResultMessage(result)" in source
    assert "Running dashboard command through backend" in source
    assert "Dashboard command failed" in source
    assert "costCommandMessage" not in source
    assert "Run guarded local Broker or Alert commands through the dashboard backend" in source
    assert "Preview or record the active plan as a local paper decision" in source
    assert "Run guarded local value-ledger commands through the dashboard backend" in source
    assert "Run guarded local value-outcome commands through the dashboard backend" in source


def test_tauri_dashboard_provider_preview_commands_use_backend_with_execute_guard() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    boundary_handler = source.index(
        "const boundaryMessage = guardedExecutionBoundaryMessage(normalized);",
    )
    backend_handler = source.index("if (backendCommandWords.has(command))")
    execute_set = source.split("const executeClassCommands = new Set([", 1)[1].split(
        "]);",
        1,
    )[0]

    assert boundary_handler < backend_handler
    assert "function guardedExecutionBoundaryMessage(normalized)" in source
    assert "function providerBackendCommandWords()" in source
    assert (
        "providerBackendCommandWords().has(command) && /\\b(?:execute|confirm)\\b/.test(normalized)"
        in source
    )
    for command in (
        "'agent'",
        "'bars'",
        "'market-bars'",
        "'options'",
        "'options-flow'",
        "'cik'",
        "'sec'",
        "'sec-cik'",
    ):
        assert command in source
    for command in (
        "'agent execute'",
        "'bars manual import execute'",
        "'bars saved capture confirm'",
        "'bars saved import execute'",
        "'cik import execute'",
        "'options import execute'",
    ):
        assert command in execute_set
    assert "Preview market-bar repair commands through the dashboard backend" in source
    assert "Preview saved grouped-daily commands through the dashboard backend" in source
    assert "Preview point-in-time options commands through the dashboard backend" in source
    assert "Preview SEC CIK override commands through the dashboard backend" in source
    assert "Preview agent gates through the dashboard backend" in source


def test_tauri_dashboard_provider_confirm_commands_stay_external_boundaries() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    boundary_function = source.split("function guardedExecutionBoundaryMessage(normalized)", 1)[
        1
    ].split("function providerBackendCommandWords()", 1)[0]

    assert "/\\b(?:execute|confirm)\\b/.test(normalized)" in boundary_function
    assert "providerBackendCommandWords().has(command)" in boundary_function
    assert "Execute commands stay outside dashboard browsing" in boundary_function
    assert "confirm variants stay external" in source


def test_tauri_dashboard_exposes_keyboard_row_detail_navigation() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "function renderCandidateDetail(snapshot, ticker)" in source
    assert "function renderAlertDetail(snapshot, alertId)" in source
    assert 'data-testid="candidate-detail"' in source
    assert 'data-testid="alert-detail"' in source
    assert 'data-testid="detail-summary"' in source
    assert 'data-open-key="${escapeHtml(key)}"' in source
    assert 'tabindex="0"' in source
    assert 'role="button"' in source
    assert "function bindQueueRows()" in source
    assert "if (!['Enter', ' '].includes(event.key)) return;" in source
    assert "await openDashboardTarget(target);" in source
    assert "page: `candidate:${ticker}`" in source
    assert "page: `alert:${alertId}`" in source
    assert "Opened candidate ${ticker}. No calls." in source
    assert "Opened alert ${alertId}. No calls." in source


def test_tauri_dashboard_detail_pages_expose_parent_navigation() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "const activePage = navigationPageKey(state.page);" in source
    assert 'aria-selected="${page.key === activePage}"' in source
    assert 'aria-current="${page.key === activePage ? \'page\' : \'false\'}"' in source
    assert 'tabindex="${page.key === activePage ? \'0\' : \'-1\'}"' in source
    assert "function pageLabelFor(page, pageInfo)" in source
    assert "function isDynamicDetailPage(page)" in source
    assert "function navigationPageKey(page)" in source
    assert "if (page.startsWith('candidate:')) return 'candidates';" in source
    assert "if (page.startsWith('alert:')) return 'alerts';" in source
    assert "main.dataset.currentNavPage = navPage;" in source
    assert "`nav=${navPage}`" in source
    assert "pages.indexOf(navigationPageKey(state.page))" in source


def test_tauri_dashboard_command_aliases_cover_legacy_page_words() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    for alias in (
        "['learn', 'tutorial']",
        "['home', 'overview']",
        "['mail', 'overview']",
        "['evidence_gaps', 'readiness']",
        "['call_plan', 'run']",
        "['safe_run', 'run']",
        "['candidate_review', 'candidates']",
        "['11', 'review']",
        "['decision_ready', 'review']",
        "['10', 'agent']",
        "['theme_rows', 'themes']",
        "['value_validation', 'validation']",
        "['value_report', 'costs']",
    ):
        assert alias in source
