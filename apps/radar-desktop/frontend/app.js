const tauriInvoke = window.__TAURI__?.core?.invoke;

const state = {
  config: null,
  snapshot: null,
  page: 'overview',
  loading: false,
  lastRefresh: null,
  scanOffset: 0,
  sourceGap: [],
  decisionGap: [],
  usefulness: null,
  availableAt: null,
  alertStatus: null,
  alertRoute: null,
  lastCommand: 'none',
  pendingLocalWrite: null,
};

const keyAliases = new Map([
  ['0', 'tutorial'],
  ['learn', 'tutorial'],
  ['start', 'tutorial'],
  ['tut', 'tutorial'],
  ['1', 'overview'],
  ['home', 'overview'],
  ['insight', 'overview'],
  ['insights', 'overview'],
  ['mail', 'overview'],
  ['messages', 'overview'],
  ['command-center', 'overview'],
  ['command_center', 'overview'],
  ['workbench', 'overview'],
  ['portfolio', 'portfolio'],
  ['portfolio-monitor', 'portfolio'],
  ['portfolio_monitor', 'portfolio'],
  ['market', 'market-radar'],
  ['market-radar', 'market-radar'],
  ['market_radar', 'market-radar'],
  ['radar', 'market-radar'],
  ['scout', 'market-radar'],
  ['scanner', 'market-radar'],
  ['trade', 'trade-planner'],
  ['trade-plan', 'trade-planner'],
  ['trade_plan', 'trade-planner'],
  ['trade-planner', 'trade-planner'],
  ['trade_planner', 'trade-planner'],
  ['planner', 'trade-planner'],
  ['risk', 'risk-desk'],
  ['risk-desk', 'risk-desk'],
  ['risk_desk', 'risk-desk'],
  ['risk-controls', 'risk-desk'],
  ['paper', 'paper-trading'],
  ['paper-trade', 'paper-trading'],
  ['paper_trade', 'paper-trading'],
  ['paper-trading', 'paper-trading'],
  ['paper_trading', 'paper-trading'],
  ['backtest', 'backtest'],
  ['backtests', 'backtest'],
  ['replay', 'backtest'],
  ['replays', 'backtest'],
  ['2', 'readiness'],
  ['blockers', 'readiness'],
  ['evidence', 'readiness'],
  ['evidence-gaps', 'readiness'],
  ['evidence_gaps', 'readiness'],
  ['gaps', 'readiness'],
  ['3', 'run'],
  ['call_plan', 'run'],
  ['plan', 'run'],
  ['safe', 'run'],
  ['safe_run', 'run'],
  ['4', 'candidates'],
  ['candidate', 'candidates'],
  ['candidate-review', 'candidates'],
  ['candidate_review', 'candidates'],
  ['11', 'review'],
  ['decision', 'review'],
  ['decisions', 'review'],
  ['decision-review', 'review'],
  ['decision_ready', 'review'],
  ['5', 'alerts'],
  ['alert', 'alerts'],
  ['6', 'ipo'],
  ['s1', 'ipo'],
  ['7', 'broker'],
  ['broker-desk', 'broker'],
  ['broker_desk', 'broker'],
  ['8', 'ops'],
  ['9', 'telemetry'],
  ['10', 'agent'],
  ['agent-cockpit', 'agent'],
  ['agent_cockpit', 'agent'],
  ['o', 'overview'],
  ['e', 'readiness'],
  ['g', 'readiness'],
  ['s', 'run'],
  ['c', 'candidates'],
  ['d', 'review'],
  ['a', 'alerts'],
  ['i', 'ipo'],
  ['b', 'broker'],
  ['t', 'telemetry'],
  ['theme', 'themes'],
  ['themes', 'themes'],
  ['theme_row', 'themes'],
  ['theme_rows', 'themes'],
  ['valid', 'validation'],
  ['validate', 'validation'],
  ['validation', 'validation'],
  ['value_validation', 'validation'],
  ['v', 'costs'],
  ['cost', 'costs'],
  ['costs', 'costs'],
  ['value', 'costs'],
  ['value_report', 'costs'],
  ['f', 'features'],
  ['journal', 'journal'],
  ['journals', 'journal'],
  ['trade-journal', 'journal'],
  ['trade_journal', 'journal'],
  ['decision-journal', 'journal'],
  ['decision_journal', 'journal'],
  ['h', 'help'],
  ['?', 'help'],
]);

const pagePaths = {
  portfolio: [['broker'], ['broker', 'exposure'], ['portfolio'], ['runtime_context']],
  'market-radar': [['priced_in_queue'], ['candidates'], ['alerts'], ['ipo_s1'], ['themes']],
  'trade-planner': [['validation'], ['decision_cards'], ['candidate_packets'], ['value_report']],
  'risk-desk': [['broker'], ['policy'], ['portfolio_impacts'], ['validation']],
  'paper-trading': [['validation'], ['paper_trading'], ['paper_trades'], ['value_outcomes']],
  backtest: [['validation'], ['validation', 'latest_run'], ['validation', 'report'], ['telemetry']],
  candidates: [['priced_in_queue'], ['candidates'], ['decision_cards'], ['candidate_packets']],
  review: [['priced_in_answer'], ['priced_in_answer', 'decision_readiness'], ['priced_in_queue']],
  ipo: [['ipo_s1'], ['events']],
  themes: [['themes'], ['priced_in_queue']],
  features: [['feature_inventory'], ['trading_workbench', 'modules', 'features']],
  readiness: [['readiness'], ['real_results'], ['full_market_trust_gate']],
  run: [['call_plan'], ['radar_run'], ['operator_next_step']],
  broker: [['broker'], ['runtime_context']],
  ops: [['ops_health'], ['runtime_context'], ['provider_preflight']],
  telemetry: [['telemetry'], ['telemetry_coverage'], ['raw_telemetry']],
  agent: [['agent_brief'], ['runtime_context']],
  validation: [['validation'], ['validation', 'latest_run'], ['validation', 'report']],
  costs: [['costs'], ['value_ledger'], ['value_outcomes'], ['value_report']],
  journal: [['value_ledger'], ['value_outcomes'], ['feedback'], ['telemetry']],
};

const executeClassCommands = new Set([
  'agent execute',
  'bars manual import execute',
  'bars saved capture confirm',
  'bars saved import execute',
  'batch execute',
  'cik import execute',
  'options import execute',
]);

const backendCommandWords = new Set([
  'action',
  'agent',
  'agent-brief',
  'agents',
  'bar',
  'bars',
  'cik',
  'ciks',
  'eval-triggers',
  'evaluate-triggers',
  'feedback',
  'ledger',
  'market-bars',
  'market_bars',
  'option',
  'options',
  'options-flow',
  'options_flow',
  'outcome',
  'outcomes',
  'order-ticket',
  'order_ticket',
  'paper-decision',
  'paper_decision',
  'sec',
  'sec-cik',
  'sec_cik',
  'ticket',
  'trigger',
  'value-ledger',
  'value-outcome',
  'value_ledger',
  'value_outcome',
]);

const sourceAliases = new Map([
  ['bars', 'market_bars'],
  ['market', 'market_bars'],
  ['market_data', 'market_bars'],
  ['events', 'catalyst_events'],
  ['event', 'catalyst_events'],
  ['catalysts', 'catalyst_events'],
  ['catalyst', 'catalyst_events'],
  ['text', 'local_text'],
  ['local', 'local_text'],
  ['news', 'local_text'],
  ['narrative', 'local_text'],
  ['option', 'options'],
  ['options_flow', 'options'],
  ['theme', 'theme_peer_sector'],
  ['themes', 'theme_peer_sector'],
  ['peer', 'theme_peer_sector'],
  ['sector', 'theme_peer_sector'],
  ['broker', 'broker_context'],
  ['schwab', 'broker_context'],
  ['portfolio', 'broker_context'],
]);

const allowedSourceGaps = new Set([
  'market_bars',
  'catalyst_events',
  'local_text',
  'options',
  'theme_peer_sector',
  'broker_context',
]);

const decisionGapAliases = new Map([
  ['packet', 'candidate_packet'],
  ['candidate_packet', 'candidate_packet'],
  ['candidate_packets', 'candidate_packet'],
  ['card', 'decision_card'],
  ['decision_card', 'decision_card'],
  ['decision_cards', 'decision_card'],
  ['broker', 'broker_context'],
  ['schwab', 'broker_context'],
  ['portfolio', 'broker_context'],
  ['options_flow', 'options'],
]);

const allowedDecisionGaps = new Set([
  'candidate_packet',
  'decision_card',
  'options',
  'broker_context',
]);

const powershellCommandPrefixes = new Set([
  'build-decision-cards',
  'build-packets',
  'ingest-csv',
  'ingest-polygon',
  'market-bars',
  'priced-in-queue',
]);

const fallbackCommandReference = [
  ['0..9, Ctrl+A, Ctrl+N/P, Tab, J/K, V, F, ?, or page name', 'Switch pages; Ctrl+A opens Agent and V opens Costs.'],
  ['portfolio / market-radar / trade-planner / risk-desk', 'Open the trading workbench platform tools without provider calls.'],
  ['paper-trading / broker-desk / backtest / journal / agent-cockpit', 'Open execution, replay, journal, and agent surfaces with live trading disabled.'],
  ['themes / validation / costs / features', 'Open local evidence pages for clustered themes, validation, costs, and feature inventory.'],
  ['setup / first', 'Show the first setup command and where to run it.'],
  ['open #|TICKER', 'Open a row from Candidate Review or show its next command.'],
  ['ticker SYMBOL|all', 'Filter ticker-aware pages.'],
  ['available-at ISO|latest', 'Set or clear the point-in-time cutoff.'],
  ['ready / full / mismatches / stocks', 'Apply decision-useful, full universe, mismatch, and stock-only scan filters.'],
  ['usefulness STATUS|all', 'Filter Inbox by usefulness verdict.'],
  ['source-gap SOURCE|all', 'Filter Inbox by missing or stale source evidence.'],
  ['decision-gap GAP|all', 'Filter Inbox by missing decision evidence.'],
  ['next / prev / offset ROW / limit 1-200', 'Page through current Inbox scan rows.'],
  ['export full / export current', 'Show JSON export commands without running them.'],
  ['batch SOURCE / batch SOURCE all / batch SOURCE execute 3', 'Plan source fills or show the external execution boundary.'],
  ['catalyst-radar COMMAND', 'Show where to run full CLI commands without executing them in the dashboard.'],
  ['bars manual template/import', 'Preview market-bar repair commands through the dashboard backend; execute stays external.'],
  ['bars saved capture/validate/import', 'Preview saved grouped-daily commands through the dashboard backend; confirm/execute stays external.'],
  ['options template/validate/import', 'Preview point-in-time options commands through the dashboard backend; execute stays external.'],
  ['cik template/validate/import', 'Preview SEC CIK override commands through the dashboard backend; execute stays external.'],
  ['agent / agent execute', 'Preview agent gates through the dashboard backend; execute stays external.'],
  ['alert-status STATUS|all / alert-route ROUTE|all', 'Filter alerts.'],
  ['run / run execute', 'Open Safe Run or show the capped run execution boundary.'],
  ['action / trigger / ticket / feedback', 'Run guarded local Broker or Alert commands through the dashboard backend.'],
  ['order-ticket preview / record', 'Preview or save the active plan as a blocked local broker ticket.'],
  ['paper-decision preview / execute', 'Preview or record the active plan as a local paper decision.'],
  ['ledger coverage / record', 'Run guarded local value-ledger commands through the dashboard backend.'],
  ['outcome coverage / update', 'Run guarded local value-outcome commands through the dashboard backend.'],
  ['json', 'Open and focus the raw JSON snapshot.'],
  ['clear-filters / refresh / q', 'Reset filters, reload, or close the native window.'],
];

const fallbackPlatformModules = [
  ['command-center', 'Command Center', 'overview', 'Operating home for account state, safe action, and agent handoff.', 'active'],
  ['portfolio', 'Portfolio', 'portfolio', 'Positions, exposure, cash, watch intent, and broker context.', 'route_ready'],
  ['market-radar', 'Market Radar', 'market-radar', 'Scouted catalysts, mispricing queues, evidence gaps, and watchlists.', 'active'],
  ['candidate-review', 'Candidate Review', 'candidates', 'Single-name evidence queue and candidate packet review.', 'route_ready'],
  ['decision-review', 'Decision Review', 'review', 'Priced-in answer status and decision-readiness gates.', 'route_ready'],
  ['evidence-gaps', 'Evidence Gaps', 'readiness', 'Readiness blockers, source gaps, and reliance gates.', 'route_ready'],
  ['safe-run', 'Safe Run', 'run', 'Provider-call budget, run guardrails, and execution gates.', 'route_ready'],
  ['trade-planner', 'Trade Planner', 'trade-planner', 'Candidate sizing, thesis, reward/risk, and decision-card assembly.', 'route_ready'],
  ['risk-desk', 'Risk Desk', 'risk-desk', 'Policy gates, portfolio impact, concentration, and hard blocks.', 'route_ready'],
  ['paper-trading', 'Paper Trading', 'paper-trading', 'Paper-only tickets, fills, outcomes, and shadow validation.', 'preview_only'],
  ['broker-desk', 'Broker Desk', 'broker', 'Read-only broker connection, order-ticket previews, and sync boundaries.', 'read_only'],
  ['backtest', 'Backtest / Replay', 'backtest', 'Historical replay, shadow-mode validation, and strategy evidence.', 'route_ready'],
  ['validation', 'Validation', 'validation', 'Shadow, paper, and useful-alert validation evidence.', 'route_ready'],
  ['alerts', 'Alerts', 'alerts', 'Research notifications, watch triggers, and operator routing.', 'active'],
  ['ipo-s1', 'IPO/S-1', 'ipo', 'Primary-source IPO registration evidence and risk flags.', 'route_ready'],
  ['themes', 'Themes', 'themes', 'Clustered catalyst patterns and repeated theme context.', 'route_ready'],
  ['features', 'Features', 'features', 'Feature inventory, evidence routing, and platform surface coverage.', 'route_ready'],
  ['costs', 'Costs', 'costs', 'Budget ledger, provider spend, and decision-support value.', 'route_ready'],
  ['ops', 'Ops', 'ops', 'Provider health, runtime context, and execution readiness.', 'route_ready'],
  ['telemetry', 'Telemetry', 'telemetry', 'Audit tape, coverage gaps, and agent action traceability.', 'route_ready'],
  ['journal', 'Journal', 'journal', 'Decision notes, feedback, value ledger, and outcome review.', 'route_ready'],
  ['agent-cockpit', 'Agent Cockpit', 'agent', 'Agent brief, proposed tool use, budget gates, and execution review.', 'preview_only'],
];

function commandReference() {
  const manifestCommands = state.config?.automation?.command_box_commands;
  if (Array.isArray(manifestCommands) && manifestCommands.length) {
    return manifestCommands.map((item) => [
      item.command,
      item.meaning,
      item.safety,
      item.route,
    ]);
  }
  return fallbackCommandReference.map(([command, meaning]) => [command, meaning, '', '']);
}

function platformManifest() {
  return state.config?.platform || {
    name: 'MarketRadar Trading Workbench',
    primary_tool: 'market-radar',
    modules: fallbackPlatformModules.map(([key, label, page, role, status]) => ({
      key,
      label,
      page,
      role,
      status,
      source: 'local dashboard snapshot',
      test_id: `platform-tool-${key}`,
      next_action: 'Open the local tool page.',
    })),
    execution_boundary: {
      live_trading_enabled: false,
      broker_order_submission: 'disabled',
      autonomous_execution: 'disabled',
      paper_trading: 'preview_only',
      provider_calls_for_browsing: 0,
    },
  };
}

function platformModules() {
  const modules = platformManifest().modules;
  return Array.isArray(modules) ? modules : [];
}

function platformBoundary() {
  return platformManifest().execution_boundary || {};
}

function platformModuleForPage(pageKey) {
  return platformModules().find((module) => module.page === pageKey || module.key === pageKey);
}

function tradingWorkbenchSnapshot(snapshot = state.snapshot || {}) {
  return snapshot?.trading_workbench || {};
}

function workbenchOperatorState(snapshot = state.snapshot || {}) {
  const operator = tradingWorkbenchSnapshot(snapshot)?.operator_state;
  return operator && typeof operator === 'object' ? operator : { state_cards: [] };
}

function workbenchExecutionSandbox(snapshot = state.snapshot || {}) {
  const sandbox = tradingWorkbenchSnapshot(snapshot)?.execution_sandbox;
  return sandbox && typeof sandbox === 'object' ? sandbox : { lanes: [] };
}

function workbenchExecutionSandboxForPage(pageKey, snapshot = state.snapshot || {}) {
  const sandbox = workbenchExecutionSandbox(snapshot);
  const lanes = Array.isArray(sandbox.lanes) ? sandbox.lanes : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return lanes;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return lanes.filter((lane) => (
    keys.has(lane?.module) || keys.has(lane?.target_page)
  ));
}

function workbenchDecisionBrief(snapshot = state.snapshot || {}) {
  const brief = tradingWorkbenchSnapshot(snapshot)?.decision_brief;
  return brief && typeof brief === 'object' ? brief : {};
}

function workbenchScenarioMatrix(snapshot = state.snapshot || {}) {
  const matrix = tradingWorkbenchSnapshot(snapshot)?.scenario_matrix;
  return matrix && typeof matrix === 'object' ? matrix : { scenarios: [] };
}

function workbenchScenarioMatrixForPage(pageKey, snapshot = state.snapshot || {}) {
  const matrix = workbenchScenarioMatrix(snapshot);
  const rows = Array.isArray(matrix.scenarios) ? matrix.scenarios : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return rows;
  const pages = new Set(['trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? rows : [];
}

function workbenchPortfolioImpact(snapshot = state.snapshot || {}) {
  const impact = tradingWorkbenchSnapshot(snapshot)?.portfolio_impact_preview;
  return impact && typeof impact === 'object' ? impact : { checks: [], exposures: [] };
}

function workbenchPortfolioImpactForPage(pageKey, snapshot = state.snapshot || {}) {
  const impact = workbenchPortfolioImpact(snapshot);
  const checks = Array.isArray(impact.checks) ? impact.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['portfolio', 'trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchPositionSizing(snapshot = state.snapshot || {}) {
  const sizing = tradingWorkbenchSnapshot(snapshot)?.position_sizing;
  return sizing && typeof sizing === 'object' ? sizing : { checks: [] };
}

function workbenchPositionSizingForPage(pageKey, snapshot = state.snapshot || {}) {
  const sizing = workbenchPositionSizing(snapshot);
  const checks = Array.isArray(sizing.checks) ? sizing.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['portfolio', 'trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchCapitalAllocation(snapshot = state.snapshot || {}) {
  const allocation = tradingWorkbenchSnapshot(snapshot)?.capital_allocation;
  return allocation && typeof allocation === 'object' ? allocation : { checks: [] };
}

function workbenchCapitalAllocationForPage(pageKey, snapshot = state.snapshot || {}) {
  const allocation = workbenchCapitalAllocation(snapshot);
  const checks = Array.isArray(allocation.checks) ? allocation.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['portfolio', 'trade-planner', 'risk-desk', 'paper-trading', 'broker', 'agent']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchOrderTicketDraft(snapshot = state.snapshot || {}) {
  const draft = tradingWorkbenchSnapshot(snapshot)?.order_ticket_draft;
  return draft && typeof draft === 'object' ? draft : { checks: [] };
}

function workbenchOrderTicketDraftForPage(pageKey, snapshot = state.snapshot || {}) {
  const draft = workbenchOrderTicketDraft(snapshot);
  const checks = Array.isArray(draft.checks) ? draft.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchPaperTradePreview(snapshot = state.snapshot || {}) {
  const preview = tradingWorkbenchSnapshot(snapshot)?.paper_trade_preview;
  return preview && typeof preview === 'object' ? preview : { checks: [] };
}

function workbenchPaperTradePreviewForPage(pageKey, snapshot = state.snapshot || {}) {
  const preview = workbenchPaperTradePreview(snapshot);
  const checks = Array.isArray(preview.checks) ? preview.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchPretradeCompliance(snapshot = state.snapshot || {}) {
  const compliance = tradingWorkbenchSnapshot(snapshot)?.pretrade_compliance;
  return compliance && typeof compliance === 'object' ? compliance : { checks: [] };
}

function workbenchPretradeComplianceForPage(pageKey, snapshot = state.snapshot || {}) {
  const compliance = workbenchPretradeCompliance(snapshot);
  const checks = Array.isArray(compliance.checks) ? compliance.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set(['portfolio', 'trade-planner', 'risk-desk', 'paper-trading', 'broker', 'agent']);
  return pages.has(pageKey) ? checks : [];
}

function workbenchTradeReadinessBrief(snapshot = state.snapshot || {}) {
  const brief = tradingWorkbenchSnapshot(snapshot)?.trade_readiness_brief;
  return brief && typeof brief === 'object' ? brief : { checks: [] };
}

function workbenchTradeReadinessBriefForPage(pageKey, snapshot = state.snapshot || {}) {
  const brief = workbenchTradeReadinessBrief(snapshot);
  const checks = Array.isArray(brief.checks) ? brief.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return checks;
  const pages = new Set([
    'portfolio',
    'market-radar',
    'trade-planner',
    'risk-desk',
    'paper-trading',
    'broker',
    'backtest',
    'validation',
    'journal',
    'agent',
  ]);
  return pages.has(pageKey) ? checks : [];
}

function workbenchLearningLoop(snapshot = state.snapshot || {}) {
  const loop = tradingWorkbenchSnapshot(snapshot)?.learning_loop;
  return loop && typeof loop === 'object' ? loop : { cards: [] };
}

function workbenchLearningLoopForPage(pageKey, snapshot = state.snapshot || {}) {
  const loop = workbenchLearningLoop(snapshot);
  const cards = Array.isArray(loop.cards) ? loop.cards : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return cards;
  const pages = new Set(['paper-trading', 'backtest', 'validation', 'journal', 'agent']);
  return pages.has(pageKey) ? cards : [];
}

function workbenchStrategyReview(snapshot = state.snapshot || {}) {
  const review = tradingWorkbenchSnapshot(snapshot)?.strategy_review;
  return review && typeof review === 'object' ? review : { hypotheses: [] };
}

function workbenchStrategyReviewForPage(pageKey, snapshot = state.snapshot || {}) {
  const review = workbenchStrategyReview(snapshot);
  const hypotheses = Array.isArray(review.hypotheses) ? review.hypotheses : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return hypotheses;
  const pages = new Set(['trade-planner', 'backtest', 'validation', 'journal', 'agent']);
  return pages.has(pageKey) ? hypotheses : [];
}

function workbenchTradeMonitor(snapshot = state.snapshot || {}) {
  const monitor = tradingWorkbenchSnapshot(snapshot)?.trade_monitor;
  return monitor && typeof monitor === 'object' ? monitor : { watch_items: [] };
}

function workbenchTradeMonitorForPage(pageKey, snapshot = state.snapshot || {}) {
  const monitor = workbenchTradeMonitor(snapshot);
  const items = Array.isArray(monitor.watch_items) ? monitor.watch_items : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return items;
  const pages = new Set(['portfolio', 'risk-desk', 'paper-trading', 'broker', 'alerts', 'journal', 'agent']);
  return pages.has(pageKey) ? items : [];
}

function workbenchRiskEnvelope(snapshot = state.snapshot || {}) {
  const envelope = tradingWorkbenchSnapshot(snapshot)?.risk_envelope;
  return envelope && typeof envelope === 'object' ? envelope : { checks: [] };
}

function workbenchRiskEnvelopeForPage(pageKey, snapshot = state.snapshot || {}) {
  const envelope = workbenchRiskEnvelope(snapshot);
  const rows = Array.isArray(envelope.checks) ? envelope.checks : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return rows;
  const pages = new Set(['portfolio', 'trade-planner', 'risk-desk', 'paper-trading', 'broker']);
  return pages.has(pageKey) ? rows : [];
}

function workbenchTradeRunbook(snapshot = state.snapshot || {}) {
  const runbook = tradingWorkbenchSnapshot(snapshot)?.trade_runbook;
  return runbook && typeof runbook === 'object' ? runbook : { steps: [] };
}

function workbenchTradeRunbookForPage(pageKey, snapshot = state.snapshot || {}) {
  const runbook = workbenchTradeRunbook(snapshot);
  const steps = Array.isArray(runbook.steps) ? runbook.steps : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return steps;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return steps.filter((step) => (
    keys.has(step?.module) || keys.has(step?.target_page)
  ));
}

function workbenchActionBus(snapshot = state.snapshot || {}) {
  const bus = tradingWorkbenchSnapshot(snapshot)?.action_bus;
  return bus && typeof bus === 'object' ? bus : { actions: [] };
}

function workbenchWorkflowMap(snapshot = state.snapshot || {}) {
  const workflow = tradingWorkbenchSnapshot(snapshot)?.workflow_map;
  return workflow && typeof workflow === 'object' ? workflow : { stages: [] };
}

function workbenchWorkflowStagesForPage(pageKey, snapshot = state.snapshot || {}) {
  const workflow = workbenchWorkflowMap(snapshot);
  const stages = Array.isArray(workflow.stages) ? workflow.stages : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return stages;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return stages.filter((stage) => keys.has(stage?.module));
}

function workbenchPriorityQueue(snapshot = state.snapshot || {}) {
  const queue = tradingWorkbenchSnapshot(snapshot)?.priority_queue;
  return queue && typeof queue === 'object' ? queue : { items: [] };
}

function workbenchPriorityItemsForPage(pageKey, snapshot = state.snapshot || {}) {
  const queue = workbenchPriorityQueue(snapshot);
  const items = Array.isArray(queue.items) ? queue.items : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return items;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return items.filter((item) => (
    keys.has(item?.module) || keys.has(item?.target_page)
  ));
}

function workbenchSupervisionGates(snapshot = state.snapshot || {}) {
  const gates = tradingWorkbenchSnapshot(snapshot)?.supervision_gates;
  return gates && typeof gates === 'object' ? gates : { gates: [] };
}

function workbenchSupervisionGatesForPage(pageKey, snapshot = state.snapshot || {}) {
  const supervision = workbenchSupervisionGates(snapshot);
  const gates = Array.isArray(supervision.gates) ? supervision.gates : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return gates;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return gates.filter((gate) => (
    keys.has(gate?.module) || keys.has(gate?.target_page)
  ));
}

function workbenchActionsForPage(pageKey, snapshot = state.snapshot || {}) {
  const bus = workbenchActionBus(snapshot);
  const actions = Array.isArray(bus.actions) ? bus.actions : [];
  if (pageKey === 'overview' || pageKey === 'command-center') return actions;
  const module = platformModuleForPage(pageKey);
  const keys = new Set([pageKey, module?.key, module?.page].filter(Boolean));
  return actions.filter((action) => (
    keys.has(action?.module) || keys.has(action?.target_page)
  ));
}

function tradingWorkbenchModule(pageKey, snapshot = state.snapshot || {}) {
  const workbench = tradingWorkbenchSnapshot(snapshot);
  const modules = workbench?.modules || {};
  const manifestModule = platformModuleForPage(pageKey);
  return modules[pageKey] || modules[manifestModule?.key] || null;
}

function updatePlatformState() {
  const manifest = platformManifest();
  const boundary = platformBoundary();
  setText(
    '#platform-state',
    [
      `app=${manifest.name || 'MarketRadar Trading Workbench'}`,
      `primary_tool=${manifest.primary_tool || 'market-radar'}`,
      `modules=${platformModules().length}`,
      `live_trading_enabled=${Boolean(boundary.live_trading_enabled)}`,
      `broker_order_submission=${boundary.broker_order_submission || 'disabled'}`,
      `autonomous_execution=${boundary.autonomous_execution || 'disabled'}`,
    ].join(' ')
  );
}

function catalogLabel(value) {
  return compact(String(value || '').replaceAll('_', ' '), 'local');
}

function isFormControlTarget(target) {
  return target instanceof HTMLInputElement || target instanceof HTMLSelectElement || target instanceof HTMLTextAreaElement;
}

function shouldPreserveNativeTab(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return false;
  if (target.id === 'dashboard-main' || target === document.body) return false;
  return Boolean(target.closest('button, a[href], input, select, textarea, [role="button"], [tabindex]'));
}

function qs(selector) {
  return document.querySelector(selector);
}

function setText(selector, value) {
  const element = qs(selector);
  if (element) element.textContent = value;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function text(value, fallback = 'n/a') {
  if (value === null || value === undefined || value === '') return fallback;
  if (Array.isArray(value)) return value.length ? value.map((item) => text(item)).join(', ') : fallback;
  if (typeof value === 'object') return value.status || value.summary || value.answer || `${Object.keys(value).length} fields`;
  return String(value);
}

function at(object, path, fallback = undefined) {
  let current = object;
  for (const part of path) {
    if (!current || typeof current !== 'object' || !(part in current)) return fallback;
    current = current[part];
  }
  return current ?? fallback;
}

function arrayAt(object, path) {
  const value = at(object, path, []);
  return Array.isArray(value) ? value : [];
}

function compact(value, fallback = '-') {
  const normalized = text(value, '').trim();
  return normalized || fallback;
}

function compactMapping(value, fallback = '-') {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return compact(value, fallback);
  }
  const rows = Object.entries(value).map(([key, item]) => (
    `${catalogLabel(key)}: ${compact(item, '0')}`
  ));
  return rows.length ? rows.join(', ') : fallback;
}

function rowsFromSnapshot(snapshot) {
  const priced = at(snapshot, ['priced_in_queue', 'rows'], null)
    || at(snapshot, ['priced_in_queue', 'items'], null);
  const candidates = at(snapshot, ['candidates', 'rows'], null)
    || at(snapshot, ['candidates', 'items'], null);
  const rows = Array.isArray(priced) && priced.length ? priced : candidates;
  return Array.isArray(rows) ? rows : [];
}

function alertRows(snapshot) {
  const rows = at(snapshot, ['alerts', 'rows'], []);
  return Array.isArray(rows) ? rows : [];
}

function ipoRows(snapshot) {
  const rows = at(snapshot, ['ipo_s1', 'rows'], []);
  return Array.isArray(rows) ? rows : [];
}

async function invoke(command, payload) {
  if (!tauriInvoke) {
    throw new Error('Tauri invoke API is unavailable. Run this dashboard through radar-desktop.');
  }
  return tauriInvoke(command, payload);
}

async function boot() {
  bindControls();
  state.config = await invoke('desktop_config');
  state.page = state.config.initial_page || 'overview';
  updatePlatformState();
  renderNav();
  renderAutomation();
  renderKeys();
  setText('#source-label', `source=${state.config.source_label}`);
  setText('#snapshot-source', friendlySource(state.config.source_label));
  await refreshSnapshot();
}

function bindControls() {
  qs('#refresh-button').addEventListener('click', () => refreshSnapshot());
  qs('#apply-filters').addEventListener('click', () => refreshSnapshot());
  qs('#copy-command').addEventListener('click', copyNextCommand);
  qs('#close-error').addEventListener('click', () => qs('#error-dialog').close());
  qs('#command-form').addEventListener('submit', handleCommandSubmit);
  qs('#command-input').addEventListener('keydown', handleCommandInputKeydown);
  document.addEventListener('keydown', handleKeyboard);
}

function renderNav() {
  const host = qs('#workflow-tabs');
  const activePage = navigationPageKey(state.page);
  host.innerHTML = state.config.pages.map((page) => `
    <button
      class="workflow-tab"
      type="button"
      role="tab"
      aria-selected="${page.key === activePage}"
      aria-current="${page.key === activePage ? 'page' : 'false'}"
      aria-keyshortcuts="${escapeHtml(page.shortcut)}"
      aria-controls="dashboard-main"
      id="tab-${escapeHtml(page.key)}"
      data-testid="${escapeHtml(page.test_id)}"
      data-page="${escapeHtml(page.key)}"
      tabindex="${page.key === activePage ? '0' : '-1'}"
      aria-label="Open ${escapeHtml(page.label)} dashboard page"
      title="${escapeHtml(page.description)}"
    >
      <span class="shortcut">${escapeHtml(page.shortcut)}</span>
      <span class="tab-label">${escapeHtml(page.label)}</span>
    </button>
  `).join('');
  host.querySelectorAll('button').forEach((button) => {
    button.addEventListener('click', () => setPage(button.dataset.page));
  });
}

function renderAutomation() {
  const notes = state.config.automation.keyboard_shortcuts.slice(0, 5);
  qs('#automation-list').innerHTML = notes.map((item) => `<li>${escapeHtml(item)}</li>`).join('');
}

function renderKeys() {
  const keys = [
    ['q / Esc', 'quit'],
    ['r / F5', 'refresh'],
    ['Up/Down', 'workflow'],
    ['Tab/Arrows', 'next/prev'],
    ['Home/End', 'first/help'],
    ['0-9 letters', 'jump'],
    ['Ctrl+A', 'agent'],
  ];
  qs('#keys-list').innerHTML = keys.map(([shortcut, action]) => `
    <li><kbd>${escapeHtml(shortcut)}</kbd><span>${escapeHtml(action)}</span></li>
  `).join('');
}

async function setPage(page) {
  if (!page) return;
  if (page === state.page) {
    await refreshSnapshot();
    qs('#dashboard-main').focus();
    return;
  }
  state.page = page;
  renderNav();
  await refreshSnapshot();
  qs('#dashboard-main').focus();
}

async function refreshSnapshot() {
  state.loading = true;
  setStatus('refreshing');
  setText('#snapshot-refresh', 'refreshing');
  if (!state.snapshot) renderLoadingDashboard();
  try {
    const snapshot = await invoke('dashboard_snapshot', { input: filterInput() });
    state.snapshot = snapshot;
    state.lastRefresh = new Date();
    renderSnapshot();
  } catch (error) {
    showError(error);
    setStatus('error');
  } finally {
    state.loading = false;
  }
}

function filterInput() {
  return {
    page: state.page,
    ticker: qs('#filter-ticker').value.trim() || null,
    available_at: state.availableAt,
    alert_status: state.alertStatus,
    alert_route: state.alertRoute,
    priced_in_status: qs('#filter-scan-mode').value,
    usefulness: state.usefulness,
    source_gap: state.sourceGap,
    decision_gap: state.decisionGap,
    stocks_only: qs('#filter-stocks-only').checked,
    scan_limit: Number(qs('#filter-limit').value || 50),
    scan_offset: state.scanOffset,
    telemetry_limit: 8,
  };
}

function renderSnapshot() {
  const snapshot = state.snapshot || {};
  const status = compact(snapshot.status || at(snapshot, ['readiness', 'status']), 'unknown');
  const pageInfo = state.config.pages.find((page) => page.key === navigationPageKey(state.page));
  const label = pageLabelFor(state.page, pageInfo);
  setText('#page-title', label);
  setStatus(status);
  setText('#refresh-label', `refresh=${state.lastRefresh ? state.lastRefresh.toLocaleTimeString() : 'pending'}`);
  setText('#provider-calls', `provider_calls=${compact(snapshot.external_calls_made, '0')}`);
  setText('#next-action', compact(snapshot.next_action || snapshot.canonical_next_action, 'Review the current page.'));
  setText('#next-command', compact(snapshot.next_command || snapshot.canonical_next_command, 'No command reported.'));
  setText('#boundary-copy', `Snapshot mode ${compact(snapshot.snapshot_mode, 'unknown')}; provider calls reported ${compact(snapshot.external_calls_made, '0')}; live trading disabled.`);
  renderSnapshotMeta(snapshot, pageInfo);
  updateAutomationState(snapshot, status, pageInfo);
  updateFilterState();
  updateCommandState();
  updatePlatformState();
  updateAutomationJson(snapshot, status, pageInfo);
  renderContent(snapshot);
  bindPlatformToolCards();
  bindWorkbenchPaperControls();
  bindWorkbenchTicketControls();
  bindWorkbenchLifecycleControls();
  bindWorkbenchAgentControls();
  bindWorkbenchReviewControls();
  bindWorkbenchActionBusControls();
  bindQueueRows();
}

function renderLoadingDashboard() {
  setText('#page-title', 'Command Center');
  setText('#next-action', 'Loading local snapshot.');
  setText('#next-command', 'dashboard-snapshot --json --fast');
  setText('#boundary-copy', 'Rendering remains local and makes zero provider calls.');
  setText('#provider-calls', 'provider_calls=0');
  setText('#snapshot-page', navigationPageKey(state.page));
  setText('#snapshot-mode', 'snapshot pending');
  updateFilterState();
  updateCommandState();
  updatePlatformState();
  updateAutomationJson();
  qs('#content').innerHTML = `
    <section class="panel wide loading-dashboard" data-testid="loading-dashboard">
      <h2>Trading Workbench</h2>
      <p>Loading market snapshot</p>
      <p>MarketRadar is reading the local trading platform contract.</p>
      <p>Rendering remains local and makes zero provider calls.</p>
    </section>
    <div class="metric-grid" data-testid="loading-metric-strip" aria-label="Loading dashboard metrics">
      ${loadingMetric('Decision status', 'pending')}
      ${loadingMetric('Next safe action', 'loading')}
      ${loadingMetric('Provider calls', '0')}
    </div>
    <section class="panel wide loading-preview" data-testid="loading-preview-queue">
      <h2>Attention Queue</h2>
      <div class="table-wrap">
        <table aria-label="Loading attention queue preview">
          <thead><tr><th>Ticker</th><th>State</th><th>Signal</th><th>Next</th></tr></thead>
          <tbody>
            <tr><td>[loading]</td><td>local snapshot</td><td>resolving dashboard contract</td><td>refresh</td></tr>
            <tr><td>[loading]</td><td>zero-call read</td><td>waiting for data rows</td><td>inspect</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function loadingMetric(label, value) {
  return `
    <article class="metric placeholder" data-testid="loading-${label.toLowerCase().replaceAll(' ', '-')}">
      <span>${escapeHtml(label)}</span>
      <b>${escapeHtml(value)}</b>
      <p>snapshot loading</p>
    </article>
  `;
}

function renderSnapshotMeta(snapshot, pageInfo) {
  setText('#snapshot-source', friendlySource(state.config?.source_label || 'pending'));
  setText('#snapshot-refresh', state.lastRefresh ? '0s ago' : 'pending');
  setText('#snapshot-page', navigationPageKey(state.page));
  setText('#snapshot-mode', compact(snapshot.snapshot_mode, 'snapshot pending'));
  qs('#snapshot-panel')?.setAttribute('data-current-page', navigationPageKey(state.page));
  qs('#snapshot-panel')?.setAttribute('data-current-mode', compact(snapshot.snapshot_mode, 'snapshot_pending'));
  qs('#snapshot-panel')?.setAttribute('data-page-label', pageLabelFor(state.page, pageInfo));
}

function friendlySource(source) {
  const value = String(source || 'pending');
  return value.startsWith('command ') ? 'local snapshot command' : value;
}

function updateAutomationState(snapshot, status, pageInfo) {
  const navPage = navigationPageKey(state.page);
  const label = pageLabelFor(state.page, pageInfo);
  const main = qs('#dashboard-main');
  if (main) {
    main.dataset.currentPage = state.page;
    main.dataset.currentNavPage = navPage;
    main.setAttribute('aria-label', `Dashboard page ${label}`);
  }
  setText(
    '#automation-state',
    [
      `page=${state.page}`,
      `nav=${navPage}`,
      `label=${label}`,
      `status=${status}`,
      `provider_calls=${compact(snapshot.external_calls_made, '0')}`,
      `next_command=${compact(snapshot.next_command || snapshot.canonical_next_command, 'none')}`,
    ].join(' ')
  );
}

function updateFilterState() {
  const filterState = automationFilterState();
  const fields = [
    ['ticker', filterState.ticker],
    ['scan_mode', filterState.scan_mode],
    ['stocks_only', filterState.stocks_only ? 'true' : 'false'],
    ['limit', String(filterState.limit)],
    ['offset', String(filterState.offset)],
    ['usefulness', filterState.usefulness],
    ['source_gap', filterState.source_gap.length ? filterState.source_gap.join(',') : 'all'],
    ['decision_gap', filterState.decision_gap.length ? filterState.decision_gap.join(',') : 'all'],
    ['available_at', filterState.available_at],
    ['alert_status', filterState.alert_status],
    ['alert_route', filterState.alert_route],
  ];
  setText('#filter-state', fields.map(([key, value]) => `${key}=${value}`).join(' '));
}

function automationFilterState() {
  return {
    ticker: qs('#filter-ticker')?.value.trim() || 'all',
    scan_mode: qs('#filter-scan-mode')?.value || 'all',
    stocks_only: Boolean(qs('#filter-stocks-only')?.checked),
    limit: Number(qs('#filter-limit')?.value || 50),
    offset: Number(state.scanOffset || 0),
    usefulness: state.usefulness || 'all',
    source_gap: [...state.sourceGap],
    decision_gap: [...state.decisionGap],
    available_at: state.availableAt || 'latest',
    alert_status: state.alertStatus || 'all',
    alert_route: state.alertRoute || 'all',
  };
}

function updateCommandState() {
  const providerCalls = compact(state.snapshot?.external_calls_made, '0');
  const result = qs('#command-status')?.textContent?.trim() || 'command=ready';
  const fields = [
    ['last_command', state.lastCommand || 'none'],
    ['page', state.page || 'overview'],
    ['nav', navigationPageKey(state.page || 'overview')],
    ['provider_calls', providerCalls],
  ];
  setText(
    '#command-state',
    `${fields.map(([key, value]) => `${key}=${value}`).join(' ')} result="${result}"`,
  );
}

function updateAutomationJson(snapshot = state.snapshot || {}, status = null, pageInfo = null) {
  const navPage = navigationPageKey(state.page || 'overview');
  const label = pageLabelFor(state.page || 'overview', pageInfo);
  const providerCalls = Number(snapshot?.external_calls_made || 0);
  const payload = {
    contract_version: state.config?.automation?.contract_version || 'market-radar-desktop-automation-v1',
    page: state.page || 'overview',
    nav: navPage,
    label,
    status: status || compact(snapshot?.status || at(snapshot, ['readiness', 'status']), 'loading'),
    provider_calls: Number.isFinite(providerCalls) ? providerCalls : 0,
    snapshot_page: qs('#snapshot-page')?.textContent?.trim() || navPage,
    snapshot_mode: compact(snapshot?.snapshot_mode, 'snapshot pending'),
    last_command: state.lastCommand || 'none',
    command_result: qs('#command-status')?.textContent?.trim() || 'command=ready',
    filters: automationFilterState(),
    platform: {
      primary_tool: platformManifest().primary_tool || 'market-radar',
      live_trading_enabled: Boolean(platformBoundary().live_trading_enabled),
      modules: platformModules().map((module) => module.key),
      action_count: Number(workbenchActionBus(snapshot)?.metrics?.action_count || 0),
      workflow_status: compact(workbenchWorkflowMap(snapshot)?.status, 'unknown'),
      active_stage_id: compact(workbenchWorkflowMap(snapshot)?.active_stage_id, 'none'),
      stage_count: Number(workbenchWorkflowMap(snapshot)?.stage_count || 0),
      priority_queue_status: compact(workbenchPriorityQueue(snapshot)?.status, 'unknown'),
      primary_priority_item_id: compact(workbenchPriorityQueue(snapshot)?.primary_item_id, 'none'),
      priority_item_count: Number(workbenchPriorityQueue(snapshot)?.metrics?.item_count || 0),
      supervision_status: compact(workbenchSupervisionGates(snapshot)?.status, 'unknown'),
      primary_supervision_gate_id: compact(workbenchSupervisionGates(snapshot)?.primary_gate_id, 'none'),
      approval_required_count: Number(workbenchSupervisionGates(snapshot)?.metrics?.approval_required_count || 0),
      armed_local_write: compact(state.pendingLocalWrite?.command, 'none'),
      operator_status: compact(workbenchOperatorState(snapshot)?.status, 'unknown'),
      operator_active_module: compact(workbenchOperatorState(snapshot)?.active_module, 'none'),
      operator_active_blocker: compact(workbenchOperatorState(snapshot)?.primary_blocker, 'none'),
      operator_next_command: compact(workbenchOperatorState(snapshot)?.primary_next_action?.command, 'none'),
      execution_sandbox_status: compact(workbenchExecutionSandbox(snapshot)?.status, 'unknown'),
      execution_sandbox_active_lane_id: compact(workbenchExecutionSandbox(snapshot)?.active_lane_id, 'none'),
      execution_sandbox_preview_count: Number(workbenchExecutionSandbox(snapshot)?.metrics?.preview_lane_count || 0),
      execution_sandbox_disabled_count: Number(workbenchExecutionSandbox(snapshot)?.metrics?.disabled_lane_count || 0),
      decision_brief_status: compact(workbenchDecisionBrief(snapshot)?.status, 'unknown'),
      decision_brief_ticker: compact(workbenchDecisionBrief(snapshot)?.ticker, 'none'),
      decision_brief_source_tool: compact(workbenchDecisionBrief(snapshot)?.source_tool, 'market-radar'),
      decision_brief_next_command: compact(workbenchDecisionBrief(snapshot)?.next_action?.command, 'none'),
      scenario_matrix_status: compact(workbenchScenarioMatrix(snapshot)?.status, 'unknown'),
      scenario_matrix_ticker: compact(workbenchScenarioMatrix(snapshot)?.ticker, 'none'),
      scenario_count: Number(workbenchScenarioMatrix(snapshot)?.metrics?.scenario_count || 0),
      scenario_reward_risk: compact(workbenchScenarioMatrix(snapshot)?.metrics?.risk_reward, 'none'),
      portfolio_impact_status: compact(workbenchPortfolioImpact(snapshot)?.status, 'unknown'),
      portfolio_impact_ticker: compact(workbenchPortfolioImpact(snapshot)?.ticker, 'none'),
      portfolio_impact_proposed_notional: compact(workbenchPortfolioImpact(snapshot)?.impact?.proposed_notional, 'none'),
      portfolio_impact_block_count: Number(workbenchPortfolioImpact(snapshot)?.blockers?.length || 0),
      position_sizing_status: compact(workbenchPositionSizing(snapshot)?.status, 'unknown'),
      position_sizing_ticker: compact(workbenchPositionSizing(snapshot)?.ticker, 'none'),
      position_sizing_suggested_shares: Number(workbenchPositionSizing(snapshot)?.recommendation?.suggested_quantity || 0),
      position_sizing_risk_budget: compact(workbenchPositionSizing(snapshot)?.recommendation?.risk_budget, 'none'),
      capital_allocation_status: compact(workbenchCapitalAllocation(snapshot)?.status, 'unknown'),
      capital_allocation_ticker: compact(workbenchCapitalAllocation(snapshot)?.ticker, 'none'),
      capital_allocation_suggested_notional: compact(workbenchCapitalAllocation(snapshot)?.allocation_plan?.suggested_notional, 'none'),
      capital_allocation_buying_power_usage_pct: compact(workbenchCapitalAllocation(snapshot)?.allocation_plan?.buying_power_usage_pct, 'none'),
      capital_allocation_blocked_check_count: Number(workbenchCapitalAllocation(snapshot)?.metrics?.blocked_check_count || 0),
      capital_allocation_allowed: Boolean(workbenchCapitalAllocation(snapshot)?.allocation_plan?.allocation_allowed),
      order_ticket_draft_status: compact(workbenchOrderTicketDraft(snapshot)?.status, 'unknown'),
      order_ticket_draft_ticker: compact(workbenchOrderTicketDraft(snapshot)?.ticker, 'none'),
      order_ticket_draft_suggested_shares: Number(workbenchOrderTicketDraft(snapshot)?.ticket?.suggested_quantity || 0),
      order_ticket_draft_preview_command: compact(workbenchOrderTicketDraft(snapshot)?.commands?.preview, 'none'),
      paper_trade_preview_status: compact(workbenchPaperTradePreview(snapshot)?.status, 'unknown'),
      paper_trade_preview_ticker: compact(workbenchPaperTradePreview(snapshot)?.ticker, 'none'),
      paper_trade_preview_decision: compact(workbenchPaperTradePreview(snapshot)?.paper_decision?.decision, 'none'),
      paper_trade_preview_suggested_quantity: Number(workbenchPaperTradePreview(snapshot)?.paper_decision?.suggested_quantity || 0),
      paper_trade_preview_block_count: Number(workbenchPaperTradePreview(snapshot)?.blockers?.length || 0),
      pretrade_compliance_status: compact(workbenchPretradeCompliance(snapshot)?.status, 'unknown'),
      pretrade_compliance_ticker: compact(workbenchPretradeCompliance(snapshot)?.ticker, 'none'),
      pretrade_compliance_primary_blocker: compact(workbenchPretradeCompliance(snapshot)?.primary_blocker, 'none'),
      pretrade_compliance_blocked_check_count: Number(workbenchPretradeCompliance(snapshot)?.metrics?.blocked_check_count || 0),
      pretrade_compliance_approval_required_count: Number(workbenchPretradeCompliance(snapshot)?.metrics?.approval_required_count || 0),
      pretrade_compliance_ready: Boolean(workbenchPretradeCompliance(snapshot)?.status === 'ready'),
      trade_readiness_status: compact(workbenchTradeReadinessBrief(snapshot)?.status, 'unknown'),
      trade_readiness_ticker: compact(workbenchTradeReadinessBrief(snapshot)?.ticker, 'none'),
      trade_readiness_primary_blocker: compact(workbenchTradeReadinessBrief(snapshot)?.primary_blocker, 'none'),
      trade_readiness_next_page: compact(workbenchTradeReadinessBrief(snapshot)?.agent_handoff?.next_page, 'none'),
      trade_readiness_blocked_check_count: Number(workbenchTradeReadinessBrief(snapshot)?.metrics?.blocked_check_count || 0),
      trade_readiness_disabled_check_count: Number(workbenchTradeReadinessBrief(snapshot)?.metrics?.disabled_check_count || 0),
      trade_readiness_paper_record_allowed: Boolean(workbenchTradeReadinessBrief(snapshot)?.paper_record_allowed),
      trade_readiness_broker_handoff_allowed: Boolean(workbenchTradeReadinessBrief(snapshot)?.broker_handoff_allowed),
      trade_readiness_strategy_update_allowed: Boolean(workbenchTradeReadinessBrief(snapshot)?.strategy_update_allowed),
      trade_readiness_monitoring_ready: Boolean(workbenchTradeReadinessBrief(snapshot)?.monitoring_ready),
      learning_loop_status: compact(workbenchLearningLoop(snapshot)?.status, 'unknown'),
      learning_loop_ticker: compact(workbenchLearningLoop(snapshot)?.ticker, 'none'),
      learning_loop_stage: compact(workbenchLearningLoop(snapshot)?.learning_stage, 'unlinked'),
      learning_loop_validation_result_id: compact(workbenchLearningLoop(snapshot)?.validation_state?.validation_result_id, 'none'),
      learning_loop_outcome_id: compact(workbenchLearningLoop(snapshot)?.journal_state?.outcome_id, 'none'),
      learning_loop_blocked_card_count: Number(workbenchLearningLoop(snapshot)?.metrics?.blocked_card_count || 0),
      strategy_review_status: compact(workbenchStrategyReview(snapshot)?.status, 'unknown'),
      strategy_review_ticker: compact(workbenchStrategyReview(snapshot)?.ticker, 'none'),
      strategy_review_stage: compact(workbenchStrategyReview(snapshot)?.strategy_stage, 'unlinked'),
      strategy_review_hypothesis_count: Number(workbenchStrategyReview(snapshot)?.metrics?.hypothesis_count || 0),
      strategy_review_blocked_hypothesis_count: Number(workbenchStrategyReview(snapshot)?.metrics?.blocked_hypothesis_count || 0),
      strategy_update_allowed: Boolean(workbenchStrategyReview(snapshot)?.strategy_update_allowed),
      trade_monitor_status: compact(workbenchTradeMonitor(snapshot)?.status, 'unknown'),
      trade_monitor_ticker: compact(workbenchTradeMonitor(snapshot)?.ticker, 'none'),
      trade_monitor_stage: compact(workbenchTradeMonitor(snapshot)?.monitor_stage, 'unlinked'),
      trade_monitor_active_trade_count: Number(workbenchTradeMonitor(snapshot)?.metrics?.active_paper_trade_count || 0),
      trade_monitor_blocker_count: Number(workbenchTradeMonitor(snapshot)?.metrics?.blocked_watch_item_count || 0),
      trade_monitor_open_order_count: Number(workbenchTradeMonitor(snapshot)?.metrics?.open_order_count || 0),
      trade_monitor_primary_trigger_id: compact(workbenchTradeMonitor(snapshot)?.alert_watch?.primary_trigger_id, 'none'),
      trade_monitor_exit_update_allowed: Boolean(workbenchTradeMonitor(snapshot)?.exit_update_allowed),
      risk_envelope_status: compact(workbenchRiskEnvelope(snapshot)?.status, 'unknown'),
      risk_envelope_ticker: compact(workbenchRiskEnvelope(snapshot)?.ticker, 'none'),
      risk_sizing_status: compact(workbenchRiskEnvelope(snapshot)?.sizing_context?.sizing_status, 'unknown'),
      risk_block_count: Number(workbenchRiskEnvelope(snapshot)?.blockers?.length || 0),
      risk_max_loss: compact(workbenchRiskEnvelope(snapshot)?.sizing_context?.estimated_max_loss, 'none'),
      runbook_status: compact(workbenchTradeRunbook(snapshot)?.status, 'unknown'),
      runbook_active_step_id: compact(workbenchTradeRunbook(snapshot)?.active_step_id, 'none'),
      runbook_step_count: Number(workbenchTradeRunbook(snapshot)?.metrics?.step_count || 0),
      runbook_blocked_step_count: Number(workbenchTradeRunbook(snapshot)?.metrics?.blocked_step_count || 0),
    },
    next_command: compact(snapshot?.next_command || snapshot?.canonical_next_command, 'none'),
    next_action: compact(snapshot?.next_action || snapshot?.canonical_next_action, 'none'),
  };
  setText('#automation-json', JSON.stringify(payload));
}

function pageLabelFor(page, pageInfo) {
  if (isDynamicDetailPage(page)) return dynamicPageLabel(page);
  return pageInfo ? pageInfo.label : dynamicPageLabel(page);
}

function isDynamicDetailPage(page) {
  return page.startsWith('candidate:') || page.startsWith('alert:');
}

function navigationPageKey(page) {
  if (page.startsWith('candidate:')) return 'candidates';
  if (page.startsWith('alert:')) return 'alerts';
  return page;
}

function dynamicPageLabel(page) {
  if (page.startsWith('candidate:')) return `Candidate ${page.split(':', 2)[1].toUpperCase()}`;
  if (page.startsWith('alert:')) return `Alert ${page.split(':', 2)[1]}`;
  return 'Dashboard';
}

function setStatus(status) {
  const pill = qs('#status-chip');
  if (!pill) return;
  const normalized = String(status || 'unknown').toLowerCase().replaceAll(' ', '_');
  pill.textContent = normalized;
  pill.className = `status-pill ${normalized}`;
}

function renderContent(snapshot) {
  if (state.page.startsWith('candidate:')) {
    const ticker = state.page.split(':', 2)[1] || '';
    qs('#content').innerHTML = `${metricGrid(snapshot)}${renderCandidateDetail(snapshot, ticker)}${rawJsonPanel(snapshot)}`;
    return;
  }
  if (state.page.startsWith('alert:')) {
    const alertId = state.page.split(':', 2)[1] || '';
    qs('#content').innerHTML = `${metricGrid(snapshot)}${renderAlertDetail(snapshot, alertId)}${rawJsonPanel(snapshot)}`;
    return;
  }
  const renderers = {
    tutorial: renderTutorial,
    overview: renderOverview,
    portfolio: () => renderPlatformModulePage('portfolio', snapshot),
    'market-radar': () => renderPlatformModulePage('market-radar', snapshot),
    'trade-planner': () => renderPlatformModulePage('trade-planner', snapshot),
    'risk-desk': () => renderPlatformModulePage('risk-desk', snapshot),
    'paper-trading': () => renderPlatformModulePage('paper-trading', snapshot),
    backtest: () => renderPlatformModulePage('backtest', snapshot),
    readiness: () => renderPlatformModulePage('readiness', snapshot),
    run: () => renderPlatformModulePage('run', snapshot),
    candidates: () => renderPlatformModulePage('candidates', snapshot),
    review: () => renderPlatformModulePage('review', snapshot),
    alerts: () => renderPlatformModulePage('alerts', snapshot),
    ipo: () => renderPlatformModulePage('ipo', snapshot),
    broker: () => renderPlatformModulePage('broker', snapshot),
    ops: () => renderPlatformModulePage('ops', snapshot),
    telemetry: () => renderPlatformModulePage('telemetry', snapshot),
    agent: () => renderPlatformModulePage('agent', snapshot),
    themes: () => renderPlatformModulePage('themes', snapshot),
    validation: () => renderPlatformModulePage('validation', snapshot),
    costs: () => renderPlatformModulePage('costs', snapshot),
    features: () => renderPlatformModulePage('features', snapshot),
    journal: () => renderPlatformModulePage('journal', snapshot),
    help: renderHelp,
  };
  const renderer = renderers[state.page] || renderOverview;
  qs('#content').innerHTML = `${metricGrid(snapshot)}${renderer(snapshot)}${rawJsonPanel(snapshot)}`;
}

function metricGrid(snapshot) {
  const rowCount = rowsFromSnapshot(snapshot).length;
  const alertCount = alertRows(snapshot).length || at(snapshot, ['alerts', 'count'], 0);
  return `
    <div class="metric-grid" aria-label="Dashboard metrics">
      ${metric('Decision', compact(snapshot.status, 'unknown'), 'current gate')}
      ${metric('Queue', rowCount, 'rows to triage')}
      ${metric('Alerts', alertCount, 'manual review')}
      ${metric('Calls', compact(snapshot.external_calls_made, '0'), 'provider calls')}
    </div>
  `;
}

function metric(label, value, caption) {
  return `<article class="metric"><span>${escapeHtml(label)}</span><b>${escapeHtml(value)}</b><p>${escapeHtml(caption)}</p></article>`;
}

function renderOverview(snapshot) {
  return `
    ${renderTradingWorkbenchOverview(snapshot)}
    ${renderWorkbenchOperatorState(snapshot)}
    ${renderWorkbenchExecutionSandbox(snapshot, 'overview')}
    ${renderWorkbenchDecisionBrief(snapshot)}
    ${renderWorkbenchScenarioMatrix(snapshot, 'overview')}
    ${renderWorkbenchPortfolioImpact(snapshot, 'overview')}
    ${renderWorkbenchPositionSizing(snapshot, 'overview')}
    ${renderWorkbenchCapitalAllocation(snapshot, 'overview')}
    ${renderWorkbenchOrderTicketDraft(snapshot, 'overview')}
    ${renderWorkbenchPaperTradePreview(snapshot, 'overview')}
    ${renderWorkbenchPretradeCompliance(snapshot, 'overview')}
    ${renderWorkbenchTradeReadinessBrief(snapshot, 'overview')}
    ${renderWorkbenchLearningLoop(snapshot, 'overview')}
    ${renderWorkbenchStrategyReview(snapshot, 'overview')}
    ${renderWorkbenchTradeMonitor(snapshot, 'overview')}
    ${renderWorkbenchRiskEnvelope(snapshot, 'overview')}
    ${renderWorkbenchTradeRunbook(snapshot, 'overview')}
    ${renderWorkbenchWorkflowMap(snapshot, 'overview')}
    ${renderWorkbenchPriorityQueue(snapshot, 'overview')}
    ${renderWorkbenchSupervisionGates(snapshot, 'overview')}
    ${renderWorkbenchActionBus(snapshot, 'overview')}
    ${renderLiveTradingBoundary()}
    <section class="panel" data-testid="first-blocker">
      <h2>First Blocker</h2>
      <p>${escapeHtml(compact(snapshot.first_blocker || at(snapshot, ['readiness', 'first_blocker']), 'No blocker reported.'))}</p>
    </section>
    <section class="panel" data-testid="operator-move">
      <h2>Operator Move</h2>
      <p>${escapeHtml(compact(snapshot.next_action || snapshot.canonical_next_action, 'Review the current page.'))}</p>
      <code>${escapeHtml(compact(snapshot.next_command || snapshot.canonical_next_command, 'No command reported.'))}</code>
    </section>
    ${queuePanel('Attention Queue', rowsFromSnapshot(snapshot))}
  `;
}

function renderTradingWorkbenchOverview(snapshot) {
  const modules = platformModules();
  const queueCount = rowsFromSnapshot(snapshot).length;
  const workbench = tradingWorkbenchSnapshot(snapshot);
  return `
    <section class="panel wide platform-map" data-testid="trading-workbench-overview">
      <div class="platform-heading">
        <div>
          <h2>Trading Workbench</h2>
          <p>MarketRadar is one tool in a local, zero-call trading platform shell.</p>
        </div>
        <div class="platform-summary" data-testid="platform-summary">
          <span>primary tool</span>
          <b>${escapeHtml(platformManifest().primary_tool || 'market-radar')}</b>
          <span>queue</span>
          <b>${escapeHtml(queueCount)}</b>
          <span>calls</span>
          <b>${escapeHtml(compact(workbench.external_calls_made, '0'))}</b>
        </div>
      </div>
      <div class="platform-tools" data-testid="platform-tools">
        ${modules.map(platformToolCard).join('')}
      </div>
    </section>
  `;
}

function platformToolCard(module) {
  const status = catalogLabel(module.status || 'route_ready');
  return `
    <article
      class="platform-tool-card"
      data-testid="platform-tool-card"
      data-tool="${escapeHtml(module.key)}"
      data-page="${escapeHtml(module.page || module.key)}"
      data-status="${escapeHtml(module.status || 'route_ready')}"
      tabindex="0"
      role="button"
      aria-label="Open ${escapeHtml(module.label)}"
      title="${escapeHtml(module.next_action || module.role || module.label)}"
    >
      <div class="tool-card-top">
        <h3>${escapeHtml(module.label)}</h3>
        <span class="tool-status">${escapeHtml(status)}</span>
      </div>
      <p>${escapeHtml(module.role || 'Local trading platform module.')}</p>
      <div class="tool-source">
        <span>Source</span>
        <b>${escapeHtml(module.source || 'local dashboard snapshot')}</b>
      </div>
    </article>
  `;
}

function renderWorkbenchOperatorState(snapshot) {
  const operator = workbenchOperatorState(snapshot);
  if (!operator || !operator.schema_version) return '';
  const readiness = operator.readiness || {};
  const risk = operator.risk || {};
  const handoff = operator.agent_handoff || {};
  const boundaries = operator.boundaries || {};
  const cards = Array.isArray(operator.state_cards) ? operator.state_cards : [];
  const nextAction = operator.primary_next_action || {};
  return `
    <section
      class="panel wide workbench-operator-state"
      data-testid="workbench-operator-state"
      data-operator-status="${escapeHtml(operator.status || 'unknown')}"
      data-operator-active-module="${escapeHtml(operator.active_module || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Operator State</h2>
          <p>${escapeHtml(operatorStateSummary(operator))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(operator.status || 'unknown'))}</span>
      </div>
      <div class="operator-state-grid">
        <div class="operator-state-block">
          <h3>Case</h3>
          <div class="kv-grid">
            <div class="kv"><span>Ticker</span><b>${escapeHtml(compact(operator.ticker, '-'))}</b></div>
            <div class="kv"><span>Module</span><b>${escapeHtml(catalogLabel(operator.active_module || '-'))}</b></div>
            <div class="kv"><span>Blocker</span><b>${escapeHtml(compact(operator.primary_blocker, '-'))}</b></div>
            <div class="kv"><span>Step</span><b>${escapeHtml(compact(operator.active_step_id, '-'))}</b></div>
          </div>
        </div>
        <div class="operator-state-block">
          <h3>Readiness</h3>
          <div class="kv-grid">
            <div class="kv"><span>Decision</span><b>${escapeHtml(catalogLabel(readiness.decision_brief_status || '-'))}</b></div>
            <div class="kv"><span>Risk</span><b>${escapeHtml(catalogLabel(readiness.risk_envelope_status || '-'))}</b></div>
            <div class="kv"><span>Supervision</span><b>${escapeHtml(catalogLabel(readiness.supervision_status || '-'))}</b></div>
            <div class="kv"><span>Approval</span><b>${escapeHtml(readiness.approval_required ? 'required' : 'not required')}</b></div>
          </div>
        </div>
        <div class="operator-state-block">
          <h3>Risk</h3>
          <div class="kv-grid">
            <div class="kv"><span>Sizing</span><b>${escapeHtml(catalogLabel(risk.sizing_status || '-'))}</b></div>
            <div class="kv"><span>Blocked checks</span><b>${escapeHtml(compact(risk.blocked_check_count, '0'))}</b></div>
            <div class="kv"><span>Max loss</span><b>${escapeHtml(text(risk.estimated_max_loss))}</b></div>
            <div class="kv"><span>Calls</span><b>${escapeHtml(compact(boundaries.external_calls_made, '0'))}</b></div>
          </div>
        </div>
        <div class="operator-state-block" data-testid="operator-state-next-action">
          <h3>Next</h3>
          <div class="kv-grid">
            <div class="kv"><span>Command</span><b>${escapeHtml(compact(handoff.next_command, '-'))}</b></div>
            <div class="kv"><span>Page</span><b>${escapeHtml(compact(handoff.next_page, '-'))}</b></div>
            <div class="kv"><span>Safety</span><b>${escapeHtml(catalogLabel(handoff.safety || '-'))}</b></div>
            <div class="kv"><span>Approval</span><b>${escapeHtml(handoff.can_execute_without_approval ? 'not required' : 'required')}</b></div>
          </div>
          <div class="decision-brief-action">${renderWorkbenchActionControl({ ...nextAction, status: 'enabled' })}</div>
        </div>
      </div>
      <div class="operator-state-cards" aria-label="Operator state cards">
        ${cards.map((card) => `
          <article
            data-testid="operator-state-card"
            data-operator-card-status="${escapeHtml(card.status || 'unknown')}"
            data-operator-card-module="${escapeHtml(card.module || '')}"
          >
            <span>${escapeHtml(compact(card.label, card.id || '-'))}</span>
            <b>${escapeHtml(catalogLabel(card.status || '-'))}</b>
            <p>${escapeHtml(compact(card.evidence, '-'))}</p>
          </article>
        `).join('')}
      </div>
    </section>
  `;
}

function operatorStateSummary(operator) {
  const metrics = operator?.metrics || {};
  const boundaries = operator?.boundaries || {};
  return [
    `${compact(operator?.ticker, 'No ticker')} ${catalogLabel(operator?.status || 'unknown')}`,
    `${compact(operator?.primary_blocker, 'no blocker')}`,
    `${compact(metrics.runbook_step_count, '0')} runbook steps`,
    `${compact(metrics.approval_required_count, '0')} approval required`,
    `provider calls ${compact(boundaries.external_calls_made, '0')}`,
  ].join('; ');
}

function renderWorkbenchExecutionSandbox(snapshot, pageKey = 'overview') {
  const sandbox = workbenchExecutionSandbox(snapshot);
  const lanes = workbenchExecutionSandboxForPage(pageKey, snapshot);
  if (!lanes.length) return '';
  return `
    <section
      class="panel wide workbench-execution-sandbox"
      data-testid="workbench-execution-sandbox"
      data-execution-sandbox-status="${escapeHtml(sandbox.status || 'unknown')}"
      data-execution-sandbox-active-lane="${escapeHtml(sandbox.active_lane_id || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Execution Sandbox</h2>
          <p>${escapeHtml(executionSandboxSummary(sandbox))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(sandbox.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Active lane</span><b>${escapeHtml(compact(sandbox.active_lane_id, '-'))}</b></div>
        <div class="kv"><span>Previews</span><b>${escapeHtml(compact(sandbox.metrics?.preview_lane_count, '0'))}</b></div>
        <div class="kv"><span>Guarded writes</span><b>${escapeHtml(compact(sandbox.metrics?.guarded_write_lane_count, '0'))}</b></div>
        <div class="kv"><span>Disabled</span><b>${escapeHtml(compact(sandbox.metrics?.disabled_lane_count, '0'))}</b></div>
      </div>
      <div class="table-wrap execution-sandbox-preview">
        <table aria-label="Workbench execution sandbox">
          <thead><tr><th>Rank</th><th>Lane</th><th>Module</th><th>Status</th><th>Control</th><th>Safety</th><th>Next</th></tr></thead>
          <tbody>
            ${lanes.map((lane) => `
              <tr
                data-testid="execution-sandbox-lane"
                data-execution-lane-status="${escapeHtml(lane.status || 'unknown')}"
                data-execution-lane-kind="${escapeHtml(lane.lane_kind || 'unknown')}"
              >
                <td data-label="Rank">${escapeHtml(compact(lane.rank, '-'))}</td>
                <td data-label="Lane">${escapeHtml(compact(lane.label, lane.id || '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(lane.module || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(lane.status || '-'))}</td>
                <td data-label="Control">${renderWorkbenchActionControl(executionSandboxLaneControl(lane))}</td>
                <td data-label="Safety">${escapeHtml(catalogLabel(lane.safety || '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(lane.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function executionSandboxLaneControl(lane) {
  const navigable = lane?.action_kind === 'page';
  const preview = lane?.lane_kind === 'preview';
  return {
    ...lane,
    status: (navigable || preview) ? 'enabled' : lane?.status,
  };
}

function executionSandboxSummary(sandbox) {
  const metrics = sandbox?.metrics || {};
  return [
    `${compact(sandbox?.ticker, 'No ticker')} ${catalogLabel(sandbox?.status || 'unknown')}`,
    `${compact(metrics.preview_lane_count, '0')} preview lanes`,
    `${compact(metrics.approval_required_count, '0')} approval required`,
    `${compact(metrics.disabled_lane_count, '0')} disabled boundaries`,
    'live trading disabled',
  ].join('; ');
}

function renderWorkbenchDecisionBrief(snapshot) {
  const brief = workbenchDecisionBrief(snapshot);
  if (!brief || !brief.schema_version) return '';
  const scout = brief.scout || {};
  const setup = brief.setup || {};
  const risk = brief.risk || {};
  const workflow = brief.workflow || {};
  const nextAction = brief.next_action || {};
  const evidence = Array.isArray(brief.evidence_chain) ? brief.evidence_chain : [];
  return `
    <section
      class="panel wide workbench-decision-brief"
      data-testid="workbench-decision-brief"
      data-decision-brief-status="${escapeHtml(brief.status || 'unknown')}"
      data-decision-brief-ticker="${escapeHtml(brief.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Decision Brief</h2>
          <p>${escapeHtml(decisionBriefSummary(brief))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(brief.status || 'unknown'))}</span>
      </div>
      <div class="decision-brief-grid">
        <div class="decision-brief-block" data-testid="decision-brief-source">
          <h3>Source</h3>
          <div class="kv-grid">
            <div class="kv"><span>Tool</span><b>${escapeHtml(compact(brief.source_tool, 'market-radar'))}</b></div>
            <div class="kv"><span>Ticker</span><b>${escapeHtml(compact(brief.ticker, '-'))}</b></div>
            <div class="kv"><span>Scout</span><b>${escapeHtml(compact(scout.subject, '-'))}</b></div>
            <div class="kv"><span>Score</span><b>${escapeHtml(text(scout.score))}</b></div>
          </div>
        </div>
        <div class="decision-brief-block" data-testid="decision-brief-setup">
          <h3>Setup</h3>
          <div class="kv-grid">
            <div class="kv"><span>Type</span><b>${escapeHtml(compact(setup.setup_type, '-'))}</b></div>
            <div class="kv"><span>Direction</span><b>${escapeHtml(compact(setup.direction, '-'))}</b></div>
            <div class="kv"><span>Entry</span><b>${escapeHtml(text(setup.entry_price))}</b></div>
            <div class="kv"><span>Invalidation</span><b>${escapeHtml(text(setup.invalidation_price))}</b></div>
          </div>
        </div>
        <div class="decision-brief-block" data-testid="decision-brief-risk">
          <h3>Risk</h3>
          <div class="kv-grid">
            <div class="kv"><span>Paper blocks</span><b>${escapeHtml(compact(risk.paper_block_count, '0'))}</b></div>
            <div class="kv"><span>Live blocks</span><b>${escapeHtml(compact(risk.live_block_count, '0'))}</b></div>
            <div class="kv"><span>Max loss</span><b>${escapeHtml(text(risk.estimated_max_loss))}</b></div>
            <div class="kv"><span>Approval</span><b>${escapeHtml(risk.requires_manual_approval ? 'manual required' : 'not required')}</b></div>
          </div>
        </div>
        <div class="decision-brief-block" data-testid="decision-brief-next-action">
          <h3>Next</h3>
          <div class="kv-grid">
            <div class="kv"><span>Stage</span><b>${escapeHtml(compact(workflow.active_stage_id, '-'))}</b></div>
            <div class="kv"><span>Gate</span><b>${escapeHtml(compact(workflow.primary_supervision_gate_id, '-'))}</b></div>
            <div class="kv"><span>Command</span><b>${escapeHtml(compact(nextAction.command, '-'))}</b></div>
            <div class="kv"><span>Safety</span><b>${escapeHtml(catalogLabel(nextAction.safety || '-'))}</b></div>
          </div>
          <div class="decision-brief-action">${renderWorkbenchActionControl(nextAction)}</div>
        </div>
      </div>
      <div class="decision-brief-evidence" aria-label="Decision brief evidence chain">
        ${evidence.map((row) => `
          <article
            data-testid="decision-brief-evidence-row"
            data-evidence-status="${escapeHtml(row.status || 'unknown')}"
          >
            <span>${escapeHtml(compact(row.label, row.step || '-'))}</span>
            <b>${escapeHtml(catalogLabel(row.status || '-'))}</b>
            <p>${escapeHtml(compact(row.artifact, '-'))}</p>
          </article>
        `).join('')}
      </div>
    </section>
  `;
}

function decisionBriefSummary(brief) {
  const metrics = brief?.metrics || {};
  return [
    compact(brief?.headline, 'No active decision brief'),
    `${compact(metrics.paper_block_count, '0')} paper blocks`,
    `${compact(metrics.live_block_count, '0')} live blocks`,
    `${compact(metrics.approval_required_count, '0')} approval gates`,
    'zero provider calls',
  ].join('; ');
}

function renderWorkbenchScenarioMatrix(snapshot, pageKey = 'overview') {
  const matrix = workbenchScenarioMatrix(snapshot);
  const rows = workbenchScenarioMatrixForPage(pageKey, snapshot);
  const assumptions = matrix.assumptions || {};
  if (!rows.length) return '';
  return `
    <section
      class="panel wide workbench-scenario-matrix"
      data-testid="workbench-scenario-matrix"
      data-scenario-matrix-status="${escapeHtml(matrix.status || 'unknown')}"
      data-scenario-matrix-ticker="${escapeHtml(matrix.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Scenario Matrix</h2>
          <p>${escapeHtml(scenarioMatrixSummary(matrix))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(matrix.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Entry</span><b>${escapeHtml(text(assumptions.entry_price))}</b></div>
        <div class="kv"><span>Invalidation</span><b>${escapeHtml(text(assumptions.invalidation_price))}</b></div>
        <div class="kv"><span>Target</span><b>${escapeHtml(text(assumptions.target_price))}</b></div>
        <div class="kv"><span>Sizing</span><b>${escapeHtml(catalogLabel(assumptions.sizing_status || 'unknown'))}</b></div>
      </div>
      <div class="table-wrap scenario-matrix-preview">
        <table aria-label="Workbench scenario matrix">
          <thead><tr><th>Scenario</th><th>Price</th><th>Move</th><th>P/L Share</th><th>Status</th><th>Boundary</th><th>Next</th></tr></thead>
          <tbody>
            ${rows.map((row) => `
              <tr
                data-testid="workbench-scenario-row"
                data-scenario-kind="${escapeHtml(row.scenario_kind || 'unknown')}"
                data-scenario-status="${escapeHtml(row.status || 'unknown')}"
              >
                <td data-label="Scenario">${escapeHtml(compact(row.label, row.id || '-'))}</td>
                <td data-label="Price">${escapeHtml(text(row.price))}</td>
                <td data-label="Move">${escapeHtml(text(row.move_pct))}</td>
                <td data-label="P/L Share">${escapeHtml(text(row.pnl_per_share))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
                <td data-label="Boundary">${escapeHtml(catalogLabel(row.boundary || '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function scenarioMatrixSummary(matrix) {
  const metrics = matrix?.metrics || {};
  const assumptions = matrix?.assumptions || {};
  return [
    `${compact(matrix?.ticker, 'No ticker')} ${catalogLabel(matrix?.status || 'unknown')}`,
    `${compact(metrics.scenario_count, '0')} scenarios`,
    `R/R ${compact(metrics.risk_reward, 'n/a')}`,
    `sizing ${catalogLabel(assumptions.sizing_status || 'unknown')}`,
    'zero provider calls',
  ].join('; ');
}

function renderWorkbenchPortfolioImpact(snapshot, pageKey = 'overview') {
  const preview = workbenchPortfolioImpact(snapshot);
  const checks = workbenchPortfolioImpactForPage(pageKey, snapshot);
  const impact = preview.impact || {};
  const exposures = Array.isArray(preview.exposures) ? preview.exposures : [];
  if (!checks.length && !exposures.length) return '';
  return `
    <section
      class="panel wide workbench-portfolio-impact-preview"
      data-testid="workbench-portfolio-impact-preview"
      data-portfolio-impact-status="${escapeHtml(preview.status || 'unknown')}"
      data-portfolio-impact-ticker="${escapeHtml(preview.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Portfolio Impact Preview</h2>
          <p>${escapeHtml(portfolioImpactSummary(preview))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(preview.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Proposed notional</span><b>${escapeHtml(text(impact.proposed_notional))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(impact.max_loss))}</b></div>
        <div class="kv"><span>Gross exposure</span><b>${escapeHtml(text(impact.current_gross_exposure_pct))}</b></div>
        <div class="kv"><span>Broker data</span><b>${escapeHtml(impact.broker_data_stale ? 'stale' : 'current')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Notional/equity</span><b>${escapeHtml(text(impact.proposed_notional_pct_of_equity))}</b></div>
        <div class="kv"><span>Loss/equity</span><b>${escapeHtml(text(impact.max_loss_pct_of_equity))}</b></div>
        <div class="kv"><span>Hard blocks</span><b>${escapeHtml(compact(impact.hard_block_count, '0'))}</b></div>
        <div class="kv"><span>Live</span><b>${escapeHtml(impact.live_trading_enabled ? 'enabled' : 'disabled')}</b></div>
      </div>
      <div class="table-wrap portfolio-impact-preview">
        <table aria-label="Workbench portfolio impact exposure scopes">
          <thead><tr><th>Scope</th><th>Status</th><th>Before</th><th>After</th><th>Delta</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${exposures.map((row) => `
              <tr
                data-testid="portfolio-impact-exposure"
                data-portfolio-impact-exposure-status="${escapeHtml(row.status || 'unknown')}"
                data-portfolio-impact-exposure-scope="${escapeHtml(row.scope || 'unknown')}"
              >
                <td data-label="Scope">${escapeHtml(compact(row.label, row.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
                <td data-label="Before">${escapeHtml(text(row.before_pct))}</td>
                <td data-label="After">${escapeHtml(text(row.after_pct))}</td>
                <td data-label="Delta">${escapeHtml(text(row.delta_pct))}</td>
                <td data-label="Finding">${escapeHtml(compact(row.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
      <div class="table-wrap portfolio-impact-check-preview">
        <table aria-label="Workbench portfolio impact checks">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="portfolio-impact-check"
                data-portfolio-impact-check-status="${escapeHtml(check.status || 'unknown')}"
                data-portfolio-impact-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function portfolioImpactSummary(preview) {
  const impact = preview?.impact || {};
  const metrics = preview?.metrics || {};
  return [
    `${compact(preview?.ticker, 'No ticker')} ${catalogLabel(preview?.status || 'unknown')}`,
    `notional ${compact(impact.proposed_notional, 'n/a')}`,
    `max loss ${compact(impact.max_loss, 'n/a')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    'live submission disabled',
  ].join('; ');
}

function renderWorkbenchPositionSizing(snapshot, pageKey = 'overview') {
  const sizing = workbenchPositionSizing(snapshot);
  const checks = workbenchPositionSizingForPage(pageKey, snapshot);
  const inputs = sizing.inputs || {};
  const recommendation = sizing.recommendation || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-position-sizing"
      data-testid="workbench-position-sizing"
      data-position-sizing-status="${escapeHtml(sizing.status || 'unknown')}"
      data-position-sizing-ticker="${escapeHtml(sizing.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Position Sizing</h2>
          <p>${escapeHtml(positionSizingSummary(sizing))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(sizing.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Suggested shares</span><b>${escapeHtml(compact(recommendation.suggested_quantity, '-'))}</b></div>
        <div class="kv"><span>Risk budget</span><b>${escapeHtml(text(recommendation.risk_budget))}</b></div>
        <div class="kv"><span>Notional</span><b>${escapeHtml(text(recommendation.estimated_notional))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(recommendation.estimated_max_loss))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Entry</span><b>${escapeHtml(text(inputs.entry_price))}</b></div>
        <div class="kv"><span>Invalidation</span><b>${escapeHtml(text(inputs.invalidation_price))}</b></div>
        <div class="kv"><span>Risk/share</span><b>${escapeHtml(text(inputs.risk_per_share))}</b></div>
        <div class="kv"><span>Current qty</span><b>${escapeHtml(compact(inputs.current_quantity, '-'))}</b></div>
      </div>
      <div class="table-wrap position-sizing-preview">
        <table aria-label="Workbench position sizing worksheet">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="position-sizing-check"
                data-position-sizing-check-status="${escapeHtml(check.status || 'unknown')}"
                data-position-sizing-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function positionSizingSummary(sizing) {
  const metrics = sizing?.metrics || {};
  const recommendation = sizing?.recommendation || {};
  return [
    `${compact(sizing?.ticker, 'No ticker')} ${catalogLabel(sizing?.status || 'unknown')}`,
    `${compact(recommendation.suggested_quantity, '0')} suggested shares`,
    `risk budget ${compact(recommendation.risk_budget, 'n/a')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    'review-only sizing',
  ].join('; ');
}

function renderWorkbenchCapitalAllocation(snapshot, pageKey = 'overview') {
  const allocation = workbenchCapitalAllocation(snapshot);
  const checks = workbenchCapitalAllocationForPage(pageKey, snapshot);
  const capital = allocation.capital_context || {};
  const plan = allocation.allocation_plan || {};
  const exposure = allocation.exposure_context || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-capital-allocation"
      data-testid="workbench-capital-allocation"
      data-capital-allocation-status="${escapeHtml(allocation.status || 'unknown')}"
      data-capital-allocation-ticker="${escapeHtml(allocation.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Capital Allocation</h2>
          <p>${escapeHtml(capitalAllocationSummary(allocation))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(allocation.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Equity</span><b>${escapeHtml(text(capital.portfolio_equity))}</b></div>
        <div class="kv"><span>Cash</span><b>${escapeHtml(text(capital.cash))}</b></div>
        <div class="kv"><span>Buying power</span><b>${escapeHtml(text(capital.buying_power))}</b></div>
        <div class="kv"><span>Broker data</span><b>${escapeHtml(capital.broker_data_stale ? 'stale' : 'current')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Suggested notional</span><b>${escapeHtml(text(plan.suggested_notional))}</b></div>
        <div class="kv"><span>Risk budget</span><b>${escapeHtml(text(plan.risk_budget))}</b></div>
        <div class="kv"><span>Buying power use</span><b>${escapeHtml(text(plan.buying_power_usage_pct))}</b></div>
        <div class="kv"><span>Allocation</span><b>${escapeHtml(plan.allocation_allowed ? 'allowed' : 'disabled')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Gross exposure</span><b>${escapeHtml(text(exposure.current_gross_exposure_pct))}</b></div>
        <div class="kv"><span>Projected notional</span><b>${escapeHtml(text(exposure.projected_notional_pct_of_equity))}</b></div>
        <div class="kv"><span>Exposure scopes</span><b>${escapeHtml(`${compact(exposure.ready_exposure_scope_count, '0')}/${compact(exposure.exposure_scope_count, '0')}`)}</b></div>
        <div class="kv"><span>Open orders</span><b>${escapeHtml(text(capital.open_order_count || 0))}</b></div>
      </div>
      <div class="table-wrap capital-allocation-preview">
        <table aria-label="Workbench capital allocation">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="capital-allocation-check"
                data-capital-allocation-check-status="${escapeHtml(check.status || 'unknown')}"
                data-capital-allocation-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(check.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function capitalAllocationSummary(allocation) {
  const metrics = allocation?.metrics || {};
  const plan = allocation?.allocation_plan || {};
  return [
    `${compact(allocation?.ticker, 'No ticker')} ${catalogLabel(allocation?.status || 'unknown')}`,
    `suggested ${compact(plan.suggested_notional, 'n/a')}`,
    `buying power ${compact(plan.buying_power_usage_pct, 'n/a')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    'allocation changes disabled',
  ].join('; ');
}

function renderWorkbenchOrderTicketDraft(snapshot, pageKey = 'overview') {
  const draft = workbenchOrderTicketDraft(snapshot);
  const checks = workbenchOrderTicketDraftForPage(pageKey, snapshot);
  const ticket = draft.ticket || {};
  const commands = draft.commands || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-order-ticket-draft"
      data-testid="workbench-order-ticket-draft"
      data-order-ticket-draft-status="${escapeHtml(draft.status || 'unknown')}"
      data-order-ticket-draft-ticker="${escapeHtml(draft.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Order Ticket Draft</h2>
          <p>${escapeHtml(orderTicketDraftSummary(draft))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(draft.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Suggested qty</span><b>${escapeHtml(compact(ticket.suggested_quantity, '-'))}</b></div>
        <div class="kv"><span>Limit</span><b>${escapeHtml(text(ticket.limit_price))}</b></div>
        <div class="kv"><span>Stop</span><b>${escapeHtml(text(ticket.stop_price))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(ticket.estimated_max_loss))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Side</span><b>${escapeHtml(catalogLabel(ticket.side || '-'))}</b></div>
        <div class="kv"><span>Mode</span><b>${escapeHtml(catalogLabel(draft.ticket_mode || '-'))}</b></div>
        <div class="kv"><span>Submission</span><b>${escapeHtml(ticket.submission_allowed ? 'allowed' : 'disabled')}</b></div>
        <div class="kv"><span>Live</span><b>${escapeHtml(ticket.live_trading_enabled ? 'enabled' : 'disabled')}</b></div>
      </div>
      <div class="plan-command-list order-ticket-draft-commands">
        <div><span>Preview</span><code>${escapeHtml(compact(commands.preview, 'order-ticket preview'))}</code></div>
        <div><span>Record</span><code>${escapeHtml(compact(commands.record, 'order-ticket record'))}</code></div>
        <div><span>Live boundary</span><code>${escapeHtml(compact(commands.live_submit, 'broker live submission'))}</code></div>
      </div>
      <div class="table-wrap order-ticket-draft-preview">
        <table aria-label="Workbench order ticket draft">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="order-ticket-draft-check"
                data-order-ticket-draft-check-status="${escapeHtml(check.status || 'unknown')}"
                data-order-ticket-draft-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function orderTicketDraftSummary(draft) {
  const metrics = draft?.metrics || {};
  const ticket = draft?.ticket || {};
  return [
    `${compact(draft?.ticker, 'No ticker')} ${catalogLabel(draft?.status || 'unknown')}`,
    `${compact(ticket.suggested_quantity, '0')} suggested shares`,
    `limit ${compact(ticket.limit_price, 'n/a')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    'live submission disabled',
  ].join('; ');
}

function renderWorkbenchPaperTradePreview(snapshot, pageKey = 'overview') {
  const preview = workbenchPaperTradePreview(snapshot);
  const checks = workbenchPaperTradePreviewForPage(pageKey, snapshot);
  const decision = preview.paper_decision || {};
  const commands = preview.commands || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-paper-trade-preview"
      data-testid="workbench-paper-trade-preview"
      data-paper-trade-preview-status="${escapeHtml(preview.status || 'unknown')}"
      data-paper-trade-preview-ticker="${escapeHtml(preview.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Paper Trade Preview</h2>
          <p>${escapeHtml(paperTradePreviewSummary(preview))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(preview.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Decision</span><b>${escapeHtml(catalogLabel(decision.decision || '-'))}</b></div>
        <div class="kv"><span>Suggested qty</span><b>${escapeHtml(compact(decision.suggested_quantity, '-'))}</b></div>
        <div class="kv"><span>Confirmed qty</span><b>${escapeHtml(compact(decision.confirmed_quantity, '-'))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(decision.estimated_max_loss))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Entry</span><b>${escapeHtml(text(decision.entry_price))}</b></div>
        <div class="kv"><span>Suggested notional</span><b>${escapeHtml(text(decision.suggested_notional))}</b></div>
        <div class="kv"><span>Record writes</span><b>${escapeHtml(compact(decision.record_db_writes_required, '0'))}</b></div>
        <div class="kv"><span>Live</span><b>${escapeHtml(preview.live_trading_enabled ? 'enabled' : 'disabled')}</b></div>
      </div>
      <div class="plan-command-list paper-trade-preview-commands">
        <div><span>Preview</span><code>${escapeHtml(compact(commands.preview, 'paper-decision preview'))}</code></div>
        <div><span>Record</span><code>${escapeHtml(compact(commands.record, 'paper-decision execute'))}</code></div>
        <div><span>Live boundary</span><code>${escapeHtml(compact(commands.live_submit, 'broker live submission'))}</code></div>
      </div>
      <div class="table-wrap paper-trade-preview-checks">
        <table aria-label="Workbench paper trade preview">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="paper-trade-preview-check"
                data-paper-trade-check-status="${escapeHtml(check.status || 'unknown')}"
                data-paper-trade-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function paperTradePreviewSummary(preview) {
  const metrics = preview?.metrics || {};
  const decision = preview?.paper_decision || {};
  return [
    `${compact(preview?.ticker, 'No ticker')} ${catalogLabel(preview?.status || 'unknown')}`,
    `decision ${catalogLabel(decision.decision || 'n/a')}`,
    `${compact(decision.suggested_quantity, '0')} suggested shares`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    'record requires approval',
  ].join('; ');
}

function renderWorkbenchPretradeCompliance(snapshot, pageKey = 'overview') {
  const compliance = workbenchPretradeCompliance(snapshot);
  const checks = workbenchPretradeComplianceForPage(pageKey, snapshot);
  const trade = compliance.trade_context || {};
  const boundary = compliance.boundary_context || {};
  const metrics = compliance.metrics || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-pretrade-compliance"
      data-testid="workbench-pretrade-compliance"
      data-pretrade-compliance-status="${escapeHtml(compliance.status || 'unknown')}"
      data-pretrade-compliance-ticker="${escapeHtml(compliance.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Pre-Trade Compliance</h2>
          <p>${escapeHtml(pretradeComplianceSummary(compliance))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(compliance.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Plan</span><b>${escapeHtml(catalogLabel(trade.active_plan_status || '-'))}</b></div>
        <div class="kv"><span>Action state</span><b>${escapeHtml(catalogLabel(trade.action_state || '-'))}</b></div>
        <div class="kv"><span>Paper approved</span><b>${escapeHtml(trade.paper_approved ? 'yes' : 'no')}</b></div>
        <div class="kv"><span>Live approved</span><b>${escapeHtml(trade.live_approved ? 'yes' : 'no')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Suggested notional</span><b>${escapeHtml(text(trade.suggested_notional))}</b></div>
        <div class="kv"><span>Suggested qty</span><b>${escapeHtml(compact(trade.suggested_quantity, '-'))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(trade.estimated_max_loss))}</b></div>
        <div class="kv"><span>Allocation</span><b>${escapeHtml(trade.allocation_allowed ? 'allowed' : 'blocked')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Manual approval</span><b>${escapeHtml(boundary.requires_manual_approval ? 'required' : 'clear')}</b></div>
        <div class="kv"><span>Approval gates</span><b>${escapeHtml(compact(boundary.approval_required_count, '0'))}</b></div>
        <div class="kv"><span>Broker orders</span><b>${escapeHtml(boundary.broker_order_submission || 'disabled')}</b></div>
        <div class="kv"><span>Autonomous</span><b>${escapeHtml(boundary.autonomous_execution || 'disabled')}</b></div>
      </div>
      <div class="table-wrap pretrade-compliance-preview">
        <table aria-label="Workbench pre-trade compliance">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="pretrade-compliance-check"
                data-pretrade-compliance-check-status="${escapeHtml(check.status || 'unknown')}"
                data-pretrade-compliance-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(check.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function pretradeComplianceSummary(compliance) {
  const metrics = compliance?.metrics || {};
  return [
    `${compact(compliance?.ticker, 'No ticker')} ${catalogLabel(compliance?.status || 'unknown')}`,
    `primary ${compact(compliance?.primary_blocker, 'none')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    `${compact(metrics.approval_required_count, '0')} approval gates`,
    'live execution disabled',
  ].join('; ');
}

function renderWorkbenchTradeReadinessBrief(snapshot, pageKey = 'overview') {
  const brief = workbenchTradeReadinessBrief(snapshot);
  const checks = workbenchTradeReadinessBriefForPage(pageKey, snapshot);
  const modes = brief.readiness_modes || {};
  const paper = modes.paper_record || {};
  const broker = modes.broker_handoff || {};
  const strategy = modes.strategy_update || {};
  const monitoring = modes.monitoring || {};
  const handoff = brief.agent_handoff || {};
  const metrics = brief.metrics || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-trade-readiness-brief"
      data-testid="workbench-trade-readiness-brief"
      data-trade-readiness-status="${escapeHtml(brief.status || 'unknown')}"
      data-trade-readiness-ticker="${escapeHtml(brief.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Trade Readiness Brief</h2>
          <p>${escapeHtml(tradeReadinessBriefSummary(brief))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(brief.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Next page</span><b>${escapeHtml(compact(handoff.next_page, '-'))}</b></div>
        <div class="kv"><span>Next command</span><b>${escapeHtml(compact(handoff.next_command, '-'))}</b></div>
        <div class="kv"><span>Safety</span><b>${escapeHtml(catalogLabel(handoff.safety || '-'))}</b></div>
        <div class="kv"><span>No approval</span><b>${escapeHtml(handoff.can_execute_without_approval ? 'yes' : 'no')}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Paper record</span><b>${escapeHtml(catalogLabel(paper.status || '-'))}</b></div>
        <div class="kv"><span>Broker handoff</span><b>${escapeHtml(catalogLabel(broker.status || '-'))}</b></div>
        <div class="kv"><span>Strategy update</span><b>${escapeHtml(catalogLabel(strategy.status || '-'))}</b></div>
        <div class="kv"><span>Monitoring</span><b>${escapeHtml(catalogLabel(monitoring.status || '-'))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Blocked checks</span><b>${escapeHtml(compact(metrics.blocked_check_count, '0'))}</b></div>
        <div class="kv"><span>Approval checks</span><b>${escapeHtml(compact(metrics.approval_required_count, '0'))}</b></div>
        <div class="kv"><span>Disabled checks</span><b>${escapeHtml(compact(metrics.disabled_check_count, '0'))}</b></div>
        <div class="kv"><span>Provider calls</span><b>${escapeHtml(compact(brief.external_calls_made, '0'))}</b></div>
      </div>
      <div class="table-wrap trade-readiness-preview">
        <table aria-label="Workbench trade readiness brief">
          <thead><tr><th>Gate</th><th>Module</th><th>Status</th><th>Kind</th><th>Finding</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="trade-readiness-check"
                data-trade-readiness-check-status="${escapeHtml(check.status || 'unknown')}"
                data-trade-readiness-check-kind="${escapeHtml(check.gate_kind || 'unknown')}"
                data-trade-readiness-check-module="${escapeHtml(check.module || 'unknown')}"
              >
                <td data-label="Gate">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(check.module || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Kind">${escapeHtml(catalogLabel(check.gate_kind || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(check.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function tradeReadinessBriefSummary(brief) {
  const metrics = brief?.metrics || {};
  return [
    `${compact(brief?.ticker, 'No ticker')} ${catalogLabel(brief?.status || 'unknown')}`,
    `primary ${compact(brief?.primary_blocker, 'none')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    `${compact(metrics.disabled_check_count, '0')} disabled boundaries`,
    'agent decision support only',
  ].join('; ');
}

function renderWorkbenchLearningLoop(snapshot, pageKey = 'overview') {
  const loop = workbenchLearningLoop(snapshot);
  const cards = workbenchLearningLoopForPage(pageKey, snapshot);
  const signal = loop.primary_signal || {};
  const validation = loop.validation_state || {};
  const journal = loop.journal_state || {};
  const paper = loop.paper_state || {};
  if (!cards.length) return '';
  return `
    <section
      class="panel wide workbench-learning-loop"
      data-testid="workbench-learning-loop"
      data-learning-loop-status="${escapeHtml(loop.status || 'unknown')}"
      data-learning-loop-stage="${escapeHtml(loop.learning_stage || 'unlinked')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Learning Loop</h2>
          <p>${escapeHtml(learningLoopSummary(loop))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(loop.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Stage</span><b>${escapeHtml(catalogLabel(loop.learning_stage || 'unlinked'))}</b></div>
        <div class="kv"><span>Paper</span><b>${escapeHtml(catalogLabel(paper.paper_state || paper.preview_status || '-'))}</b></div>
        <div class="kv"><span>Validation</span><b>${escapeHtml(compact(validation.final_score, '-'))}</b></div>
        <div class="kv"><span>Outcome</span><b>${escapeHtml(catalogLabel(journal.outcome_status || '-'))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Decision</span><b>${escapeHtml(catalogLabel(signal.paper_decision || signal.recommended_paper_decision || '-'))}</b></div>
        <div class="kv"><span>Suggested qty</span><b>${escapeHtml(compact(signal.suggested_quantity, '-'))}</b></div>
        <div class="kv"><span>20D return</span><b>${escapeHtml(text(journal.return_20d))}</b></div>
        <div class="kv"><span>SPY relative</span><b>${escapeHtml(text(journal.spy_relative_return_20d))}</b></div>
      </div>
      <div class="plan-command-list learning-loop-commands">
        <div><span>Validation</span><code>${escapeHtml(compact(validation.validation_result_id, 'no validation result'))}</code></div>
        <div><span>Ledger</span><code>${escapeHtml(compact(journal.ledger_entry_id, 'no ledger entry'))}</code></div>
        <div><span>Outcome</span><code>${escapeHtml(compact(journal.primary_command, 'no outcome command'))}</code></div>
      </div>
      <div class="table-wrap learning-loop-preview">
        <table aria-label="Workbench learning loop">
          <thead><tr><th>Step</th><th>Module</th><th>Status</th><th>Finding</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${cards.map((card) => `
              <tr
                data-testid="learning-loop-card"
                data-learning-loop-card-status="${escapeHtml(card.status || 'unknown')}"
                data-learning-loop-card-module="${escapeHtml(card.module || 'unknown')}"
              >
                <td data-label="Step">${escapeHtml(compact(card.label, card.id || '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(card.module || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(card.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(card.finding, '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(card.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(card.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function learningLoopSummary(loop) {
  const metrics = loop?.metrics || {};
  return [
    `${compact(loop?.ticker, 'No ticker')} ${catalogLabel(loop?.status || 'unknown')}`,
    `${catalogLabel(loop?.learning_stage || 'unlinked')} stage`,
    `${compact(metrics.validation_result_count, '0')} validation results`,
    `${compact(metrics.linked_outcome_count, '0')} linked outcomes`,
    'strategy updates disabled',
  ].join('; ');
}

function renderWorkbenchStrategyReview(snapshot, pageKey = 'overview') {
  const review = workbenchStrategyReview(snapshot);
  const hypotheses = workbenchStrategyReviewForPage(pageKey, snapshot);
  const context = review.strategy_context || {};
  const evidence = review.evidence || {};
  const recommendation = review.recommendation || {};
  const commands = review.commands || {};
  if (!hypotheses.length) return '';
  return `
    <section
      class="panel wide workbench-strategy-review"
      data-testid="workbench-strategy-review"
      data-strategy-review-status="${escapeHtml(review.status || 'unknown')}"
      data-strategy-review-stage="${escapeHtml(review.strategy_stage || 'unlinked')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Strategy Review</h2>
          <p>${escapeHtml(strategyReviewSummary(review))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(review.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Stage</span><b>${escapeHtml(catalogLabel(review.strategy_stage || 'unlinked'))}</b></div>
        <div class="kv"><span>Decision</span><b>${escapeHtml(catalogLabel(recommendation.decision || '-'))}</b></div>
        <div class="kv"><span>Update</span><b>${escapeHtml(recommendation.strategy_update_allowed ? 'allowed' : 'disabled')}</b></div>
        <div class="kv"><span>Validation</span><b>${escapeHtml(compact(evidence.final_score, '-'))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Reward/risk</span><b>${escapeHtml(compact(context.reward_risk, '-'))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(context.estimated_max_loss))}</b></div>
        <div class="kv"><span>20D return</span><b>${escapeHtml(text(evidence.return_20d))}</b></div>
        <div class="kv"><span>SPY relative</span><b>${escapeHtml(text(evidence.spy_relative_return_20d))}</b></div>
      </div>
      <div class="plan-command-list strategy-review-commands">
        <div><span>Review</span><code>${escapeHtml(compact(commands.review, 'agent'))}</code></div>
        <div><span>Validation</span><code>${escapeHtml(compact(commands.validation, 'validation'))}</code></div>
        <div><span>Journal</span><code>${escapeHtml(compact(commands.journal, 'journal'))}</code></div>
        <div><span>Update boundary</span><code>${escapeHtml(compact(commands.strategy_update, 'agent execute'))}</code></div>
      </div>
      <div class="table-wrap strategy-review-preview">
        <table aria-label="Workbench strategy review">
          <thead><tr><th>Hypothesis</th><th>Driver</th><th>Status</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${hypotheses.map((row) => `
              <tr
                data-testid="strategy-review-hypothesis"
                data-strategy-hypothesis-status="${escapeHtml(row.status || 'unknown')}"
                data-strategy-hypothesis-driver="${escapeHtml(row.driver || 'unknown')}"
              >
                <td data-label="Hypothesis">${escapeHtml(compact(row.label, row.id || '-'))}</td>
                <td data-label="Driver">${escapeHtml(catalogLabel(row.driver || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(row.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function strategyReviewSummary(review) {
  const metrics = review?.metrics || {};
  return [
    `${compact(review?.ticker, 'No ticker')} ${catalogLabel(review?.status || 'unknown')}`,
    `${catalogLabel(review?.strategy_stage || 'unlinked')} stage`,
    `${compact(metrics.hypothesis_count, '0')} hypotheses`,
    `${compact(metrics.blocked_hypothesis_count, '0')} blocked`,
    'autonomous updates disabled',
  ].join('; ');
}

function renderWorkbenchTradeMonitor(snapshot, pageKey = 'overview') {
  const monitor = workbenchTradeMonitor(snapshot);
  const items = workbenchTradeMonitorForPage(pageKey, snapshot);
  const active = monitor.active_trade || {};
  const risk = monitor.risk_watch || {};
  const alerts = monitor.alert_watch || {};
  const exit = monitor.exit_plan || {};
  const commands = monitor.commands || {};
  if (!items.length) return '';
  return `
    <section
      class="panel wide workbench-trade-monitor"
      data-testid="workbench-trade-monitor"
      data-trade-monitor-status="${escapeHtml(monitor.status || 'unknown')}"
      data-trade-monitor-stage="${escapeHtml(monitor.monitor_stage || 'unlinked')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Trade Monitor</h2>
          <p>${escapeHtml(tradeMonitorSummary(monitor))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(monitor.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Stage</span><b>${escapeHtml(catalogLabel(monitor.monitor_stage || 'unlinked'))}</b></div>
        <div class="kv"><span>Paper</span><b>${escapeHtml(catalogLabel(active.paper_state || '-'))}</b></div>
        <div class="kv"><span>Exit</span><b>${escapeHtml(catalogLabel(exit.stop_status || '-'))}</b></div>
        <div class="kv"><span>Trigger</span><b>${escapeHtml(compact(alerts.primary_trigger_id, 'none'))}</b></div>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Shares</span><b>${escapeHtml(text(active.shares))}</b></div>
        <div class="kv"><span>Notional</span><b>${escapeHtml(text(active.notional))}</b></div>
        <div class="kv"><span>Max loss</span><b>${escapeHtml(text(active.max_loss || risk.estimated_max_loss))}</b></div>
        <div class="kv"><span>Open orders</span><b>${escapeHtml(text(monitor.metrics?.open_order_count || 0))}</b></div>
      </div>
      <div class="plan-command-list trade-monitor-commands">
        <div><span>Paper</span><code>${escapeHtml(compact(commands.paper_trade, 'paper'))}</code></div>
        <div><span>Alerts</span><code>${escapeHtml(compact(commands.alerts, 'alerts'))}</code></div>
        <div><span>Journal</span><code>${escapeHtml(compact(commands.journal, 'journal'))}</code></div>
        <div><span>Broker boundary</span><code>${escapeHtml(compact(commands.broker_boundary, 'broker'))}</code></div>
      </div>
      <div class="table-wrap trade-monitor-preview">
        <table aria-label="Workbench trade monitor">
          <thead><tr><th>Watch</th><th>Scope</th><th>Status</th><th>Finding</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${items.map((item) => `
              <tr
                data-testid="trade-monitor-watch-item"
                data-trade-monitor-item-status="${escapeHtml(item.status || 'unknown')}"
                data-trade-monitor-item-scope="${escapeHtml(item.scope || 'unknown')}"
              >
                <td data-label="Watch">${escapeHtml(compact(item.label, item.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(item.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(item.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(item.finding, '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(item.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(item.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function tradeMonitorSummary(monitor) {
  const metrics = monitor?.metrics || {};
  return [
    `${compact(monitor?.ticker, 'No ticker')} ${catalogLabel(monitor?.status || 'unknown')}`,
    `${catalogLabel(monitor?.monitor_stage || 'unlinked')} stage`,
    `${compact(metrics.active_paper_trade_count, '0')} active paper trades`,
    `${compact(metrics.blocked_watch_item_count, '0')} blocked watch items`,
    'exit updates disabled',
  ].join('; ');
}

function renderWorkbenchRiskEnvelope(snapshot, pageKey = 'overview') {
  const envelope = workbenchRiskEnvelope(snapshot);
  const checks = workbenchRiskEnvelopeForPage(pageKey, snapshot);
  const portfolio = envelope.portfolio_context || {};
  const sizing = envelope.sizing_context || {};
  if (!checks.length) return '';
  return `
    <section
      class="panel wide workbench-risk-envelope"
      data-testid="workbench-risk-envelope"
      data-risk-envelope-status="${escapeHtml(envelope.status || 'unknown')}"
      data-risk-envelope-ticker="${escapeHtml(envelope.ticker || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Risk Envelope</h2>
          <p>${escapeHtml(riskEnvelopeSummary(envelope))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(envelope.status || 'unknown'))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Equity</span><b>${escapeHtml(text(portfolio.portfolio_equity))}</b></div>
        <div class="kv"><span>Buying Power</span><b>${escapeHtml(text(portfolio.buying_power))}</b></div>
        <div class="kv"><span>Max Loss</span><b>${escapeHtml(text(sizing.estimated_max_loss))}</b></div>
        <div class="kv"><span>Sizing</span><b>${escapeHtml(catalogLabel(sizing.sizing_status || 'unknown'))}</b></div>
      </div>
      <div class="table-wrap risk-envelope-preview">
        <table aria-label="Workbench risk envelope">
          <thead><tr><th>Check</th><th>Scope</th><th>Status</th><th>Finding</th><th>Next</th></tr></thead>
          <tbody>
            ${checks.map((check) => `
              <tr
                data-testid="workbench-risk-check"
                data-risk-check-status="${escapeHtml(check.status || 'unknown')}"
                data-risk-check-scope="${escapeHtml(check.scope || 'unknown')}"
              >
                <td data-label="Check">${escapeHtml(compact(check.label, check.id || '-'))}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(check.scope || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(check.status || '-'))}</td>
                <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function riskEnvelopeSummary(envelope) {
  const metrics = envelope?.metrics || {};
  const sizing = envelope?.sizing_context || {};
  return [
    `${compact(envelope?.ticker, 'No ticker')} ${catalogLabel(envelope?.status || 'unknown')}`,
    `${compact(metrics.blocked_check_count, '0')} blocked checks`,
    `${compact(metrics.disabled_check_count, '0')} disabled boundaries`,
    `max loss ${compact(sizing.estimated_max_loss, 'n/a')}`,
    'zero provider calls',
  ].join('; ');
}

function renderWorkbenchTradeRunbook(snapshot, pageKey = 'overview') {
  const runbook = workbenchTradeRunbook(snapshot);
  const steps = workbenchTradeRunbookForPage(pageKey, snapshot);
  if (!steps.length) return '';
  return `
    <section
      class="panel wide workbench-trade-runbook"
      data-testid="workbench-trade-runbook"
      data-runbook-status="${escapeHtml(runbook.status || 'unknown')}"
      data-runbook-active-step="${escapeHtml(runbook.active_step_id || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Trade Runbook</h2>
          <p>${escapeHtml(tradeRunbookSummary(runbook))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(runbook.status || 'unknown'))}</span>
      </div>
      <div class="table-wrap trade-runbook-preview">
        <table aria-label="Workbench trade runbook">
          <thead><tr><th>Rank</th><th>Step</th><th>Module</th><th>Status</th><th>Control</th><th>Evidence</th><th>Next</th></tr></thead>
          <tbody>
            ${steps.map((step) => `
              <tr
                data-testid="workbench-runbook-step"
                data-runbook-step-status="${escapeHtml(step.status || 'unknown')}"
                data-runbook-step-kind="${escapeHtml(step.step_kind || 'unknown')}"
              >
                <td data-label="Rank">${escapeHtml(compact(step.rank, '-'))}</td>
                <td data-label="Step">${escapeHtml(compact(step.label, step.id || '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(step.module || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(step.status || '-'))}</td>
                <td data-label="Control">${renderWorkbenchActionControl(runbookStepControl(step))}</td>
                <td data-label="Evidence">${escapeHtml(compact(step.evidence, '-'))}</td>
                <td data-label="Next">${escapeHtml(compact(step.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function runbookStepControl(step) {
  const navigable = step?.action_kind === 'page';
  const preview = step?.step_kind === 'preview';
  return {
    ...step,
    status: (navigable || preview) ? 'enabled' : step?.status,
  };
}

function tradeRunbookSummary(runbook) {
  const metrics = runbook?.metrics || {};
  return [
    `${compact(metrics.step_count, '0')} steps`,
    `${compact(metrics.blocked_step_count, '0')} blocked`,
    `${compact(metrics.approval_required_count, '0')} approval required`,
    `${compact(metrics.disabled_step_count, '0')} disabled`,
    'live trading disabled',
  ].join('; ');
}

function renderWorkbenchWorkflowMap(snapshot, pageKey = 'overview') {
  const workflow = workbenchWorkflowMap(snapshot);
  const stages = workbenchWorkflowStagesForPage(pageKey, snapshot);
  if (!stages.length) return '';
  return `
    <section
      class="panel wide workbench-workflow-map"
      data-testid="workbench-workflow-map"
      data-workflow-status="${escapeHtml(workflow.status || 'unknown')}"
      data-active-stage="${escapeHtml(workflow.active_stage_id || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Workflow Map</h2>
          <p>${escapeHtml(workflowMapSummary(workflow))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(workflow.status || 'unknown'))}</span>
      </div>
      <div class="table-wrap workflow-map-preview">
        <table aria-label="Supervised trading workflow map">
          <thead><tr><th>Stage</th><th>Module</th><th>Status</th><th>Evidence</th><th>Action</th><th>Next</th></tr></thead>
          <tbody>
            ${stages.map((stage) => `
              <tr
                data-testid="workbench-workflow-stage"
                data-stage="${escapeHtml(stage.id || '')}"
                data-stage-status="${escapeHtml(stage.status || 'unknown')}"
              >
                <td data-label="Stage">${escapeHtml(compact(stage.label, '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(stage.module || '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(stage.status || '-'))}</td>
                <td data-label="Evidence">${escapeHtml(compact(stage.evidence_count, '0'))}</td>
                <td data-label="Action">${renderWorkbenchActionControl(stage.action || {})}</td>
                <td data-label="Next">${escapeHtml(compact(stage.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function workflowMapSummary(workflow) {
  return [
    `${compact(workflow.stage_count, '0')} stages`,
    `${compact(workflow.blocked_stage_count, '0')} blocked`,
    `${compact(workflow.disabled_stage_count, '0')} disabled`,
    'MarketRadar to review to supervised paper workflow',
    'live trading disabled',
  ].join('; ');
}

function renderWorkbenchPriorityQueue(snapshot, pageKey = 'overview') {
  const queue = workbenchPriorityQueue(snapshot);
  const items = workbenchPriorityItemsForPage(pageKey, snapshot).slice(0, 12);
  if (!items.length) return '';
  return `
    <section
      class="panel wide workbench-priority-queue"
      data-testid="workbench-priority-queue"
      data-priority-queue-status="${escapeHtml(queue.status || 'empty')}"
      data-primary-priority-item="${escapeHtml(queue.primary_item_id || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Priority Queue</h2>
          <p>${escapeHtml(priorityQueueSummary(queue))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(queue.status || 'empty'))}</span>
      </div>
      <div class="table-wrap priority-queue-preview">
        <table aria-label="Workbench priority queue">
          <thead><tr><th>Rank</th><th>Module</th><th>Item</th><th>Status</th><th>Reason</th><th>Control</th><th>Next</th></tr></thead>
          <tbody>
            ${items.map((item) => `
              <tr
                data-testid="workbench-priority-item"
                data-priority-item-status="${escapeHtml(item.status || 'unknown')}"
                data-priority-item-kind="${escapeHtml(item.item_kind || 'unknown')}"
              >
                <td data-label="Rank">${escapeHtml(compact(item.rank, '-'))}</td>
                <td data-label="Module">${escapeHtml(catalogLabel(item.module || '-'))}</td>
                <td data-label="Item">${escapeHtml(compact(item.label, '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(item.status || '-'))}</td>
                <td data-label="Reason">${escapeHtml(compact(item.reason, '-'))}</td>
                <td data-label="Control">${renderWorkbenchActionControl(priorityItemControl(item))}</td>
                <td data-label="Next">${escapeHtml(compact(item.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function priorityItemControl(item) {
  const safeStagePage = item?.item_kind === 'workflow_stage' && item?.action_kind === 'page';
  return {
    ...item,
    status: safeStagePage ? 'enabled' : item?.status,
  };
}

function priorityQueueSummary(queue) {
  const metrics = queue?.metrics || {};
  return [
    `${compact(metrics.item_count, '0')} prioritized items`,
    `${compact(metrics.blocked_item_count, '0')} blockers`,
    `${compact(metrics.local_write_count, '0')} guarded local writes`,
    'live trading disabled',
  ].join('; ');
}

function renderWorkbenchSupervisionGates(snapshot, pageKey = 'overview') {
  const supervision = workbenchSupervisionGates(snapshot);
  const gates = workbenchSupervisionGatesForPage(pageKey, snapshot).slice(0, 8);
  if (!gates.length) return '';
  return `
    <section
      class="panel wide workbench-supervision-gates"
      data-testid="workbench-supervision-gates"
      data-supervision-status="${escapeHtml(supervision.status || 'empty')}"
      data-primary-supervision-gate="${escapeHtml(supervision.primary_gate_id || '')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Supervision Gates</h2>
          <p>${escapeHtml(supervisionGateSummary(supervision))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(supervision.status || 'empty'))}</span>
      </div>
      <div class="table-wrap supervision-gate-preview">
        <table aria-label="Workbench supervision gates">
          <thead><tr><th>Rank</th><th>Gate</th><th>Status</th><th>Approval</th><th>Scope</th><th>Control</th><th>Next</th></tr></thead>
          <tbody>
            ${gates.map((gate) => `
              <tr
                data-testid="workbench-supervision-gate"
                data-supervision-gate-status="${escapeHtml(gate.status || 'unknown')}"
                data-supervision-gate-kind="${escapeHtml(gate.gate_kind || 'unknown')}"
              >
                <td data-label="Rank">${escapeHtml(compact(gate.rank, '-'))}</td>
                <td data-label="Gate">${escapeHtml(compact(gate.label, '-'))}</td>
                <td data-label="Status">${escapeHtml(catalogLabel(gate.status || '-'))}</td>
                <td data-label="Approval">${escapeHtml(gate.approval_required ? 'required' : 'not required')}</td>
                <td data-label="Scope">${escapeHtml(catalogLabel(gate.safety || gate.gate_kind || '-'))}</td>
                <td data-label="Control">${renderWorkbenchActionControl(supervisionGateControl(gate))}</td>
                <td data-label="Next">${escapeHtml(compact(gate.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function supervisionGateControl(gate) {
  const ready = ['ready', 'enabled'].includes(gate?.status);
  return {
    ...gate,
    status: ready ? 'enabled' : gate?.status,
    local_write_allowed: Boolean(gate?.local_write_allowed),
    requires_arm_before_run: Boolean(gate?.requires_arm_before_run),
  };
}

function supervisionGateSummary(supervision) {
  const metrics = supervision?.metrics || {};
  return [
    `${compact(metrics.gate_count, '0')} gates`,
    `${compact(metrics.approval_required_count, '0')} approval required`,
    `${compact(metrics.disabled_gate_count, '0')} disabled/out of scope`,
    'provider calls zero',
  ].join('; ');
}

function renderWorkbenchActionBus(snapshot, pageKey = 'overview') {
  const bus = workbenchActionBus(snapshot);
  const actions = workbenchActionsForPage(pageKey, snapshot).slice(0, 10);
  if (!actions.length) return '';
  return `
    <section
      class="panel wide workbench-action-bus"
      data-testid="workbench-action-bus"
      data-action-bus-status="${escapeHtml(bus.status || 'empty')}"
    >
      <div class="module-title-row">
        <div>
          <h2>Action Bus</h2>
          <p>${escapeHtml(actionBusSummary(bus))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(bus.status || 'empty'))}</span>
      </div>
      <div class="table-wrap action-bus-preview">
        <table aria-label="Workbench action bus">
          <thead><tr><th>Module</th><th>Action</th><th>Safety</th><th>Writes</th><th>Control</th><th>Next</th></tr></thead>
          <tbody>
            ${actions.map((action) => `
              <tr data-testid="workbench-action-row" data-action-kind="${escapeHtml(action.action_kind || 'unknown')}">
                <td data-label="Module">${escapeHtml(catalogLabel(action.module || '-'))}</td>
                <td data-label="Action">${escapeHtml(compact(action.label, '-'))}</td>
                <td data-label="Safety">${escapeHtml(catalogLabel(action.safety || 'local_backend_preview'))}</td>
                <td data-label="Writes">${escapeHtml(action.local_write_allowed ? 'local DB' : 'none')}</td>
                <td data-label="Control">${renderWorkbenchActionControl(action)}</td>
                <td data-label="Next">${escapeHtml(compact(action.next_action, '-'))}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function actionBusSummary(bus) {
  const metrics = bus?.metrics || {};
  return [
    `${compact(metrics.action_count, '0')} supervised actions`,
    `${compact(metrics.backend_command_count, '0')} backend previews/records`,
    `${compact(metrics.boundary_count, '0')} disabled boundaries`,
    'live trading disabled',
  ].join('; ');
}

function renderWorkbenchActionControl(action) {
  const kind = compact(action?.action_kind, 'backend_command');
  const command = compact(action?.command, '');
  const targetPage = compact(action?.target_page, action?.module || '');
  const disabled = action?.status && action.status !== 'enabled';
  if (kind === 'boundary') {
    return `<code class="workbench-boundary-code">${escapeHtml(command || targetPage || 'disabled')}</code>`;
  }
  if (kind === 'page') {
    return `
      <button
        class="workbench-action-button"
        type="button"
        data-testid="workbench-action-page"
        data-workbench-action-kind="page"
        data-workbench-action-page="${escapeHtml(targetPage)}"
        data-workbench-action-label="${escapeHtml(action?.label || targetPage)}"
        ${disabled ? 'disabled' : ''}
      >Open</button>
    `;
  }
  return `
    <button
      class="workbench-action-button"
      type="button"
      data-testid="workbench-action-command"
      data-workbench-action-kind="backend_command"
      data-workbench-action-command="${escapeHtml(command)}"
      data-workbench-local-write="${action?.local_write_allowed ? 'true' : 'false'}"
      data-workbench-action-label="${escapeHtml(action?.label || command)}"
      ${disabled || !command ? 'disabled' : ''}
    >${escapeHtml(action?.local_write_allowed ? 'Run Local' : 'Preview')}</button>
  `;
}

function renderPlatformModulePage(pageKey, snapshot) {
  const module = platformModuleForPage(pageKey) || {
    key: pageKey,
    label: pageLabelFor(pageKey, state.config?.pages?.find((page) => page.key === pageKey)),
    role: 'Local trading platform module.',
    source: 'local dashboard snapshot',
    status: 'route_ready',
    next_action: 'Review the local evidence before taking action.',
  };
  const moduleData = tradingWorkbenchModule(pageKey, snapshot);
  const paths = pagePaths[pageKey] || [];
  const dataPanel = pageKey === 'market-radar'
    ? queuePanel('Market Radar Queue', rowsFromSnapshot(snapshot))
    : renderStructuredPage(module.label, paths);
  const status = moduleData?.status || module.status || 'route_ready';
  const summary = moduleData?.summary || module.role || 'Local trading platform module.';
  const nextAction = moduleData?.next_action || module.next_action || 'Review local evidence.';
  return `
    <section class="panel wide module-page" data-testid="platform-module-page" data-tool="${escapeHtml(module.key)}">
      <div class="module-title-row">
        <div>
          <h2>${escapeHtml(module.label)}</h2>
          <p>${escapeHtml(summary)}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(status))}</span>
      </div>
      <div class="module-kpis">
        <div class="kv"><span>Source</span><b>${escapeHtml(module.source || 'local dashboard snapshot')}</b></div>
        <div class="kv"><span>Next</span><b>${escapeHtml(nextAction)}</b></div>
        <div class="kv"><span>Provider calls</span><b>${escapeHtml(compact(snapshot.external_calls_made, '0'))}</b></div>
      </div>
    </section>
    ${renderWorkbenchOperatorState(snapshot)}
    ${renderWorkbenchExecutionSandbox(snapshot, pageKey)}
    ${renderWorkbenchScenarioMatrix(snapshot, pageKey)}
    ${renderWorkbenchPortfolioImpact(snapshot, pageKey)}
    ${renderWorkbenchPositionSizing(snapshot, pageKey)}
    ${renderWorkbenchCapitalAllocation(snapshot, pageKey)}
    ${renderWorkbenchOrderTicketDraft(snapshot, pageKey)}
    ${renderWorkbenchPaperTradePreview(snapshot, pageKey)}
    ${renderWorkbenchPretradeCompliance(snapshot, pageKey)}
    ${renderWorkbenchTradeReadinessBrief(snapshot, pageKey)}
    ${renderWorkbenchLearningLoop(snapshot, pageKey)}
    ${renderWorkbenchStrategyReview(snapshot, pageKey)}
    ${renderWorkbenchTradeMonitor(snapshot, pageKey)}
    ${renderWorkbenchRiskEnvelope(snapshot, pageKey)}
    ${renderWorkbenchTradeRunbook(snapshot, pageKey)}
    ${renderWorkbenchWorkflowMap(snapshot, pageKey)}
    ${renderWorkbenchPriorityQueue(snapshot, pageKey)}
    ${renderWorkbenchSupervisionGates(snapshot, pageKey)}
    ${renderWorkbenchActionBus(snapshot, pageKey)}
    ${renderWorkbenchModuleData(moduleData)}
    ${renderLiveTradingBoundary()}
    ${dataPanel}
  `;
}

function renderWorkbenchModuleData(moduleData) {
  if (!moduleData || typeof moduleData !== 'object') return '';
  const metrics = moduleData.metrics && typeof moduleData.metrics === 'object'
    ? Object.entries(moduleData.metrics)
    : [];
  const sourceKeys = Array.isArray(moduleData.source_keys) ? moduleData.source_keys : [];
  const rows = workbenchModuleRows(moduleData);
  return `
    <section class="panel wide platform-module-data" data-testid="platform-module-data">
      <div class="module-data-columns">
        <div class="module-data-block" data-testid="platform-module-metrics">
          <h2>Module Metrics</h2>
          <div class="kv-grid">
            ${metrics.length ? metrics.map(([key, value]) => `
              <div class="kv"><span>${escapeHtml(catalogLabel(key))}</span><b>${escapeHtml(text(value))}</b></div>
            `).join('') : '<p>No module metrics reported.</p>'}
          </div>
        </div>
        <div class="module-data-block" data-testid="platform-module-sources">
          <h2>Local Sources</h2>
          <ul class="source-key-list">
            ${sourceKeys.length ? sourceKeys.map((source) => `<li>${escapeHtml(source)}</li>`).join('') : '<li>local dashboard snapshot</li>'}
          </ul>
        </div>
      </div>
      ${renderWorkbenchActivePlan(moduleData.active_plan)}
      ${renderWorkbenchTradeLifecycle(moduleData.trade_lifecycle_rows)}
      ${renderWorkbenchTradeSetups(moduleData.trade_setup_rows)}
      ${renderWorkbenchSizingRows(moduleData.sizing_rows)}
      ${renderWorkbenchPaperIntents(moduleData.paper_intent_rows)}
      ${renderWorkbenchOrderIntents(moduleData.order_intent_rows)}
      ${renderWorkbenchRiskApprovals(moduleData.risk_approval_rows)}
      ${renderWorkbenchAgentCapabilities(moduleData.capability_map)}
      ${renderWorkbenchAgentContributions(moduleData.agent_contributions)}
      ${renderWorkbenchAgentActions(moduleData.agent_actions)}
      ${renderWorkbenchAgentInsights(moduleData.agent_insights)}
      ${renderWorkbenchAgentSecurityChecks(moduleData.security_checks)}
      ${renderWorkbenchRiskBlocks(moduleData.risk_blocks)}
      ${renderWorkbenchReadinessChecks(moduleData.readiness_checks)}
      ${renderWorkbenchAlerts(moduleData.alerts)}
      ${renderWorkbenchMarketTriggers(moduleData.triggers)}
      ${renderWorkbenchOpportunityActions(moduleData.opportunity_actions)}
      ${renderWorkbenchIpoRows(moduleData.ipo_s1_rows)}
      ${renderWorkbenchThemeRows(moduleData.theme_rows)}
      ${renderWorkbenchBudgetRows(moduleData.budget_rows)}
      ${renderWorkbenchValueEconomicsRows(moduleData.value_economics_rows)}
      ${renderWorkbenchOpsProviders(moduleData.provider_rows)}
      ${renderWorkbenchOpsJobs(moduleData.job_rows)}
      ${renderWorkbenchCallPlanRows(moduleData.call_plan_rows)}
      ${renderWorkbenchTelemetryEvents(moduleData.telemetry_events)}
      ${renderWorkbenchTelemetryCoverage(moduleData.coverage_domains)}
      ${renderWorkbenchPaperTrades(moduleData.paper_trades)}
      ${renderWorkbenchOrderTickets(moduleData.order_tickets)}
      ${renderWorkbenchExecutionAudit(moduleData.execution_audit_rows)}
      ${renderWorkbenchTicketAudit(moduleData.ticket_audit_rows)}
      ${renderWorkbenchJournalLedger(moduleData.value_ledger_entries)}
      ${renderWorkbenchJournalOutcomes(moduleData.value_outcomes)}
      ${renderWorkbenchValidationResults(moduleData.validation_results)}
      ${renderWorkbenchUsefulLabels(moduleData.useful_label_rows)}
      ${renderWorkbenchPortfolioPositions(moduleData.positions)}
      ${renderWorkbenchPortfolioBalances(moduleData.balances)}
      ${renderWorkbenchPortfolioExposure(moduleData.exposure_rows)}
      ${renderWorkbenchPortfolioOpenOrders(moduleData.open_order_checks)}
      ${renderWorkbenchFeatureRows(moduleData.feature_rows)}
      ${renderWorkbenchModuleRows(rows)}
    </section>
  `;
}

function renderWorkbenchActivePlan(activePlan) {
  if (!activePlan || typeof activePlan !== 'object' || activePlan.status === 'missing') return '';
  const strategy = activePlan.strategy_proposal || {};
  const risk = activePlan.risk_approval || {};
  const order = activePlan.order_intent || {};
  const controls = activePlan.execution_controls || {};
  const supervision = activePlan.supervision || {};
  const paperDecision = activePlan.paper_decision || {};
  const orderTicket = activePlan.order_ticket || {};
  const canPaperDecision = Boolean(paperDecision.decision_card_id && paperDecision.decision && paperDecision.available_at);
  const canOrderTicket = Boolean(orderTicket.ticker && orderTicket.side && orderTicket.entry_price && orderTicket.invalidation_price);
  return `
    <div class="workbench-active-plan" data-testid="workbench-active-plan">
      <div class="module-title-row">
        <div>
          <h2>Active Trading Plan</h2>
          <p>${escapeHtml(compact(activePlan.next_action, 'Review the active plan before any paper action.'))}</p>
        </div>
        <span class="tool-status">${escapeHtml(catalogLabel(activePlan.status || 'blocked'))}</span>
      </div>
      <div class="active-plan-grid">
        ${activePlanBlock('Strategy', [
          ['Ticker', activePlan.ticker || strategy.ticker],
          ['Autonomy', activePlan.autonomy_level],
          ['Paper decision', activePlan.recommended_paper_decision || paperDecision.decision],
          ['Direction', strategy.direction],
          ['Entry', strategy.entry_price],
          ['Invalidation', strategy.invalidation_price],
          ['Reward/risk', strategy.reward_risk],
        ])}
        ${activePlanBlock('Risk', [
          ['Paper approved', risk.approved_for_paper_trade],
          ['Live approved', risk.approved_for_live_submission],
          ['Paper blocks', Array.isArray(risk.paper_trade_blocks) ? risk.paper_trade_blocks.length : 0],
          ['Live blocks', Array.isArray(risk.live_submission_blocks) ? risk.live_submission_blocks.length : 0],
          ['Max loss', risk.estimated_max_loss],
          ['Manual approval', risk.requires_manual_approval],
        ])}
        ${activePlanBlock('Order Intent', [
          ['Route', order.route],
          ['Side', order.side],
          ['Quantity', order.quantity],
          ['Limit', order.limit_price],
          ['Stop', order.stop_price],
          ['Submission', order.submission_allowed ? 'allowed' : 'disabled'],
        ])}
        ${activePlanBlock('Execution Controls', [
          ['External calls', controls.external_calls_made],
          ['DB writes', controls.db_writes_made],
          ['Broker order', controls.broker_order_submitted],
          ['Order allowed', controls.order_submission_allowed],
          ['No execution', controls.no_execution],
          ['Kill switch', controls.live_trading_kill_switch],
        ])}
      </div>
      <div class="plan-command-list" data-testid="workbench-plan-controls">
        <div>
          <span>Preview</span>
          <code>${escapeHtml(compact(paperDecision.preview_command || supervision.paper_decision_preview_command, 'No preview command.'))}</code>
        </div>
        <div>
          <span>Execute boundary</span>
          <code>${escapeHtml(compact(paperDecision.execute_command || supervision.paper_decision_execute_command, 'No execute command.'))}</code>
        </div>
        <div>
          <span>Ticket preview</span>
          <code>${escapeHtml(compact(orderTicket.preview_command, 'No ticket preview command.'))}</code>
        </div>
        <div>
          <span>Ticket record</span>
          <code>${escapeHtml(compact(orderTicket.record_command, 'No ticket record command.'))}</code>
        </div>
      </div>
      <div class="plan-action-row" data-testid="workbench-paper-actions">
        <button
          type="button"
          data-testid="workbench-paper-preview"
          data-paper-mode="preview"
          ${canPaperDecision ? '' : 'disabled'}
        >Preview Paper</button>
        <button
          type="button"
          data-testid="workbench-paper-record"
          data-paper-mode="execute"
          ${canPaperDecision ? '' : 'disabled'}
        >Record Paper Decision</button>
        <button
          type="button"
          data-testid="workbench-ticket-preview"
          data-ticket-mode="preview"
          ${canOrderTicket ? '' : 'disabled'}
        >Preview Ticket</button>
        <button
          type="button"
          data-testid="workbench-ticket-record"
          data-ticket-mode="record"
          ${canOrderTicket ? '' : 'disabled'}
        >Save Ticket</button>
        <span>${escapeHtml(compact(paperDecision.decision ? `decision=${paperDecision.decision}` : '', 'No active paper decision.'))}</span>
        <span>${escapeHtml(compact(orderTicket.ticker ? `ticket=${orderTicket.ticker} ${orderTicket.side || ''}` : '', 'No active order ticket.'))}</span>
      </div>
    </div>
  `;
}

function activePlanBlock(title, rows) {
  return `
    <div class="active-plan-block">
      <h3>${escapeHtml(title)}</h3>
      <div class="kv-grid">
        ${rows.map(([key, value]) => `
          <div class="kv"><span>${escapeHtml(key)}</span><b>${escapeHtml(text(value))}</b></div>
        `).join('')}
      </div>
    </div>
  `;
}

function renderWorkbenchTradeSetups(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap trade-setup-preview" data-testid="workbench-trade-setups">
      <table aria-label="Trade planner setup proposal">
        <thead><tr><th>Ticker</th><th>Setup</th><th>State</th><th>Entry Zone</th><th>Entry</th><th>Stop</th><th>Reward/Risk</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-trade-setup-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Setup">${escapeHtml(catalogLabel(row.setup_type || '-'))}</td>
              <td data-label="State">${escapeHtml(catalogLabel(row.action_state || '-'))}</td>
              <td data-label="Entry Zone">${escapeHtml(compact(row.entry_zone, '-'))}</td>
              <td data-label="Entry">${escapeHtml(compact(row.entry_price, '-'))}</td>
              <td data-label="Stop">${escapeHtml(compact(row.invalidation_price, '-'))}</td>
              <td data-label="Reward/Risk">${escapeHtml(compact(row.reward_risk, '-'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchSizingRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap trade-sizing-preview" data-testid="workbench-trade-sizing">
      <table aria-label="Trade planner sizing proposal">
        <thead><tr><th>Ticker</th><th>Side</th><th>Qty</th><th>Notional</th><th>Max Loss</th><th>Risk %</th><th>Paper</th><th>Broker Order</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-trade-sizing-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Side">${escapeHtml(compact(row.side, '-'))}</td>
              <td data-label="Qty">${escapeHtml(compact(row.quantity, '0'))}</td>
              <td data-label="Notional">${escapeHtml(compact(row.estimated_notional, '0'))}</td>
              <td data-label="Max Loss">${escapeHtml(compact(row.estimated_max_loss, '-'))}</td>
              <td data-label="Risk %">${escapeHtml(compact(row.risk_per_trade_pct, '-'))}</td>
              <td data-label="Paper">${escapeHtml(row.paper_approved ? 'approved' : 'blocked')}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPaperIntents(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap paper-intent-preview" data-testid="workbench-paper-intents">
      <table aria-label="Trade planner paper intent">
        <thead><tr><th>Decision Card</th><th>Decision</th><th>Entry</th><th>Blocks</th><th>Provider Calls</th><th>DB Writes</th><th>No Execution</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-paper-intent-row">
              <td data-label="Decision Card">${escapeHtml(compact(row.decision_card_id, '-'))}</td>
              <td data-label="Decision">${escapeHtml(catalogLabel(row.decision || '-'))}</td>
              <td data-label="Entry">${escapeHtml(compact(row.entry_price, '-'))}</td>
              <td data-label="Blocks">${escapeHtml(compact(row.hard_block_count, '0'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
              <td data-label="No Execution">${escapeHtml(row.no_execution ? 'yes' : 'no')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchOrderIntents(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap order-intent-preview" data-testid="workbench-order-intents">
      <table aria-label="Trade planner order intent">
        <thead><tr><th>Ticker</th><th>Route</th><th>Side</th><th>Qty</th><th>Limit</th><th>Stop</th><th>Submission</th><th>DB Writes</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-order-intent-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Route">${escapeHtml(catalogLabel(row.route || '-'))}</td>
              <td data-label="Side">${escapeHtml(compact(row.side, '-'))}</td>
              <td data-label="Qty">${escapeHtml(compact(row.quantity, '0'))}</td>
              <td data-label="Limit">${escapeHtml(compact(row.limit_price, '-'))}</td>
              <td data-label="Stop">${escapeHtml(compact(row.stop_price, '-'))}</td>
              <td data-label="Submission">${escapeHtml(row.submission_allowed ? 'allowed' : 'disabled')}</td>
              <td data-label="DB Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchRiskApprovals(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap risk-approval-preview" data-testid="workbench-risk-approvals">
      <table aria-label="Risk desk approval gates">
        <thead><tr><th>Gate</th><th>Status</th><th>Approved</th><th>Blocks</th><th>Max Loss</th><th>Manual</th><th>Command</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-risk-approval-row">
              <td data-label="Gate">${escapeHtml(catalogLabel(row.gate || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Approved">${escapeHtml(row.approved ? 'yes' : 'no')}</td>
              <td data-label="Blocks">${escapeHtml(compact(row.block_count, '0'))}</td>
              <td data-label="Max Loss">${escapeHtml(compact(row.estimated_max_loss, '-'))}</td>
              <td data-label="Manual">${escapeHtml(row.requires_manual_approval ? 'required' : 'not required')}</td>
              <td data-label="Command">${renderWorkbenchRiskActionControls(row)}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchRiskActionControls(row) {
  const previewCommand = compact(row?.paper_preview_command, '');
  const reviewCommand = compact(row?.risk_review_command, '');
  const boundaryCommand = compact(row?.live_boundary_command, '');
  const buttons = [];
  if (previewCommand && !previewCommand.toLowerCase().split(/\s+/).includes('execute')) {
    buttons.push(`
      <button
        class="review-action-button"
        type="button"
        data-testid="workbench-risk-paper-preview"
        data-risk-command="${escapeHtml(previewCommand)}"
        title="${escapeHtml(previewCommand)}"
      >Preview Paper</button>
    `);
  }
  if (reviewCommand) {
    buttons.push(reviewPageButton(reviewCommand, 'Risk', 'workbench-risk-review-page'));
  }
  const boundary = boundaryCommand
    ? `<code title="${escapeHtml(boundaryCommand)}">${escapeHtml(boundaryCommand)}</code>`
    : '';
  return buttons.length || boundary
    ? `<div class="risk-action-row">${buttons.join('')}${boundary}</div>`
    : `<code>${escapeHtml(compact(row?.primary_command, 'risk-desk'))}</code>`;
}

function renderWorkbenchAgentCapabilities(capabilities) {
  if (!Array.isArray(capabilities) || !capabilities.length) return '';
  return `
    <div class="table-wrap agent-capability-preview" data-testid="workbench-agent-capabilities">
      <table aria-label="Agent capability map">
        <thead><tr><th>Level</th><th>Capability</th><th>Status</th><th>Boundary</th><th>Provider Calls</th><th>DB Writes</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${capabilities.slice(0, 8).map((capability) => `
            <tr data-testid="workbench-agent-capability-row">
              <td data-label="Level">${escapeHtml(compact(capability.level, '-'))}</td>
              <td data-label="Capability">${escapeHtml(catalogLabel(capability.name || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(capability.status || 'preview_only'))}</td>
              <td data-label="Boundary">${escapeHtml(catalogLabel(capability.boundary || 'preview_only'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(capability.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(capability.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(capability.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(capability.description || capability.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchAgentContributions(contributions) {
  if (!Array.isArray(contributions) || !contributions.length) return '';
  return `
    <div class="table-wrap agent-contribution-preview" data-testid="workbench-agent-contributions">
      <table aria-label="Agent contribution brief">
        <thead><tr><th>Agent</th><th>Role</th><th>Confidence</th><th>Summary</th><th>Provider Calls</th><th>Broker Order</th></tr></thead>
        <tbody>
          ${contributions.slice(0, 8).map((row) => `
            <tr data-testid="workbench-agent-contribution-row">
              <td data-label="Agent">${escapeHtml(compact(row.agent, '-'))}</td>
              <td data-label="Role">${escapeHtml(compact(row.role, '-'))}</td>
              <td data-label="Confidence">${escapeHtml(catalogLabel(row.confidence || '-'))}</td>
              <td data-label="Summary">${escapeHtml(compact(row.summary, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchAgentActions(actions) {
  if (!Array.isArray(actions) || !actions.length) return '';
  return `
    <div class="table-wrap agent-action-preview" data-testid="workbench-agent-actions">
      <table aria-label="Agent proposed human actions">
        <thead><tr><th>Rank</th><th>Status</th><th>Action</th><th>Command</th><th>Provider Calls</th><th>DB Writes</th><th>Broker Order</th></tr></thead>
        <tbody>
          ${actions.slice(0, 8).map((row) => `
            <tr data-testid="workbench-agent-action-row">
              <td data-label="Rank">${escapeHtml(compact(row.rank, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || 'manual_review'))}</td>
              <td data-label="Action">${escapeHtml(compact(row.action || row.next_action, '-'))}</td>
              <td data-label="Command">${renderWorkbenchAgentActionControls(row)}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchAgentActionControls(row) {
  const command = compact(row?.agent_preview_command, '');
  if (!command || command.toLowerCase().split(/\s+/).includes('execute')) {
    return `<code>${escapeHtml(compact(row?.agent_execute_boundary_command, 'agent execute'))}</code>`;
  }
  return `
    <div class="agent-action-command-row">
      <button
        class="agent-action-command-button"
        type="button"
        data-testid="workbench-agent-preview-action"
        data-agent-command="${escapeHtml(command)}"
        title="${escapeHtml(command)}"
      >Preview</button>
      <code>${escapeHtml(compact(row?.agent_execute_boundary_command, 'agent execute'))}</code>
    </div>
  `;
}

function renderWorkbenchAgentInsights(insights) {
  if (!Array.isArray(insights) || !insights.length) return '';
  return `
    <div class="table-wrap agent-insight-preview" data-testid="workbench-agent-insights">
      <table aria-label="Agent insight brief">
        <thead><tr><th>Rank</th><th>Status</th><th>Insight</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${insights.slice(0, 8).map((row) => `
            <tr data-testid="workbench-agent-insight-row">
              <td data-label="Rank">${escapeHtml(compact(row.rank, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || 'decision_support'))}</td>
              <td data-label="Insight">${escapeHtml(compact(row.insight, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchAgentSecurityChecks(checks) {
  if (!Array.isArray(checks) || !checks.length) return '';
  return `
    <div class="table-wrap agent-security-preview" data-testid="workbench-agent-security-checks">
      <table aria-label="Agent security checks">
        <thead><tr><th>Check</th><th>Status</th><th>Detail</th><th>Provider Calls</th><th>DB Writes</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${checks.slice(0, 8).map((row) => `
            <tr data-testid="workbench-agent-security-check-row">
              <td data-label="Check">${escapeHtml(compact(row.name, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || 'unknown'))}</td>
              <td data-label="Detail">${escapeHtml(compact(row.detail, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchRiskBlocks(blocks) {
  if (!Array.isArray(blocks) || !blocks.length) return '';
  return `
    <div class="table-wrap risk-block-preview" data-testid="workbench-risk-blocks">
      <table aria-label="Risk desk active plan blocks">
        <thead><tr><th>Scope</th><th>Code</th><th>Status</th><th>Boundary</th><th>Provider Calls</th><th>DB Writes</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${blocks.slice(0, 10).map((block) => `
            <tr data-testid="workbench-risk-block-row">
              <td data-label="Scope">${escapeHtml(catalogLabel(block.scope || block.source || '-'))}</td>
              <td data-label="Code">${escapeHtml(compact(block.code || block.finding, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(block.status || 'blocked'))}</td>
              <td data-label="Boundary">${escapeHtml(catalogLabel(block.boundary || 'manual_review_required'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(block.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(block.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(block.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(block.next_action || block.finding, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchReadinessChecks(checks) {
  if (!Array.isArray(checks) || !checks.length) return '';
  return `
    <div class="table-wrap risk-readiness-preview" data-testid="workbench-readiness-checks">
      <table aria-label="Risk readiness checks">
        <thead><tr><th>Source</th><th>Area</th><th>Status</th><th>Finding</th><th>Evidence</th><th>Provider Calls</th><th>DB Writes</th><th>Next</th></tr></thead>
        <tbody>
          ${checks.slice(0, 12).map((check) => `
            <tr data-testid="workbench-readiness-check-row">
              <td data-label="Source">${escapeHtml(catalogLabel(check.source || '-'))}</td>
              <td data-label="Area">${escapeHtml(compact(check.area || check.code, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(check.status || 'unknown'))}</td>
              <td data-label="Finding">${escapeHtml(compact(check.finding, '-'))}</td>
              <td data-label="Evidence">${escapeHtml(compact(check.evidence, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(check.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(check.db_writes_made, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(check.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchAlerts(alerts) {
  if (!Array.isArray(alerts) || !alerts.length) return '';
  return `
    <div class="table-wrap alert-preview" data-testid="workbench-alerts">
      <table aria-label="Workbench alerts">
        <thead><tr><th>Ticker</th><th>Route</th><th>Status</th><th>Priority</th><th>Trigger</th><th>Feedback</th><th>Provider Calls</th><th>Next</th></tr></thead>
        <tbody>
          ${alerts.slice(0, 8).map((alert) => `
            <tr data-testid="workbench-alert-row">
              <td data-label="Ticker">${escapeHtml(compact(alert.ticker, '-'))}</td>
              <td data-label="Route">${escapeHtml(catalogLabel(alert.route || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(alert.status || '-'))}</td>
              <td data-label="Priority">${escapeHtml(catalogLabel(alert.priority || '-'))}</td>
              <td data-label="Trigger">${escapeHtml(compact(alert.trigger_kind || alert.score_trigger, '-'))}</td>
              <td data-label="Feedback">${escapeHtml(catalogLabel(alert.feedback_label || 'unlabeled'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(alert.external_calls_made, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(alert.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchMarketTriggers(triggers) {
  if (!Array.isArray(triggers) || !triggers.length) return '';
  return `
    <div class="table-wrap market-trigger-preview" data-testid="workbench-market-triggers">
      <table aria-label="Saved market triggers">
        <thead><tr><th>Ticker</th><th>Trigger</th><th>Operator</th><th>Threshold</th><th>Latest</th><th>Status</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${triggers.slice(0, 8).map((trigger) => `
            <tr data-testid="workbench-market-trigger-row">
              <td data-label="Ticker">${escapeHtml(compact(trigger.ticker, '-'))}</td>
              <td data-label="Trigger">${escapeHtml(catalogLabel(trigger.trigger_type || '-'))}</td>
              <td data-label="Operator">${escapeHtml(compact(trigger.operator, '-'))}</td>
              <td data-label="Threshold">${escapeHtml(compact(trigger.threshold, '-'))}</td>
              <td data-label="Latest">${escapeHtml(compact(trigger.latest_value, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(trigger.status || '-'))}</td>
              <td data-label="Broker Order">${escapeHtml(trigger.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(trigger.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchOpportunityActions(actions) {
  if (!Array.isArray(actions) || !actions.length) return '';
  return `
    <div class="table-wrap opportunity-action-preview" data-testid="workbench-opportunity-actions">
      <table aria-label="Saved opportunity actions">
        <thead><tr><th>Ticker</th><th>Action</th><th>Status</th><th>Notes</th><th>Provider Calls</th><th>DB Writes</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${actions.slice(0, 8).map((action) => `
            <tr data-testid="workbench-opportunity-action-row">
              <td data-label="Ticker">${escapeHtml(compact(action.ticker, '-'))}</td>
              <td data-label="Action">${escapeHtml(catalogLabel(action.action || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(action.status || '-'))}</td>
              <td data-label="Notes">${escapeHtml(compact(action.notes, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(action.external_calls_made, '0'))}</td>
              <td data-label="DB Writes">${escapeHtml(compact(action.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(action.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(action.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchIpoRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap ipo-s1-preview" data-testid="workbench-ipo-s1">
      <table aria-label="IPO/S-1 filings">
        <thead><tr><th>Ticker</th><th>Form</th><th>Exchange</th><th>Price Range</th><th>Gross Proceeds</th><th>Risk Flags</th><th>Provider Calls</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-ipo-s1-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Form">${escapeHtml(compact(row.form_type, '-'))}</td>
              <td data-label="Exchange">${escapeHtml(compact(row.exchange, '-'))}</td>
              <td data-label="Price Range">${escapeHtml(compact(row.price_range_low, '-'))} - ${escapeHtml(compact(row.price_range_high, '-'))}</td>
              <td data-label="Gross Proceeds">${escapeHtml(compact(row.estimated_gross_proceeds, '-'))}</td>
              <td data-label="Risk Flags">${escapeHtml(compact(row.risk_flags, 'none'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchThemeRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap theme-preview" data-testid="workbench-themes">
      <table aria-label="Workbench theme clusters">
        <thead><tr><th>Theme</th><th>Candidates</th><th>Avg Score</th><th>Top Tickers</th><th>States</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-theme-row">
              <td data-label="Theme">${escapeHtml(catalogLabel(row.theme || '-'))}</td>
              <td data-label="Candidates">${escapeHtml(compact(row.candidate_count, '0'))}</td>
              <td data-label="Avg Score">${escapeHtml(compact(row.avg_score, '-'))}</td>
              <td data-label="Top Tickers">${escapeHtml(compact(row.top_tickers, 'none'))}</td>
              <td data-label="States">${escapeHtml(compactMapping(row.states, 'none'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchBudgetRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap budget-ledger-preview" data-testid="workbench-budget-ledger">
      <table aria-label="Workbench budget ledger">
        <thead><tr><th>Ticker</th><th>Task</th><th>Status</th><th>Model</th><th>Input</th><th>Output</th><th>Actual Cost</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-budget-ledger-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Task">${escapeHtml(catalogLabel(row.task || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || row.skip_reason || '-'))}</td>
              <td data-label="Model">${escapeHtml(compact(row.model || row.provider, '-'))}</td>
              <td data-label="Input">${escapeHtml(compact(row.input_tokens, '0'))}</td>
              <td data-label="Output">${escapeHtml(compact(row.output_tokens, '0'))}</td>
              <td data-label="Actual Cost">${escapeHtml(compact(row.actual_cost_usd, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchValueEconomicsRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap value-economics-preview" data-testid="workbench-value-economics">
      <table aria-label="Workbench value economics">
        <thead><tr><th>Ticker</th><th>Label</th><th>Artifact</th><th>Value</th><th>Weighted</th><th>Cost</th><th>Net</th><th>Calls</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-value-economics-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Label">${escapeHtml(catalogLabel(row.label || '-'))}</td>
              <td data-label="Artifact">${escapeHtml(catalogLabel(row.artifact_type || '-'))}</td>
              <td data-label="Value">${escapeHtml(compact(row.estimated_value_usd, '0'))}</td>
              <td data-label="Weighted">${escapeHtml(compact(row.confidence_weighted_value_usd, '0'))}</td>
              <td data-label="Cost">${escapeHtml(compact(row.cost_to_produce_usd, '0'))}</td>
              <td data-label="Net">${escapeHtml(compact(row.net_confidence_weighted_value_usd, '0'))}</td>
              <td data-label="Calls">${escapeHtml(`${compact(row.provider_call_count, '0')} / ${compact(row.llm_call_count, '0')}`)}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchOpsProviders(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap ops-provider-preview" data-testid="workbench-ops-providers">
      <table aria-label="Ops provider health">
        <thead><tr><th>Provider</th><th>Status</th><th>Checked</th><th>Source</th><th>Reason</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-ops-provider-row">
              <td data-label="Provider">${escapeHtml(catalogLabel(row.provider || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Checked">${escapeHtml(compact(row.checked_at, '-'))}</td>
              <td data-label="Source">${escapeHtml(compact(row.source, '-'))}</td>
              <td data-label="Reason">${escapeHtml(compact(row.reason, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchOpsJobs(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap ops-job-preview" data-testid="workbench-ops-jobs">
      <table aria-label="Ops recent jobs">
        <thead><tr><th>Job</th><th>Status</th><th>Started</th><th>Finished</th><th>Requested</th><th>Raw</th><th>Normalized</th><th>Error</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-ops-job-row">
              <td data-label="Job">${escapeHtml(catalogLabel(row.job_type || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Started">${escapeHtml(compact(row.started_at, '-'))}</td>
              <td data-label="Finished">${escapeHtml(compact(row.finished_at, '-'))}</td>
              <td data-label="Requested">${escapeHtml(compact(row.requested_count, '0'))}</td>
              <td data-label="Raw">${escapeHtml(compact(row.raw_count, '0'))}</td>
              <td data-label="Normalized">${escapeHtml(compact(row.normalized_count, '0'))}</td>
              <td data-label="Error">${escapeHtml(compact(row.error_summary, 'clear'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchCallPlanRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap call-plan-preview" data-testid="workbench-call-plan">
      <table aria-label="Ops call plan">
        <thead><tr><th>Layer</th><th>Status</th><th>Max Calls</th><th>Approval</th><th>Guardrail</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-call-plan-row">
              <td data-label="Layer">${escapeHtml(compact(row.layer, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Max Calls">${escapeHtml(compact(row.external_call_count_max, '0'))}</td>
              <td data-label="Approval">${escapeHtml(row.approval_required ? 'required' : 'not required')}</td>
              <td data-label="Guardrail">${escapeHtml(compact(row.guardrail, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchTelemetryEvents(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap telemetry-event-preview" data-testid="workbench-telemetry-events">
      <table aria-label="Telemetry audit events">
        <thead><tr><th>Occurred</th><th>Event</th><th>Status</th><th>Reason</th><th>Artifact</th><th>Summary</th><th>Provider Calls</th><th>Broker Order</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-telemetry-event-row">
              <td data-label="Occurred">${escapeHtml(compact(row.occurred_at, '-'))}</td>
              <td data-label="Event">${escapeHtml(catalogLabel(row.event || '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Reason">${escapeHtml(compact(row.reason, '-'))}</td>
              <td data-label="Artifact">${escapeHtml(compact(row.artifact, '-'))}</td>
              <td data-label="Summary">${escapeHtml(compact(row.summary, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchTelemetryCoverage(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap telemetry-coverage-preview" data-testid="workbench-telemetry-coverage">
      <table aria-label="Telemetry coverage domains">
        <thead><tr><th>Domain</th><th>Status</th><th>Required</th><th>Events</th><th>Missing</th><th>Last Seen</th><th>Provider Calls</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-telemetry-coverage-row">
              <td data-label="Domain">${escapeHtml(compact(row.domain, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || '-'))}</td>
              <td data-label="Required">${escapeHtml(row.required ? 'required' : 'optional')}</td>
              <td data-label="Events">${escapeHtml(compact(row.event_count, '0'))}</td>
              <td data-label="Missing">${escapeHtml(compact(row.missing_events, 'none'))}</td>
              <td data-label="Last Seen">${escapeHtml(compact(row.last_seen_at, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function workbenchModuleRows(moduleData) {
  if (Array.isArray(moduleData?.rows)) return moduleData.rows;
  if (moduleData?.focus && typeof moduleData.focus === 'object') return [moduleData.focus];
  return [];
}

function renderWorkbenchModuleRows(rows) {
  if (!rows.length) return '';
  return `
    <div class="table-wrap module-row-preview">
      <table aria-label="Workbench module preview rows">
        <thead><tr><th>Ticker</th><th>State</th><th>Signal</th><th>Decision Card</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 5).map((row) => `
            <tr data-testid="platform-module-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker || row.symbol, '-'))}</td>
              <td data-label="State">${escapeHtml(workbenchRowState(row))}</td>
              <td data-label="Signal">${escapeHtml(compact(row.subject || row.title || row.summary, '-'))}</td>
              <td data-label="Decision Card">${escapeHtml(compact(row.decision_card_id || row.card, '-'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action || row.command, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPaperTrades(trades) {
  if (!Array.isArray(trades) || !trades.length) return '';
  return `
    <div class="table-wrap paper-trade-preview" data-testid="workbench-paper-trades">
      <table aria-label="Paper trade ledger">
        <thead><tr><th>Ticker</th><th>Decision</th><th>State</th><th>Shares</th><th>Entry</th><th>Max Loss</th><th>Boundary</th></tr></thead>
        <tbody>
          ${trades.slice(0, 8).map((trade) => `
            <tr data-testid="workbench-paper-trade-row">
              <td data-label="Ticker">${escapeHtml(compact(trade.ticker, '-'))}</td>
              <td data-label="Decision">${escapeHtml(catalogLabel(trade.decision || '-'))}</td>
              <td data-label="State">${escapeHtml(catalogLabel(trade.state || '-'))}</td>
              <td data-label="Shares">${escapeHtml(compact(trade.shares, '0'))}</td>
              <td data-label="Entry">${escapeHtml(compact(trade.entry_price, '-'))}</td>
              <td data-label="Max Loss">${escapeHtml(compact(trade.max_loss, '-'))}</td>
              <td data-label="Boundary">${escapeHtml(trade.no_execution ? 'no execution' : 'local review')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchTradeLifecycle(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap trade-lifecycle-preview" data-testid="workbench-trade-lifecycle">
      <table aria-label="Trade lifecycle">
        <thead><tr><th>Ticker</th><th>Stage</th><th>Decision Card</th><th>Paper Trade</th><th>Audit</th><th>Validation</th><th>Journal</th><th>Outcome</th><th>Commands</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-trade-lifecycle-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Stage">${escapeHtml(catalogLabel(row.current_stage || '-'))}</td>
              <td data-label="Decision Card">${escapeHtml(compact(row.decision_card_id, '-'))}</td>
              <td data-label="Paper Trade">${escapeHtml(compact(row.paper_trade_id, '-'))}</td>
              <td data-label="Audit">${escapeHtml(compact(row.audit_event_id, '-'))}</td>
              <td data-label="Validation">${escapeHtml(compact(row.validation_result_id, '-'))}</td>
              <td data-label="Journal">${escapeHtml(compact(row.ledger_entry_id, '-'))}</td>
              <td data-label="Outcome">${escapeHtml(compact(row.outcome_status || row.outcome_id, '-'))}</td>
              <td data-label="Commands">${renderWorkbenchLifecycleActions(row)}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchLifecycleActions(row) {
  const buttons = [
    lifecycleActionButton(row, 'ledger_show_command', 'Ledger', 'workbench-lifecycle-show-ledger'),
    lifecycleActionButton(row, 'outcome_show_command', 'Outcome', 'workbench-lifecycle-show-outcome'),
    lifecycleActionButton(row, 'outcome_preview_command', 'Preview', 'workbench-lifecycle-preview-outcome'),
    lifecycleActionButton(row, 'outcome_update_command', 'Update', 'workbench-lifecycle-update-outcome', true),
  ].filter(Boolean);
  return buttons.length
    ? `<div class="lifecycle-action-row">${buttons.join('')}</div>`
    : `<code>${escapeHtml(compact(row.primary_command, 'ledger coverage'))}</code>`;
}

function lifecycleActionButton(row, commandKey, label, testId, writes = false) {
  const command = compact(row?.[commandKey], '');
  if (!command) return '';
  return `
    <button
      class="lifecycle-action-button"
      type="button"
      data-testid="${escapeHtml(testId)}"
      data-lifecycle-command="${escapeHtml(command)}"
      title="${escapeHtml(command)}"
      ${writes ? 'data-lifecycle-write="true"' : ''}
    >${escapeHtml(label)}</button>
  `;
}

function renderWorkbenchOrderTickets(tickets) {
  if (!Array.isArray(tickets) || !tickets.length) return '';
  return `
    <div class="table-wrap order-ticket-preview" data-testid="workbench-order-tickets">
      <table aria-label="Blocked workbench order tickets">
        <thead><tr><th>Ticker</th><th>Side</th><th>Qty</th><th>Limit</th><th>Invalidation</th><th>Status</th><th>Hard Blocks</th></tr></thead>
        <tbody>
          ${tickets.slice(0, 8).map((ticket) => `
            <tr data-testid="workbench-order-ticket-row">
              <td data-label="Ticker">${escapeHtml(compact(ticket.ticker, '-'))}</td>
              <td data-label="Side">${escapeHtml(compact(ticket.side, '-'))}</td>
              <td data-label="Qty">${escapeHtml(compact(ticket.quantity, '0'))}</td>
              <td data-label="Limit">${escapeHtml(compact(ticket.limit_price, '-'))}</td>
              <td data-label="Invalidation">${escapeHtml(compact(ticket.invalidation_price, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(ticket.status || 'blocked'))}</td>
              <td data-label="Hard Blocks">${escapeHtml(compact(ticket.hard_blocks, 'broker_submission_disabled'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchExecutionAudit(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap execution-audit-preview" data-testid="workbench-execution-audit">
      <table aria-label="Paper execution audit">
        <thead><tr><th>Event</th><th>Ticker</th><th>Decision</th><th>State</th><th>Paper Trade</th><th>Writes</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-execution-audit-row">
              <td data-label="Event">${escapeHtml(catalogLabel(row.event_type || '-'))}</td>
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Decision">${escapeHtml(catalogLabel(row.decision || '-'))}</td>
              <td data-label="State">${escapeHtml(catalogLabel(row.record_state || row.status || '-'))}</td>
              <td data-label="Paper Trade">${escapeHtml(compact(row.paper_trade_id, '-'))}</td>
              <td data-label="Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchTicketAudit(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap ticket-audit-preview" data-testid="workbench-ticket-audit">
      <table aria-label="Broker ticket audit">
        <thead><tr><th>Event</th><th>Ticker</th><th>Side</th><th>Status</th><th>Ticket</th><th>Hard Blocks</th><th>Writes</th><th>Broker Order</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-ticket-audit-row">
              <td data-label="Event">${escapeHtml(catalogLabel(row.event_type || '-'))}</td>
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Side">${escapeHtml(compact(row.decision, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.record_state || row.status || 'blocked'))}</td>
              <td data-label="Ticket">${escapeHtml(compact(row.order_ticket_id, '-'))}</td>
              <td data-label="Hard Blocks">${escapeHtml(compact(row.hard_blocks, 'broker_submission_disabled'))}</td>
              <td data-label="Writes">${escapeHtml(compact(row.db_writes_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchJournalLedger(entries) {
  if (!Array.isArray(entries) || !entries.length) return '';
  return `
    <div class="table-wrap journal-ledger-preview" data-testid="workbench-journal-ledger">
      <table aria-label="Journal value ledger entries">
        <thead><tr><th>Ticker</th><th>Label</th><th>Action</th><th>Decision</th><th>Value</th><th>Confidence</th><th>Outcome</th></tr></thead>
        <tbody>
          ${entries.slice(0, 8).map((entry) => `
            <tr data-testid="workbench-journal-ledger-row">
              <td data-label="Ticker">${escapeHtml(compact(entry.ticker, '-'))}</td>
              <td data-label="Label">${escapeHtml(catalogLabel(entry.label || '-'))}</td>
              <td data-label="Action">${escapeHtml(catalogLabel(entry.supported_action || '-'))}</td>
              <td data-label="Decision">${escapeHtml(catalogLabel(entry.user_decision || '-'))}</td>
              <td data-label="Value">${escapeHtml(compact(entry.estimated_value_usd, '-'))}</td>
              <td data-label="Confidence">${escapeHtml(compact(entry.confidence, '-'))}</td>
              <td data-label="Outcome">${escapeHtml(catalogLabel(entry.outcome_status || 'pending'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchJournalOutcomes(outcomes) {
  if (!Array.isArray(outcomes) || !outcomes.length) return '';
  return `
    <div class="table-wrap journal-outcome-preview" data-testid="workbench-journal-outcomes">
      <table aria-label="Journal value outcomes">
        <thead><tr><th>Ticker</th><th>Status</th><th>Days</th><th>Entry</th><th>20D</th><th>SPY Rel 20D</th><th>Invalidation</th></tr></thead>
        <tbody>
          ${outcomes.slice(0, 8).map((outcome) => `
            <tr data-testid="workbench-journal-outcome-row">
              <td data-label="Ticker">${escapeHtml(compact(outcome.ticker, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(outcome.status || '-'))}</td>
              <td data-label="Days">${escapeHtml(compact(outcome.trading_days_observed, '0'))}</td>
              <td data-label="Entry">${escapeHtml(compact(outcome.entry_price, '-'))}</td>
              <td data-label="20D">${escapeHtml(compact(outcome.return_20d, '-'))}</td>
              <td data-label="SPY Rel 20D">${escapeHtml(compact(outcome.spy_relative_return_20d, '-'))}</td>
              <td data-label="Invalidation">${escapeHtml(outcome.invalidation_touched ? 'touched' : 'clear')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchValidationResults(results) {
  if (!Array.isArray(results) || !results.length) return '';
  return `
    <div class="table-wrap validation-result-preview" data-testid="workbench-validation-results">
      <table aria-label="Backtest validation results">
        <thead><tr><th>Ticker</th><th>State</th><th>Score</th><th>Baseline</th><th>Labels</th><th>Leakage</th><th>Decision Card</th></tr></thead>
        <tbody>
          ${results.slice(0, 8).map((result) => `
            <tr data-testid="workbench-validation-result-row">
              <td data-label="Ticker">${escapeHtml(compact(result.ticker, '-'))}</td>
              <td data-label="State">${escapeHtml(catalogLabel(result.state || '-'))}</td>
              <td data-label="Score">${escapeHtml(compact(result.final_score, '-'))}</td>
              <td data-label="Baseline">${escapeHtml(compact(result.baseline, 'candidate'))}</td>
              <td data-label="Labels">${escapeHtml(compact(result.positive_labels, 'none'))}</td>
              <td data-label="Leakage">${escapeHtml(compact(result.leakage_flags, 'clear'))}</td>
              <td data-label="Decision Card">${escapeHtml(compact(result.decision_card_id, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchUsefulLabels(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap useful-label-preview" data-testid="workbench-useful-labels">
      <table aria-label="Validation useful labels">
        <thead><tr><th>Ticker</th><th>Label</th><th>Artifact</th><th>Notes</th><th>Created</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-useful-label-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Label">${escapeHtml(catalogLabel(row.label || '-'))}</td>
              <td data-label="Artifact">${escapeHtml(catalogLabel(row.artifact_type || '-'))} ${escapeHtml(compact(row.artifact_id, ''))}</td>
              <td data-label="Notes">${escapeHtml(compact(row.notes, '-'))}</td>
              <td data-label="Created">${escapeHtml(compact(row.created_at, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPortfolioPositions(positions) {
  if (!Array.isArray(positions) || !positions.length) return '';
  return `
    <div class="table-wrap portfolio-position-preview" data-testid="workbench-portfolio-positions">
      <table aria-label="Portfolio positions">
        <thead><tr><th>Ticker</th><th>Qty</th><th>Average</th><th>Market Value</th><th>Unrealized P/L</th><th>Exposure</th><th>Theme</th><th>Actions</th></tr></thead>
        <tbody>
          ${positions.slice(0, 8).map((position) => `
            <tr data-testid="workbench-portfolio-position-row">
              <td data-label="Ticker">${escapeHtml(compact(position.ticker, '-'))}</td>
              <td data-label="Qty">${escapeHtml(compact(position.quantity, '0'))}</td>
              <td data-label="Average">${escapeHtml(compact(position.average_price, '-'))}</td>
              <td data-label="Market Value">${escapeHtml(compact(position.market_value, '-'))}</td>
              <td data-label="Unrealized P/L">${escapeHtml(compact(position.unrealized_pnl, '-'))}</td>
              <td data-label="Exposure">${escapeHtml(compact(position.exposure_pct, '-'))}</td>
              <td data-label="Theme">${escapeHtml(catalogLabel(position.theme || 'broker_synced'))}</td>
              <td data-label="Actions">${renderWorkbenchPortfolioActionControls(position)}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPortfolioBalances(balances) {
  if (!Array.isArray(balances) || !balances.length) return '';
  return `
    <div class="table-wrap portfolio-balance-preview" data-testid="workbench-portfolio-balances">
      <table aria-label="Portfolio account balances">
        <thead><tr><th>Account</th><th>As Of</th><th>Cash</th><th>Buying Power</th><th>Equity</th><th>Liquidation</th><th>Actions</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${balances.slice(0, 8).map((balance) => `
            <tr data-testid="workbench-portfolio-balance-row">
              <td data-label="Account">${escapeHtml(compact(balance.display_name || balance.account_id, '-'))}</td>
              <td data-label="As Of">${escapeHtml(compact(balance.as_of, '-'))}</td>
              <td data-label="Cash">${escapeHtml(compact(balance.cash, '0'))}</td>
              <td data-label="Buying Power">${escapeHtml(compact(balance.buying_power, '0'))}</td>
              <td data-label="Equity">${escapeHtml(compact(balance.equity, '0'))}</td>
              <td data-label="Liquidation">${escapeHtml(compact(balance.liquidation_value, '0'))}</td>
              <td data-label="Actions">${renderWorkbenchPortfolioActionControls(balance)}</td>
              <td data-label="Broker Order">${escapeHtml(balance.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(balance.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPortfolioExposure(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap portfolio-exposure-preview" data-testid="workbench-portfolio-exposure">
      <table aria-label="Portfolio exposure summary">
        <thead><tr><th>Scope</th><th>Metric</th><th>Value</th><th>Status</th><th>Snapshot</th><th>Stale</th><th>Actions</th><th>Provider Calls</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 10).map((row) => `
            <tr data-testid="workbench-portfolio-exposure-row">
              <td data-label="Scope">${escapeHtml(catalogLabel(row.scope || '-'))}</td>
              <td data-label="Metric">${escapeHtml(compact(row.metric, '-'))}</td>
              <td data-label="Value">${escapeHtml(compact(row.value, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || 'unknown'))}</td>
              <td data-label="Snapshot">${escapeHtml(compact(row.snapshot_as_of, '-'))}</td>
              <td data-label="Stale">${escapeHtml(row.broker_data_stale ? 'yes' : 'no')}</td>
              <td data-label="Actions">${renderWorkbenchPortfolioActionControls(row)}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPortfolioOpenOrders(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap portfolio-open-order-preview" data-testid="workbench-portfolio-open-orders">
      <table aria-label="Portfolio open order boundary">
        <thead><tr><th>Ticker</th><th>Side</th><th>Type</th><th>Qty</th><th>Limit</th><th>Status</th><th>Actions</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 8).map((row) => `
            <tr data-testid="workbench-portfolio-open-order-row">
              <td data-label="Ticker">${escapeHtml(compact(row.ticker, '-'))}</td>
              <td data-label="Side">${escapeHtml(compact(row.side, '-'))}</td>
              <td data-label="Type">${escapeHtml(compact(row.order_type, '-'))}</td>
              <td data-label="Qty">${escapeHtml(compact(row.quantity, '0'))}</td>
              <td data-label="Limit">${escapeHtml(compact(row.limit_price, '-'))}</td>
              <td data-label="Status">${escapeHtml(catalogLabel(row.status || 'none'))}</td>
              <td data-label="Actions">${renderWorkbenchPortfolioActionControls(row)}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderWorkbenchPortfolioActionControls(row) {
  const buttons = [
    reviewPageButton(row?.portfolio_review_command, 'Portfolio', 'workbench-portfolio-review-page'),
    reviewPageButton(row?.risk_review_command, 'Risk', 'workbench-portfolio-risk-page'),
    reviewPageButton(row?.broker_review_command, 'Broker', 'workbench-portfolio-broker-page'),
  ].filter(Boolean);
  return buttons.length
    ? `<div class="portfolio-action-row">${buttons.join('')}</div>`
    : `<code>${escapeHtml(compact(row?.primary_command, 'portfolio'))}</code>`;
}

function reviewPageButton(page, label, testId) {
  const resolved = compact(page, '');
  if (!resolved) return '';
  return `
    <button
      class="review-action-button"
      type="button"
      data-testid="${escapeHtml(testId)}"
      data-review-page="${escapeHtml(resolved)}"
      title="${escapeHtml(resolved)}"
    >${escapeHtml(label)}</button>
  `;
}

function renderWorkbenchFeatureRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return `
    <div class="table-wrap feature-inventory-preview" data-testid="workbench-feature-inventory">
      <table aria-label="Workbench feature inventory">
        <thead><tr><th>Area</th><th>Feature</th><th>Page</th><th>Use</th><th>Provider Calls</th><th>Broker Order</th><th>Next</th></tr></thead>
        <tbody>
          ${rows.slice(0, 20).map((row) => `
            <tr data-testid="workbench-feature-row">
              <td data-label="Area">${escapeHtml(compact(row.area, '-'))}</td>
              <td data-label="Feature">${escapeHtml(compact(row.feature, '-'))}</td>
              <td data-label="Page">${escapeHtml(compact(row.page, '-'))}</td>
              <td data-label="Use">${escapeHtml(compact(row.use, '-'))}</td>
              <td data-label="Provider Calls">${escapeHtml(compact(row.external_calls_made, '0'))}</td>
              <td data-label="Broker Order">${escapeHtml(row.broker_order_submitted ? 'submitted' : 'not submitted')}</td>
              <td data-label="Next">${escapeHtml(compact(row.next_action, '-'))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function workbenchRowState(row) {
  return catalogLabel(row.usefulness_status || row.state || row.status || '-');
}

function renderLiveTradingBoundary() {
  const boundary = platformBoundary();
  const liveEnabled = Boolean(boundary.live_trading_enabled);
  return `
    <section class="panel wide platform-boundary" data-testid="live-trading-disabled" data-live-trading-enabled="${liveEnabled}">
      <h2>Execution Boundary</h2>
      <div class="boundary-grid">
        <div class="kv"><span>Live trading</span><b>${liveEnabled ? 'enabled' : 'disabled'}</b></div>
        <div class="kv"><span>Broker orders</span><b>${escapeHtml(boundary.broker_order_submission || 'disabled')}</b></div>
        <div class="kv"><span>Autonomous execution</span><b>${escapeHtml(boundary.autonomous_execution || 'disabled')}</b></div>
        <div class="kv"><span>Paper trading</span><b>${escapeHtml(boundary.paper_trading || 'preview_only')}</b></div>
      </div>
    </section>
  `;
}

function bindPlatformToolCards() {
  document.querySelectorAll('[data-testid="platform-tool-card"]').forEach((card) => {
    card.addEventListener('click', () => setPage(card.dataset.page));
    card.addEventListener('keydown', (event) => {
      if (!['Enter', ' '].includes(event.key)) return;
      event.preventDefault();
      setPage(card.dataset.page);
    });
  });
}

function bindWorkbenchPaperControls() {
  document.querySelectorAll('[data-paper-mode]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchPaperDecision(button.dataset.paperMode));
  });
}

async function runWorkbenchPaperDecision(mode) {
  const resolvedMode = mode === 'execute' ? 'execute' : 'preview';
  const command = `paper-decision ${resolvedMode}`;
  await dispatchWorkbenchAction({
    action_kind: 'backend_command',
    command,
    local_write_allowed: resolvedMode === 'execute',
    label: resolvedMode === 'execute' ? 'Record paper decision' : 'Preview paper decision',
  });
}

function bindWorkbenchTicketControls() {
  document.querySelectorAll('[data-ticket-mode]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchOrderTicket(button.dataset.ticketMode));
  });
}

async function runWorkbenchOrderTicket(mode) {
  const resolvedMode = mode === 'record' ? 'record' : 'preview';
  const command = `order-ticket ${resolvedMode}`;
  await dispatchWorkbenchAction({
    action_kind: 'backend_command',
    command,
    local_write_allowed: resolvedMode === 'record',
    label: resolvedMode === 'record' ? 'Save blocked ticket' : 'Preview order ticket',
  });
}

function bindWorkbenchLifecycleControls() {
  document.querySelectorAll('[data-lifecycle-command]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchLifecycleCommand(button.dataset.lifecycleCommand));
  });
}

async function runWorkbenchLifecycleCommand(command) {
  const resolved = compact(command, '');
  if (!resolved) return;
  await dispatchWorkbenchAction({
    action_kind: 'backend_command',
    command: resolved,
    local_write_allowed: resolved.startsWith('outcome update'),
    label: 'Lifecycle action',
  });
}

function bindWorkbenchAgentControls() {
  document.querySelectorAll('[data-agent-command]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchAgentCommand(button.dataset.agentCommand));
  });
}

async function runWorkbenchAgentCommand(command) {
  const resolved = compact(command, '');
  if (!resolved) return;
  await dispatchWorkbenchAction({
    action_kind: 'backend_command',
    command: resolved,
    label: 'Agent preview',
  });
}

function bindWorkbenchReviewControls() {
  document.querySelectorAll('[data-review-page]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchReviewPage(button.dataset.reviewPage));
  });
  document.querySelectorAll('[data-risk-command]').forEach((button) => {
    button.addEventListener('click', () => runWorkbenchRiskCommand(button.dataset.riskCommand));
  });
}

async function runWorkbenchReviewPage(page) {
  const resolved = compact(page, '');
  if (!resolved) return;
  await dispatchWorkbenchAction({
    action_kind: 'page',
    target_page: resolved,
    command: resolved,
    label: pageLabelFor(resolved),
  });
}

async function runWorkbenchRiskCommand(command) {
  const resolved = compact(command, '');
  if (!resolved || resolved.toLowerCase().split(/\s+/).includes('execute')) return;
  await dispatchWorkbenchAction({
    action_kind: 'backend_command',
    command: resolved,
    label: 'Risk preview',
  });
}

function bindWorkbenchActionBusControls() {
  document.querySelectorAll('[data-workbench-action-command]').forEach((button) => {
    button.addEventListener('click', () => dispatchWorkbenchAction({
      action_kind: button.dataset.workbenchActionKind || 'backend_command',
      command: button.dataset.workbenchActionCommand,
      local_write_allowed: button.dataset.workbenchLocalWrite === 'true',
      label: button.dataset.workbenchActionLabel,
    }));
  });
  document.querySelectorAll('[data-workbench-action-page]').forEach((button) => {
    button.addEventListener('click', () => dispatchWorkbenchAction({
      action_kind: button.dataset.workbenchActionKind || 'page',
      target_page: button.dataset.workbenchActionPage,
      command: button.dataset.workbenchActionPage,
      label: button.dataset.workbenchActionLabel,
    }));
  });
}

function localWriteArmKey(action) {
  const command = compact(action?.command, '');
  const target = compact(action?.target_page || action?.page, '');
  const label = compact(action?.label, 'local-write');
  return `${command || target || label}`.toLowerCase();
}

function clearPendingLocalWrite() {
  state.pendingLocalWrite = null;
}

async function dispatchWorkbenchAction(action) {
  const kind = compact(action?.action_kind || action?.kind, 'backend_command');
  const command = compact(action?.command, '');
  const targetPage = compact(action?.target_page || action?.page, '');
  if (kind === 'boundary') {
    clearPendingLocalWrite();
    const label = compact(action?.label, 'Boundary');
    setCommandStatus(`${label} remains disabled in the workbench.`);
    return;
  }
  if (kind === 'page') {
    const page = targetPage || command;
    if (!page) return;
    clearPendingLocalWrite();
    state.lastCommand = command || page;
    setCommandStatus(`Opened ${pageLabelFor(page)}.`);
    await setPage(page);
    return;
  }
  if (!command) return;
  if (commandHasExecuteToken(command) && !action?.local_write_allowed) {
    clearPendingLocalWrite();
    setCommandStatus('Execute commands stay outside dashboard browsing unless explicitly marked as local writes.');
    return;
  }
  if (action?.local_write_allowed) {
    const armKey = localWriteArmKey(action);
    if (state.pendingLocalWrite?.key !== armKey) {
      state.pendingLocalWrite = {
        key: armKey,
        command,
        label: compact(action?.label, command),
      };
      setCommandStatus(
        `Armed local write: ${state.pendingLocalWrite.label}. Click again to confirm; no provider or broker calls made.`,
      );
      return;
    }
    clearPendingLocalWrite();
  } else {
    clearPendingLocalWrite();
  }
  state.lastCommand = command;
  await handleBackendDashboardCommand(command);
}

function commandHasExecuteToken(command) {
  return compact(command, '').toLowerCase().split(/\s+/).includes('execute');
}

function renderTutorial() {
  return `
    <section class="panel wide" data-testid="tutorial-panel">
      <h2>First 90 Seconds</h2>
      <div class="stack">
        <p>1 Command Center: route platform work safely.</p>
        <p>2 Evidence Gaps: fix missing market or decision evidence.</p>
        <p>3 Safe Run: review provider calls before execution.</p>
        <p>4 Candidate Review: inspect a single evidence case.</p>
        <p>The desktop app reads local snapshots until an explicit command is chosen outside the browsing flow.</p>
      </div>
    </section>
  `;
}

function renderStructuredPage(title, paths) {
  const snapshot = state.snapshot || {};
  const panels = paths.map((path) => objectPanel(path.join(' / '), at(snapshot, path, null))).join('');
  return `<section class="panel wide"><h2>${escapeHtml(title)}</h2><div class="stack">${panels || '<p>No data for this view.</p>'}</div></section>`;
}

function objectPanel(title, value) {
  if (!value || typeof value !== 'object') {
    return `<div class="kv"><span>${escapeHtml(title)}</span><b>No data</b></div>`;
  }
  const entries = Object.entries(value).slice(0, 12);
  return `
    <article class="panel-sub" data-testid="object-${escapeHtml(title.replaceAll(' ', '-'))}">
      <h3>${escapeHtml(title)}</h3>
      <div class="kv-grid">
        ${entries.map(([key, item]) => `<div class="kv"><span>${escapeHtml(key)}</span><b>${escapeHtml(text(item))}</b></div>`).join('')}
      </div>
    </article>
  `;
}

function renderQueuePage(title, rows, columns = candidateColumns) {
  return queuePanel(title, rows, columns);
}

function queuePanel(title, rows, columns = candidateColumns) {
  if (!rows.length) {
    return `
      <section class="panel wide empty" data-testid="attention-queue">
        <h2>${escapeHtml(title)}</h2>
        <p>No rows for this view.</p>
        <p>${escapeHtml(compact(state.snapshot?.next_action || state.snapshot?.canonical_next_action, 'Refresh the dashboard snapshot.'))}</p>
      </section>
    `;
  }
  return `
    <section class="panel wide" data-testid="attention-queue">
      <h2>${escapeHtml(title)}</h2>
      <div class="table-wrap">
        <table aria-label="${escapeHtml(title)}">
          <thead><tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join('')}</tr></thead>
          <tbody>${rows.slice(0, 50).map((row, index) => rowHtml(row, index, columns)).join('')}</tbody>
        </table>
      </div>
    </section>
  `;
}

function rowHtml(row, index, columns) {
  const key = rowOpenKey(row, index);
  const ticker = compact(row.ticker || row.symbol || row.security || key, `row-${index + 1}`);
  const label = `Open dashboard row ${ticker}`;
  return `
    <tr
      data-testid="queue-row"
      data-ticker="${escapeHtml(ticker)}"
      data-open-key="${escapeHtml(key)}"
      data-row-index="${index + 1}"
      tabindex="0"
      role="button"
      aria-label="${escapeHtml(label)}"
      title="${escapeHtml(label)}"
    >
      ${columns.map((column) => `<td class="${column.className || ''}">${escapeHtml(column.value(row, index))}</td>`).join('')}
    </tr>
  `;
}

function rowOpenKey(row, index) {
  if (state.page === 'alerts') {
    return compact(row.id || row.alert_id || row.key || row.ticker || row.symbol, String(index + 1));
  }
  return compact(row.ticker || row.symbol || row.security || row.id || row.key, String(index + 1));
}

function bindQueueRows() {
  document.querySelectorAll('[data-testid="queue-row"]').forEach((row) => {
    row.addEventListener('click', () => openRowFromElement(row));
    row.addEventListener('keydown', (event) => {
      if (!['Enter', ' '].includes(event.key)) return;
      event.preventDefault();
      openRowFromElement(row);
    });
  });
}

async function openRowFromElement(row) {
  const target = row.dataset.openKey || row.dataset.rowIndex || '';
  await openDashboardTarget(target);
}

const candidateColumns = [
  { label: 'Ticker', className: 'ticker', value: (row) => compact(row.ticker || row.symbol || row.security) },
  { label: 'State', className: 'state', value: (row) => compact(row.state || row.status || row.decision_status || row.usefulness, 'review') },
  { label: 'Signal', value: (row) => compact(row.subject || row.title || row.setup || row.top_catalyst || row.why_now, 'Open the row for evidence.') },
  { label: 'Next', value: (row) => compact(row.next_action || row.action || row.command || row.next_command, 'inspect') },
];

const alertColumns = [
  { label: 'Ticker', className: 'ticker', value: (row) => compact(row.ticker || row.symbol) },
  { label: 'Status', className: 'state', value: (row) => compact(row.status || row.route) },
  { label: 'Subject', value: (row) => compact(row.subject || row.title || row.message) },
  { label: 'Next', value: (row) => compact(row.next_action || row.command, 'review') },
];

const ipoColumns = [
  { label: 'Ticker', className: 'ticker', value: (row) => compact(row.ticker || row.symbol) },
  { label: 'Filing', className: 'state', value: (row) => compact(row.form || row.filing_type || row.status) },
  { label: 'Summary', value: (row) => compact(row.summary || row.subject || row.risk_summary) },
  { label: 'Next', value: (row) => compact(row.next_action || row.command, 'inspect filing') },
];

function themeRows(snapshot) {
  const rows = at(snapshot, ['themes', 'rows'], []);
  return Array.isArray(rows) ? rows : [];
}

const themeColumns = [
  { label: 'Theme', className: 'ticker', value: (row) => compact(row.theme || row.name || row.cluster) },
  { label: 'Count', className: 'state', value: (row) => compact(row.candidate_count || row.count || row.rows) },
  { label: 'Avg Score', value: (row) => compact(row.avg_score || row.average_score || row.score) },
  { label: 'Tickers', value: (row) => compact(row.top_tickers || row.tickers || row.sample_tickers) },
  { label: 'States', value: (row) => compact(row.states || row.state_counts || row.statuses) },
];

function renderCosts(snapshot) {
  return `
    <section class="panel wide" data-testid="costs-panel">
      <h2>Costs</h2>
      <div class="stack">
        ${objectPanel('costs', at(snapshot, ['costs'], null))}
        ${objectPanel('value ledger', at(snapshot, ['value_ledger'], null))}
        ${objectPanel('value outcomes', at(snapshot, ['value_outcomes'], null))}
        ${objectPanel('value report', at(snapshot, ['value_report'], null))}
      </div>
    </section>
  `;
}

function renderCandidateDetail(snapshot, ticker) {
  const row = candidateDetailRow(snapshot, ticker);
  if (!row) return missingDetailPanel('Candidate', ticker, 'No local candidate row matched this ticker.');
  return `
    <section class="panel wide" data-testid="candidate-detail">
      <h2>Candidate ${escapeHtml(ticker.toUpperCase())}</h2>
      <div class="stack">
        ${detailGrid(candidateDetailPairs(row, ticker))}
        ${objectPanel('candidate row', row)}
      </div>
    </section>
  `;
}

function renderAlertDetail(snapshot, alertId) {
  const row = alertDetailRow(snapshot, alertId);
  if (!row) return missingDetailPanel('Alert', alertId, 'No local alert row matched this identifier.');
  return `
    <section class="panel wide" data-testid="alert-detail">
      <h2>Alert ${escapeHtml(alertId)}</h2>
      <div class="stack">
        ${detailGrid(alertDetailPairs(row, alertId))}
        ${objectPanel('alert row', row)}
      </div>
    </section>
  `;
}

function missingDetailPanel(kind, identifier, message) {
  return `
    <section class="panel wide empty" data-testid="detail-missing">
      <h2>${escapeHtml(kind)} ${escapeHtml(identifier)}</h2>
      <p>${escapeHtml(message)}</p>
      <p>No provider calls were made.</p>
    </section>
  `;
}

function detailGrid(pairs) {
  return `
    <article class="panel-sub" data-testid="detail-summary">
      <div class="kv-grid">
        ${pairs.map(([key, value]) => `<div class="kv"><span>${escapeHtml(key)}</span><b>${escapeHtml(compact(value))}</b></div>`).join('')}
      </div>
    </article>
  `;
}

function candidateDetailRow(snapshot, ticker) {
  const query = String(ticker || '').toUpperCase();
  return candidateSearchRows(snapshot).find((row) => String(row.ticker || row.symbol || row.security || '').toUpperCase() === query) || null;
}

function candidateSearchRows(snapshot) {
  const rows = [
    ...rowsFromSnapshot(snapshot),
    ...arrayAt(snapshot, ['candidates', 'rows']),
    ...arrayAt(snapshot, ['candidates', 'items']),
  ];
  const seen = new Set();
  return rows.filter((row) => {
    const key = compact(row.ticker || row.symbol || row.security || row.id || JSON.stringify(row));
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function alertDetailRow(snapshot, alertId) {
  const query = String(alertId || '').toUpperCase();
  return alertRows(snapshot).find((row) => String(row.id || row.alert_id || row.key || row.ticker || row.symbol || '').toUpperCase() === query) || null;
}

function candidateDetailPairs(row, ticker) {
  const nextCommand = compact(row.priced_in_next_command || row.next_command || row.command, '');
  const sourceGaps = compact(row.source_gaps || row.missing_sources || row.optional_context_gaps, 'none');
  return [
    ['Can I act now?', row.decision_ready ? 'Research review only; not trade approval.' : 'No - research only until readiness says this is decision-ready.'],
    ['What happened?', row.why_now || row.subject || row.title || row.setup || 'No local explanation recorded.'],
    ['What is missing?', sourceGaps],
    ['Next safe action', row.next_action || row.next_step || row.decision_next_step || 'Review evidence before action.'],
    ['Next command', nextCommand || 'n/a'],
    ['Where to run', nextCommand ? 'normal PowerShell prompt, not the dashboard command box.' : 'n/a'],
  ];
}

function alertDetailPairs(row, alertId) {
  return [
    ['Alert id', row.id || row.alert_id || alertId],
    ['Ticker', row.ticker || row.symbol || 'n/a'],
    ['Status', row.status || row.route || 'review'],
    ['Subject', row.subject || row.title || row.message || 'No local alert text recorded.'],
    ['Next safe action', row.next_action || row.command || 'Review alert evidence before action.'],
    ['Boundary', 'Feedback writes require an explicit command; browsing made no provider calls.'],
  ];
}

function renderHelp() {
  const shortcuts = state.config.automation.keyboard_shortcuts;
  return `
    <section class="panel wide" data-testid="help-panel">
      <h2>Keyboard And Automation</h2>
      <div class="kv-grid">
        ${shortcuts.map((item, index) => `<div class="kv"><span>Shortcut ${index + 1}</span><b>${escapeHtml(item)}</b></div>`).join('')}
      </div>
      <div class="table-wrap command-reference" data-testid="command-reference">
        <table aria-label="Dashboard command reference">
          <thead><tr><th>Command</th><th>Meaning</th><th>Safety</th><th>Route</th></tr></thead>
          <tbody>${commandReference().map(([command, meaning, safety, route]) => `
            <tr
              data-testid="command-reference-row"
              data-command="${escapeHtml(command)}"
              data-safety="${escapeHtml(safety)}"
              data-route="${escapeHtml(route)}"
              tabindex="0"
              aria-label="${escapeHtml(`${command}: ${meaning} Safety ${catalogLabel(safety)}. Route ${catalogLabel(route)}.`)}"
            >
              <td class="ticker">${escapeHtml(command)}</td>
              <td>${escapeHtml(meaning)}</td>
              <td class="state">${escapeHtml(catalogLabel(safety))}</td>
              <td class="command-route">${escapeHtml(catalogLabel(route))}</td>
            </tr>
          `).join('')}</tbody>
        </table>
      </div>
    </section>
  `;
}

function rawJsonPanel(snapshot) {
  return `
    <section class="panel wide" data-testid="snapshot-json">
      <details>
        <summary>Raw JSON snapshot</summary>
        <pre
          id="snapshot-json-output"
          class="raw-json"
          data-testid="snapshot-json-output"
          tabindex="0"
          role="textbox"
          aria-label="Raw JSON dashboard snapshot"
          aria-readonly="true"
        >${escapeHtml(JSON.stringify(snapshot, null, 2))}</pre>
      </details>
    </section>
  `;
}

async function handleCommandSubmit(event) {
  event.preventDefault();
  const input = qs('#command-input');
  const raw = input.value.trim();
  input.value = '';
  const shouldFocusCommand = await applyCommand(raw);
  if (shouldFocusCommand !== false) input.focus();
}

async function handleCommandInputKeydown(event) {
  if (event.key !== 'Enter') return;
  event.preventDefault();
  await handleCommandSubmit(event);
}

async function applyCommand(raw) {
  state.lastCommand = raw || 'refresh';
  if (!raw) {
    setCommandStatus('Refreshed.');
    await refreshSnapshot();
    return;
  }
  const normalized = raw.toLowerCase().replace(/\s+/g, ' ').trim();
  const [command, ...parts] = normalized.split(' ');
  const value = parts.join(' ').trim();

  if (['q', 'quit', 'exit'].includes(command)) {
    setCommandStatus('Closing MarketRadar Trading Workbench.');
    await closeDashboardWindow();
    return false;
  }
  if (['r', 'refresh'].includes(command)) {
    setCommandStatus('Refreshed.');
    await refreshSnapshot();
    return;
  }
  if (['setup', 'first', 'first-step', 'first_step'].includes(command)) {
    setCommandStatus('Opened Evidence Gaps.');
    await setPage('readiness');
    return;
  }
  if (['now', 'what-now', 'whatnow', 'todo', 'do'].includes(command)) {
    setCommandStatus(compact(state.snapshot?.next_action || state.snapshot?.canonical_next_action, 'Opened Inbox.'));
    await setPage('overview');
    return;
  }
  if (['all', 'full', 'full-scan'].includes(command)) {
    qs('#filter-scan-mode').value = 'all';
    qs('#filter-stocks-only').checked = false;
    state.usefulness = null;
    state.scanOffset = 0;
    setCommandStatus('Full scan mode.');
    await setPage('overview');
    return;
  }
  if (['stock', 'stocks', 'stocks-only', 'stocks_only'].includes(command)) {
    qs('#filter-scan-mode').value = 'all';
    qs('#filter-stocks-only').checked = true;
    state.scanOffset = 0;
    setCommandStatus('Stocks-only mode.');
    await setPage('overview');
    return;
  }
  if (['d', 'ready', 'decision', 'decision-ready', 'decision_ready'].includes(command)) {
    qs('#filter-scan-mode').value = 'actionable';
    state.usefulness = 'decision_useful';
    state.scanOffset = 0;
    setCommandStatus('Decision-ready review filter.');
    await setPage('review');
    return;
  }
  if (['m', 'mismatch', 'mismatches', 'actionable'].includes(command)) {
    qs('#filter-scan-mode').value = 'actionable';
    state.usefulness = null;
    state.scanOffset = 0;
    setCommandStatus('Mismatches mode.');
    await setPage('overview');
    return;
  }
  if (command === 'scan') {
    const mode = value || 'all';
    const select = qs('#filter-scan-mode');
    if ([...select.options].some((option) => option.value === mode)) {
      select.value = mode;
      state.scanOffset = 0;
      setCommandStatus(`Scan filter: ${mode}.`);
      await setPage('overview');
    } else {
      setCommandStatus(`Unsupported scan filter: ${mode}.`);
    }
    return;
  }
  if (command === 'export') {
    setCommandStatus(exportCommandMessage(value));
    return;
  }
  if (['next', 'more'].includes(command)) {
    const pagination = paginationStateFromSnapshot();
    const nextOffset = pagination.offset + Math.max(1, pagination.limit);
    if (pagination.total && nextOffset >= pagination.total) {
      setCommandStatus('Already at the end of the current scan filter.');
      await setPage('overview');
      return;
    }
    state.scanOffset = nextOffset;
    setCommandStatus(`Rows starting at ${state.scanOffset + 1}.`);
    await setPage('overview');
    return;
  }
  if (['prev', 'previous', 'back'].includes(command)) {
    const limit = Math.max(1, Number(qs('#filter-limit').value || 50));
    state.scanOffset = Math.max(0, state.scanOffset - limit);
    setCommandStatus(`Rows starting at ${state.scanOffset + 1}.`);
    await setPage('overview');
    return;
  }
  if (command === 'offset') {
    if (!isPositiveIntegerText(value)) {
      setCommandStatus('Usage: offset ROW.');
      return;
    }
    const offset = Number(value);
    state.scanOffset = Math.floor(offset) - 1;
    setCommandStatus(`Rows starting at ${state.scanOffset + 1}.`);
    await setPage('overview');
    return;
  }
  if (command === 'limit') {
    if (!isPositiveIntegerText(value)) {
      setCommandStatus('Usage: limit 1-200.');
      return;
    }
    const limit = Number(value);
    qs('#filter-limit').value = String(Math.min(200, Math.max(1, Math.floor(limit))));
    state.scanOffset = 0;
    setCommandStatus(`Rows per page: ${qs('#filter-limit').value}.`);
    await setPage('overview');
    return;
  }
  if (['ticker', 'tkr'].includes(command)) {
    const ticker = value.toUpperCase();
    qs('#filter-ticker').value = ['', 'ALL', 'NONE'].includes(ticker) ? '' : ticker;
    state.scanOffset = 0;
    setCommandStatus(qs('#filter-ticker').value ? `Ticker filter: ${qs('#filter-ticker').value}.` : 'Ticker filter cleared.');
    await refreshSnapshot();
    return;
  }
  if (['source-gap', 'source_gaps', 'data-gap', 'data_gaps'].includes(command)) {
    const sourceGap = validatedListFilter(value, sourceAliases, allowedSourceGaps, 'source-gap');
    if (sourceGap.error) {
      setCommandStatus(sourceGap.error);
      return;
    }
    state.sourceGap = sourceGap.values;
    state.scanOffset = 0;
    setCommandStatus(state.sourceGap.length ? `Source gaps: ${state.sourceGap.join(', ')}.` : 'Source-gap filter cleared.');
    await setPage('overview');
    return;
  }
  if (['decision-gap', 'decision_gaps', 'gap'].includes(command)) {
    const decisionGap = validatedListFilter(value, decisionGapAliases, allowedDecisionGaps, 'decision-gap');
    if (decisionGap.error) {
      setCommandStatus(decisionGap.error);
      return;
    }
    state.decisionGap = decisionGap.values;
    state.scanOffset = 0;
    setCommandStatus(state.decisionGap.length ? `Decision gaps: ${state.decisionGap.join(', ')}.` : 'Decision-gap filter cleared.');
    await setPage('overview');
    return;
  }
  if (['usefulness', 'useful'].includes(command)) {
    state.usefulness = normalizeOptionalFilterValue(value);
    state.scanOffset = 0;
    setCommandStatus(state.usefulness ? `Usefulness: ${state.usefulness}.` : 'Usefulness filter cleared.');
    await setPage('overview');
    return;
  }
  if (['available-at', 'cutoff'].includes(command)) {
    const availableAt = parseAvailableAtCommand(value);
    if (availableAt.error) {
      setCommandStatus(availableAt.error);
      return;
    }
    state.availableAt = availableAt.value;
    state.scanOffset = 0;
    setCommandStatus(state.availableAt ? `Available-at: ${state.availableAt}.` : 'Available-at filter cleared.');
    await refreshSnapshot();
    return;
  }
  if (command === 'alert-status') {
    state.alertStatus = isOptionalClearValue(value) ? null : value;
    setCommandStatus(state.alertStatus ? `Alert status: ${state.alertStatus}.` : 'Alert-status filter cleared.');
    await setPage('alerts');
    return;
  }
  if (command === 'alert-route') {
    state.alertRoute = isOptionalClearValue(value) ? null : value;
    setCommandStatus(state.alertRoute ? `Alert route: ${state.alertRoute}.` : 'Alert-route filter cleared.');
    await setPage('alerts');
    return;
  }
  if (['clear', 'clear-filters', 'reset'].includes(command)) {
    clearFilters();
    setCommandStatus('Filters cleared.');
    await refreshSnapshot();
    return;
  }
  if (['j', 'json'].includes(command)) {
    qs('details')?.setAttribute('open', 'open');
    qs('#snapshot-json-output')?.focus?.();
    setCommandStatus('Raw JSON opened.');
    return false;
  }
  if (command === 'open') {
    await openDashboardTarget(value);
    return;
  }
  if (['batch', 'batches', 'source-batch', 'source-batches'].includes(command)) {
    setCommandStatus(sourceBatchCommandMessage(value));
    await setPage('ops');
    return;
  }
  if (command === 'run') {
    await handleRunCommand(value);
    return;
  }
  const boundaryMessage = guardedExecutionBoundaryMessage(normalized);
  if (boundaryMessage) {
    setCommandStatus(boundaryMessage);
    const boundaryPage = guardedCommandPage(command);
    if (boundaryPage) await setPage(boundaryPage);
    return;
  }
  if (backendCommandWords.has(command)) {
    await handleBackendDashboardCommand(raw);
    return;
  }
  const guardedMessage = guardedCommandMessage(normalized);
  if (guardedMessage) {
    setCommandStatus(guardedMessage);
    const guardedPage = guardedCommandPage(command);
    if (guardedPage) await setPage(guardedPage);
    return;
  }

  const page = pageFromCommand(raw);
  if (page) {
    setCommandStatus(`Opened ${page}.`);
    await setPage(page);
    return;
  }
  setCommandStatus(`Unknown command: ${raw}. Type help for commands.`);
}

function clearFilters() {
  const preservedLimit = qs('#filter-limit').value || '50';
  qs('#filter-ticker').value = '';
  qs('#filter-scan-mode').value = 'all';
  qs('#filter-stocks-only').checked = false;
  qs('#filter-limit').value = preservedLimit;
  state.availableAt = null;
  state.alertStatus = null;
  state.alertRoute = null;
  state.sourceGap = [];
  state.decisionGap = [];
  state.usefulness = null;
  state.scanOffset = 0;
}

function listFilter(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['', 'all', 'none'].includes(normalized)
    ? []
    : String(value || '').replaceAll(';', ',').split(/[,\s]+/).map((item) => item.trim()).filter(Boolean);
}

function validatedListFilter(value, aliases, allowed, commandLabel) {
  const values = unique(listFilter(value).map((item) => normalizeFilterName(item, aliases)));
  const invalid = values.filter((item) => !allowed.has(item));
  if (invalid.length) {
    return {
      values: [],
      error: `Unsupported ${commandLabel} value: ${invalid.join(', ')}. No calls made; filter unchanged. Use all or one of: ${[...allowed].join(', ')}.`,
    };
  }
  return { values, error: '' };
}

function normalizeFilterName(value, aliases) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
  return aliases.get(normalized) || normalized;
}

function unique(values) {
  return [...new Set(values)];
}

function paginationStateFromSnapshot() {
  const queue = at(state.snapshot || {}, ['priced_in_queue'], {});
  const queueFilters = queue?.filters && typeof queue.filters === 'object' ? queue.filters : {};
  return {
    total: Math.max(0, Number(queue?.total_count || 0)),
    offset: Math.max(0, Number(queue?.offset ?? state.scanOffset ?? 0)),
    limit: Math.max(1, Number(queueFilters?.limit || qs('#filter-limit')?.value || 50)),
  };
}

function isPositiveIntegerText(value) {
  return /^[0-9]+$/.test(String(value || '').trim());
}

function isOptionalClearValue(value, includeAny = false) {
  const clearValues = includeAny ? ['', 'all', 'any', 'none'] : ['', 'all', 'none'];
  return clearValues.includes(String(value || '').trim().toLowerCase());
}

function normalizeOptionalFilterValue(value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
  return isOptionalClearValue(normalized, true) ? null : normalized;
}

function parseAvailableAtCommand(value) {
  const raw = String(value || '').trim();
  const normalized = raw.toLowerCase();
  if (['', 'latest', 'all', 'none'].includes(normalized)) {
    return { value: null, error: '' };
  }
  if (!isIsoDateTimeText(raw)) {
    return { value: null, error: 'Invalid timestamp. No calls made; filter unchanged.' };
  }
  return { value: raw, error: '' };
}

function isIsoDateTimeText(value) {
  const normalized = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+|T[0-9:.+-]+Z)?$/.test(normalized)) {
    return false;
  }
  return !Number.isNaN(Date.parse(normalized.replace(/Z$/, '+00:00')));
}

function parseSourceBatchCommand(value) {
  const parts = String(value || '').split(/\s+/).map((part) => part.trim()).filter(Boolean);
  const executeWords = new Set(['execute', 'exec', 'run']);
  const fullPlanWords = new Set(['all', 'full', 'full-scan', 'fullscan', 'plan']);
  const sourceParts = [];
  let execute = false;
  let allBatches = false;
  let maxBatches = 1;

  parts.forEach((part) => {
    const lowered = part.toLowerCase();
    if (executeWords.has(lowered)) {
      execute = true;
      return;
    }
    if (fullPlanWords.has(lowered)) {
      allBatches = true;
      return;
    }
    if (/^\d+$/.test(lowered)) {
      maxBatches = Math.max(1, Number(lowered));
      return;
    }
    sourceParts.push(part);
  });

  const sourceText = sourceParts.join(' ');
  return {
    source: sourceText ? normalizeSourceName(sourceText) : (allBatches ? 'all' : ''),
    execute,
    allBatches,
    maxBatches,
  };
}

function normalizeSourceName(value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
  return sourceAliases.get(normalized) || normalized;
}

function sourceBatchCommandMessage(value) {
  const parsed = parseSourceBatchCommand(value);
  if (!parsed.source) {
    return 'Usage: batch SOURCE. Try: batch catalyst_events, batch local_text, batch options, batch SOURCE all, or batch SOURCE execute 3. No calls made.';
  }
  if (parsed.source === 'all') {
    if (parsed.execute) {
      return 'batch all is plan-only. Choose one source before running execute, for example: batch catalyst_events execute. No calls made.';
    }
    return allSourceBatchPlanMessage();
  }
  if (parsed.execute) return sourceBatchExecuteBoundary(parsed);
  if (parsed.allBatches) return sourceBatchAllPlanMessage(parsed.source);
  return sourceBatchPlanMessage(parsed.source);
}

function allSourceBatchPlanMessage() {
  const workflow = at(state.snapshot || {}, ['priced_in_source_workflow'], {});
  const command = compact(workflow?.overview_command, 'catalyst-radar priced-in-source-batches --source all');
  const headline = compact(workflow?.headline || workflow?.next_action, 'Review full-scan source coverage.');
  return `All-source plan: ${headline} PowerShell command: ${command}. No provider calls made in the desktop app.`;
}

function sourceBatchPlanMessage(source) {
  const step = sourceWorkflowStep(source);
  const fallbackCommand = `catalyst-radar priced-in-source-batches --source ${source}`;
  if (!step) {
    return `${sourceLabel(source)}: no source workflow row is available in the current snapshot. PowerShell command: ${fallbackCommand}. No provider calls made in the desktop app.`;
  }
  const status = compact(step.status, 'unknown');
  const action = compact(step.action || step.next_action, 'Review the source plan before execution.');
  const command = compact(step.command || step.batch_plan_command, fallbackCommand);
  const gapText = sourceGapText(step);
  return `${sourceLabel(source)}: ${status}; ${gapText}. ${action} PowerShell command: ${command}. No provider calls made in the desktop app.`;
}

function sourceBatchAllPlanMessage(source) {
  return `${sourceLabel(source)} full batch plan stays external. PowerShell command: catalyst-radar priced-in-source-batches --source ${source} --all. No provider calls made in the desktop app.`;
}

function sourceBatchExecuteBoundary(parsed) {
  const command = parsed.maxBatches > 1
    ? `catalyst-radar priced-in-source-batches --source ${parsed.source} --execute-batches ${parsed.maxBatches}`
    : `catalyst-radar priced-in-source-batches --source ${parsed.source} --execute-next`;
  const step = sourceWorkflowStep(parsed.source);
  const review = step ? ` Review first: ${compact(step.action || step.next_action, 'inspect the source plan')}.` : '';
  return `${sourceLabel(parsed.source)} execute stays outside dashboard browsing. Run this in PowerShell only after accepting the provider/write boundary: ${command}.${review} provider_calls=0 in the desktop app.`;
}

function sourceWorkflowStep(source) {
  return arrayAt(state.snapshot || {}, ['priced_in_source_workflow', 'steps']).find((step) => (
    normalizeSourceName(step?.source) === source
  ));
}

function sourceGapText(step) {
  const gap = step?.gap_rows ?? step?.actionable_gap_rows ?? step?.decision_useful_gap_rows;
  if (gap === null || gap === undefined || gap === '') return 'gap rows unknown';
  return `${gap} gap row(s)`;
}

function sourceLabel(source) {
  return String(source || 'source').replaceAll('_', ' ');
}

function pageFromCommand(raw) {
  const normalized = raw.toLowerCase().replace(/[\s-]+/g, '_');
  const direct = keyAliases.get(raw.toLowerCase()) || keyAliases.get(normalized);
  if (direct) return direct;
  const pages = state.config?.pages || [];
  const page = pages.find((item) => (
    item.key === normalized
    || item.label.toLowerCase().replace(/^[0-9?]\s*/, '').replace(/[\s/-]+/g, '_') === normalized
  ));
  return page?.key || null;
}

async function openDashboardTarget(value) {
  const target = resolveOpenTarget(value);
  if (!target) {
    setCommandStatus(openNoMatchMessage(value));
    return;
  }
  state.page = target.page;
  renderNav();
  setCommandStatus(target.message);
  renderSnapshot();
  qs('#dashboard-main').focus();
}

function resolveOpenTarget(value) {
  const token = String(value || '').trim();
  if (!token) return null;
  const snapshot = state.snapshot || {};
  const numericIndex = Number(token);
  const isIndex = Number.isInteger(numericIndex) && numericIndex >= 1;

  if (isIndex && ['overview', 'review', 'candidates'].includes(state.page)) {
    return candidateTargetFromRow(rowsFromSnapshot(snapshot)[numericIndex - 1]);
  }
  if (isIndex && state.page === 'alerts') {
    return alertTargetFromRow(alertRows(snapshot)[numericIndex - 1], numericIndex);
  }
  if (isIndex) return null;

  const query = token.toUpperCase();
  const candidate = candidateSearchRows(snapshot).find((row) => String(row.ticker || row.symbol || row.security || '').toUpperCase() === query);
  if (candidate) return candidateTargetFromRow(candidate);
  const alert = alertRows(snapshot).find((row) => String(row.id || row.alert_id || row.key || row.ticker || row.symbol || '').toUpperCase() === query);
  return alertTargetFromRow(alert, token);
}

function candidateTargetFromRow(row) {
  if (!row) return null;
  const ticker = compact(row.ticker || row.symbol || row.security, '').toUpperCase();
  if (!ticker) return null;
  return {
    page: `candidate:${ticker}`,
    message: `Opened candidate ${ticker}. No calls. Review evidence before action.`,
  };
}

function alertTargetFromRow(row, fallback) {
  if (!row) return null;
  const alertId = compact(row.id || row.alert_id || row.key || row.ticker || row.symbol, fallback || '');
  if (!alertId) return null;
  return {
    page: `alert:${alertId}`,
    message: `Opened alert ${alertId}. No calls. Review alert evidence before feedback.`,
  };
}

function openNoMatchMessage(value) {
  const token = String(value || '').trim();
  if (!token) return 'Open command needs a target. No calls made. Type open TICKER, open ALERT_ID, or use row numbers on Inbox, Candidate Review, or Alerts.';
  if (/^\d+$/.test(token)) {
    return `No row ${token} is openable on ${dynamicPageLabel(state.page)}. No calls made. Use row numbers on Inbox, Candidate Review, or Alerts; from any page type open TICKER or open ALERT_ID.`;
  }
  return `No local candidate or alert matched ${token}. No calls made. Try open TICKER, open ALERT_ID, or refresh if you expected it in the latest scan.`;
}

function exportCommandMessage(value) {
  const mode = value.toLowerCase();
  const scanScope = at(state.snapshot || {}, ['priced_in_answer', 'scan_scope'], {});
  if (['', 'full', 'full-scan', 'scan', 'all'].includes(mode)) {
    const command = compact(
      scanScope?.full_scan_export_command,
      'catalyst-radar priced-in-queue --full-scan --all --json'
    );
    return `Full-scan export command: ${command}`;
  }
  if (['current', 'filter', 'filtered'].includes(mode)) {
    const command = compact(
      scanScope?.current_filter_export_command,
      'catalyst-radar priced-in-queue --all --json'
    );
    return `Current-filter export command: ${command}`;
  }
  return 'Usage: export full or export current. No calls made.';
}

async function handleRunCommand(value) {
  if (String(value || '').trim().toLowerCase() !== 'execute') {
    setCommandStatus('Run is guarded. Review the call plan, then type run execute to start one capped radar cycle.');
    await setPage('run');
    return;
  }
  state.page = 'run';
  renderNav();
  setCommandStatus('Starting guarded radar run through the dashboard backend...');
  try {
    const result = await invoke('execute_dashboard_command', {
      command: 'run execute',
      input: filterInput(),
    });
    const message = radarRunResultMessage(result);
    await refreshSnapshot();
    setCommandStatus(message);
  } catch (error) {
    setCommandStatus(`Radar run failed: ${error?.message || error}`);
    await refreshSnapshot();
  }
}

async function handleBackendDashboardCommand(raw) {
  setCommandStatus('Running dashboard command through backend...');
  try {
    const result = await invoke('execute_dashboard_command', {
      command: raw,
      input: filterInput(),
    });
    applyBackendDashboardResult(result);
    const message = dashboardCommandResultMessage(result);
    await refreshSnapshot();
    setCommandStatus(message);
  } catch (error) {
    setCommandStatus(`Dashboard command failed: ${error?.message || error}`);
    await refreshSnapshot();
  }
}

function applyBackendDashboardResult(result) {
  applyBackendDashboardFilters(result?.filters);
  if (result?.page) state.page = String(result.page);
  renderNav();
}

function applyBackendDashboardFilters(filters) {
  if (!filters || typeof filters !== 'object') return;
  qs('#filter-ticker').value = filters.ticker || '';
  qs('#filter-scan-mode').value = filters.priced_in_status || 'all';
  qs('#filter-stocks-only').checked = Boolean(filters.priced_in_stocks_only);
  qs('#filter-limit').value = String(filters.priced_in_limit || qs('#filter-limit').value || 50);
  state.availableAt = filters.available_at || null;
  state.alertStatus = filters.alert_status || null;
  state.alertRoute = filters.alert_route || null;
  state.sourceGap = Array.isArray(filters.priced_in_source_gap) ? filters.priced_in_source_gap : [];
  state.decisionGap = Array.isArray(filters.priced_in_decision_gap) ? filters.priced_in_decision_gap : [];
  state.usefulness = filters.priced_in_usefulness || null;
  state.scanOffset = Math.max(0, Number(filters.priced_in_offset || 0));
}

function dashboardCommandResultMessage(result) {
  return result?.message || 'Dashboard command completed.';
}

function guardedExecutionBoundaryMessage(normalized) {
  const command = normalized.split(' ')[0];
  const powershellMessage = powershellCommandMessage(normalized);
  if (powershellMessage) return powershellMessage;
  if (executeClassCommands.has(normalized)) {
    return 'Execute commands stay outside dashboard browsing. Copy the displayed command and run it in PowerShell after reviewing call/write boundaries.';
  }
  // Provider previews can use the backend; execute and confirm variants stay external.
  if (providerBackendCommandWords().has(command) && /\b(?:execute|confirm)\b/.test(normalized)) {
    return 'Execute commands stay outside dashboard browsing. Copy the displayed command and run it in PowerShell after reviewing call/write boundaries.';
  }
  return '';
}

function providerBackendCommandWords() {
  return new Set([
    'agent',
    'agent-brief',
    'agents',
    'bar',
    'bars',
    'cik',
    'ciks',
    'market-bars',
    'market_bars',
    'option',
    'options',
    'options-flow',
    'options_flow',
    'sec',
    'sec-cik',
    'sec_cik',
  ]);
}

function radarRunResultMessage(result) {
  if (result?.message) return result.message;
  const detail = result?.detail;
  if (detail) {
    if (typeof detail === 'string') return `Radar run blocked: ${detail}`;
    if (detail.retry_after_seconds !== undefined) {
      return `Radar run rate limited for ${detail.retry_after_seconds} second(s).`;
    }
    return `Radar run blocked: ${compact(detail.reason || detail.status || JSON.stringify(detail), 'Review run response.')}`;
  }
  const daily = result?.daily_result || {};
  const status = compact(daily.status || result?.reason, 'unknown');
  const requiredDone = compact(daily.required_completed_count, '0');
  const requiredTotal = compact(daily.required_step_count, '0');
  const callPlan = at(result?.discovery_snapshot || state.snapshot || {}, ['call_plan'], {});
  const maxExternal = compact(callPlan?.max_external_call_count, '0');
  return `Radar run finished: status=${status}; required=${requiredDone}/${requiredTotal}; call_plan_max_external=${maxExternal}. Refresh to inspect updated readiness.`;
}

function guardedCommandMessage(normalized) {
  const first = normalized.split(' ')[0];
  const powershellMessage = powershellCommandMessage(normalized);
  if (powershellMessage) return powershellMessage;
  if (executeClassCommands.has(normalized) || normalized.includes(' execute')) {
    return 'Execute commands stay outside dashboard browsing. Copy the displayed command and run it in PowerShell after reviewing call/write boundaries.';
  }
  if ([
    'action',
    'agent',
    'bars',
    'batch',
    'batches',
    'cik',
    'eval-triggers',
    'evaluate-triggers',
    'feedback',
    'market-bars',
    'options',
    'run',
    'sec',
    'ticket',
    'trigger',
  ].includes(first)) {
    const nextCommand = compact(state.snapshot?.next_command || state.snapshot?.canonical_next_command, 'catalyst-radar dashboard-snapshot --json --fast');
    return `Review command boundary. Suggested external command: ${nextCommand}`;
  }
  return '';
}

function powershellCommandMessage(normalized) {
  const parts = normalized.split(' ');
  const first = parts[0];
  const shellCommand = first === 'catalyst-radar'
    ? normalized
    : powershellCommandPrefixes.has(first)
      ? `catalyst-radar ${normalized}`
      : '';
  if (!shellCommand) return '';
  return [
    'PowerShell command, not a dashboard command.',
    `Run this in a normal PowerShell prompt: ${shellCommand}.`,
    powershellCommandBoundary(shellCommand),
  ].join(' ');
}

function powershellCommandBoundary(shellCommand) {
  const normalized = shellCommand.toLowerCase();
  if (normalized.includes(' market-bars residual-review ')) {
    return 'Read-only market-bar review; no provider, OpenAI, broker, order, or DB write calls.';
  }
  if (normalized.includes(' build-packets ') || normalized.includes(' build-decision-cards ')) {
    return 'Candidate evidence writes stay outside dashboard browsing and require explicit PowerShell review.';
  }
  return 'Run it only after accepting the command call/write boundary.';
}

function guardedCommandPage(command) {
  if (command === 'agent') return 'agent';
  if (['bars', 'market-bars', 'options', 'run'].includes(command)) return 'run';
  if (['batch', 'batches', 'cik', 'sec'].includes(command)) return 'ops';
  if (['action', 'eval-triggers', 'evaluate-triggers', 'ticket', 'trigger'].includes(command)) return 'broker';
  if (command === 'feedback') return 'alerts';
  return null;
}

function setCommandStatus(message) {
  setText('#command-status', `command=${message}`);
  updateFilterState();
  updateCommandState();
  updateAutomationJson();
}

async function closeDashboardWindow() {
  try {
    await invoke('close_dashboard_window');
  } catch (error) {
    setCommandStatus('Close the MarketRadar window to exit.');
    showError(error);
  }
}

async function copyNextCommand() {
  const command = qs('#next-command').textContent.trim();
  try {
    await navigator.clipboard.writeText(command);
    setText('#copy-command', 'Copied');
    setTimeout(() => { setText('#copy-command', 'Copy Command'); }, 1200);
  } catch {
    showError(new Error('Clipboard write failed. Select the command text manually.'));
  }
}

function showError(error) {
  const message = qs('#error-message');
  if (message) message.textContent = error?.message || String(error);
  const dialog = qs('#error-dialog');
  if (dialog && typeof dialog.showModal === 'function') dialog.showModal();
}

function handleKeyboard(event) {
  if (event.key === 'Escape') {
    event.preventDefault();
    qs('#command-input').focus();
    setCommandStatus('Command box focused.');
    return;
  }
  if (isFormControlTarget(event.target)) {
    return;
  }
  if (event.ctrlKey && event.key.toLowerCase() === 'a') {
    event.preventDefault();
    setPage('agent');
    return;
  }
  if (event.ctrlKey && event.key.toLowerCase() === 'n') {
    event.preventDefault();
    stepPage(1);
    return;
  }
  if (event.ctrlKey && event.key.toLowerCase() === 'p') {
    event.preventDefault();
    stepPage(-1);
    return;
  }
  if (event.key === 'Tab' && !shouldPreserveNativeTab(event)) {
    event.preventDefault();
    stepPage(event.shiftKey ? -1 : 1);
    return;
  }
  if (event.key === 'F5') {
    event.preventDefault();
    refreshSnapshot();
    return;
  }
  if (event.key === 'ArrowRight' || event.key === 'ArrowDown' || event.key === 'PageDown') {
    event.preventDefault();
    stepPage(1);
    return;
  }
  if (event.key === 'ArrowLeft' || event.key === 'ArrowUp' || event.key === 'PageUp') {
    event.preventDefault();
    stepPage(-1);
    return;
  }
  if (event.key === 'Home') {
    event.preventDefault();
    setPage('tutorial');
    return;
  }
  if (event.key === 'End') {
    event.preventDefault();
    setPage('help');
    return;
  }
  const commandModifier = event.ctrlKey || event.altKey || event.metaKey;
  const plainKey = event.key.length === 1 ? event.key.toLowerCase() : '';
  if (!commandModifier) {
    if (plainKey === 'q') {
      event.preventDefault();
      setCommandStatus('Closing MarketRadar Trading Workbench.');
      closeDashboardWindow();
      return;
    }
    if (plainKey === 'r') {
      event.preventDefault();
      setCommandStatus('Refreshed.');
      refreshSnapshot();
      return;
    }
    if (plainKey === 'j') {
      event.preventDefault();
      stepPage(1);
      return;
    }
    if (plainKey === 'k') {
      event.preventDefault();
      stepPage(-1);
      return;
    }
    const alias = keyAliases.get(plainKey);
    if (alias) {
      event.preventDefault();
      setPage(alias);
    }
  }
}

function stepPage(delta) {
  const pages = state.config.pages.map((page) => page.key);
  const current = Math.max(0, pages.indexOf(navigationPageKey(state.page)));
  const next = (current + delta + pages.length) % pages.length;
  setPage(pages[next]);
}

boot().catch((error) => {
  console.error(error?.stack || error?.message || error);
  setStatus('error');
  showError(error);
});


