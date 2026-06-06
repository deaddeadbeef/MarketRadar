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
        "function tradingWorkbenchModule",
        "trading_workbench",
        'data-testid="trading-workbench-overview"',
        'data-testid="platform-tool-card"',
        'data-tool="${escapeHtml(module.key)}"',
        "function renderPlatformModulePage",
        "function renderWorkbenchModuleData",
        "function workbenchModuleRows",
        "function renderWorkbenchModuleRows",
        'data-testid="platform-module-page"',
        'data-testid="platform-module-data"',
        'data-testid="platform-module-metrics"',
        'data-testid="platform-module-sources"',
        'data-testid="platform-module-row"',
        'data-testid="live-trading-disabled"',
        "function bindPlatformToolCards",
        "platform: {",
        "primary_tool: platformManifest().primary_tool",
        "live_trading_enabled: Boolean(platformBoundary().live_trading_enabled)",
    ):
        assert text in source

    for alias in (
        "['portfolio', 'portfolio']",
        "['market-radar', 'market-radar']",
        "['trade-planner', 'trade-planner']",
        "['risk-desk', 'risk-desk']",
        "['paper-trading', 'paper-trading']",
        "['broker-desk', 'broker']",
        "['backtest', 'backtest']",
        "['journal', 'journal']",
        "['agent-cockpit', 'agent']",
    ):
        assert alias in source

    for tool in (
        "platform-tool-market-radar",
        "platform-tool-trade-planner",
        "platform-tool-risk-desk",
        "platform-tool-paper-trading",
        "platform-tool-agent-cockpit",
    ):
        assert tool in rust_source

    assert ".platform-tools" in styles
    assert ".platform-tool-card" in styles
    assert ".platform-boundary" in styles
    assert ".module-page" in styles


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
    assert "renderQueuePage('Themes'" in source
    assert "renderStructuredPage('Validation'" in source
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
