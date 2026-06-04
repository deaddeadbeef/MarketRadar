from __future__ import annotations

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
    assert "setCommandStatus('Closing MarketRadar.');" in source
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
    form_control_guard = source.index(
        "event.target instanceof HTMLInputElement",
    )

    assert escape_handler < form_control_guard
    assert "qs('#command-input').focus();" in source
    assert "setCommandStatus('Command box focused.');" in source


def test_tauri_dashboard_exposes_cli_command_reference_families() -> None:
    source = Path("apps/radar-desktop/frontend/app.js").read_text(
        encoding="utf-8",
    )

    assert "data-testid=\"command-reference\"" in source
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
