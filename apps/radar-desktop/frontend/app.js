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
  ['2', 'readiness'],
  ['blockers', 'readiness'],
  ['evidence', 'readiness'],
  ['evidence_gaps', 'readiness'],
  ['gaps', 'readiness'],
  ['3', 'run'],
  ['call_plan', 'run'],
  ['plan', 'run'],
  ['safe', 'run'],
  ['safe_run', 'run'],
  ['4', 'candidates'],
  ['candidate', 'candidates'],
  ['candidate_review', 'candidates'],
  ['11', 'review'],
  ['decision', 'review'],
  ['decisions', 'review'],
  ['decision_ready', 'review'],
  ['5', 'alerts'],
  ['alert', 'alerts'],
  ['6', 'ipo'],
  ['s1', 'ipo'],
  ['7', 'broker'],
  ['8', 'ops'],
  ['9', 'telemetry'],
  ['10', 'agent'],
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
  ['h', 'help'],
  ['?', 'help'],
]);

const pagePaths = {
  readiness: [['readiness'], ['real_results'], ['full_market_trust_gate']],
  run: [['call_plan'], ['radar_run'], ['operator_next_step']],
  broker: [['broker'], ['runtime_context']],
  ops: [['ops_health'], ['runtime_context'], ['provider_preflight']],
  telemetry: [['telemetry'], ['telemetry_coverage'], ['raw_telemetry']],
  agent: [['agent_brief'], ['runtime_context']],
  validation: [['validation'], ['validation', 'latest_run'], ['validation', 'report']],
  costs: [['costs'], ['value_ledger'], ['value_outcomes'], ['value_report']],
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
  'eval-triggers',
  'evaluate-triggers',
  'feedback',
  'ledger',
  'outcome',
  'outcomes',
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

const commandReference = [
  ['0..9, Ctrl+A, V, F, ?, or page name', 'Switch pages; Ctrl+A opens Agent and V opens Costs.'],
  ['themes / validation / costs / features', 'Open local evidence pages for clustered themes, validation, costs, and feature inventory.'],
  ['setup / first', 'Show the first setup command and where to run it.'],
  ['open #|TICKER', 'Open a row from Candidate Review or show its next command.'],
  ['ticker SYMBOL|all', 'Filter ticker-aware pages.'],
  ['available-at ISO|latest', 'Set or clear the point-in-time cutoff.'],
  ['ready / full / mismatches / stocks', 'Switch between decision-useful, full universe, mismatch, and stock-only scan views.'],
  ['usefulness STATUS|all', 'Filter Inbox by usefulness verdict.'],
  ['source-gap SOURCE|all', 'Filter Inbox by missing or stale source evidence.'],
  ['decision-gap GAP|all', 'Filter Inbox by missing decision evidence.'],
  ['next / prev / offset ROW / limit 1-200', 'Page through current Inbox scan rows.'],
  ['export full / export current', 'Show JSON export commands without running them.'],
  ['batch SOURCE / batch SOURCE all / batch SOURCE execute 3', 'Plan source fills or show the external execution boundary.'],
  ['catalyst-radar COMMAND', 'Show where to run full CLI commands without executing them in the dashboard.'],
  ['bars manual template/import', 'Show market-bar repair command boundaries.'],
  ['bars saved capture/validate/import', 'Show saved grouped-daily command boundaries.'],
  ['options template/validate/import', 'Show point-in-time options command boundaries.'],
  ['cik template/validate/import', 'Show SEC CIK override command boundaries.'],
  ['agent / agent execute', 'Preview agent gates or show the OpenAI execution boundary.'],
  ['alert-status STATUS|all / alert-route ROUTE|all', 'Filter alerts.'],
  ['run / run execute', 'Open Safe Run or show the capped run execution boundary.'],
  ['action / trigger / ticket / feedback', 'Run guarded local Broker or Alert commands through the dashboard backend.'],
  ['ledger coverage / record', 'Run guarded local value-ledger commands through the dashboard backend.'],
  ['outcome coverage / update', 'Run guarded local value-outcome commands through the dashboard backend.'],
  ['json', 'Open and focus the raw JSON snapshot.'],
  ['clear-filters / refresh / q', 'Reset filters, reload, or close the native window.'],
];

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

function featureRows(snapshot) {
  const rows = snapshot?.feature_inventory || at(snapshot, ['features', 'rows'], []);
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
  renderNav();
  renderAutomation();
  setText('#source-label', `source=${state.config.source_label}`);
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
  setText('#boundary-copy', `Snapshot mode ${compact(snapshot.snapshot_mode, 'unknown')}; provider calls reported ${compact(snapshot.external_calls_made, '0')}.`);
  updateAutomationState(snapshot, status, pageInfo);
  renderContent(snapshot);
  bindQueueRows();
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
    readiness: () => renderStructuredPage('Evidence Gaps', pagePaths.readiness),
    run: () => renderStructuredPage('Safe Run', pagePaths.run),
    candidates: () => renderQueuePage('Candidate Review', rowsFromSnapshot(snapshot)),
    review: () => renderQueuePage('Decision Review', rowsFromSnapshot(snapshot)),
    alerts: () => renderQueuePage('Alerts', alertRows(snapshot), alertColumns),
    ipo: () => renderQueuePage('IPO/S-1', ipoRows(snapshot), ipoColumns),
    broker: () => renderStructuredPage('Broker', pagePaths.broker),
    ops: () => renderStructuredPage('Ops', pagePaths.ops),
    telemetry: renderTelemetry,
    agent: () => renderStructuredPage('Agent', pagePaths.agent),
    themes: () => renderQueuePage('Themes', themeRows(snapshot), themeColumns),
    validation: () => renderStructuredPage('Validation', pagePaths.validation),
    costs: renderCosts,
    features: renderFeatures,
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

function renderTutorial() {
  return `
    <section class="panel wide" data-testid="tutorial-panel">
      <h2>First 90 Seconds</h2>
      <div class="stack">
        <p>1 Inbox: triage what matters now.</p>
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

function renderTelemetry(snapshot) {
  const events = at(snapshot, ['telemetry', 'events'], []);
  const rows = Array.isArray(events) ? events : [];
  return `${queuePanel('Telemetry', rows, telemetryColumns)}${objectPanel('telemetry coverage', at(snapshot, ['telemetry_coverage'], null))}`;
}

const telemetryColumns = [
  { label: 'Type', className: 'ticker', value: (row) => compact(row.event_type || row.type) },
  { label: 'Status', className: 'state', value: (row) => compact(row.status) },
  { label: 'Artifact', value: (row) => compact(row.artifact_type || row.artifact_id) },
  { label: 'Reason', value: (row) => compact(row.reason || row.occurred_at || row.created_at) },
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

function renderFeatures(snapshot) {
  const rows = featureRows(snapshot);
  return queuePanel('Features', rows, [
    { label: 'Area', className: 'ticker', value: (row) => compact(row.area) },
    { label: 'Feature', value: (row) => compact(row.feature) },
    { label: 'Page', className: 'state', value: (row) => compact(row.page) },
    { label: 'Use', value: (row) => compact(row.use) },
  ]);
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
          <thead><tr><th>Command</th><th>Meaning</th></tr></thead>
          <tbody>${commandReference.map(([command, meaning]) => `
            <tr>
              <td class="ticker">${escapeHtml(command)}</td>
              <td>${escapeHtml(meaning)}</td>
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
  if (!raw) {
    setCommandStatus('Refreshed.');
    await refreshSnapshot();
    return;
  }
  const normalized = raw.toLowerCase().replace(/\s+/g, ' ').trim();
  const [command, ...parts] = normalized.split(' ');
  const value = parts.join(' ').trim();

  if (['q', 'quit', 'exit'].includes(command)) {
    setCommandStatus('Closing MarketRadar.');
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
  if (event.target instanceof HTMLInputElement || event.target instanceof HTMLSelectElement || event.target instanceof HTMLTextAreaElement) {
    return;
  }
  if (event.ctrlKey && event.key.toLowerCase() === 'a') {
    event.preventDefault();
    setPage('agent');
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
  const alias = keyAliases.get(event.key.toLowerCase());
  if (alias) {
    event.preventDefault();
    setPage(alias);
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


