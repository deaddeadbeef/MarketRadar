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
};

const keyAliases = new Map([
  ['0', 'tutorial'],
  ['1', 'overview'],
  ['2', 'readiness'],
  ['3', 'run'],
  ['4', 'candidates'],
  ['5', 'alerts'],
  ['6', 'ipo'],
  ['7', 'broker'],
  ['8', 'ops'],
  ['9', 'telemetry'],
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
};

const executeClassCommands = new Set([
  'action',
  'agent execute',
  'bars manual import execute',
  'bars saved capture confirm',
  'bars saved import execute',
  'batch execute',
  'cik import execute',
  'feedback',
  'ledger record',
  'options import execute',
  'outcome update',
  'run execute',
  'ticket',
  'trigger',
]);

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
  host.innerHTML = state.config.pages.map((page) => `
    <button
      class="workflow-tab"
      type="button"
      role="tab"
      aria-selected="${page.key === state.page}"
      aria-current="${page.key === state.page ? 'page' : 'false'}"
      aria-keyshortcuts="${escapeHtml(page.shortcut)}"
      aria-controls="dashboard-main"
      id="tab-${escapeHtml(page.key)}"
      data-testid="${escapeHtml(page.test_id)}"
      data-page="${escapeHtml(page.key)}"
      tabindex="${page.key === state.page ? '0' : '-1'}"
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
  const pageInfo = state.config.pages.find((page) => page.key === state.page);
  setText('#page-title', pageInfo ? pageInfo.label : 'Dashboard');
  setStatus(status);
  setText('#refresh-label', `refresh=${state.lastRefresh ? state.lastRefresh.toLocaleTimeString() : 'pending'}`);
  setText('#provider-calls', `provider_calls=${compact(snapshot.external_calls_made, '0')}`);
  setText('#next-action', compact(snapshot.next_action || snapshot.canonical_next_action, 'Review the current page.'));
  setText('#next-command', compact(snapshot.next_command || snapshot.canonical_next_command, 'No command reported.'));
  setText('#boundary-copy', `Snapshot mode ${compact(snapshot.snapshot_mode, 'unknown')}; provider calls reported ${compact(snapshot.external_calls_made, '0')}.`);
  updateAutomationState(snapshot, status, pageInfo);
  renderContent(snapshot);
}

function updateAutomationState(snapshot, status, pageInfo) {
  const main = qs('#dashboard-main');
  if (main) {
    main.dataset.currentPage = state.page;
    main.setAttribute('aria-label', `Dashboard page ${pageInfo ? pageInfo.label : state.page}`);
  }
  setText(
    '#automation-state',
    [
      `page=${state.page}`,
      `label=${pageInfo ? pageInfo.label : state.page}`,
      `status=${status}`,
      `provider_calls=${compact(snapshot.external_calls_made, '0')}`,
      `next_command=${compact(snapshot.next_command || snapshot.canonical_next_command, 'none')}`,
    ].join(' ')
  );
}

function setStatus(status) {
  const pill = qs('#status-chip');
  if (!pill) return;
  const normalized = String(status || 'unknown').toLowerCase().replaceAll(' ', '_');
  pill.textContent = normalized;
  pill.className = `status-pill ${normalized}`;
}

function renderContent(snapshot) {
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
  const ticker = compact(row.ticker || row.symbol || row.security, `row-${index + 1}`);
  const label = `Dashboard row ${ticker}`;
  return `
    <tr data-testid="queue-row" data-ticker="${escapeHtml(ticker)}" aria-label="${escapeHtml(label)}">
      ${columns.map((column) => `<td class="${column.className || ''}">${escapeHtml(column.value(row, index))}</td>`).join('')}
    </tr>
  `;
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
    </section>
  `;
}

function rawJsonPanel(snapshot) {
  return `
    <section class="panel wide" data-testid="snapshot-json">
      <details>
        <summary>Raw JSON snapshot</summary>
        <pre class="raw-json">${escapeHtml(JSON.stringify(snapshot, null, 2))}</pre>
      </details>
    </section>
  `;
}

async function handleCommandSubmit(event) {
  event.preventDefault();
  const input = qs('#command-input');
  const raw = input.value.trim();
  input.value = '';
  await applyCommand(raw);
  input.focus();
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
    setCommandStatus('Close the MarketRadar window to exit.');
    return;
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
  if (['next', 'more'].includes(command)) {
    const limit = Math.max(1, Number(qs('#filter-limit').value || 50));
    state.scanOffset += limit;
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
    const offset = Number(value);
    if (!Number.isFinite(offset) || offset < 1) {
      setCommandStatus('Usage: offset ROW.');
      return;
    }
    state.scanOffset = Math.floor(offset) - 1;
    setCommandStatus(`Rows starting at ${state.scanOffset + 1}.`);
    await setPage('overview');
    return;
  }
  if (command === 'limit') {
    const limit = Number(value);
    if (!Number.isFinite(limit)) {
      setCommandStatus('Usage: limit 1-200.');
      return;
    }
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
    state.sourceGap = listFilter(value);
    state.scanOffset = 0;
    setCommandStatus(state.sourceGap.length ? `Source gaps: ${state.sourceGap.join(', ')}.` : 'Source-gap filter cleared.');
    await setPage('overview');
    return;
  }
  if (['decision-gap', 'decision_gaps', 'gap'].includes(command)) {
    state.decisionGap = listFilter(value);
    state.scanOffset = 0;
    setCommandStatus(state.decisionGap.length ? `Decision gaps: ${state.decisionGap.join(', ')}.` : 'Decision-gap filter cleared.');
    await setPage('overview');
    return;
  }
  if (['usefulness', 'useful'].includes(command)) {
    state.usefulness = ['', 'all', 'none'].includes(value) ? null : value;
    state.scanOffset = 0;
    setCommandStatus(state.usefulness ? `Usefulness: ${state.usefulness}.` : 'Usefulness filter cleared.');
    await setPage('overview');
    return;
  }
  if (['available-at', 'cutoff'].includes(command)) {
    state.availableAt = ['', 'latest', 'all', 'none'].includes(value) ? null : value;
    state.scanOffset = 0;
    setCommandStatus(state.availableAt ? `Available-at: ${state.availableAt}.` : 'Available-at filter cleared.');
    await refreshSnapshot();
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
    qs('.raw-json')?.focus?.();
    setCommandStatus('Raw JSON opened.');
    return;
  }
  if (command === 'open') {
    setCommandStatus(openCommandMessage(value));
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
  qs('#filter-ticker').value = '';
  qs('#filter-scan-mode').value = 'all';
  qs('#filter-stocks-only').checked = false;
  qs('#filter-limit').value = '50';
  state.availableAt = null;
  state.sourceGap = [];
  state.decisionGap = [];
  state.usefulness = null;
  state.scanOffset = 0;
}

function listFilter(value) {
  return ['', 'all', 'none'].includes(value)
    ? []
    : value.split(/[,\s]+/).map((item) => item.trim()).filter(Boolean);
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

function openCommandMessage(value) {
  const rows = rowsFromSnapshot(state.snapshot || {});
  const query = value.trim().toUpperCase();
  if (!query) return 'Usage: open # or open TICKER.';
  const index = Number(query);
  const row = Number.isFinite(index) && index >= 1 ? rows[index - 1] : rows.find((item) => String(item.ticker || item.symbol || '').toUpperCase() === query);
  if (!row) return `No row matched ${value}.`;
  const ticker = compact(row.ticker || row.symbol || row.security, value);
  const command = compact(row.next_command || row.next_action || row.command, 'Review the row details.');
  return `${ticker}: ${command}`;
}

function guardedCommandMessage(normalized) {
  const first = normalized.split(' ')[0];
  if (executeClassCommands.has(normalized) || normalized.includes(' execute')) {
    return 'Execute commands stay outside dashboard browsing. Copy the displayed command and run it in PowerShell after reviewing call/write boundaries.';
  }
  if (['agent', 'bars', 'batch', 'batches', 'cik', 'options', 'run'].includes(first)) {
    const nextCommand = compact(state.snapshot?.next_command || state.snapshot?.canonical_next_command, 'catalyst-radar dashboard-snapshot --json --fast');
    return `Review command boundary. Suggested external command: ${nextCommand}`;
  }
  return '';
}

function guardedCommandPage(command) {
  if (command === 'agent') return 'agent';
  if (['bars', 'options', 'run'].includes(command)) return 'run';
  if (['batch', 'batches', 'cik'].includes(command)) return 'ops';
  return null;
}

function setCommandStatus(message) {
  setText('#command-status', `command=${message}`);
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
  if (event.target instanceof HTMLInputElement || event.target instanceof HTMLSelectElement || event.target instanceof HTMLTextAreaElement) {
    return;
  }
  if (event.key === 'Escape') {
    event.preventDefault();
    qs('#command-input').focus();
    setCommandStatus('Command box focused.');
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
  const current = Math.max(0, pages.indexOf(state.page));
  const next = (current + delta + pages.length) % pages.length;
  setPage(pages[next]);
}

boot().catch((error) => {
  console.error(error?.stack || error?.message || error);
  setStatus('error');
  showError(error);
});


