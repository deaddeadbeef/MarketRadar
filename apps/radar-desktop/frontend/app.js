const tauriInvoke = window.__TAURI__?.core?.invoke;

const state = {
  config: null,
  snapshot: null,
  page: 'overview',
  loading: false,
  lastRefresh: null,
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
      aria-controls="dashboard-main"
      id="tab-${escapeHtml(page.key)}"
      data-testid="${escapeHtml(page.test_id)}"
      data-page="${escapeHtml(page.key)}"
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
  if (!page || page === state.page) return;
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
    priced_in_status: qs('#filter-scan-mode').value,
    stocks_only: qs('#filter-stocks-only').checked,
    scan_limit: Number(qs('#filter-limit').value || 50),
    scan_offset: 0,
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
  renderContent(snapshot);
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


