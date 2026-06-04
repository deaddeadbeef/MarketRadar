use std::env;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use radar_tui::client::{
    SnapshotFilters, SnapshotRequest, SnapshotSource,
    execute_dashboard_command as execute_client_dashboard_command, fetch_snapshot,
};
use radar_tui::model::Page;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tauri::{AppHandle, Manager, State};

#[derive(Clone, Debug, Serialize)]
struct PageInfo {
    key: &'static str,
    label: &'static str,
    shortcut: &'static str,
    description: &'static str,
    test_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct DesktopConfig {
    app_name: &'static str,
    initial_page: String,
    source_label: String,
    repo_root: String,
    pages: Vec<PageInfo>,
    automation: AutomationManifest,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationManifest {
    contract_version: &'static str,
    landmark_test_ids: Vec<&'static str>,
    keyboard_shortcuts: Vec<&'static str>,
    native_window_title: &'static str,
    native_executable: &'static str,
    computer_use_steps: Vec<ComputerUseStep>,
    zero_call_assertions: Vec<&'static str>,
    notes: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
struct ComputerUseStep {
    step: &'static str,
    action: &'static str,
    target: &'static str,
    expected: &'static str,
}

struct DesktopState {
    config: DesktopConfig,
    source: SnapshotSource,
    last_snapshot: Mutex<Option<Value>>,
}

#[derive(Debug, Deserialize)]
struct SnapshotInput {
    page: Option<String>,
    ticker: Option<String>,
    available_at: Option<String>,
    alert_status: Option<String>,
    alert_route: Option<String>,
    priced_in_status: Option<String>,
    usefulness: Option<String>,
    source_gap: Option<Vec<String>>,
    decision_gap: Option<Vec<String>>,
    stocks_only: Option<bool>,
    scan_limit: Option<u16>,
    scan_offset: Option<u32>,
    telemetry_limit: Option<u16>,
}

#[derive(Debug, Default)]
struct DesktopArgs {
    page: Option<String>,
    api_base_url: Option<String>,
    api_role: Option<String>,
    allow_invalid_certs: bool,
    snapshot_command: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PageRequest {
    snapshot_page: Page,
    selected_page: String,
    detail_ticker: Option<String>,
}

#[tauri::command]
fn desktop_config(state: State<'_, DesktopState>) -> DesktopConfig {
    state.config.clone()
}

#[tauri::command]
fn dashboard_snapshot(
    state: State<'_, DesktopState>,
    input: SnapshotInput,
) -> Result<Value, String> {
    let page_request = page_request(
        input
            .page
            .as_deref()
            .unwrap_or(state.config.initial_page.as_str()),
    );
    let mut filters = snapshot_filters(input);
    if let Some(ticker) = page_request.detail_ticker.clone() {
        filters.ticker = Some(ticker);
        filters.scan_offset = 0;
    }
    let request = SnapshotRequest {
        page: page_request.snapshot_page,
        requested_page: Some(page_request.selected_page.clone()),
        filters,
    };
    let mut value = fetch_snapshot(&state.source, &request).map_err(|err| err.to_string())?;
    ensure_selected_page(&mut value, &page_request.selected_page);
    if let Ok(mut last_snapshot) = state.last_snapshot.lock() {
        *last_snapshot = Some(value.clone());
    }
    Ok(value)
}

#[tauri::command]
fn last_dashboard_snapshot(state: State<'_, DesktopState>) -> Option<Value> {
    state
        .last_snapshot
        .lock()
        .ok()
        .and_then(|snapshot| snapshot.clone())
}

#[tauri::command]
fn execute_dashboard_command(
    state: State<'_, DesktopState>,
    command: String,
    input: SnapshotInput,
) -> Result<Value, String> {
    let page_request = page_request(
        input
            .page
            .as_deref()
            .unwrap_or(state.config.initial_page.as_str()),
    );
    let mut filters = snapshot_filters(input);
    if let Some(ticker) = page_request.detail_ticker.clone() {
        filters.ticker = Some(ticker);
        filters.scan_offset = 0;
    }
    let request = SnapshotRequest {
        page: page_request.snapshot_page,
        requested_page: Some(page_request.selected_page),
        filters,
    };
    execute_client_dashboard_command(&state.source, &command, &request)
        .map_err(|err| err.to_string())
}

#[tauri::command]
fn close_dashboard_window(app: AppHandle) -> Result<(), String> {
    let window = app
        .get_webview_window("main")
        .ok_or_else(|| "dashboard window not found".to_string())?;
    window.close().map_err(|err| err.to_string())
}

fn main() {
    let args = parse_args(env::args().skip(1));
    let repo_root = find_repo_root().unwrap_or_else(|| env::current_dir().unwrap_or_default());
    let source = snapshot_source(&args, &repo_root);
    let config = DesktopConfig {
        app_name: "MarketRadar",
        initial_page: initial_page_key(args.page.as_deref()),
        source_label: source.label(),
        repo_root: repo_root.display().to_string(),
        pages: page_infos(),
        automation: automation_manifest(),
    };

    tauri::Builder::default()
        .manage(DesktopState {
            config,
            source,
            last_snapshot: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            desktop_config,
            dashboard_snapshot,
            last_dashboard_snapshot,
            execute_dashboard_command,
            close_dashboard_window
        ])
        .run(tauri::generate_context!())
        .expect("error while running MarketRadar desktop dashboard");
}

fn snapshot_filters(input: SnapshotInput) -> SnapshotFilters {
    SnapshotFilters {
        database_url: None,
        ticker: input.ticker,
        available_at: input.available_at,
        alert_status: input.alert_status,
        alert_route: input.alert_route,
        priced_in_status: input.priced_in_status.unwrap_or_else(|| "all".to_string()),
        usefulness: input.usefulness,
        source_gap: input.source_gap.unwrap_or_default(),
        decision_gap: input.decision_gap.unwrap_or_default(),
        stocks_only: input.stocks_only.unwrap_or(false),
        scan_limit: input.scan_limit.unwrap_or(50),
        scan_offset: input.scan_offset.unwrap_or(0),
        telemetry_limit: input.telemetry_limit.unwrap_or(8),
    }
}

fn ensure_selected_page(value: &mut Value, page: &str) {
    match value {
        Value::Object(object) => {
            object.insert("selected_page".to_string(), Value::String(page.to_string()));
        }
        _ => {
            let mut object = Map::new();
            object.insert("selected_page".to_string(), Value::String(page.to_string()));
            object.insert("payload".to_string(), value.clone());
            *value = Value::Object(object);
        }
    }
}

fn initial_page_key(raw_page: Option<&str>) -> String {
    page_request(raw_page.unwrap_or("overview")).selected_page
}

fn page_request(raw_page: &str) -> PageRequest {
    let trimmed = raw_page.trim();
    if let Some(ticker) = detail_suffix(trimmed, "candidate:") {
        let ticker = ticker.to_ascii_uppercase();
        return PageRequest {
            snapshot_page: Page::Overview,
            selected_page: format!("candidate:{ticker}"),
            detail_ticker: Some(ticker),
        };
    }
    if let Some(alert_id) = detail_suffix(trimmed, "alert:") {
        return PageRequest {
            snapshot_page: Page::Alerts,
            selected_page: format!("alert:{alert_id}"),
            detail_ticker: None,
        };
    }
    let page = Page::from_input(trimmed);
    PageRequest {
        snapshot_page: page,
        selected_page: page.key().to_string(),
        detail_ticker: None,
    }
}

fn detail_suffix<'a>(raw_page: &'a str, prefix: &str) -> Option<&'a str> {
    raw_page
        .get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))
        .and_then(|_| raw_page.get(prefix.len()..))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn snapshot_source(args: &DesktopArgs, repo_root: &Path) -> SnapshotSource {
    if let Some(command) = args
        .snapshot_command
        .clone()
        .or_else(|| env::var("CATALYST_DASHBOARD_SNAPSHOT_COMMAND").ok())
    {
        return SnapshotSource::Command { command };
    }

    if let Some(base_url) = args
        .api_base_url
        .clone()
        .or_else(|| env::var("CATALYST_DASHBOARD_API_BASE_URL").ok())
    {
        return SnapshotSource::Api {
            base_url,
            role: args
                .api_role
                .clone()
                .or_else(|| env::var("CATALYST_API_ROLE").ok()),
            allow_invalid_certs: args.allow_invalid_certs,
        };
    }

    SnapshotSource::Command {
        command: default_snapshot_command(repo_root),
    }
}

fn default_snapshot_command(repo_root: &Path) -> String {
    let src_path = repo_root.join("src");
    let python = local_python(repo_root)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "python".to_string());
    if cfg!(windows) {
        format!(
            "$env:PYTHONPATH={}; & {} -m catalyst_radar.cli dashboard-snapshot --json --fast",
            powershell_quote(&src_path.display().to_string()),
            powershell_quote(&python)
        )
    } else {
        format!(
            "PYTHONPATH={} {} -m catalyst_radar.cli dashboard-snapshot --json --fast",
            shell_quote(&src_path.display().to_string()),
            shell_quote(&python)
        )
    }
}

fn local_python(repo_root: &Path) -> Option<PathBuf> {
    let path = if cfg!(windows) {
        repo_root.join(".venv").join("Scripts").join("python.exe")
    } else {
        repo_root.join(".venv").join("bin").join("python")
    };
    path.exists().then_some(path)
}

fn parse_args<I>(args: I) -> DesktopArgs
where
    I: IntoIterator<Item = String>,
{
    let mut parsed = DesktopArgs::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--page" => parsed.page = args.next(),
            "--api-base-url" => parsed.api_base_url = args.next(),
            "--api-role" => parsed.api_role = args.next(),
            "--allow-invalid-certs" => parsed.allow_invalid_certs = true,
            "--snapshot-command" => parsed.snapshot_command = args.next(),
            _ => {}
        }
    }
    parsed
}

fn find_repo_root() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(current) = env::current_dir() {
        candidates.push(current);
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(parent) = exe.parent() {
            candidates.push(parent.to_path_buf());
        }
    }
    for candidate in candidates {
        for path in candidate.ancestors() {
            if path.join("pyproject.toml").exists()
                && path.join("src").join("catalyst_radar").exists()
            {
                return Some(path.to_path_buf());
            }
        }
    }
    None
}

fn powershell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn page_infos() -> Vec<PageInfo> {
    Page::ALL
        .iter()
        .map(|page| PageInfo {
            key: page.key(),
            label: page.label(),
            shortcut: page_shortcut(*page),
            description: page_description(*page),
            test_id: format!("nav-page-{}", page.key()),
        })
        .collect()
}

fn page_shortcut(page: Page) -> &'static str {
    match page {
        Page::Tutorial => "0",
        Page::Overview => "1",
        Page::Readiness => "2",
        Page::Run => "3",
        Page::Candidates => "4",
        Page::Review => "D",
        Page::Alerts => "5",
        Page::Ipo => "6",
        Page::Broker => "7",
        Page::Ops => "8",
        Page::Telemetry => "9",
        Page::Agent => "Ctrl+A",
        Page::Themes => "theme",
        Page::Validation => "valid",
        Page::Costs => "V",
        Page::Features => "F",
        Page::Help => "?",
    }
}

fn page_description(page: Page) -> &'static str {
    match page {
        Page::Tutorial => "First-run path and safe operating boundary.",
        Page::Overview => "Inbox, status, first blocker, and next safe action.",
        Page::Readiness => "Evidence gaps and setup blockers before relying on output.",
        Page::Run => "Safe run plan, provider-call budget, and execution gates.",
        Page::Candidates => "Candidate queue with source and decision gaps.",
        Page::Review => "Decision-ready rows filtered to useful review candidates.",
        Page::Alerts => "Research alerts and routing status.",
        Page::Ipo => "IPO/S-1 catalyst evidence rows.",
        Page::Broker => "Read-only broker and portfolio context.",
        Page::Ops => "Provider health, runtime context, and run diagnostics.",
        Page::Telemetry => "Audit tape and telemetry coverage.",
        Page::Agent => "Zero-call agent preview and gated OpenAI execution status.",
        Page::Themes => "Clustered catalyst patterns and repeated theme context.",
        Page::Validation => "Shadow, paper, and value validation evidence.",
        Page::Costs => "Value ledger, outcomes, validation, and cost evidence.",
        Page::Features => "Feature inventory and where each feature lives.",
        Page::Help => "Keyboard, automation, and command reference.",
    }
}

fn automation_manifest() -> AutomationManifest {
    AutomationManifest {
        contract_version: "market-radar-desktop-automation-v1",
        landmark_test_ids: vec![
            "desktop-shell",
            "workflow-nav",
            "dashboard-toolbar",
            "dashboard-page",
            "command-form",
            "command-input",
            "command-status",
            "automation-state",
            "attention-queue",
            "next-safe-action",
            "snapshot-json",
            "snapshot-json-output",
        ],
        keyboard_shortcuts: vec![
            "0-9 jump to numbered workflow pages",
            "Ctrl+A opens Agent",
            "Type themes or validation to open evidence pages",
            "V opens Costs",
            "F opens Features",
            "? opens Help",
            "ArrowRight/ArrowDown moves forward",
            "ArrowLeft/ArrowUp moves backward",
            "F5 refreshes the local snapshot",
            "Home opens Start, End opens Help",
            "Esc focuses the command box",
            "next and prev page through scan rows without walking past the end",
            "clear-filters resets filters while preserving the row limit",
            "usefulness clears with all, any, none, or blank; alert filters clear with all, none, or blank",
            "Command box accepts safe page, filter, refresh, help, and JSON commands",
            "offset, limit, and available-at commands reject invalid values before refreshing",
            "source-gap and decision-gap commands reject unsupported values before refreshing",
            "batch SOURCE opens an Ops source plan; batch SOURCE all and batch SOURCE execute N show PowerShell boundaries",
            "run opens Safe Run; run execute starts the guarded radar-run API/CLI backend path",
            "action, trigger, ticket, feedback, ledger, and outcome commands use the guarded dashboard backend for local DB-only operations",
            "q, quit, or exit closes the native desktop window",
            "Full catalyst-radar commands show a PowerShell boundary instead of executing in-app",
        ],
        native_window_title: "MarketRadar Command Center",
        native_executable: "target\\release\\radar-desktop.exe",
        computer_use_steps: computer_use_steps(),
        zero_call_assertions: vec![
            "Dashboard browsing, command-box navigation, filtering, copy, and raw JSON inspection must leave provider_calls=0.",
            "Local broker, feedback, value-ledger, and outcome commands may write the local DB through the guarded dashboard backend, but must not make provider, OpenAI, broker, order, or external calls unless the command explicitly reports an external-call budget.",
            "Source batch plan commands may read the current snapshot, but execute variants must remain external PowerShell boundaries and leave provider_calls=0.",
            "Invalid source-gap or decision-gap filter commands must not refresh the snapshot or change filters.",
            "Invalid offset, limit, or available-at commands must not refresh the snapshot or change filters.",
            "Pagination commands must not advance scan_offset beyond priced_in_queue.total_count.",
            "clear-filters must preserve the chosen row limit while clearing ticker, source, decision, availability, alert, usefulness, and offset filters.",
            "Optional usefulness filters must clear case-insensitively for all, any, none, or blank input; alert-status and alert-route clear for all, none, or blank input.",
            "Full catalyst-radar commands typed into the desktop command box must stay external and leave provider_calls=0.",
            "Clicking or pressing Enter on queue rows must open local candidate/alert detail without provider calls.",
            "Dynamic detail pages must expose both page=<candidate|alert detail> and nav=<parent workflow page> for automation.",
            "q, quit, and exit close the native window through the Tauri window API and must not run provider, OpenAI, broker, or DB-write actions.",
        ],
        notes: vec![
            "Every workflow button has role=tab, aria-selected, and a nav-page-* data-testid.",
            "The current page title is exposed through data-testid=page-title.",
            "The exact selected page, parent nav page, and provider-call count are exposed through data-testid=automation-state.",
            "The dashboard main region exposes data-current-page and data-current-nav-page for dynamic detail pages.",
            "Candidate detail pages keep nav-page-candidates selected; alert detail pages keep nav-page-alerts selected.",
            "Rows use data-testid=queue-row, are keyboard focusable, and include ticker-specific labels when available.",
            "Refreshing reads the existing dashboard JSON contract and makes zero provider calls.",
            "Local broker, feedback, value-ledger, and outcome commands use the guarded dashboard backend; source-batch execute and provider import commands remain external PowerShell boundaries; run execute uses the guarded radar-run API/CLI backend path.",
        ],
    }
}

fn computer_use_steps() -> Vec<ComputerUseStep> {
    vec![
        ComputerUseStep {
            step: "launch",
            action: "Launch the app by executable path through Computer Use, then select the returned window object.",
            target: "target\\release\\radar-desktop.exe",
            expected: "A native window titled MarketRadar Command Center is targetable.",
        },
        ComputerUseStep {
            step: "capture",
            action: "Capture screenshot and accessibility text for the selected window.",
            target: "MarketRadar Command Center",
            expected: "The window exposes MarketRadar workflow tabs, dashboard-page, command-input, automation-state, next-safe-action, page=<PAGE>, nav=<WORKFLOW_PAGE>, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "focus-command",
            action: "Press Escape in the dashboard window.",
            target: "command-input",
            expected: "The command box receives focus and command-status reports command box focused.",
        },
        ComputerUseStep {
            step: "filter-command",
            action: "Type ticker MSFT and press Return.",
            target: "command-input",
            expected: "filter-ticker is MSFT, automation-state remains page=overview, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "filter-validation-command",
            action: "Type source-gap nonsense and press Return.",
            target: "command-input",
            expected: "command-status reports Unsupported source-gap value, the filter is unchanged, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "numeric-validation-command",
            action: "Type limit 1.5 and press Return.",
            target: "command-input",
            expected: "command-status reports Usage: limit 1-200, the scan limit is unchanged, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "time-validation-command",
            action: "Type available-at nonsense and press Return.",
            target: "command-input",
            expected: "command-status reports Invalid timestamp, available_at is unchanged, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "pagination-boundary-command",
            action: "When the current scan page is at the end, type next and press Return.",
            target: "command-input",
            expected: "command-status reports Already at the end of the current scan filter and provider_calls=0.",
        },
        ComputerUseStep {
            step: "clear-filters-command",
            action: "Type limit 25, press Return, then type clear-filters and press Return.",
            target: "command-input",
            expected: "filter-limit remains 25, non-limit filters are reset, scan_offset returns to 0, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "optional-filter-clear-command",
            action: "Type usefulness ANY and press Return.",
            target: "command-input",
            expected: "usefulness is cleared case-insensitively, command-status reports Usefulness filter cleared, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "page-command",
            action: "Type ready and press Return.",
            target: "command-input",
            expected: "dashboard-page reports page=review and the selected tab is Review.",
        },
        ComputerUseStep {
            step: "row-open",
            action: "Focus a queue-row and press Return, or type open 1 and press Return.",
            target: "queue-row",
            expected: "dashboard-page reports page=candidate:<TICKER> with nav=candidates or page=alert:<ID> with nav=alerts, the detail panel is visible, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "guarded-command",
            action: "Type batch catalyst_events and press Return.",
            target: "command-input",
            expected: "dashboard-page reports page=ops, command-status shows a source-specific Ops plan or workflow status, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "source-batch-execute-boundary",
            action: "Type batch catalyst_events execute 3 and press Return.",
            target: "command-input",
            expected: "dashboard-page reports page=ops, command-status shows the PowerShell command with --execute-batches 3 and provider_calls=0.",
        },
        ComputerUseStep {
            step: "local-dashboard-command",
            action: "Type action ACME watch Codex smoke and press Return only after intentional local write validation.",
            target: "command-input",
            expected: "dashboard-page reports page=broker, command-status reports Local only, db_writes=1, and no provider, OpenAI, broker, order, or external calls occur after refresh.",
        },
        ComputerUseStep {
            step: "safe-run-execute-command",
            action: "Type run execute and press Return only after reviewing the Safe Run call plan.",
            target: "command-input",
            expected: "dashboard-page reports page=run, command-status reports Radar run finished, blocked, or rate limited, and the backend returns the radar_run telemetry contract.",
        },
        ComputerUseStep {
            step: "powershell-command",
            action: "Type catalyst-radar priced-in-queue --full-scan --all --json and press Return.",
            target: "command-input",
            expected: "command-status says it is a PowerShell command, not a dashboard command, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "json-command",
            action: "Type json and press Return.",
            target: "snapshot-json-output",
            expected: "Raw JSON snapshot opens, focus moves to snapshot-json-output, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "close-command",
            action: "Type q and press Return only when the automation session is finished.",
            target: "command-input",
            expected: "The native MarketRadar Command Center window closes without provider, OpenAI, broker, or DB-write actions.",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_command_uses_local_snapshot_contract() {
        let command = default_snapshot_command(Path::new("C:/repo/MarketRadar"));

        assert!(command.contains("dashboard-snapshot --json --fast"));
        assert!(command.contains("PYTHONPATH") || command.contains("$env:PYTHONPATH"));
    }

    #[test]
    fn page_manifest_exposes_stable_automation_ids() {
        let pages = page_infos();

        assert!(pages.iter().any(|page| page.test_id == "nav-page-overview"));
        assert!(pages.iter().any(|page| page.shortcut == "Ctrl+A"));
        assert!(pages.iter().any(|page| page.test_id == "nav-page-themes"));
        assert!(
            pages
                .iter()
                .any(|page| page.test_id == "nav-page-validation")
        );
        assert!(pages.iter().any(|page| page.test_id == "nav-page-costs"));
    }

    #[test]
    fn automation_manifest_exposes_command_surface() {
        let manifest = automation_manifest();

        assert!(manifest.landmark_test_ids.contains(&"command-input"));
        assert!(manifest.landmark_test_ids.contains(&"automation-state"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-json-output"));
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("command box"))
        );
        assert!(manifest.notes.iter().any(
            |note| note.contains("data-current-page") && note.contains("data-current-nav-page")
        ));
        assert!(
            manifest.notes.iter().any(
                |note| note.contains("nav-page-candidates") && note.contains("nav-page-alerts")
            )
        );
    }

    #[test]
    fn page_request_preserves_candidate_detail_refresh() {
        let request = page_request(" candidate:msft ");

        assert_eq!(request.snapshot_page, Page::Overview);
        assert_eq!(request.selected_page, "candidate:MSFT");
        assert_eq!(request.detail_ticker.as_deref(), Some("MSFT"));
    }

    #[test]
    fn page_request_preserves_alert_detail_refresh() {
        let request = page_request(" Alert:demo-alert-1 ");

        assert_eq!(request.snapshot_page, Page::Alerts);
        assert_eq!(request.selected_page, "alert:demo-alert-1");
        assert_eq!(request.detail_ticker, None);
    }

    #[test]
    fn page_request_canonicalizes_normal_page_aliases() {
        let request = page_request("safe-run");

        assert_eq!(request.snapshot_page, Page::Run);
        assert_eq!(request.selected_page, "run");
        assert_eq!(request.detail_ticker, None);
    }

    #[test]
    fn initial_page_key_preserves_candidate_detail_arg() {
        assert_eq!(initial_page_key(Some(" candidate:msft ")), "candidate:MSFT");
    }

    #[test]
    fn initial_page_key_preserves_alert_detail_arg() {
        assert_eq!(
            initial_page_key(Some(" Alert:demo-alert-1 ")),
            "alert:demo-alert-1"
        );
    }

    #[test]
    fn initial_page_key_canonicalizes_normal_page_aliases() {
        assert_eq!(initial_page_key(Some("safe-run")), "run");
        assert_eq!(initial_page_key(None), "overview");
    }

    #[test]
    fn automation_manifest_exposes_native_computer_use_recipe() {
        let manifest = automation_manifest();

        assert_eq!(manifest.native_window_title, "MarketRadar Command Center");
        assert_eq!(
            manifest.native_executable,
            "target\\release\\radar-desktop.exe"
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "filter-validation-command"
                    && step.expected.contains("Unsupported source-gap value")
                    && step.expected.contains("filter is unchanged"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "numeric-validation-command"
                    && step.expected.contains("Usage: limit 1-200")
                    && step.expected.contains("scan limit is unchanged"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "time-validation-command"
                    && step.expected.contains("Invalid timestamp")
                    && step.expected.contains("available_at is unchanged"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "pagination-boundary-command"
                    && step.expected.contains("Already at the end"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "clear-filters-command"
                    && step.expected.contains("filter-limit remains 25")
                    && step.expected.contains("scan_offset returns to 0"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "optional-filter-clear-command"
                    && step.expected.contains("Usefulness filter cleared"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "guarded-command"
                    && step.expected.contains("source-specific Ops plan"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "source-batch-execute-boundary"
                    && step.expected.contains("--execute-batches 3"))
        );
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "safe-run-execute-command"
                    && step.expected.contains("radar_run telemetry contract"))
        );
        assert!(manifest.keyboard_shortcuts.iter().any(|shortcut| {
            shortcut.contains("action, trigger, ticket, feedback, ledger, and outcome")
                && shortcut.contains("guarded dashboard backend")
        }));
        assert!(manifest.computer_use_steps.iter().any(|step| step.step
            == "local-dashboard-command"
            && step.expected.contains("Local only")
            && step.expected.contains("db_writes=1")
            && step.expected.contains("no provider")));
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "row-open"
                    && step.target == "queue-row"
                    && step.expected.contains("candidate:<TICKER>")
                    && step.expected.contains("nav=candidates")
                    && step.expected.contains("nav=alerts"))
        );
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("Invalid source-gap")
                    && assertion.contains("must not refresh"))
        );
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("Invalid offset")
                    && assertion.contains("must not refresh"))
        );
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("Pagination commands")
                    && assertion.contains("priced_in_queue.total_count"))
        );
        assert!(manifest.zero_call_assertions.iter().any(|assertion| {
            assertion.contains("clear-filters must preserve")
                && assertion.contains("clearing ticker")
        }));
        assert!(manifest.zero_call_assertions.iter().any(|assertion| {
            assertion.contains("Optional usefulness filters")
                && assertion.contains("alert-status and alert-route clear")
        }));
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("Source batch plan commands")
                    && assertion.contains("provider_calls=0"))
        );
        assert!(manifest.zero_call_assertions.iter().any(|assertion| {
            assertion.contains("Local broker, feedback, value-ledger, and outcome commands")
                && assertion.contains("guarded dashboard backend")
                && assertion.contains("provider, OpenAI, broker, order, or external calls")
        }));
        assert!(manifest.notes.iter().any(|note| {
            note.contains("Local broker, feedback, value-ledger, and outcome commands")
                && note.contains("run execute uses the guarded radar-run")
        }));
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("queue rows")
                    && assertion.contains("candidate/alert detail"))
        );
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("Dynamic detail pages")
                    && assertion.contains("nav=<parent workflow page>"))
        );
        assert!(manifest.computer_use_steps.iter().any(|step| {
            step.step == "close-command"
                && step.action.contains("Type q")
                && step
                    .expected
                    .contains("native MarketRadar Command Center window closes")
        }));
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("q, quit, and exit close"))
        );
    }
}
