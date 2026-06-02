use std::env;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use radar_tui::client::{SnapshotFilters, SnapshotRequest, SnapshotSource, fetch_snapshot};
use radar_tui::model::Page;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tauri::State;

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
    notes: Vec<&'static str>,
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

#[tauri::command]
fn desktop_config(state: State<'_, DesktopState>) -> DesktopConfig {
    state.config.clone()
}

#[tauri::command]
fn dashboard_snapshot(
    state: State<'_, DesktopState>,
    input: SnapshotInput,
) -> Result<Value, String> {
    let page = Page::from_input(
        input
            .page
            .as_deref()
            .unwrap_or(state.config.initial_page.as_str()),
    );
    let request = SnapshotRequest {
        page,
        filters: snapshot_filters(input),
    };
    let mut value = fetch_snapshot(&state.source, &request).map_err(|err| err.to_string())?;
    ensure_selected_page(&mut value, page.key());
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

fn main() {
    let args = parse_args(env::args().skip(1));
    let repo_root = find_repo_root().unwrap_or_else(|| env::current_dir().unwrap_or_default());
    let source = snapshot_source(&args, &repo_root);
    let initial_page = Page::from_input(args.page.as_deref().unwrap_or("overview"));
    let config = DesktopConfig {
        app_name: "MarketRadar",
        initial_page: initial_page.key().to_string(),
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
            last_dashboard_snapshot
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
        ],
        keyboard_shortcuts: vec![
            "0-9 jump to numbered workflow pages",
            "Ctrl+A opens Agent",
            "F opens Features",
            "? opens Help",
            "ArrowRight/ArrowDown moves forward",
            "ArrowLeft/ArrowUp moves backward",
            "F5 refreshes the local snapshot",
            "Home opens Start, End opens Help",
            "Esc focuses the command box",
            "Command box accepts safe page, filter, refresh, help, and JSON commands",
        ],
        notes: vec![
            "Every workflow button has role=tab, aria-selected, and a nav-page-* data-testid.",
            "The current page title is exposed through data-testid=page-title.",
            "The selected page and provider-call count are exposed through data-testid=automation-state.",
            "Rows use data-testid=queue-row and include ticker-specific labels when available.",
            "Refreshing reads the existing dashboard JSON contract and makes zero provider calls.",
            "Execute-class commands remain external and require the normal PowerShell command boundary.",
        ],
    }
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
    }

    #[test]
    fn automation_manifest_exposes_command_surface() {
        let manifest = automation_manifest();

        assert!(manifest.landmark_test_ids.contains(&"command-input"));
        assert!(manifest.landmark_test_ids.contains(&"automation-state"));
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("command box"))
        );
    }
}
