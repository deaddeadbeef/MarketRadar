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

const TRADING_WORKBENCH_TITLE: &str = "MarketRadar Trading Workbench";

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
    schema_version: &'static str,
    external_calls_made: u16,
    surfaces: DashboardSurfaces,
    app_name: &'static str,
    initial_page: String,
    source_label: String,
    repo_root: String,
    pages: Vec<PageInfo>,
    platform: TradingPlatformManifest,
    automation: AutomationManifest,
    data_contract: DashboardDataContract,
}

#[derive(Clone, Debug, Serialize)]
struct DashboardSurfaces {
    default: &'static str,
    terminal: &'static str,
    legacy: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct DashboardDataContract {
    snapshot_endpoint: &'static str,
    snapshot_command: &'static str,
    provider_calls_for_browsing: u16,
}

#[derive(Clone, Debug, Serialize)]
struct TradingPlatformManifest {
    schema_version: &'static str,
    name: &'static str,
    primary_tool: &'static str,
    modules: Vec<TradingPlatformModule>,
    execution_boundary: TradingExecutionBoundary,
}

#[derive(Clone, Debug, Serialize)]
struct TradingPlatformModule {
    key: &'static str,
    label: &'static str,
    role: &'static str,
    source: &'static str,
    status: &'static str,
    page: &'static str,
    test_id: &'static str,
    next_action: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct TradingExecutionBoundary {
    live_trading_enabled: bool,
    broker_order_submission: &'static str,
    autonomous_execution: &'static str,
    paper_trading: &'static str,
    provider_calls_for_browsing: u16,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationManifest {
    contract_version: &'static str,
    #[serde(rename = "landmarks")]
    landmark_test_ids: Vec<&'static str>,
    keyboard_shortcuts: Vec<&'static str>,
    command_box_commands: Vec<CommandBoxCommand>,
    automation_recipe: AutomationRecipe,
    native_window_title: &'static str,
    native_executable: &'static str,
    computer_use_steps: Vec<ComputerUseStep>,
    zero_call_assertions: Vec<&'static str>,
    notes: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
struct CommandBoxCommand {
    command: &'static str,
    meaning: &'static str,
    safety: &'static str,
    route: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct ComputerUseStep {
    step: &'static str,
    action: &'static str,
    target: &'static str,
    expected: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationRecipe {
    schema_version: &'static str,
    launch: AutomationRecipeLaunch,
    state_sources: AutomationStateSources,
    expected_json_keys: Vec<&'static str>,
    expected_filter_keys: Vec<&'static str>,
    actions: Vec<AutomationRecipeAction>,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationRecipeLaunch {
    executable: &'static str,
    window_title: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationStateSources {
    page: &'static str,
    filters: &'static str,
    command: &'static str,
    json: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct AutomationRecipeAction {
    id: &'static str,
    input_kind: &'static str,
    input: &'static str,
    target_test_id: &'static str,
    route: &'static str,
    expected_page: Option<&'static str>,
    expected_nav: Option<&'static str>,
    expected_provider_calls: Option<u16>,
    expected_state: Vec<&'static str>,
    requires_review: bool,
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
    print_config_json: bool,
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
    let config = build_desktop_config(&args, &repo_root);

    if args.print_config_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&config).expect("serialize desktop config")
        );
        return;
    }

    tauri::Builder::default()
        .manage(DesktopState {
            config,
            source: snapshot_source(&args, &repo_root),
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
        .expect("error while running MarketRadar Trading Workbench");
}

fn build_desktop_config(args: &DesktopArgs, repo_root: &Path) -> DesktopConfig {
    let source = snapshot_source(args, repo_root);
    DesktopConfig {
        schema_version: "dashboard-ui-manifest-v1",
        external_calls_made: 0,
        surfaces: dashboard_surfaces(),
        app_name: TRADING_WORKBENCH_TITLE,
        initial_page: initial_page_key(args.page.as_deref()),
        source_label: source.label(),
        repo_root: repo_root.display().to_string(),
        pages: page_infos(),
        platform: trading_platform_manifest(),
        automation: automation_manifest(),
        data_contract: dashboard_data_contract(),
    }
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

fn dashboard_surfaces() -> DashboardSurfaces {
    DashboardSurfaces {
        default: "tauri_desktop",
        terminal: "rust_tui",
        legacy: "python_textual",
    }
}

fn dashboard_data_contract() -> DashboardDataContract {
    DashboardDataContract {
        snapshot_endpoint: "/api/dashboard/snapshot?fast=true",
        snapshot_command: "catalyst-radar dashboard-snapshot --json --fast",
        provider_calls_for_browsing: 0,
    }
}

fn trading_platform_manifest() -> TradingPlatformManifest {
    TradingPlatformManifest {
        schema_version: "trading-platform-manifest-v1",
        name: TRADING_WORKBENCH_TITLE,
        primary_tool: "market-radar",
        modules: trading_platform_modules(),
        execution_boundary: TradingExecutionBoundary {
            live_trading_enabled: false,
            broker_order_submission: "disabled",
            autonomous_execution: "disabled",
            paper_trading: "preview_only",
            provider_calls_for_browsing: 0,
        },
    }
}

fn trading_platform_modules() -> Vec<TradingPlatformModule> {
    vec![
        TradingPlatformModule {
            key: "command-center",
            label: "Command Center",
            role: "Operating home for account state, safe action, and agent handoff.",
            source: "local dashboard snapshot",
            status: "active",
            page: "overview",
            test_id: "platform-tool-command-center",
            next_action: "Review the safe action and route work to a tool.",
        },
        TradingPlatformModule {
            key: "portfolio",
            label: "Portfolio",
            role: "Positions, exposure, cash, watch intent, and broker context.",
            source: "read-only broker and local portfolio records",
            status: "route_ready",
            page: "portfolio",
            test_id: "platform-tool-portfolio",
            next_action: "Inspect exposure before any trade plan.",
        },
        TradingPlatformModule {
            key: "market-radar",
            label: "Market Radar",
            role: "Scouted catalysts, mispricing queues, evidence gaps, and watchlists.",
            source: "priced-in queue and catalyst evidence",
            status: "active",
            page: "market-radar",
            test_id: "platform-tool-market-radar",
            next_action: "Open the top evidence row or fill missing sources.",
        },
        TradingPlatformModule {
            key: "candidate-review",
            label: "Candidate Review",
            role: "Single-name evidence queue and candidate packet review.",
            source: "priced-in queue, candidate rows, and decision cards",
            status: "route_ready",
            page: "candidates",
            test_id: "platform-tool-candidate-review",
            next_action: "Open a candidate row and review evidence before planning.",
        },
        TradingPlatformModule {
            key: "decision-review",
            label: "Decision Review",
            role: "Priced-in answer status and decision-readiness gates.",
            source: "priced-in answer, trust blockers, and queue rows",
            status: "route_ready",
            page: "review",
            test_id: "platform-tool-decision-review",
            next_action: "Resolve decision-readiness blockers before acting.",
        },
        TradingPlatformModule {
            key: "evidence-gaps",
            label: "Evidence Gaps",
            role: "Readiness blockers, source gaps, and reliance gates.",
            source: "radar readiness and evidence-gate payloads",
            status: "route_ready",
            page: "readiness",
            test_id: "platform-tool-evidence-gaps",
            next_action: "Clear readiness blockers before relying on platform output.",
        },
        TradingPlatformModule {
            key: "safe-run",
            label: "Safe Run",
            role: "Provider-call budget, run guardrails, and execution gates.",
            source: "call plan and run guardrails",
            status: "route_ready",
            page: "run",
            test_id: "platform-tool-safe-run",
            next_action: "Review the call plan before any run execute command.",
        },
        TradingPlatformModule {
            key: "trade-planner",
            label: "Trade Planner",
            role: "Candidate sizing, thesis, reward/risk, and decision-card assembly.",
            source: "decision cards and validation evidence",
            status: "route_ready",
            page: "trade-planner",
            test_id: "platform-tool-trade-planner",
            next_action: "Draft a plan from a decision-ready candidate.",
        },
        TradingPlatformModule {
            key: "risk-desk",
            label: "Risk Desk",
            role: "Policy gates, portfolio impact, concentration, and hard blocks.",
            source: "policy scan, broker context, and validation artifacts",
            status: "route_ready",
            page: "risk-desk",
            test_id: "platform-tool-risk-desk",
            next_action: "Resolve hard blocks before paper or live consideration.",
        },
        TradingPlatformModule {
            key: "paper-trading",
            label: "Paper Trading",
            role: "Paper-only tickets, fills, outcomes, and shadow validation.",
            source: "paper trades and value outcomes",
            status: "preview_only",
            page: "paper-trading",
            test_id: "platform-tool-paper-trading",
            next_action: "Use paper execution only after risk approval.",
        },
        TradingPlatformModule {
            key: "broker-desk",
            label: "Broker Desk",
            role: "Read-only broker connection, order-ticket previews, and sync boundaries.",
            source: "broker snapshot and local order-ticket records",
            status: "read_only",
            page: "broker",
            test_id: "platform-tool-broker-desk",
            next_action: "Authenticate only for portfolio context; order submission is disabled.",
        },
        TradingPlatformModule {
            key: "backtest",
            label: "Backtest / Replay",
            role: "Historical replay, shadow-mode validation, and strategy evidence.",
            source: "validation runs and backtest artifacts",
            status: "route_ready",
            page: "backtest",
            test_id: "platform-tool-backtest",
            next_action: "Compare candidate logic against replay evidence.",
        },
        TradingPlatformModule {
            key: "validation",
            label: "Validation",
            role: "Shadow, paper, and useful-alert validation evidence.",
            source: "validation runs, reports, and useful labels",
            status: "route_ready",
            page: "validation",
            test_id: "platform-tool-validation",
            next_action: "Review validation evidence before trusting strategy changes.",
        },
        TradingPlatformModule {
            key: "alerts",
            label: "Alerts",
            role: "Research notifications, watch triggers, and operator routing.",
            source: "local alert rows",
            status: "active",
            page: "alerts",
            test_id: "platform-tool-alerts",
            next_action: "Open an alert as research context, not trade approval.",
        },
        TradingPlatformModule {
            key: "ipo-s1",
            label: "IPO/S-1",
            role: "Primary-source IPO registration evidence and risk flags.",
            source: "SEC filing rows and local IPO analysis",
            status: "route_ready",
            page: "ipo",
            test_id: "platform-tool-ipo-s1",
            next_action: "Review S-1 terms before adding IPO evidence to a thesis.",
        },
        TradingPlatformModule {
            key: "themes",
            label: "Themes",
            role: "Clustered catalyst patterns and repeated theme context.",
            source: "local candidate feature metadata",
            status: "route_ready",
            page: "themes",
            test_id: "platform-tool-themes",
            next_action: "Compare theme concentration before selecting a ticker.",
        },
        TradingPlatformModule {
            key: "features",
            label: "Features",
            role: "Feature inventory, evidence routing, and platform surface coverage.",
            source: "local feature inventory",
            status: "route_ready",
            page: "features",
            test_id: "platform-tool-features",
            next_action: "Use the inventory to route work to the right local module.",
        },
        TradingPlatformModule {
            key: "costs",
            label: "Costs",
            role: "Budget ledger, provider spend, and decision-support value.",
            source: "budget ledger and local value report",
            status: "route_ready",
            page: "costs",
            test_id: "platform-tool-costs",
            next_action: "Compare spend with attributed decision-support value.",
        },
        TradingPlatformModule {
            key: "ops",
            label: "Ops",
            role: "Provider health, runtime context, and execution readiness.",
            source: "ops health, runtime context, and call plan",
            status: "route_ready",
            page: "ops",
            test_id: "platform-tool-ops",
            next_action: "Resolve runtime blockers before expanding agent autonomy.",
        },
        TradingPlatformModule {
            key: "telemetry",
            label: "Telemetry",
            role: "Audit tape, coverage gaps, and agent action traceability.",
            source: "local audit events and telemetry coverage",
            status: "route_ready",
            page: "telemetry",
            test_id: "platform-tool-telemetry",
            next_action: "Review telemetry before relying on automation.",
        },
        TradingPlatformModule {
            key: "journal",
            label: "Journal",
            role: "Decision notes, feedback, value ledger, and outcome review.",
            source: "local feedback and value ledger records",
            status: "route_ready",
            page: "journal",
            test_id: "platform-tool-journal",
            next_action: "Record feedback and outcome evidence locally.",
        },
        TradingPlatformModule {
            key: "agent-cockpit",
            label: "Agent Cockpit",
            role: "Agent brief, proposed tool use, budget gates, and execution review.",
            source: "agent brief and runtime context",
            status: "preview_only",
            page: "agent",
            test_id: "platform-tool-agent-cockpit",
            next_action: "Preview agent reasoning; execute remains gated.",
        },
    ]
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
            "--print-config-json" => parsed.print_config_json = true,
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
        Page::Portfolio => "portfolio",
        Page::MarketRadar => "radar",
        Page::TradePlanner => "planner",
        Page::RiskDesk => "risk",
        Page::PaperTrading => "paper",
        Page::Backtest => "replay",
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
        Page::Journal => "journal",
        Page::Help => "?",
    }
}

fn page_description(page: Page) -> &'static str {
    match page {
        Page::Tutorial => "First-run path and safe operating boundary.",
        Page::Overview => "Trading workbench command center, account state, and next safe action.",
        Page::Portfolio => "Positions, exposure, cash, and portfolio context.",
        Page::MarketRadar => "MarketRadar catalyst scout, mispricing queue, and evidence gaps.",
        Page::TradePlanner => "Trade thesis, sizing, reward/risk, and decision-card planning.",
        Page::RiskDesk => "Policy gates, portfolio impact, concentration, and hard blocks.",
        Page::PaperTrading => "Paper-only tickets, fills, and shadow validation.",
        Page::Backtest => "Replay, backtest, and validation evidence.",
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
        Page::Journal => "Decision journal, feedback, value ledger, and outcome review.",
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
            "command-state",
            "automation-state",
            "automation-json",
            "filter-state",
            "attention-queue",
            "loading-dashboard",
            "loading-metric-strip",
            "loading-preview-queue",
            "next-safe-action",
            "keys-panel",
            "keys-list",
            "snapshot-panel",
            "snapshot-source",
            "snapshot-refresh",
            "snapshot-page",
            "snapshot-mode",
            "snapshot-json",
            "snapshot-json-output",
        ],
        keyboard_shortcuts: vec![
            "0-9 jump to numbered workflow pages",
            "Ctrl+A opens Agent",
            "Ctrl+N moves forward; Ctrl+P moves backward",
            "Type themes or validation to open evidence pages",
            "V opens Costs",
            "F opens Features",
            "? opens Help",
            "ArrowRight/ArrowDown/Tab/J moves forward",
            "ArrowLeft/ArrowUp/Shift+Tab/K moves backward",
            "F5 or R refreshes the local snapshot",
            "Home opens Start, End opens Help",
            "Esc focuses the command box",
            "next and prev page through scan rows without walking past the end",
            "clear-filters resets filters while preserving the row limit",
            "usefulness clears with all, any, none, or blank; alert filters clear with all, none, or blank",
            "Command box accepts safe page, filter, refresh, help, and JSON commands",
            "offset, limit, and available-at commands reject invalid values before refreshing",
            "source-gap and decision-gap commands reject unsupported values before refreshing",
            "ready applies the decision-ready scan filter; review opens the Review page",
            "batch SOURCE opens an Ops source plan; batch SOURCE all and batch SOURCE execute N show PowerShell boundaries",
            "run opens Safe Run; run execute starts the guarded radar-run API/CLI backend path",
            "action, trigger, ticket, feedback, paper-decision, order-ticket, ledger, and outcome commands use the guarded dashboard backend for local DB-only operations",
            "agent, bars, options, and cik/sec planning commands use the guarded dashboard backend for preview/status output; execute and confirm variants stay external boundaries",
            "Q closes the native desktop window; q, quit, or exit also close from the command box",
            "Full catalyst-radar commands show a PowerShell boundary instead of executing in-app",
        ],
        command_box_commands: command_box_commands(),
        automation_recipe: automation_recipe(),
        native_window_title: TRADING_WORKBENCH_TITLE,
        native_executable: "target\\release\\radar-desktop.exe",
        computer_use_steps: computer_use_steps(),
        zero_call_assertions: vec![
            "Dashboard browsing, command-box navigation, filtering, copy, and raw JSON inspection must leave provider_calls=0.",
            "Local broker, feedback, paper-decision, order-ticket, value-ledger, and outcome commands may write the local DB through the guarded dashboard backend, but must not make provider, OpenAI, broker, order, or external calls unless the command explicitly reports an external-call budget.",
            "Agent, market-bar, options, and SEC CIK preview/status commands may use the dashboard backend, but execute or confirm variants must remain external PowerShell boundaries unless the backend command explicitly reports an accepted external-call budget.",
            "Source batch plan commands may read the current snapshot, but execute variants must remain external PowerShell boundaries and leave provider_calls=0.",
            "Invalid source-gap or decision-gap filter commands must not refresh the snapshot or change filters.",
            "Invalid offset, limit, or available-at commands must not refresh the snapshot or change filters.",
            "ready must update filter-state to scan_mode=actionable and usefulness=decision_useful while opening Review without provider calls; review must open the Review page without changing filters.",
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
            "The latest command, current page/nav, provider-call count, and command result are exposed through data-testid=command-state.",
            "The exact selected page, parent nav page, and provider-call count are exposed through data-testid=automation-state.",
            "The aggregate automation state is exposed as machine-readable JSON through data-testid=automation-json.",
            "The active ticker, scan, availability, alert, source-gap, decision-gap, usefulness, limit, and offset filters are exposed through data-testid=filter-state.",
            "The dashboard main region exposes data-current-page and data-current-nav-page for dynamic detail pages.",
            "Before the first snapshot loads, the main region exposes loading-dashboard, loading-metric-strip, and loading-preview-queue instead of a blank box.",
            "The right rail exposes keys-panel and snapshot-panel, including snapshot-source, snapshot-refresh, snapshot-page, and snapshot-mode.",
            "Candidate detail pages keep nav-page-candidates selected; alert detail pages keep nav-page-alerts selected.",
            "Rows use data-testid=queue-row, are keyboard focusable, and include ticker-specific labels when available.",
            "Refreshing reads the existing dashboard JSON contract and makes zero provider calls.",
            "Local broker, feedback, paper-decision, order-ticket, value-ledger, and outcome commands use the guarded dashboard backend; source-batch execute and provider execute/confirm commands remain external PowerShell boundaries; provider preview/status commands use the guarded dashboard backend; run execute uses the guarded radar-run API/CLI backend path.",
        ],
    }
}

fn command_box_commands() -> Vec<CommandBoxCommand> {
    vec![
        CommandBoxCommand {
            command: "0..9, Ctrl+A, Ctrl+N/P, Tab, J/K, V, F, ?, or page name",
            meaning: "Switch pages; Ctrl+A opens Agent and V opens Costs.",
            safety: "zero_provider_calls",
            route: "local_navigation",
        },
        CommandBoxCommand {
            command: "themes / validation / costs / features",
            meaning: "Open local evidence pages for clustered themes, validation, costs, and feature inventory.",
            safety: "zero_provider_calls",
            route: "local_navigation",
        },
        CommandBoxCommand {
            command: "setup / first",
            meaning: "Show the first setup command and where to run it.",
            safety: "zero_provider_calls",
            route: "local_navigation",
        },
        CommandBoxCommand {
            command: "open #|TICKER",
            meaning: "Open a row from Candidate Review or show its next command.",
            safety: "zero_provider_calls",
            route: "local_detail",
        },
        CommandBoxCommand {
            command: "ticker SYMBOL|all",
            meaning: "Filter ticker-aware pages.",
            safety: "zero_provider_calls",
            route: "snapshot_refresh",
        },
        CommandBoxCommand {
            command: "available-at ISO|latest",
            meaning: "Set or clear the point-in-time cutoff.",
            safety: "zero_provider_calls",
            route: "snapshot_refresh",
        },
        CommandBoxCommand {
            command: "ready / full / mismatches / stocks",
            meaning: "Switch between decision-useful, full universe, mismatch, and stock-only scan views.",
            safety: "zero_provider_calls",
            route: "local_filter",
        },
        CommandBoxCommand {
            command: "usefulness STATUS|all",
            meaning: "Filter Inbox by usefulness verdict.",
            safety: "zero_provider_calls",
            route: "local_filter",
        },
        CommandBoxCommand {
            command: "source-gap SOURCE|all",
            meaning: "Filter Inbox by missing or stale source evidence.",
            safety: "zero_provider_calls",
            route: "local_filter",
        },
        CommandBoxCommand {
            command: "decision-gap GAP|all",
            meaning: "Filter Inbox by missing decision evidence.",
            safety: "zero_provider_calls",
            route: "local_filter",
        },
        CommandBoxCommand {
            command: "next / prev / offset ROW / limit 1-200",
            meaning: "Page through current Inbox scan rows.",
            safety: "zero_provider_calls",
            route: "local_pagination",
        },
        CommandBoxCommand {
            command: "export full / export current",
            meaning: "Show JSON export commands without running them.",
            safety: "external_boundary",
            route: "command_preview",
        },
        CommandBoxCommand {
            command: "batch SOURCE / batch SOURCE all / batch SOURCE execute 3",
            meaning: "Plan source fills or show the external execution boundary.",
            safety: "plan_only_execute_external",
            route: "local_plan",
        },
        CommandBoxCommand {
            command: "catalyst-radar COMMAND",
            meaning: "Show where to run full CLI commands without executing them in the dashboard.",
            safety: "external_boundary",
            route: "powershell_boundary",
        },
        CommandBoxCommand {
            command: "bars manual template/import",
            meaning: "Preview market-bar repair commands through the dashboard backend; execute stays external.",
            safety: "preview_only_execute_external",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "bars saved capture/validate/import",
            meaning: "Preview saved grouped-daily commands through the dashboard backend; confirm/execute stays external.",
            safety: "preview_only_confirm_execute_external",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "options template/validate/import",
            meaning: "Preview point-in-time options commands through the dashboard backend; execute stays external.",
            safety: "preview_only_execute_external",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "cik template/validate/import",
            meaning: "Preview SEC CIK override commands through the dashboard backend; execute stays external.",
            safety: "preview_only_execute_external",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "agent / agent execute",
            meaning: "Preview agent gates through the dashboard backend; execute stays external.",
            safety: "preview_only_execute_external",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "alert-status STATUS|all / alert-route ROUTE|all",
            meaning: "Filter alerts.",
            safety: "zero_provider_calls",
            route: "local_filter",
        },
        CommandBoxCommand {
            command: "run / run execute",
            meaning: "Open Safe Run or show the capped run execution boundary.",
            safety: "guarded_execution",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "action / trigger / ticket / feedback",
            meaning: "Run guarded local Broker or Alert commands through the dashboard backend.",
            safety: "local_db_only",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "order-ticket preview / record",
            meaning: "Preview or save the active trading plan as a blocked local broker ticket; broker order submission stays disabled.",
            safety: "local_db_only_no_broker_order",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "paper-decision preview / execute",
            meaning: "Preview or record the active trading plan as a local paper decision; broker order submission stays disabled.",
            safety: "local_db_only_no_broker_order",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "ledger coverage / record",
            meaning: "Run guarded local value-ledger commands through the dashboard backend.",
            safety: "local_db_only",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "outcome coverage / update",
            meaning: "Run guarded local value-outcome commands through the dashboard backend.",
            safety: "local_db_only",
            route: "dashboard_backend",
        },
        CommandBoxCommand {
            command: "json",
            meaning: "Open and focus the raw JSON snapshot.",
            safety: "zero_provider_calls",
            route: "local_snapshot_view",
        },
        CommandBoxCommand {
            command: "clear-filters / refresh / q",
            meaning: "Reset filters, reload, or close the native window.",
            safety: "zero_provider_calls",
            route: "local_navigation",
        },
    ]
}

fn automation_recipe() -> AutomationRecipe {
    AutomationRecipe {
        schema_version: "dashboard-computer-use-recipe-v1",
        launch: AutomationRecipeLaunch {
            executable: "target\\release\\radar-desktop.exe",
            window_title: TRADING_WORKBENCH_TITLE,
        },
        state_sources: AutomationStateSources {
            page: "automation-state",
            filters: "filter-state",
            command: "command-state",
            json: "automation-json",
        },
        expected_json_keys: vec![
            "contract_version",
            "page",
            "nav",
            "status",
            "provider_calls",
            "last_command",
            "filters",
        ],
        expected_filter_keys: vec![
            "ticker",
            "scan_mode",
            "stocks_only",
            "limit",
            "offset",
            "usefulness",
            "source_gap",
            "decision_gap",
            "available_at",
            "alert_status",
            "alert_route",
        ],
        actions: vec![
            AutomationRecipeAction {
                id: "focus-command",
                input_kind: "key",
                input: "Escape",
                target_test_id: "command-input",
                route: "local_navigation",
                expected_page: Some("overview"),
                expected_nav: Some("overview"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains command box focused",
                    "automation-json.provider_calls=0",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "filter-ticker",
                input_kind: "command",
                input: "ticker MSFT",
                target_test_id: "command-input",
                route: "snapshot_refresh",
                expected_page: Some("overview"),
                expected_nav: Some("overview"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "automation-json.filters.ticker=MSFT",
                    "filter-state contains ticker=MSFT",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "reject-invalid-source-gap",
                input_kind: "command",
                input: "source-gap nonsense",
                target_test_id: "command-input",
                route: "local_filter_validation",
                expected_page: Some("overview"),
                expected_nav: Some("overview"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains Unsupported source-gap value",
                    "automation-json.filters.source_gap remains unchanged",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "ready-review-filter",
                input_kind: "command",
                input: "ready",
                target_test_id: "command-input",
                route: "local_filter",
                expected_page: Some("review"),
                expected_nav: Some("review"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "automation-json.filters.scan_mode=actionable",
                    "automation-json.filters.usefulness=decision_useful",
                    "filter-state contains scan_mode=actionable",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "open-review-page",
                input_kind: "command",
                input: "review",
                target_test_id: "command-input",
                route: "local_navigation",
                expected_page: Some("review"),
                expected_nav: Some("review"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "dashboard-page data-current-page=review",
                    "automation-json.page=review",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "open-row",
                input_kind: "command",
                input: "open 1",
                target_test_id: "command-input",
                route: "local_detail",
                expected_page: Some("candidate:<TICKER>|alert:<ID>"),
                expected_nav: Some("candidates|alerts"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "dashboard-page exposes candidate-detail or alert-detail",
                    "automation-json.nav is candidates or alerts",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "source-plan",
                input_kind: "command",
                input: "batch catalyst_events",
                target_test_id: "command-input",
                route: "local_plan",
                expected_page: Some("ops"),
                expected_nav: Some("ops"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains source plan",
                    "automation-json.provider_calls=0",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "source-execute-boundary",
                input_kind: "command",
                input: "batch catalyst_events execute 3",
                target_test_id: "command-input",
                route: "powershell_boundary",
                expected_page: Some("ops"),
                expected_nav: Some("ops"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains --execute-batches 3",
                    "command-state contains provider_calls=0 in the desktop app",
                ],
                requires_review: true,
            },
            AutomationRecipeAction {
                id: "provider-preview",
                input_kind: "command",
                input: "bars status",
                target_test_id: "command-input",
                route: "dashboard_backend",
                expected_page: Some("run"),
                expected_nav: Some("run"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains Market-bar status",
                    "automation-json.provider_calls=0",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "order-ticket-command",
                input_kind: "command",
                input: "order-ticket preview",
                target_test_id: "command-input",
                route: "dashboard_backend",
                expected_page: Some("broker"),
                expected_nav: Some("broker"),
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state contains order_ticket",
                    "command-state contains broker_order_submitted=false",
                    "automation-json.provider_calls=0",
                ],
                requires_review: true,
            },
            AutomationRecipeAction {
                id: "safe-run-execute",
                input_kind: "command",
                input: "run execute",
                target_test_id: "command-input",
                route: "guarded_dashboard_backend",
                expected_page: Some("run"),
                expected_nav: Some("run"),
                expected_provider_calls: None,
                expected_state: vec![
                    "command-state contains Radar run finished, blocked, or rate limited",
                    "backend result includes radar_run telemetry",
                ],
                requires_review: true,
            },
            AutomationRecipeAction {
                id: "powershell-boundary",
                input_kind: "command",
                input: "catalyst-radar priced-in-queue --full-scan --all --json",
                target_test_id: "command-input",
                route: "powershell_boundary",
                expected_page: None,
                expected_nav: None,
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "command-state says PowerShell command, not a dashboard command",
                    "automation-json.provider_calls=0",
                ],
                requires_review: true,
            },
            AutomationRecipeAction {
                id: "open-json",
                input_kind: "command",
                input: "json",
                target_test_id: "command-input",
                route: "local_snapshot_view",
                expected_page: None,
                expected_nav: None,
                expected_provider_calls: Some(0),
                expected_state: vec![
                    "snapshot-json-output is focused",
                    "automation-json remains parseable",
                ],
                requires_review: false,
            },
            AutomationRecipeAction {
                id: "close-window",
                input_kind: "command",
                input: "q",
                target_test_id: "command-input",
                route: "local_window_control",
                expected_page: None,
                expected_nav: None,
                expected_provider_calls: Some(0),
                expected_state: vec!["native MarketRadar Trading Workbench window closes"],
                requires_review: true,
            },
        ],
    }
}

fn computer_use_steps() -> Vec<ComputerUseStep> {
    vec![
        ComputerUseStep {
            step: "launch",
            action: "Launch the app by executable path through Computer Use, then select the returned window object.",
            target: "target\\release\\radar-desktop.exe",
            expected: "A native window titled MarketRadar Trading Workbench is targetable.",
        },
        ComputerUseStep {
            step: "capture",
            action: "Capture screenshot and accessibility text for the selected window.",
            target: "MarketRadar Trading Workbench",
            expected: "The window exposes MarketRadar workflow tabs, dashboard-page, command-input, command-state, automation-state, automation-json, filter-state, loading-dashboard before first data, next-safe-action, keys-panel, snapshot-panel, page=<PAGE>, nav=<WORKFLOW_PAGE>, snapshot-page=<PAGE>, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "focus-command",
            action: "Press Escape in the dashboard window.",
            target: "command-input",
            expected: "The command box receives focus and command-state reports command box focused.",
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
            step: "ready-filter-command",
            action: "Type ready and press Return.",
            target: "filter-state",
            expected: "filter-state reports scan_mode=actionable and usefulness=decision_useful, automation-json reports last_command=ready, filters.scan_mode=actionable, filters.usefulness=decision_useful, page=review, and provider_calls=0.",
        },
        ComputerUseStep {
            step: "page-command",
            action: "Type review and press Return.",
            target: "command-input",
            expected: "dashboard-page reports page=review, the selected tab is Review, filter-state is still exposed, and provider_calls=0.",
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
            step: "paper-decision-command",
            action: "Type paper-decision preview and press Return, then type paper-decision execute only after reviewing the active plan.",
            target: "command-input",
            expected: "dashboard-page reports page=paper-trading, command-status reports paper_decision, external_calls=0, no_execution=true, broker_order_submitted=false, and any DB write is a local paper-trade/audit record only.",
        },
        ComputerUseStep {
            step: "order-ticket-command",
            action: "Type order-ticket preview and press Return, then type order-ticket record only after reviewing the active plan.",
            target: "command-input",
            expected: "dashboard-page reports page=broker, command-status reports order_ticket, external_calls=0, no_execution=true, broker_order_submitted=false, and any DB write is a local blocked ticket only.",
        },
        ComputerUseStep {
            step: "provider-preview-command",
            action: "Type bars status and press Return.",
            target: "command-input",
            expected: "dashboard-page reports page=run, command-status reports Market-bar status from the dashboard backend, and provider_calls=0 after refresh.",
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
            expected: "The native MarketRadar Trading Workbench window closes without provider, OpenAI, broker, or DB-write actions.",
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
    fn parse_args_recognizes_headless_desktop_config_export() {
        let args = parse_args(
            [
                "--print-config-json",
                "--page",
                "alerts",
                "--snapshot-command",
                "catalyst-radar dashboard-snapshot --json --fast",
            ]
            .into_iter()
            .map(str::to_string),
        );

        assert!(args.print_config_json);
        assert_eq!(args.page.as_deref(), Some("alerts"));
        assert_eq!(
            args.snapshot_command.as_deref(),
            Some("catalyst-radar dashboard-snapshot --json --fast")
        );
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
        assert!(manifest.landmark_test_ids.contains(&"command-state"));
        assert!(manifest.landmark_test_ids.contains(&"automation-state"));
        assert!(manifest.landmark_test_ids.contains(&"automation-json"));
        assert!(manifest.landmark_test_ids.contains(&"filter-state"));
        assert!(manifest.landmark_test_ids.contains(&"loading-dashboard"));
        assert!(manifest.landmark_test_ids.contains(&"loading-metric-strip"));
        assert!(
            manifest
                .landmark_test_ids
                .contains(&"loading-preview-queue")
        );
        assert!(manifest.landmark_test_ids.contains(&"keys-panel"));
        assert!(manifest.landmark_test_ids.contains(&"keys-list"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-panel"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-source"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-refresh"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-page"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-mode"));
        assert!(manifest.landmark_test_ids.contains(&"snapshot-json-output"));
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("command box"))
        );
        assert!(manifest.keyboard_shortcuts.iter().any(|shortcut| {
            shortcut.contains("Ctrl+N moves forward") && shortcut.contains("Ctrl+P moves backward")
        }));
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("Tab/J moves forward"))
        );
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("Shift+Tab/K moves backward"))
        );
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("F5 or R refreshes"))
        );
        assert!(
            manifest
                .keyboard_shortcuts
                .iter()
                .any(|shortcut| shortcut.contains("Q closes the native desktop window"))
        );
        assert!(manifest.keyboard_shortcuts.iter().any(|shortcut| {
            shortcut.contains("ready applies the decision-ready scan filter")
                && shortcut.contains("review opens the Review page")
        }));
        assert!(manifest.command_box_commands.iter().any(|command| {
            command.command == "bars saved capture/validate/import"
                && command.safety == "preview_only_confirm_execute_external"
                && command.route == "dashboard_backend"
        }));
        assert!(manifest.command_box_commands.iter().any(|command| {
            command.command == "action / trigger / ticket / feedback"
                && command.safety == "local_db_only"
                && command.route == "dashboard_backend"
        }));
        assert!(manifest.command_box_commands.iter().any(|command| {
            command.command == "paper-decision preview / execute"
                && command.safety == "local_db_only_no_broker_order"
                && command.route == "dashboard_backend"
        }));
        assert!(manifest.command_box_commands.iter().any(|command| {
            command.command == "order-ticket preview / record"
                && command.safety == "local_db_only_no_broker_order"
                && command.route == "dashboard_backend"
        }));
        assert!(manifest.command_box_commands.iter().any(|command| {
            command.command == "catalyst-radar COMMAND"
                && command.safety == "external_boundary"
                && command.route == "powershell_boundary"
        }));
        assert!(manifest.notes.iter().any(
            |note| note.contains("data-current-page") && note.contains("data-current-nav-page")
        ));
        assert!(manifest.notes.iter().any(|note| {
            note.contains("data-testid=automation-json") && note.contains("machine-readable JSON")
        }));
        assert!(manifest.notes.iter().any(|note| {
            note.contains("data-testid=command-state")
                && note.contains("latest command")
                && note.contains("provider-call count")
        }));
        assert!(manifest.notes.iter().any(|note| {
            note.contains("data-testid=filter-state")
                && note.contains("ticker")
                && note.contains("offset")
        }));
        assert!(
            manifest.notes.iter().any(
                |note| note.contains("nav-page-candidates") && note.contains("nav-page-alerts")
            )
        );
        assert!(manifest.notes.iter().any(|note| {
            note.contains("loading-dashboard")
                && note.contains("loading-metric-strip")
                && note.contains("loading-preview-queue")
        }));
        assert!(manifest.notes.iter().any(|note| {
            note.contains("keys-panel")
                && note.contains("snapshot-panel")
                && note.contains("snapshot-source")
                && note.contains("snapshot-page")
        }));
    }

    #[test]
    fn automation_manifest_serializes_api_compatible_landmarks_key() {
        let manifest = automation_manifest();
        let payload = serde_json::to_value(&manifest).expect("serialize automation manifest");

        assert!(payload.get("landmarks").is_some());
        assert!(payload.get("landmark_test_ids").is_none());
        assert!(
            payload["landmarks"]
                .as_array()
                .expect("landmarks array")
                .iter()
                .any(|value| value == "automation-json")
        );
    }

    #[test]
    fn desktop_config_serializes_api_manifest_compatible_top_level_fields() {
        let args = DesktopArgs {
            snapshot_command: Some("catalyst-radar dashboard-snapshot --json --fast".to_string()),
            ..DesktopArgs::default()
        };
        let config = build_desktop_config(&args, Path::new("C:\\repo\\MarketRadar"));
        let payload = serde_json::to_value(&config).expect("serialize desktop config");

        assert_eq!(payload["schema_version"], "dashboard-ui-manifest-v1");
        assert_eq!(payload["external_calls_made"], 0);
        assert_eq!(payload["app_name"], TRADING_WORKBENCH_TITLE);
        assert_eq!(payload["initial_page"], "overview");
        assert_eq!(
            payload["source_label"],
            "command catalyst-radar dashboard-snapshot --json --fast"
        );
        assert_eq!(payload["repo_root"], "C:\\repo\\MarketRadar");
        assert_eq!(payload["surfaces"]["default"], "tauri_desktop");
        assert_eq!(payload["surfaces"]["terminal"], "rust_tui");
        assert_eq!(payload["surfaces"]["legacy"], "python_textual");
        assert_eq!(
            payload["data_contract"]["snapshot_endpoint"],
            "/api/dashboard/snapshot?fast=true"
        );
        assert_eq!(
            payload["data_contract"]["snapshot_command"],
            "catalyst-radar dashboard-snapshot --json --fast"
        );
        assert_eq!(payload["data_contract"]["provider_calls_for_browsing"], 0);
        assert_eq!(
            payload["platform"]["schema_version"],
            "trading-platform-manifest-v1"
        );
        assert_eq!(payload["platform"]["primary_tool"], "market-radar");
        assert_eq!(
            payload["platform"]["execution_boundary"]["live_trading_enabled"],
            false
        );
        assert_eq!(
            payload["platform"]["execution_boundary"]["broker_order_submission"],
            "disabled"
        );
        assert!(
            payload["platform"]["modules"]
                .as_array()
                .expect("platform modules")
                .iter()
                .any(|module| module["key"] == "trade-planner"
                    && module["page"] == "trade-planner")
        );
        assert!(payload["automation"]["landmarks"].is_array());
        assert_eq!(
            payload["automation"]["automation_recipe"]["schema_version"],
            "dashboard-computer-use-recipe-v1"
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

        let platform_request = page_request("trade-planner");
        assert_eq!(platform_request.snapshot_page, Page::TradePlanner);
        assert_eq!(platform_request.selected_page, "trade-planner");
        assert_eq!(platform_request.detail_ticker, None);
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

        assert_eq!(manifest.native_window_title, TRADING_WORKBENCH_TITLE);
        assert_eq!(
            manifest.native_executable,
            "target\\release\\radar-desktop.exe"
        );
        let recipe = &manifest.automation_recipe;
        assert_eq!(recipe.schema_version, "dashboard-computer-use-recipe-v1");
        assert_eq!(recipe.launch.window_title, TRADING_WORKBENCH_TITLE);
        assert_eq!(
            recipe.launch.executable,
            "target\\release\\radar-desktop.exe"
        );
        assert_eq!(recipe.state_sources.page, "automation-state");
        assert_eq!(recipe.state_sources.filters, "filter-state");
        assert_eq!(recipe.state_sources.command, "command-state");
        assert_eq!(recipe.state_sources.json, "automation-json");
        assert!(recipe.expected_json_keys.contains(&"provider_calls"));
        assert!(recipe.expected_json_keys.contains(&"filters"));
        assert!(recipe.expected_filter_keys.contains(&"source_gap"));
        assert!(recipe.expected_filter_keys.contains(&"decision_gap"));
        let recipe_action = |id: &str| {
            recipe
                .actions
                .iter()
                .find(|action| action.id == id)
                .unwrap_or_else(|| panic!("missing automation recipe action {id}"))
        };
        let focus_command = recipe_action("focus-command");
        assert_eq!(focus_command.input_kind, "key");
        assert_eq!(focus_command.input, "Escape");
        assert_eq!(focus_command.expected_provider_calls, Some(0));
        let filter_ticker = recipe_action("filter-ticker");
        assert_eq!(filter_ticker.input, "ticker MSFT");
        assert_eq!(filter_ticker.expected_page, Some("overview"));
        assert!(
            filter_ticker
                .expected_state
                .contains(&"automation-json.filters.ticker=MSFT")
        );
        let ready_filter = recipe_action("ready-review-filter");
        assert_eq!(ready_filter.expected_page, Some("review"));
        assert_eq!(ready_filter.expected_nav, Some("review"));
        assert!(
            ready_filter
                .expected_state
                .contains(&"automation-json.filters.usefulness=decision_useful")
        );
        let source_boundary = recipe_action("source-execute-boundary");
        assert_eq!(source_boundary.route, "powershell_boundary");
        assert_eq!(source_boundary.expected_provider_calls, Some(0));
        assert!(source_boundary.requires_review);
        let provider_preview = recipe_action("provider-preview");
        assert_eq!(provider_preview.route, "dashboard_backend");
        assert_eq!(provider_preview.expected_provider_calls, Some(0));
        let run_execute = recipe_action("safe-run-execute");
        assert_eq!(run_execute.route, "guarded_dashboard_backend");
        assert_eq!(run_execute.expected_provider_calls, None);
        assert!(run_execute.requires_review);
        let powershell_boundary = recipe_action("powershell-boundary");
        assert_eq!(powershell_boundary.route, "powershell_boundary");
        assert!(powershell_boundary.requires_review);
        assert!(
            recipe_action("open-json")
                .expected_state
                .contains(&"automation-json remains parseable")
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
        assert!(manifest.computer_use_steps.iter().any(|step| {
            step.step == "ready-filter-command"
                && step.target == "filter-state"
                && step.expected.contains("scan_mode=actionable")
                && step.expected.contains("usefulness=decision_useful")
                && step.expected.contains("last_command=ready")
                && step.expected.contains("filters.scan_mode=actionable")
                && step.expected.contains("filters.usefulness=decision_useful")
                && step.expected.contains("page=review")
        }));
        assert!(manifest.computer_use_steps.iter().any(|step| {
            step.step == "page-command"
                && step.action.contains("Type review")
                && step.expected.contains("page=review")
                && step.expected.contains("filter-state")
                && step.expected.contains("provider_calls=0")
        }));
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
            shortcut
                .contains("action, trigger, ticket, feedback, paper-decision, order-ticket, ledger, and outcome")
                && shortcut.contains("guarded dashboard backend")
        }));
        assert!(manifest.keyboard_shortcuts.iter().any(|shortcut| {
            shortcut.contains("agent, bars, options, and cik/sec planning commands")
                && shortcut.contains("preview/status output")
                && shortcut.contains("execute and confirm variants")
        }));
        assert!(manifest.computer_use_steps.iter().any(|step| step.step
            == "local-dashboard-command"
            && step.expected.contains("Local only")
            && step.expected.contains("db_writes=1")
            && step.expected.contains("no provider")));
        assert!(manifest.computer_use_steps.iter().any(|step| step.step
            == "paper-decision-command"
            && step.expected.contains("paper_decision")
            && step.expected.contains("external_calls=0")
            && step.expected.contains("broker_order_submitted=false")));
        assert!(
            manifest
                .computer_use_steps
                .iter()
                .any(|step| step.step == "order-ticket-command"
                    && step.expected.contains("order_ticket")
                    && step.expected.contains("external_calls=0")
                    && step.expected.contains("broker_order_submitted=false"))
        );
        assert!(manifest.computer_use_steps.iter().any(|step| step.step
            == "provider-preview-command"
            && step.expected.contains("Market-bar status")
            && step.expected.contains("dashboard backend")
            && step.expected.contains("provider_calls=0")));
        assert!(manifest.computer_use_steps.iter().any(|step| {
            step.step == "capture"
                && step
                    .expected
                    .contains("loading-dashboard before first data")
                && step.expected.contains("command-state")
                && step.expected.contains("automation-json")
                && step.expected.contains("filter-state")
                && step.expected.contains("keys-panel")
                && step.expected.contains("snapshot-panel")
                && step.expected.contains("snapshot-page=<PAGE>")
        }));
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
        assert!(manifest.zero_call_assertions.iter().any(|assertion| {
            assertion.contains("ready must update filter-state")
                && assertion.contains("scan_mode=actionable")
                && assertion.contains("review must open the Review page")
        }));
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
            assertion.contains(
                "Local broker, feedback, paper-decision, order-ticket, value-ledger, and outcome commands",
            ) && assertion.contains("guarded dashboard backend")
                && assertion.contains("provider, OpenAI, broker, order, or external calls")
        }));
        assert!(manifest.zero_call_assertions.iter().any(|assertion| {
            assertion.contains("Agent, market-bar, options, and SEC CIK preview/status commands")
                && assertion.contains("execute or confirm variants")
                && assertion.contains("external PowerShell boundaries")
        }));
        assert!(manifest.notes.iter().any(|note| {
            note.contains(
                "Local broker, feedback, paper-decision, order-ticket, value-ledger, and outcome commands",
            ) && note.contains("provider preview/status commands use the guarded dashboard backend")
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
                    .contains("native MarketRadar Trading Workbench window closes")
        }));
        assert!(
            manifest
                .zero_call_assertions
                .iter()
                .any(|assertion| assertion.contains("q, quit, and exit close"))
        );
    }
}
