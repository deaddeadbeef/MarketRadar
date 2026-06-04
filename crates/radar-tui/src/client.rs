use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::model::Page;

#[derive(Clone, Debug)]
pub struct SnapshotFilters {
    pub database_url: Option<String>,
    pub ticker: Option<String>,
    pub available_at: Option<String>,
    pub alert_status: Option<String>,
    pub alert_route: Option<String>,
    pub priced_in_status: String,
    pub usefulness: Option<String>,
    pub source_gap: Vec<String>,
    pub decision_gap: Vec<String>,
    pub stocks_only: bool,
    pub scan_limit: u16,
    pub scan_offset: u32,
    pub telemetry_limit: u16,
}

impl Default for SnapshotFilters {
    fn default() -> Self {
        Self {
            database_url: None,
            ticker: None,
            available_at: None,
            alert_status: None,
            alert_route: None,
            priced_in_status: "all".to_string(),
            usefulness: None,
            source_gap: Vec::new(),
            decision_gap: Vec::new(),
            stocks_only: false,
            scan_limit: 50,
            scan_offset: 0,
            telemetry_limit: 8,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotRequest {
    pub page: Page,
    pub requested_page: Option<String>,
    pub filters: SnapshotFilters,
}

#[derive(Clone, Debug)]
pub enum SnapshotSource {
    Api {
        base_url: String,
        role: Option<String>,
        allow_invalid_certs: bool,
    },
    Command {
        command: String,
    },
}

impl SnapshotSource {
    pub fn label(&self) -> String {
        match self {
            SnapshotSource::Api { base_url, .. } => format!("api {}", base_url),
            SnapshotSource::Command { command } => format!("command {}", command),
        }
    }
}

pub fn fetch_snapshot(source: &SnapshotSource, request: &SnapshotRequest) -> Result<Value> {
    match source {
        SnapshotSource::Api {
            base_url,
            role,
            allow_invalid_certs,
        } => fetch_api_snapshot(base_url, role.as_deref(), *allow_invalid_certs, request),
        SnapshotSource::Command { command } => fetch_command_snapshot(command, request),
    }
}

pub fn execute_dashboard_command(
    source: &SnapshotSource,
    command_text: &str,
    request: &SnapshotRequest,
) -> Result<Value> {
    match source {
        SnapshotSource::Api {
            base_url,
            role,
            allow_invalid_certs,
        } => execute_api_dashboard_command(
            base_url,
            role.as_deref(),
            *allow_invalid_certs,
            command_text,
        ),
        SnapshotSource::Command { command } => {
            execute_command_dashboard_command(command, command_text, request)
        }
    }
}

fn execute_api_dashboard_command(
    base_url: &str,
    role: Option<&str>,
    allow_invalid_certs: bool,
    command_text: &str,
) -> Result<Value> {
    if !is_run_execute_command(command_text) {
        bail!("API dashboard command is only implemented for run execute");
    }
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .danger_accept_invalid_certs(allow_invalid_certs)
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("{}/api/radar/runs", base_url.trim_end_matches('/'));
    let mut request = client.post(url).json(&json!({
        "run_llm": true,
        "llm_dry_run": true,
        "dry_run_alerts": true,
    }));
    if let Some(role) = role {
        request = request.header("x-catalyst-role", role);
    }
    let response = request.send().context("radar run request failed")?;
    let status = response.status();
    let text = response
        .text()
        .context("radar run response body could not be read")?;
    if !status.is_success() {
        bail!(
            "radar run endpoint returned {status}: {}",
            response_error_detail(&text)
        );
    }
    serde_json::from_str::<Value>(&text).context("radar run response was not valid JSON")
}

fn execute_command_dashboard_command(
    command: &str,
    command_text: &str,
    request: &SnapshotRequest,
) -> Result<Value> {
    let command_line = command_line_for_dashboard_command(command, command_text, request)?;
    let output = shell_command(&command_line)
        .with_context(|| format!("dashboard command failed to start: {command_line}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("dashboard command failed: {}", stderr.trim());
    }
    let stdout = String::from_utf8(output.stdout).context("dashboard command emitted non-UTF8")?;
    serde_json::from_str::<Value>(&stdout).context("dashboard command did not emit JSON")
}

fn fetch_api_snapshot(
    base_url: &str,
    role: Option<&str>,
    allow_invalid_certs: bool,
    request: &SnapshotRequest,
) -> Result<Value> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .danger_accept_invalid_certs(allow_invalid_certs)
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("{}/api/dashboard/snapshot", base_url.trim_end_matches('/'));
    let query = api_query(request);
    let mut request = client.get(url).query(&query);
    if let Some(role) = role {
        request = request.header("x-catalyst-role", role);
    }
    let response = request
        .send()
        .context("dashboard snapshot request failed")?
        .error_for_status()
        .context("dashboard snapshot endpoint returned an error")?;
    response
        .json::<Value>()
        .context("dashboard snapshot response was not valid JSON")
}

fn fetch_command_snapshot(command: &str, request: &SnapshotRequest) -> Result<Value> {
    let mut command_line = command_line_for_snapshot(command, request);
    append_command_filters(&mut command_line, &request.filters);
    let output = shell_command(&command_line)
        .with_context(|| format!("snapshot command failed to start: {command_line}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("snapshot command failed: {}", stderr.trim());
    }
    let stdout = String::from_utf8(output.stdout).context("snapshot command emitted non-UTF8")?;
    serde_json::from_str::<Value>(&stdout).context("snapshot command did not emit JSON")
}

#[cfg(windows)]
fn shell_command(command_line: &str) -> std::io::Result<std::process::Output> {
    Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            command_line,
        ])
        .output()
}

#[cfg(not(windows))]
fn shell_command(command_line: &str) -> std::io::Result<std::process::Output> {
    Command::new("sh").args(["-lc", command_line]).output()
}

fn command_line_for_snapshot(command: &str, request: &SnapshotRequest) -> String {
    let page = request_page(request);
    if command.contains("{page}") {
        command.replace("{page}", page)
    } else {
        format!("{command} --page {}", shell_quote(page))
    }
}

fn command_line_for_dashboard_command(
    command: &str,
    command_text: &str,
    request: &SnapshotRequest,
) -> Result<String> {
    if !command.contains("dashboard-snapshot") {
        bail!("snapshot command cannot be adapted to dashboard-command");
    }
    let mut command_line = command.replacen("dashboard-snapshot", "dashboard-command", 1);
    if command_line.contains("{command}") {
        command_line = command_line.replace("{command}", &shell_quote(command_text));
    } else {
        command_line.push_str(" --command ");
        command_line.push_str(&shell_quote(command_text));
    }
    let page = request_page(request);
    if command_line.contains("{page}") {
        command_line = command_line.replace("{page}", page);
    } else {
        command_line.push_str(" --page ");
        command_line.push_str(&shell_quote(page));
    }
    append_command_filters(&mut command_line, &request.filters);
    Ok(command_line)
}

fn is_run_execute_command(command_text: &str) -> bool {
    command_text
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
        == "run execute"
}

fn response_error_detail(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "empty response body".to_string();
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(detail) = value.get("detail") {
            return match detail {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
        }
    }
    trimmed.to_string()
}

fn api_query(request: &SnapshotRequest) -> Vec<(String, String)> {
    let mut query = vec![
        ("page".to_string(), request_page(request).to_string()),
        ("fast".to_string(), "true".to_string()),
        (
            "priced_in_status".to_string(),
            request.filters.priced_in_status.clone(),
        ),
        (
            "scan_limit".to_string(),
            request.filters.scan_limit.to_string(),
        ),
        (
            "scan_offset".to_string(),
            request.filters.scan_offset.to_string(),
        ),
        (
            "telemetry_limit".to_string(),
            request.filters.telemetry_limit.to_string(),
        ),
    ];
    push_query(&mut query, "ticker", request.filters.ticker.as_deref());
    push_query(
        &mut query,
        "available_at",
        request.filters.available_at.as_deref(),
    );
    push_query(
        &mut query,
        "alert_status",
        request.filters.alert_status.as_deref(),
    );
    push_query(
        &mut query,
        "alert_route",
        request.filters.alert_route.as_deref(),
    );
    push_query(
        &mut query,
        "usefulness",
        request.filters.usefulness.as_deref(),
    );
    if request.filters.stocks_only {
        query.push(("stocks_only".to_string(), "true".to_string()));
    }
    for source_gap in &request.filters.source_gap {
        query.push(("source_gap".to_string(), source_gap.clone()));
    }
    for decision_gap in &request.filters.decision_gap {
        query.push(("decision_gap".to_string(), decision_gap.clone()));
    }
    query
}

fn request_page(request: &SnapshotRequest) -> &str {
    request
        .requested_page
        .as_deref()
        .map(str::trim)
        .filter(|page| !page.is_empty())
        .unwrap_or_else(|| request.page.key())
}

fn push_query(query: &mut Vec<(String, String)>, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        query.push((name.to_string(), value.to_string()));
    }
}

fn append_command_filters(command_line: &mut String, filters: &SnapshotFilters) {
    push_command_arg(
        command_line,
        "--database-url",
        filters.database_url.as_deref(),
    );
    push_command_arg(command_line, "--ticker", filters.ticker.as_deref());
    push_command_arg(
        command_line,
        "--available-at",
        filters.available_at.as_deref(),
    );
    push_command_arg(
        command_line,
        "--alert-status",
        filters.alert_status.as_deref(),
    );
    push_command_arg(
        command_line,
        "--alert-route",
        filters.alert_route.as_deref(),
    );
    push_command_arg(
        command_line,
        "--scan-mode",
        Some(filters.priced_in_status.as_str()),
    );
    push_command_arg(command_line, "--usefulness", filters.usefulness.as_deref());
    for source_gap in &filters.source_gap {
        push_command_arg(command_line, "--source-gap", Some(source_gap));
    }
    for decision_gap in &filters.decision_gap {
        push_command_arg(command_line, "--decision-gap", Some(decision_gap));
    }
    if filters.stocks_only {
        command_line.push_str(" --stocks-only");
    }
    push_command_arg(
        command_line,
        "--scan-limit",
        Some(&filters.scan_limit.to_string()),
    );
    push_command_arg(
        command_line,
        "--scan-offset",
        Some(&filters.scan_offset.to_string()),
    );
    push_command_arg(
        command_line,
        "--telemetry-limit",
        Some(&filters.telemetry_limit.to_string()),
    );
}

fn push_command_arg(command_line: &mut String, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        command_line.push(' ');
        command_line.push_str(name);
        command_line.push(' ');
        command_line.push_str(&shell_quote(value));
    }
}

#[cfg(windows)]
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(not(windows))]
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_labels_are_operator_readable() {
        let source = SnapshotSource::Api {
            base_url: "http://127.0.0.1:8000".to_string(),
            role: None,
            allow_invalid_certs: false,
        };

        assert_eq!(source.label(), "api http://127.0.0.1:8000");
    }

    #[test]
    fn api_query_preserves_existing_dashboard_filters() {
        let request = SnapshotRequest {
            page: Page::Review,
            requested_page: None,
            filters: SnapshotFilters {
                ticker: Some("MSFT".to_string()),
                available_at: Some("2026-05-18T16:00:00+00:00".to_string()),
                priced_in_status: "actionable".to_string(),
                source_gap: vec!["options".to_string(), "local_text".to_string()],
                decision_gap: vec!["decision_card".to_string()],
                stocks_only: true,
                scan_limit: 12,
                scan_offset: 24,
                telemetry_limit: 5,
                ..SnapshotFilters::default()
            },
        };

        let query = api_query(&request);

        assert!(query.contains(&("page".to_string(), "review".to_string())));
        assert!(query.contains(&("ticker".to_string(), "MSFT".to_string())));
        assert!(query.contains(&("source_gap".to_string(), "options".to_string())));
        assert!(query.contains(&("source_gap".to_string(), "local_text".to_string())));
        assert!(query.contains(&("decision_gap".to_string(), "decision_card".to_string())));
        assert!(query.contains(&("stocks_only".to_string(), "true".to_string())));
        assert!(query.contains(&("scan_limit".to_string(), "12".to_string())));
        assert!(query.contains(&("scan_offset".to_string(), "24".to_string())));
        assert!(query.contains(&("telemetry_limit".to_string(), "5".to_string())));
    }

    #[test]
    fn api_query_preserves_exact_detail_page_request() {
        let request = SnapshotRequest {
            page: Page::Overview,
            requested_page: Some("candidate:MSFT".to_string()),
            filters: SnapshotFilters {
                ticker: Some("MSFT".to_string()),
                scan_offset: 0,
                ..SnapshotFilters::default()
            },
        };

        let query = api_query(&request);

        assert!(query.contains(&("page".to_string(), "candidate:MSFT".to_string())));
        assert!(query.contains(&("ticker".to_string(), "MSFT".to_string())));
        assert!(query.contains(&("scan_offset".to_string(), "0".to_string())));
    }

    #[test]
    fn command_filters_append_snapshot_arguments() {
        let filters = SnapshotFilters {
            database_url: Some("sqlite:///data/local/test.db".to_string()),
            ticker: Some("MSFT".to_string()),
            priced_in_status: "all".to_string(),
            source_gap: vec!["options,local_text".to_string()],
            stocks_only: true,
            scan_limit: 50,
            scan_offset: 0,
            telemetry_limit: 8,
            ..SnapshotFilters::default()
        };
        let mut command = "catalyst-radar dashboard-snapshot --json --fast".to_string();

        append_command_filters(&mut command, &filters);

        assert!(command.contains("--database-url 'sqlite:///data/local/test.db'"));
        assert!(command.contains("--ticker 'MSFT'"));
        assert!(command.contains("--scan-mode 'all'"));
        assert!(command.contains("--source-gap 'options,local_text'"));
        assert!(command.contains("--stocks-only"));
        assert!(command.contains("--scan-limit '50'"));
    }

    #[test]
    fn command_line_preserves_exact_detail_page_request() {
        let request = SnapshotRequest {
            page: Page::Alerts,
            requested_page: Some("alert:demo-alert-1".to_string()),
            filters: SnapshotFilters::default(),
        };

        let command =
            command_line_for_snapshot("catalyst-radar dashboard-snapshot --json --fast", &request);

        assert!(command.contains("--page 'alert:demo-alert-1'"));
    }

    #[test]
    fn command_line_substitutes_exact_page_placeholder() {
        let request = SnapshotRequest {
            page: Page::Run,
            requested_page: Some("safe-run".to_string()),
            filters: SnapshotFilters::default(),
        };

        let command = command_line_for_snapshot(
            "catalyst-radar dashboard-snapshot --json --fast --page {page}",
            &request,
        );

        assert!(command.contains("--page safe-run"));
    }

    #[test]
    fn dashboard_command_reuses_snapshot_command_and_filters() {
        let request = SnapshotRequest {
            page: Page::Run,
            requested_page: None,
            filters: SnapshotFilters {
                ticker: Some("MSFT".to_string()),
                priced_in_status: "actionable".to_string(),
                scan_limit: 25,
                ..SnapshotFilters::default()
            },
        };

        let command = command_line_for_dashboard_command(
            "catalyst-radar dashboard-snapshot --json --fast",
            "run execute",
            &request,
        )
        .unwrap();

        assert!(command.contains("catalyst-radar dashboard-command --json --fast"));
        assert!(command.contains("--command 'run execute'"));
        assert!(command.contains("--page 'run'"));
        assert!(command.contains("--ticker 'MSFT'"));
        assert!(command.contains("--scan-mode 'actionable'"));
        assert!(command.contains("--scan-limit '25'"));
    }

    #[test]
    fn dashboard_command_rejects_non_snapshot_command_templates() {
        let request = SnapshotRequest {
            page: Page::Run,
            requested_page: None,
            filters: SnapshotFilters::default(),
        };

        assert!(
            command_line_for_dashboard_command("python custom.py", "run execute", &request)
                .is_err()
        );
    }

    #[test]
    fn run_execute_command_normalizes_whitespace_and_case() {
        assert!(is_run_execute_command(" RUN   Execute "));
        assert!(!is_run_execute_command("agent execute"));
    }
}
