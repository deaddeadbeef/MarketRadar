use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::model::Page;

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

pub fn fetch_snapshot(source: &SnapshotSource, page: Page) -> Result<Value> {
    match source {
        SnapshotSource::Api {
            base_url,
            role,
            allow_invalid_certs,
        } => fetch_api_snapshot(base_url, role.as_deref(), *allow_invalid_certs, page),
        SnapshotSource::Command { command } => fetch_command_snapshot(command, page),
    }
}

fn fetch_api_snapshot(
    base_url: &str,
    role: Option<&str>,
    allow_invalid_certs: bool,
    page: Page,
) -> Result<Value> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .danger_accept_invalid_certs(allow_invalid_certs)
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("{}/api/dashboard/snapshot", base_url.trim_end_matches('/'));
    let mut request = client
        .get(url)
        .query(&[("page", page.key()), ("fast", "true")]);
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

fn fetch_command_snapshot(command: &str, page: Page) -> Result<Value> {
    let command_line = if command.contains("{page}") {
        command.replace("{page}", page.key())
    } else {
        format!("{command} --page {}", page.key())
    };
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
}
