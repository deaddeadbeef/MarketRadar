use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Page {
    Tutorial,
    Overview,
    Portfolio,
    MarketRadar,
    TradePlanner,
    RiskDesk,
    PaperTrading,
    Backtest,
    Readiness,
    Run,
    Candidates,
    Review,
    Alerts,
    Ipo,
    Broker,
    Ops,
    Telemetry,
    Agent,
    Themes,
    Validation,
    Costs,
    Features,
    Journal,
    Help,
}

impl Page {
    pub const ALL: [Page; 24] = [
        Page::Tutorial,
        Page::Overview,
        Page::Portfolio,
        Page::MarketRadar,
        Page::TradePlanner,
        Page::RiskDesk,
        Page::PaperTrading,
        Page::Backtest,
        Page::Readiness,
        Page::Run,
        Page::Candidates,
        Page::Review,
        Page::Alerts,
        Page::Ipo,
        Page::Broker,
        Page::Ops,
        Page::Telemetry,
        Page::Agent,
        Page::Themes,
        Page::Validation,
        Page::Costs,
        Page::Features,
        Page::Journal,
        Page::Help,
    ];

    pub fn key(self) -> &'static str {
        match self {
            Page::Tutorial => "tutorial",
            Page::Overview => "overview",
            Page::Portfolio => "portfolio",
            Page::MarketRadar => "market-radar",
            Page::TradePlanner => "trade-planner",
            Page::RiskDesk => "risk-desk",
            Page::PaperTrading => "paper-trading",
            Page::Backtest => "backtest",
            Page::Readiness => "readiness",
            Page::Run => "run",
            Page::Candidates => "candidates",
            Page::Review => "review",
            Page::Alerts => "alerts",
            Page::Ipo => "ipo",
            Page::Broker => "broker",
            Page::Ops => "ops",
            Page::Telemetry => "telemetry",
            Page::Agent => "agent",
            Page::Themes => "themes",
            Page::Validation => "validation",
            Page::Costs => "costs",
            Page::Features => "features",
            Page::Journal => "journal",
            Page::Help => "help",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Page::Tutorial => "0 Start",
            Page::Overview => "1 Command Center",
            Page::Portfolio => "Portfolio",
            Page::MarketRadar => "Market Radar",
            Page::TradePlanner => "Trade Planner",
            Page::RiskDesk => "Risk Desk",
            Page::PaperTrading => "Paper Trading",
            Page::Backtest => "Backtest",
            Page::Readiness => "2 Evidence Gaps",
            Page::Run => "3 Safe Run",
            Page::Candidates => "4 Candidate Review",
            Page::Review => "Review",
            Page::Alerts => "5 Alerts",
            Page::Ipo => "6 IPO/S-1",
            Page::Broker => "7 Broker",
            Page::Ops => "8 Ops",
            Page::Telemetry => "9 Telemetry",
            Page::Agent => "Ctrl+A Agent",
            Page::Themes => "Themes",
            Page::Validation => "Validation",
            Page::Costs => "Costs",
            Page::Features => "F Features",
            Page::Journal => "Journal",
            Page::Help => "? Help",
        }
    }

    pub fn from_input(value: &str) -> Page {
        match value
            .trim()
            .to_ascii_lowercase()
            .replace([' ', '-'], "_")
            .as_str()
        {
            "0" | "learn" | "start" | "tut" | "tutorial" => Page::Tutorial,
            "1" | "home" | "inbox" | "insight" | "insights" | "mail" | "messages" | "overview"
            | "command_center" | "workbench" | "o" => Page::Overview,
            "portfolio" | "portfolio_monitor" | "portfolio_monitoring" => Page::Portfolio,
            "market" | "market_radar" | "radar" | "scout" | "scanner" => Page::MarketRadar,
            "trade" | "trade_plan" | "trade_planner" | "planner" => Page::TradePlanner,
            "risk" | "risk_desk" | "risk_controls" => Page::RiskDesk,
            "paper" | "paper_trade" | "paper_trading" | "paper_trader" => Page::PaperTrading,
            "backtest" | "backtests" | "replay" | "replays" => Page::Backtest,
            "2" | "readiness" | "ready" | "evidence" | "evidence_gaps" | "gaps" => Page::Readiness,
            "3" | "run" | "safe" | "safe_run" | "call_plan" | "plan" => Page::Run,
            "4" | "candidate" | "candidates" | "candidate_review" | "c" => Page::Candidates,
            "11" | "review" | "decision" | "decisions" | "decision_ready" | "d" => Page::Review,
            "5" | "alert" | "alerts" | "a" => Page::Alerts,
            "6" | "ipo" | "s1" => Page::Ipo,
            "7" | "broker" | "b" => Page::Broker,
            "8" | "ops" => Page::Ops,
            "9" | "telemetry" | "t" => Page::Telemetry,
            "10" | "agent" | "agents" | "brief" => Page::Agent,
            "theme" | "themes" | "theme_row" | "theme_rows" => Page::Themes,
            "valid" | "validate" | "validation" | "value_validation" | "value-validation" => {
                Page::Validation
            }
            "cost" | "costs" | "value" | "value_report" | "value-report" => Page::Costs,
            "f" | "features" => Page::Features,
            "journal" | "journals" | "trade_journal" | "decision_journal" => Page::Journal,
            "?" | "h" | "help" => Page::Help,
            _ => Page::Overview,
        }
    }

    pub fn next(self) -> Page {
        let index = Self::ALL.iter().position(|page| *page == self).unwrap_or(0);
        Self::ALL[(index + 1) % Self::ALL.len()]
    }

    pub fn previous(self) -> Page {
        let index = Self::ALL.iter().position(|page| *page == self).unwrap_or(0);
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    pub fn first() -> Page {
        Self::ALL[0]
    }

    pub fn last() -> Page {
        Self::ALL[Self::ALL.len() - 1]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueRow {
    pub ticker: String,
    pub state: String,
    pub subject: String,
    pub next_action: String,
}

#[derive(Clone, Debug)]
pub struct SnapshotView {
    pub raw: Value,
    pub schema_version: String,
    pub snapshot_mode: String,
    pub status: String,
    pub first_blocker: String,
    pub next_action: String,
    pub next_command: String,
    pub external_calls: String,
    pub rows: Vec<QueueRow>,
    pub metrics: Vec<(String, String)>,
}

impl SnapshotView {
    pub fn from_value(raw: Value) -> Self {
        let rows = queue_rows(&raw);
        let metrics = vec![
            (
                "Scan rows".to_string(),
                metric_count(&raw, &["candidates", "count"]),
            ),
            (
                "Queue rows".to_string(),
                metric_count_any(
                    &raw,
                    &[
                        &["priced_in_queue", "returned_count"],
                        &["priced_in_queue", "count"],
                        &["priced_in_queue", "total_count"],
                    ],
                ),
            ),
            (
                "Alerts".to_string(),
                metric_count(&raw, &["alerts", "count"]),
            ),
            (
                "Provider calls".to_string(),
                text_at(&raw, &["external_calls_made"]).unwrap_or_else(|| "0".to_string()),
            ),
        ];
        Self {
            schema_version: text_at(&raw, &["schema_version"]).unwrap_or_default(),
            snapshot_mode: text_at(&raw, &["snapshot_mode"]).unwrap_or_default(),
            status: text_at(&raw, &["status"]).unwrap_or_else(|| "unknown".to_string()),
            first_blocker: text_at(&raw, &["first_blocker"]).unwrap_or_default(),
            next_action: text_at(&raw, &["next_action"])
                .or_else(|| text_at(&raw, &["canonical_next_action"]))
                .unwrap_or_default(),
            next_command: text_at(&raw, &["next_command"])
                .or_else(|| text_at(&raw, &["canonical_next_command"]))
                .unwrap_or_default(),
            external_calls: text_at(&raw, &["external_calls_made"])
                .unwrap_or_else(|| "0".to_string()),
            rows,
            metrics,
            raw,
        }
    }
}

pub fn text_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    match current {
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => {
            let text = value.trim();
            (!text.is_empty()).then(|| text.to_string())
        }
        Value::Array(values) => Some(values.len().to_string()),
        Value::Object(values) => Some(values.len().to_string()),
    }
}

pub fn array_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Vec<Value>> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    current.as_array()
}

pub fn object_count_at(value: &Value, path: &[&str]) -> Option<usize> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    current.as_object().map(|object| object.len())
}

pub fn compact(value: Option<String>, fallback: &str) -> String {
    value
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn queue_rows(raw: &Value) -> Vec<QueueRow> {
    let rows = array_at(raw, &["priced_in_queue", "rows"])
        .or_else(|| array_at(raw, &["priced_in_queue", "items"]))
        .or_else(|| array_at(raw, &["candidates", "rows"]))
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    rows.iter().take(12).map(queue_row).collect()
}

fn queue_row(value: &Value) -> QueueRow {
    QueueRow {
        ticker: compact(
            text_at(value, &["ticker"])
                .or_else(|| text_at(value, &["symbol"]))
                .or_else(|| text_at(value, &["security"])),
            "-",
        ),
        state: compact(
            text_at(value, &["state"])
                .or_else(|| text_at(value, &["status"]))
                .or_else(|| text_at(value, &["decision_status"]))
                .or_else(|| text_at(value, &["usefulness"])),
            "review",
        ),
        subject: compact(
            text_at(value, &["subject"])
                .or_else(|| text_at(value, &["title"]))
                .or_else(|| text_at(value, &["setup"]))
                .or_else(|| text_at(value, &["top_catalyst"]))
                .or_else(|| text_at(value, &["why_now"])),
            "Open the row for evidence.",
        ),
        next_action: compact(
            text_at(value, &["next_action"])
                .or_else(|| text_at(value, &["action"]))
                .or_else(|| text_at(value, &["command"]))
                .or_else(|| text_at(value, &["next_command"])),
            "inspect",
        ),
    }
}

fn metric_count(value: &Value, path: &[&str]) -> String {
    text_at(value, path).unwrap_or_else(|| "0".to_string())
}

fn metric_count_any(value: &Value, paths: &[&[&str]]) -> String {
    paths
        .iter()
        .find_map(|path| text_at(value, path))
        .unwrap_or_else(|| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_aliases_match_existing_dashboard_words() {
        assert_eq!(Page::from_input("learn"), Page::Tutorial);
        assert_eq!(Page::from_input("home"), Page::Overview);
        assert_eq!(Page::from_input("mail"), Page::Overview);
        assert_eq!(Page::from_input("2"), Page::Readiness);
        assert_eq!(Page::from_input("evidence-gaps"), Page::Readiness);
        assert_eq!(Page::from_input("safe-run"), Page::Run);
        assert_eq!(Page::from_input("call-plan"), Page::Run);
        assert_eq!(Page::from_input("candidate-review"), Page::Candidates);
        assert_eq!(Page::from_input("portfolio"), Page::Portfolio);
        assert_eq!(Page::from_input("market-radar"), Page::MarketRadar);
        assert_eq!(Page::from_input("trade-planner"), Page::TradePlanner);
        assert_eq!(Page::from_input("risk-desk"), Page::RiskDesk);
        assert_eq!(Page::from_input("paper-trading"), Page::PaperTrading);
        assert_eq!(Page::from_input("backtest"), Page::Backtest);
        assert_eq!(Page::from_input("journal"), Page::Journal);
        assert_eq!(Page::from_input("11"), Page::Review);
        assert_eq!(Page::from_input("decision_ready"), Page::Review);
        assert_eq!(Page::from_input("10"), Page::Agent);
        assert_eq!(Page::from_input("themes"), Page::Themes);
        assert_eq!(Page::from_input("value-validation"), Page::Validation);
        assert_eq!(Page::from_input("costs"), Page::Costs);
        assert_eq!(Page::from_input("F"), Page::Features);
    }

    #[test]
    fn snapshot_view_extracts_top_level_status_and_rows() {
        let raw = serde_json::json!({
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
            "status": "blocked",
            "first_blocker": "Need fresh market bars",
            "next_action": "Import bars",
            "next_command": "bars import",
            "external_calls_made": 0,
            "candidates": {"count": 1},
            "alerts": {"count": 0},
            "priced_in_queue": {
                "returned_count": 1,
                "rows": [{
                    "ticker": "MSFT",
                    "status": "actionable",
                    "subject": "Gap in expected reaction",
                    "next_command": "review MSFT"
                }]
            }
        });

        let view = SnapshotView::from_value(raw);

        assert_eq!(view.status, "blocked");
        assert_eq!(view.next_command, "bars import");
        assert_eq!(view.rows.len(), 1);
        assert_eq!(view.rows[0].ticker, "MSFT");
        assert_eq!(view.rows[0].next_action, "review MSFT");
        assert!(
            view.metrics
                .iter()
                .any(|metric| metric == &("Queue rows".into(), "1".into()))
        );
    }
}
