use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap};
use serde_json::Value;

use crate::app::DashboardApp;
use crate::model::{Page, SnapshotView, array_at, compact, object_count_at, text_at};

pub fn render(frame: &mut Frame<'_>, app: &DashboardApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(27), Constraint::Min(30)])
        .split(chunks[1]);
    render_nav(frame, body[0], app);
    render_page(frame, body[1], app);
    render_footer(frame, chunks[2], app);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let status = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.status.as_str())
        .unwrap_or("loading");
    let refresh = if app.loading { "refreshing" } else { "ready" };
    let title = vec![Line::from(vec![
        Span::styled(
            "MarketRadar",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::raw(app.page.label()),
        Span::raw("  status="),
        Span::styled(
            status.to_string(),
            Style::default().fg(status_color(status)),
        ),
        Span::raw("  "),
        Span::raw(refresh),
    ])];
    frame.render_widget(
        Paragraph::new(title).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_nav(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let items = Page::ALL
        .iter()
        .map(|page| {
            let style = if *page == app.page {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(page.label(), style)))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(Block::default().title("Workflow").borders(Borders::ALL)),
        area,
    );
}

fn render_page(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    if let Some(error) = &app.error {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Snapshot unavailable",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
                Line::from(error.clone()),
                Line::from("Press r to retry or q to quit."),
            ])
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Error").borders(Borders::ALL)),
            area,
        );
        return;
    }

    let Some(snapshot) = &app.snapshot else {
        frame.render_widget(
            Paragraph::new("Loading dashboard snapshot...")
                .block(Block::default().title("Dashboard").borders(Borders::ALL)),
            area,
        );
        return;
    };

    match app.page {
        Page::Overview | Page::Tutorial => render_overview(frame, area, app, snapshot),
        Page::Candidates | Page::Review => render_rows(frame, area, snapshot, "Scan Queue"),
        Page::Alerts => render_json_array(frame, area, snapshot, &["alerts", "rows"], "Alerts"),
        Page::Features => {
            render_json_array(frame, area, snapshot, &["feature_inventory"], "Features")
        }
        Page::Readiness => {
            render_object_summary(frame, area, snapshot, &["readiness"], "Evidence Gaps")
        }
        Page::Run => render_object_summary(frame, area, snapshot, &["call_plan"], "Safe Run"),
        Page::Ipo => render_json_array(frame, area, snapshot, &["ipo_s1", "rows"], "IPO/S-1"),
        Page::Broker => render_object_summary(frame, area, snapshot, &["broker"], "Broker"),
        Page::Ops => render_object_summary(frame, area, snapshot, &["ops_health"], "Ops"),
        Page::Telemetry => {
            render_json_array(frame, area, snapshot, &["telemetry", "events"], "Telemetry")
        }
        Page::Agent => {
            render_object_summary(frame, area, snapshot, &["agent_brief"], "Agent Coach")
        }
        Page::Help => render_help(frame, area),
    }
}

fn render_overview(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp, snapshot: &SnapshotView) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Min(8),
        ])
        .split(area);

    let summary = vec![
        Line::from(vec![
            Span::styled(
                "Decision status: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                snapshot.status.clone(),
                Style::default().fg(status_color(&snapshot.status)),
            ),
        ]),
        Line::from(format!(
            "First blocker: {}",
            compact(Some(snapshot.first_blocker.clone()), "none")
        )),
        Line::from(format!(
            "Next action: {}",
            compact(Some(snapshot.next_action.clone()), "review the queue")
        )),
        Line::from(format!(
            "Next command: {}",
            compact(Some(snapshot.next_command.clone()), "none")
        )),
    ];
    frame.render_widget(
        Paragraph::new(summary)
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Now").borders(Borders::ALL)),
        chunks[0],
    );

    let metric_cells = snapshot
        .metrics
        .iter()
        .map(|(label, value)| {
            Line::from(vec![
                Span::styled(format!("{label}: "), Style::default().fg(Color::Gray)),
                Span::styled(value.clone(), Style::default().add_modifier(Modifier::BOLD)),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(metric_cells).block(
            Block::default()
                .title("Local Snapshot")
                .borders(Borders::ALL),
        ),
        chunks[1],
    );

    if app.page == Page::Tutorial {
        render_tutorial(frame, chunks[2]);
    } else {
        render_rows(frame, chunks[2], snapshot, "Inbox");
    }
}

fn render_tutorial(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("Start with Inbox for the best current action."),
            Line::from("Use Evidence Gaps when the answer is blocked by missing data."),
            Line::from("Use Safe Run only after the call plan is visible."),
            Line::from("Rendering and navigation are local; snapshot refresh is read-only."),
        ])
        .wrap(Wrap { trim: false })
        .block(Block::default().title("Start Path").borders(Borders::ALL)),
        area,
    );
}

fn render_rows(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView, title: &str) {
    let rows = snapshot.rows.iter().map(|row| {
        Row::new(vec![
            Cell::from(row.ticker.clone()),
            Cell::from(row.state.clone()),
            Cell::from(row.subject.clone()),
            Cell::from(row.next_action.clone()),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(16),
            Constraint::Percentage(42),
            Constraint::Percentage(32),
        ],
    )
    .header(
        Row::new(vec!["Ticker", "State", "Subject", "Next"]).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().title(title).borders(Borders::ALL))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    frame.render_widget(table, area);
}

fn render_json_array(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &SnapshotView,
    path: &[&str],
    title: &str,
) {
    let lines = array_at(&snapshot.raw, path)
        .map(|values| {
            values
                .iter()
                .take(14)
                .enumerate()
                .map(|(index, value)| Line::from(format!("{}  {}", index + 1, short_value(value))))
                .collect::<Vec<_>>()
        })
        .filter(|lines| !lines.is_empty())
        .unwrap_or_else(|| vec![Line::from("No rows for this view.")]);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(Block::default().title(title).borders(Borders::ALL)),
        area,
    );
}

fn render_object_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &SnapshotView,
    path: &[&str],
    title: &str,
) {
    let value = path
        .iter()
        .try_fold(&snapshot.raw, |current, key| current.get(*key));
    let Some(value) = value else {
        frame.render_widget(
            Paragraph::new("No data for this view.")
                .block(Block::default().title(title).borders(Borders::ALL)),
            area,
        );
        return;
    };
    let mut lines = object_lines(value);
    if lines.is_empty() {
        lines.push(Line::from(short_value(value)));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(Block::default().title(title).borders(Borders::ALL)),
        area,
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("q / Esc: quit"),
            Line::from("r / F5: refresh snapshot"),
            Line::from("Tab or Right: next page"),
            Line::from("Shift+Tab or Left: previous page"),
            Line::from("0-9, f, ?, Ctrl+A: jump to a workflow page"),
            Line::from("Snapshot reads are zero provider-call dashboard reads."),
        ])
        .wrap(Wrap { trim: false })
        .block(Block::default().title("Help").borders(Borders::ALL)),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let refreshed = app
        .last_refresh
        .map(|last_refresh| format!("{}s ago", last_refresh.elapsed().as_secs()))
        .unwrap_or_else(|| "never".to_string());
    let calls = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.external_calls.as_str())
        .unwrap_or("0");
    let text = format!(
        "source={} | refreshed={} | provider_calls={} | q quit | r refresh | Tab pages",
        app.source_label, refreshed, calls
    );
    frame.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn object_lines(value: &Value) -> Vec<Line<'static>> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .take(18)
                .map(|(key, value)| {
                    Line::from(vec![
                        Span::styled(format!("{key}: "), Style::default().fg(Color::Gray)),
                        Span::raw(short_value(value)),
                    ])
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn short_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => format!("{} rows", values.len()),
        Value::Object(_) => text_at(value, &["status"])
            .or_else(|| text_at(value, &["summary"]))
            .or_else(|| text_at(value, &["answer"]))
            .or_else(|| object_count_at(value, &[]).map(|count| format!("{count} fields")))
            .unwrap_or_else(|| "object".to_string()),
    }
}

fn status_color(status: &str) -> Color {
    match status.trim().to_ascii_lowercase().as_str() {
        "ready" | "ok" | "success" | "complete" | "completed" => Color::Green,
        "blocked" | "error" | "failed" => Color::Red,
        "warning" | "stale" | "research_only" => Color::Yellow,
        _ => Color::White,
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::app::DashboardApp;
    use crate::model::{Page, SnapshotView};

    #[test]
    fn render_inbox_contains_navigation_and_next_action() {
        let snapshot = SnapshotView::from_value(serde_json::json!({
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
            "status": "blocked",
            "first_blocker": "Need bars",
            "next_action": "Import bars",
            "next_command": "bars import",
            "external_calls_made": 0,
            "candidates": {"count": 1},
            "alerts": {"count": 0},
            "priced_in_queue": {"returned_count": 1, "rows": [{
                "ticker": "MSFT",
                "state": "warning",
                "subject": "Evidence gap",
                "next_action": "inspect"
            }]}
        }));
        let app = DashboardApp::with_snapshot(Page::Overview, snapshot);
        let backend = TestBackend::new(120, 32);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal
            .draw(|frame| render(frame, &app))
            .expect("draw frame");
        let content = terminal.backend().buffer().content();
        let text = content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>()
            .join("");

        assert!(text.contains("MarketRadar"));
        assert!(text.contains("1 Inbox"));
        assert!(text.contains("Import bars"));
        assert!(text.contains("MSFT"));
    }
}
