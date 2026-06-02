use std::io;

use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, List, ListItem, Padding, Paragraph, Row, Table, Wrap,
};
use serde_json::Value;

use crate::app::DashboardApp;
use crate::model::{Page, SnapshotView, array_at, compact, object_count_at, text_at};

const PANEL_BG: Color = Color::Rgb(17, 24, 31);
const PANEL_ALT_BG: Color = Color::Rgb(22, 31, 40);
const INK: Color = Color::Rgb(229, 234, 242);
const MUTED: Color = Color::Rgb(139, 152, 168);
const CYAN: Color = Color::Rgb(91, 218, 255);
const BLUE: Color = Color::Rgb(73, 142, 255);
const GREEN: Color = Color::Rgb(86, 211, 146);
const AMBER: Color = Color::Rgb(247, 191, 87);
const RED: Color = Color::Rgb(255, 112, 112);
const MAGENTA: Color = Color::Rgb(209, 137, 255);

pub fn render(frame: &mut Frame<'_>, app: &DashboardApp) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(16),
            Constraint::Length(3),
        ])
        .split(frame.area());

    render_header(frame, root[0], app);

    if root[1].width >= 120 {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(24),
                Constraint::Length(1),
                Constraint::Min(50),
                Constraint::Length(1),
                Constraint::Length(36),
            ])
            .split(root[1]);
        render_nav(frame, body[0], app);
        render_page(frame, body[2], app);
        render_side_panel(frame, body[4], app);
    } else {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(22),
                Constraint::Length(1),
                Constraint::Min(28),
            ])
            .split(root[1]);
        render_nav(frame, body[0], app);
        render_page(frame, body[2], app);
    }

    render_footer(frame, root[2], app);
}

pub fn render_to_text(app: &DashboardApp, width: u16, height: u16) -> io::Result<String> {
    let width = width.max(80);
    let height = height.max(24);
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend)?;
    terminal.draw(|frame| render(frame, app))?;

    let lines = terminal
        .backend()
        .buffer()
        .content()
        .chunks(width as usize)
        .map(|row| {
            row.iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>();
    Ok(format!("{}\n", lines.join("\n")))
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let status = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.status.as_str())
        .unwrap_or("loading");
    let mode = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.snapshot_mode.as_str())
        .unwrap_or("snapshot pending");
    let rows = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.rows.len().to_string())
        .unwrap_or_else(|| "-".to_string());
    let calls = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.external_calls.as_str())
        .unwrap_or("0");
    let refresh = if app.loading {
        "refreshing"
    } else if app.error.is_some() {
        "attention"
    } else {
        "ready"
    };
    let lines = vec![
        Line::from(vec![
            Span::styled(
                "MARKETRADAR",
                Style::default()
                    .fg(CYAN)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            ),
            Span::raw("  "),
            Span::styled("Rust TUI", Style::default().fg(MUTED)),
            Span::raw("  "),
            Span::styled(app.page.label(), Style::default().fg(INK)),
            Span::raw("  "),
            Span::styled(refresh, Style::default().fg(status_color(refresh))),
        ]),
        Line::from(vec![
            Span::styled("Status ", Style::default().fg(MUTED)),
            Span::styled(
                status.to_string(),
                Style::default().fg(status_color(status)),
            ),
            Span::raw("   "),
            Span::styled("Snapshot ", Style::default().fg(MUTED)),
            Span::styled(mode.to_string(), Style::default().fg(BLUE)),
            Span::raw("   "),
            Span::styled("Rows ", Style::default().fg(MUTED)),
            Span::styled(rows, Style::default().fg(INK)),
            Span::raw("   "),
            Span::styled("Provider calls ", Style::default().fg(MUTED)),
            Span::styled(calls.to_string(), Style::default().fg(GREEN)),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .block(dashboard_block(" COMMAND CENTER ", CYAN))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_nav(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let label_width = area.width.saturating_sub(5) as usize;
    let items = Page::ALL
        .iter()
        .map(|page| {
            let (prefix, style) = if *page == app.page {
                (
                    "> ",
                    Style::default()
                        .fg(Color::Black)
                        .bg(CYAN)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                ("  ", Style::default().fg(INK))
            };
            ListItem::new(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(abbreviate(page.label(), label_width), style),
            ]))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(
            dashboard_block(" WORKFLOW ", BLUE)
                .padding(Padding::new(1, 0, 0, 0))
                .style(Style::default().bg(PANEL_BG)),
        ),
        area,
    );
}

fn render_page(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    if let Some(error) = &app.error {
        render_error(frame, area, error);
        return;
    }

    let Some(snapshot) = &app.snapshot else {
        render_loading(frame, area);
        return;
    };

    match app.page {
        Page::Overview => render_overview(frame, area, snapshot),
        Page::Tutorial => render_tutorial(frame, area),
        Page::Candidates | Page::Review => render_rows(frame, area, snapshot, " ATTENTION QUEUE "),
        Page::Alerts => render_json_array(frame, area, snapshot, &["alerts", "rows"], " ALERTS "),
        Page::Features => {
            render_json_array(frame, area, snapshot, &["feature_inventory"], " FEATURES ")
        }
        Page::Readiness => {
            render_object_summary(frame, area, snapshot, &["readiness"], " EVIDENCE GAPS ")
        }
        Page::Run => render_object_summary(frame, area, snapshot, &["call_plan"], " SAFE RUN "),
        Page::Ipo => render_json_array(frame, area, snapshot, &["ipo_s1", "rows"], " IPO/S-1 "),
        Page::Broker => render_object_summary(frame, area, snapshot, &["broker"], " BROKER "),
        Page::Ops => render_object_summary(frame, area, snapshot, &["ops_health"], " OPS "),
        Page::Telemetry => render_json_array(
            frame,
            area,
            snapshot,
            &["telemetry", "events"],
            " TELEMETRY ",
        ),
        Page::Agent => render_object_summary(frame, area, snapshot, &["agent_brief"], " AGENT "),
        Page::Costs => render_costs(frame, area, snapshot),
        Page::Help => render_help(frame, area),
    }
}

fn render_overview(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Min(10),
        ])
        .split(area);

    render_metric_strip(frame, chunks[0], snapshot);
    render_decision_panel(frame, chunks[1], snapshot);
    render_rows(frame, chunks[2], snapshot, " ATTENTION QUEUE ");
}

fn render_metric_strip(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    let cards = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

    render_metric_card(
        frame,
        cards[0],
        "Decision",
        &snapshot.status,
        status_color(&snapshot.status),
        "current gate",
    );
    render_metric_card(
        frame,
        cards[1],
        "Queue",
        &metric_value(snapshot, "Queue rows"),
        CYAN,
        "rows to triage",
    );
    render_metric_card(
        frame,
        cards[2],
        "Alerts",
        &metric_value(snapshot, "Alerts"),
        AMBER,
        "manual review",
    );
    render_metric_card(
        frame,
        cards[3],
        "Calls",
        &snapshot.external_calls,
        GREEN,
        "provider calls",
    );
}

fn render_metric_card(
    frame: &mut Frame<'_>,
    area: Rect,
    label: &str,
    value: &str,
    color: Color,
    caption: &str,
) {
    let lines = vec![
        Line::from(Span::styled(
            label.to_ascii_uppercase(),
            Style::default().fg(MUTED),
        )),
        Line::from(Span::styled(
            abbreviate(value, area.width.saturating_sub(4) as usize),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(caption, Style::default().fg(MUTED))),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .block(dashboard_block("", color).style(Style::default().bg(PANEL_ALT_BG))),
        area,
    );
}

fn render_decision_panel(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
        .split(area);
    let blocker = compact(Some(snapshot.first_blocker.clone()), "No blocker reported.");
    let next = operator_action_summary(&snapshot.next_action);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "FIRST BLOCKER",
                Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
            )),
            Line::from(blocker),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" WHY NOT READY ", AMBER)),
        columns[0],
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "NEXT SAFE ACTION",
                Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
            )),
            Line::from(next),
            Line::from(""),
            Line::from(vec![
                Span::styled("Command: ", Style::default().fg(MUTED)),
                Span::raw(compact(Some(snapshot.next_command.clone()), "none")),
            ]),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" OPERATOR MOVE ", GREEN)),
        columns[1],
    );
}

fn render_rows(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView, title: &str) {
    if snapshot.rows.is_empty() {
        render_empty_inbox(frame, area, snapshot, title);
        return;
    }

    let rows = snapshot.rows.iter().enumerate().map(|(index, row)| {
        let style = if index % 2 == 0 {
            Style::default().bg(PANEL_BG)
        } else {
            Style::default().bg(PANEL_ALT_BG)
        };
        Row::new(vec![
            Cell::from(row.ticker.clone()).style(Style::default().fg(CYAN)),
            Cell::from(row.state.clone()).style(Style::default().fg(status_color(&row.state))),
            Cell::from(row.subject.clone()).style(Style::default().fg(INK)),
            Cell::from(row.next_action.clone()).style(Style::default().fg(GREEN)),
        ])
        .style(style)
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
        Row::new(vec!["Ticker", "State", "Signal", "Next"]).style(
            Style::default()
                .fg(Color::Black)
                .bg(CYAN)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(dashboard_block(title, CYAN))
    .row_highlight_style(Style::default().bg(BLUE));
    frame.render_widget(table, area);
}

fn render_empty_inbox(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView, title: &str) {
    let next_action = operator_action_summary(&snapshot.next_action);
    let command = compact(Some(snapshot.next_command.clone()), "No command reported.");
    let lines = vec![
        Line::from(Span::styled(
            "No market scan rows yet",
            Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
        )),
        Line::from("MarketRadar will turn this into a triage inbox after the first scan."),
        Line::from(""),
        Line::from(vec![
            Span::styled("Next: ", Style::default().fg(MUTED)),
            Span::raw(next_action),
        ]),
        Line::from(vec![
            Span::styled("Command: ", Style::default().fg(MUTED)),
            Span::raw(command),
        ]),
        Line::from(""),
        Line::from("Expected path: 1) build universe  2) fill bars  3) safe run  4) review rows"),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(dashboard_block(title, CYAN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_side_panel(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Min(5),
        ])
        .split(area);

    if let Some(snapshot) = &app.snapshot {
        render_side_next(frame, chunks[0], snapshot);
        render_side_boundary(frame, chunks[1], snapshot);
    } else {
        render_side_loading(frame, chunks[0]);
        render_side_boundary_empty(frame, chunks[1]);
    }
    render_keys(frame, chunks[2]);
    render_snapshot_meta(frame, chunks[3], app);
}

fn render_side_next(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    let next_action = operator_action_summary(&snapshot.next_action);
    let command = compact(Some(snapshot.next_command.clone()), "none");
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "NEXT SAFE ACTION",
                Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
            )),
            Line::from(next_action),
            Line::from(""),
            Line::from(Span::styled("COMMAND", Style::default().fg(MUTED))),
            Line::from(abbreviate(&command, area.width.saturating_mul(2) as usize)),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" NOW ", GREEN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_side_boundary(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "ZERO PROVIDER CALLS",
                Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "Snapshot reported {} call(s).",
                snapshot.external_calls
            )),
            Line::from("Rendering, navigation, and refresh are read-only."),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" BOUNDARY ", GREEN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_side_loading(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Loading local snapshot",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from("The first render stays useful while the database read completes."),
            Line::from(""),
            Line::from("Next: decision, blocker, queue, and action cards."),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" STARTING ", CYAN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_side_boundary_empty(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "ZERO PROVIDER CALLS",
                Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
            )),
            Line::from("The Rust UI only reads the local dashboard snapshot."),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" BOUNDARY ", GREEN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_keys(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("q / Esc     quit"),
            Line::from("r / F5      refresh"),
            Line::from("Up/Down     workflow"),
            Line::from("Tab/Arrows  next/prev"),
            Line::from("Home/End    first/help"),
            Line::from("0-9 letters jump"),
            Line::from("Ctrl+A      agent"),
        ])
        .block(dashboard_block(" KEYS ", BLUE).padding(Padding::new(1, 0, 0, 0))),
        area,
    );
}

fn render_snapshot_meta(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let refreshed = app
        .last_refresh
        .map(|last_refresh| format!("{}s ago", last_refresh.elapsed().as_secs()))
        .unwrap_or_else(|| "pending".to_string());
    let source = friendly_source(&app.source_label);
    let lines = vec![
        Line::from(vec![
            Span::styled("Source: ", Style::default().fg(MUTED)),
            Span::raw(abbreviate(&source, area.width.saturating_sub(6) as usize)),
        ]),
        Line::from(vec![
            Span::styled("Refresh: ", Style::default().fg(MUTED)),
            Span::raw(refreshed),
        ]),
        Line::from(vec![
            Span::styled("Page: ", Style::default().fg(MUTED)),
            Span::raw(app.page.key()),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(dashboard_block(" SNAPSHOT ", MAGENTA).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_loading(frame: &mut Frame<'_>, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Min(8),
        ])
        .split(area);

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Loading market snapshot",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from("MarketRadar is reading the local dashboard contract."),
            Line::from("Rendering remains local and makes zero provider calls."),
            Line::from(""),
            Line::from(vec![
                Span::styled("Expected first screen: ", Style::default().fg(MUTED)),
                Span::raw("decision status, next safe action, metrics, and inbox rows."),
            ]),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" MARKET COMMAND CENTER ", CYAN).padding(Padding::new(1, 1, 0, 0))),
        chunks[0],
    );

    let cards = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(chunks[1]);
    render_placeholder_card(frame, cards[0], "Decision status", "pending");
    render_placeholder_card(frame, cards[1], "Next safe action", "loading");
    render_placeholder_card(frame, cards[2], "Provider calls", "0");

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "ATTENTION QUEUE",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from("ticker     state            signal                         next"),
            Line::from(
                "---------  ---------------  -----------------------------  ----------------",
            ),
            Line::from("[loading]  local snapshot   resolving dashboard contract    refresh"),
            Line::from("[loading]  zero-call read    waiting for data rows           inspect"),
        ])
        .block(dashboard_block(" PREVIEW ", CYAN).padding(Padding::new(1, 1, 0, 0))),
        chunks[2],
    );
}

fn render_placeholder_card(frame: &mut Frame<'_>, area: Rect, label: &str, value: &str) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                label.to_ascii_uppercase(),
                Style::default().fg(MUTED),
            )),
            Line::from(Span::styled(
                value,
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled("snapshot loading", Style::default().fg(MUTED))),
        ])
        .alignment(Alignment::Center)
        .block(dashboard_block("", CYAN).style(Style::default().bg(PANEL_ALT_BG))),
        area,
    );
}

fn render_error(frame: &mut Frame<'_>, area: Rect, error: &str) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Snapshot unavailable",
                Style::default().fg(RED).add_modifier(Modifier::BOLD),
            )),
            Line::from(error.to_string()),
            Line::from(""),
            Line::from("Press r to retry, or use radar --python-tui for the legacy fallback."),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" ATTENTION ", RED).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_tutorial(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Start Path",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from("1 Inbox: triage what matters now."),
            Line::from("2 Evidence Gaps: fix missing market or decision evidence."),
            Line::from("3 Safe Run: review provider calls before execution."),
            Line::from("4 Candidate Review: inspect a single evidence case."),
            Line::from(""),
            Line::from(
                "The Rust TUI is a read-only command center until you choose an explicit command.",
            ),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" FIRST 90 SECONDS ", CYAN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
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
                .take(18)
                .enumerate()
                .map(|(index, value)| {
                    Line::from(vec![
                        Span::styled(format!("{:>2}. ", index + 1), Style::default().fg(CYAN)),
                        Span::raw(short_value(value)),
                    ])
                })
                .collect::<Vec<_>>()
        })
        .filter(|lines| !lines.is_empty())
        .unwrap_or_else(|| vec![Line::from("No rows for this view.")]);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(dashboard_block(title, CYAN).padding(Padding::new(1, 1, 0, 0))),
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
                .block(dashboard_block(title, AMBER).padding(Padding::new(1, 1, 0, 0))),
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
            .block(dashboard_block(title, CYAN).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_costs(frame: &mut Frame<'_>, area: Rect, snapshot: &SnapshotView) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

    render_object_summary(frame, chunks[0], snapshot, &["costs"], " COSTS ");
    render_object_summary(
        frame,
        chunks[1],
        snapshot,
        &["value_ledger"],
        " VALUE LEDGER ",
    );
    render_object_summary(
        frame,
        chunks[2],
        snapshot,
        &["value_outcomes"],
        " VALUE OUTCOMES ",
    );
    render_object_summary(
        frame,
        chunks[3],
        snapshot,
        &["value_report"],
        " VALUE REPORT ",
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Keyboard",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
            Line::from("q / Esc: quit"),
            Line::from("r / F5: refresh snapshot"),
            Line::from("Up/Down, j/k, PageUp/PageDown: move through workflow pages"),
            Line::from("Tab/Shift+Tab or Left/Right: previous/next page"),
            Line::from("Home/End: start/help"),
            Line::from("0-9, A/B/C/D/E/F/G/H/I/O/S/T/?: jump to a workflow page"),
            Line::from("Ctrl+A: agent brief"),
            Line::from(""),
            Line::from("Snapshot reads are zero provider-call dashboard reads."),
        ])
        .wrap(Wrap { trim: false })
        .block(dashboard_block(" HELP ", BLUE).padding(Padding::new(1, 1, 0, 0))),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let refreshed = app
        .last_refresh
        .map(|last_refresh| format!("{}s ago", last_refresh.elapsed().as_secs()))
        .unwrap_or_else(|| "pending".to_string());
    let calls = app
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.external_calls.as_str())
        .unwrap_or("0");
    let source = friendly_source(&app.source_label);
    let text = format!(
        "source={}  refresh={}  provider_calls={}  q quit  r refresh  tab pages",
        source, refreshed, calls
    );
    frame.render_widget(
        Paragraph::new(abbreviate(&text, area.width.saturating_sub(4) as usize))
            .block(dashboard_block("", MUTED).padding(Padding::horizontal(1))),
        area,
    );
}

fn object_lines(value: &Value) -> Vec<Line<'static>> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .take(20)
                .map(|(key, value)| {
                    Line::from(vec![
                        Span::styled(format!("{key}: "), Style::default().fg(MUTED)),
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

fn metric_value(snapshot: &SnapshotView, label: &str) -> String {
    snapshot
        .metrics
        .iter()
        .find_map(|(metric_label, value)| (metric_label == label).then(|| value.clone()))
        .unwrap_or_else(|| "0".to_string())
}

fn dashboard_block<'a>(title: &'a str, color: Color) -> Block<'a> {
    Block::default()
        .title(title)
        .title_alignment(Alignment::Left)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(color))
        .style(Style::default().fg(INK).bg(PANEL_BG))
}

fn status_color(status: &str) -> Color {
    match status.trim().to_ascii_lowercase().as_str() {
        "ready" | "ok" | "success" | "complete" | "completed" | "ready_to_run" => GREEN,
        "blocked" | "error" | "failed" | "setup_required" => RED,
        "warning" | "stale" | "research_only" | "attention" | "refreshing" => AMBER,
        "actionable" | "decision_useful" => CYAN,
        "monitor" | "monitor_only" => MAGENTA,
        _ => INK,
    }
}

fn abbreviate(value: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= max_len {
        return value.to_string();
    }
    if max_len <= 3 {
        return chars.into_iter().take(max_len).collect();
    }
    let mut output = chars.into_iter().take(max_len - 3).collect::<String>();
    output.push_str("...");
    output
}

fn friendly_source(source: &str) -> String {
    if source.starts_with("command ") {
        "local snapshot command".to_string()
    } else {
        source.to_string()
    }
}

fn operator_action_summary(action: &str) -> String {
    let stripped = strip_inline_code(action);
    let mut cleaned = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
    cleaned = cleaned.replace(" with before ", " before ");
    cleaned = cleaned.replace(" with.", ".");
    cleaned = cleaned.trim().trim_end_matches(" with").trim().to_string();
    if cleaned.is_empty() {
        "Review the current page.".to_string()
    } else {
        abbreviate(&cleaned, 140)
    }
}

fn strip_inline_code(value: &str) -> String {
    value
        .split('`')
        .enumerate()
        .filter_map(|(index, part)| (index % 2 == 0).then_some(part))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::app::DashboardApp;
    use crate::client::{SnapshotFilters, SnapshotSource};
    use crate::model::{Page, SnapshotView};

    #[test]
    fn render_loading_state_is_a_real_dashboard_not_a_blank_box() {
        let source = SnapshotSource::Command {
            command: "catalyst-radar dashboard-snapshot --json --fast".to_string(),
        };
        let mut app = DashboardApp::new(
            source,
            Page::Overview,
            SnapshotFilters::default(),
            Duration::from_secs(30),
        );
        app.loading = true;
        let text = render_to_text(&app, 140, 42).expect("render text");

        assert!(text.contains("MARKET COMMAND CENTER"));
        assert!(text.contains("Loading market snapshot"));
        assert!(text.contains("ZERO PROVIDER CALLS"));
        assert!(text.contains("ATTENTION QUEUE"));
        assert!(text.contains("DECISION STATUS"));
        assert!(!text.contains("Loading dashboard snapshot..."));
    }

    #[test]
    fn render_inbox_contains_navigation_cards_and_next_action() {
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
        let text = render_to_text(&app, 140, 42).expect("render text");

        assert!(text.contains("MARKETRADAR"));
        assert!(text.contains("COMMAND CENTER"));
        assert!(text.contains("1 Inbox"));
        assert!(text.contains("NEXT SAFE ACTION"));
        assert!(text.contains("ATTENTION QUEUE"));
        assert!(text.contains("Import bars"));
        assert!(text.contains("MSFT"));
    }

    #[test]
    fn footer_hides_long_snapshot_command() {
        let source = SnapshotSource::Command {
            command: "& 'C:\\Users\\fpan1\\MarketRadar\\.venv\\Scripts\\python.exe' -m catalyst_radar.cli dashboard-snapshot --json --fast".to_string(),
        };
        let app = DashboardApp::new(
            source,
            Page::Overview,
            SnapshotFilters::default(),
            Duration::from_secs(30),
        );
        let text = render_to_text(&app, 140, 42).expect("render text");

        assert!(text.contains("source=local snapshot command"));
        assert!(!text.contains("C:\\Users\\fpan1"));
    }

    #[test]
    fn action_summary_removes_command_chunks() {
        let summary = operator_action_summary(
            "Seed or refresh the universe with `catalyst-radar ingest-csv --securities data/sample/securities.csv` before relying on broad discovery.",
        );

        assert_eq!(
            summary,
            "Seed or refresh the universe before relying on broad discovery."
        );
        assert!(!summary.contains("catalyst-radar"));
    }
}
