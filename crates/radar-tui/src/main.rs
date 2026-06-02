use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{ArgAction, Parser};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use radar_tui::app::DashboardApp;
use radar_tui::client::{SnapshotFilters, SnapshotRequest, SnapshotSource, fetch_snapshot};
use radar_tui::model::{Page, SnapshotView};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

#[derive(Debug, Parser)]
#[command(author, version, about = "Rust terminal dashboard for MarketRadar")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8000")]
    api_base_url: String,
    #[arg(long)]
    api_role: Option<String>,
    #[arg(
        long,
        help = "Accept invalid local HTTPS certificates for the API client"
    )]
    allow_invalid_certs: bool,
    #[arg(
        long,
        help = "Shell command that emits dashboard JSON. Use {page} to place the active page."
    )]
    snapshot_command: Option<String>,
    #[arg(long, default_value = "overview")]
    page: String,
    #[arg(long)]
    database_url: Option<String>,
    #[arg(long)]
    ticker: Option<String>,
    #[arg(long)]
    available_at: Option<String>,
    #[arg(long)]
    alert_status: Option<String>,
    #[arg(long)]
    alert_route: Option<String>,
    #[arg(
        long = "scan-mode",
        visible_alias = "priced-in-status",
        default_value = "all"
    )]
    priced_in_status: String,
    #[arg(long)]
    usefulness: Option<String>,
    #[arg(long, action = ArgAction::Append)]
    source_gap: Vec<String>,
    #[arg(long, action = ArgAction::Append)]
    decision_gap: Vec<String>,
    #[arg(long)]
    stocks_only: bool,
    #[arg(long, default_value_t = 50)]
    scan_limit: u16,
    #[arg(long, default_value_t = 0)]
    scan_offset: u32,
    #[arg(long, default_value_t = 8)]
    telemetry_limit: u16,
    #[arg(long, default_value_t = 10)]
    refresh_seconds: u64,
    #[arg(long, help = "Fetch one snapshot, print a compact summary, and exit")]
    once: bool,
    #[arg(
        long,
        help = "Fetch one snapshot, render a static terminal frame, and exit"
    )]
    render_frame: bool,
    #[arg(
        long,
        default_value_t = 140,
        help = "Static frame width for --render-frame"
    )]
    frame_width: u16,
    #[arg(
        long,
        default_value_t = 42,
        help = "Static frame height for --render-frame"
    )]
    frame_height: u16,
    #[arg(
        long,
        help = "Render the initial loading dashboard instead of fetching data for --render-frame"
    )]
    loading_frame: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let page = Page::from_input(&args.page);
    let filters = args.snapshot_filters();
    let source = match args.snapshot_command {
        Some(command) => SnapshotSource::Command { command },
        None => SnapshotSource::Api {
            base_url: args.api_base_url,
            role: args.api_role,
            allow_invalid_certs: args.allow_invalid_certs,
        },
    };

    if args.render_frame {
        let app = static_frame_app(
            &source,
            page,
            filters.clone(),
            Duration::from_secs(args.refresh_seconds.max(1)),
            args.loading_frame,
        )?;
        print!(
            "{}",
            radar_tui::ui::render_to_text(&app, args.frame_width, args.frame_height)?
        );
        return Ok(());
    }

    if args.once {
        let request = SnapshotRequest { page, filters };
        let value = fetch_snapshot(&source, &request)?;
        let snapshot = SnapshotView::from_value(value);
        println!(
            "MarketRadar page={} mode={} status={} next_action={} next_command={} rows={} provider_calls={}",
            page.key(),
            snapshot.snapshot_mode,
            snapshot.status,
            empty_as_dash(&snapshot.next_action),
            empty_as_dash(&snapshot.next_command),
            snapshot.rows.len(),
            snapshot.external_calls
        );
        return Ok(());
    }

    let mut terminal = setup_terminal()?;
    let mut app = DashboardApp::new(
        source,
        page,
        filters,
        Duration::from_secs(args.refresh_seconds.max(1)),
    );
    app.request_refresh();
    let result = run_app(&mut terminal, &mut app);
    restore_terminal(&mut terminal)?;
    result
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut DashboardApp,
) -> Result<()> {
    while !app.should_quit {
        app.handle_refresh_messages();
        if app.refresh_due() {
            app.request_refresh();
        }
        terminal.draw(|frame| radar_tui::ui::render(frame, app))?;
        if event::poll(Duration::from_millis(80))?
            && let Event::Key(key) = event::read()?
        {
            handle_key(app, key);
        }
    }
    Ok(())
}

fn static_frame_app(
    source: &SnapshotSource,
    page: Page,
    filters: SnapshotFilters,
    refresh_every: Duration,
    loading_frame: bool,
) -> Result<DashboardApp> {
    let mut app = DashboardApp::new(source.clone(), page, filters.clone(), refresh_every);
    if loading_frame {
        app.loading = true;
        return Ok(app);
    }

    let request = SnapshotRequest { page, filters };
    let value = fetch_snapshot(source, &request)?;
    app.snapshot = Some(SnapshotView::from_value(value));
    app.last_refresh = Some(Instant::now());
    Ok(app)
}

fn handle_key(app: &mut DashboardApp, key: KeyEvent) {
    match key_action(key) {
        KeyAction::Quit => app.should_quit = true,
        KeyAction::Refresh => app.request_refresh(),
        KeyAction::NextPage => app.next_page(),
        KeyAction::PreviousPage => app.previous_page(),
        KeyAction::Jump(page) => app.set_page(page),
        KeyAction::None => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyAction {
    Quit,
    Refresh,
    NextPage,
    PreviousPage,
    Jump(Page),
    None,
}

fn key_action(key: KeyEvent) -> KeyAction {
    if key.kind == KeyEventKind::Release {
        return KeyAction::None;
    }

    let command_modifier = key
        .modifiers
        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);

    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match lower_char(key.code) {
            Some('a') => return KeyAction::Jump(Page::Agent),
            Some('n') => return KeyAction::NextPage,
            Some('p') => return KeyAction::PreviousPage,
            _ => {}
        }
    }

    match key.code {
        KeyCode::Esc => KeyAction::Quit,
        KeyCode::F(5) => KeyAction::Refresh,
        KeyCode::Tab | KeyCode::Right | KeyCode::Down | KeyCode::PageDown => KeyAction::NextPage,
        KeyCode::BackTab | KeyCode::Left | KeyCode::Up | KeyCode::PageUp => KeyAction::PreviousPage,
        KeyCode::Home => KeyAction::Jump(Page::first()),
        KeyCode::End => KeyAction::Jump(Page::last()),
        KeyCode::Char(value) if !command_modifier => plain_key_action(value),
        _ => KeyAction::None,
    }
}

fn plain_key_action(value: char) -> KeyAction {
    match value.to_ascii_lowercase() {
        'q' => KeyAction::Quit,
        'r' => KeyAction::Refresh,
        'j' => KeyAction::NextPage,
        'k' => KeyAction::PreviousPage,
        value if value.is_ascii_digit() => KeyAction::Jump(Page::from_input(&value.to_string())),
        '?' => KeyAction::Jump(Page::Help),
        'o' => KeyAction::Jump(Page::Overview),
        'e' | 'g' => KeyAction::Jump(Page::Readiness),
        's' => KeyAction::Jump(Page::Run),
        'c' => KeyAction::Jump(Page::Candidates),
        'd' => KeyAction::Jump(Page::Review),
        'a' => KeyAction::Jump(Page::Alerts),
        'i' => KeyAction::Jump(Page::Ipo),
        'b' => KeyAction::Jump(Page::Broker),
        't' => KeyAction::Jump(Page::Telemetry),
        'v' => KeyAction::Jump(Page::Costs),
        'f' => KeyAction::Jump(Page::Features),
        'h' => KeyAction::Jump(Page::Help),
        _ => KeyAction::None,
    }
}

fn lower_char(code: KeyCode) -> Option<char> {
    match code {
        KeyCode::Char(value) => Some(value.to_ascii_lowercase()),
        _ => None,
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(Into::into)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn empty_as_dash(value: &str) -> &str {
    if value.trim().is_empty() { "-" } else { value }
}

impl Args {
    fn snapshot_filters(&self) -> SnapshotFilters {
        SnapshotFilters {
            database_url: self.database_url.clone(),
            ticker: self.ticker.clone(),
            available_at: self.available_at.clone(),
            alert_status: self.alert_status.clone(),
            alert_route: self.alert_route.clone(),
            priced_in_status: self.priced_in_status.clone(),
            usefulness: self.usefulness.clone(),
            source_gap: self.source_gap.clone(),
            decision_gap: self.decision_gap.clone(),
            stocks_only: self.stocks_only,
            scan_limit: self.scan_limit,
            scan_offset: self.scan_offset,
            telemetry_limit: self.telemetry_limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn ctrl(value: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(value), KeyModifiers::CONTROL)
    }

    #[test]
    fn arrow_and_tab_keys_move_through_workflow() {
        assert_eq!(key_action(press(KeyCode::Down)), KeyAction::NextPage);
        assert_eq!(key_action(press(KeyCode::Right)), KeyAction::NextPage);
        assert_eq!(key_action(press(KeyCode::Tab)), KeyAction::NextPage);
        assert_eq!(key_action(press(KeyCode::PageDown)), KeyAction::NextPage);

        assert_eq!(key_action(press(KeyCode::Up)), KeyAction::PreviousPage);
        assert_eq!(key_action(press(KeyCode::Left)), KeyAction::PreviousPage);
        assert_eq!(key_action(press(KeyCode::BackTab)), KeyAction::PreviousPage);
        assert_eq!(key_action(press(KeyCode::PageUp)), KeyAction::PreviousPage);
    }

    #[test]
    fn home_end_and_letter_keys_jump_to_expected_pages() {
        assert_eq!(
            key_action(press(KeyCode::Home)),
            KeyAction::Jump(Page::first())
        );
        assert_eq!(
            key_action(press(KeyCode::End)),
            KeyAction::Jump(Page::last())
        );
        assert_eq!(
            key_action(press(KeyCode::Char('0'))),
            KeyAction::Jump(Page::Tutorial)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('9'))),
            KeyAction::Jump(Page::Telemetry)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('A'))),
            KeyAction::Jump(Page::Alerts)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('g'))),
            KeyAction::Jump(Page::Readiness)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('s'))),
            KeyAction::Jump(Page::Run)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('i'))),
            KeyAction::Jump(Page::Ipo)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('h'))),
            KeyAction::Jump(Page::Help)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('v'))),
            KeyAction::Jump(Page::Costs)
        );
    }

    #[test]
    fn control_shortcuts_and_release_events_are_stable() {
        assert_eq!(key_action(ctrl('a')), KeyAction::Jump(Page::Agent));
        assert_eq!(key_action(ctrl('A')), KeyAction::Jump(Page::Agent));
        assert_eq!(key_action(ctrl('n')), KeyAction::NextPage);
        assert_eq!(key_action(ctrl('p')), KeyAction::PreviousPage);

        let release =
            KeyEvent::new_with_kind(KeyCode::Down, KeyModifiers::empty(), KeyEventKind::Release);
        assert_eq!(key_action(release), KeyAction::None);

        let repeat =
            KeyEvent::new_with_kind(KeyCode::Down, KeyModifiers::empty(), KeyEventKind::Repeat);
        assert_eq!(key_action(repeat), KeyAction::NextPage);
    }
}
