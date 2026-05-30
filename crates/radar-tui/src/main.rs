use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use clap::{ArgAction, Parser};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
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

fn handle_key(app: &mut DashboardApp, key: KeyEvent) {
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) | (KeyCode::Char('q'), _) => app.should_quit = true,
        (KeyCode::F(5), _) | (KeyCode::Char('r'), _) => app.request_refresh(),
        (KeyCode::Tab, _) | (KeyCode::Right, _) => app.next_page(),
        (KeyCode::BackTab, _) | (KeyCode::Left, _) => app.previous_page(),
        (KeyCode::Char('n'), KeyModifiers::CONTROL) => app.next_page(),
        (KeyCode::Char('p'), KeyModifiers::CONTROL) => app.previous_page(),
        (KeyCode::Char('a'), KeyModifiers::CONTROL) => app.set_page(Page::Agent),
        (KeyCode::Char('?'), _) => app.set_page(Page::Help),
        (KeyCode::Char('f'), _) | (KeyCode::Char('F'), _) => app.set_page(Page::Features),
        (KeyCode::Char(value), _) => {
            if value.is_ascii_digit() {
                app.set_page(Page::from_input(&value.to_string()));
            }
        }
        _ => {}
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
