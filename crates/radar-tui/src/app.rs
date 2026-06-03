use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::client::{SnapshotFilters, SnapshotRequest, SnapshotSource, fetch_snapshot};
use crate::model::{Page, SnapshotView};

#[derive(Debug)]
enum RefreshMessage {
    Loaded(Result<Value, String>),
}

pub struct DashboardApp {
    pub page: Page,
    pub source_label: String,
    pub snapshot: Option<SnapshotView>,
    pub error: Option<String>,
    pub loading: bool,
    pub pending_refresh: bool,
    pub should_quit: bool,
    pub last_refresh: Option<Instant>,
    pub refresh_every: Duration,
    filters: SnapshotFilters,
    source: SnapshotSource,
    tx: Sender<RefreshMessage>,
    rx: Receiver<RefreshMessage>,
}

impl DashboardApp {
    pub fn new(
        source: SnapshotSource,
        page: Page,
        filters: SnapshotFilters,
        refresh_every: Duration,
    ) -> Self {
        let source_label = source.label();
        let (tx, rx) = channel();
        Self {
            page,
            source_label,
            snapshot: None,
            error: None,
            loading: false,
            pending_refresh: false,
            should_quit: false,
            last_refresh: None,
            refresh_every,
            filters,
            source,
            tx,
            rx,
        }
    }

    pub fn with_snapshot(page: Page, snapshot: SnapshotView) -> Self {
        let source = SnapshotSource::Command {
            command: "test".to_string(),
        };
        let mut app = Self::new(
            source,
            page,
            SnapshotFilters::default(),
            Duration::from_secs(30),
        );
        app.snapshot = Some(snapshot);
        app.last_refresh = Some(Instant::now());
        app
    }

    pub fn request_refresh(&mut self) {
        if self.loading {
            self.pending_refresh = true;
            return;
        }
        self.loading = true;
        self.pending_refresh = false;
        self.error = None;
        let source = self.source.clone();
        let request = SnapshotRequest {
            page: self.page,
            requested_page: None,
            filters: self.filters.clone(),
        };
        let tx = self.tx.clone();
        thread::spawn(move || {
            let result = fetch_snapshot(&source, &request).map_err(|err| err.to_string());
            let _ = tx.send(RefreshMessage::Loaded(result));
        });
    }

    pub fn handle_refresh_messages(&mut self) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                RefreshMessage::Loaded(Ok(value)) => {
                    self.snapshot = Some(SnapshotView::from_value(value));
                    self.error = None;
                    self.last_refresh = Some(Instant::now());
                    self.loading = false;
                    if self.pending_refresh {
                        self.request_refresh();
                    }
                }
                RefreshMessage::Loaded(Err(error)) => {
                    self.error = Some(error);
                    self.last_refresh = Some(Instant::now());
                    self.loading = false;
                    if self.pending_refresh {
                        self.request_refresh();
                    }
                }
            }
        }
    }

    pub fn refresh_due(&self) -> bool {
        if self.loading {
            return false;
        }
        match self.last_refresh {
            Some(last_refresh) => last_refresh.elapsed() >= self.refresh_every,
            None => true,
        }
    }

    pub fn set_page(&mut self, page: Page) {
        if self.page == page {
            return;
        }
        self.page = page;
        self.request_refresh();
    }

    pub fn next_page(&mut self) {
        self.set_page(self.page.next());
    }

    pub fn previous_page(&mut self) {
        self.set_page(self.page.previous());
    }
}
