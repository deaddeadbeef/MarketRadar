# Phase 11 Alerts And Feedback Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add shadow-mode alert artifacts, deterministic alert routing, dedupe/suppression records, alert feedback, and alert review surfaces so the product can measure whether notifications are useful before any real-capital workflow.

**Architecture:** Keep alert generation deterministic and database-backed. Build alerts from point-in-time candidate states, candidate packets, and Decision Cards; persist every planned alert and every suppression; keep external delivery dry-run/shadow by default. Use `useful_alert_labels` as the validation metric source and add `user_feedback` as the audit trail for feedback submissions.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite/PostgreSQL-compatible schema, FastAPI, Streamlit, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ 95aa81e feat: add api and dashboard review surfaces
```

Verified baseline:

```text
python -m pytest
325 passed in 85.15s (0:01:25)

python -m ruff check src tests apps
All checks passed!
```

Relevant existing capabilities:

- Candidate states store `ticker`, `as_of`, `state`, `previous_state`, `final_score`, `score_delta_5d`, hard blocks, transition reasons, policy version, and `created_at`.
- Candidate packets and Decision Cards are persisted with `source_ts`, `available_at`, payloads, and point-in-time repository helpers.
- Phase 10 added API/dashboard feedback capture and validates `candidate_packet`, `decision_card`, and `paper_trade` artifacts.
- `useful_alert_labels` already supports useful/noisy/late/ignored/acted feedback labels, but `artifact_type="alert"` currently validates against `candidate_states`; Phase 11 must replace that with a real `alerts` table.
- Dashboard data helpers are the shared read layer for API and Streamlit.

Important current limits:

- No `alerts`, `alert_suppressions`, or `user_feedback` tables exist.
- No alert routing, dedupe, digest grouping, or dry-run delivery exists.
- No alert API or alert dashboard page exists.
- No external channel should be enabled in this phase.

## Scope

In this phase, implement:

- Alert models, deterministic IDs, routes, channels, statuses, and trigger fingerprints.
- `alerts`, `alert_suppressions`, and `user_feedback` schema plus PostgreSQL migration.
- Alert repository with point-in-time list/detail queries, upsert, suppression recording, and feedback audit writes.
- Alert planner that reads latest point-in-time candidate states with optional candidate packets and Decision Cards.
- Dedupe rules that suppress unchanged repeat alerts but allow state changes, threshold score-delta triggers, new high-quality evidence triggers, and invalidation/weakening triggers.
- Dry-run channel abstraction and digest grouping. No real email/webhook send by default.
- CLI commands for planning alerts, listing alerts, rendering a digest, and dry-run delivery.
- FastAPI routes for alert list/detail and alert feedback.
- Dashboard data helpers and a Streamlit Alerts page.
- Validation/cost useful-alert summaries that count feedback on real alert artifacts.

Out of scope:

- Real email/Telegram/Slack delivery credentials.
- Scheduler/worker automation.
- Authentication and authorization.
- Broker/order placement.
- Alert tuning based on live performance.

## File Structure

Create:

- `src/catalyst_radar/alerts/__init__.py`
- `src/catalyst_radar/alerts/models.py`
- `src/catalyst_radar/alerts/routing.py`
- `src/catalyst_radar/alerts/dedupe.py`
- `src/catalyst_radar/alerts/planner.py`
- `src/catalyst_radar/alerts/digest.py`
- `src/catalyst_radar/alerts/channels/__init__.py`
- `src/catalyst_radar/alerts/channels/base.py`
- `src/catalyst_radar/alerts/channels/email.py`
- `src/catalyst_radar/alerts/channels/webhook.py`
- `src/catalyst_radar/storage/alert_repositories.py`
- `src/catalyst_radar/feedback/__init__.py`
- `src/catalyst_radar/feedback/service.py`
- `src/catalyst_radar/api/routes/alerts.py`
- `apps/dashboard/pages/6_Alerts.py`
- `sql/migrations/010_alerts.sql`
- `tests/unit/test_alert_routing.py`
- `tests/unit/test_alert_dedupe.py`
- `tests/unit/test_alert_digest.py`
- `tests/integration/test_alert_repository.py`
- `tests/integration/test_alerts_cli.py`
- `tests/integration/test_alert_api_routes.py`
- `docs/phase-11-review.md`

Modify:

- `src/catalyst_radar/storage/schema.py`
- `src/catalyst_radar/cli.py`
- `apps/api/main.py`
- `src/catalyst_radar/api/routes/feedback.py`
- `src/catalyst_radar/dashboard/data.py`
- `apps/dashboard/Home.py`
- `tests/integration/test_api_routes.py`
- `tests/integration/test_dashboard_data.py`

## Data Contracts

Alert row:

```text
id
ticker
as_of
source_ts
available_at
candidate_state_id
candidate_packet_id
decision_card_id
action_state
route
channel
priority
status
dedupe_key
trigger_kind
trigger_fingerprint
title
summary
feedback_url
payload
created_at
sent_at
```

Alert suppression row:

```text
id
ticker
as_of
available_at
candidate_state_id
decision_card_id
route
dedupe_key
trigger_kind
trigger_fingerprint
reason
payload
created_at
```

User feedback row:

```text
id
artifact_type
artifact_id
ticker
label
notes
source
payload
created_at
```

Allowed feedback labels:

```text
useful, noisy, too_late, too_early, ignored, acted
```

Allowed alert routes:

```text
immediate_manual_review
warning_digest
daily_digest
position_watch
```

Allowed alert channels:

```text
dashboard, digest, email, webhook
```

Allowed alert statuses:

```text
planned, dry_run, sent, failed
```

## Invariants

Decision-support invariant:

```text
Alert and dashboard copy must use review, candidate, evidence, setup, simulated-paper, and manual-review wording. It must not say buy now, sell now, execute, place order, or automatic trade.
```

Human-boundary invariant:

```text
Alerts are notifications and review prompts only. They must not mutate candidate state, policy output, Decision Cards, paper trades, or portfolio data.
```

Point-in-time invariant:

```text
Alert planning must use only rows visible at available_at. Candidate states require created_at <= available_at. Candidate packets, Decision Cards, events, snippets, and paper rows require available_at <= cutoff.
```

Dedupe invariant:

```text
Rerunning alert planning with the same inputs must not create duplicate active alerts. It must record suppression rows for skipped duplicate triggers.
```

Delivery invariant:

```text
External channels are dry-run only unless a caller explicitly asks for delivery and supplies a channel adapter. Phase 11 CLI and tests must use dry-run behavior.
```

Feedback invariant:

```text
Feedback writes user_feedback for audit and useful_alert_labels for validation metrics. Feedback must validate that the artifact exists and the ticker matches the artifact.
```

## Routing Rules

Use these deterministic routing rules:

| Candidate state | Condition | Route | Channel | Priority |
| --- | --- | --- | --- | --- |
| `EligibleForManualBuyReview` | Decision Card visible | `immediate_manual_review` | `dashboard` | `high` |
| `Warning` | `score_delta_5d >= 10` | `warning_digest` | `digest` | `high` |
| `ResearchOnly` | always | `daily_digest` | `digest` | `normal` |
| `AddToWatchlist` | always | `daily_digest` | `digest` | `normal` |
| `ThesisWeakening` | always | `position_watch` | `dashboard` | `high` |
| `ExitInvalidateReview` | always | `position_watch` | `dashboard` | `critical` |
| `Blocked` | always | no alert | none | none |
| `NoAction` | always | no alert | none | none |
| `Warning` | `score_delta_5d < 10` | no alert | none | none |

Trigger fingerprint rules:

```text
state_transition:<previous_state>-><state>
score_delta:<floor(score_delta_5d / 5) * 5>
event:<top_supporting_evidence.source_id or source_url or title_hash>
invalidation:<state>:<invalidation_price or action_state>
```

Stable dedupe key:

```text
alert-dedupe-v1:<ticker>:<route>:<action_state>:<trigger_kind>:<trigger_fingerprint>
```

Do not use raw `candidate_state_id`, `candidate_packet_id`, or `decision_card_id` as the primary dedupe key because those can change across rebuilds.

## Task 1: Alert Schema, Models, And Repository

**Files:**

- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `sql/migrations/010_alerts.sql`
- Create: `src/catalyst_radar/alerts/__init__.py`
- Create: `src/catalyst_radar/alerts/models.py`
- Create: `src/catalyst_radar/storage/alert_repositories.py`
- Test: `tests/integration/test_alert_repository.py`

- [ ] **Step 1: Write failing repository tests**

Create `tests/integration/test_alert_repository.py` with tests named:

```python
def test_alert_repository_upserts_and_lists_visible_alerts(tmp_path): ...
def test_alert_repository_filters_future_alerts(tmp_path): ...
def test_alert_repository_records_suppression(tmp_path): ...
def test_alert_repository_records_user_feedback_and_useful_label(tmp_path): ...
```

The fixture must create a SQLite engine with `create_schema(engine)`, insert one alert at `AVAILABLE_AT`, one future alert at `FUTURE_AT`, one suppression, and one feedback row.

Run:

```powershell
python -m pytest tests/integration/test_alert_repository.py
```

Expected before implementation:

```text
ModuleNotFoundError: No module named 'catalyst_radar.alerts'
```

- [ ] **Step 2: Add alert tables to SQLAlchemy schema**

In `src/catalyst_radar/storage/schema.py`, add:

```python
alerts = Table(
    "alerts",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("candidate_state_id", String),
    Column("candidate_packet_id", String),
    Column("decision_card_id", String),
    Column("action_state", String, nullable=False),
    Column("route", String, nullable=False),
    Column("channel", String, nullable=False),
    Column("priority", String, nullable=False),
    Column("status", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("trigger_kind", String, nullable=False),
    Column("trigger_fingerprint", String, nullable=False),
    Column("title", Text, nullable=False),
    Column("summary", Text, nullable=False),
    Column("feedback_url", Text),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("sent_at", DateTime(timezone=True)),
)

alert_suppressions = Table(
    "alert_suppressions",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("candidate_state_id", String),
    Column("decision_card_id", String),
    Column("route", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("trigger_kind", String, nullable=False),
    Column("trigger_fingerprint", String, nullable=False),
    Column("reason", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

user_feedback = Table(
    "user_feedback",
    metadata,
    Column("id", String, primary_key=True),
    Column("artifact_type", String, nullable=False),
    Column("artifact_id", String, nullable=False),
    Column("ticker", String, nullable=False),
    Column("label", String, nullable=False),
    Column("notes", Text),
    Column("source", String, nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
```

Add indexes:

```python
Index("ix_alerts_ticker_available_at", alerts.c.ticker, alerts.c.available_at)
Index("ix_alerts_route_status", alerts.c.route, alerts.c.status)
Index("ix_alerts_dedupe_key", alerts.c.dedupe_key)
Index("ix_alert_suppressions_dedupe_key", alert_suppressions.c.dedupe_key)
Index("ix_user_feedback_artifact", user_feedback.c.artifact_type, user_feedback.c.artifact_id)
```

- [ ] **Step 3: Add PostgreSQL migration**

Create `sql/migrations/010_alerts.sql` with PostgreSQL-compatible `CREATE TABLE IF NOT EXISTS` statements for `alerts`, `alert_suppressions`, and `user_feedback`, using `JSONB` for payloads and `TIMESTAMPTZ` for timestamps. Include the same indexes as Step 2.

- [ ] **Step 4: Implement alert models**

Create `src/catalyst_radar/alerts/models.py` with:

```python
class AlertRoute(StrEnum): ...
class AlertChannel(StrEnum): ...
class AlertStatus(StrEnum): ...
class AlertPriority(StrEnum): ...

@dataclass(frozen=True)
class Alert: ...

@dataclass(frozen=True)
class AlertSuppression: ...

@dataclass(frozen=True)
class UserFeedback: ...

def alert_id(*, ticker: str, route: str, dedupe_key: str, available_at: datetime) -> str: ...
def alert_suppression_id(*, dedupe_key: str, reason: str, available_at: datetime) -> str: ...
def user_feedback_id(*, artifact_type: str, artifact_id: str, label: str, created_at: datetime) -> str: ...
```

Rules:

- All tickers uppercase.
- All datetimes timezone-aware UTC.
- `status` coerces to `AlertStatus`.
- `route` coerces to `AlertRoute`.
- `channel` coerces to `AlertChannel`.
- `priority` coerces to `AlertPriority`.
- IDs must be deterministic and stable for the same inputs.

- [ ] **Step 5: Implement repository**

Create `src/catalyst_radar/storage/alert_repositories.py` with:

```python
class AlertRepository:
    def __init__(self, engine: Engine) -> None: ...
    def upsert_alert(self, alert: Alert) -> None: ...
    def insert_suppression(self, suppression: AlertSuppression) -> None: ...
    def latest_alert_by_dedupe_key(self, dedupe_key: str, available_at: datetime) -> Alert | None: ...
    def alert_by_id(self, alert_id: str, available_at: datetime | None = None) -> Alert | None: ...
    def list_alerts(
        self,
        *,
        available_at: datetime | None = None,
        ticker: str | None = None,
        status: str | None = None,
        route: str | None = None,
        limit: int = 200,
    ) -> list[Alert]: ...
    def list_suppressions(self, *, available_at: datetime | None = None, limit: int = 200) -> list[AlertSuppression]: ...
    def insert_user_feedback(self, feedback: UserFeedback) -> None: ...
    def latest_feedback(self, *, artifact_type: str, artifact_id: str) -> UserFeedback | None: ...
```

Implementation rules:

- Use explicit delete/insert upsert like other repositories.
- Preserve point-in-time filtering with `available_at <= cutoff`.
- Sort newest alerts first by `available_at`, `created_at`, then `id`.
- Do not mutate candidate states or decision cards.

- [ ] **Step 6: Run focused verification**

Run:

```powershell
python -m pytest tests/integration/test_alert_repository.py
python -m ruff check src tests
```

Expected:

```text
4 passed
All checks passed!
```

## Task 2: Alert Routing And Dedupe

**Files:**

- Create: `src/catalyst_radar/alerts/routing.py`
- Create: `src/catalyst_radar/alerts/dedupe.py`
- Test: `tests/unit/test_alert_routing.py`
- Test: `tests/unit/test_alert_dedupe.py`

- [ ] **Step 1: Write routing tests**

Create `tests/unit/test_alert_routing.py` with tests:

```python
def test_routes_eligible_manual_review_to_immediate_alert(): ...
def test_routes_high_delta_warning_to_digest(): ...
def test_suppresses_low_delta_warning(): ...
def test_routes_research_and_watchlist_to_daily_digest(): ...
def test_routes_thesis_weakening_and_exit_review_to_position_watch(): ...
def test_suppresses_blocked_and_no_action(): ...
def test_routing_requires_decision_card_for_manual_review(): ...
```

Use small dictionaries or dataclasses representing candidate rows with fields:

```text
ticker, as_of, state, previous_state, final_score, score_delta_5d,
candidate_state_id, candidate_packet_id, decision_card_id,
top_supporting_evidence, entry_zone, invalidation_price
```

- [ ] **Step 2: Implement routing**

Create `src/catalyst_radar/alerts/routing.py` with:

```python
@dataclass(frozen=True)
class AlertCandidate:
    ticker: str
    as_of: datetime
    source_ts: datetime
    available_at: datetime
    candidate_state_id: str
    action_state: ActionState
    previous_state: ActionState | None
    final_score: float
    score_delta_5d: float
    hard_blocks: Sequence[str] = ()
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    top_supporting_evidence: Mapping[str, Any] | None = None
    entry_zone: Sequence[float] | None = None
    invalidation_price: float | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class AlertRouteDecision:
    should_alert: bool
    route: AlertRoute | None
    channel: AlertChannel | None
    priority: AlertPriority | None
    trigger_kind: str
    trigger_fingerprint: str
    reason: str
```

Add:

```python
def route_alert(candidate: AlertCandidate, *, warning_delta_threshold: float = 10.0) -> AlertRouteDecision: ...
def alert_title(candidate: AlertCandidate, decision: AlertRouteDecision) -> str: ...
def alert_summary(candidate: AlertCandidate, decision: AlertRouteDecision) -> str: ...
```

Implementation rules:

- `EligibleForManualBuyReview` requires `decision_card_id`; otherwise suppress with reason `manual_review_missing_decision_card`.
- Low-delta `Warning` suppresses with reason `warning_delta_below_threshold`.
- `Blocked` and `NoAction` suppress with reason `state_not_alertable`.
- Summary must say “manual review”, “candidate”, “evidence”, or “review”; never use prohibited recommendation wording.

- [ ] **Step 3: Write dedupe tests**

Create `tests/unit/test_alert_dedupe.py` with tests:

```python
def test_dedupe_key_is_stable_for_same_trigger(): ...
def test_state_change_produces_distinct_dedupe_key(): ...
def test_new_evidence_produces_distinct_dedupe_key(): ...
def test_score_delta_bucket_produces_distinct_key_when_threshold_moves(): ...
def test_duplicate_existing_alert_returns_suppression_decision(): ...
```

- [ ] **Step 4: Implement dedupe**

Create `src/catalyst_radar/alerts/dedupe.py` with:

```python
@dataclass(frozen=True)
class DedupeDecision:
    emit: bool
    dedupe_key: str
    reason: str | None = None

def trigger_fingerprint(candidate: AlertCandidate, decision: AlertRouteDecision) -> str: ...
def alert_dedupe_key(candidate: AlertCandidate, decision: AlertRouteDecision) -> str: ...
def decide_dedupe(existing_alert: Alert | None, dedupe_key: str) -> DedupeDecision: ...
```

Rules:

- Dedupe key uses `ticker`, `route`, `action_state`, `trigger_kind`, and `trigger_fingerprint`.
- State-transition fingerprint includes previous and current state.
- Event fingerprint prefers `source_id`, then `source_url`, then stable title hash.
- Score-delta fingerprint uses a 5-point bucket only after threshold is met.
- If `existing_alert` exists, return `emit=False` and `reason="duplicate_trigger"`.

- [ ] **Step 5: Run focused verification**

Run:

```powershell
python -m pytest tests/unit/test_alert_routing.py tests/unit/test_alert_dedupe.py
python -m ruff check src tests
```

Expected:

```text
12 passed
All checks passed!
```

## Task 3: Alert Planner And CLI

**Files:**

- Create: `src/catalyst_radar/alerts/planner.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_alerts_cli.py`

- [ ] **Step 1: Write CLI tests**

Create `tests/integration/test_alerts_cli.py` with tests:

```python
def test_build_alerts_creates_visible_alert_and_lists_it(tmp_path, monkeypatch, capsys): ...
def test_build_alerts_rerun_records_suppression_not_duplicate(tmp_path, monkeypatch, capsys): ...
def test_build_alerts_does_not_use_future_decision_card(tmp_path, monkeypatch, capsys): ...
def test_alert_digest_groups_digest_routes(tmp_path, monkeypatch, capsys): ...
def test_send_alerts_dry_run_marks_alerts_without_external_delivery(tmp_path, monkeypatch, capsys): ...
```

Fixture requirements:

- Insert at least:
  - `EligibleForManualBuyReview` candidate state with visible decision card.
  - high-delta `Warning` candidate state.
  - low-delta `Warning` candidate state.
  - `ResearchOnly` candidate state.
  - future decision card that should not be visible before `available_at`.
- Assert rerun creates one alert and one suppression for the same dedupe key.

- [ ] **Step 2: Implement planner**

Create `src/catalyst_radar/alerts/planner.py` with:

```python
@dataclass(frozen=True)
class AlertPlanResult:
    alerts: tuple[Alert, ...]
    suppressions: tuple[AlertSuppression, ...]

def plan_alerts(
    alert_repo: AlertRepository,
    *,
    as_of: datetime,
    available_at: datetime,
    ticker: str | None = None,
    limit: int = 200,
) -> AlertPlanResult: ...
```

Data access rules:

- Query `candidate_states` where `as_of <= requested as_of` and `created_at <= available_at`.
- Rank latest candidate state per ticker by `as_of desc`, `created_at desc`, `id desc`.
- Join or lookup latest visible candidate packet by `candidate_state_id` with `candidate_packets.available_at <= available_at`.
- Join or lookup latest visible decision card by packet ID with `decision_cards.available_at <= available_at`.
- Do not use `candidate_packets_for_replay()` or `decision_cards_for_replay()` because they currently discard `available_at`.

Persistence rules:

- If routing suppresses, record an `AlertSuppression`.
- If routing emits but an alert with the same dedupe key already exists before cutoff, record duplicate suppression.
- If routing emits and dedupe passes, upsert an `Alert` with `status="planned"`.
- All payloads include source IDs and audit fields.

- [ ] **Step 3: Add CLI parsers**

In `build_parser()` add:

```python
build_alerts = subparsers.add_parser("build-alerts")
build_alerts.add_argument("--as-of", type=date.fromisoformat, required=True)
build_alerts.add_argument("--available-at", type=_parse_aware_datetime)
build_alerts.add_argument("--ticker")
build_alerts.add_argument("--json", action="store_true")

alerts_list = subparsers.add_parser("alerts-list")
alerts_list.add_argument("--ticker")
alerts_list.add_argument("--status")
alerts_list.add_argument("--route")
alerts_list.add_argument("--available-at", type=_parse_aware_datetime)
alerts_list.add_argument("--json", action="store_true")

alert_digest = subparsers.add_parser("alert-digest")
alert_digest.add_argument("--available-at", type=_parse_aware_datetime)
alert_digest.add_argument("--json", action="store_true")

send_alerts = subparsers.add_parser("send-alerts")
send_alerts.add_argument("--available-at", type=_parse_aware_datetime)
send_alerts.add_argument("--dry-run", action="store_true", default=True)
send_alerts.add_argument("--json", action="store_true")
```

- [ ] **Step 4: Add CLI command handlers**

In `main()` add handlers:

```python
if args.command == "build-alerts": ...
if args.command == "alerts-list": ...
if args.command == "alert-digest": ...
if args.command == "send-alerts": ...
```

Expected text outputs:

```text
built_alerts alerts=2 suppressions=1 available_at=...
MSFT alert route=immediate_manual_review status=planned dedupe_key=...
alert_digest groups=2 alerts=3 suppressed=1
send_alerts dry_run=true alerts=3
```

Rules:

- `send-alerts` must require dry-run behavior in Phase 11. If `--dry-run` is false or omitted by future parser changes, return nonzero with `external delivery is not enabled in Phase 11`.
- JSON output must be deterministic: sort keys and stable row ordering.

- [ ] **Step 5: Run focused verification**

Run:

```powershell
python -m pytest tests/integration/test_alerts_cli.py
python -m ruff check src tests
```

Expected:

```text
5 passed
All checks passed!
```

## Task 4: Feedback Service And API Routes

**Files:**

- Create: `src/catalyst_radar/feedback/__init__.py`
- Create: `src/catalyst_radar/feedback/service.py`
- Create: `src/catalyst_radar/api/routes/alerts.py`
- Modify: `src/catalyst_radar/api/routes/feedback.py`
- Modify: `apps/api/main.py`
- Test: `tests/integration/test_alert_api_routes.py`
- Modify: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Write API tests**

Create `tests/integration/test_alert_api_routes.py` with tests:

```python
def test_get_alerts_returns_rows(tmp_path, monkeypatch): ...
def test_get_alert_detail_returns_404_for_missing_alert(tmp_path, monkeypatch): ...
def test_get_alert_detail_returns_payload(tmp_path, monkeypatch): ...
def test_post_alert_feedback_records_user_feedback_and_useful_label(tmp_path, monkeypatch): ...
def test_generic_feedback_validates_alert_table(tmp_path, monkeypatch): ...
def test_alert_feedback_rejects_ticker_mismatch(tmp_path, monkeypatch): ...
```

Extend `tests/integration/test_api_routes.py` so `POST /api/feedback` with `artifact_type="alert"` succeeds for a real alert ID and fails for a missing alert ID.

- [ ] **Step 2: Implement shared feedback service**

Create `src/catalyst_radar/feedback/service.py` with:

```python
ALLOWED_FEEDBACK_LABELS = frozenset({"useful", "noisy", "too_late", "too_early", "ignored", "acted"})
ALLOWED_ARTIFACT_TYPES = frozenset({"candidate_packet", "decision_card", "paper_trade", "alert"})

@dataclass(frozen=True)
class FeedbackRecordResult:
    user_feedback: UserFeedback
    useful_label: UsefulAlertLabel

def record_feedback(
    engine: Engine,
    *,
    artifact_type: str,
    artifact_id: str,
    ticker: str,
    label: str,
    notes: str | None = None,
    source: str = "api",
    created_at: datetime | None = None,
) -> FeedbackRecordResult: ...
```

Validation rules:

- `candidate_packet` validates against `candidate_packets`.
- `decision_card` validates against `decision_cards`.
- `paper_trade` validates against `paper_trades`.
- `alert` validates against `alerts`.
- Ticker must match the referenced artifact.
- Write both `user_feedback` and `useful_alert_labels`.

- [ ] **Step 3: Update generic feedback API**

Refactor `src/catalyst_radar/api/routes/feedback.py` to call `record_feedback()` instead of duplicating artifact validation. Preserve response shape:

```json
{
  "id": "useful-alert-label-v1:...",
  "artifact_type": "alert",
  "artifact_id": "alert-...",
  "ticker": "MSFT",
  "label": "useful"
}
```

- [ ] **Step 4: Add alerts API router**

Create `src/catalyst_radar/api/routes/alerts.py`:

```python
router = APIRouter(prefix="/api/alerts", tags=["alerts"])

@router.get("")
def alerts(...): ...

@router.get("/{alert_id}")
def alert_detail(alert_id: str): ...

@router.post("/{alert_id}/feedback")
def alert_feedback(alert_id: str, request: AlertFeedbackRequest): ...
```

Register it in `apps/api/main.py`.

Rules:

- `GET /api/alerts` uses dashboard data helper or `AlertRepository`.
- Detail returns 404 for missing alert.
- Feedback endpoint resolves ticker from the alert; caller does not need to provide ticker.

- [ ] **Step 5: Run focused verification**

Run:

```powershell
python -m pytest tests/integration/test_alert_api_routes.py tests/integration/test_api_routes.py
python -m ruff check apps src tests
```

Expected:

```text
all alert and existing API tests pass
All checks passed!
```

## Task 5: Dashboard Data And Alerts Page

**Files:**

- Modify: `src/catalyst_radar/dashboard/data.py`
- Modify: `apps/dashboard/Home.py`
- Create: `apps/dashboard/pages/6_Alerts.py`
- Modify: `apps/dashboard/pages/4_Costs.py`
- Test: `tests/integration/test_dashboard_data.py`

- [ ] **Step 1: Extend dashboard data tests**

Add tests:

```python
def test_load_alert_rows_returns_latest_alerts_with_feedback(tmp_path): ...
def test_load_alert_detail_returns_payload_and_feedback(tmp_path): ...
def test_load_alert_rows_respects_available_at_cutoff(tmp_path): ...
def test_load_cost_summary_counts_useful_alert_feedback(tmp_path): ...
```

- [ ] **Step 2: Add dashboard data helpers**

In `src/catalyst_radar/dashboard/data.py`, add:

```python
def load_alert_rows(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    ticker: str | None = None,
    status: str | None = None,
    route: str | None = None,
) -> list[dict[str, object]]: ...

def load_alert_detail(
    engine: Engine,
    alert_id: str,
    *,
    available_at: datetime | None = None,
) -> dict[str, object] | None: ...
```

Include latest feedback label and notes for each alert when available.

Update `load_cost_summary()` so alert useful labels count when `artifact_type="alert"` and the alert points to a validation-result artifact through its `candidate_state_id`, `candidate_packet_id`, or `decision_card_id`.

- [ ] **Step 3: Add Alerts page**

Create `apps/dashboard/pages/6_Alerts.py` with:

- Sidebar filters for ticker, status, and route.
- Alert metric counts: total, planned, dry-run, high/critical priority, useful feedback count.
- Alert table with ticker, route, channel, priority, status, state, score trigger, dedupe key, title, available time, feedback.
- Detail section selected by alert ID text input.
- Feedback link or artifact ID display only; no direct browser form submission in this phase.

Copy rules:

- Use “review”, “candidate”, “alert”, “evidence”, and “feedback”.
- Do not use prohibited trade-instruction phrases.

- [ ] **Step 4: Update Home and Costs pages**

Home:

- Add a small “Recent Alerts” section below the candidate queue using `load_alert_rows(limit=10)` if available.
- Do not duplicate SQL in Streamlit pages.

Costs:

- Display alert useful count if present in cost summary.
- Keep default zero spend behavior.

- [ ] **Step 5: Run focused verification**

Run:

```powershell
python -m pytest tests/integration/test_dashboard_data.py
python -m ruff check apps src tests
```

Expected:

```text
dashboard data tests pass
All checks passed!
```

## Task 6: Digest And Dry-Run Channels

**Files:**

- Create: `src/catalyst_radar/alerts/digest.py`
- Create: `src/catalyst_radar/alerts/channels/__init__.py`
- Create: `src/catalyst_radar/alerts/channels/base.py`
- Create: `src/catalyst_radar/alerts/channels/email.py`
- Create: `src/catalyst_radar/alerts/channels/webhook.py`
- Test: `tests/unit/test_alert_digest.py`

- [ ] **Step 1: Write digest/channel tests**

Create `tests/unit/test_alert_digest.py` with:

```python
def test_digest_groups_alerts_by_route_and_priority(): ...
def test_digest_includes_suppressed_count(): ...
def test_dry_run_channel_marks_payload_without_network_send(): ...
def test_email_and_webhook_channels_are_disabled_without_explicit_adapter(): ...
```

- [ ] **Step 2: Implement digest renderer**

Create `src/catalyst_radar/alerts/digest.py`:

```python
@dataclass(frozen=True)
class AlertDigest:
    generated_at: datetime
    groups: Mapping[str, Sequence[Alert]]
    suppressed_count: int

def build_alert_digest(alerts: Sequence[Alert], suppressions: Sequence[AlertSuppression], generated_at: datetime) -> AlertDigest: ...
def digest_payload(digest: AlertDigest) -> dict[str, object]: ...
```

- [ ] **Step 3: Implement dry-run channels**

Create `channels/base.py` with:

```python
@dataclass(frozen=True)
class DeliveryResult:
    alert_id: str
    channel: str
    status: str
    dry_run: bool
    payload: Mapping[str, object]

class AlertChannelAdapter(Protocol):
    def deliver(self, alert: Alert, *, dry_run: bool = True) -> DeliveryResult: ...
```

Create `email.py` and `webhook.py` adapters that:

- Return dry-run result when `dry_run=True`.
- Raise `RuntimeError("external alert delivery is not enabled")` when `dry_run=False`.
- Never perform network I/O.

- [ ] **Step 4: Run focused verification**

Run:

```powershell
python -m pytest tests/unit/test_alert_digest.py
python -m ruff check src tests
```

Expected:

```text
4 passed
All checks passed!
```

## Task 7: Phase Review And Smoke

**Files:**

- Create: `docs/phase-11-review.md`
- Modify: `docs/superpowers/plans/2026-05-10-phase-11-alerts-feedback-loop.md`

- [ ] **Step 1: Run focused tests**

Run:

```powershell
python -m pytest tests/unit/test_alert_routing.py tests/unit/test_alert_dedupe.py tests/unit/test_alert_digest.py tests/integration/test_alert_repository.py tests/integration/test_alerts_cli.py tests/integration/test_alert_api_routes.py
python -m ruff check apps src tests
```

- [ ] **Step 2: Run full verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
```

- [ ] **Step 3: Run CLI smoke**

Run from a fresh local DB:

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
python -m catalyst_radar.cli build-packets --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-decision-cards --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-alerts --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli alerts-list --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli alert-digest --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli send-alerts --available-at 2026-05-10T14:00:00Z --dry-run
```

If sample fixtures produce no alertable candidates, document that result and run the integration fixture CLI smoke from `tests/integration/test_alerts_cli.py`.

- [ ] **Step 4: Run API smoke**

Start:

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8010
```

Open:

```text
http://127.0.0.1:8010/docs
http://127.0.0.1:8010/api/alerts
```

- [ ] **Step 5: Run dashboard smoke**

Start:

```powershell
$env:PYTHONPATH="src"
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
streamlit run apps/dashboard/Home.py --server.port 8509
```

Open:

```text
http://127.0.0.1:8509/Alerts
```

- [ ] **Step 6: Document phase outcome**

Create `docs/phase-11-review.md` with:

- Outcome.
- Verification outputs.
- CLI smoke result.
- API smoke result.
- Dashboard smoke result.
- Review findings and fixes.
- Residual risks.

## Exit Criteria

- `alerts`, `alert_suppressions`, and `user_feedback` tables exist in SQLAlchemy schema and PostgreSQL migration.
- Alert planning creates deterministic alert artifacts from point-in-time candidate data.
- Reruns create suppression rows instead of duplicate active alerts.
- Alert feedback validates real alert artifacts and writes both audit feedback and useful-alert labels.
- API exposes alert list/detail and alert feedback.
- Dashboard has a usable Alerts page.
- Dry-run delivery and digest rendering work without network I/O.
- Full pytest and ruff pass.
- Phase review document exists with smoke evidence.

## Review Gate

Before merge, run a review focused on:

- Point-in-time correctness in alert planning.
- Dedupe stability across reruns and rebuilds.
- No future candidate packet or Decision Card leakage.
- No prohibited trade-instruction wording.
- Feedback does not mutate candidate states, scores, policy outputs, Decision Cards, or paper trades.
- Dry-run channels do not perform network I/O.
