# Phase 15 Security, Secrets, and Compliance Controls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Protect credentials, account-sensitive data, provider-license boundaries, audit history, role boundaries, and the manual decision-support/no-broker boundary before real-world testing.

**Architecture:** Add a small `catalyst_radar.security` package with separate modules for secrets, redaction, audit events, access roles, and provider license policy. Keep the security layer at system boundaries: config/env loading, provider/LLM error capture, API route dependencies, Streamlit page gates, prompt construction, feedback/paper-decision writes, and provider raw-record persistence. Preserve the current local-first workflow by making strict authentication configurable, while tests exercise fail-closed behavior.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite/Postgres-compatible schema, FastAPI dependencies, Streamlit guards, pytest, ruff, Docker Compose.

---

## Current State

- `.env` and `.env.local` are already ignored, but `.env.local` loading is duplicated in CLI, worker, and dashboard entrypoints.
- `AppConfig` stores raw secrets such as `polygon_api_key`.
- Polygon appends API keys to request URLs. Existing HTTP redaction only masks URL query parameters.
- OpenAI reads `OPENAI_API_KEY` directly and sends full candidate/evidence payloads.
- Provider raw records already include `license_tag` and `retention_policy`, but there is no central policy registry.
- No FastAPI or Streamlit role guard exists.
- Feedback, alert feedback, useful labels, paper decisions, paper outcome updates, hard-block bypasses, and model calls have no append-only audit log.
- Existing no-broker protections are mostly copy/schema tests and paper-trading `no_execution` payloads.

## Non-Goals

- Do not add broker integration, order routing, portfolio execution, or real trade placement.
- Do not add OAuth, SSO, or a user database. Phase 15 uses deterministic role headers/env controls suitable for a single-user pilot.
- Do not build a full retention purge scheduler. Phase 15 adds policy validation and queryable retention metadata; purge automation can follow once real providers are selected.
- Do not turn LLM review on by default.

---

## File Map

- Create `src/catalyst_radar/security/__init__.py`: exports security helpers.
- Create `src/catalyst_radar/security/secrets.py`: secret wrappers, environment loading policy, required-secret validation.
- Create `src/catalyst_radar/security/redaction.py`: recursive redaction, URL/DB URL masking, prompt minimization.
- Create `src/catalyst_radar/security/audit.py`: audit event model and append-only repository.
- Create `src/catalyst_radar/security/access.py`: role model and FastAPI/Streamlit guard helpers.
- Create `src/catalyst_radar/security/licenses.py`: provider license and retention policy registry.
- Create `sql/migrations/013_security_audit.sql`: `audit_events` schema.
- Create `docs/runbooks/secrets.md`: operator guidance for local and production secret handling.
- Modify `src/catalyst_radar/core/config.py`: central secret loading, role/auth config, sanitized config output.
- Modify `src/catalyst_radar/storage/schema.py`: add `audit_events` table.
- Modify `src/catalyst_radar/storage/db.py`: register migration/table creation.
- Modify `src/catalyst_radar/storage/provider_repositories.py`: validate license/retention and redact persisted error payloads.
- Modify `src/catalyst_radar/connectors/http.py`: replace local URL masking with security redaction.
- Modify `src/catalyst_radar/connectors/polygon.py`: use `SecretValue` and never expose keys through repr/logging.
- Modify `src/catalyst_radar/connectors/provider_ingest.py`: redact provider errors before health/jobs/incidents.
- Modify `src/catalyst_radar/agents/openai_client.py`: read OpenAI secret via security helper and redact/minimize prompt input.
- Modify `src/catalyst_radar/agents/router.py`: redact ledger error payloads and append model-call audit events.
- Modify `src/catalyst_radar/cli.py`: audited feedback/paper/LLM commands, override reason, redacted errors/config output.
- Modify `src/catalyst_radar/feedback/service.py`: append audit events for user feedback.
- Modify `src/catalyst_radar/api/routes/*.py`: add route role dependencies and redacted payloads where needed.
- Modify `apps/dashboard/Home.py` and `apps/dashboard/pages/*.py`: call shared dashboard role guard before data loads.
- Test `tests/unit/test_secrets.py`.
- Test `tests/unit/test_redaction.py`.
- Test `tests/unit/test_access_roles.py`.
- Test `tests/unit/test_provider_license_policy.py`.
- Test `tests/integration/test_audit_logs.py`.
- Test `tests/integration/test_security_boundaries.py`.

---

## Task 1: Secrets And Redaction Foundation

**Files:**

- Create: `src/catalyst_radar/security/__init__.py`
- Create: `src/catalyst_radar/security/secrets.py`
- Create: `src/catalyst_radar/security/redaction.py`
- Modify: `src/catalyst_radar/core/config.py`
- Test: `tests/unit/test_secrets.py`
- Test: `tests/unit/test_redaction.py`

- [ ] **Step 1: Write failing unit tests for secret values and env policy**

Create `tests/unit/test_secrets.py` with tests covering:

```python
from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.secrets import SecretValue, load_local_dotenv, required_secret


def test_secret_value_never_renders_plaintext() -> None:
    secret = SecretValue("sk-test-secret")

    assert secret.reveal() == "sk-test-secret"
    assert str(secret) == "<redacted>"
    assert repr(secret) == "SecretValue(<redacted>)"
    assert secret.masked() == "sk***et"


def test_required_secret_wraps_nonblank_env_value() -> None:
    secret = required_secret({"OPENAI_API_KEY": " sk-live "}, "OPENAI_API_KEY")

    assert secret.reveal() == "sk-live"


def test_required_secret_fails_closed_for_missing_value() -> None:
    with pytest.raises(ValueError, match="OPENAI_API_KEY is required"):
        required_secret({}, "OPENAI_API_KEY")


def test_local_dotenv_loader_refuses_production() -> None:
    with pytest.raises(ValueError, match="must not load .env.local in production"):
        load_local_dotenv(environment="production", dotenv_path=".env.local")


def test_app_config_sanitized_payload_masks_secrets() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_POLYGON_API_KEY": "polygon-secret",
            "CATALYST_DATABASE_URL": "postgresql://user:pass@db:5432/app",
        }
    )

    payload = config.sanitized()

    assert payload["polygon_api_key"] == "<redacted>"
    assert payload["database_url"] == "postgresql://user:<redacted>@db:5432/app"
```

Run:

```powershell
python -m pytest tests\unit\test_secrets.py -q
```

Expected: fails because `catalyst_radar.security.secrets` and `AppConfig.sanitized()` do not exist.

- [ ] **Step 2: Write failing unit tests for recursive redaction**

Create `tests/unit/test_redaction.py` with tests covering:

```python
from catalyst_radar.security.redaction import redact_text, redact_value


def test_redacts_secret_keys_recursively_without_mutating_input() -> None:
    payload = {
        "api_key": "abc123",
        "nested": [{"Authorization": "Bearer sk-test"}, {"safe": "value"}],
    }

    redacted = redact_value(payload)

    assert redacted == {
        "api_key": "<redacted>",
        "nested": [{"Authorization": "<redacted>"}, {"safe": "value"}],
    }
    assert payload["api_key"] == "abc123"


def test_redacts_database_urls_and_query_secrets_in_text() -> None:
    text = (
        "postgresql://user:pass@localhost:5432/db "
        "https://api.example.test/v1?apikey=abc&token=def&x=1"
    )

    redacted = redact_text(text)

    assert "pass" not in redacted
    assert "abc" not in redacted
    assert "def" not in redacted
    assert "postgresql://user:<redacted>@localhost:5432/db" in redacted
    assert "apikey=<redacted>" in redacted
    assert "token=<redacted>" in redacted


def test_redacts_known_secret_values_inside_error_text() -> None:
    redacted = redact_text(
        "request failed with OPENAI_API_KEY=sk-live-secret",
        known_secrets=("sk-live-secret",),
    )

    assert "sk-live-secret" not in redacted
```

Run:

```powershell
python -m pytest tests\unit\test_redaction.py -q
```

Expected: fails because redaction helpers do not exist.

- [ ] **Step 3: Implement `security/secrets.py`**

Implement these exact public helpers:

```python
@dataclass(frozen=True)
class SecretValue:
    _value: str

    def __post_init__(self) -> None:
        if not str(self._value).strip():
            raise ValueError("secret value must not be blank")
        object.__setattr__(self, "_value", str(self._value).strip())

    def reveal(self) -> str:
        return self._value

    def masked(self) -> str:
        if len(self._value) <= 4:
            return "<redacted>"
        return f"{self._value[:2]}***{self._value[-2:]}"

    def __str__(self) -> str:
        return "<redacted>"

    def __repr__(self) -> str:
        return "SecretValue(<redacted>)"


def required_secret(source: Mapping[str, str], key: str) -> SecretValue:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        raise ValueError(f"{key} is required")
    return SecretValue(raw)


def optional_secret(source: Mapping[str, str], key: str) -> SecretValue | None:
    raw = source.get(key)
    return None if raw is None or raw.strip() == "" else SecretValue(raw)


def load_local_dotenv(*, environment: str, dotenv_path: str = ".env.local") -> bool:
    if environment.strip().lower() in {"production", "prod"}:
        raise ValueError("must not load .env.local in production")
    from dotenv import load_dotenv

    return bool(load_dotenv(dotenv_path, override=False))
```

- [ ] **Step 4: Implement `security/redaction.py`**

Implement:

```python
SECRET_KEY_MARKERS = ("api_key", "apikey", "token", "password", "secret", "authorization")
REDACTED = "<redacted>"


def redact_value(value: Any, *, known_secrets: Sequence[str] = ()) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED
            if _is_secret_key(str(key))
            else redact_value(item, known_secrets=known_secrets)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact_value(item, known_secrets=known_secrets) for item in value]
    if isinstance(value, str):
        return redact_text(value, known_secrets=known_secrets)
    return value


def redact_text(text: str, *, known_secrets: Sequence[str] = ()) -> str:
    redacted = _redact_database_urls(text)
    redacted = _redact_query_params(redacted)
    redacted = _redact_authorization_values(redacted)
    for secret in known_secrets:
        if secret:
            redacted = redacted.replace(secret, REDACTED)
    return redacted


def redact_url(url: str, *, known_secrets: Sequence[str] = ()) -> str:
    return redact_text(url, known_secrets=known_secrets)
```

Use `urllib.parse.urlsplit`, `parse_qsl`, and `urlunsplit` for query masking. Use regex only for DB URL userinfo and authorization token text.

- [ ] **Step 5: Integrate sanitized config**

Modify `AppConfig`:

```python
from catalyst_radar.security.redaction import redact_value


def sanitized(self) -> dict[str, object]:
    return redact_value(asdict(self))
```

Keep `polygon_api_key` typed as `str | None` for compatibility in this task. Convert call sites to `SecretValue` in Task 2.

- [ ] **Step 6: Verify and commit**

Run:

```powershell
python -m pytest tests\unit\test_secrets.py tests\unit\test_redaction.py
python -m ruff check src\catalyst_radar\security src\catalyst_radar\core\config.py tests\unit\test_secrets.py tests\unit\test_redaction.py
git diff --check
git add src\catalyst_radar\security src\catalyst_radar\core\config.py tests\unit\test_secrets.py tests\unit\test_redaction.py
git commit -m "feat: add security secret redaction foundation"
```

---

## Task 2: Redaction Integration And LLM Prompt Minimization

**Files:**

- Modify: `src/catalyst_radar/connectors/http.py`
- Modify: `src/catalyst_radar/connectors/polygon.py`
- Modify: `src/catalyst_radar/connectors/provider_ingest.py`
- Modify: `src/catalyst_radar/agents/openai_client.py`
- Modify: `src/catalyst_radar/agents/router.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/unit/test_redaction.py`
- Test: `tests/integration/test_security_boundaries.py`

- [ ] **Step 1: Add failing integration tests for boundary redaction**

Create `tests/integration/test_security_boundaries.py` with:

```python
def test_provider_ingest_redacts_secret_from_health_job_and_incident(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'security.db').as_posix()}", future=True)
    create_schema(engine)

    class LeakyConnector:
        def healthcheck(self):
            return ConnectorHealth(
                provider="leaky",
                status=ConnectorHealthStatus.HEALTHY,
                checked_at=datetime(2026, 5, 10, tzinfo=UTC),
                reason="ok",
            )

        def fetch(self, request):
            raise RuntimeError("bad apikey=secret-token")

        def normalize(self, records):
            return []

    with pytest.raises(ProviderIngestError):
        ingest_provider_records(
            connector=LeakyConnector(),
            request=ConnectorRequest(
                provider="leaky",
                endpoint="test",
                params={},
                requested_at=datetime(2026, 5, 10, tzinfo=UTC),
            ),
            market_repo=MarketRepository(engine),
            provider_repo=ProviderRepository(engine),
            job_type="test",
            metadata={},
        )

    with engine.connect() as conn:
        job_error = conn.scalar(select(job_runs.c.error_summary))
        incident_reason = conn.scalar(select(data_quality_incidents.c.reason))

    assert "secret-token" not in job_error
    assert "secret-token" not in incident_reason
    assert "<redacted>" in job_error
```

Add a prompt minimization unit test in `tests/unit/test_redaction.py`:

```python
def test_minimize_prompt_payload_removes_account_sensitive_fields() -> None:
    payload = minimize_prompt_payload(
        {
            "candidate_packet": {
                "ticker": "MSFT",
                "payload": {
                    "portfolio_impact": {"portfolio_value": 100000, "cash": 5000},
                    "evidence": [{"source_id": "event-1", "source_url": "https://x?apikey=secret"}],
                },
            }
        }
    )

    text = json.dumps(payload)
    assert "portfolio_value" not in text
    assert "cash" not in text
    assert "secret" not in text
    assert "event-1" in text
```

- [ ] **Step 2: Route all URL/error redaction through `security.redaction`**

Replace local `connectors.http.redact_url` implementation with:

```python
from catalyst_radar.security.redaction import redact_url
```

In `provider_ingest.py`, redact all exception text before persisting:

```python
from catalyst_radar.security.redaction import redact_text, redact_value

reason = redact_text(str(exc))
metadata = redact_value(metadata)
```

Use the redacted values for provider health, job error summaries, and data quality incidents.

- [ ] **Step 3: Minimize OpenAI prompt input**

In `redaction.py`, implement:

```python
ACCOUNT_SENSITIVE_KEYS = {
    "portfolio_value",
    "portfolio_cash",
    "cash",
    "shares",
    "market_value",
    "account_equity",
    "notes",
    "user_notes",
}


def minimize_prompt_payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
    redacted = redact_value(value)
    return _drop_keys(redacted, ACCOUNT_SENSITIVE_KEYS)
```

In `openai_client._request_input_json()`, apply minimization to the payload passed to OpenAI:

```python
payload = minimize_prompt_payload(
    {
        "task": request.task.name.value,
        "prompt_version": request.prompt_version,
        "schema_version": request.schema_version,
        "candidate_packet": json.loads(request.candidate_json),
        "agent_evidence_packet": request.evidence_packet,
    }
)
return json.dumps(payload, sort_keys=True, separators=(",", ":"))
```

- [ ] **Step 4: Redact LLM ledger errors and CLI/API JSON helpers**

In `agents/router.py`, wrap client errors:

```python
error=redact_text(str(exc))
```

In CLI payload helpers that include ledger payloads or errors, call `redact_value()` before returning JSON.

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests\unit\test_redaction.py tests\integration\test_security_boundaries.py
python -m ruff check src tests\unit\test_redaction.py tests\integration\test_security_boundaries.py
git diff --check
git add src tests
git commit -m "fix: redact secrets across runtime boundaries"
```

---

## Task 3: Append-Only Audit Event Store

**Files:**

- Create: `src/catalyst_radar/security/audit.py`
- Create: `sql/migrations/013_security_audit.sql`
- Modify: `src/catalyst_radar/storage/schema.py`
- Modify: `src/catalyst_radar/storage/db.py`
- Test: `tests/integration/test_audit_logs.py`

- [ ] **Step 1: Write failing audit schema/repository tests**

Create `tests/integration/test_audit_logs.py`:

```python
def test_create_schema_creates_audit_events_table_and_indexes() -> None:
    engine = _engine()

    inspector = inspect(engine)

    assert "audit_events" in inspector.get_table_names()
    assert any(index["name"] == "ix_audit_events_event_type_occurred" for index in inspector.get_indexes("audit_events"))


def test_append_audit_event_is_append_only() -> None:
    engine = _engine()
    repo = AuditLogRepository(engine)
    now = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)

    first = repo.append_event(
        event_type="feedback_recorded",
        actor_source="api",
        actor_id="user-1",
        actor_role="analyst",
        artifact_type="decision_card",
        artifact_id="card-1",
        ticker="MSFT",
        status="success",
        metadata={"label": "useful"},
        occurred_at=now,
    )
    second = repo.append_event(
        event_type="feedback_recorded",
        actor_source="api",
        actor_id="user-1",
        actor_role="analyst",
        artifact_type="decision_card",
        artifact_id="card-1",
        ticker="MSFT",
        status="success",
        metadata={"label": "acted"},
        occurred_at=now + timedelta(seconds=1),
    )

    events = repo.list_events(artifact_type="decision_card", artifact_id="card-1")

    assert first.id != second.id
    assert [event.metadata["label"] for event in events] == ["useful", "acted"]
```

- [ ] **Step 2: Add SQLAlchemy table and SQL migration**

Add `audit_events` to `schema.py` with these columns:

```python
audit_events = Table(
    "audit_events",
    metadata,
    Column("id", String, primary_key=True),
    Column("event_type", String, nullable=False),
    Column("actor_source", String, nullable=False),
    Column("actor_id", String),
    Column("actor_role", String),
    Column("artifact_type", String),
    Column("artifact_id", String),
    Column("ticker", String),
    Column("candidate_state_id", String),
    Column("candidate_packet_id", String),
    Column("decision_card_id", String),
    Column("paper_trade_id", String),
    Column("alert_id", String),
    Column("budget_ledger_id", String),
    Column("decision", String),
    Column("status", String, nullable=False),
    Column("reason", Text),
    Column("hard_blocks", json_type, nullable=False),
    Column("before_payload", json_type, nullable=False),
    Column("after_payload", json_type, nullable=False),
    Column("metadata", json_type, nullable=False),
    Column("available_at", DateTime(timezone=True)),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
```

Add indexes:

```python
Index("ix_audit_events_event_type_occurred", audit_events.c.event_type, audit_events.c.occurred_at)
Index("ix_audit_events_artifact", audit_events.c.artifact_type, audit_events.c.artifact_id)
Index("ix_audit_events_ticker_occurred", audit_events.c.ticker, audit_events.c.occurred_at)
```

Create matching `sql/migrations/013_security_audit.sql`.

- [ ] **Step 3: Implement `security/audit.py`**

Implement:

```python
@dataclass(frozen=True)
class AuditEvent:
    id: str
    event_type: str
    actor_source: str
    status: str
    occurred_at: datetime
    actor_id: str | None = None
    actor_role: str | None = None
    artifact_type: str | None = None
    artifact_id: str | None = None
    ticker: str | None = None
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    paper_trade_id: str | None = None
    alert_id: str | None = None
    budget_ledger_id: str | None = None
    decision: str | None = None
    reason: str | None = None
    hard_blocks: Mapping[str, Any] | Sequence[str] = ()
    before_payload: Mapping[str, Any] = field(default_factory=dict)
    after_payload: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    available_at: datetime | None = None


class AuditLogRepository:
    def append_event(self, *, event_type: str, actor_source: str, status: str = "success", **kwargs: Any) -> AuditEvent:
        event = AuditEvent(
            id=audit_event_id(event_type=event_type, occurred_at=kwargs.get("occurred_at")),
            event_type=event_type,
            actor_source=actor_source,
            status=status,
            occurred_at=_to_utc(kwargs.pop("occurred_at", None) or datetime.now(UTC), "occurred_at"),
            **kwargs,
        )
        self._insert(event)
        return event
```

Use UUID or hash plus timestamp for event IDs. Do not implement update/delete.

- [ ] **Step 4: Verify and commit**

Run:

```powershell
python -m pytest tests\integration\test_audit_logs.py::test_create_schema_creates_audit_events_table_and_indexes tests\integration\test_audit_logs.py::test_append_audit_event_is_append_only
python -m ruff check src\catalyst_radar\security\audit.py src\catalyst_radar\storage tests\integration\test_audit_logs.py
git diff --check
git add src\catalyst_radar\security\audit.py src\catalyst_radar\storage sql\migrations\013_security_audit.sql tests\integration\test_audit_logs.py
git commit -m "feat: add append-only audit event log"
```

---

## Task 4: Audit Feedback, Paper Decisions, Overrides, And Model Calls

**Files:**

- Modify: `src/catalyst_radar/feedback/service.py`
- Modify: `src/catalyst_radar/api/routes/alerts.py`
- Modify: `src/catalyst_radar/api/routes/feedback.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/agents/router.py`
- Test: `tests/integration/test_audit_logs.py`

- [ ] **Step 1: Add failing audit integration tests**

Add concrete tests with seeded artifacts and explicit assertions:

```python
def test_generic_feedback_route_appends_audit_event_and_does_not_mutate_candidate() -> None:
    seed_decision_card(engine, id="card-MSFT", ticker="MSFT")
    response = client.post(
        "/api/feedback",
        headers={"X-Catalyst-Role": "analyst"},
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert response.status_code == 200
    events = AuditLogRepository(engine).list_events(
        artifact_type="decision_card",
        artifact_id="card-MSFT",
    )
    assert [event.event_type for event in events] == ["feedback_recorded"]
    assert events[0].metadata["label"] == "useful"
```

Also add separate tests for:

- Alert feedback route appends `feedback_recorded`.
- Invalid feedback writes no audit event.
- `useful-label` CLI appends a `cli` source event.
- `paper-decision` appends `paper_decision_recorded`.
- Approving a blocked card requires `--override-reason` and appends `hard_block_bypass_recorded`.
- `paper-update-outcomes --labels-json` appends `paper_outcome_updated`.
- Missing-packet `run-llm-review` appends `model_call_recorded`.
- Repeated feedback and paper decisions append distinct audit events.

Use existing fixture helpers from `test_api_routes.py`, `test_alert_api_routes.py`, `test_validation_cli.py`, and `test_dashboard_data.py` rather than duplicating large payloads.

- [ ] **Step 2: Add actor metadata parameters**

Update `record_feedback()` signature:

```python
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
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> FeedbackRecordResult:
```

After successful DB writes, append:

```python
AuditLogRepository(engine).append_event(
    event_type="feedback_recorded",
    actor_source=resolved_source,
    actor_id=actor_id,
    actor_role=actor_role,
    artifact_type=resolved_artifact_type,
    artifact_id=resolved_artifact_id,
    ticker=resolved_ticker,
    status="success",
    metadata={"label": resolved_label},
    after_payload={"notes": resolved_notes},
    occurred_at=resolved_created_at,
)
```

- [ ] **Step 3: Audit paper decisions and hard-block bypasses**

In CLI `paper-decision`:

- Add `--override-reason`.
- Load the decision card payload before creating a paper trade.
- If `decision == approved` and card state or payload contains hard blocks, require `--override-reason`.
- Append `paper_decision_recorded`.
- Append `hard_block_bypass_recorded` when applicable.

Hard-block detector:

```python
def _card_hard_blocks(card: Mapping[str, Any]) -> Sequence[str]:
    payload = card.get("payload") if isinstance(card, Mapping) else {}
    controls = payload.get("controls") if isinstance(payload, Mapping) else {}
    portfolio = payload.get("portfolio_impact") if isinstance(payload, Mapping) else {}
    values = [
        *(controls.get("hard_blocks") or ()),
        *(portfolio.get("hard_blocks") or ()),
    ]
    return tuple(str(value) for value in values if str(value))
```

- [ ] **Step 4: Audit paper outcome updates**

After `paper-update-outcomes` writes a trade outcome, append:

```python
event_type="paper_outcome_updated"
artifact_type="paper_trade"
artifact_id=updated_trade.id
decision_card_id=updated_trade.decision_card_id
metadata={"label_source": "labels_json" if args.labels_json else "computed"}
```

- [ ] **Step 5: Audit LLM review attempts**

In `LLMRouter.review_candidate()`, after any ledger entry is written, append:

```python
AuditLogRepository(self.budget.ledger_repo.engine).append_event(
    event_type="model_call_recorded",
    actor_source="llm_router",
    artifact_type="candidate_packet",
    artifact_id=candidate.id,
    ticker=candidate.ticker,
    candidate_packet_id=candidate.id,
    budget_ledger_id=entry.id,
    status=entry.status.value if hasattr(entry.status, "value") else str(entry.status),
    metadata={"task": task.name.value, "provider": provider, "model": model},
    available_at=available_at,
    occurred_at=entry.attempted_at,
)
```

For CLI missing packet skip path, append the same event type after manually creating the ledger row.

- [ ] **Step 6: Verify and commit**

Run:

```powershell
python -m pytest tests\integration\test_audit_logs.py
python -m pytest tests\integration\test_api_routes.py tests\integration\test_alert_api_routes.py tests\integration\test_validation_cli.py tests\integration\test_budget_repository.py
python -m ruff check src tests\integration\test_audit_logs.py
git diff --check
git add src tests\integration\test_audit_logs.py
git commit -m "feat: audit user decisions and model calls"
```

---

## Task 5: Role-Based Access Controls

**Files:**

- Create: `src/catalyst_radar/security/access.py`
- Modify: `src/catalyst_radar/core/config.py`
- Modify: `src/catalyst_radar/api/routes/radar.py`
- Modify: `src/catalyst_radar/api/routes/ops.py`
- Modify: `src/catalyst_radar/api/routes/costs.py`
- Modify: `src/catalyst_radar/api/routes/alerts.py`
- Modify: `src/catalyst_radar/api/routes/feedback.py`
- Modify: `apps/dashboard/Home.py`
- Modify: `apps/dashboard/pages/*.py`
- Test: `tests/unit/test_access_roles.py`
- Test: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Write failing role tests**

Create `tests/unit/test_access_roles.py`:

```python
def test_role_ordering_allows_analyst_to_read_and_write() -> None:
    assert role_allows("analyst", "viewer") is True
    assert role_allows("viewer", "analyst") is False
    assert role_allows("admin", "analyst") is True


def test_unknown_role_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown role"):
        parse_role("owner")
```

Add integration tests using FastAPI `TestClient`:

```python
def test_auth_required_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    response = client.get("/api/radar/candidates")
    assert response.status_code == 401


def test_viewer_can_read_but_cannot_post_feedback(monkeypatch) -> None:
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    assert client.get("/api/radar/candidates", headers={"X-Catalyst-Role": "viewer"}).status_code == 200
    assert client.post(
        "/api/feedback",
        headers={"X-Catalyst-Role": "viewer"},
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    ).status_code == 403
```

- [ ] **Step 2: Implement `security/access.py`**

Implement:

```python
class Role(StrEnum):
    VIEWER = "viewer"
    ANALYST = "analyst"
    ADMIN = "admin"


ROLE_RANK = {Role.VIEWER: 1, Role.ANALYST: 2, Role.ADMIN: 3}


def parse_role(value: str | None) -> Role:
    if value is None or value.strip() == "":
        raise ValueError("role is required")
    try:
        return Role(value.strip().lower())
    except ValueError as exc:
        raise ValueError(f"unknown role: {value}") from exc


def role_allows(actual: Role | str, required: Role | str) -> bool:
    return ROLE_RANK[parse_role(str(actual))] >= ROLE_RANK[parse_role(str(required))]


def require_role(required: Role):
    def dependency(x_catalyst_role: Annotated[str | None, Header()] = None) -> Role:
        if AppConfig.from_env().api_auth_mode == "disabled":
            return Role.ADMIN
        try:
            role = parse_role(x_catalyst_role)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        if not role_allows(role, required):
            raise HTTPException(status_code=403, detail="insufficient role")
        return role
    return dependency
```

Add config fields:

```python
api_auth_mode: str = "disabled"
dashboard_auth_mode: str = "disabled"
dashboard_role: str = "admin"
```

- [ ] **Step 3: Attach API dependencies**

- `/api/health`: public.
- GET radar/costs/ops/alerts routes: `Depends(require_role(Role.VIEWER))`.
- POST feedback and alert feedback: `Depends(require_role(Role.ANALYST))`.

Keep auth disabled by default so local tests and existing single-user workflows keep working.

- [ ] **Step 4: Add dashboard guard**

Add:

```python
def require_dashboard_role(required: Role = Role.VIEWER) -> Role:
    config = AppConfig.from_env()
    if config.dashboard_auth_mode == "disabled":
        return Role.ADMIN
    role = parse_role(config.dashboard_role)
    if not role_allows(role, required):
        st.error("Insufficient role")
        st.stop()
    return role
```

Call it before DB/data loading on every Streamlit page.

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests\unit\test_access_roles.py tests\integration\test_api_routes.py tests\integration\test_alert_api_routes.py
python -m ruff check src apps tests\unit\test_access_roles.py
git diff --check
git add src apps tests
git commit -m "feat: add role-based access controls"
```

---

## Task 6: Provider License Policies And No-Broker Boundary

**Files:**

- Create: `src/catalyst_radar/security/licenses.py`
- Modify: `src/catalyst_radar/storage/provider_repositories.py`
- Modify: `src/catalyst_radar/connectors/base.py`
- Test: `tests/unit/test_provider_license_policy.py`
- Test: `tests/integration/test_security_boundaries.py`

- [ ] **Step 1: Write failing license and no-broker tests**

Create `tests/unit/test_provider_license_policy.py`:

```python
def test_known_license_policy_defines_prompt_and_export_flags() -> None:
    policy = policy_for_license("polygon-market-data")

    assert policy.license_tag == "polygon-market-data"
    assert policy.prompt_allowed is False
    assert policy.external_export_allowed is False


def test_unknown_license_tag_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown provider license tag"):
        policy_for_license("unknown")
```

Add to `tests/integration/test_security_boundaries.py`:

```python
def test_source_imports_do_not_include_broker_sdks() -> None:
    forbidden = {"alpaca", "ib_insync", "interactive_brokers", "robin_stocks", "tda"}
    source_text = "\n".join(path.read_text(encoding="utf-8") for path in Path("src").rglob("*.py"))
    assert not any(f"import {name}" in source_text or f"from {name}" in source_text for name in forbidden)


def test_openapi_has_no_order_or_broker_routes() -> None:
    paths = create_app().openapi()["paths"]
    forbidden = ("broker", "order", "execute")
    assert not [path for path in paths if any(word in path.lower() for word in forbidden)]
```

- [ ] **Step 2: Implement license policy registry**

In `licenses.py`:

```python
@dataclass(frozen=True)
class ProviderLicensePolicy:
    license_tag: str
    retention_policy: str
    raw_retention_days: int | None
    normalized_retention_days: int | None
    prompt_allowed: bool
    external_export_allowed: bool
    attribution_required: bool


POLICIES = {
    "sec-public": ProviderLicensePolicy("sec-public", "public-sec-retain", None, None, True, True, True),
    "local-csv-fixture": ProviderLicensePolicy("local-csv-fixture", "local-fixture-retain", None, None, True, False, False),
    "news-fixture": ProviderLicensePolicy("news-fixture", "fixture-retain", None, None, True, False, False),
    "earnings-fixture": ProviderLicensePolicy("earnings-fixture", "fixture-retain", None, None, True, False, False),
    "polygon-market-data": ProviderLicensePolicy("polygon-market-data", "retain-per-provider-license", 365, None, False, False, True),
}
```

Expose:

```python
def policy_for_license(license_tag: str) -> ProviderLicensePolicy:
    try:
        return POLICIES[license_tag]
    except KeyError as exc:
        raise ValueError(f"unknown provider license tag: {license_tag}") from exc


def validate_raw_record_policy(
    license_tag: str,
    retention_policy: str,
) -> ProviderLicensePolicy:
    policy = policy_for_license(license_tag)
    if policy.retention_policy != retention_policy:
        raise ValueError(
            f"retention_policy {retention_policy} does not match license {license_tag}"
        )
    return policy
```

- [ ] **Step 3: Validate raw records before persistence**

In `ProviderRepository.save_raw_records()`:

```python
validate_raw_record_policy(record.license_tag, record.retention_policy)
```

Fail closed for unknown tags or mismatched retention policies.

- [ ] **Step 4: Strengthen no-broker contract tests**

Extend existing tests for:

- Decision-card payloads retain manual-review-only disclaimer.
- Paper trade payload has `manual_review_only=True` and `no_execution=True`.
- Alert copy does not contain forbidden execution phrases.
- API paths do not include broker/order/execute routes.

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests\unit\test_provider_license_policy.py tests\integration\test_security_boundaries.py tests\unit\test_decision_card_builder.py tests\unit\test_agent_schemas.py tests\unit\test_alert_routing.py
python -m ruff check src tests
git diff --check
git add src tests
git commit -m "feat: enforce provider license and no-broker policies"
```

---

## Task 7: Security Runbook, Checklist, Review, And Merge

**Files:**

- Create: `docs/runbooks/secrets.md`
- Create: `docs/phase-15-review.md`
- Modify: `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

- [ ] **Step 1: Add secrets runbook**

Create `docs/runbooks/secrets.md` with:

- Local dev: `.env.local` is allowed only outside production and is gitignored.
- Production: set secrets through the process manager, host secret store, Docker/VM secret injection, or `docker compose --env-file` from an operator-controlled file outside git.
- Required keys: `CATALYST_DATABASE_URL`, `CATALYST_POLYGON_API_KEY` when Polygon is enabled, `OPENAI_API_KEY` only when real LLM review is explicitly enabled.
- Rotation: update secret source, restart API/dashboard/worker, confirm `/api/ops/health`, run `llm-budget-status`, and verify audit events.
- Incident response: run redaction checks, inspect `audit_events`, rotate exposed credentials, and record incident follow-up.

- [ ] **Step 2: Mark Phase 15 master checklist complete**

In `docs/superpowers/plans/2026-05-09-full-product-implementation.md`, change Phase 15 task boxes to `[x]` only after implementation and verification pass.

- [ ] **Step 3: Add Phase 15 review doc**

Create `docs/phase-15-review.md`:

```markdown
# Phase 15 Review

## Completed

- Central secret wrappers and redaction helpers.
- Runtime redaction for provider, LLM, CLI, API, and prompt boundaries.
- Append-only audit log for feedback, paper decisions, overrides, outcomes, and model calls.
- Header/env role controls for API and dashboard pilot use.
- Provider license policy registry and no-broker boundary tests.

## Verification

- `python -m pytest`
- `python -m ruff check src tests apps`
- `git diff --check`
- `docker compose config`
- `docker compose -f infra/docker/docker-compose.prod.yml config`

## Residual Risk

- Production authentication remains header/env based and must be fronted by a trusted reverse proxy or private network.
- Production secret storage depends on the deployment environment's managed secret mechanism.
- Provider retention purge automation is policy-defined but not scheduled until provider contracts are finalized.
```

- [ ] **Step 4: Full verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
git diff --check
docker compose config
$env:POSTGRES_PASSWORD='dummy'; $env:CATALYST_DATABASE_URL='postgresql+psycopg://catalyst:dummy@postgres:5432/catalyst_radar'; docker compose -f infra\docker\docker-compose.prod.yml config; Remove-Item Env:\POSTGRES_PASSWORD; Remove-Item Env:\CATALYST_DATABASE_URL
```

- [ ] **Step 5: Commit docs**

```powershell
git add docs\runbooks\secrets.md docs\phase-15-review.md docs\superpowers\plans\2026-05-09-full-product-implementation.md
git commit -m "docs: review phase 15 security controls"
```

- [ ] **Step 6: Final reviews**

Request subagent reviews:

- Spec compliance against this plan and Phase 15 master checklist.
- Code quality and runtime security review.
- Compliance/no-broker/provider-license review.

Fix all Critical and Important findings before merge.

- [ ] **Step 7: Merge to main and verify**

From `C:\Users\fpan1\MarketRadar`:

```powershell
git status --short --branch
git merge --ff-only feature/phase-15-security-compliance
python -m pytest
python -m ruff check src tests apps
git diff --check
docker compose config
```

---

## Phase 15 Acceptance Checklist

- [ ] `.env.local` is never loaded in production mode.
- [ ] Secret values do not appear in `str()`, `repr()`, CLI JSON, provider job errors, provider incidents, LLM ledger error payloads, or OpenAI prompt input.
- [ ] Prompt payloads preserve source-linked evidence IDs while removing account-sensitive fields.
- [ ] `audit_events` is append-only and records user feedback, alert feedback, useful labels, paper decisions, hard-block bypasses, paper outcome updates, and model-call attempts.
- [ ] Hard-blocked paper approvals require explicit override reason and create a bypass audit event.
- [ ] FastAPI role controls protect reads and writes when `CATALYST_API_AUTH_MODE=header`.
- [ ] Dashboard role guard blocks insufficient roles when enabled.
- [ ] Provider raw records fail closed on unknown license tags or mismatched retention policies.
- [ ] No broker/order/execute route or broker SDK import exists.
- [ ] Phase 15 docs and runbooks are updated.
- [ ] Full test/lint/Compose verification passes on the feature branch and after merge to `main`.
