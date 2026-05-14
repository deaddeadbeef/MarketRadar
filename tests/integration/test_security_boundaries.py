from __future__ import annotations

import ast
import re
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.routing import APIRoute
from sqlalchemy import create_engine, select

from apps.api.main import create_app
from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ingest_provider_records,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import data_quality_incidents, job_runs

FORBIDDEN_BROKER_IMPORTS = {
    "alpaca",
    "alpaca_trade_api",
    "ibapi",
    "ib_insync",
    "interactive_brokers",
    "robin_stocks",
    "tda",
}
FORBIDDEN_BROKER_PACKAGES = {
    "alpaca",
    "alpaca-py",
    "alpaca-trade-api",
    "ib-insync",
    "ibapi",
    "interactive-brokers",
    "robin-stocks",
    "tda",
    "tda-api",
}
FORBIDDEN_ROUTE_TERMS = {
    "broker",
    "brokers",
    "execute",
    "execution",
    "executions",
    "order",
    "orders",
}
PRODUCT_PYTHON_ROOTS = (Path("src"), Path("apps"))
DEPENDENCY_DECLARATION_PATTERNS = (
    "pyproject.toml",
    "requirements*.txt",
    "poetry.lock",
    "uv.lock",
    "pylock.toml",
    "Pipfile",
    "Pipfile.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
)
EXPECTED_API_ROUTES = {
    (
        "POST",
        "/api/agents/review",
    ): ("catalyst_radar.api.routes.agents", "review_candidate", ("agents",)),
    (
        "GET",
        "/api/agents/reviews",
    ): ("catalyst_radar.api.routes.agents", "review_history", ("agents",)),
    ("GET", "/api/alerts"): ("catalyst_radar.api.routes.alerts", "alerts", ("alerts",)),
    (
        "GET",
        "/api/alerts/{alert_id}",
    ): ("catalyst_radar.api.routes.alerts", "alert_detail", ("alerts",)),
    (
        "POST",
        "/api/alerts/{alert_id}/feedback",
    ): ("catalyst_radar.api.routes.alerts", "alert_feedback", ("alerts",)),
    ("GET", "/api/costs/summary"): ("catalyst_radar.api.routes.costs", "summary", ("costs",)),
    (
        "POST",
        "/api/feedback",
    ): ("catalyst_radar.api.routes.feedback", "record_feedback", ("feedback",)),
    ("GET", "/api/health"): ("apps.api.main", "health", ()),
    ("GET", "/api/ops/health"): ("catalyst_radar.api.routes.ops", "health", ("ops",)),
    (
        "GET",
        "/api/radar/candidates",
    ): ("catalyst_radar.api.routes.radar", "candidates", ("radar",)),
    (
        "GET",
        "/api/radar/candidates/{ticker}",
    ): ("catalyst_radar.api.routes.radar", "candidate_detail", ("radar",)),
    (
        "POST",
        "/api/radar/runs",
    ): ("catalyst_radar.api.routes.radar", "run_radar", ("radar",)),
    (
        "GET",
        "/api/radar/runs/latest",
    ): ("catalyst_radar.api.routes.radar", "latest_radar_run", ("radar",)),
    (
        "GET",
        "/api/radar/readiness",
    ): ("catalyst_radar.api.routes.radar", "radar_readiness", ("radar",)),
    (
        "GET",
        "/api/radar/live-activation",
    ): ("catalyst_radar.api.routes.radar", "radar_live_activation", ("radar",)),
    (
        "GET",
        "/api/radar/research-shortlist",
    ): ("catalyst_radar.api.routes.radar", "radar_research_shortlist", ("radar",)),
    (
        "POST",
        "/api/radar/runs/call-plan",
    ): ("catalyst_radar.api.routes.radar", "radar_run_call_plan", ("radar",)),
    (
        "POST",
        "/api/radar/universe/seed",
    ): ("catalyst_radar.api.routes.radar", "seed_universe", ("radar",)),
    (
        "GET",
        "/api/brokers/schwab/connect",
    ): ("catalyst_radar.api.routes.brokers", "schwab_connect", ("brokers",)),
    (
        "GET",
        "/api/brokers/schwab/callback",
    ): ("catalyst_radar.api.routes.brokers", "schwab_callback", ("brokers",)),
    (
        "GET",
        "/api/brokers/schwab/status",
    ): ("catalyst_radar.api.routes.brokers", "schwab_status", ("brokers",)),
    (
        "POST",
        "/api/brokers/schwab/disconnect",
    ): ("catalyst_radar.api.routes.brokers", "schwab_disconnect", ("brokers",)),
    (
        "POST",
        "/api/brokers/schwab/sync",
    ): ("catalyst_radar.api.routes.brokers", "schwab_sync", ("brokers",)),
    (
        "POST",
        "/api/brokers/schwab/market-sync",
    ): ("catalyst_radar.api.routes.brokers", "schwab_market_sync", ("brokers",)),
    (
        "GET",
        "/api/market/context",
    ): ("catalyst_radar.api.routes.brokers", "market_context", ("brokers",)),
    (
        "POST",
        "/api/opportunities/actions",
    ): ("catalyst_radar.api.routes.brokers", "opportunity_action", ("brokers",)),
    (
        "GET",
        "/api/opportunities/actions",
    ): ("catalyst_radar.api.routes.brokers", "opportunity_actions", ("brokers",)),
    (
        "POST",
        "/api/market/triggers",
    ): ("catalyst_radar.api.routes.brokers", "market_trigger", ("brokers",)),
    (
        "POST",
        "/api/market/triggers/evaluate",
    ): ("catalyst_radar.api.routes.brokers", "market_triggers_evaluate", ("brokers",)),
    (
        "GET",
        "/api/market/triggers",
    ): ("catalyst_radar.api.routes.brokers", "market_triggers", ("brokers",)),
    (
        "GET",
        "/api/portfolio/snapshot",
    ): ("catalyst_radar.api.routes.brokers", "portfolio_snapshot", ("brokers",)),
    (
        "GET",
        "/api/portfolio/positions",
    ): ("catalyst_radar.api.routes.brokers", "portfolio_positions", ("brokers",)),
    (
        "GET",
        "/api/portfolio/balances",
    ): ("catalyst_radar.api.routes.brokers", "portfolio_balances", ("brokers",)),
    (
        "GET",
        "/api/portfolio/open-orders",
    ): ("catalyst_radar.api.routes.brokers", "portfolio_open_orders", ("brokers",)),
    (
        "GET",
        "/api/portfolio/exposure",
    ): ("catalyst_radar.api.routes.brokers", "portfolio_exposure", ("brokers",)),
    (
        "POST",
        "/api/orders/preview",
    ): ("catalyst_radar.api.routes.brokers", "order_preview", ("brokers",)),
    (
        "POST",
        "/api/orders/tickets",
    ): ("catalyst_radar.api.routes.brokers", "order_ticket", ("brokers",)),
    (
        "GET",
        "/api/orders/tickets",
    ): ("catalyst_radar.api.routes.brokers", "order_tickets", ("brokers",)),
}
ALLOWED_BROKER_ROUTE_TERMS = {
    ("GET", "/api/brokers/schwab/connect"),
    ("GET", "/api/brokers/schwab/callback"),
    ("GET", "/api/brokers/schwab/status"),
    ("POST", "/api/brokers/schwab/disconnect"),
    ("POST", "/api/brokers/schwab/sync"),
    ("POST", "/api/brokers/schwab/market-sync"),
    ("GET", "/api/market/context"),
    ("POST", "/api/opportunities/actions"),
    ("GET", "/api/opportunities/actions"),
    ("POST", "/api/market/triggers"),
    ("POST", "/api/market/triggers/evaluate"),
    ("GET", "/api/market/triggers"),
    ("GET", "/api/portfolio/snapshot"),
    ("GET", "/api/portfolio/positions"),
    ("GET", "/api/portfolio/balances"),
    ("GET", "/api/portfolio/open-orders"),
    ("GET", "/api/portfolio/exposure"),
    ("POST", "/api/orders/preview"),
    ("POST", "/api/orders/tickets"),
    ("GET", "/api/orders/tickets"),
}


def test_provider_ingest_redacts_secret_from_health_job_and_incident(
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'security.db').as_posix()}", future=True)
    create_schema(engine)
    provider_repo = ProviderRepository(engine)

    with pytest.raises(ProviderIngestError) as excinfo:
        ingest_provider_records(
            connector=_LeakyConnector(),
            request=ConnectorRequest(
                provider="leaky",
                endpoint="test",
                params={},
                requested_at=datetime(2026, 5, 10, tzinfo=UTC),
            ),
            market_repo=MarketRepository(engine),
            provider_repo=provider_repo,
            job_type="leaky_ingest",
            metadata={"api_key": "metadata-secret"},
        )

    with engine.connect() as conn:
        job = conn.execute(select(job_runs)).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    health = provider_repo.latest_health("leaky")
    assert health is not None
    persisted = " ".join(
        [
            str(excinfo.value),
            str(health.reason),
            str(job.error_summary),
            str(job.metadata),
            str(incident.reason),
            str(incident.payload),
        ]
    )
    assert "secret-token" not in persisted
    assert "metadata-secret" not in persisted
    assert "<redacted>" in persisted


def test_source_imports_do_not_include_broker_sdks() -> None:
    violations = [
        violation
        for root in PRODUCT_PYTHON_ROOTS
        if root.exists()
        for path in root.rglob("*.py")
        for violation in _broker_import_violations(path)
    ]

    assert not violations


@pytest.mark.parametrize(
    "source",
    [
        "import alpaca_trade_api\n",
        "import ibapi.client\n",
        "from ib_insync import IB\n",
        "import importlib\nimportlib.import_module('alpaca')\n",
        "__import__('ib_insync')\n",
        "from importlib import import_module\nimport_module('tda')\n",
    ],
)
def test_broker_import_guard_catches_static_and_dynamic_import_roots(
    tmp_path: Path,
    source: str,
) -> None:
    path = tmp_path / "forbidden.py"
    path.write_text(source, encoding="utf-8")

    assert _broker_import_violations(path)


def test_broker_import_guard_ignores_comments_and_strings(tmp_path: Path) -> None:
    path = tmp_path / "benign.py"
    path.write_text(
        "# import alpaca_trade_api\ntext = 'from ib_insync import IB and __import__(\"tda\")'\n",
        encoding="utf-8",
    )

    assert _broker_import_violations(path) == []


def test_dependency_declarations_do_not_include_broker_sdks() -> None:
    violations = [
        (path.as_posix(), package)
        for path in _dependency_declaration_files()
        for package in _declared_dependency_names(path)
        if _normalize_package_name(package) in FORBIDDEN_BROKER_PACKAGES
    ]

    assert not violations


def test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit() -> None:
    app = create_app()
    actual_routes = {
        (method, route.path): _route_metadata(route)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
        if method not in {"HEAD", "OPTIONS"}
    }
    schema = app.openapi()
    openapi_routes = {
        (method.upper(), path): (
            str(operation.get("operationId", "")),
            tuple(str(tag) for tag in operation.get("tags", ())),
        )
        for path, methods in schema["paths"].items()
        for method, operation in methods.items()
    }

    assert actual_routes == EXPECTED_API_ROUTES
    assert set(openapi_routes) == set(EXPECTED_API_ROUTES)
    assert not [
        (method, path, metadata)
        for (method, path), metadata in actual_routes.items()
        if (method, path) not in ALLOWED_BROKER_ROUTE_TERMS
        and _route_metadata_has_forbidden_semantics(method, path, metadata)
    ]
    assert not [
        (method, path, metadata)
        for (method, path), metadata in openapi_routes.items()
        if (method, path) not in ALLOWED_BROKER_ROUTE_TERMS
        and _route_metadata_has_forbidden_semantics(method, path, metadata)
    ]


def test_production_compose_defaults_to_loopback_and_header_auth() -> None:
    compose = Path("infra/docker/docker-compose.prod.yml").read_text(encoding="utf-8")

    assert "CATALYST_ENV: production" in compose
    assert "PYTHONPATH: /app/src:/app" in compose
    assert "CATALYST_API_AUTH_MODE: ${CATALYST_API_AUTH_MODE:-header}" in compose
    assert "CATALYST_DASHBOARD_AUTH_MODE: ${CATALYST_DASHBOARD_AUTH_MODE:-header}" in compose
    assert "CATALYST_SEC_ENABLE_LIVE: ${CATALYST_SEC_ENABLE_LIVE:-false}" in compose
    assert "CATALYST_SEC_USER_AGENT: ${CATALYST_SEC_USER_AGENT:-}" in compose
    assert "CATALYST_SEC_BASE_URL: ${CATALYST_SEC_BASE_URL:-https://data.sec.gov}" in compose
    assert "CATALYST_SEC_DAILY_MAX_TICKERS: ${CATALYST_SEC_DAILY_MAX_TICKERS:-5}" in compose
    assert "--server.headless true" in compose
    assert '"${CATALYST_API_BIND:-127.0.0.1}:8000:8000"' in compose
    assert '"${CATALYST_DASHBOARD_BIND:-127.0.0.1}:8501:8501"' in compose


def test_local_compose_passes_dashboard_runtime_env() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert "PYTHONPATH: /app/src:/app" in compose
    assert (
        "CATALYST_DATABASE_URL: "
        "postgresql+psycopg://catalyst:catalyst@postgres:5432/catalyst_radar" in compose
    )
    assert "CATALYST_SEC_ENABLE_LIVE: ${CATALYST_SEC_ENABLE_LIVE:-false}" in compose
    assert "CATALYST_SEC_USER_AGENT: ${CATALYST_SEC_USER_AGENT:-}" in compose
    assert "CATALYST_SEC_BASE_URL: ${CATALYST_SEC_BASE_URL:-https://data.sec.gov}" in compose
    assert "CATALYST_SEC_DAILY_MAX_TICKERS: ${CATALYST_SEC_DAILY_MAX_TICKERS:-5}" in compose
    assert "--server.headless true" in compose


def _broker_import_violations(path: Path) -> list[tuple[str, int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[tuple[str, int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                violations.extend(_static_import_violations(path, node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            violations.extend(_static_import_violations(path, node.lineno, node.module))
        elif isinstance(node, ast.Call):
            module_name = _dynamic_import_module_name(node)
            if module_name is not None:
                violations.extend(_static_import_violations(path, node.lineno, module_name))

    return violations


def _static_import_violations(
    path: Path,
    line_number: int,
    module_name: str,
) -> list[tuple[str, int, str]]:
    root = module_name.split(".", maxsplit=1)[0]
    if root in FORBIDDEN_BROKER_IMPORTS:
        return [(path.as_posix(), line_number, module_name)]
    return []


def _dynamic_import_module_name(node: ast.Call) -> str | None:
    if not node.args:
        return None
    if isinstance(node.func, ast.Name) and node.func.id in {"__import__", "import_module"}:
        return _string_literal(node.args[0])
    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "import_module"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "importlib"
    ):
        return _string_literal(node.args[0])
    return None


def _string_literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _dependency_declaration_files() -> list[Path]:
    paths: set[Path] = set()
    for pattern in DEPENDENCY_DECLARATION_PATTERNS:
        paths.update(Path(".").glob(pattern))
    return sorted(path for path in paths if path.is_file())


def _declared_dependency_names(path: Path) -> set[str]:
    if path.name == "pyproject.toml":
        return _pyproject_dependency_names(path)
    if path.name.startswith("requirements") and path.suffix == ".txt":
        return _requirements_dependency_names(path)
    return _lockfile_dependency_names(path)


def _pyproject_dependency_names(path: Path) -> set[str]:
    pyproject = tomllib.loads(path.read_text(encoding="utf-8"))
    dependencies: set[str] = set()
    project = pyproject.get("project", {})

    for requirement in project.get("dependencies", ()):
        dependencies.add(_dependency_name_from_requirement(str(requirement)))
    for group in project.get("optional-dependencies", {}).values():
        for requirement in group:
            dependencies.add(_dependency_name_from_requirement(str(requirement)))

    return dependencies


def _requirements_dependency_names(path: Path) -> set[str]:
    dependencies: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.partition("#")[0].strip()
        if line and not line.startswith(("-", "http://", "https://")):
            dependencies.add(_dependency_name_from_requirement(line))
    return dependencies


def _lockfile_dependency_names(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    candidates = set(re.findall(r'(?im)^\s*name\s*=\s*["\']?([A-Za-z0-9_.-]+)', text))
    candidates.update(re.findall(r'(?im)^\s*"?([A-Za-z0-9_.-]+)"?\s*:', text))
    return {_dependency_name_from_requirement(candidate) for candidate in candidates}


def _dependency_name_from_requirement(requirement: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    return match.group(1) if match else requirement


def _normalize_package_name(package_name: str) -> str:
    return re.sub(r"[-_.]+", "-", package_name).lower()


def _route_metadata(route: APIRoute) -> tuple[str, str, tuple[str, ...]]:
    return (
        route.endpoint.__module__,
        route.endpoint.__name__,
        tuple(str(tag) for tag in route.tags),
    )


def _route_metadata_has_forbidden_semantics(
    method: str,
    path: str,
    metadata: tuple[str, ...],
) -> bool:
    tokens = {method.lower()}
    tokens.update(_semantic_tokens(path))
    for value in metadata:
        if isinstance(value, tuple):
            for item in value:
                tokens.update(_semantic_tokens(item))
        else:
            tokens.update(_semantic_tokens(value))
    return bool(tokens & FORBIDDEN_ROUTE_TERMS)


def _semantic_tokens(value: str) -> set[str]:
    return {token for token in re.split(r"[^A-Za-z0-9]+|_", value.lower()) if token}


class _LeakyConnector:
    def healthcheck(self) -> ConnectorHealth:
        return ConnectorHealth(
            provider="leaky",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime(2026, 5, 10, tzinfo=UTC),
            reason="ok",
        )

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        del request
        raise RuntimeError("provider failed with apikey=secret-token")

    def normalize(self, records: list[RawRecord]) -> list[NormalizedRecord]:
        del records
        return []

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        del request
        return ProviderCostEstimate(
            provider="leaky",
            request_count=1,
            estimated_cost_usd=0.0,
        )
