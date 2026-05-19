from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from urllib.parse import urlencode

from sqlalchemy import select
from sqlalchemy.engine import Engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.agents.openai_client import OpenAIResponsesClient
from catalyst_radar.agents.router import (
    FakeLLMClient,
    LLMClientRequest,
    LLMClientResult,
    LLMRouter,
)
from catalyst_radar.agents.sdk_orchestrator import run_market_radar_agents
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.alerts.channels.base import DryRunAlertChannel
from catalyst_radar.alerts.digest import build_alert_digest, digest_payload
from catalyst_radar.alerts.models import AlertStatus
from catalyst_radar.alerts.planner import plan_alerts
from catalyst_radar.brokers.interactive import upsert_schwab_option_features
from catalyst_radar.brokers.portfolio_context import latest_broker_portfolio_context
from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.options import (
    OptionsAggregateConnector,
    validate_options_fixture_json,
    write_options_fixture_template_json,
)
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard.data import (
    load_ticker_detail,
    options_fixture_template_payload,
    priced_in_all_source_gap_batches_payload,
    priced_in_answer_payload,
    priced_in_full_scan_audit_payload,
    priced_in_preflight_payload,
    priced_in_queue_payload,
    priced_in_source_gap_batches_payload,
    sec_cik_override_template_payload,
)
from catalyst_radar.dashboard.demo_seed import (
    default_sec_document_fixture_path,
    default_sec_fixture_path,
    seed_dashboard_demo,
)
from catalyst_radar.dashboard.source_batches import (
    execute_priced_in_source_batch,
    execute_priced_in_source_batches,
    source_batch_execution_summary,
    source_batch_run_summary,
)
from catalyst_radar.dashboard.tui import (
    DashboardFilters,
    dashboard_filters_for_page,
    dashboard_json_default,
    dashboard_snapshot_payload,
    render_dashboard_tui,
    run_dashboard_tui,
)
from catalyst_radar.decision_cards.builder import build_decision_card
from catalyst_radar.events.sec_cik import (
    apply_sec_cik_overrides_csv,
    refresh_sec_cik_metadata,
    validate_sec_cik_overrides_csv,
    write_sec_cik_override_template_csv,
)
from catalyst_radar.events.sec_ingest import (
    ingest_sec_record,
    ingest_sec_submissions_batch,
    parse_sec_submission_target,
)
from catalyst_radar.feedback.service import (
    FeedbackError,
    MissingArtifactError,
    record_feedback,
)
from catalyst_radar.jobs.scheduler import (
    SchedulerConfig,
    SchedulerRunResult,
    run_once,
    scheduler_run_payload,
)
from catalyst_radar.market.manual_bars import (
    import_manual_market_bars,
    write_manual_market_bars_template,
)
from catalyst_radar.pipeline.candidate_packet import build_candidate_packet
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.security.licenses import (
    ProviderLicenseError,
    require_external_export_allowed,
)
from catalyst_radar.security.redaction import redact_text, redact_value
from catalyst_radar.security.secrets import load_app_dotenv
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.candidate_packet_repositories import CandidatePacketRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.textint.pipeline import run_text_pipeline
from catalyst_radar.universe.builder import UniverseBuilder
from catalyst_radar.universe.filters import UniverseFilterConfig
from catalyst_radar.validation.baselines import (
    event_only_watchlist,
    random_eligible_universe,
    sector_relative_momentum,
    spy_relative_momentum,
    user_watchlist,
)
from catalyst_radar.validation.models import (
    PaperDecision,
    PaperTrade,
    ValidationResult,
    ValidationRun,
    ValidationRunStatus,
    validation_result_id,
)
from catalyst_radar.validation.outcomes import compute_forward_outcomes, outcome_labels_as_dict
from catalyst_radar.validation.paper import create_paper_trade_from_card, update_trade_outcome
from catalyst_radar.validation.replay import build_replay_results, deterministic_replay_run_id
from catalyst_radar.validation.reports import build_validation_report, validation_report_payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalyst-radar")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db")
    init_db.add_argument("--database-url")

    seed_dashboard = subparsers.add_parser("seed-dashboard-demo")
    seed_dashboard.add_argument("--database-url")
    seed_dashboard.add_argument("--ticker", default="ACME")
    seed_dashboard.add_argument("--cik", default="0002000001")
    seed_dashboard.add_argument("--sec-fixture", type=Path)
    seed_dashboard.add_argument("--document-fixture", type=Path)

    run_daily = subparsers.add_parser("run-daily")
    run_daily.add_argument("--database-url")
    run_daily.add_argument("--as-of", type=date.fromisoformat, required=True)
    run_daily.add_argument("--available-at", type=_parse_aware_datetime, required=True)
    run_daily.add_argument("--outcome-available-at", type=_parse_aware_datetime)
    run_daily.add_argument("--provider")
    run_daily.add_argument("--universe")
    run_daily.add_argument("--ticker", action="append")
    run_daily.add_argument("--run-llm", action="store_true")
    run_daily.add_argument("--real-llm", action="store_true")
    run_daily.add_argument("--deliver-alerts", action="store_true")
    run_daily.add_argument("--json", action="store_true")

    ingest = subparsers.add_parser("ingest-csv")
    ingest.add_argument("--securities", type=Path, required=True)
    ingest.add_argument("--daily-bars", type=Path, required=True)
    ingest.add_argument("--holdings", type=Path)

    market_bars = subparsers.add_parser("market-bars")
    market_bars_sub = market_bars.add_subparsers(
        dest="market_bars_command",
        required=True,
    )
    market_bars_template = market_bars_sub.add_parser("template")
    market_bars_template.add_argument("--database-url")
    market_bars_template.add_argument(
        "--expected-as-of",
        type=date.fromisoformat,
        required=True,
    )
    market_bars_template.add_argument("--out", type=Path, required=True)
    market_bars_template.add_argument("--provider", default="manual_csv")
    market_bars_template.add_argument("--missing-only", action="store_true")
    market_bars_template.add_argument("--json", action="store_true")
    market_bars_import = market_bars_sub.add_parser("import")
    market_bars_import.add_argument("--database-url")
    market_bars_import.add_argument("--daily-bars", type=Path, required=True)
    market_bars_import.add_argument("--expected-as-of", type=date.fromisoformat)
    market_bars_import.add_argument("--execute", action="store_true")
    market_bars_import.add_argument("--json", action="store_true")

    polygon = subparsers.add_parser("ingest-polygon")
    polygon_sub = polygon.add_subparsers(dest="polygon_command", required=True)
    grouped = polygon_sub.add_parser("grouped-daily")
    grouped.add_argument("--date", type=date.fromisoformat, required=True)
    grouped.add_argument("--fixture", type=Path)
    grouped.add_argument("--confirm-external-call", action="store_true")
    tickers = polygon_sub.add_parser("tickers")
    tickers.add_argument("--fixture", type=Path)
    tickers.add_argument("--date", type=date.fromisoformat)
    tickers.add_argument("--max-pages", type=int)
    tickers.add_argument("--confirm-external-call", action="store_true")

    sec = subparsers.add_parser("ingest-sec")
    sec_sub = sec.add_subparsers(dest="sec_command", required=True)
    submissions = sec_sub.add_parser("submissions")
    submissions.add_argument("--ticker", required=True)
    submissions.add_argument("--cik", required=True)
    submissions.add_argument("--fixture", type=Path)
    submissions_batch = sec_sub.add_parser("submissions-batch")
    submissions_batch.add_argument(
        "--target",
        action="append",
        required=True,
        help="Ticker and CIK pair in TICKER:CIK form.",
    )
    submissions_batch.add_argument("--fixture", type=Path)
    company_tickers = sec_sub.add_parser("company-tickers")
    company_tickers.add_argument("--fixture", type=Path)
    cik_overrides = sec_sub.add_parser("cik-overrides")
    cik_overrides.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="CSV with ticker,cik[,sec_company_name] columns. Makes no external calls.",
    )
    cik_overrides.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the CSV without writing to the database.",
    )
    cik_overrides_template = sec_sub.add_parser("cik-overrides-template")
    cik_overrides_template.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output CSV template for catalyst_events rows missing SEC CIK metadata.",
    )
    cik_overrides_template.add_argument(
        "--stocks-only",
        action="store_true",
        help="Restrict the template to stock-like priced-in scan rows.",
    )
    cik_overrides_template.add_argument("--json", action="store_true")
    ipo_s1 = sec_sub.add_parser("ipo-s1")
    ipo_s1.add_argument("--ticker", required=True)
    ipo_s1.add_argument("--cik", required=True)
    ipo_s1.add_argument("--fixture", type=Path)
    ipo_s1.add_argument("--document-fixture", type=Path)

    news = subparsers.add_parser("ingest-news")
    news.add_argument("--fixture", type=Path, required=True)

    earnings = subparsers.add_parser("ingest-earnings")
    earnings.add_argument("--fixture", type=Path, required=True)

    options = subparsers.add_parser("ingest-options")
    options.add_argument("--fixture", type=Path)
    options.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate an options fixture without importing or writing to the database.",
    )
    options.add_argument("--expected-as-of", type=date.fromisoformat)
    options.add_argument(
        "--fixture-template",
        action="store_true",
        help="Write a zero-call point-in-time options fixture template.",
    )
    options.add_argument("--out", type=Path)
    options.add_argument(
        "--stocks-only",
        action="store_true",
        help="Restrict the fixture template to stock-like priced-in scan rows.",
    )
    options.add_argument(
        "--from-schwab-market",
        action="store_true",
        help="Promote stored Schwab market snapshots into aggregate option features.",
    )
    options.add_argument("--ticker", action="append")
    options.add_argument("--json", action="store_true")

    schwab_market_sync = subparsers.add_parser("schwab-market-sync")
    schwab_market_sync.add_argument("--ticker", action="append", required=True)
    schwab_market_sync.add_argument("--skip-history", action="store_true")
    schwab_market_sync.add_argument("--skip-options", action="store_true")
    schwab_market_sync.add_argument("--json", action="store_true")

    events = subparsers.add_parser("events")
    events.add_argument("--ticker", required=True)
    events.add_argument("--as-of", type=date.fromisoformat, required=True)
    events.add_argument("--available-at", type=_parse_aware_datetime)
    events.add_argument("--limit", type=int, default=20)

    ipo_s1_analysis = subparsers.add_parser("ipo-s1-analysis")
    ipo_s1_analysis.add_argument("--ticker", required=True)
    ipo_s1_analysis.add_argument("--as-of", type=date.fromisoformat)
    ipo_s1_analysis.add_argument("--available-at", type=_parse_aware_datetime)
    ipo_s1_analysis.add_argument("--json", action="store_true")

    run_textint = subparsers.add_parser("run-textint")
    run_textint.add_argument("--as-of", type=date.fromisoformat, required=True)
    run_textint.add_argument("--available-at", type=_parse_aware_datetime)
    run_textint.add_argument("--ontology", type=Path, default=Path("config/themes.yaml"))
    run_textint.add_argument("--ticker", action="append")

    text_features = subparsers.add_parser("text-features")
    text_features.add_argument("--ticker", required=True)
    text_features.add_argument("--as-of", type=date.fromisoformat, required=True)
    text_features.add_argument("--available-at", type=_parse_aware_datetime)

    scan = subparsers.add_parser("scan")
    scan.add_argument("--as-of", type=date.fromisoformat, required=True)
    scan.add_argument("--available-at", type=_parse_aware_datetime)
    scan.add_argument("--provider")
    scan.add_argument("--universe")

    build_packets = subparsers.add_parser("build-packets")
    build_packets.add_argument("--as-of", type=date.fromisoformat, required=True)
    build_packets.add_argument("--available-at", type=_parse_aware_datetime)
    build_packets.add_argument("--ticker", action="append")
    build_packets.add_argument(
        "--min-state",
        choices=[state.value for state in ActionState],
        default=ActionState.WARNING.value,
    )

    build_cards = subparsers.add_parser("build-decision-cards")
    build_cards.add_argument("--as-of", type=date.fromisoformat, required=True)
    build_cards.add_argument("--available-at", type=_parse_aware_datetime)
    build_cards.add_argument("--ticker", action="append")
    build_cards.add_argument(
        "--min-state",
        choices=[state.value for state in ActionState],
        default=ActionState.WARNING.value,
    )

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

    budget_status = subparsers.add_parser("llm-budget-status")
    budget_status.add_argument("--available-at", type=_parse_aware_datetime)
    budget_status.add_argument("--json", action="store_true")

    llm_review = subparsers.add_parser("run-llm-review")
    llm_review.add_argument("--ticker", required=True)
    llm_review.add_argument("--as-of", type=date.fromisoformat, required=True)
    llm_review.add_argument("--available-at", type=_parse_aware_datetime)
    llm_review.add_argument(
        "--task",
        choices=["mini_extraction", "mid_review", "skeptic_review", "gpt55_decision_card"],
        default="mid_review",
    )
    llm_review.add_argument("--fake", action="store_true")
    llm_review.add_argument("--dry-run", action="store_true")
    llm_review.add_argument("--json", action="store_true")

    packet = subparsers.add_parser("candidate-packet")
    packet.add_argument("--ticker", required=True)
    packet.add_argument("--as-of", type=date.fromisoformat, required=True)
    packet.add_argument("--available-at", type=_parse_aware_datetime)
    packet.add_argument("--json", action="store_true")

    card = subparsers.add_parser("decision-card")
    card.add_argument("--ticker", required=True)
    card.add_argument("--as-of", type=date.fromisoformat, required=True)
    card.add_argument("--available-at", type=_parse_aware_datetime)
    card.add_argument("--json", action="store_true")

    validation_replay = subparsers.add_parser("validation-replay")
    validation_replay.add_argument("--as-of-start", type=date.fromisoformat, required=True)
    validation_replay.add_argument("--as-of-end", type=date.fromisoformat, required=True)
    validation_replay.add_argument("--available-at", type=_parse_aware_datetime, required=True)
    validation_replay.add_argument("--outcome-available-at", type=_parse_aware_datetime)
    validation_replay.add_argument("--ticker", action="append")
    validation_replay.add_argument(
        "--state",
        action="append",
        choices=[state.value for state in ActionState],
    )

    validation_report = subparsers.add_parser("validation-report")
    validation_report.add_argument("--run-id", required=True)
    validation_report.add_argument("--available-at", type=_parse_aware_datetime)
    validation_report.add_argument("--json", action="store_true")

    paper_decision = subparsers.add_parser("paper-decision")
    paper_decision.add_argument("--decision-card-id", required=True)
    paper_decision.add_argument(
        "--decision",
        choices=[decision.value for decision in PaperDecision],
        required=True,
    )
    paper_decision.add_argument("--available-at", type=_parse_aware_datetime, required=True)
    paper_decision.add_argument("--entry-price", type=float)
    paper_decision.add_argument("--entry-at", type=_parse_aware_datetime)
    paper_decision.add_argument("--override-reason")

    paper_update = subparsers.add_parser("paper-update-outcomes")
    paper_update.add_argument("--decision-card-id", required=True)
    paper_update.add_argument("--available-at", type=_parse_aware_datetime, required=True)
    paper_update.add_argument("--labels-json", type=Path)

    useful_label = subparsers.add_parser("useful-label")
    useful_label.add_argument(
        "--artifact-type",
        choices=["candidate_packet", "decision_card", "paper_trade", "alert"],
        required=True,
    )
    useful_label.add_argument("--artifact-id", required=True)
    useful_label.add_argument("--ticker", required=True)
    useful_label.add_argument("--label", required=True)
    useful_label.add_argument("--notes")
    useful_label.add_argument("--created-at", type=_parse_aware_datetime)

    build_universe = subparsers.add_parser("build-universe")
    build_universe.add_argument("--name")
    build_universe.add_argument("--provider")
    build_universe.add_argument("--as-of", type=date.fromisoformat, required=True)
    build_universe.add_argument("--available-at", type=_parse_aware_datetime)

    provider_health = subparsers.add_parser("provider-health")
    provider_health.add_argument("--provider", required=True)

    dashboard_snapshot = subparsers.add_parser("dashboard-snapshot")
    dashboard_snapshot.add_argument("--database-url")
    dashboard_snapshot.add_argument("--ticker")
    dashboard_snapshot.add_argument("--available-at", type=_parse_aware_datetime)
    dashboard_snapshot.add_argument("--alert-status")
    dashboard_snapshot.add_argument("--alert-route")
    dashboard_snapshot.add_argument(
        "--scan-mode",
        "--priced-in-status",
        dest="priced_in_status",
        default="all",
        help="Insights queue mode: actionable/mismatches or all/full.",
    )
    dashboard_snapshot.add_argument("--telemetry-limit", type=int, default=8)
    dashboard_snapshot.add_argument(
        "--scan-limit",
        type=int,
        default=50,
        help="Insights rows per page for full-scan/mismatch queue views.",
    )
    dashboard_snapshot.add_argument(
        "--scan-offset",
        type=int,
        default=0,
        help="Zero-based Insights row offset for paging through the scan.",
    )
    dashboard_snapshot.add_argument(
        "--usefulness",
        help=(
            "Filter Insights rows by usefulness verdict: useful, research_useful, "
            "decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    dashboard_snapshot.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter Insights rows missing or stale for a source class. Repeat or "
            "comma-separate: market_bars,catalyst_events,local_text,options,"
            "theme_peer_sector,broker_context."
        ),
    )
    dashboard_snapshot.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter Insights rows by missing decision evidence. Repeat or "
            "comma-separate: candidate_packet,decision_card,options,broker_context."
        ),
    )
    dashboard_snapshot.add_argument(
        "--stocks-only",
        action="store_true",
        help="Show only common-stock and ADR rows from the ranked priced-in scan.",
    )
    dashboard_snapshot.add_argument("--page", default="overview")
    dashboard_snapshot.add_argument("--json", action="store_true")

    priced_in = subparsers.add_parser("priced-in-queue")
    priced_in.add_argument("--database-url")
    priced_in.add_argument("--limit", type=int, default=20)
    priced_in.add_argument("--offset", type=int, default=0)
    priced_in.add_argument(
        "--all",
        dest="all_rows",
        action="store_true",
        help=(
            "Return every ranked row that matches the current filters. "
            "Best used with --json for full-scan export or tests."
        ),
    )
    priced_in.add_argument("--available-at", type=_parse_aware_datetime)
    priced_in.add_argument("--status")
    priced_in.add_argument(
        "--full-scan",
        action="store_const",
        dest="status",
        const="all",
        help="Review the full ranked scan instead of only actionable mismatches.",
    )
    priced_in.add_argument(
        "--mismatches",
        "--actionable",
        action="store_const",
        dest="status",
        const="actionable",
        help="Review only bullish/bearish not-priced-in mismatches from the full scan.",
    )
    priced_in.add_argument(
        "--decision-ready",
        action="store_true",
        help=(
            "Shortcut for --mismatches --usefulness decision_useful: show rows "
            "that answer the priced-in question without blocked/research-only noise."
        ),
    )
    priced_in.add_argument(
        "--usefulness",
        help=(
            "Filter by usefulness verdict: useful, research_useful, "
            "decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    priced_in.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter rows missing or stale for a source class. Repeat or comma-separate: "
            "market_bars,catalyst_events,local_text,options,theme_peer_sector,broker_context."
        ),
    )
    priced_in.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter rows missing decision evidence. Repeat or comma-separate: "
            "candidate_packet,decision_card,options,broker_context."
        ),
    )
    priced_in.add_argument("--min-gap", type=float)
    priced_in.add_argument(
        "--stocks-only",
        action="store_true",
        help="Show only stock-like rows (common stocks and ADRs) from the ranked scan.",
    )
    priced_in.add_argument("--json", action="store_true")

    priced_in_batches = subparsers.add_parser("priced-in-source-batches")
    priced_in_batches.add_argument("--database-url")
    priced_in_batches.add_argument(
        "--source",
        required=True,
        help="Source to plan, or 'all' for a plan-only overview across sources.",
    )
    priced_in_batches.add_argument("--batch-limit", "--limit", type=int, default=5)
    priced_in_batches.add_argument("--batch-offset", "--offset", type=int, default=0)
    priced_in_batches.add_argument("--batch-size", type=int)
    priced_in_batches.add_argument(
        "--execute-next",
        action="store_true",
        help=(
            "Execute only the next planned source-fill batch. Without this flag, "
            "the command remains plan-only and makes no provider calls."
        ),
    )
    priced_in_batches.add_argument(
        "--execute-batches",
        type=int,
        help=(
            "Execute up to N planned source-fill batches, stopping on blocked or "
            "failed chunks. Must be explicit because this can call providers."
        ),
    )
    priced_in_batches.add_argument(
        "--all",
        dest="all_batches",
        action="store_true",
        help=(
            "Return every planned source-fill batch for the current full-scan "
            "filters. Planning makes no provider calls."
        ),
    )
    priced_in_batches.add_argument("--available-at", type=_parse_aware_datetime)
    priced_in_batches.add_argument("--status")
    priced_in_batches.add_argument(
        "--usefulness",
        help=(
            "Filter by usefulness verdict before planning batches: useful, "
            "research_useful, decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    priced_in_batches.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter rows missing decision evidence before planning batches. Repeat "
            "or comma-separate: candidate_packet,decision_card,options,broker_context."
        ),
    )
    priced_in_batches.add_argument("--min-gap", type=float)
    priced_in_batches.add_argument(
        "--stocks-only",
        action="store_true",
        help=(
            "Plan source-fill batches only for stock-like rows "
            "(common stocks and ADRs) from the ranked scan."
        ),
    )
    priced_in_batches.add_argument("--json", action="store_true")

    priced_in_preflight = subparsers.add_parser("priced-in-preflight")
    priced_in_preflight.add_argument("--database-url")
    priced_in_preflight.add_argument("--json", action="store_true")

    priced_in_answer = subparsers.add_parser("priced-in-answer")
    priced_in_answer.add_argument("--database-url")
    priced_in_answer.add_argument("--limit", type=int, default=5)
    priced_in_answer.add_argument("--available-at", type=_parse_aware_datetime)
    priced_in_answer.add_argument("--status")
    priced_in_answer.add_argument(
        "--usefulness",
        help=(
            "Filter answer rows by usefulness verdict: useful, research_useful, "
            "decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    priced_in_answer.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter answer rows missing or stale for a source class. Repeat or "
            "comma-separate: market_bars,catalyst_events,local_text,options,"
            "theme_peer_sector,broker_context."
        ),
    )
    priced_in_answer.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter answer rows missing decision evidence. Repeat or comma-separate: "
            "candidate_packet,decision_card,options,broker_context."
        ),
    )
    priced_in_answer.add_argument("--min-gap", type=float)
    priced_in_answer.add_argument(
        "--stocks-only",
        action="store_true",
        help="Answer using only stock-like rows (common stocks and ADRs).",
    )
    priced_in_answer.add_argument("--json", action="store_true")

    priced_in_audit = subparsers.add_parser("priced-in-audit")
    priced_in_audit.add_argument("--database-url")
    priced_in_audit.add_argument("--available-at", type=_parse_aware_datetime)
    priced_in_audit.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Full-scan audit preview rows to show.",
    )
    priced_in_audit.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Zero-based full-scan audit preview row offset.",
    )
    priced_in_audit.add_argument(
        "--all",
        dest="all_rows",
        action="store_true",
        help=(
            "Return every ranked full-scan audit row. Best used with --json for "
            "full-universe export or functional tests."
        ),
    )
    priced_in_audit.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter audit preview rows missing or stale for a source class. Repeat "
            "or comma-separate: market_bars,catalyst_events,local_text,options,"
            "theme_peer_sector,broker_context."
        ),
    )
    priced_in_audit.add_argument(
        "--stocks-only",
        action="store_true",
        help="Audit only stock-like rows (common stocks and ADRs) from the full scan.",
    )
    priced_in_audit.add_argument("--json", action="store_true")

    candidate_detail = subparsers.add_parser("candidate-detail")
    candidate_detail.add_argument("ticker")
    candidate_detail.add_argument("--database-url")
    candidate_detail.add_argument("--json", action="store_true")

    agent_brief = subparsers.add_parser("agent-brief")
    agent_brief.add_argument("--database-url")
    agent_brief.add_argument("--ticker")
    agent_brief.add_argument("--available-at", type=_parse_aware_datetime)
    agent_brief.add_argument("--alert-status")
    agent_brief.add_argument("--alert-route")
    agent_brief.add_argument(
        "--scan-mode",
        "--priced-in-status",
        dest="priced_in_status",
        default="all",
        help="Insights queue mode used in the agent brief context.",
    )
    agent_brief.add_argument("--telemetry-limit", type=int, default=8)
    agent_brief.add_argument(
        "--scan-limit",
        type=int,
        default=50,
        help="Insights rows per page included in the agent brief context.",
    )
    agent_brief.add_argument(
        "--scan-offset",
        type=int,
        default=0,
        help="Zero-based Insights row offset included in the agent brief context.",
    )
    agent_brief.add_argument(
        "--usefulness",
        help=(
            "Filter brief context by usefulness verdict: useful, research_useful, "
            "decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    agent_brief.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter brief context by missing or stale source evidence. Repeat or "
            "comma-separate: market_bars,catalyst_events,local_text,options,"
            "theme_peer_sector,broker_context."
        ),
    )
    agent_brief.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter brief context by missing decision evidence. Repeat or "
            "comma-separate: candidate_packet,decision_card,options,broker_context."
        ),
    )
    agent_brief.add_argument(
        "--stocks-only",
        action="store_true",
        help="Brief only common-stock and ADR rows from the ranked priced-in scan.",
    )
    agent_brief.add_argument("--goal")
    agent_brief.add_argument("--real", action="store_true")
    agent_brief.add_argument("--json", action="store_true")

    dashboard_tui = subparsers.add_parser("dashboard-tui")
    dashboard_tui.add_argument("--database-url")
    dashboard_tui.add_argument("--ticker")
    dashboard_tui.add_argument("--available-at", type=_parse_aware_datetime)
    dashboard_tui.add_argument("--alert-status")
    dashboard_tui.add_argument("--alert-route")
    dashboard_tui.add_argument(
        "--scan-mode",
        "--priced-in-status",
        dest="priced_in_status",
        default="all",
        help="Insights queue mode: actionable/mismatches or all/full.",
    )
    dashboard_tui.add_argument("--telemetry-limit", type=int, default=8)
    dashboard_tui.add_argument(
        "--scan-limit",
        type=int,
        default=50,
        help="Insights rows per page for full-scan/mismatch queue views.",
    )
    dashboard_tui.add_argument(
        "--scan-offset",
        type=int,
        default=0,
        help="Zero-based Insights row offset for paging through the scan.",
    )
    dashboard_tui.add_argument(
        "--usefulness",
        help=(
            "Filter Insights rows by usefulness verdict: useful, research_useful, "
            "decision_useful, blocked, monitor_only, not_useful."
        ),
    )
    dashboard_tui.add_argument(
        "--source-gap",
        action="append",
        help=(
            "Filter Insights rows missing or stale for a source class. Repeat or "
            "comma-separate: market_bars,catalyst_events,local_text,options,"
            "theme_peer_sector,broker_context."
        ),
    )
    dashboard_tui.add_argument(
        "--decision-gap",
        action="append",
        help=(
            "Filter Insights rows by missing decision evidence. Repeat or "
            "comma-separate: candidate_packet,decision_card,options,broker_context."
        ),
    )
    dashboard_tui.add_argument(
        "--stocks-only",
        action="store_true",
        help="Start the terminal dashboard in stock-only priced-in scan scope.",
    )
    dashboard_tui.add_argument("--page", default="overview")
    dashboard_tui.add_argument("--once", action="store_true")
    dashboard_tui.add_argument("--no-clear", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    dotenv_loaded = load_app_dotenv()
    args = build_parser().parse_args(argv)
    config = AppConfig.from_env()
    database_url = getattr(args, "database_url", None) or config.database_url
    config = replace(config, database_url=database_url)
    engine = engine_from_url(database_url)

    if args.command == "init-db":
        create_schema(engine)
        print("initialized database")
        return 0

    if args.command == "seed-dashboard-demo":
        create_schema(engine)
        try:
            result = seed_dashboard_demo(
                engine,
                ticker=args.ticker,
                cik=args.cik,
                sec_fixture_path=args.sec_fixture or default_sec_fixture_path(),
                document_fixture_path=(
                    args.document_fixture or default_sec_document_fixture_path()
                ),
            )
        except (FileNotFoundError, ValueError) as exc:
            print(f"seed dashboard demo failed: {exc}", file=sys.stderr)
            return 1
        print(
            f"seeded dashboard demo ticker={result.ticker} "
            f"sec_events={result.sec_result.event_count} "
            f"candidate_state={result.candidate_state_id} "
            f"alert={result.alert_id} "
            f"validation_run={result.validation_run_id} "
            f"budget_ledger={result.budget_ledger_id}"
        )
        return 0

    if args.command == "run-daily":
        if args.real_llm:
            print(
                "run-daily --real-llm is not supported; use run-llm-review per candidate",
                file=sys.stderr,
            )
            return 2
        if args.deliver_alerts:
            print(
                "run-daily --deliver-alerts is not supported; use send-alerts --dry-run",
                file=sys.stderr,
            )
            return 2
        create_schema(engine)
        scheduler_config = SchedulerConfig(
            owner="cli",
            lock_name="daily-run",
            as_of=args.as_of,
            decision_available_at=args.available_at,
            outcome_available_at=args.outcome_available_at,
            provider=args.provider,
            universe=args.universe,
            tickers=tuple(args.ticker or ()),
            run_llm=args.run_llm,
            llm_dry_run=not args.real_llm,
            dry_run_alerts=not args.deliver_alerts,
        )
        result = run_once(engine=engine, config=scheduler_config)
        if args.json:
            print(json.dumps(scheduler_run_payload(result), sort_keys=True))
        elif result.reason == "lock_held":
            print("daily run skipped: lock held")
        elif result.daily_result is not None:
            print(f"daily run status={result.daily_result.status}")
        return _scheduler_exit_code(result)

    if args.command == "ingest-csv":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        connector = CsvMarketDataConnector(
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )
        return _ingest_csv_provider(
            connector=connector,
            market_repo=market_repo,
            provider_repo=provider_repo,
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )

    if args.command == "market-bars":
        create_schema(engine)
        try:
            if args.market_bars_command == "template":
                result = write_manual_market_bars_template(
                    engine,
                    output_path=args.out,
                    expected_as_of=args.expected_as_of,
                    provider=args.provider,
                    missing_only=args.missing_only,
                )
                payload = result.as_payload()
                if args.json:
                    print(json.dumps(payload, sort_keys=True))
                else:
                    _print_manual_market_bars_template(payload)
                return 0
            if args.market_bars_command == "import":
                result = import_manual_market_bars(
                    engine,
                    daily_bars_path=args.daily_bars,
                    expected_as_of=args.expected_as_of,
                    execute=args.execute,
                )
                payload = result.as_payload()
                if args.json:
                    print(json.dumps(payload, sort_keys=True))
                else:
                    _print_manual_market_bars_import(payload)
                return 0 if result.status in {"ready", "imported"} else 2
        except (FileNotFoundError, KeyError, ValueError) as exc:
            print(f"manual market bars failed: {exc}", file=sys.stderr)
            return 1

    if args.command == "ingest-polygon":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        return _ingest_polygon_provider(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            polygon_command=args.polygon_command,
            date_value=args.date if hasattr(args, "date") else None,
            fixture_path=args.fixture,
            max_pages=getattr(args, "max_pages", None),
            confirm_external_call=getattr(args, "confirm_external_call", False),
        )

    if args.command == "ingest-sec":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        if args.sec_command == "company-tickers":
            return _refresh_sec_cik_metadata_cli(
                engine=engine,
                config=config,
                fixture_path=args.fixture,
            )
        if args.sec_command == "cik-overrides":
            return _apply_sec_cik_overrides_cli(
                engine=engine,
                csv_path=args.csv,
                validate_only=args.validate_only,
            )
        if args.sec_command == "cik-overrides-template":
            return _write_sec_cik_override_template_cli(
                engine=engine,
                config=config,
                output_path=args.out,
                stocks_only=args.stocks_only,
                as_json=args.json,
            )
        if args.sec_command == "submissions-batch":
            return _ingest_sec_submissions_batch(
                config=config,
                market_repo=market_repo,
                provider_repo=provider_repo,
                event_repo=event_repo,
                targets=args.target,
                fixture_path=args.fixture,
            )
        return _ingest_sec_provider(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            sec_command=args.sec_command,
            ticker=args.ticker,
            cik=args.cik,
            fixture_path=args.fixture,
            document_fixture_path=getattr(args, "document_fixture", None),
        )

    if args.command == "ingest-news":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_news_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            fixture_path=args.fixture,
        )

    if args.command == "ingest-earnings":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_earnings_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            fixture_path=args.fixture,
        )

    if args.command == "ingest-options":
        create_schema(engine)
        if args.validate_only:
            if args.fixture is None:
                print(
                    "options fixture validation failed: provide --fixture",
                    file=sys.stderr,
                )
                return 1
            return _validate_options_fixture_cli(
                fixture_path=args.fixture,
                expected_as_of=args.expected_as_of,
                as_json=args.json,
            )
        if args.fixture_template:
            if args.out is None:
                print(
                    "options fixture template failed: provide --out",
                    file=sys.stderr,
                )
                return 1
            return _write_options_fixture_template_cli(
                engine=engine,
                config=config,
                output_path=args.out,
                stocks_only=args.stocks_only,
                as_json=args.json,
            )
        feature_repo = FeatureRepository(engine)
        if args.from_schwab_market:
            return _ingest_schwab_market_options(
                broker_repo=BrokerRepository(engine),
                feature_repo=feature_repo,
                tickers=args.ticker or [],
            )
        if args.fixture is None:
            print(
                "options ingest failed: provide --fixture or --from-schwab-market",
                file=sys.stderr,
            )
            return 1
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        return _ingest_options_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            feature_repo=feature_repo,
            fixture_path=args.fixture,
        )

    if args.command == "schwab-market-sync":
        return _sync_schwab_market_context_cli(args)

    if args.command == "events":
        create_schema(engine)
        event_repo = EventRepository(engine)
        as_of = datetime.combine(args.as_of, time.max, tzinfo=UTC)
        available_at = args.available_at or datetime.now(UTC)
        for event in event_repo.list_events_for_ticker(
            args.ticker,
            as_of=as_of,
            available_at=available_at,
            limit=args.limit,
        ):
            print(
                f"{event.ticker} {event.available_at.isoformat()} "
                f"{event.event_type.value} materiality={event.materiality:.2f} "
                f"quality={event.source_quality:.2f} source={event.source} "
                f"title={event.title}"
            )
        return 0

    if args.command == "ipo-s1-analysis":
        create_schema(engine)
        event_repo = EventRepository(engine)
        as_of = (
            datetime.combine(args.as_of, time.max, tzinfo=UTC)
            if args.as_of is not None
            else datetime.now(UTC)
        )
        available_at = args.available_at or datetime.now(UTC)
        events_with_analysis = [
            event
            for event in event_repo.list_events_for_ticker(
                args.ticker,
                as_of=as_of,
                available_at=available_at,
                limit=50,
            )
            if isinstance(event.payload, Mapping) and "ipo_analysis" in event.payload
        ]
        if not events_with_analysis:
            print(f"ipo_s1_analysis ticker={args.ticker.upper()} status=not_found")
            return 1
        payload = _ipo_s1_analysis_payload(events_with_analysis[0])
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            analysis = payload["analysis"]
            print(
                f"ipo_s1_analysis ticker={payload['ticker']} "
                f"form={payload['form_type']} filed={payload['filing_date']} "
                f"source={payload['source_url']}"
            )
            print(f"summary={payload['summary']}")
            print(
                "terms="
                f"symbol={analysis.get('proposed_ticker')} "
                f"exchange={analysis.get('exchange')} "
                f"shares={analysis.get('shares_offered')} "
                f"price_low={analysis.get('price_range_low')} "
                f"price_high={analysis.get('price_range_high')} "
                f"gross_proceeds={analysis.get('estimated_gross_proceeds')}"
            )
            underwriters = analysis.get("underwriters") or []
            risk_flags = analysis.get("risk_flags") or []
            print(f"underwriters={', '.join(str(item) for item in underwriters) or 'none'}")
            print(f"risk_flags={', '.join(str(item) for item in risk_flags) or 'none'}")
        return 0

    if args.command == "provider-health":
        create_schema(engine)
        provider_repo = ProviderRepository(engine)
        health = provider_repo.latest_health(args.provider)
        if health is None:
            print(f"provider={args.provider} status=unknown")
            return 1
        print(f"provider={health.provider} status={health.status.value}")
        return 0

    if args.command == "dashboard-snapshot":
        create_schema(engine)
        filters = DashboardFilters(
            ticker=args.ticker,
            available_at=args.available_at,
            alert_status=args.alert_status,
            alert_route=args.alert_route,
            priced_in_status=args.priced_in_status,
            priced_in_usefulness=args.usefulness,
            priced_in_source_gap=args.source_gap,
            priced_in_decision_gap=args.decision_gap,
            priced_in_stocks_only=args.stocks_only,
            priced_in_limit=args.scan_limit,
            priced_in_offset=args.scan_offset,
            telemetry_limit=args.telemetry_limit,
        )
        payload = dashboard_snapshot_payload(
            engine=engine,
            config=config,
            dotenv_loaded=dotenv_loaded,
            filters=filters,
        )
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            print(render_dashboard_tui(payload, page=args.page))
        return 0

    if args.command == "priced-in-queue":
        create_schema(engine)
        priced_in_status = "actionable" if args.decision_ready else args.status
        priced_in_usefulness = (
            "decision_useful" if args.decision_ready else args.usefulness
        )
        payload = priced_in_queue_payload(
            engine,
            config,
            limit=1_000_000 if args.all_rows else args.limit,
            offset=0 if args.all_rows else args.offset,
            available_at=args.available_at,
            status=priced_in_status,
            usefulness=priced_in_usefulness,
            source_gap=args.source_gap,
            decision_gap=args.decision_gap,
            min_gap=args.min_gap,
            stocks_only=args.stocks_only,
        )
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            _print_priced_in_queue(payload)
        return 0

    if args.command == "priced-in-source-batches":
        create_schema(engine)
        execute_batches = int(args.execute_batches or 0)
        if args.execute_next and args.all_batches:
            print(
                "priced-in-source-batches --execute-next cannot be combined with --all",
                file=sys.stderr,
            )
            return 2
        if execute_batches and args.all_batches:
            print(
                "priced-in-source-batches --execute-batches cannot be combined with --all",
                file=sys.stderr,
            )
            return 2
        if args.execute_next and execute_batches:
            print(
                "priced-in-source-batches choose either --execute-next or "
                "--execute-batches, not both",
                file=sys.stderr,
            )
            return 2
        if args.execute_batches is not None and execute_batches <= 0:
            print("priced-in-source-batches --execute-batches must be positive", file=sys.stderr)
            return 2
        source_name = str(args.source or "").strip().lower()
        if (args.execute_next or execute_batches) and source_name in {"all", "*"}:
            print(
                "priced-in-source-batches --source all is plan-only; choose one "
                "source before executing batches",
                file=sys.stderr,
            )
            return 2
        try:
            if args.execute_next:
                payload = execute_priced_in_source_batch(
                    engine,
                    config,
                    source=args.source,
                    available_at=args.available_at,
                    status=args.status,
                    usefulness=args.usefulness,
                    decision_gap=args.decision_gap,
                    min_gap=args.min_gap,
                    stocks_only=args.stocks_only,
                )
            elif execute_batches:
                payload = execute_priced_in_source_batches(
                    engine,
                    config,
                    source=args.source,
                    max_batches=execute_batches,
                    available_at=args.available_at,
                    status=args.status,
                    usefulness=args.usefulness,
                    decision_gap=args.decision_gap,
                    min_gap=args.min_gap,
                    stocks_only=args.stocks_only,
                )
            elif source_name in {"all", "*"}:
                payload = priced_in_all_source_gap_batches_payload(
                    engine,
                    config,
                    batch_size=args.batch_size,
                    available_at=args.available_at,
                    status=args.status,
                    usefulness=args.usefulness,
                    decision_gap=args.decision_gap,
                    min_gap=args.min_gap,
                    stocks_only=args.stocks_only,
                )
            else:
                payload = priced_in_source_gap_batches_payload(
                    engine,
                    config,
                    source=args.source,
                    batch_limit=args.batch_limit,
                    batch_offset=args.batch_offset,
                    batch_size=args.batch_size,
                    all_batches=args.all_batches,
                    available_at=args.available_at,
                    status=args.status,
                    usefulness=args.usefulness,
                    decision_gap=args.decision_gap,
                    min_gap=args.min_gap,
                    stocks_only=args.stocks_only,
                )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        elif execute_batches:
            _print_priced_in_source_batch_run(payload)
        elif args.execute_next:
            _print_priced_in_source_batch_execution(payload)
        elif str(payload.get("schema_version") or "") == (
            "priced-in-source-batch-overview-v1"
        ):
            _print_priced_in_all_source_batches(payload)
        else:
            _print_priced_in_source_batches(payload)
        if args.execute_next:
            execution_status = str(payload.get("status") or "")
            return 0 if execution_status in {"executed", "no_action"} else 1
        if execute_batches:
            execution_status = str(payload.get("status") or "")
            return 0 if execution_status in {"executed", "complete", "no_action"} else 1
        return 0

    if args.command == "priced-in-preflight":
        create_schema(engine)
        payload = priced_in_preflight_payload(engine, config)
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            _print_priced_in_preflight(payload)
        return 0

    if args.command == "priced-in-answer":
        create_schema(engine)
        payload = priced_in_answer_payload(
            engine,
            config,
            limit=args.limit,
            available_at=args.available_at,
            status=args.status,
            usefulness=args.usefulness,
            source_gap=args.source_gap,
            decision_gap=args.decision_gap,
            min_gap=args.min_gap,
            stocks_only=args.stocks_only,
        )
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            _print_priced_in_answer(payload)
        return 0

    if args.command == "priced-in-audit":
        create_schema(engine)
        payload = priced_in_full_scan_audit_payload(
            engine,
            config,
            available_at=args.available_at,
            source_gap=args.source_gap,
            preview_limit=1_000_000 if args.all_rows else args.limit,
            preview_offset=0 if args.all_rows else args.offset,
            all_rows=args.all_rows,
            stocks_only=args.stocks_only,
        )
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            _print_priced_in_audit(payload)
        return 0

    if args.command == "candidate-detail":
        create_schema(engine)
        payload = load_ticker_detail(engine, args.ticker.upper())
        if payload is None:
            print(f"candidate detail not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        else:
            _print_candidate_detail(payload)
        return 0

    if args.command == "agent-brief":
        create_schema(engine)
        filters = DashboardFilters(
            ticker=args.ticker,
            available_at=args.available_at,
            alert_status=args.alert_status,
            alert_route=args.alert_route,
            priced_in_status=args.priced_in_status,
            priced_in_usefulness=args.usefulness,
            priced_in_source_gap=args.source_gap,
            priced_in_decision_gap=args.decision_gap,
            priced_in_stocks_only=args.stocks_only,
            priced_in_limit=args.scan_limit,
            priced_in_offset=args.scan_offset,
            telemetry_limit=args.telemetry_limit,
        )
        payload = dashboard_snapshot_payload(
            engine=engine,
            config=config,
            dotenv_loaded=dotenv_loaded,
            filters=filters,
        )
        brief = run_market_radar_agents(
            payload,
            config,
            real=args.real,
            operator_goal=args.goal,
        )
        if args.json:
            print(json.dumps(brief, default=dashboard_json_default, sort_keys=True))
        else:
            _print_agent_brief(brief)
        return 2 if args.real and brief.get("status") == "blocked" else 0

    if args.command == "dashboard-tui":
        create_schema(engine)
        filters = DashboardFilters(
            ticker=args.ticker,
            available_at=args.available_at,
            alert_status=args.alert_status,
            alert_route=args.alert_route,
            priced_in_status=args.priced_in_status,
            priced_in_usefulness=args.usefulness,
            priced_in_source_gap=args.source_gap,
            priced_in_decision_gap=args.decision_gap,
            priced_in_stocks_only=args.stocks_only,
            priced_in_limit=args.scan_limit,
            priced_in_offset=args.scan_offset,
            telemetry_limit=args.telemetry_limit,
        )
        filters = dashboard_filters_for_page(filters, args.page)
        if args.once:
            payload = dashboard_snapshot_payload(
                engine=engine,
                config=config,
                dotenv_loaded=dotenv_loaded,
                filters=filters,
            )
            print(render_dashboard_tui(payload, page=args.page))
            return 0
        return run_dashboard_tui(
            engine=engine,
            config=config,
            dotenv_loaded=dotenv_loaded,
            filters=filters,
            initial_page=args.page,
            clear_screen=not args.no_clear,
        )

    if args.command == "run-textint":
        create_schema(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        tickers = args.ticker if args.ticker else None
        result = run_text_pipeline(
            event_repo,
            text_repo,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
            ontology_path=args.ontology,
            tickers=tickers,
        )
        print(f"processed text_features={result.feature_count} snippets={result.snippet_count}")
        return 0

    if args.command == "text-features":
        create_schema(engine)
        text_repo = TextRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        features = text_repo.latest_text_features_by_ticker(
            [args.ticker],
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
        )
        feature = features.get(args.ticker.upper())
        if feature is None:
            print(f"text feature not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        print(
            f"{feature.ticker} local_narrative={feature.local_narrative_score:.2f} "
            f"novelty={feature.novelty_score:.2f} "
            f"snippets={len(feature.selected_snippet_ids)}"
        )
        return 0

    if args.command == "scan":
        create_schema(engine)
        repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        feature_repo = FeatureRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        universe_tickers = _universe_tickers_for_scan(
            provider_repo=provider_repo,
            universe_name=args.universe,
            as_of=args.as_of,
            available_at=available_at,
        )
        if args.universe is not None and universe_tickers is None:
            print(f"universe not found: {args.universe}", file=sys.stderr)
            return 1
        scan_provider = args.provider
        if args.universe is not None:
            snapshot = _universe_snapshot_for_scan(
                provider_repo=provider_repo,
                universe_name=args.universe,
                as_of=args.as_of,
                available_at=available_at,
            )
            scan_provider = snapshot.provider if snapshot is not None else scan_provider
        results = run_scan(
            repo,
            as_of=args.as_of,
            available_at=available_at,
            provider=scan_provider,
            universe_tickers=universe_tickers,
            config=config,
            event_repo=event_repo,
            text_repo=text_repo,
            feature_repo=feature_repo,
        )
        for result in results:
            repo.save_scan_result(result.candidate, result.policy)
        print(f"scanned candidates={len(results)}")
        return 0

    if args.command == "build-packets":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        feature_repo = FeatureRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        try:
            packets = _build_candidate_packets(
                packet_repo=packet_repo,
                event_repo=event_repo,
                text_repo=text_repo,
                feature_repo=feature_repo,
                as_of=_scan_timestamp(args.as_of),
                available_at=available_at,
                ticker=args.ticker,
                states=_states_at_or_above(ActionState(args.min_state)),
            )
        except ValueError as exc:
            print(f"build packets failed: {exc}", file=sys.stderr)
            return 1
        print(f"built candidate_packets={len(packets)}")
        return 0

    if args.command == "build-decision-cards":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        feature_repo = FeatureRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        try:
            packets = _build_candidate_packets(
                packet_repo=packet_repo,
                event_repo=event_repo,
                text_repo=text_repo,
                feature_repo=feature_repo,
                as_of=_scan_timestamp(args.as_of),
                available_at=available_at,
                ticker=args.ticker,
                states=_states_at_or_above(ActionState(args.min_state)),
            )
            cards = []
            for packet in packets:
                card = build_decision_card(
                    packet,
                    available_at=available_at,
                    broker_portfolio_context=latest_broker_portfolio_context(
                        engine,
                        ticker=packet.ticker,
                        available_at=available_at,
                        config=config,
                    ),
                )
                packet_repo.upsert_decision_card(card)
                cards.append(card)
        except ValueError as exc:
            print(f"build decision cards failed: {exc}", file=sys.stderr)
            return 1
        print(f"built decision_cards={len(cards)}")
        return 0

    if args.command == "build-alerts":
        create_schema(engine)
        alert_repo = AlertRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        result = plan_alerts(
            alert_repo,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
            ticker=args.ticker,
        )
        if args.json:
            print(json.dumps(_alert_plan_payload(result, available_at), sort_keys=True))
        else:
            print(
                f"built_alerts alerts={len(result.alerts)} "
                f"suppressions={len(result.suppressions)} "
                f"available_at={available_at.isoformat()}"
            )
        return 0

    if args.command == "alerts-list":
        create_schema(engine)
        alert_repo = AlertRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        alerts = _stable_alerts(
            alert_repo.list_alerts(
                available_at=available_at,
                ticker=args.ticker,
                status=args.status,
                route=args.route,
            )
        )
        if args.json:
            print(
                json.dumps(
                    {"alerts": [_alert_cli_payload(alert) for alert in alerts]},
                    sort_keys=True,
                )
            )
        else:
            for alert in alerts:
                print(
                    f"{alert.ticker} alert route={alert.route.value} "
                    f"status={alert.status.value} dedupe_key={alert.dedupe_key}"
                )
        return 0

    if args.command == "alert-digest":
        create_schema(engine)
        alert_repo = AlertRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        alerts = _stable_alerts(alert_repo.list_alerts(available_at=available_at))
        digest_alerts = [
            alert for alert in alerts if getattr(alert.channel, "value", alert.channel) == "digest"
        ]
        suppressions = alert_repo.list_suppressions(available_at=available_at)
        digest = build_alert_digest(digest_alerts, suppressions, available_at)
        payload = digest_payload(digest)
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            print(
                f"alert_digest groups={payload['group_count']} "
                f"alerts={len(alerts)} suppressed={digest.suppressed_count}"
            )
        return 0

    if args.command == "send-alerts":
        create_schema(engine)
        if getattr(args, "dry_run", True) is not True:
            print("external delivery is not enabled in Phase 11", file=sys.stderr)
            return 1
        alert_repo = AlertRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        channel = DryRunAlertChannel()
        alerts = _stable_alerts(
            alert_repo.list_alerts(
                available_at=available_at,
                status=AlertStatus.PLANNED.value,
            )
        )
        deliveries = []
        for alert in alerts:
            delivery = channel.deliver(alert, dry_run=True)
            deliveries.append(delivery)
            alert_repo.upsert_alert(
                replace(alert, status=AlertStatus.DRY_RUN, sent_at=available_at)
            )
        if args.json:
            print(
                json.dumps(
                    {
                        "alerts": len(alerts),
                        "dry_run": True,
                        "deliveries": [
                            {
                                "alert_id": delivery.alert_id,
                                "channel": delivery.channel,
                                "dry_run": delivery.dry_run,
                                "status": delivery.status,
                            }
                            for delivery in deliveries
                        ],
                    },
                    sort_keys=True,
                )
            )
        else:
            print(f"send_alerts dry_run=true alerts={len(alerts)}")
        return 0

    if args.command == "llm-budget-status":
        create_schema(engine)
        ledger_repo = BudgetLedgerRepository(engine)
        available_at = args.available_at or _now_utc()
        payload = _llm_budget_status_payload(
            summary=ledger_repo.summary(available_at=available_at),
            config=config,
            available_at=available_at,
        )
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            summary = payload["summary"]
            status_counts = summary["status_counts"]
            print(
                "llm_budget_status "
                f"actual_cost={float(summary['total_actual_cost_usd']):.6f} "
                f"estimated_cost={float(summary['total_estimated_cost_usd']):.6f} "
                f"attempts={summary['attempt_count']} "
                f"skipped={status_counts.get('skipped', 0)} "
                f"completed={status_counts.get('completed', 0)} "
                "source=budget_ledger"
            )
        return 0

    if args.command == "run-llm-review":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        ledger_repo = BudgetLedgerRepository(engine)
        available_at = args.available_at or _now_utc()
        attempted_at = _now_utc()
        task = DEFAULT_TASKS[args.task]
        packet = packet_repo.latest_candidate_packet(
            args.ticker,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
        )
        if packet is None:
            model = getattr(config, task.model_config_key)
            entry = BudgetLedgerEntry(
                id=budget_ledger_id(
                    task=task.name.value,
                    ticker=args.ticker,
                    candidate_packet_id=None,
                    status=LLMCallStatus.SKIPPED.value,
                    available_at=available_at,
                    prompt_version=task.prompt_version,
                    attempted_at=attempted_at,
                ),
                ts=attempted_at,
                available_at=available_at,
                task=task.name,
                status=LLMCallStatus.SKIPPED,
                estimated_cost=0.0,
                actual_cost=0.0,
                ticker=args.ticker,
                model=str(model).strip() if model else None,
                provider=config.llm_provider,
                skip_reason=LLMSkipReason.CANDIDATE_PACKET_MISSING,
                token_usage=TokenUsage(),
                prompt_version=task.prompt_version,
                schema_version=task.schema_version,
                payload={"error": "candidate packet not found"},
                created_at=attempted_at,
            )
            ledger_repo.upsert_entry(entry)
            _append_model_call_audit_event(
                engine,
                entry=entry,
                actor_source="cli",
                artifact_type="candidate_packet",
                artifact_id=None,
            )
            print(f"candidate packet not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        budget = BudgetController(
            config=config,
            ledger_repo=ledger_repo,
            now=lambda: attempted_at,
        )
        client = _llm_client_for_provider(config=config, fake=args.fake)
        router = LLMRouter(budget=budget, client=client, now=lambda: attempted_at)
        result = router.review_candidate(
            task=task,
            candidate=packet,
            available_at=available_at,
            dry_run=args.dry_run,
        )
        payload = _llm_review_payload(result)
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            fields = [
                f"llm_review ticker={packet.ticker}",
                f"task={task.name.value}",
                f"status={result.status.value}",
            ]
            if result.ledger_entry.skip_reason is not None:
                fields.append(f"reason={result.ledger_entry.skip_reason.value}")
            if result.ledger_entry.model is not None:
                fields.append(f"model={result.ledger_entry.model}")
            fields.extend(
                [
                    f"estimated_cost={result.ledger_entry.estimated_cost:.6f}",
                    f"actual_cost={result.ledger_entry.actual_cost:.6f}",
                    f"ledger_id={result.ledger_entry.id}",
                ]
            )
            print(" ".join(fields))
        if result.status in {LLMCallStatus.FAILED, LLMCallStatus.SCHEMA_REJECTED}:
            return 1
        return 0

    if args.command == "candidate-packet":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        packet = packet_repo.latest_candidate_packet(
            args.ticker,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
        )
        if packet is None:
            print(f"candidate packet not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        if args.json:
            return _print_external_json(thaw_json_value(packet.payload))
        else:
            print(
                f"{packet.ticker} packet state={packet.state.value} "
                f"supporting={len(packet.supporting_evidence)} "
                f"disconfirming={len(packet.disconfirming_evidence)} "
                f"conflicts={len(packet.conflicts)} "
                f"supporting_top={_evidence_summary(packet.supporting_evidence)} "
                f"disconfirming_top={_evidence_summary(packet.disconfirming_evidence)}"
            )
        return 0

    if args.command == "decision-card":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        card = packet_repo.latest_decision_card(
            args.ticker,
            as_of=_scan_timestamp(args.as_of),
            available_at=available_at,
        )
        if card is None:
            print(f"decision card not found: {args.ticker.upper()}", file=sys.stderr)
            return 1
        if args.json:
            return _print_external_json(thaw_json_value(card.payload))
        else:
            evidence = card.payload.get("evidence", [])
            disconfirming = card.payload.get("disconfirming_evidence", [])
            print(
                f"{card.ticker} decision_card state={card.action_state.value} "
                f"next_review_at={card.next_review_at.isoformat()} "
                f"supporting_top={_mapping_evidence_summary(evidence)} "
                f"disconfirming_top={_mapping_evidence_summary(disconfirming)}"
            )
        return 0

    if args.command == "validation-replay":
        create_schema(engine)
        packet_repo = CandidatePacketRepository(engine)
        validation_repo = ValidationRepository(engine)
        as_of_start = _scan_timestamp(args.as_of_start)
        as_of_end = _scan_timestamp(args.as_of_end)
        available_at = args.available_at
        outcome_available_at = args.outcome_available_at or available_at
        if outcome_available_at < available_at:
            print(
                "--outcome-available-at must be greater than or equal to --available-at",
                file=sys.stderr,
            )
            return 1
        states = tuple(ActionState(state) for state in args.state or ())
        tickers = tuple(ticker.upper() for ticker in args.ticker or ())
        run_id = deterministic_replay_run_id(
            as_of_start=as_of_start,
            as_of_end=as_of_end,
            decision_available_at=available_at,
            states=states,
            tickers=tickers,
        )
        run = ValidationRun(
            id=run_id,
            run_type="point_in_time_replay",
            as_of_start=as_of_start,
            as_of_end=as_of_end,
            decision_available_at=available_at,
            status=ValidationRunStatus.RUNNING,
            config={
                "states": [state.value for state in states],
                "tickers": list(tickers),
                "outcome_available_at": outcome_available_at.isoformat(),
                "no_external_calls": True,
            },
        )
        validation_repo.upsert_validation_run(run)
        try:
            results = build_replay_results(
                packet_repo,
                validation_repo,
                as_of_start=as_of_start,
                as_of_end=as_of_end,
                decision_available_at=available_at,
                states=states or None,
                tickers=tickers or None,
                run_id=run_id,
            )
            labeled_results = _with_outcome_labels(
                engine,
                results,
                available_at=outcome_available_at,
            )
            baseline_results = _baseline_validation_results(
                engine,
                run_id=run_id,
                rows=labeled_results,
                as_of_start=as_of_start,
                as_of_end=as_of_end,
                available_at=available_at,
            )
            all_results = [*labeled_results, *baseline_results]
            count = validation_repo.upsert_validation_results(all_results)
            report = build_validation_report(
                run_id,
                all_results,
                useful_alert_labels=validation_repo.list_useful_alert_labels(
                    available_at=outcome_available_at,
                ),
            )
            metrics = validation_report_payload(report)
            validation_repo.finish_validation_run(
                run_id,
                ValidationRunStatus.SUCCESS,
                metrics,
            )
        except Exception as exc:
            validation_repo.finish_validation_run(
                run_id,
                ValidationRunStatus.FAILED,
                {"error": str(exc)},
            )
            print(f"validation replay failed: {exc}", file=sys.stderr)
            return 1
        print(
            f"validation_replay run_id={run_id} candidate_results={len(labeled_results)} "
            f"baseline_results={len(baseline_results)} results={count} "
            f"decision_available_at={available_at.isoformat()} "
            f"outcome_available_at={outcome_available_at.isoformat()} "
            f"leakage_failures={metrics['leakage_failure_count']} "
            f"precision_target_20d_25={metrics['precision'].get('target_20d_25', 0.0):.2f}"
        )
        return 0

    if args.command == "validation-report":
        create_schema(engine)
        validation_repo = ValidationRepository(engine)
        results = validation_repo.list_validation_results(
            args.run_id,
            available_at=args.available_at,
        )
        if not results:
            print(f"validation results not found: {args.run_id}", file=sys.stderr)
            return 1
        report = build_validation_report(
            args.run_id,
            results,
            useful_alert_labels=validation_repo.list_useful_alert_labels(
                available_at=args.available_at,
            ),
        )
        payload = validation_report_payload(report)
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            print(
                f"validation_report run_id={args.run_id} "
                f"candidates={payload['candidate_count']} "
                f"useful_alert_rate={payload['useful_alert_rate']:.2f} "
                f"precision_target_20d_25={payload['precision'].get('target_20d_25', 0.0):.2f} "
                f"false_positives={payload['false_positive_count']} "
                f"missed_opportunities={payload['missed_opportunity_count']} "
                f"leakage_failures={payload['leakage_failure_count']}"
            )
        return 0

    if args.command == "paper-decision":
        create_schema(engine)
        validation_repo = ValidationRepository(engine)
        card = validation_repo.decision_card_payload(
            args.decision_card_id,
            available_at=args.available_at,
        )
        if card is None:
            print(f"decision card not found: {args.decision_card_id}", file=sys.stderr)
            return 1
        hard_blocks = _card_hard_blocks(card)
        override_reason = _optional_cli_text(args.override_reason)
        if args.decision == PaperDecision.APPROVED.value and hard_blocks and not override_reason:
            print("--override-reason is required to approve a blocked card", file=sys.stderr)
            return 1
        entry_at = args.entry_at or (args.available_at if args.entry_price is not None else None)
        trade = create_paper_trade_from_card(
            card,
            PaperDecision(args.decision),
            available_at=args.available_at,
            entry_price=args.entry_price,
            entry_at=entry_at,
        )
        validation_repo.upsert_paper_trade(trade)
        _append_paper_decision_audit_events(
            engine,
            card=card,
            trade=trade,
            decision=PaperDecision(args.decision),
            hard_blocks=hard_blocks,
            override_reason=override_reason,
            occurred_at=args.available_at,
        )
        print(
            f"paper_trade id={trade.id} decision_card_id={trade.decision_card_id} "
            f"ticker={trade.ticker} decision={trade.decision.value} "
            f"state={trade.state.value} no_execution=true"
        )
        return 0

    if args.command == "paper-update-outcomes":
        create_schema(engine)
        validation_repo = ValidationRepository(engine)
        trade = validation_repo.latest_paper_trade_for_card(
            args.decision_card_id,
            args.available_at,
        )
        if trade is None:
            print(f"paper trade not found: {args.decision_card_id}", file=sys.stderr)
            return 1
        if args.labels_json is not None:
            labels = _read_json_object(args.labels_json)
        else:
            if trade.entry_price is None:
                print(
                    f"paper trade has no entry price: {args.decision_card_id}",
                    file=sys.stderr,
                )
                return 1
            future_prices = _future_price_rows(
                engine,
                ticker=trade.ticker,
                after=(trade.entry_at or trade.as_of).date(),
                available_at=args.available_at,
            )
            if not future_prices:
                print(
                    f"future prices not found for paper trade: {args.decision_card_id}",
                    file=sys.stderr,
                )
                return 1
            labels = outcome_labels_as_dict(
                compute_forward_outcomes(
                    trade.entry_price,
                    future_prices,
                    invalidation_price=trade.invalidation_price,
                )
            )
        updated = update_trade_outcome(trade, labels, args.available_at)
        validation_repo.upsert_paper_trade(updated)
        AuditLogRepository(engine).append_event(
            event_type="paper_outcome_updated",
            actor_source="cli",
            artifact_type="paper_trade",
            artifact_id=updated.id,
            ticker=updated.ticker,
            decision_card_id=updated.decision_card_id,
            paper_trade_id=updated.id,
            decision=updated.decision.value,
            status="success",
            metadata={
                "label_source": "labels_json" if args.labels_json else "computed",
                "state": updated.state.value,
            },
            after_payload={"outcome_labels": thaw_json_value(updated.outcome_labels)},
            available_at=updated.available_at,
            occurred_at=args.available_at,
        )
        print(
            f"paper_trade id={updated.id} decision_card_id={updated.decision_card_id} "
            f"ticker={updated.ticker} state={updated.state.value} "
            f"labels={json.dumps(thaw_json_value(updated.outcome_labels), sort_keys=True)}"
        )
        return 0

    if args.command == "useful-label":
        create_schema(engine)
        try:
            result = record_feedback(
                engine,
                artifact_type=args.artifact_type,
                artifact_id=args.artifact_id,
                ticker=args.ticker,
                label=args.label,
                notes=args.notes,
                source="cli",
                created_at=args.created_at or datetime.now(UTC),
            )
        except MissingArtifactError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        except FeedbackError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        label = result.useful_label
        print(
            f"useful_label artifact_type={label.artifact_type} "
            f"artifact_id={label.artifact_id} ticker={label.ticker} label={label.label}"
        )
        return 0

    if args.command == "build-universe":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        builder = UniverseBuilder(
            market_repo=market_repo,
            provider_repo=provider_repo,
            config=UniverseFilterConfig(
                min_price=config.universe_min_price,
                min_avg_dollar_volume=config.universe_min_avg_dollar_volume,
                require_sector=config.universe_require_sector,
                include_etfs=config.universe_include_etfs,
                include_adrs=config.universe_include_adrs,
            ),
            name=args.name or config.universe_name,
            provider=(
                args.provider or config.daily_market_provider or config.market_provider
            ),
        )
        snapshot = builder.build(as_of=args.as_of, available_at=available_at)
        print(
            f"built universe={snapshot.name} members={snapshot.member_count} "
            f"excluded={snapshot.excluded_count}"
        )
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


def _ingest_csv_provider(
    *,
    connector: CsvMarketDataConnector,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    securities_path: Path,
    daily_bars_path: Path,
    holdings_path: Path | None,
) -> int:
    metadata = {
        "securities": str(securities_path),
        "daily_bars": str(daily_bars_path),
        "holdings": str(holdings_path) if holdings_path is not None else None,
    }
    request = ConnectorRequest(
        provider=connector.provider,
        endpoint="csv_ingest",
        params=metadata,
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="csv_ingest",
            metadata=metadata,
        )
    except ProviderIngestError as exc:
        print(f"csv ingest failed: {exc}", file=sys.stderr)
        return 1

    message = (
        f"ingested securities={result.security_count} daily_bars={result.daily_bar_count}"
    )
    if holdings_path is not None:
        message = f"{message} holdings={result.holding_count}"
    print(message)
    return 0


def _ingest_polygon_provider(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    polygon_command: str,
    date_value: date | None,
    fixture_path: Path | None,
    max_pages: int | None,
    confirm_external_call: bool,
) -> int:
    try:
        connector, request, metadata, job_type = _build_polygon_ingest(
            config=config,
            polygon_command=polygon_command,
            date_value=date_value,
            fixture_path=fixture_path,
            max_pages=max_pages,
        )
        if (
            fixture_path is None
            and config.polygon_api_key_configured
            and not confirm_external_call
        ):
            print(
                "polygon ingest requires --confirm-external-call "
                "for live provider requests",
                file=sys.stderr,
            )
            return 2
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type=job_type,
            metadata=metadata,
        )
    except (ProviderIngestError, ValueError) as exc:
        print(f"polygon ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_sec_provider(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    sec_command: str,
    ticker: str,
    cik: str,
    fixture_path: Path | None,
    document_fixture_path: Path | None,
) -> int:
    try:
        result = ingest_sec_record(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            sec_command=sec_command,
            ticker=ticker,
            cik=cik,
            fixture_path=fixture_path,
            document_fixture_path=document_fixture_path,
        )
    except (ProviderIngestError, ValueError) as exc:
        print(f"sec ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _refresh_sec_cik_metadata_cli(
    *,
    engine: Engine,
    config: AppConfig,
    fixture_path: Path | None,
) -> int:
    try:
        result = refresh_sec_cik_metadata(
            engine,
            config,
            fixture_path=fixture_path,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"sec cik metadata refresh failed: {exc}", file=sys.stderr)
        return 1
    payload = result.as_payload()
    print(
        "refreshed_sec_cik_metadata "
        f"provider=sec "
        f"live={payload['live']} "
        f"active={payload['active_security_count']} "
        f"missing_before={payload['missing_before_count']} "
        f"matched={payload['matched_missing_count']} "
        f"updated={payload['updated_count']} "
        f"missing_after={payload['missing_after_count']} "
        f"external_calls={payload['external_calls_made']}"
    )
    updated = payload.get("updated_tickers")
    if isinstance(updated, list | tuple) and updated:
        print(f"updated_examples={','.join(str(ticker) for ticker in updated)}")
    unmatched = payload.get("unmatched_tickers")
    if isinstance(unmatched, list | tuple) and unmatched:
        print(f"unmatched_examples={','.join(str(ticker) for ticker in unmatched)}")
    print(f"next_action={payload['next_action']}")
    return 0


def _apply_sec_cik_overrides_cli(
    *,
    engine: Engine,
    csv_path: Path,
    validate_only: bool = False,
) -> int:
    if validate_only:
        try:
            result = validate_sec_cik_overrides_csv(engine, csv_path)
        except (OSError, ValueError) as exc:
            print(f"sec cik override validation failed: {exc}", file=sys.stderr)
            return 1
        payload = result.as_payload()
        print(
            "validated_sec_cik_overrides "
            f"provider=manual "
            f"status={payload['status']} "
            f"live={payload['live']} "
            f"requested={payload['requested_count']} "
            f"valid={payload['valid_count']} "
            f"updates={payload['update_candidate_count']} "
            f"skipped={payload['skipped_count']} "
            f"unmatched={payload['unmatched_count']} "
            f"invalid={payload['invalid_count']} "
            f"duplicates={payload['duplicate_count']} "
            f"external_calls={payload['external_calls_made']}"
        )
        _print_sec_cik_override_validation_examples(payload)
        print(f"import_command={payload['import_command']}")
        print(f"next_action={payload['next_action']}")
        return 0 if payload["status"] in {"ready", "noop"} else 1
    try:
        result = apply_sec_cik_overrides_csv(engine, csv_path)
    except (OSError, ValueError) as exc:
        print(f"sec cik override import failed: {exc}", file=sys.stderr)
        return 1
    payload = result.as_payload()
    print(
        "imported_sec_cik_overrides "
        f"provider=manual "
        f"live={payload['live']} "
        f"requested={payload['requested_count']} "
        f"updated={payload['updated_count']} "
        f"skipped={payload['skipped_count']} "
        f"unmatched={payload['unmatched_count']} "
        f"invalid={payload['invalid_count']} "
        f"external_calls={payload['external_calls_made']}"
    )
    updated = payload.get("updated_tickers")
    if isinstance(updated, list | tuple) and updated:
        print(f"updated_examples={','.join(str(ticker) for ticker in updated)}")
    skipped = payload.get("skipped_tickers")
    if isinstance(skipped, list | tuple) and skipped:
        print(f"skipped_examples={','.join(str(ticker) for ticker in skipped)}")
    unmatched = payload.get("unmatched_tickers")
    if isinstance(unmatched, list | tuple) and unmatched:
        print(f"unmatched_examples={','.join(str(ticker) for ticker in unmatched)}")
    invalid = payload.get("invalid_rows")
    if isinstance(invalid, list | tuple) and invalid:
        print(f"invalid_rows={','.join(str(row) for row in invalid)}")
    print(f"next_action={payload['next_action']}")
    return 1 if payload["invalid_count"] else 0


def _print_sec_cik_override_validation_examples(payload: Mapping[str, object]) -> None:
    examples = (
        ("update_candidate_tickers", "update_examples"),
        ("skipped_tickers", "skipped_examples"),
        ("unmatched_tickers", "unmatched_examples"),
        ("duplicate_tickers", "duplicate_examples"),
        ("invalid_rows", "invalid_rows"),
    )
    for key, label in examples:
        rows = payload.get(key)
        if isinstance(rows, list | tuple) and rows:
            print(f"{label}={','.join(str(row) for row in rows)}")


def _write_sec_cik_override_template_cli(
    *,
    engine: Engine,
    config: AppConfig,
    output_path: Path,
    stocks_only: bool,
    as_json: bool,
) -> int:
    payload = sec_cik_override_template_payload(
        engine,
        config,
        stocks_only=stocks_only,
    )
    try:
        write_result = write_sec_cik_override_template_csv(
            output_path,
            _sequence_value(payload.get("rows")),
        )
    except OSError as exc:
        print(f"sec cik override template failed: {exc}", file=sys.stderr)
        return 1
    write_payload = write_result.as_payload()
    payload = {
        **payload,
        "output_path": write_payload["output_path"],
        "write_schema_version": write_payload["schema_version"],
        "generated_at": write_payload["generated_at"],
        "import_command": write_payload["import_command"],
        "validate_command": write_payload["validate_command"],
    }
    if as_json:
        print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        return 0
    print(
        "sec_cik_override_template "
        f"status={payload.get('status')} "
        f"source={payload.get('source')} "
        f"stocks_only={str(bool(payload.get('stocks_only'))).lower()} "
        f"source_gap_rows={payload.get('source_gap_rows')} "
        f"rows={payload.get('row_count')} "
        f"output={payload.get('output_path')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    samples = payload.get("sample_tickers")
    if isinstance(samples, list | tuple) and samples:
        print(f"missing_cik_examples={','.join(str(ticker) for ticker in samples)}")
    routed_count = _int_value(payload.get("routed_non_company_count"))
    if routed_count:
        routed_samples = payload.get("sample_routed_non_company_tickers")
        routed_text = (
            ",".join(str(ticker) for ticker in routed_samples)
            if isinstance(routed_samples, list | tuple)
            else ""
        )
        print(f"routed_non_company={routed_count} examples={routed_text}")
    print(f"columns={','.join(str(column) for column in payload.get('columns', []))}")
    print(f"validate_command={_compact_cli_text(payload.get('validate_command'))}")
    print(f"import_command={_compact_cli_text(payload.get('import_command'))}")
    print(f"api={_compact_cli_text(payload.get('api'))}")
    print(f"boundary={_compact_cli_text(payload.get('boundary'))}")
    print(f"next_action={_compact_cli_text(payload.get('next_action'))}")
    return 0


def _write_options_fixture_template_cli(
    *,
    engine: Engine,
    config: AppConfig,
    output_path: Path,
    stocks_only: bool,
    as_json: bool,
) -> int:
    payload = options_fixture_template_payload(
        engine,
        config,
        stocks_only=stocks_only,
    )
    try:
        write_result = write_options_fixture_template_json(
            output_path,
            _mapping_value(payload.get("fixture")),
        )
    except (OSError, ValueError) as exc:
        print(f"options fixture template failed: {exc}", file=sys.stderr)
        return 1
    write_payload = write_result.as_payload()
    payload = {
        **payload,
        "output_path": write_payload["output_path"],
        "write_schema_version": write_payload["schema_version"],
        "generated_at": write_payload["generated_at"],
        "import_command": write_payload["import_command"],
    }
    if as_json:
        print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        return 0
    print(
        "options_fixture_template "
        f"status={payload.get('status')} "
        f"source={payload.get('source')} "
        f"stocks_only={str(bool(payload.get('stocks_only'))).lower()} "
        f"target_as_of={payload.get('target_as_of')} "
        f"source_gap_rows={payload.get('source_gap_rows')} "
        f"rows={payload.get('row_count')} "
        f"output={payload.get('output_path')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    samples = payload.get("sample_tickers")
    if isinstance(samples, list | tuple) and samples:
        print(f"template_examples={','.join(str(ticker) for ticker in samples)}")
    print(f"columns={','.join(str(column) for column in payload.get('columns', []))}")
    print(f"import_command={_compact_cli_text(payload.get('import_command'))}")
    print(f"api={_compact_cli_text(payload.get('api'))}")
    print(f"boundary={_compact_cli_text(payload.get('boundary'))}")
    print(f"next_action={_compact_cli_text(payload.get('next_action'))}")
    return 0


def _validate_options_fixture_cli(
    *,
    fixture_path: Path,
    expected_as_of: date | None,
    as_json: bool,
) -> int:
    result = validate_options_fixture_json(
        fixture_path,
        expected_as_of=expected_as_of,
    )
    payload = result.as_payload()
    if as_json:
        print(json.dumps(payload, default=dashboard_json_default, sort_keys=True))
        return 0 if payload["status"] == "ready" else 1
    print(
        "options_fixture_validation "
        f"status={payload.get('status')} "
        f"rows={payload.get('row_count')} "
        f"valid={payload.get('valid_row_count')} "
        f"invalid={payload.get('invalid_row_count')} "
        f"blank_required={payload.get('blank_required_count')} "
        f"invalid_numeric={payload.get('invalid_numeric_count')} "
        f"missing_fields={payload.get('missing_field_count')} "
        f"duplicates={payload.get('duplicate_ticker_count')} "
        f"as_of={payload.get('as_of')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    errors = payload.get("errors")
    if isinstance(errors, list | tuple):
        for error in errors:
            print(f"error={_compact_cli_text(error)}")
    import_command = payload.get("import_command")
    if import_command:
        print(f"import_command={_compact_cli_text(import_command)}")
    print(f"next_action={_compact_cli_text(payload.get('next_action'))}")
    return 0 if payload["status"] == "ready" else 1


def _ingest_sec_submissions_batch(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    targets: Sequence[str],
    fixture_path: Path | None,
) -> int:
    try:
        parsed_targets = [parse_sec_submission_target(target) for target in targets]
    except ValueError as exc:
        print(f"sec batch ingest failed: {exc}", file=sys.stderr)
        return 1
    try:
        result = ingest_sec_submissions_batch(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            targets=parsed_targets,
            fixture_path=fixture_path,
        )
    except (ProviderIngestError, ValueError) as exc:
        print(f"sec batch ingest failed: {exc}", file=sys.stderr)
        return 1
    payload = result.as_payload()

    print(
        "ingested_batch provider=sec "
        f"targets={payload['target_count']} "
        f"raw={payload['raw_count']} "
        f"normalized={payload['normalized_count']} "
        f"securities={payload['security_count']} "
        f"daily_bars={payload['daily_bar_count']} "
        f"holdings={payload['holding_count']} "
        f"events={payload['event_count']} "
        f"rejected={payload['rejected_count']}"
    )
    return 0


def _ingest_news_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    fixture_path: Path,
) -> int:
    connector = NewsJsonConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "news_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="news_fixture",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"news ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_earnings_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    fixture_path: Path,
) -> int:
    connector = EarningsCalendarConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "earnings_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="earnings_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="earnings_fixture",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"earnings ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
    return 0


def _ingest_options_provider(
    *,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    feature_repo: FeatureRepository,
    fixture_path: Path,
) -> int:
    connector = OptionsAggregateConnector(fixture_path=fixture_path)
    metadata = {
        "provider": "options_fixture",
        "endpoint": "fixture",
        "fixture": str(fixture_path),
    }
    request = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="options_fixture",
            metadata=metadata,
            feature_repo=feature_repo,
        )
    except ProviderIngestError as exc:
        print(f"options ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_options_provider_result(result)
    return 0


def _ingest_schwab_market_options(
    *,
    broker_repo: BrokerRepository,
    feature_repo: FeatureRepository,
    tickers: Sequence[str],
) -> int:
    requested = sorted(
        {
            ticker.strip().upper()
            for raw in tickers
            for ticker in str(raw).split(",")
            if ticker.strip()
        }
    )
    snapshots = broker_repo.latest_market_snapshots(tickers=requested or None)
    count = upsert_schwab_option_features(
        feature_repo=feature_repo,
        snapshots=snapshots,
    )
    print(
        "ingested "
        "provider=schwab_option_chain "
        f"raw={len(snapshots)} "
        f"normalized={count} "
        f"option_features={count} "
        f"rejected={max(0, len(snapshots) - count)}"
    )
    return 0


def _sync_schwab_market_context_cli(args: argparse.Namespace) -> int:
    tickers = sorted(
        {
            ticker.strip().upper()
            for raw in args.ticker
            for ticker in str(raw).split(",")
            if ticker.strip()
        }
    )
    payload = {
        "tickers": tickers,
        "include_history": not bool(args.skip_history),
        "include_options": not bool(args.skip_options),
    }
    try:
        from fastapi import HTTPException

        from catalyst_radar.api.routes.brokers import schwab_market_sync

        result = schwab_market_sync(payload)
    except HTTPException as exc:
        detail = exc.detail
        if args.json:
            print(
                json.dumps(
                    {
                        "status": "failed",
                        "status_code": exc.status_code,
                        "detail": detail,
                    },
                    sort_keys=True,
                )
            )
        else:
            print(
                f"schwab_market_sync failed status={exc.status_code} detail={detail}",
                file=sys.stderr,
            )
        return 1
    if args.json:
        print(json.dumps(result, sort_keys=True))
        return 0
    items = result.get("items") if isinstance(result, Mapping) else []
    option_count = result.get("option_features_upserted") if isinstance(result, Mapping) else 0
    print(
        "schwab_market_sync "
        f"tickers={len(tickers)} "
        f"snapshots={len(items) if isinstance(items, list | tuple) else 0} "
        f"option_features={option_count} "
        f"include_history={str(payload['include_history']).lower()} "
        f"include_options={str(payload['include_options']).lower()} "
        "boundary=explicit_read_only_rate_limited"
    )
    if isinstance(items, list | tuple):
        for item in items:
            if not isinstance(item, Mapping):
                continue
            print(
                f"- {item.get('ticker')} "
                f"last={item.get('last_price')} "
                f"trend_5d={item.get('price_trend_5d_percent')} "
                f"call_put={item.get('option_call_put_ratio')} "
                f"iv={item.get('option_iv_percentile')}"
            )
    return 0


def _read_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"expected JSON object: {path}"
        raise ValueError(msg)
    return payload


def _stable_alerts(alerts: list[object]) -> list[object]:
    return sorted(
        alerts,
        key=lambda alert: (
            alert.ticker,
            getattr(alert.route, "value", alert.route),
            alert.available_at.isoformat(),
            alert.id,
        ),
    )


def _alert_plan_payload(result: object, available_at: datetime) -> dict[str, object]:
    alerts = _stable_alerts(list(result.alerts))
    suppressions = sorted(
        result.suppressions,
        key=lambda suppression: (
            suppression.ticker,
            suppression.route.value,
            suppression.dedupe_key,
            suppression.id,
        ),
    )
    return {
        "alert_count": len(alerts),
        "alerts": [_alert_cli_payload(alert) for alert in alerts],
        "available_at": available_at.isoformat(),
        "suppression_count": len(suppressions),
        "suppressions": [
            {
                "dedupe_key": suppression.dedupe_key,
                "id": suppression.id,
                "reason": suppression.reason,
                "route": suppression.route.value,
                "ticker": suppression.ticker,
            }
            for suppression in suppressions
        ],
    }


def _alert_cli_payload(alert: object) -> dict[str, object]:
    return {
        "action_state": alert.action_state,
        "as_of": alert.as_of.isoformat(),
        "available_at": alert.available_at.isoformat(),
        "candidate_packet_id": alert.candidate_packet_id,
        "candidate_state_id": alert.candidate_state_id,
        "channel": alert.channel.value,
        "decision_card_id": alert.decision_card_id,
        "dedupe_key": alert.dedupe_key,
        "id": alert.id,
        "priority": alert.priority.value,
        "route": alert.route.value,
        "status": alert.status.value,
        "ticker": alert.ticker,
        "title": alert.title,
        "trigger_fingerprint": alert.trigger_fingerprint,
        "trigger_kind": alert.trigger_kind,
    }


def _scheduler_exit_code(result: SchedulerRunResult) -> int:
    if result.reason == "lock_held":
        return 0
    if result.reason is not None:
        return 1
    if result.daily_result is None:
        return 0
    return 0 if result.daily_result.status == "success" else 1


def _future_price_rows(
    engine: Engine,
    *,
    ticker: str,
    after: date,
    available_at: datetime,
) -> list[dict[str, float]]:
    stmt = (
        select(daily_bars)
        .where(
            daily_bars.c.ticker == ticker.upper(),
            daily_bars.c.date > after,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.date)
        .limit(60)
    )
    with engine.connect() as conn:
        return [
            {
                "close": row.close,
                "high": row.high,
                "low": row.low,
            }
            for row in conn.execute(stmt)
        ]


def _future_price_rows_for_validation(
    engine: Engine,
    *,
    ticker: str,
    after: date,
    available_at: datetime,
) -> list[dict[str, float]]:
    stmt = (
        select(daily_bars)
        .where(
            daily_bars.c.ticker == ticker.upper(),
            daily_bars.c.date > after,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.date)
        .limit(60)
    )
    with engine.connect() as conn:
        return [
            {
                "close": row.close,
                "high": row.high,
                "low": row.low,
            }
            for row in conn.execute(stmt)
        ]


def _historical_close_on_or_before(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    available_at: datetime,
) -> float | None:
    stmt = (
        select(daily_bars.c.close)
        .where(
            daily_bars.c.ticker == ticker.upper(),
            daily_bars.c.date <= as_of,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.date.desc())
        .limit(1)
    )
    with engine.connect() as conn:
        value = conn.execute(stmt).scalar_one_or_none()
    return _float_or_none(value)


def _with_outcome_labels(
    engine: Engine,
    rows: list[ValidationResult],
    *,
    available_at: datetime,
) -> list[ValidationResult]:
    labeled = []
    for row in rows:
        entry_price = _entry_price_for_validation_result(
            engine,
            row,
            available_at=available_at,
        )
        if entry_price is None:
            labeled.append(row)
            continue
        future_prices = _future_price_rows_for_validation(
            engine,
            ticker=row.ticker,
            after=row.as_of.date(),
            available_at=available_at,
        )
        if not future_prices:
            labeled.append(row)
            continue
        sector_prices = _future_price_rows_for_validation(
            engine,
            ticker="SPY",
            after=row.as_of.date(),
            available_at=available_at,
        )
        labels = outcome_labels_as_dict(
            compute_forward_outcomes(
                entry_price,
                future_prices,
                sector_future_prices=sector_prices,
                invalidation_price=_invalidation_price_for_validation_result(row),
            )
        )
        payload = thaw_json_value(row.payload)
        if not isinstance(payload, dict):
            payload = {}
        payload["outcome_audit"] = {
            "entry_price": entry_price,
            "future_price_count": len(future_prices),
            "outcome_available_at": available_at.isoformat(),
            "sector_proxy": "SPY" if sector_prices else None,
            "label_only_not_candidate_input": True,
        }
        labeled.append(replace(row, available_at=available_at, labels=labels, payload=payload))
    return labeled


def _entry_price_for_validation_result(
    engine: Engine,
    row: ValidationResult,
    *,
    available_at: datetime,
) -> float | None:
    payload = thaw_json_value(row.payload)
    if isinstance(payload, Mapping):
        replay_payload = _mapping_value(payload.get("payload"))
        for path in (
            ("decision_card", "trade_plan", "entry_zone"),
            ("packet", "trade_plan", "entry_zone"),
            ("signal_payload", "candidate", "entry_zone"),
        ):
            value = _nested_mapping_value(replay_payload, *path)
            price = _entry_zone_price(value)
            if price is not None:
                return price
    return _historical_close_on_or_before(
        engine,
        ticker=row.ticker,
        as_of=row.as_of.date(),
        available_at=available_at,
    )


def _invalidation_price_for_validation_result(row: ValidationResult) -> float | None:
    payload = thaw_json_value(row.payload)
    if not isinstance(payload, Mapping):
        return None
    replay_payload = _mapping_value(payload.get("payload"))
    for path in (
        ("decision_card", "trade_plan", "invalidation_price"),
        ("packet", "trade_plan", "invalidation_price"),
        ("signal_payload", "candidate", "invalidation_price"),
    ):
        value = _nested_mapping_value(replay_payload, *path)
        price = _float_or_none(value)
        if price is not None:
            return price
    return None


def _baseline_validation_results(
    engine: Engine,
    *,
    run_id: str,
    rows: list[ValidationResult],
    as_of_start: datetime,
    as_of_end: datetime,
    available_at: datetime,
) -> list[ValidationResult]:
    result_rows = []
    for as_of in _daily_replay_datetimes(as_of_start, as_of_end):
        rows_for_day = [row for row in rows if row.as_of.date() == as_of.date()]
        baseline_rows = _daily_bar_baseline_rows(
            engine,
            rows_for_day,
            as_of=as_of,
            available_at=available_at,
        )
        candidates = [
            *spy_relative_momentum(baseline_rows, limit=10),
            *sector_relative_momentum(baseline_rows, limit=10),
            *event_only_watchlist(baseline_rows, limit=10),
            *random_eligible_universe(
                baseline_rows,
                seed=f"{run_id}:{as_of.isoformat()}",
                limit=10,
            ),
            *user_watchlist(baseline_rows, limit=10),
        ]
        for candidate in candidates:
            candidate_as_of = candidate.as_of if isinstance(candidate.as_of, datetime) else as_of
            state = ActionState.RESEARCH_ONLY
            result_rows.append(
                ValidationResult(
                    id=validation_result_id(
                        run_id=run_id,
                        ticker=candidate.ticker,
                        as_of=candidate_as_of,
                        state=state,
                        baseline=candidate.baseline,
                    ),
                    run_id=run_id,
                    ticker=candidate.ticker,
                    as_of=candidate_as_of,
                    available_at=available_at,
                    state=state,
                    final_score=candidate.score,
                    baseline=candidate.baseline,
                    labels={},
                    leakage_flags=(),
                    payload=candidate.as_dict(),
                )
            )
    return result_rows


def _daily_replay_datetimes(start: datetime, end: datetime) -> list[datetime]:
    values = []
    current = start
    while current <= end:
        values.append(current)
        current += timedelta(days=1)
    return values


def _daily_bar_baseline_rows(
    engine: Engine,
    rows: list[ValidationResult],
    *,
    as_of: datetime,
    available_at: datetime,
) -> list[dict[str, object]]:
    replay_rows = [_baseline_row_from_result(row) for row in rows if row.baseline is None]
    replay_by_ticker = {str(row["ticker"]): row for row in replay_rows}
    bar_rows = _historical_bar_baseline_rows(
        engine,
        as_of=as_of.date(),
        available_at=available_at,
    )
    bar_by_ticker = {str(row["ticker"]): row for row in bar_rows}
    tickers = set(bar_by_ticker)
    tickers.update(replay_by_ticker)
    if not tickers:
        return []
    result = []
    for ticker in sorted(tickers):
        row = dict(bar_by_ticker.get(ticker, {"ticker": ticker, "as_of": as_of}))
        if ticker in replay_by_ticker:
            overlay = replay_by_ticker[ticker]
            payload = {
                **_mapping_value(row.get("payload")),
                **_mapping_value(overlay.get("payload")),
            }
            row.update(overlay)
            row["payload"] = payload
        row.setdefault("eligible", True)
        result.append(row)
    return result


def _historical_bar_baseline_rows(
    engine: Engine,
    *,
    as_of: date,
    available_at: datetime,
) -> list[dict[str, object]]:
    stmt = (
        select(
            daily_bars.c.ticker,
            daily_bars.c.date,
            daily_bars.c.close,
        )
        .where(
            daily_bars.c.date <= as_of,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.ticker, daily_bars.c.date.desc())
    )
    by_ticker: dict[str, list[tuple[date, float]]] = {}
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            close = _float_or_none(row.close)
            if close is None or close <= 0:
                continue
            ticker = str(row.ticker).upper()
            by_ticker.setdefault(ticker, []).append((row.date, close))

    spy_bars = by_ticker.get("SPY", [])
    spy_return_20d = _window_return(spy_bars, lookback=20)
    spy_return_60d = _window_return(spy_bars, lookback=60)
    result = []
    for ticker, bars in sorted(by_ticker.items()):
        if ticker == "SPY":
            continue
        ret_20d = _window_return(bars, lookback=20)
        ret_60d = _window_return(bars, lookback=60)
        if ret_20d is None and ret_60d is None:
            continue
        result.append(
            {
                "ticker": ticker,
                "as_of": datetime.combine(as_of, time(21), tzinfo=UTC),
                "eligible": True,
                "ret_20d": ret_20d,
                "ret_60d": ret_60d,
                "spy_return_20d": spy_return_20d,
                "spy_return_60d": spy_return_60d,
                "sector_relative_score": ret_20d,
                "payload": {
                    "baseline_source": "daily_bars",
                    "bar_count": len(bars),
                },
            }
        )
    return result


def _window_return(
    bars_desc: list[tuple[date, float]],
    *,
    lookback: int,
) -> float | None:
    if len(bars_desc) < 2:
        return None
    window = bars_desc[:lookback]
    latest = window[0][1]
    earliest = window[-1][1]
    if earliest <= 0:
        return None
    return (latest / earliest) - 1


def _baseline_row_from_result(row: ValidationResult) -> dict[str, object]:
    payload = thaw_json_value(row.payload)
    replay_payload = _mapping_value(payload.get("payload")) if isinstance(payload, Mapping) else {}
    signal_payload = _mapping_value(replay_payload.get("signal_payload"))
    candidate = _mapping_value(signal_payload.get("candidate"))
    metadata = _mapping_value(candidate.get("metadata"))
    features = _mapping_value(candidate.get("features"))
    packet = _mapping_value(replay_payload.get("packet"))
    packet_metadata = _mapping_value(packet.get("metadata"))
    merged = {
        **features,
        **metadata,
        **packet_metadata,
    }
    event_support = _float_or_none(
        merged.get("event_support_score") or merged.get("material_event_score")
    )
    material_event_count = _float_or_none(
        merged.get("material_event_count") or len(_sequence_value(merged.get("events")))
    )
    baseline_row: dict[str, object] = {
        "ticker": row.ticker,
        "as_of": row.as_of,
        "eligible": not row.leakage_flags,
        "hard_blocks": (),
        "leakage_flags": row.leakage_flags,
        "final_score": row.final_score,
        "payload": {
            "candidate": {
                "features": features,
                "metadata": metadata,
            },
            "metadata": merged,
            "events": _sequence_value(merged.get("events")),
        },
    }
    for name in (
        "spy_relative_return_20d",
        "spy_relative_return_60d",
        "relative_return_20d_spy",
        "relative_return_60d_spy",
        "ret_20d",
        "ret_60d",
        "spy_return_20d",
        "spy_return_60d",
        "sector_relative_score",
        "sector_momentum_score",
        "rs_20_sector",
    ):
        value = _float_or_none(merged.get(name))
        if value is not None:
            baseline_row[name] = value
    if event_support is not None:
        baseline_row["event_support_score"] = event_support
    if material_event_count is not None:
        baseline_row["material_event_count"] = material_event_count
    return baseline_row


def _mapping_value(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _nested_mapping_value(source: Mapping[str, object], *keys: str) -> object | None:
    value: object = source
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _sequence_value(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple):
        return list(value)
    return [value]


def _entry_zone_price(value: object) -> float | None:
    if isinstance(value, list | tuple) and value:
        return _float_or_none(value[0])
    return _float_or_none(value)


def _float_or_none(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _append_model_call_audit_event(
    engine: Engine,
    *,
    entry: BudgetLedgerEntry,
    actor_source: str,
    artifact_type: str,
    artifact_id: str | None,
) -> None:
    AuditLogRepository(engine).append_event(
        event_type="model_call_recorded",
        actor_source=actor_source,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        ticker=entry.ticker,
        candidate_state_id=entry.candidate_state_id,
        candidate_packet_id=entry.candidate_packet_id,
        budget_ledger_id=entry.id,
        status=entry.status.value,
        metadata={
            "task": entry.task.value,
            "provider": entry.provider,
            "model": entry.model,
            "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
            "prompt_version": entry.prompt_version,
            "schema_version": entry.schema_version,
        },
        available_at=entry.available_at,
        occurred_at=entry.created_at,
    )


def _append_paper_decision_audit_events(
    engine: Engine,
    *,
    card: Mapping[str, object],
    trade: PaperTrade,
    decision: PaperDecision,
    hard_blocks: Sequence[str],
    override_reason: str | None,
    occurred_at: datetime,
) -> None:
    repo = AuditLogRepository(engine)
    repo.append_event(
        event_type="paper_decision_recorded",
        actor_source="cli",
        artifact_type="decision_card",
        artifact_id=trade.decision_card_id,
        ticker=trade.ticker,
        decision_card_id=trade.decision_card_id,
        paper_trade_id=trade.id,
        decision=decision.value,
        reason=redact_text(override_reason) if override_reason is not None else None,
        hard_blocks=tuple(hard_blocks),
        status="success",
        metadata={
            "state": trade.state.value,
            "action_state": str(card.get("action_state") or ""),
            "manual_review_only": bool(trade.payload.get("manual_review_only")),
            "no_execution": bool(trade.payload.get("no_execution")),
        },
        after_payload={"paper_trade_id": trade.id, "state": trade.state.value},
        available_at=trade.available_at,
        occurred_at=occurred_at,
    )
    if decision == PaperDecision.APPROVED and hard_blocks:
        repo.append_event(
            event_type="hard_block_bypass_recorded",
            actor_source="cli",
            artifact_type="decision_card",
            artifact_id=trade.decision_card_id,
            ticker=trade.ticker,
            decision_card_id=trade.decision_card_id,
            paper_trade_id=trade.id,
            decision=decision.value,
            reason=redact_text(override_reason) if override_reason is not None else None,
            hard_blocks=tuple(hard_blocks),
            status="success",
            metadata={"state": trade.state.value},
            after_payload={"paper_trade_id": trade.id},
            available_at=trade.available_at,
            occurred_at=occurred_at,
        )


def _card_hard_blocks(card: Mapping[str, object]) -> tuple[str, ...]:
    payload = _mapping_value(card.get("payload"))
    controls = _mapping_value(payload.get("controls"))
    portfolio = _mapping_value(payload.get("portfolio_impact"))
    values = [
        *_sequence_value(payload.get("hard_blocks")),
        *_sequence_value(controls.get("hard_blocks")),
        *_sequence_value(portfolio.get("hard_blocks")),
    ]
    hard_blocks = tuple(dict.fromkeys(str(value) for value in values if str(value).strip()))
    if hard_blocks:
        return hard_blocks
    if str(card.get("action_state") or "") == ActionState.BLOCKED.value:
        return ("blocked_action_state",)
    return ()


def _optional_cli_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _llm_budget_status_payload(
    *,
    summary: dict[str, object],
    config: AppConfig,
    available_at: datetime,
) -> dict[str, object]:
    return {
        "available_at": available_at.isoformat(),
        "caps": {
            "enable_premium_llm": config.enable_premium_llm,
            "daily_budget_usd": config.llm_daily_budget_usd,
            "monthly_budget_usd": config.llm_monthly_budget_usd,
            "monthly_soft_cap_pct": config.llm_monthly_soft_cap_pct,
            "task_daily_caps": dict(sorted(config.llm_task_daily_caps.items())),
            "pricing_updated_at": config.llm_pricing_updated_at,
            "pricing_stale_after_days": config.llm_pricing_stale_after_days,
        },
        "source": "budget_ledger",
        "summary": summary,
    }


def _llm_review_payload(result) -> dict[str, object]:
    entry = result.ledger_entry
    return {
        "result": {
            "status": result.status.value,
            "error": redact_text(result.error) if result.error is not None else None,
            "payload": (
                redact_value(thaw_json_value(result.payload))
                if result.payload is not None
                else None
            ),
        },
        "route": {
            "skip": result.decision.skip,
            "reason": result.decision.reason.value if result.decision.reason else None,
            "task": result.decision.task.name.value,
            "model": result.decision.model,
            "estimated_cost_usd": result.decision.estimated_cost,
            "max_tokens": result.decision.max_tokens,
            "estimated_usage": {
                "input_tokens": result.decision.estimated_usage.input_tokens,
                "cached_input_tokens": result.decision.estimated_usage.cached_input_tokens,
                "output_tokens": result.decision.estimated_usage.output_tokens,
            },
        },
        "ledger": _llm_ledger_payload(entry),
    }


def _llm_ledger_payload(entry) -> dict[str, object]:
    return {
        "id": entry.id,
        "ts": entry.ts.isoformat(),
        "available_at": entry.available_at.isoformat(),
        "ticker": entry.ticker,
        "task": entry.task.value,
        "model": entry.model,
        "provider": entry.provider,
        "status": entry.status.value,
        "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
        "input_tokens": entry.token_usage.input_tokens,
        "cached_input_tokens": entry.token_usage.cached_input_tokens,
        "output_tokens": entry.token_usage.output_tokens,
        "estimated_cost_usd": entry.estimated_cost,
        "actual_cost_usd": entry.actual_cost,
        "currency": entry.currency,
        "candidate_state": entry.candidate_state,
        "candidate_state_id": entry.candidate_state_id,
        "candidate_packet_id": entry.candidate_packet_id,
        "prompt_version": entry.prompt_version,
        "schema_version": entry.schema_version,
        "outcome_label": entry.outcome_label,
        "payload": redact_value(thaw_json_value(entry.payload)),
    }


def _print_agent_brief(payload: Mapping[str, object]) -> None:
    print(
        "agent_brief "
        f"mode={payload.get('mode')} "
        f"status={payload.get('status')} "
        f"boundary={payload.get('decision_boundary')}"
    )
    calls = payload.get("external_calls_made")
    if isinstance(calls, Mapping):
        print(
            "external_calls "
            f"openai={calls.get('openai', 0)} "
            f"market_data={calls.get('market_data', 0)} "
            f"broker={calls.get('broker', 0)}"
        )
    print("agents:")
    for agent in payload.get("agents", []):
        if not isinstance(agent, Mapping):
            continue
        print(
            f"- {agent.get('agent')}: {agent.get('summary')} "
            f"(confidence={agent.get('confidence')})"
        )
    print("insights:")
    for insight in payload.get("insights", []):
        print(f"- {insight}")
    print("next_actions:")
    for action in payload.get("next_actions", []):
        print(f"- {action}")
    print("security:")
    for check in payload.get("security_checks", []):
        if not isinstance(check, Mapping):
            continue
        print(f"- {check.get('name')}: {check.get('status')} - {check.get('detail')}")


def _print_manual_market_bars_template(payload: Mapping[str, object]) -> None:
    print(
        "manual_market_bars_template "
        f"status={payload.get('status')} "
        f"rows={payload.get('row_count')} "
        f"scope={payload.get('template_scope')} "
        f"expected_as_of={payload.get('expected_as_of')} "
        f"path={payload.get('output_path')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(
        "coverage="
        f"active={payload.get('active_security_count')} "
        f"existing={payload.get('existing_as_of_bar_count')} "
        f"missing={payload.get('missing_as_of_bar_count')} "
        f"missing_only={str(bool(payload.get('missing_only'))).lower()}"
    )
    if payload.get("row_order"):
        print(f"row_order={payload.get('row_order')}")
    print(f"next_action={payload.get('next_action')}")
    print(f"import_command={payload.get('import_command')}")
    print(f"execute_command={payload.get('execute_command')}")


def _print_manual_market_bars_import(payload: Mapping[str, object]) -> None:
    print(
        "manual_market_bars_import "
        f"status={payload.get('status')} "
        f"rows={payload.get('row_count')} "
        f"tickers={payload.get('ticker_count')} "
        f"active={payload.get('active_security_count')} "
        f"latest_bar={payload.get('latest_bar_date') or 'n/a'} "
        f"expected_as_of={payload.get('expected_as_of') or 'n/a'} "
        f"executed={str(bool(payload.get('executed'))).lower()} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    if payload.get("bars_at_expected_as_of") is not None:
        print(
            "coverage="
            f"bars_at_expected={payload.get('bars_at_expected_as_of')} "
            f"existing={payload.get('existing_as_of_bar_count')} "
            f"after_import={payload.get('coverage_after_import_count')} "
            f"missing={payload.get('missing_expected_count')}"
        )
    missing = payload.get("missing_expected_tickers")
    if isinstance(missing, list | tuple) and missing:
        sample = ",".join(str(ticker) for ticker in missing)
        more = int(payload.get("missing_expected_more") or 0)
        suffix = f" plus {more} more" if more else ""
        print(f"missing_expected_tickers={sample}{suffix}")
    invalid_count = int(payload.get("invalid_row_count") or 0)
    if invalid_count:
        print(
            "invalid="
            f"rows={invalid_count} "
            f"blank_required={payload.get('blank_required_count')} "
            f"invalid_numeric={payload.get('invalid_numeric_count')}"
        )
        examples = payload.get("invalid_examples")
        if isinstance(examples, list | tuple) and examples:
            print("invalid_examples=" + " | ".join(str(item) for item in examples))
    if payload.get("status") == "ready":
        print("Plan only: no database writes were made.")
    print(f"next_action={payload.get('next_action')}")
    print(f"execute_command={payload.get('execute_command')}")


def _print_priced_in_queue(payload: Mapping[str, object]) -> None:
    filters = payload.get("filters")
    print(
        "priced_in_queue "
        f"status={payload.get('status')} "
        f"count={payload.get('count')} "
        f"total={payload.get('total_count')} "
        f"offset={payload.get('offset')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    scan = payload.get("scan")
    if isinstance(scan, Mapping):
        scan_total = 0
        for key in ("scanned_candidate_states", "candidate_states", "scanned_securities"):
            scan_total = _int_value(scan.get(key))
            if scan_total:
                break
        requested = _int_value(scan.get("requested_securities"))
        status_filter = (
            str(filters.get("status") or "all") if isinstance(filters, Mapping) else "all"
        )
        print(
            "scan_scope="
            f"scanned={scan_total or 'n/a'} "
            f"requested={requested or 'n/a'} "
            f"filter={status_filter} "
            f"ranked_after_filter={payload.get('total_count')} "
            f"visible_page={payload.get('count')}"
        )
    scan_selection = payload.get("scan_selection")
    if isinstance(scan_selection, Mapping):
        mode = str(scan_selection.get("mode") or "")
        if mode == "previous_useful_scan":
            print(
                "scan_selection="
                f"mode={mode} "
                f"latest_run_as_of={scan_selection.get('latest_run_as_of') or 'n/a'} "
                f"selected_as_of={scan_selection.get('selected_candidate_as_of') or 'n/a'} "
                f"reason={scan_selection.get('reason') or 'n/a'}"
            )
    print(f"headline={payload.get('headline')}")
    print(f"next_action={payload.get('next_action')}")
    usefulness_counts = payload.get("usefulness_counts")
    if isinstance(usefulness_counts, Mapping) and usefulness_counts:
        print(f"usefulness_counts={_count_summary(usefulness_counts)}")
    source_coverage = payload.get("source_coverage")
    if isinstance(source_coverage, Mapping):
        print(f"source_coverage={_compact_cli_text(source_coverage.get('summary'))}")
        weak_sources = source_coverage.get("weak_sources")
        if isinstance(weak_sources, list | tuple) and weak_sources:
            print(f"weak_sources={','.join(str(item) for item in weak_sources)}")
        actions = source_coverage.get("actions")
        if isinstance(actions, list | tuple) and actions:
            print("source_actions:")
            for action in actions:
                if not isinstance(action, Mapping):
                    continue
                if str(action.get("status") or "") == "ready":
                    continue
                sample = action.get("sample_tickers")
                sample_text = (
                    ",".join(str(item) for item in sample)
                    if isinstance(sample, list | tuple) and sample
                    else ""
                )
                sample_suffix = (
                    f" example_tickers={sample_text}" if sample_text else ""
                )
                print(
                    "- "
                    f"{action.get('source')} "
                    f"status={action.get('status')} "
                    f"coverage={action.get('coverage_pct')} "
                    f"gap_rows={_int_value(action.get('gap_count'))} "
                    f"next={_compact_cli_text(action.get('next_action'))} "
                    f"command={_compact_cli_text(action.get('command'))}"
                    f"{sample_suffix}"
                )
                sample_scope = action.get("sample_scope")
                if sample_scope:
                    print(f"  sample_scope={_compact_cli_text(sample_scope)}")
                full_scan_command = action.get("full_scan_gap_review_command")
                if full_scan_command:
                    print(
                        "  full_scan_review="
                        f"{_compact_cli_text(full_scan_command)}"
                    )
                full_scan_export = action.get("full_scan_export_command")
                if full_scan_export:
                    print(
                        "  full_scan_export="
                        f"{_compact_cli_text(full_scan_export)}"
                    )
                batch_plan_command = action.get("batch_plan_command")
                if batch_plan_command:
                    print(
                        "  batch_plan="
                        f"{_compact_cli_text(batch_plan_command)}"
                    )
                sample_command = action.get("sample_command")
                if sample_command:
                    print(
                        "  sample_command="
                        f"{_compact_cli_text(sample_command)}"
                    )
                diagnostic = action.get("diagnostic")
                if isinstance(diagnostic, Mapping) and diagnostic.get("evidence"):
                    print(
                        "  diagnostic="
                        f"{_compact_cli_text(diagnostic.get('evidence'))}"
                    )
    _print_priced_in_instrument_scope(payload.get("instrument_scope"))
    rows = payload.get("rows")
    if not isinstance(rows, list | tuple) or not rows:
        print("No priced-in rows.")
        return
    print("ticker status usefulness blocked direction gap emotion reaction priced score")
    print("data next_step")
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        usefulness = row.get("usefulness")
        if not isinstance(usefulness, Mapping):
            usefulness = {}
        next_command = usefulness.get("next_command")
        command_suffix = f" command={next_command}" if next_command else ""
        print(
            f"{row.get('ticker')} "
            f"{row.get('priced_in_status')} "
            f"{usefulness.get('status') or 'n/a'} "
            f"{str(bool(row.get('blocked'))).lower()} "
            f"{row.get('priced_in_direction') or 'n/a'} "
            f"{row.get('emotion_reaction_gap')} "
            f"{row.get('emotion_score')} "
            f"{row.get('reaction_score')} "
            f"{row.get('priced_in_score')} "
            f"{row.get('score')} "
            f"{_priced_in_data_summary(row)} "
            f"{row.get('next_step')}"
            f"{command_suffix}"
        )
        evidence = row.get("non_company_evidence")
        if isinstance(evidence, Mapping):
            print(
                "  non_company_evidence="
                f"status={evidence.get('status')} "
                f"route={evidence.get('route')} "
                f"summary={_compact_cli_text(evidence.get('summary'))}"
            )
    if payload.get("has_more"):
        filters = payload.get("filters")
        next_offset = _int_value(payload.get("offset")) + _int_value(payload.get("count"))
        limit = (
            _int_value(filters.get("limit"))
            if isinstance(filters, Mapping)
            else _int_value(payload.get("count"))
        )
        print(f"more={_priced_in_more_command(filters, limit, next_offset)}")


def _print_priced_in_all_source_batches(payload: Mapping[str, object]) -> None:
    print(
        "priced_in_source_batch_overview "
        f"status={payload.get('status')} "
        f"sources={payload.get('source_count')} "
        f"ready_sources={payload.get('ready_source_count')} "
        f"blocked_sources={payload.get('blocked_source_count')} "
        f"gap_rows={payload.get('total_gap_rows')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(f"headline={payload.get('headline')}")
    scan_scope = payload.get("scan_scope")
    if isinstance(scan_scope, Mapping):
        print(
            "full_scan="
            f"mode={scan_scope.get('mode')} "
            f"active={scan_scope.get('active_securities')} "
            f"scanned={scan_scope.get('scanned_rows')} "
            f"ranked={scan_scope.get('ranked_rows')} "
            f"stocks_only={str(bool(scan_scope.get('stocks_only'))).lower()} "
            f"source_gap_rows={scan_scope.get('source_gap_rows')} "
            f"examples_are_samples={str(bool(scan_scope.get('examples_are_samples'))).lower()}"
        )
        explanation = scan_scope.get("explanation")
        if explanation:
            print(f"scope_note={_compact_cli_text(explanation)}")
        review_command = scan_scope.get("review_full_scan_command")
        if review_command:
            print(f"full_scan_review={_compact_cli_text(review_command)}")
        export_command = scan_scope.get("export_full_scan_command")
        if export_command:
            print(f"full_scan_export={_compact_cli_text(export_command)}")
    alignment = payload.get("goal_alignment")
    if isinstance(alignment, Mapping):
        print(
            "goal_alignment="
            f"status={alignment.get('status')} "
            f"stocks_only={str(bool(alignment.get('stocks_only'))).lower()} "
            f"ranked={alignment.get('ranked_rows')} "
            f"source_gap_rows={alignment.get('source_gap_rows')} "
            f"useful={_compact_cli_text(alignment.get('useful_definition'))}"
        )
        print(f"  goal={_compact_cli_text(alignment.get('goal'))}")
        print(f"  current={_compact_cli_text(alignment.get('current_state'))}")
        print(f"  blocker={_compact_cli_text(alignment.get('current_blocker'))}")
        print(
            "  next_useful_step="
            f"{_compact_cli_text(alignment.get('next_useful_step'))}"
        )
        print(
            "  next_useful_command="
            f"{_compact_cli_text(alignment.get('next_command'))} "
            f"calls={_int_value(alignment.get('next_external_calls_required'))}"
        )
        print(f"  boundary={_compact_cli_text(alignment.get('provider_boundary'))}")
    print(f"next_action={payload.get('next_action')}")
    coverage = payload.get("coverage_first_recommendation")
    if isinstance(coverage, Mapping):
        print(
            "coverage_first="
            f"source={coverage.get('source') or 'n/a'} "
            f"gaps={coverage.get('total_gap_rows')} "
            f"calls={coverage.get('first_batch_external_calls')} "
            f"command={_compact_cli_text(coverage.get('command'))}"
        )
        print(f"  why={_compact_cli_text(coverage.get('rationale'))}")
        blocker_detail = coverage.get("blocker_detail")
        if blocker_detail:
            print(f"  blocker={_compact_cli_text(blocker_detail)}")
        _print_priced_in_recommendation_first_batch(
            payload,
            coverage,
            label="coverage_first",
        )
    decision = payload.get("decision_shortcut_recommendation")
    if isinstance(decision, Mapping):
        print(
            "decision_shortcut="
            f"source={decision.get('source') or 'n/a'} "
            f"decision={decision.get('decision_useful_gap_rows')} "
            f"actionable={decision.get('actionable_gap_rows')} "
            f"calls={decision.get('first_batch_external_calls')} "
            f"command={_compact_cli_text(decision.get('command'))}"
        )
        samples = decision.get("sample_tickers")
        if isinstance(samples, list | tuple) and samples:
            print(
                "  examples="
                f"{','.join(str(ticker) for ticker in samples if str(ticker).strip())}"
            )
        _print_priced_in_recommendation_first_batch(
            payload,
            decision,
            label="decision_shortcut",
        )
    boundary = payload.get("execution_boundary")
    if boundary:
        print(f"boundary={_compact_cli_text(boundary)}")
    rows = payload.get("sources")
    if not isinstance(rows, list | tuple) or not rows:
        return
    print(
        "source status gap_rows decision research actionable "
        "plannable routed batches first_calls next_command"
    )
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        first_batch = row.get("first_batch")
        first_calls = 0
        if isinstance(first_batch, Mapping):
            first_calls = _int_value(first_batch.get("external_calls_required"))
        print(
            f"{row.get('source')} "
            f"{row.get('status')} "
            f"{row.get('total_gap_rows')} "
            f"{row.get('decision_useful_gap_rows', 0)} "
            f"{row.get('research_useful_gap_rows', 0)} "
            f"{row.get('actionable_gap_rows', 0)} "
            f"{row.get('plannable_gap_rows')} "
            f"{row.get('routed_gap_rows')} "
            f"{row.get('batch_count')} "
            f"{first_calls} "
            f"{_compact_cli_text(row.get('execute_next_command'))}"
        )
        samples = row.get("priority_sample_tickers")
        if isinstance(samples, list | tuple) and samples:
            print(
                "  priority_examples_preview="
                f"{','.join(str(ticker) for ticker in samples)}"
            )
        action = row.get("next_action")
        if action:
            print(f"  next={_compact_cli_text(action)}")
        plan_command = row.get("all_batches_command")
        if plan_command:
            print(f"  plan={_compact_cli_text(plan_command)}")
        capped_command = row.get("execute_batches_command")
        if capped_command:
            print(f"  capped_execute={_compact_cli_text(capped_command)}")
        _print_priced_in_source_diagnostic(row.get("diagnostic"), indent="  ")
        approval = row.get("approval_checklist")
        if isinstance(approval, Mapping):
            _print_priced_in_approval_checklist(approval, indent="  ")


def _print_priced_in_recommendation_first_batch(
    payload: Mapping[str, object],
    recommendation: Mapping[str, object],
    *,
    label: str,
) -> None:
    source = str(recommendation.get("source") or "").strip()
    if not source:
        return
    source_row = _source_row(payload, source)
    first_batch = source_row.get("first_batch") if source_row else None
    if not isinstance(first_batch, Mapping):
        return
    raw_tickers = first_batch.get("tickers")
    tickers = (
        [str(ticker) for ticker in raw_tickers if str(ticker).strip()]
        if isinstance(raw_tickers, list | tuple)
        else []
    )
    ticker_text = ",".join(tickers) if tickers else "n/a"
    row_start = first_batch.get("row_start")
    row_end = first_batch.get("row_end")
    calls = _int_value(first_batch.get("external_calls_required"))
    command = first_batch.get("command") or recommendation.get("command")
    print(
        f"  {label}_batch="
        "scope=first_provider_chunk "
        f"rows={row_start}-{row_end} "
        f"tickers={ticker_text} "
        f"calls={calls} "
        f"command={_compact_cli_text(command)}"
    )


def _source_row(
    payload: Mapping[str, object],
    source: str,
) -> Mapping[str, object] | None:
    for row in payload.get("sources", []):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("source") or "").strip() == source:
            return row
    return None


def _print_priced_in_source_diagnostic(
    diagnostic: object,
    *,
    indent: str = "",
) -> None:
    if not isinstance(diagnostic, Mapping):
        return
    blocked_rows = _int_value(diagnostic.get("blocked_rows"))
    eligible_rows = _int_value(diagnostic.get("eligible_rows"))
    reason = str(diagnostic.get("blocked_reason") or "").strip()
    samples = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(diagnostic.get("sample_blocked_tickers"))
        if str(ticker).strip()
    ]
    if blocked_rows <= 0 and not reason and not samples:
        return
    print(
        f"{indent}diagnostic="
        f"eligible={eligible_rows} blocked={blocked_rows} "
        f"reason={reason or 'n/a'}"
    )
    if samples:
        print(f"{indent}blocked_examples={','.join(samples)}")
    for key, label in (
        ("fix_command", "refresh"),
        ("manual_template_command", "template"),
        ("manual_validate_command", "validate"),
        ("manual_fix_command", "import"),
    ):
        command = diagnostic.get(key)
        if command:
            print(f"{indent}{label}={_compact_cli_text(command)}")


def _print_priced_in_approval_checklist(
    approval: Mapping[str, object],
    *,
    indent: str = "",
) -> None:
    print(
        f"{indent}approval_checklist="
        f"required={str(bool(approval.get('approval_required'))).lower()} "
        f"provider={approval.get('provider') or 'n/a'} "
        f"calls={_int_value(approval.get('external_calls_required'))} "
        f"trade_orders={str(bool(approval.get('trade_order_submission_allowed'))).lower()} "
        f"command={_compact_cli_text(approval.get('execute_next_command'))}"
    )
    summary = approval.get("summary")
    if summary:
        print(f"{indent}  approval_summary={_compact_cli_text(summary)}")
    for index, item in enumerate(_sequence_value(approval.get("items")), start=1):
        if not isinstance(item, Mapping):
            continue
        print(
            f"{indent}  approval_{index}="
            f"{_compact_cli_text(item.get('item'))}: "
            f"{_compact_cli_text(item.get('detail'))}"
        )


def _print_priced_in_source_batches(payload: Mapping[str, object]) -> None:
    print(
        "priced_in_source_batches "
        f"source={payload.get('source')} "
        f"status={payload.get('status')} "
        f"gap_rows={payload.get('total_gap_rows')} "
        f"plannable={payload.get('plannable_gap_rows')} "
        f"routed={payload.get('routed_gap_rows')} "
        f"planned_at={payload.get('planned_at')} "
        f"batch_size={payload.get('batch_size')} "
        f"batches={payload.get('count')} "
        f"total_batches={payload.get('batch_count')} "
        f"batch_offset={payload.get('batch_offset')} "
        f"all_batches={payload.get('all_batches')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(f"headline={payload.get('headline')}")
    scan_scope = payload.get("scan_scope")
    if isinstance(scan_scope, Mapping):
        print(
            "scan_scope="
            f"mode={scan_scope.get('mode')} "
            f"stocks_only={str(bool(scan_scope.get('stocks_only'))).lower()} "
            f"gap_rows={scan_scope.get('full_scan_gap_rows')} "
            f"plannable={scan_scope.get('plannable_rows')} "
            f"returned_batches={scan_scope.get('returned_batches')} "
            f"planned_batches={scan_scope.get('planned_batches')} "
            f"returned_tickers={scan_scope.get('returned_tickers')} "
            f"batch_sample={str(bool(scan_scope.get('tickers_are_batch_sample'))).lower()} "
            f"ticker_scope={scan_scope.get('returned_ticker_scope') or 'n/a'}"
        )
        explanation = scan_scope.get("explanation")
        if explanation:
            print(f"scope_note={_compact_cli_text(explanation)}")
        batch_note = scan_scope.get("batch_preview_note")
        if batch_note:
            print(f"ticker_scope_note={_compact_cli_text(batch_note)}")
    print(f"next_action={payload.get('next_action')}")
    boundary = payload.get("execution_boundary")
    if boundary:
        print(f"boundary={_compact_cli_text(boundary)}")
    approval = payload.get("approval_checklist")
    if isinstance(approval, Mapping):
        _print_priced_in_approval_checklist(approval)
    review_command = payload.get("review_rows_command")
    if review_command:
        print(f"review_full_scan_source_gap={_compact_cli_text(review_command)}")
    export_command = payload.get("export_rows_command")
    if export_command:
        print(f"export_full_scan_source_gap={_compact_cli_text(export_command)}")
    diagnostic = payload.get("diagnostic")
    if isinstance(diagnostic, Mapping):
        print(
            "diagnostic="
            f"status={diagnostic.get('status')} "
            f"eligible={diagnostic.get('eligible_rows')} "
            f"blocked={diagnostic.get('blocked_rows')} "
            f"reason={_compact_cli_text(diagnostic.get('reason'))}"
        )
        blocked_samples = diagnostic.get("sample_blocked_tickers")
        if isinstance(blocked_samples, list | tuple) and blocked_samples:
            print(
                "blocked_examples="
                f"{','.join(str(ticker) for ticker in blocked_samples)} "
                f"reason={_compact_cli_text(diagnostic.get('blocked_reason'))}"
            )
        type_counts = diagnostic.get("missing_cik_type_counts")
        if isinstance(type_counts, Mapping) and type_counts:
            print(
                "missing_cik_types="
                f"{_count_summary({str(key): value for key, value in type_counts.items()})} "
                f"company_like={_int_value(diagnostic.get('missing_cik_company_like_rows'))} "
                f"non_company={_int_value(diagnostic.get('missing_cik_non_company_rows'))} "
                f"unknown={_int_value(diagnostic.get('missing_cik_unknown_type_rows'))}"
            )
        non_company_samples = diagnostic.get("sample_non_company_missing_cik_tickers")
        if isinstance(non_company_samples, list | tuple) and non_company_samples:
            print(
                "non_company_cik_examples="
                f"{','.join(str(ticker) for ticker in non_company_samples)}"
            )
        routed_samples = diagnostic.get("sample_routed_non_company_tickers")
        routed_count = _int_value(diagnostic.get("routed_non_company_rows"))
        if routed_count:
            routed_sample_text = (
                ",".join(str(ticker) for ticker in routed_samples)
                if isinstance(routed_samples, list | tuple)
                else ""
            )
            print(
                "non_company_route="
                f"routed={routed_count} "
                f"examples={routed_sample_text} "
                f"route={_compact_cli_text(diagnostic.get('non_company_evidence_route'))}"
            )
        company_like_samples = diagnostic.get("sample_company_like_missing_cik_tickers")
        if isinstance(company_like_samples, list | tuple) and company_like_samples:
            print(
                "company_like_cik_examples="
                f"{','.join(str(ticker) for ticker in company_like_samples)}"
            )
        diagnostic_next = diagnostic.get("next_action")
        if diagnostic_next:
            print(f"diagnostic_next={_compact_cli_text(diagnostic_next)}")
        point_in_time_template = diagnostic.get("point_in_time_template_command")
        if point_in_time_template:
            print(
                "diagnostic_point_in_time_template="
                f"{_compact_cli_text(point_in_time_template)}"
            )
        point_in_time_validate = diagnostic.get("point_in_time_validate_command")
        if point_in_time_validate:
            print(
                "diagnostic_point_in_time_validate="
                f"{_compact_cli_text(point_in_time_validate)}"
            )
        point_in_time_import = diagnostic.get("point_in_time_import_command")
        if point_in_time_import:
            print(
                "diagnostic_point_in_time_import="
                f"{_compact_cli_text(point_in_time_import)}"
            )
        fix_command = diagnostic.get("fix_command")
        if fix_command:
            print(f"diagnostic_command={_compact_cli_text(fix_command)}")
        manual_fix_command = diagnostic.get("manual_fix_command")
        if manual_fix_command:
            print(f"diagnostic_manual_command={_compact_cli_text(manual_fix_command)}")
        manual_fix_api = diagnostic.get("manual_fix_api")
        if manual_fix_api:
            print(f"diagnostic_manual_api={_compact_cli_text(manual_fix_api)}")
        manual_validate_command = diagnostic.get("manual_validate_command")
        if manual_validate_command:
            print(
                "diagnostic_manual_validate_command="
                f"{_compact_cli_text(manual_validate_command)}"
            )
        manual_validate_api = diagnostic.get("manual_validate_api")
        if manual_validate_api:
            print(
                "diagnostic_manual_validate_api="
                f"{_compact_cli_text(manual_validate_api)}"
            )
        manual_template_command = diagnostic.get("manual_template_command")
        if manual_template_command:
            print(
                "diagnostic_manual_template_command="
                f"{_compact_cli_text(manual_template_command)}"
            )
        manual_template_api = diagnostic.get("manual_template_api")
        if manual_template_api:
            print(
                "diagnostic_manual_template_api="
                f"{_compact_cli_text(manual_template_api)}"
            )
        fix_api = diagnostic.get("fix_api")
        if fix_api:
            print(f"diagnostic_api={_compact_cli_text(fix_api)}")
    review_command = payload.get("review_rows_command")
    if review_command:
        print(f"review_rows={_compact_cli_text(review_command)}")
    export_command = payload.get("export_rows_command")
    if export_command:
        print(f"export_rows={_compact_cli_text(export_command)}")
    all_batches_command = payload.get("all_batches_command")
    if all_batches_command:
        print(f"all_batches={_compact_cli_text(all_batches_command)}")
    execute_batches_command = payload.get("execute_batches_command")
    if execute_batches_command:
        print(f"execute_batches={_compact_cli_text(execute_batches_command)}")
    all_batches_api = payload.get("all_batches_api")
    if all_batches_api:
        print(f"all_batches_api={_compact_cli_text(all_batches_api)}")
    batches = payload.get("batches")
    if not isinstance(batches, list | tuple) or not batches:
        return
    print("batch calls row_start row_end tickers command")
    for batch in batches:
        if not isinstance(batch, Mapping):
            continue
        tickers = batch.get("tickers")
        ticker_text = (
            ",".join(str(ticker) for ticker in tickers)
            if isinstance(tickers, list | tuple)
            else ""
        )
        print(
            f"{batch.get('number')} "
            f"{batch.get('external_calls_required')} "
            f"{batch.get('row_start')} "
            f"{batch.get('row_end')} "
            f"{ticker_text} "
            f"{_compact_cli_text(batch.get('command'))}"
        )
        breakdown = batch.get("external_call_breakdown")
        if isinstance(breakdown, Mapping) and breakdown:
            print(f"  calls={_count_summary(breakdown)}")
        call_plan_status = batch.get("call_plan_status")
        if call_plan_status:
            print(f"  call_plan={_compact_cli_text(call_plan_status)}")
    next_command = payload.get("next_batch_command")
    if next_command:
        print(f"more={_compact_cli_text(next_command)}")


def _print_priced_in_source_batch_execution(payload: Mapping[str, object]) -> None:
    plan = payload.get("plan")
    batch = payload.get("batch")
    result = payload.get("result")
    print(
        "priced_in_source_batch_execution "
        f"source={payload.get('source')} "
        f"status={payload.get('status')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    reason = payload.get("reason")
    if reason:
        print(f"reason={_compact_cli_text(reason)}")
    if isinstance(plan, Mapping):
        print(
            "plan="
            f"status={plan.get('status')} "
            f"gap_rows={plan.get('total_gap_rows')} "
            f"plannable={plan.get('plannable_gap_rows')} "
            f"total_batches={plan.get('batch_count')} "
            f"batch_size={plan.get('batch_size')}"
        )
    print(f"summary={_compact_cli_text(source_batch_execution_summary(payload))}")
    if isinstance(batch, Mapping) and batch:
        tickers = batch.get("tickers")
        ticker_text = (
            ",".join(str(ticker) for ticker in tickers)
            if isinstance(tickers, list | tuple)
            else ""
        )
        print(
            "batch="
            f"number={batch.get('number')} "
            f"rows={batch.get('row_start')}-{batch.get('row_end')} "
            f"tickers={ticker_text} "
            f"call_plan={_compact_cli_text(batch.get('call_plan_status'))}"
        )
    if isinstance(result, Mapping) and result:
        print(
            "result="
            f"provider={result.get('provider')} "
            f"endpoint={result.get('endpoint')} "
            f"ticker_count={result.get('ticker_count')} "
            f"feature_count={result.get('feature_count')} "
            f"snippet_count={result.get('snippet_count')} "
            f"event_count={result.get('event_count')} "
            f"option_features={result.get('option_features_upserted')}"
        )
    post_execution = payload.get("post_execution")
    if isinstance(post_execution, Mapping) and post_execution:
        print(
            "post_execution="
            f"status={post_execution.get('status')} "
            f"gap_rows={post_execution.get('before_gap_rows')}->"
            f"{post_execution.get('after_gap_rows')} "
            f"resolved={post_execution.get('gap_rows_resolved')} "
            f"plannable={post_execution.get('before_plannable_rows')}->"
            f"{post_execution.get('after_plannable_rows')} "
            f"external_calls={post_execution.get('external_calls_made')}"
        )
        next_action = post_execution.get("next_action")
        if next_action:
            print(f"post_next={_compact_cli_text(next_action)}")
        next_plan = post_execution.get("all_batches_command")
        if next_plan:
            print(f"post_plan={_compact_cli_text(next_plan)}")


def _print_priced_in_source_batch_run(payload: Mapping[str, object]) -> None:
    before = payload.get("before_plan")
    after = payload.get("after_plan")
    print(
        "priced_in_source_batch_run "
        f"source={payload.get('source')} "
        f"status={payload.get('status')} "
        f"executed={payload.get('executed_batches')}/"
        f"{payload.get('requested_batches')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        print(
            "coverage="
            f"gap_rows={before.get('total_gap_rows')}->{after.get('total_gap_rows')} "
            f"resolved={payload.get('gap_rows_resolved')} "
            f"plannable={before.get('plannable_gap_rows')}->"
            f"{after.get('plannable_gap_rows')} "
            f"batches={before.get('batch_count')}->{after.get('batch_count')}"
        )
    stopped = payload.get("stopped_reason")
    if stopped:
        print(f"stopped={_compact_cli_text(stopped)}")
    print(f"summary={_compact_cli_text(source_batch_run_summary(payload))}")
    next_action = payload.get("next_action")
    if next_action:
        print(f"next_action={_compact_cli_text(next_action)}")
    next_command = payload.get("next_command")
    if next_command:
        print(f"next_command={_compact_cli_text(next_command)}")
    executions = payload.get("executions")
    if isinstance(executions, list | tuple) and executions:
        print("chunks:")
        for index, execution in enumerate(executions, start=1):
            if not isinstance(execution, Mapping):
                continue
            batch = execution.get("batch")
            tickers = ""
            if isinstance(batch, Mapping):
                raw_tickers = batch.get("tickers")
                if isinstance(raw_tickers, list | tuple):
                    tickers = ",".join(str(ticker) for ticker in raw_tickers)
            print(
                f"- {index} "
                f"status={execution.get('status')} "
                f"calls={execution.get('external_calls_made')} "
                f"tickers={tickers} "
                f"reason={_compact_cli_text(execution.get('reason'))}"
            )


def _count_summary(counts: Mapping[object, object]) -> str:
    parts = [
        f"{key}:{_int_value(value)}"
        for key, value in sorted(counts.items(), key=lambda item: str(item[0]))
    ]
    return ",".join(parts)


def _print_priced_in_instrument_scope(value: object) -> None:
    if not isinstance(value, Mapping) or not value:
        return
    type_counts = value.get("type_counts")
    type_summary = (
        _count_summary({str(key): item for key, item in type_counts.items()})
        if isinstance(type_counts, Mapping) and type_counts
        else "n/a"
    )
    print(
        "instrument_scope="
        f"rows={value.get('row_count')} "
        f"company_like={value.get('company_like_rows')} "
        f"non_company={value.get('non_company_rows')} "
        f"unknown={value.get('unknown_type_rows')} "
        f"types={type_summary}"
    )
    sec_scope = value.get("sec_catalyst_applicability")
    if isinstance(sec_scope, Mapping):
        print(
            "sec_catalyst_applicability="
            f"applicable={sec_scope.get('applicable_rows')} "
            f"non_applicable={sec_scope.get('non_applicable_rows')} "
            f"unknown={sec_scope.get('unknown_type_rows')} "
            f"next={_compact_cli_text(sec_scope.get('next_action'))}"
        )


def _priced_in_more_command(filters: object, limit: int, next_offset: int) -> str:
    parts = ["catalyst-radar", "priced-in-queue"]
    if isinstance(filters, Mapping):
        available_at = str(filters.get("available_at") or "").strip()
        if available_at:
            parts.extend(["--available-at", available_at])
        status = str(filters.get("status") or "").strip()
        if status and status != "all":
            parts.extend(["--status", status])
        usefulness = str(filters.get("usefulness") or "").strip()
        if usefulness and usefulness != "all":
            parts.extend(["--usefulness", usefulness])
        source_gap = filters.get("source_gap")
        if isinstance(source_gap, list | tuple):
            for source in source_gap:
                parts.extend(["--source-gap", str(source)])
        decision_gap = filters.get("decision_gap")
        if isinstance(decision_gap, list | tuple):
            for gap in decision_gap:
                parts.extend(["--decision-gap", str(gap)])
        min_gap = filters.get("min_gap")
        if min_gap is not None:
            parts.extend(["--min-gap", str(min_gap)])
    parts.extend(["--limit", str(limit), "--offset", str(next_offset)])
    return " ".join(parts)


def _int_value(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _priced_in_data_summary(row: Mapping[str, object]) -> str:
    data_sources = row.get("data_sources")
    if isinstance(data_sources, Mapping):
        usefulness = row.get("usefulness")
        if not isinstance(usefulness, Mapping):
            usefulness = {}
        routed = {
            str(item)
            for item in _sequence_value(usefulness.get("routed_optional_sources"))
            if str(item).strip()
        }
        parts: list[str] = []
        for label in ("available", "stale", "missing"):
            values = [
                str(item)
                for item in _sequence_value(data_sources.get(label))
                if str(item).strip() and str(item) not in routed
            ]
            if values:
                parts.append(f"{label}: {', '.join(values)}")
        if routed:
            parts.append(f"routed: {', '.join(sorted(routed))}")
        if parts:
            return "; ".join(parts).replace(" ", "_")
    return "n/a"


def _print_priced_in_audit(payload: Mapping[str, object]) -> None:
    scope = payload.get("scope") if isinstance(payload.get("scope"), Mapping) else {}
    counts = payload.get("counts") if isinstance(payload.get("counts"), Mapping) else {}
    market = (
        payload.get("market_bars")
        if isinstance(payload.get("market_bars"), Mapping)
        else {}
    )
    coverage = (
        payload.get("source_coverage")
        if isinstance(payload.get("source_coverage"), Mapping)
        else {}
    )
    print(
        "priced_in_audit "
        f"status={payload.get('status')} "
        f"active={scope.get('active_securities')} "
        f"scanned={scope.get('scanned_rows')} "
        f"ranked={scope.get('ranked_rows')} "
        f"research={counts.get('research_lead_rows')} "
        f"decision={counts.get('decision_ready_rows')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(f"question={payload.get('question')}")
    print(f"answer={_compact_cli_text(payload.get('answer'))}")
    print(f"headline={_compact_cli_text(payload.get('headline'))}")
    print(
        "market_bars="
        f"status={market.get('status')} "
        f"coverage={market.get('with_as_of_bar')}/"
        f"{market.get('active_securities')} "
        f"missing={market.get('missing_as_of_bar')} "
        f"coverage_pct={market.get('coverage_pct')}"
    )
    repair = market.get("repair")
    if isinstance(repair, Mapping):
        sample = ",".join(
            str(ticker)
            for ticker in _sequence_value(
                repair.get("missing_as_of_bar_ticker_sample")
            )
        )
        print(
            "market_bar_repair="
            f"status={repair.get('status')} "
            f"expected_as_of={repair.get('target_as_of') or 'n/a'} "
            f"missing={repair.get('missing_as_of_bar')} "
            f"sample={sample or '-'} "
            f"external_calls={repair.get('external_calls_made')}"
        )
        if repair.get("template_command"):
            print(f"  template={_compact_cli_text(repair.get('template_command'))}")
        if repair.get("import_preview_command"):
            print(
                "  preview_import="
                f"{_compact_cli_text(repair.get('import_preview_command'))}"
            )
        if repair.get("import_execute_command"):
            print(
                "  execute_import="
                f"{_compact_cli_text(repair.get('import_execute_command'))}"
            )
        if repair.get("write_boundary"):
            print(f"  boundary={_compact_cli_text(repair.get('write_boundary'))}")
        diagnostic = repair.get("diagnostic")
        if isinstance(diagnostic, Mapping) and diagnostic:
            print(
                "  missing_bar_diagnostic="
                f"status={diagnostic.get('status')} "
                f"company_like={_int_value(diagnostic.get('company_like_missing_count'))} "
                f"fund_like={_int_value(diagnostic.get('fund_like_missing_count'))} "
                f"wrappers={_int_value(diagnostic.get('wrapper_missing_count'))} "
                f"unknown={_int_value(diagnostic.get('unknown_missing_count'))} "
                f"external_calls={diagnostic.get('external_calls_made')}"
            )
            type_counts = diagnostic.get("type_counts")
            if isinstance(type_counts, Mapping) and type_counts:
                print(
                    "    missing_bar_types="
                    + ",".join(
                        f"{key}:{_int_value(value)}"
                        for key, value in sorted(type_counts.items())
                    )
                )
            samples = []
            for key in (
                "sample_company_like_tickers",
                "sample_fund_like_tickers",
                "sample_wrapper_tickers",
                "sample_unknown_tickers",
            ):
                values = _sequence_value(diagnostic.get(key))
                if values:
                    samples.append(f"{key}={','.join(str(value) for value in values)}")
            if samples:
                print("    " + " ".join(samples))
            route_boundary = diagnostic.get("route_boundary")
            if route_boundary:
                print(f"    route_boundary={_compact_cli_text(route_boundary)}")
        stock_scope = repair.get("stock_scope")
        if isinstance(stock_scope, Mapping) and stock_scope:
            print(
                "  stock_bar_scope="
                f"status={stock_scope.get('status')} "
                f"coverage={_int_value(stock_scope.get('stock_like_with_as_of_bar'))}/"
                f"{_int_value(stock_scope.get('stock_like_active'))} "
                f"missing={_int_value(stock_scope.get('stock_like_missing_as_of_bar'))} "
                f"non_stock_missing={_int_value(stock_scope.get('non_stock_missing_as_of_bar'))} "
                f"unknown_missing={_int_value(stock_scope.get('unknown_type_missing_as_of_bar'))} "
                f"external_calls={stock_scope.get('external_calls_made')}"
            )
            missing_stocks = _sequence_value(
                stock_scope.get("sample_missing_stock_like_tickers")
            )
            if missing_stocks:
                print(
                    "    sample_missing_stock_like_tickers="
                    f"{','.join(str(ticker) for ticker in missing_stocks)}"
                )
            answer_boundary = stock_scope.get("answer_boundary")
            if answer_boundary:
                print(f"    answer_boundary={_compact_cli_text(answer_boundary)}")
        provider_plan = repair.get("provider_fill_plan")
        if isinstance(provider_plan, Mapping) and provider_plan:
            print(
                "  provider_fill_plan="
                f"provider={provider_plan.get('provider_label') or provider_plan.get('provider')} "
                f"status={provider_plan.get('status')} "
                f"execute_calls={_int_value(provider_plan.get('execute_external_call_count'))} "
                f"key_configured={str(bool(provider_plan.get('provider_key_configured'))).lower()} "
                f"external_calls={provider_plan.get('external_calls_made')}"
            )
            provider_command = provider_plan.get("provider_call_command")
            if provider_command:
                print(f"    provider_command={_compact_cli_text(provider_command)}")
            manual_command = provider_plan.get("manual_template_command")
            if manual_command:
                print(f"    manual_template={_compact_cli_text(manual_command)}")
            approval_boundary = provider_plan.get("approval_boundary")
            if approval_boundary:
                print(f"    approval_boundary={_compact_cli_text(approval_boundary)}")
            point_in_time_boundary = provider_plan.get("point_in_time_boundary")
            if point_in_time_boundary:
                print(
                    "    point_in_time_boundary="
                    f"{_compact_cli_text(point_in_time_boundary)}"
                )
    print(
        "source_coverage="
        f"ready={coverage.get('ready_source_count')}/"
        f"{coverage.get('source_count')} "
        f"weak={','.join(str(item) for item in _sequence_value(coverage.get('weak_sources')))}"
    )
    performance = payload.get("performance")
    if isinstance(performance, Mapping):
        perf_parts = [
            f"cache={performance.get('cache_status')}",
            f"ttl_s={performance.get('cache_ttl_seconds')}",
        ]
        if performance.get("build_elapsed_ms") is not None:
            perf_parts.append(f"build_ms={performance.get('build_elapsed_ms')}")
        if performance.get("cache_age_ms") is not None:
            perf_parts.append(f"age_ms={performance.get('cache_age_ms')}")
        print("performance=" + " ".join(perf_parts))
    primary_scan = payload.get("primary_scan")
    if isinstance(primary_scan, Mapping):
        print(
            "primary_full_scan="
            f"scope={primary_scan.get('scope')} "
            f"active={primary_scan.get('active_securities')} "
            f"scanned={primary_scan.get('scanned_rows')} "
            f"ranked={primary_scan.get('ranked_rows')} "
            f"display={primary_scan.get('display_mode')} "
            f"visible={primary_scan.get('visible_row_start')}-"
            f"{primary_scan.get('visible_row_end')} "
            f"external_calls={primary_scan.get('external_calls_made')}"
        )
        print(
            "  boundary="
            f"{_compact_cli_text(primary_scan.get('scope_boundary'))}"
        )
        visible_note = primary_scan.get("visible_rows_note")
        if visible_note:
            print(f"  visible_rows_note={_compact_cli_text(visible_note)}")
        export_command = primary_scan.get("export_command")
        if export_command:
            print(f"  export_full_scan={_compact_cli_text(export_command)}")
    recommended_source = payload.get("recommended_source_gap")
    if isinstance(recommended_source, Mapping):
        print(
            "recommended_source_gap="
            f"source={recommended_source.get('source')} "
            f"decision={recommended_source.get('decision_useful_gap_rows')} "
            f"actionable={recommended_source.get('actionable_gap_rows')} "
            f"research={recommended_source.get('research_useful_gap_rows')} "
            f"gap_rows={recommended_source.get('gap_count')} "
            f"review={_compact_cli_text(recommended_source.get('review_command'))}"
        )
        print(
            "  why="
            f"{_compact_cli_text(recommended_source.get('rationale'))}"
        )
        print(
            "  boundary="
            f"{_compact_cli_text(recommended_source.get('execution_boundary'))}"
        )
        full_scan_command = recommended_source.get("full_scan_command")
        if full_scan_command:
            print(
                "  full_source_gap_export="
                f"{_compact_cli_text(full_scan_command)}"
            )
        sample_boundary = recommended_source.get("sample_boundary")
        if sample_boundary:
            print(f"  sample_boundary={_compact_cli_text(sample_boundary)}")
        repair = recommended_source.get("repair")
        if isinstance(repair, Mapping) and repair:
            print(
                "  source_gap_repair="
                f"source={repair.get('source')} "
                f"status={repair.get('status')} "
                f"diagnostic={repair.get('diagnostic_status') or 'n/a'} "
                f"provider_batch_allowed="
                f"{str(bool(repair.get('provider_batch_allowed'))).lower()} "
                f"external_calls={repair.get('external_calls_made')}"
            )
            print(f"    next={_compact_cli_text(repair.get('next_action'))}")
            point_in_time = repair.get("point_in_time_import_command")
            if point_in_time:
                print(
                    "    point_in_time_import="
                    f"{_compact_cli_text(point_in_time)}"
                )
            non_company_route = repair.get("non_company_route")
            if non_company_route:
                print(
                    "    non_company_route="
                    f"{_compact_cli_text(non_company_route)}"
                )
            prerequisite = repair.get("prerequisite_command")
            if prerequisite:
                print(f"    prerequisite={_compact_cli_text(prerequisite)}")
            batch_plan = repair.get("batch_plan_command")
            if batch_plan:
                print(f"    batch_plan={_compact_cli_text(batch_plan)}")
            boundary = repair.get("write_boundary")
            if boundary:
                print(f"    boundary={_compact_cli_text(boundary)}")
    shortlist = payload.get("answer_shortlist")
    if isinstance(shortlist, Mapping):
        print(
            "answer_shortlist="
            f"status={shortlist.get('status')} "
            f"focus={shortlist.get('focus')} "
            f"decision_ready={shortlist.get('decision_ready_rows')} "
            f"actionable={shortlist.get('actionable_mismatch_rows')} "
            f"visible={shortlist.get('visible_rows')} "
            f"sample={str(bool(shortlist.get('visible_rows_are_sample'))).lower()} "
            f"selection={shortlist.get('selection_scope')} "
            f"full_scan_rows={shortlist.get('full_scan_rows')} "
            f"external_calls={shortlist.get('external_calls_made')}"
        )
        print(f"  summary={_compact_cli_text(shortlist.get('summary'))}")
        selection_note = shortlist.get("selection_note")
        if selection_note:
            print(f"  selection_note={_compact_cli_text(selection_note)}")
        boundary = shortlist.get("investment_decision_boundary")
        if boundary:
            print(f"  boundary={_compact_cli_text(boundary)}")
        shortlist_export = shortlist.get("full_scan_export_command")
        if shortlist_export:
            print(f"  full_scan_export={_compact_cli_text(shortlist_export)}")
        rows = shortlist.get("rows")
        if isinstance(rows, list | tuple) and rows:
            print("  ticker rank status decision_ready gap emotion reaction missing next_step")
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                missing = ",".join(
                    str(item) for item in _sequence_value(row.get("missing_sources"))
                )
                print(
                    "  "
                    f"{row.get('ticker')} "
                    f"{row.get('rank')} "
                    f"{row.get('status')} "
                    f"{str(bool(row.get('decision_ready'))).lower()} "
                    f"{row.get('emotion_reaction_gap')} "
                    f"{row.get('emotion_score')} "
                    f"{row.get('reaction_score')} "
                    f"{missing or '-'} "
                    f"{_compact_cli_text(row.get('next_step'))}"
                )
                drilldown = row.get("drilldown")
                if isinstance(drilldown, Mapping):
                    detail = drilldown.get("detail_command")
                    if detail:
                        print(f"    detail={_compact_cli_text(detail)}")
                    gaps = drilldown.get("evidence_gap_summary")
                    if gaps:
                        print(f"    evidence_gaps={_compact_cli_text(gaps)}")
                    for action in _sequence_value(
                        drilldown.get("source_gap_actions")
                    ):
                        if not isinstance(action, Mapping):
                            continue
                        print(
                            "    source_gap_action="
                            f"{action.get('source')} "
                            f"status={action.get('status')} "
                            f"review={_compact_cli_text(action.get('review_command'))} "
                            f"plan={_compact_cli_text(action.get('plan_command'))}"
                        )
    _print_priced_in_instrument_scope(payload.get("instrument_scope"))
    preview = payload.get("preview")
    if isinstance(preview, Mapping):
        print(
            "full_scan_rows="
            f"{preview.get('row_start')}-{preview.get('row_end')}/"
            f"{preview.get('total_rows')} "
            f"display={'complete' if bool(preview.get('all_rows')) else 'page_preview'} "
            f"sample={str(bool(preview.get('has_more'))).lower()} "
            f"all_rows={str(bool(preview.get('all_rows'))).lower()} "
            f"export={_compact_cli_text(preview.get('export_command'))}"
        )
        explanation = preview.get("sample_explanation")
        if explanation:
            print(f"full_scan_row_note={_compact_cli_text(explanation)}")
        source_actions = preview.get("source_gap_actions")
        if isinstance(source_actions, list | tuple) and source_actions:
            print("selected_source_gap_actions:")
            for action in source_actions:
                if not isinstance(action, Mapping):
                    continue
                print(
                    "- "
                    f"{action.get('source')} "
                    f"status={action.get('status')} "
                    f"gap_rows={_int_value(action.get('gap_count'))} "
                    f"next={_compact_cli_text(action.get('next_action'))} "
                    f"plan={_compact_cli_text(action.get('plan_command'))}"
                )
                boundary = action.get("execution_boundary")
                if boundary:
                    print(f"  boundary={_compact_cli_text(boundary)}")
                full_scan_export = action.get("export_rows_command")
                if full_scan_export:
                    print(
                        "  source_gap_full_scan_export="
                        f"{_compact_cli_text(full_scan_export)}"
                    )
                all_batches = action.get("all_batches_command")
                if all_batches:
                    print(
                        "  all_provider_batches="
                        f"{_compact_cli_text(all_batches)}"
                    )
                batch_status = action.get("batch_status")
                if batch_status:
                    print(
                        "  provider_batch_plan="
                        f"status={batch_status} "
                        f"gap_rows={_int_value(action.get('full_scan_gap_rows'))} "
                        f"plannable={_int_value(action.get('plannable_gap_rows'))} "
                        f"batches={_int_value(action.get('provider_batch_count'))}"
                    )
                first_batch_tickers = _sequence_value(action.get("first_batch_tickers"))
                if first_batch_tickers:
                    print(
                        "  first_provider_batch="
                        f"tickers={','.join(str(item) for item in first_batch_tickers)} "
                        f"calls={_int_value(action.get('first_batch_external_calls'))} "
                        f"command={_compact_cli_text(action.get('first_batch_command'))}"
                    )
                execute_next = action.get("execute_next_command")
                if execute_next:
                    print(f"  execute_next={_compact_cli_text(execute_next)}")
                approval = action.get("approval_checklist")
                if isinstance(approval, Mapping):
                    _print_priced_in_approval_checklist(approval, indent="  ")
                blocked_reason = action.get("blocked_reason")
                if blocked_reason:
                    print(
                        "  blocked="
                        f"reason={_compact_cli_text(blocked_reason)} "
                        f"next={_compact_cli_text(action.get('diagnostic_next_action'))}"
                    )
                batch_scope = action.get("batch_scope")
                if batch_scope:
                    print(f"  batch_scope={_compact_cli_text(batch_scope)}")
                batch_note = action.get("batch_preview_note")
                if batch_note:
                    print(f"  ticker_scope_note={_compact_cli_text(batch_note)}")
        audit_next = preview.get("audit_next_page_command")
        if audit_next:
            print(f"more={_compact_cli_text(audit_next)}")
    rows = payload.get("preview_rows")
    if isinstance(rows, list | tuple) and rows:
        print("full_scan_preview:")
        print("ticker status usefulness decision_ready gap emotion reaction priced")
        print("data next_step")
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            missing = ",".join(str(item) for item in _sequence_value(row.get("missing_sources")))
            stale = ",".join(str(item) for item in _sequence_value(row.get("stale_sources")))
            data_parts = []
            if missing:
                data_parts.append(f"missing:{missing}")
            if stale:
                data_parts.append(f"stale:{stale}")
            data_text = ";".join(data_parts) if data_parts else "covered"
            print(
                f"{row.get('ticker')} "
                f"{row.get('status')} "
                f"{row.get('usefulness') or 'n/a'} "
                f"{str(bool(row.get('decision_ready'))).lower()} "
                f"{row.get('emotion_reaction_gap')} "
                f"{row.get('emotion_score')} "
                f"{row.get('reaction_score')} "
                f"{row.get('priced_in_score')} "
                f"{data_text} "
                f"{_compact_cli_text(row.get('next_step'))}"
            )
    next_action = payload.get("next_action")
    if next_action:
        print(f"next_action={_compact_cli_text(next_action)}")
    next_command = payload.get("next_command")
    if next_command:
        print(f"next_command={_compact_cli_text(next_command)}")
    print("sources:")
    for row in _sequence_value(payload.get("sources")):
        if not isinstance(row, Mapping):
            continue
        print(
            f"- {row.get('source')} "
            f"status={row.get('status')} "
            f"coverage={row.get('available')}/{row.get('row_count')} "
            f"gap_rows={row.get('gap_count')} "
            f"decision={row.get('decision_useful_gap_rows', 0)} "
            f"research={row.get('research_useful_gap_rows', 0)} "
            f"actionable={row.get('actionable_gap_rows', 0)} "
            f"next={_compact_cli_text(row.get('next_action'))}"
        )
        samples = row.get("priority_sample_tickers")
        if isinstance(samples, list | tuple) and samples:
            print(
                "  priority_examples_preview="
                f"{','.join(str(ticker) for ticker in samples)}"
            )
        repair = row.get("repair")
        if isinstance(repair, Mapping) and repair:
            print(
                "  repair="
                f"status={repair.get('status')} "
                f"diagnostic={repair.get('diagnostic_status') or 'n/a'} "
                f"provider_batch_allowed="
                f"{str(bool(repair.get('provider_batch_allowed'))).lower()} "
                f"next={_compact_cli_text(repair.get('next_action'))}"
            )
            point_in_time = repair.get("point_in_time_import_command")
            if point_in_time:
                print(
                    "    point_in_time_import="
                    f"{_compact_cli_text(point_in_time)}"
                )
            non_company_route = repair.get("non_company_route")
            if non_company_route:
                print(
                    "    non_company_route="
                    f"{_compact_cli_text(non_company_route)}"
                )
            prerequisite = repair.get("prerequisite_command")
            if prerequisite:
                print(f"    prerequisite={_compact_cli_text(prerequisite)}")
            batch_plan = repair.get("batch_plan_command")
            if batch_plan:
                print(f"    batch_plan={_compact_cli_text(batch_plan)}")
            boundary = repair.get("write_boundary")
            if boundary:
                print(f"    boundary={_compact_cli_text(boundary)}")
    commands = payload.get("commands")
    if isinstance(commands, Mapping):
        print("commands:")
        for name, command in commands.items():
            print(f"- {name}={_compact_cli_text(command)}")


def _print_priced_in_answer(payload: Mapping[str, object]) -> None:
    counts = payload.get("counts") if isinstance(payload.get("counts"), Mapping) else {}
    print(
        "priced_in_answer "
        f"status={payload.get('status')} "
        f"decision_ready={str(bool(payload.get('decision_ready'))).lower()} "
        f"investment_decision_ready="
        f"{str(bool(payload.get('can_make_investment_decision'))).lower()} "
        f"total={counts.get('total_rows')} "
        f"mismatches={counts.get('actionable_mismatch_rows')} "
        f"research={counts.get('research_lead_rows')} "
        f"blocked={counts.get('blocked_rows')} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(f"question={payload.get('question')}")
    print(f"answer={payload.get('answer')}")
    print(f"headline={payload.get('headline')}")
    boundary = payload.get("investment_decision_boundary")
    if boundary:
        print(f"investment_boundary={_compact_cli_text(boundary)}")
    scan_scope = payload.get("scan_scope")
    if isinstance(scan_scope, Mapping):
        print(f"scan_scope={_compact_cli_text(scan_scope.get('explanation'))}")
        current_filter_export = scan_scope.get("current_filter_export_command")
        if current_filter_export:
            print(f"current_filter_export={_compact_cli_text(current_filter_export)}")
        full_scan_export = scan_scope.get("full_scan_export_command")
        if full_scan_export:
            print(f"full_scan_export={_compact_cli_text(full_scan_export)}")
        next_page = scan_scope.get("next_page_command")
        if next_page:
            print(f"next_page={_compact_cli_text(next_page)}")
    full_scan = payload.get("full_scan")
    if isinstance(full_scan, Mapping):
        print(
            "full_scan="
            f"mode={full_scan.get('mode')} "
            f"active={full_scan.get('active_securities') or 'n/a'} "
            f"scanned={full_scan.get('scanned_rows') or 'n/a'} "
            f"ranked={full_scan.get('ranked_rows')} "
            f"visible={full_scan.get('visible_row_start')}-"
            f"{full_scan.get('visible_row_end')} "
            f"sample={str(bool(full_scan.get('visible_tickers_are_sample'))).lower()}"
        )
        sample_explanation = full_scan.get("sample_explanation")
        if sample_explanation:
            print(f"sample_explanation={_compact_cli_text(sample_explanation)}")
        review_command = full_scan.get("review_command")
        if review_command:
            print(f"review_full_scan={_compact_cli_text(review_command)}")
        export_command = full_scan.get("full_export_command") or full_scan.get(
            "export_command"
        )
        if export_command:
            print(f"export_full_scan={_compact_cli_text(export_command)}")
    decision_readiness = payload.get("decision_readiness")
    if isinstance(decision_readiness, Mapping):
        print(
            "decision_readiness="
            f"status={decision_readiness.get('status')} "
            f"actionable={decision_readiness.get('actionable_mismatch_rows')} "
            f"decision_ready={decision_readiness.get('decision_ready_rows')} "
            f"summary={_compact_cli_text(decision_readiness.get('summary'))}"
        )
        recommended = decision_readiness.get("recommended_gap")
        if isinstance(recommended, Mapping):
            print(
                "recommended_gap="
                f"{recommended.get('gap')} "
                f"count={recommended.get('count')} "
                f"command={_compact_cli_text(recommended.get('command'))}"
            )
    print(f"next_action={payload.get('next_action')}")
    if payload.get("next_command"):
        print(f"next_command={_compact_cli_text(payload.get('next_command'))}")
    source_coverage = payload.get("source_coverage")
    if isinstance(source_coverage, Mapping):
        print(f"source_coverage={_compact_cli_text(source_coverage.get('summary'))}")
    blockers = payload.get("trust_blockers")
    if isinstance(blockers, list | tuple) and blockers:
        print("trust_blockers:")
        for blocker in blockers:
            if not isinstance(blocker, Mapping):
                continue
            print(
                "- "
                f"{blocker.get('area')} "
                f"status={blocker.get('status')} "
                f"next={_compact_cli_text(blocker.get('next_action'))} "
                f"command={_compact_cli_text(blocker.get('command'))}"
            )
    rows = payload.get("top_rows")
    if not isinstance(rows, list | tuple) or not rows:
        print("No useful priced-in rows.")
        return
    print(
        "actionable_rows_sample="
        "ranked actionable mismatches from the visible page; "
        "the full scan is the queue/export above."
    )
    print("ticker status usefulness decision_ready gap emotion reaction next_step")
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        print(
            f"{row.get('ticker')} "
            f"{row.get('status')} "
            f"{row.get('usefulness')} "
            f"{str(bool(row.get('decision_ready'))).lower()} "
            f"{row.get('emotion_reaction_gap')} "
            f"{row.get('emotion_score')} "
            f"{row.get('reaction_score')} "
            f"{_compact_cli_text(row.get('next_step'))}"
        )


def _print_priced_in_preflight(payload: Mapping[str, object]) -> None:
    print(
        "priced_in_preflight "
        f"status={payload.get('status')} "
        f"scan_status={payload.get('scan_status')} "
        f"target_as_of={payload.get('target_as_of') or 'n/a'} "
        f"target_source={payload.get('target_as_of_source') or 'n/a'} "
        f"external_calls={payload.get('external_calls_made')}"
    )
    print(f"headline={payload.get('headline')}")
    print(f"next_action={payload.get('next_action')}")
    scan_scope = payload.get("scan_scope")
    if isinstance(scan_scope, Mapping):
        print(
            "scan_scope "
            f"active={scan_scope.get('active_security_count')} "
            f"requested={scan_scope.get('requested_securities')} "
            f"scanned={scan_scope.get('scanned_securities')} "
            f"universe={scan_scope.get('universe') or 'all'}"
        )
    provider_blocker = payload.get("provider_blocker")
    if isinstance(provider_blocker, Mapping) and provider_blocker:
        print(
            "provider_blocker "
            f"provider={provider_blocker.get('provider')} "
            f"target_as_of={provider_blocker.get('target_as_of')} "
            f"reason={_compact_cli_text(provider_blocker.get('reason'))}"
        )
    provider = payload.get("provider")
    if isinstance(provider, Mapping):
        print(
            "provider "
            f"market={provider.get('market_provider')} "
            f"ticker_seed_cap_pages={provider.get('ticker_seed_cap_pages')} "
            f"ticker_page_delay_seconds={provider.get('ticker_page_delay_seconds')} "
            f"latest_bar_date={provider.get('latest_daily_bar_date')} "
            f"latest_bar_tickers={provider.get('latest_daily_bar_ticker_count')} "
            f"estimated_ticker_seed_pages={provider.get('estimated_ticker_seed_pages')}"
        )
    print("area status finding next_action command api")
    for row in payload.get("rows", []):
        if not isinstance(row, Mapping):
            continue
        print(
            f"{row.get('area')} "
            f"{row.get('status')} "
            f"{_compact_cli_text(row.get('finding'))} "
            f"{_compact_cli_text(row.get('next_action'))} "
            f"{_compact_cli_text(row.get('command'))} "
            f"{_compact_cli_text(row.get('api'))}"
        )
    evidence_plan = payload.get("evidence_plan")
    if isinstance(evidence_plan, Mapping):
        print(
            "evidence_plan "
            f"status={evidence_plan.get('status')} "
            f"steps={len(_sequence_value(evidence_plan.get('steps')))} "
            f"next={_compact_cli_text(evidence_plan.get('next_action'))}"
        )
        print("priority area status depends_on action command")
        for step in _sequence_value(evidence_plan.get("steps")):
            if not isinstance(step, Mapping):
                continue
            depends_on = step.get("depends_on")
            if isinstance(depends_on, list | tuple):
                depends_text = ",".join(str(item) for item in depends_on if str(item))
            else:
                depends_text = "none"
            print(
                f"{step.get('priority')} "
                f"{step.get('area')} "
                f"{step.get('status')} "
                f"{depends_text or 'none'} "
                f"{_compact_cli_text(step.get('action'))} "
                f"{_compact_cli_text(step.get('command'))}"
            )


def _print_candidate_detail(payload: Mapping[str, object]) -> None:
    ticker = str(payload.get("ticker") or "").upper()
    brief = payload.get("priced_in_evidence_brief")
    latest = payload.get("latest_candidate")
    if not isinstance(brief, Mapping):
        brief = {}
    if not isinstance(latest, Mapping):
        latest = {}
    print(
        "candidate_detail "
        f"ticker={ticker} "
        f"status={brief.get('status') or latest.get('priced_in_status') or 'n/a'} "
        f"blocked={str(bool(brief.get('blocked'))).lower()}"
    )
    print(f"why_now={_compact_cli_text(brief.get('why_now'))}")
    print(
        "emotion_vs_reaction="
        f"emotion={brief.get('emotion_score')} "
        f"reaction={brief.get('reaction_score')} "
        f"gap={brief.get('emotion_reaction_gap')} "
        f"priced={brief.get('priced_in_score')}"
    )
    print(f"data={_compact_cli_text(_detail_data_summary(brief))}")
    evidence_route = brief.get("non_company_evidence")
    if isinstance(evidence_route, Mapping):
        print(
            "non_company_evidence="
            f"status={evidence_route.get('status')} "
            f"route={evidence_route.get('route')} "
            f"summary={_compact_cli_text(evidence_route.get('summary'))}"
        )
    usefulness = brief.get("usefulness")
    if isinstance(usefulness, Mapping):
        missing = ",".join(
            str(item)
            for item in _sequence_value(usefulness.get("missing_for_decision"))
            if str(item)
        )
        optional = ",".join(
            str(item)
            for item in _sequence_value(usefulness.get("optional_context_gaps"))
            if str(item)
        )
        missing_suffix = f" missing={missing}" if missing else ""
        optional_suffix = f" optional_context={optional}" if optional else ""
        print(
            "usefulness="
            f"{usefulness.get('status')} "
            f"decision_ready={str(bool(usefulness.get('decision_ready'))).lower()} "
            f"next={_compact_cli_text(usefulness.get('next_action'))}"
            f"{missing_suffix}"
            f"{optional_suffix}"
        )
    source_actions = brief.get("source_actions")
    if isinstance(source_actions, list | tuple) and source_actions:
        printed_header = False
        for action in source_actions:
            if not isinstance(action, Mapping):
                continue
            if str(action.get("status") or "") == "ready":
                continue
            if not printed_header:
                print("source_actions:")
                printed_header = True
            sample = action.get("sample_tickers")
            sample_text = (
                ",".join(str(item) for item in sample)
                if isinstance(sample, list | tuple) and sample
                else ""
            )
            sample_suffix = (
                f" example_tickers={sample_text}" if sample_text else ""
            )
            print(
                "- "
                f"{action.get('source')} "
                f"status={action.get('status')} "
                f"next={_compact_cli_text(action.get('next_action'))}"
                f" command={_compact_cli_text(action.get('command'))}"
                f"{sample_suffix}"
            )
            sample_command = action.get("sample_command")
            if sample_command:
                print(
                    "  sample_command="
                    f"{_compact_cli_text(sample_command)}"
                )
    blockers = brief.get("blockers")
    if isinstance(blockers, list | tuple) and blockers:
        print(f"blockers={','.join(str(item) for item in blockers)}")
    evidence = brief.get("evidence")
    if isinstance(evidence, list | tuple) and evidence:
        print("evidence:")
        for index, item in enumerate(evidence[:5], start=1):
            if isinstance(item, Mapping):
                title = _compact_cli_text(item.get("title"))
                source = _compact_cli_text(item.get("source"))
                print(f"{index}. {title} ({source})")
    print(f"next_step={_compact_cli_text(brief.get('next_step'))}")


def _detail_data_summary(brief: Mapping[str, object]) -> object:
    data_sources = brief.get("data_sources")
    if isinstance(data_sources, Mapping):
        return data_sources.get("summary")
    return None


def _compact_cli_text(value: object) -> str:
    text = str(value or "n/a").strip()
    return " ".join(text.split())


def _print_external_json(payload: Mapping[str, object]) -> int:
    try:
        require_external_export_allowed(payload)
    except ProviderLicenseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(payload, sort_keys=True))
    return 0


class _SafeDisabledLLMClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        del request
        raise RuntimeError("real_llm_provider_disabled")


def _llm_client_for_provider(*, config: AppConfig, fake: bool):
    provider = config.llm_provider.strip().lower()
    if fake or provider == "fake":
        return FakeLLMClient()
    if provider == "openai":
        return OpenAIResponsesClient(api_key=config.openai_api_key)
    return _SafeDisabledLLMClient()


def _build_polygon_ingest(
    *,
    config: AppConfig,
    polygon_command: str,
    date_value: date | None,
    fixture_path: Path | None,
    max_pages: int | None = None,
) -> tuple[PolygonMarketDataConnector, ConnectorRequest, dict[str, object], str]:
    if polygon_command == "grouped-daily":
        if date_value is None:
            msg = "grouped-daily requires --date"
            raise ValueError(msg)
        endpoint = PolygonEndpoint.GROUPED_DAILY
        params = {
            "date": date_value.isoformat(),
            "adjusted": True,
            "include_otc": False,
        }
        first_url = _polygon_grouped_daily_url(
            config=config,
            date_value=date_value,
            api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
        )
        metadata: dict[str, object] = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "date": date_value.isoformat(),
            "fixture": str(fixture_path) if fixture_path is not None else None,
            "availability_policy": config.provider_availability_policy,
        }
    elif polygon_command == "tickers":
        endpoint = PolygonEndpoint.TICKERS
        page_cap = config.polygon_tickers_max_pages if max_pages is None else max_pages
        if page_cap <= 0:
            msg = "max_pages must be greater than zero"
            raise ValueError(msg)
        params = {
            "market": "stocks",
            "active": True,
            "limit": 1000,
            "max_pages": page_cap,
        }
        if date_value is not None:
            params["date"] = date_value.isoformat()
        first_url = _polygon_tickers_url(
            config=config,
            api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
            date_value=date_value,
        )
        metadata = {
            "provider": "polygon",
            "endpoint": endpoint.value,
            "date": date_value.isoformat() if date_value is not None else None,
            "fixture": str(fixture_path) if fixture_path is not None else None,
            "max_pages": page_cap,
            "availability_policy": config.provider_availability_policy,
        }
    else:
        msg = f"unsupported polygon command: {polygon_command}"
        raise ValueError(msg)

    transport = (
        _fixture_transport(
            first_url=first_url,
            fixture_path=fixture_path,
            max_pages=(
                config.polygon_tickers_max_pages if max_pages is None else max_pages
            ),
        )
        if fixture_path is not None
        else UrlLibHttpTransport()
    )
    connector = PolygonMarketDataConnector(
        api_key=_polygon_api_key(config=config, fixture_path=fixture_path),
        client=JsonHttpClient(
            transport=transport,
            timeout_seconds=config.http_timeout_seconds,
        ),
        base_url=config.polygon_base_url,
        availability_policy=config.provider_availability_policy,
        ticker_page_delay_seconds=config.polygon_ticker_page_delay_seconds,
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=endpoint.value,
        params=params,
        requested_at=datetime.now(UTC),
    )
    return connector, request, metadata, endpoint.value


def _fixture_transport(
    *,
    first_url: str,
    fixture_path: Path,
    max_pages: int | None = None,
) -> FakeHttpTransport:
    responses = {first_url: _fixture_response(first_url, fixture_path)}
    payload = _read_fixture_payload(fixture_path)
    next_url = payload.get("next_url")
    current_path = fixture_path
    page_count = 1
    while next_url:
        if max_pages is not None and page_count >= max_pages:
            break
        if not isinstance(next_url, str):
            msg = "polygon fixture next_url must be a string"
            raise ValueError(msg)
        current_path = _next_fixture_path(current_path)
        page_count += 1
        if not current_path.exists():
            msg = f"missing polygon fixture page for {next_url}: {current_path}"
            raise ValueError(msg)
        response_url = _fixture_next_url(str(next_url))
        responses[response_url] = _fixture_response(response_url, current_path)
        payload = _read_fixture_payload(current_path)
        next_url = payload.get("next_url")
    return FakeHttpTransport(responses)


def _fixture_response(url: str, fixture_path: Path) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        url=url,
        headers={"content-type": "application/json"},
        body=fixture_path.read_bytes(),
    )


def _read_fixture_payload(fixture_path: Path) -> dict[str, object]:
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"polygon fixture must contain a JSON object: {fixture_path}"
        raise ValueError(msg)
    return payload


def _next_fixture_path(fixture_path: Path) -> Path:
    prefix, separator, suffix = fixture_path.stem.rpartition("_")
    if suffix.isdigit():
        return fixture_path.with_name(
            f"{prefix}{separator}{int(suffix) + 1}{fixture_path.suffix}"
        )
    return fixture_path.with_name(f"{fixture_path.stem}_2{fixture_path.suffix}")


def _polygon_grouped_daily_url(
    *,
    config: AppConfig,
    date_value: date,
    api_key: str | None,
) -> str:
    query = urlencode(
        {
            "adjusted": "true",
            "include_otc": "false",
            "apiKey": api_key or "",
        }
    )
    base_url = config.polygon_base_url.rstrip("/")
    return f"{base_url}/v2/aggs/grouped/locale/us/market/stocks/{date_value.isoformat()}?{query}"


def _polygon_tickers_url(
    *,
    config: AppConfig,
    api_key: str | None,
    date_value: date | None = None,
) -> str:
    params: dict[str, str] = {
        "market": "stocks",
        "active": "true",
        "limit": "1000",
    }
    if date_value is not None:
        params["date"] = date_value.isoformat()
    params["apiKey"] = api_key or ""
    query = urlencode(params)
    return f"{config.polygon_base_url.rstrip('/')}/v3/reference/tickers?{query}"


def _fixture_next_url(url: str) -> str:
    separator = "&" if "?" in url else "?"
    if "apiKey=" in url:
        return url
    return f"{url}{separator}apiKey=fixture-key"


def _print_provider_result(result: ProviderIngestResult) -> None:
    print(
        f"ingested provider={result.provider} raw={result.raw_count} "
        f"normalized={result.normalized_count} securities={result.security_count} "
        f"daily_bars={result.daily_bar_count} holdings={result.holding_count} "
        f"events={result.event_count} rejected={result.rejected_count}"
    )


def _print_options_provider_result(result: ProviderIngestResult) -> None:
    print(
        f"ingested provider={result.provider} raw={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"option_features={result.option_feature_count} rejected={result.rejected_count}"
    )


def _ipo_s1_analysis_payload(event) -> dict[str, object]:
    event_payload = thaw_json_value(event.payload)
    analysis = event_payload.get("ipo_analysis")
    if not isinstance(analysis, Mapping):
        analysis = {}
    return {
        "ticker": event.ticker,
        "event_id": event.id,
        "event_type": event.event_type.value,
        "source_url": event.source_url,
        "title": event.title,
        "source_ts": event.source_ts.isoformat(),
        "available_at": event.available_at.isoformat(),
        "form_type": event_payload.get("form_type"),
        "filing_date": event_payload.get("filing_date"),
        "accession_number": event_payload.get("accession_number"),
        "document_url": event_payload.get("document_url"),
        "document_text_hash": event_payload.get("document_text_hash"),
        "summary": event_payload.get("summary"),
        "analysis": dict(analysis),
    }


def _build_candidate_packets(
    *,
    packet_repo: CandidatePacketRepository,
    event_repo: EventRepository,
    text_repo: TextRepository,
    feature_repo: FeatureRepository,
    as_of: datetime,
    available_at: datetime,
    ticker: str | Sequence[str] | None,
    states: tuple[ActionState, ...],
):
    if isinstance(ticker, str):
        tickers = [ticker.upper()]
    elif ticker:
        tickers = [str(item).upper() for item in ticker if str(item).strip()]
    else:
        tickers = None
    inputs = packet_repo.list_candidate_inputs(
        as_of=as_of,
        available_at=available_at,
        tickers=tickers,
        states=states,
    )
    packets = []
    for item in inputs:
        candidate_state = item["candidate_state"]
        candidate_ticker = str(candidate_state["ticker"]).upper()
        text_features = text_repo.latest_text_features_by_ticker(
            [candidate_ticker],
            as_of=as_of,
            available_at=available_at,
        )
        option_features = feature_repo.latest_option_features_by_ticker(
            [candidate_ticker],
            as_of=as_of,
            available_at=available_at,
        )
        packet = build_candidate_packet(
            candidate_state=candidate_state,
            signal_features_payload=item["signal_payload"],
            events=event_repo.list_events_for_ticker(
                candidate_ticker,
                as_of=as_of,
                available_at=available_at,
            ),
            snippets=text_repo.list_snippets_for_ticker(
                candidate_ticker,
                as_of=as_of,
                available_at=available_at,
            ),
            text_features=text_features.get(candidate_ticker),
            option_features=option_features.get(candidate_ticker),
            requested_available_at=available_at,
        )
        packet_repo.upsert_candidate_packet(packet)
        packets.append(packet)
    return packets


def _states_at_or_above(min_state: ActionState) -> tuple[ActionState, ...]:
    floor = _state_rank(min_state)
    return tuple(state for state in ActionState if _state_rank(state) >= floor)


def _state_rank(state: ActionState) -> int:
    return {
        ActionState.NO_ACTION: 0,
        ActionState.RESEARCH_ONLY: 1,
        ActionState.ADD_TO_WATCHLIST: 2,
        ActionState.WARNING: 3,
        ActionState.BLOCKED: 3,
        ActionState.THESIS_WEAKENING: 3,
        ActionState.EXIT_INVALIDATE_REVIEW: 3,
        ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW: 4,
    }[state]


def _evidence_summary(items: object) -> str:
    if not isinstance(items, (list, tuple)) or not items:
        return "none"
    item = items[0]
    title = getattr(item, "title", "evidence")
    link = (
        getattr(item, "source_url", None)
        or getattr(item, "source_id", None)
        or getattr(item, "computed_feature_id", None)
        or "unlinked"
    )
    return f"{title} [{link}]"


def _mapping_evidence_summary(items: object) -> str:
    if not isinstance(items, (list, tuple)) or not items or not isinstance(items[0], Mapping):
        return "none"
    item = items[0]
    title = str(item.get("title") or "evidence")
    link = (
        item.get("source_url")
        or item.get("source_id")
        or item.get("computed_feature_id")
        or "unlinked"
    )
    return f"{title} [{link}]"


def _universe_tickers_for_scan(
    *,
    provider_repo: ProviderRepository,
    universe_name: str | None,
    as_of: date,
    available_at: datetime,
) -> set[str] | None:
    if universe_name is None:
        return None
    snapshot = _universe_snapshot_for_scan(
        provider_repo=provider_repo,
        universe_name=universe_name,
        as_of=as_of,
        available_at=available_at,
    )
    if snapshot is None:
        return None
    return {row.ticker for row in provider_repo.list_universe_member_rows(snapshot.id)}


def _universe_snapshot_for_scan(
    *,
    provider_repo: ProviderRepository,
    universe_name: str,
    as_of: date,
    available_at: datetime,
):
    as_of_dt = _scan_timestamp(as_of)
    return provider_repo.latest_universe_snapshot(
        name=universe_name,
        as_of=as_of_dt,
        available_at=available_at,
    )


def _polygon_api_key(*, config: AppConfig, fixture_path: Path | None) -> str | None:
    if fixture_path is not None:
        return "fixture-key"
    return config.polygon_api_key if config.polygon_api_key_configured else None


def _scan_timestamp(value: date) -> datetime:
    return datetime.combine(value, time(21), tzinfo=UTC)


def _parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = "--available-at must include timezone information"
        raise argparse.ArgumentTypeError(msg)
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
