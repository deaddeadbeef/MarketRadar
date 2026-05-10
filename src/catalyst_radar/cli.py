from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv
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
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.alerts.channels.base import DryRunAlertChannel
from catalyst_radar.alerts.digest import build_alert_digest, digest_payload
from catalyst_radar.alerts.models import AlertStatus
from catalyst_radar.alerts.planner import plan_alerts
from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    HttpTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.options import OptionsAggregateConnector
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.decision_cards.builder import build_decision_card
from catalyst_radar.feedback.service import (
    FeedbackError,
    MissingArtifactError,
    record_feedback,
)
from catalyst_radar.pipeline.candidate_packet import build_candidate_packet
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.alert_repositories import AlertRepository
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

    subparsers.add_parser("init-db")

    ingest = subparsers.add_parser("ingest-csv")
    ingest.add_argument("--securities", type=Path, required=True)
    ingest.add_argument("--daily-bars", type=Path, required=True)
    ingest.add_argument("--holdings", type=Path)

    polygon = subparsers.add_parser("ingest-polygon")
    polygon_sub = polygon.add_subparsers(dest="polygon_command", required=True)
    grouped = polygon_sub.add_parser("grouped-daily")
    grouped.add_argument("--date", type=date.fromisoformat, required=True)
    grouped.add_argument("--fixture", type=Path)
    tickers = polygon_sub.add_parser("tickers")
    tickers.add_argument("--fixture", type=Path)
    tickers.add_argument("--date", type=date.fromisoformat)

    sec = subparsers.add_parser("ingest-sec")
    sec_sub = sec.add_subparsers(dest="sec_command", required=True)
    submissions = sec_sub.add_parser("submissions")
    submissions.add_argument("--ticker", required=True)
    submissions.add_argument("--cik", required=True)
    submissions.add_argument("--fixture", type=Path)

    news = subparsers.add_parser("ingest-news")
    news.add_argument("--fixture", type=Path, required=True)

    earnings = subparsers.add_parser("ingest-earnings")
    earnings.add_argument("--fixture", type=Path, required=True)

    options = subparsers.add_parser("ingest-options")
    options.add_argument("--fixture", type=Path, required=True)

    events = subparsers.add_parser("events")
    events.add_argument("--ticker", required=True)
    events.add_argument("--as-of", type=date.fromisoformat, required=True)
    events.add_argument("--available-at", type=_parse_aware_datetime)
    events.add_argument("--limit", type=int, default=20)

    run_textint = subparsers.add_parser("run-textint")
    run_textint.add_argument("--as-of", type=date.fromisoformat, required=True)
    run_textint.add_argument("--available-at", type=_parse_aware_datetime)
    run_textint.add_argument("--ontology", type=Path, default=Path("config/themes.yaml"))
    run_textint.add_argument("--ticker")

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
    build_packets.add_argument("--ticker")
    build_packets.add_argument(
        "--min-state",
        choices=[state.value for state in ActionState],
        default=ActionState.WARNING.value,
    )

    build_cards = subparsers.add_parser("build-decision-cards")
    build_cards.add_argument("--as-of", type=date.fromisoformat, required=True)
    build_cards.add_argument("--available-at", type=_parse_aware_datetime)
    build_cards.add_argument("--ticker")

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

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(".env.local")
    args = build_parser().parse_args(argv)
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)

    if args.command == "init-db":
        create_schema(engine)
        print("initialized database")
        return 0

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
        )

    if args.command == "ingest-sec":
        create_schema(engine)
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        event_repo = EventRepository(engine)
        return _ingest_sec_provider(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            sec_command=args.sec_command,
            ticker=args.ticker,
            cik=args.cik,
            fixture_path=args.fixture,
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
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        feature_repo = FeatureRepository(engine)
        return _ingest_options_provider(
            market_repo=market_repo,
            provider_repo=provider_repo,
            feature_repo=feature_repo,
            fixture_path=args.fixture,
        )

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

    if args.command == "provider-health":
        create_schema(engine)
        provider_repo = ProviderRepository(engine)
        health = provider_repo.latest_health(args.provider)
        if health is None:
            print(f"provider={args.provider} status=unknown")
            return 1
        print(f"provider={health.provider} status={health.status.value}")
        return 0

    if args.command == "run-textint":
        create_schema(engine)
        event_repo = EventRepository(engine)
        text_repo = TextRepository(engine)
        available_at = args.available_at or datetime.now(UTC)
        tickers = [args.ticker] if args.ticker else None
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
                states=_states_at_or_above(ActionState.WARNING),
            )
            cards = []
            for packet in packets:
                card = build_decision_card(packet, available_at=available_at)
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
            print(json.dumps(thaw_json_value(packet.payload), sort_keys=True))
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
            print(json.dumps(thaw_json_value(card.payload), sort_keys=True))
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
        entry_at = args.entry_at or (args.available_at if args.entry_price is not None else None)
        trade = create_paper_trade_from_card(
            card,
            PaperDecision(args.decision),
            available_at=args.available_at,
            entry_price=args.entry_price,
            entry_at=entry_at,
        )
        validation_repo.upsert_paper_trade(trade)
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
            provider=args.provider or config.market_provider,
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
) -> int:
    try:
        connector, request, metadata, job_type = _build_polygon_ingest(
            config=config,
            polygon_command=polygon_command,
            date_value=date_value,
            fixture_path=fixture_path,
        )
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
) -> int:
    if sec_command != "submissions":
        print(f"sec ingest failed: unsupported sec command: {sec_command}", file=sys.stderr)
        return 1
    if fixture_path is None and not config.sec_enable_live:
        print(
            "sec ingest failed: live SEC ingest requires CATALYST_SEC_ENABLE_LIVE=1",
            file=sys.stderr,
        )
        return 1
    if fixture_path is None and not config.sec_user_agent:
        print(
            "sec ingest failed: CATALYST_SEC_USER_AGENT is required for live SEC ingest",
            file=sys.stderr,
        )
        return 1

    transport: HttpTransport | None = None
    if fixture_path is None:
        transport = _HeaderInjectingTransport(
            UrlLibHttpTransport(),
            {"User-Agent": config.sec_user_agent or ""},
        )
    connector = SecSubmissionsConnector(
        fixture_path=fixture_path,
        client=(
            JsonHttpClient(
                transport=transport,
                timeout_seconds=config.http_timeout_seconds,
            )
            if transport is not None
            else None
        ),
        base_url=config.sec_base_url,
    )
    metadata = {
        "provider": "sec",
        "endpoint": "submissions",
        "ticker": ticker.upper(),
        "cik": cik,
        "fixture": str(fixture_path) if fixture_path is not None else None,
        "live": fixture_path is None,
    }
    request = ConnectorRequest(
        provider="sec",
        endpoint="submissions",
        params={"ticker": ticker.upper(), "cik": cik},
        requested_at=datetime.now(UTC),
    )
    try:
        result = ingest_provider_records(
            connector=connector,
            request=request,
            market_repo=market_repo,
            provider_repo=provider_repo,
            job_type="sec_submissions",
            metadata=metadata,
            event_repo=event_repo,
        )
    except ProviderIngestError as exc:
        print(f"sec ingest failed: {exc}", file=sys.stderr)
        return 1

    _print_provider_result(result)
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
            "error": result.error,
            "payload": thaw_json_value(result.payload) if result.payload is not None else None,
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
        "payload": thaw_json_value(entry.payload),
    }


class _SafeDisabledLLMClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        del request
        raise RuntimeError("real_llm_provider_disabled")


def _llm_client_for_provider(*, config: AppConfig, fake: bool):
    provider = config.llm_provider.strip().lower()
    if fake or provider == "fake":
        return FakeLLMClient()
    if provider == "openai":
        return OpenAIResponsesClient()
    return _SafeDisabledLLMClient()


def _build_polygon_ingest(
    *,
    config: AppConfig,
    polygon_command: str,
    date_value: date | None,
    fixture_path: Path | None,
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
        params = {"market": "stocks", "active": True, "limit": 1000}
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
            "availability_policy": config.provider_availability_policy,
        }
    else:
        msg = f"unsupported polygon command: {polygon_command}"
        raise ValueError(msg)

    transport = (
        _fixture_transport(first_url=first_url, fixture_path=fixture_path)
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
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=endpoint.value,
        params=params,
        requested_at=datetime.now(UTC),
    )
    return connector, request, metadata, endpoint.value


def _fixture_transport(*, first_url: str, fixture_path: Path) -> FakeHttpTransport:
    responses = {first_url: _fixture_response(first_url, fixture_path)}
    payload = _read_fixture_payload(fixture_path)
    next_url = payload.get("next_url")
    current_path = fixture_path
    while next_url:
        if not isinstance(next_url, str):
            msg = "polygon fixture next_url must be a string"
            raise ValueError(msg)
        current_path = _next_fixture_path(current_path)
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


class _HeaderInjectingTransport:
    def __init__(
        self,
        transport: HttpTransport,
        headers: Mapping[str, str],
    ) -> None:
        self.transport = transport
        self.headers = dict(headers)

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        merged_headers: dict[str, str] = {**self.headers, **dict(headers)}
        return self.transport.get(
            url,
            headers=merged_headers,
            timeout_seconds=timeout_seconds,
        )


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


def _build_candidate_packets(
    *,
    packet_repo: CandidatePacketRepository,
    event_repo: EventRepository,
    text_repo: TextRepository,
    feature_repo: FeatureRepository,
    as_of: datetime,
    available_at: datetime,
    ticker: str | None,
    states: tuple[ActionState, ...],
):
    tickers = [ticker.upper()] if ticker else None
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
    return config.polygon_api_key


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
