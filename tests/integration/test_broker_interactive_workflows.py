from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    create_trigger,
    evaluate_triggers,
    record_opportunity_action,
    sync_market_context,
)
from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
)
from catalyst_radar.brokers.schwab import SchwabClient
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_interactive_market_context_triggers_actions_and_blocked_ticket(
    tmp_path: Path,
) -> None:
    engine = engine_from_url(f"sqlite:///{(tmp_path / 'interactive.db').as_posix()}")
    create_schema(engine)
    repo = BrokerRepository(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    _seed_account(repo, now)
    base_url = "https://api.schwabapi.com"
    chain_url = f"{base_url}/marketdata/v1/chains?symbol=GLW&contractType=ALL&strategy=SINGLE"
    client = SchwabClient(
        client=JsonHttpClient(
            FakeHttpTransport(
                {
                    f"{base_url}/marketdata/v1/quotes?symbols=GLW&indicative=false": (
                        _response(
                            b'{"GLW":{"quote":{"lastPrice":12.5,"bidPrice":12.45,'
                            b'"askPrice":12.55,"totalVolume":2000000,'
                            b'"netPercentChange":3.2,"52WeekHigh":20.0,'
                            b'"52WeekLow":8.0}}}'
                        )
                    ),
                    (
                        f"{base_url}/marketdata/v1/pricehistory?symbol=GLW&"
                        "periodType=day&period=10&frequencyType=minute&frequency=5&"
                        "needExtendedHoursData=true&needPreviousClose=true"
                    ): _response(
                        b'{"candles":[{"close":10.0,"volume":1000000},'
                        b'{"close":12.5,"volume":1200000}]}'
                    ),
                    chain_url: _response(
                        b'{"callExpDateMap":{"2026-06-19:38":{"12.5":['
                        b'{"totalVolume":900,"volatility":42.0}]}},'
                        b'"putExpDateMap":{"2026-06-19:38":{"10.0":['
                        b'{"totalVolume":300,"volatility":39.0}]}}}'
                    ),
                }
            ),
            timeout_seconds=5.0,
        ),
        access_token="fake-access-token",
        base_url=base_url,
    )

    snapshots = sync_market_context(client=client, repo=repo, tickers=["GLW"], now=now)
    action = record_opportunity_action(
        repo=repo,
        ticker="GLW",
        action="watch",
        thesis="early breakout with rising volume apikey=hidden-action-secret",
        now=now,
        actor_source="dashboard",
        actor_id="local-dashboard",
        actor_role="analyst",
    )
    trigger = create_trigger(
        repo=repo,
        ticker="GLW",
        trigger_type="price_above",
        operator="gte",
        threshold=12.0,
        now=now,
        actor_source="dashboard",
        actor_id="local-dashboard",
        actor_role="analyst",
    )
    evaluated = evaluate_triggers(
        repo=repo,
        tickers=["GLW"],
        now=now,
        actor_source="dashboard",
        actor_id="local-dashboard",
        actor_role="analyst",
    )
    ticket = create_blocked_order_ticket(
        repo=repo,
        ticker="GLW",
        side="buy",
        entry_price=12.5,
        invalidation_price=11.5,
        config=AppConfig(portfolio_value=100000.0),
        notes="operator preview apikey=hidden-ticket-secret",
        now=now,
        actor_source="dashboard",
        actor_id="local-dashboard",
        actor_role="analyst",
    )

    assert snapshots[0].last_price == 12.5
    assert snapshots[0].price_trend_5d_percent == 25.0
    assert snapshots[0].option_call_put_ratio == 3.0
    assert action.status == "active"
    assert trigger.status.value == "active"
    assert evaluated[0].status.value == "fired"
    assert repo.latest_market_snapshot("GLW").last_price == 12.5
    assert repo.list_opportunity_actions(ticker="GLW")[0].thesis == (
        "early breakout with rising volume apikey=hidden-action-secret"
    )
    assert repo.list_triggers(ticker="GLW")[0].status.value == "fired"
    assert ticket.submission_allowed is False
    assert ticket.status.value == "blocked"
    assert "broker_submission_disabled" in ticket.preview_payload["hard_blocks"]
    events = AuditLogRepository(engine).list_events(ticker="GLW")
    assert [event.event_type for event in events] == [
        "telemetry.operator.opportunity_action.saved",
        "telemetry.operator.trigger.saved",
        "telemetry.operator.triggers.evaluated",
        "telemetry.operator.order_ticket.preview_saved",
    ]
    assert {event.actor_source for event in events} == {"dashboard"}
    assert {event.actor_role for event in events} == {"analyst"}
    assert events[0].artifact_type == "opportunity_action"
    assert events[0].artifact_id == action.id
    assert events[0].decision == "watch"
    assert "hidden-action-secret" not in str(events[0].after_payload)
    assert events[2].metadata["fired_count"] == 1
    assert events[3].artifact_type == "order_ticket"
    assert events[3].artifact_id == ticket.id
    assert events[3].metadata["submission_allowed"] is False
    assert "hidden-ticket-secret" not in str(events[3].after_payload)


def _seed_account(repo: BrokerRepository, now: datetime) -> None:
    connection_id = broker_connection_id()
    account_id = broker_account_id("schwab", "account-hash-123")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            last_successful_sync_at=now,
            metadata={},
        )
    )
    repo.upsert_accounts(
        [
            BrokerAccount(
                id=account_id,
                connection_id=connection_id,
                broker="schwab",
                broker_account_id="12345678",
                account_hash="account-hash-123",
                account_type="MARGIN",
                display_name="MARGIN ending 5678",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        ]
    )
    repo.upsert_balance_snapshots(
        [
            BrokerBalanceSnapshot(
                id=broker_balance_snapshot_id(account_id, now),
                account_id=account_id,
                as_of=now,
                cash=50000.0,
                buying_power=100000.0,
                liquidation_value=250000.0,
                equity=250000.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )


def _response(body: bytes) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        url="fixture://schwab-market",
        headers={"content-type": "application/json"},
        body=body,
    )
