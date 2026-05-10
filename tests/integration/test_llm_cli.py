from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert, select

from catalyst_radar.agents.models import TokenUsage
from catalyst_radar.agents.router import LLMClientRequest, LLMClientResult
from catalyst_radar.cli import main
from catalyst_radar.storage.schema import budget_ledger, candidate_packets

AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)
AS_OF_TEXT = "2026-05-08"
SOURCE_TS = datetime(2026, 5, 8, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 14, tzinfo=UTC)
AVAILABLE_AT_TEXT = "2026-05-10T14:00:00Z"
HISTORICAL_AVAILABLE_AT = datetime(2026, 5, 9, 0, tzinfo=UTC)
HISTORICAL_AVAILABLE_AT_TEXT = "2026-05-09T00:00:00Z"
ATTEMPTED_AT = datetime(2026, 5, 10, 20, tzinfo=UTC)


def test_llm_budget_status_reports_zero_without_ledger_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)

    assert (
        main(["llm-budget-status", "--available-at", AVAILABLE_AT_TEXT])
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == (
        "llm_budget_status actual_cost=0.000000 estimated_cost=0.000000 "
        "attempts=0 skipped=0 completed=0 source=budget_ledger\n"
    )
    assert _ledger_rows(database_url) == []


def test_run_llm_review_requires_candidate_packet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)

    exit_code = main(
        [
            "run-llm-review",
            "--ticker",
            "MSFT",
            "--as-of",
            AS_OF_TEXT,
            "--available-at",
            AVAILABLE_AT_TEXT,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "candidate packet not found: MSFT\n"
    rows = _ledger_rows(database_url)
    assert [
        (
            row.status,
            row.skip_reason,
            row.ticker,
            row.candidate_packet_id,
            row.task,
            row.schema_version,
        )
        for row in rows
    ] == [
        (
            "skipped",
            "candidate_packet_missing",
            "MSFT",
            None,
            "mid_review",
            "evidence-review-v1",
        )
    ]


def test_run_llm_review_dry_run_logs_dry_run_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_fake_safe_llm(monkeypatch)
    _seed_candidate_packet(database_url)

    assert (
        main(
            [
                "run-llm-review",
                "--ticker",
                "MSFT",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--dry-run",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "llm_review ticker=MSFT task=mid_review status=dry_run" in captured.out
    rows = _ledger_rows(database_url)
    assert [(row.status, row.skip_reason, row.model) for row in rows] == [
        ("dry_run", None, "fake")
    ]


def test_run_llm_review_uses_attempt_time_for_ledger_ts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_fake_safe_llm(monkeypatch)
    _seed_candidate_packet(database_url, available_at=HISTORICAL_AVAILABLE_AT)

    assert (
        main(
            [
                "run-llm-review",
                "--ticker",
                "MSFT",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                HISTORICAL_AVAILABLE_AT_TEXT,
                "--dry-run",
            ]
        )
        == 0
    )

    rows = _ledger_rows(database_url)
    assert len(rows) == 1
    assert _as_utc(rows[0].available_at) == HISTORICAL_AVAILABLE_AT
    assert _as_utc(rows[0].ts) == ATTEMPTED_AT


def test_run_llm_review_fake_client_logs_completed_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_fake_safe_llm(monkeypatch)
    _seed_candidate_packet(database_url)

    assert (
        main(
            [
                "run-llm-review",
                "--ticker",
                "MSFT",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                AVAILABLE_AT_TEXT,
                "--fake",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "llm_review ticker=MSFT task=mid_review status=completed" in captured.out
    rows = _ledger_rows(database_url)
    assert [(row.status, row.skip_reason, row.model, row.provider) for row in rows] == [
        ("completed", None, "fake", "fake")
    ]


def test_run_llm_review_repeated_attempts_append_distinct_ledger_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_fake_safe_llm(monkeypatch)
    _seed_candidate_packet(database_url)

    command = [
        "run-llm-review",
        "--ticker",
        "MSFT",
        "--as-of",
        AS_OF_TEXT,
        "--available-at",
        AVAILABLE_AT_TEXT,
        "--fake",
    ]
    assert main(command) == 0
    capsys.readouterr()
    assert main(command) == 0

    captured = capsys.readouterr()
    assert "llm_review ticker=MSFT task=mid_review status=completed" in captured.out
    rows = _ledger_rows(database_url)
    assert [row.status for row in rows] == ["completed", "completed"]
    assert len({row.id for row in rows}) == 2


def test_run_llm_review_default_premium_disabled_logs_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "false")
    _seed_candidate_packet(database_url)

    assert (
        main(
            [
                "run-llm-review",
                "--ticker",
                "MSFT",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "status=skipped reason=premium_llm_disabled" in captured.out
    rows = _ledger_rows(database_url)
    assert [(row.status, row.skip_reason) for row in rows] == [
        ("skipped", "premium_llm_disabled")
    ]


def test_run_llm_review_enabled_without_fake_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_enabled_openai_llm(monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "")
    _seed_candidate_packet(database_url)

    exit_code = main(
        [
            "run-llm-review",
            "--ticker",
            "MSFT",
            "--as-of",
            AS_OF_TEXT,
            "--available-at",
            AVAILABLE_AT_TEXT,
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["result"]["status"] == "failed"
    assert payload["ledger"]["skip_reason"] == "client_error"
    assert payload["ledger"]["provider"] == "openai"
    assert payload["ledger"]["payload"]["error"] == "openai_api_key_missing"
    rows = _ledger_rows(database_url)
    assert [(row.status, row.skip_reason, row.provider, row.payload) for row in rows] == [
        (
            "failed",
            "client_error",
            "openai",
            {"error": "openai_api_key_missing"},
        )
    ]


def test_run_llm_review_openai_provider_can_be_monkeypatched_without_fake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    _configure_enabled_openai_llm(monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "")
    _seed_candidate_packet(database_url)
    calls = []

    class FakeOpenAIResponsesClient:
        def complete(self, request: LLMClientRequest) -> LLMClientResult:
            calls.append(request)
            return LLMClientResult(
                payload={
                    "ticker": request.candidate.ticker,
                    "as_of": request.candidate.as_of.isoformat(),
                    "claims": [
                        {
                            "claim": "MSFT reported a material product catalyst.",
                            "source_id": "event-msft",
                            "source_quality": 0.9,
                            "evidence_type": "news",
                            "sentiment": 0.81,
                            "confidence": 0.81,
                            "uncertainty_notes": "Source-linked fake OpenAI response.",
                        }
                    ],
                    "bear_case": [],
                    "unresolved_conflicts": [],
                    "recommended_policy_downgrade": False,
                },
                token_usage=TokenUsage(input_tokens=10, output_tokens=20),
                model=request.model,
                provider="openai",
            )

    monkeypatch.setattr(
        "catalyst_radar.cli.OpenAIResponsesClient",
        FakeOpenAIResponsesClient,
    )

    exit_code = main(
        [
            "run-llm-review",
            "--ticker",
            "MSFT",
            "--as-of",
            AS_OF_TEXT,
            "--available-at",
            AVAILABLE_AT_TEXT,
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert len(calls) == 1
    assert payload["result"]["status"] == "completed"
    assert payload["ledger"]["provider"] == "openai"
    rows = _ledger_rows(database_url)
    assert [(row.status, row.skip_reason, row.provider) for row in rows] == [
        ("completed", None, "openai")
    ]


def test_llm_budget_status_json_includes_caps_and_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _init_db(tmp_path, monkeypatch, capsys)
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "false")
    monkeypatch.setenv("CATALYST_LLM_DAILY_BUDGET_USD", "7.5")
    monkeypatch.setenv("CATALYST_LLM_MONTHLY_BUDGET_USD", "55")
    monkeypatch.setenv("CATALYST_LLM_TASK_DAILY_CAPS", "mid_review=3")
    _seed_candidate_packet(database_url)

    assert (
        main(
            [
                "run-llm-review",
                "--ticker",
                "MSFT",
                "--as-of",
                AS_OF_TEXT,
                "--available-at",
                AVAILABLE_AT_TEXT,
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        main(["llm-budget-status", "--available-at", AVAILABLE_AT_TEXT, "--json"])
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "budget_ledger"
    assert payload["caps"]["daily_budget_usd"] == 7.5
    assert payload["caps"]["monthly_budget_usd"] == 55.0
    assert payload["caps"]["task_daily_caps"] == {"mid_review": 3}
    assert payload["summary"]["attempt_count"] == 1
    assert payload["summary"]["status_counts"] == {"skipped": 1}
    assert payload["summary"]["rows"][0]["skip_reason"] == "premium_llm_disabled"


def _init_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    database_url = f"sqlite:///{(tmp_path / 'llm-cli.db').as_posix()}"
    monkeypatch.setattr("catalyst_radar.cli._now_utc", lambda: ATTEMPTED_AT)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    return database_url


def _configure_fake_safe_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "true")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "fake")
    monkeypatch.setenv("CATALYST_LLM_EVIDENCE_MODEL", "fake")
    _configure_enabled_llm_budget(monkeypatch)


def _configure_enabled_openai_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "true")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "openai")
    monkeypatch.setenv("CATALYST_LLM_EVIDENCE_MODEL", "dummy-model")
    _configure_enabled_llm_budget(monkeypatch)


def _configure_enabled_llm_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CATALYST_LLM_INPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_CACHED_INPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_OUTPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_PRICING_UPDATED_AT", "2026-05-10")
    monkeypatch.setenv("CATALYST_LLM_DAILY_BUDGET_USD", "1")
    monkeypatch.setenv("CATALYST_LLM_MONTHLY_BUDGET_USD", "10")


def _seed_candidate_packet(
    database_url: str,
    *,
    available_at: datetime = AVAILABLE_AT,
) -> None:
    engine = create_engine(database_url, future=True)
    with engine.begin() as conn:
        conn.execute(insert(candidate_packets).values(**_packet(available_at=available_at)))


def _ledger_rows(database_url: str):
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        return list(
            conn.execute(select(budget_ledger).order_by(budget_ledger.c.created_at))
        )


def _as_utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _packet(*, available_at: datetime = AVAILABLE_AT) -> dict[str, object]:
    return {
        "id": "packet-msft",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "candidate_state_id": "state-msft",
        "state": "Warning",
        "final_score": 82.0,
        "schema_version": "candidate-packet-v1",
        "source_ts": SOURCE_TS,
        "available_at": available_at,
        "payload": {
            "supporting_evidence": [
                {
                    "kind": "news",
                    "title": "MSFT evidence update",
                    "summary": "MSFT reported a material product catalyst.",
                    "polarity": "supporting",
                    "strength": 0.81,
                    "source_id": "event-msft",
                    "source_quality": 0.9,
                    "source_ts": SOURCE_TS.isoformat(),
                    "available_at": available_at.isoformat(),
                }
            ],
            "disconfirming_evidence": [
                {
                    "kind": "risk",
                    "title": "MSFT valuation risk",
                    "summary": "Valuation remains extended versus recent growth.",
                    "polarity": "disconfirming",
                    "strength": 0.42,
                    "computed_feature_id": "risk-msft",
                    "source_quality": 0.7,
                    "source_ts": SOURCE_TS.isoformat(),
                    "available_at": available_at.isoformat(),
                }
            ],
            "conflicts": [],
            "hard_blocks": [],
            "trade_plan": {
                "entry_zone": [100.0, 104.0],
                "invalidation_price": 94.0,
            },
        },
        "created_at": available_at,
    }
