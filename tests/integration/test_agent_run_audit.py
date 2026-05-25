from __future__ import annotations

from catalyst_radar.agents.run_audit import (
    load_agent_run_audit,
    record_agent_run_audit,
)
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_real_agent_run_audit_records_model_calls_and_snapshot_hash(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'agent-audit.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    create_schema(engine_from_url(database_url))

    run_id = record_agent_run_audit(
        database_url=database_url,
        mode="real",
        model="gpt-5.1",
        snapshot_hash="sha256:test",
        external_calls_planned={"openai": 3, "market_data": 0, "broker": 0},
        external_calls_made={"openai": 2, "market_data": 0, "broker": 0},
        token_usage={"input_tokens": 100, "cached_input_tokens": 0, "output_tokens": 20},
        status="completed",
        final_output_summary="Reviewed two real scan rows.",
        safety_verdict="passed",
    )

    row = load_agent_run_audit(database_url, run_id)

    assert row["snapshot_hash"] == "sha256:test"
    assert row["external_calls_planned"]["openai"] == 3
    assert row["external_calls_made"]["openai"] == 2
    assert row["token_usage"]["output_tokens"] == 20
    assert row["status"] == "completed"
    assert row["safety_verdict"] == "passed"
