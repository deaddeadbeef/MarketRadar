from __future__ import annotations

import sys

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is optional for worker startup.
    load_dotenv = None

from catalyst_radar.core.config import AppConfig
from catalyst_radar.jobs.scheduler import SchedulerConfig, SchedulerRunResult, run_forever, run_once
from catalyst_radar.storage.db import create_schema, engine_from_url


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(".env.local")

    app_config = AppConfig.from_env()
    engine = engine_from_url(app_config.database_url)
    create_schema(engine)

    try:
        scheduler_config = SchedulerConfig.from_env()
    except ValueError as exc:
        print(f"worker config error: {exc}", file=sys.stderr)
        return 2
    if scheduler_config.run_interval.total_seconds() <= 0:
        result = run_once(engine=engine, config=scheduler_config)
        return _exit_code(result)

    run_forever(engine=engine, config=scheduler_config)
    return 0


def _exit_code(result: SchedulerRunResult) -> int:
    if result.reason == "lock_held":
        return 0
    if result.reason is not None:
        return 1
    if result.daily_result is None:
        return 0
    return 0 if result.daily_result.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
