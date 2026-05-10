from __future__ import annotations

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is optional for worker startup.
    load_dotenv = None

from catalyst_radar.core.config import AppConfig
from catalyst_radar.jobs.scheduler import SchedulerConfig, run_forever, run_once
from catalyst_radar.storage.db import create_schema, engine_from_url


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(".env.local")

    app_config = AppConfig.from_env()
    engine = engine_from_url(app_config.database_url)
    create_schema(engine)

    scheduler_config = SchedulerConfig.from_env()
    if scheduler_config.run_interval.total_seconds() <= 0:
        result = run_once(engine=engine, config=scheduler_config)
        return 0 if result.reason in {None, "lock_held"} else 1

    run_forever(engine=engine, config=scheduler_config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
