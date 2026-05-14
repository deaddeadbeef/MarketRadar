from __future__ import annotations

from fastapi import FastAPI

from catalyst_radar.api.routes.agents import router as agents_router
from catalyst_radar.api.routes.alerts import router as alerts_router
from catalyst_radar.api.routes.brokers import router as brokers_router
from catalyst_radar.api.routes.costs import router as costs_router
from catalyst_radar.api.routes.feedback import router as feedback_router
from catalyst_radar.api.routes.ops import router as ops_router
from catalyst_radar.api.routes.radar import router as radar_router
from catalyst_radar.core.runtime import APP_VERSION, SERVICE_NAME, build_info
from catalyst_radar.security.secrets import load_app_dotenv


def create_app() -> FastAPI:
    load_app_dotenv()

    app = FastAPI(
        title="Catalyst Radar API",
        version=APP_VERSION,
        description="Decision-support API for reviewing market radar candidates.",
    )

    @app.get("/api/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "service": SERVICE_NAME, "build": build_info()}

    app.include_router(radar_router)
    app.include_router(ops_router)
    app.include_router(costs_router)
    app.include_router(feedback_router)
    app.include_router(alerts_router)
    app.include_router(brokers_router)
    app.include_router(agents_router)

    return app


app = create_app()
