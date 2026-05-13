from __future__ import annotations

from fastapi import FastAPI

from catalyst_radar.api.routes.alerts import router as alerts_router
from catalyst_radar.api.routes.brokers import router as brokers_router
from catalyst_radar.api.routes.costs import router as costs_router
from catalyst_radar.api.routes.feedback import router as feedback_router
from catalyst_radar.api.routes.ops import router as ops_router
from catalyst_radar.api.routes.radar import router as radar_router
from catalyst_radar.security.secrets import load_app_dotenv


def create_app() -> FastAPI:
    load_app_dotenv()

    app = FastAPI(
        title="Catalyst Radar API",
        version="0.1.0",
        description="Decision-support API for reviewing market radar candidates.",
    )

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "catalyst-radar"}

    app.include_router(radar_router)
    app.include_router(ops_router)
    app.include_router(costs_router)
    app.include_router(feedback_router)
    app.include_router(alerts_router)
    app.include_router(brokers_router)

    return app


app = create_app()
