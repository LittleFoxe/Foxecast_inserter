from fastapi import FastAPI
from src.controllers.http import router as http_router
from src.metrics.metrics import setup_metrics, metrics_router


def create_app() -> FastAPI:
    """Create and configure FastAPI application.

    All comments and OpenAPI descriptions are in English as required.
    """
    app = FastAPI(title="Forecast Inserter", version="0.1.0")

    # Health and insert endpoints
    app.include_router(http_router)

    # Prometheus metrics endpoint
    setup_metrics(app)
    app.include_router(metrics_router)

    return app


app = create_app()


