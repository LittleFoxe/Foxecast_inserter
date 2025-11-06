import asyncio
from fastapi import FastAPI
from src.controllers.http import router as http_router
from src.metrics.metrics import setup_metrics, metrics_router
from src.infrastructure.rabbit_consumer import run_consumer


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

    # Start RabbitMQ consumer in background so it does not block HTTP server
    # Keep a reference in app.state to manage lifecycle on shutdown
    async def on_startup() -> None:
        app.state.rabbit_consumer_task = asyncio.create_task(run_consumer())

    async def on_shutdown() -> None:
        task = getattr(app.state, "rabbit_consumer_task", None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                # Expected during shutdown; consumer should close connection in its finally block
                pass

    app.add_event_handler("startup", on_startup)
    app.add_event_handler("shutdown", on_shutdown)

    return app


app = create_app()
