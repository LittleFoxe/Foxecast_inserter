import asyncio
from fastapi import FastAPI

from src.infrastructure.service_provider import get_broker_consumer
from src.controllers.http import router as http_router
from src.metrics.metrics import setup_metrics, metrics_router


async def on_startup() -> None:
    """Configure the app and run asynchronous tasks with the main program"""
    app.state.consumer_task = asyncio.create_task(get_broker_consumer())

async def on_shutdown() -> None:
    """Clean up the app and close asynchronous connections"""
    task = getattr(app.state, "consumer_task", None)
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # Expected during shutdown; consumer should close connection in its finally block
            pass

def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.
    """
    app = FastAPI(title="Forecast Inserter", version="1.0.0")

    # Health and insert endpoints
    app.include_router(http_router)

    # Prometheus metrics endpoint
    setup_metrics(app)
    app.include_router(metrics_router)

    # Hook up asynchronous tasks to run with the main program
    app.add_event_handler("startup", on_startup)
    app.add_event_handler("shutdown", on_shutdown)

    return app


app = create_app()
