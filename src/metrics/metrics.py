from fastapi import APIRouter, FastAPI
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response


metrics_router = APIRouter()

# Core metrics requested
file_download_seconds = Histogram(
    "file_download_seconds", "Time spent downloading a file in seconds"
)
file_size_bytes = Gauge("file_size_bytes", "Size of the processed file in bytes")
network_bytes_total = Counter(
    "network_bytes_total", "Total network bytes downloaded by the service"
)
parse_seconds = Histogram("parse_seconds", "Time spent parsing a file in seconds")
db_insert_seconds = Histogram("db_insert_seconds", "Time spent inserting batch into DB in seconds")


@metrics_router.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def setup_metrics(app: FastAPI) -> None:
    """
    Basic middleware for setting up metrics.
    Currently it is just an empty middleware, because metrics are sending directly from endpoints
    """
    # Currently no middleware required as the endpoints will update metrics directly
    _ = app


