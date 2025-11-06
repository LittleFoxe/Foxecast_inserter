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

def update_all_metrics(
        download_ms: int,
        parse_ms: int,
        db_ms: int,
        file_size: int):
    """
    Update all core metrics with the provided performance and size data.
    
    This method updates the following metrics:
    - file_download_seconds: Converts download time to seconds and records it
    - parse_seconds: Converts parsing time to seconds and records it  
    - db_insert_seconds: Converts database insertion time to seconds and records it
    - file_size_bytes: Sets the current file size in bytes
    - network_bytes_total: Increments the total network traffic by the file size
    
    Args:
        download_ms (int): Time taken to download the file in milliseconds
        parse_ms (int): Time taken to parse the file content in milliseconds
        db_ms (int): Time taken to insert data into the database in milliseconds
        file_size (int): Size of the processed file in bytes
    """
    file_download_seconds.observe(download_ms / 1000.0)
    parse_seconds.observe(parse_ms / 1000.0)
    db_insert_seconds.observe(db_ms / 1000.0)
    file_size_bytes.set(file_size)
    network_bytes_total.inc(file_size)

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
