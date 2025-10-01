from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, AnyUrl
from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

from infrastructure.config import settings
from services.parser_service import ParserService
from services.db_service import DatabaseService
from infrastructure.downloader import download_to_tempfile
from metrics.metrics import file_download_seconds, file_size_bytes, network_bytes_total, parse_seconds, db_insert_seconds


router = APIRouter()


class InsertRequest(BaseModel):
    """Request model for inserting forecast data from a file URL."""

    url: AnyUrl


@router.get("/health", status_code=HTTP_200_OK, summary="Health check", tags=["system"])
def health() -> dict:
    """Returns 200 OK if the service is up."""
    return {"status": "ok"}


@router.post("/insert", status_code=HTTP_200_OK, summary="Parse file and insert to DB", tags=["ingest"])
def insert(payload: InsertRequest) -> dict:
    """Downloads a binary file by URL, parses the content, and inserts data into ClickHouse.

    - On parsing error returns 400 with details
    - On DB error returns 500 with details
    """
    # Determine file name as the last path segment of URL
    file_name = payload.url.path.split("/")[-1]

    try:
        local_path, size_bytes, download_ms = download_to_tempfile(str(payload.url), settings.download_timeout_seconds)
    except Exception as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=f"Download error: {exc}") from exc

    try:
        parser = ParserService()
        dtos, parse_ms = parser.parse_file(local_path, file_name=file_name)
    except Exception as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=f"Parsing error: {exc}") from exc

    try:
        db = DatabaseService()
        inserted_rows, db_ms = db.insert_batch(dtos, file_name=file_name)
    except Exception as exc:
        raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error: {exc}") from exc

    # Update metrics
    file_download_seconds.observe(download_ms / 1000.0)
    parse_seconds.observe(parse_ms / 1000.0)
    db_insert_seconds.observe(db_ms / 1000.0)
    file_size_bytes.set(size_bytes)
    network_bytes_total.inc(size_bytes)

    return {
        "status": "ok",
        "file_name": file_name,
        "download_ms": download_ms,
        "file_size_bytes": size_bytes,
        "parse_ms": parse_ms,
        "db_ms": db_ms,
        "inserted_rows": inserted_rows,
    }


