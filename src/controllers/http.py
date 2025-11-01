from typing import Callable, Tuple
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, AnyUrl, Field
from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

from src.metrics.metrics import file_download_seconds, file_size_bytes, network_bytes_total, parse_seconds, db_insert_seconds
from src.infrastructure.service_provider import get_settings, get_downloader, get_parser_service, get_db_service


router = APIRouter()


class InsertRequest(BaseModel):
    """Request model for inserting forecast data from a file URL."""

    url: AnyUrl = Field(
        ...,
        json_schema_extra={
            "example": "https://data.ecmwf.int/forecasts/{DATE}/{TIME}z/ifs/0p25/oper/{FILE}.grib2",
            "description": "URL to GRIB2 file from ECMWF Open Data. Replace {DATE}, {TIME}, and {FILE} with actual values."
        }
    )


@router.get("/health", status_code=HTTP_200_OK, summary="Health check", tags=["system"])
def health() -> dict:
    """Returns 200 OK if the service is up."""
    return {"status": "ok"}


@router.post("/insert", status_code=HTTP_200_OK, summary="Parse file and insert to DB", tags=["usage"])
def insert(
    payload: InsertRequest,
    settings_dep = Depends(get_settings),
    downloader: Callable[[str, int], Tuple[str, int, int]] = Depends(get_downloader),
    parser = Depends(get_parser_service),
    db = Depends(get_db_service),
) -> dict:
    """Downloads a binary file by URL, parses the content, and inserts data into ClickHouse.

    - On parsing error returns 400 with details
    - On DB error returns 500 with details

    **Example URL for ECMWF Open Data template:**
    - `https://data.ecmwf.int/forecasts/20250930/12z/ifs/0p25/oper/20250930120000-15h-oper-fc.grib2`
    """
    # Determine file name as the last path segment of URL
    file_name = payload.url.path.split("/")[-1]

    try:
        local_path, size_bytes, download_ms = downloader(str(payload.url), settings_dep.download_timeout_seconds)
    except Exception as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=f"Download error: {exc}") from exc

    try:
        dtos, parse_ms = parser.parse_file(local_path, file_name=file_name)
    except Exception as exc:
        raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Parsing error: {exc}") from exc

    try:
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
