import time
from typing import Iterable, List, Tuple

from clickhouse_connect import get_client

from domain.dto import ForecastDataDTO
from infrastructure.config import settings


class DatabaseService:
    """Handles batch inserts into ClickHouse with simple file_name uniqueness check."""

    def __init__(self) -> None:
        self.client = get_client(
            host=settings.ch_host,
            port=settings.ch_port,
            username=settings.ch_user,
            password=settings.ch_password,
            database=settings.ch_database,
        )

    def _already_ingested(self, file_name: str) -> bool:
        query = "SELECT count() FROM forecast_data WHERE file_name = %(file_name)s LIMIT 1"
        res = self.client.query(query, parameters={"file_name": file_name})
        return int(res.result_rows[0][0]) > 0

    def insert_batch(self, dtos: Iterable[ForecastDataDTO], file_name: str) -> Tuple[int, int]:
        start = time.perf_counter()

        if self._already_ingested(file_name):
            return 0, int((time.perf_counter() - start) * 1000)

        rows: List[tuple] = []
        for d in dtos:
            rows.append(
                (
                    d.id,
                    d.forecast_date,
                    d.forecast_hour,
                    d.data_source,
                    d.parameter,
                    d.parameter_unit,
                    d.surface_type,
                    d.surface_value,
                    d.min_lon,
                    d.max_lon,
                    d.min_lat,
                    d.max_lat,
                    d.lon_step,
                    d.lat_step,
                    d.grid_size_lat,
                    d.grid_size_lon,
                    d.values,
                    d.file_name,
                )
            )

        self.client.insert(
            "forecast_data",
            rows,
            column_names=[
                "id",
                "forecast_date",
                "forecast_hour",
                "data_source",
                "parameter",
                "parameter_unit",
                "surface_type",
                "surface_value",
                "min_lon",
                "max_lon",
                "min_lat",
                "max_lat",
                "lon_step",
                "lat_step",
                "grid_size_lat",
                "grid_size_lon",
                "values",
                "file_name",
            ],
        )

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return len(rows), elapsed_ms


