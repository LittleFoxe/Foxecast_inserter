import time
from typing import Iterable, List, Tuple

from clickhouse_connect import get_client

from src.domain.dto import ForecastDataDTO
from src.infrastructure.config import settings


class DatabaseService:
    """Handles batch inserts into ClickHouse with simple file_name uniqueness check."""

    def __init__(self) -> None:
        """Initializes the DatabaseService with a ClickHouse client using configuration from settings module."""
        self.client = get_client(
            host=settings.ch_host,
            port=settings.ch_port,
            username=settings.ch_user,
            password=settings.ch_password,
            database=settings.ch_database,
        )

    def _already_ingested(self, file_name: str) -> bool:
        """
        Checks if a file has already been ingested into the forecast_data table.

        Executes a COUNT() query with LIMIT 1 to verify existence of records with the specified file_name.

        Args:
            file_name (str): Name of the file to check for existence

        Returns:
            bool: True if file_name exists in the table (file has been ingested), False otherwise
        """
        query = "SELECT count() FROM forecast_data WHERE file_name = %(file_name)s LIMIT 1"
        res = self.client.query(query, parameters={"file_name": file_name})
        return int(res.result_rows[0][0]) > 0

    def insert_batch(self, dtos: Iterable[ForecastDataDTO], file_name: str) -> Tuple[int, int]:
        """
        Inserts a batch of ForecastDataDTO records with file's name into ClickHouse
        if the file has not already been ingested.

        Performs a uniqueness check before insertion and returns metrics about the operation.

        Args:
            dtos (Iterable[ForecastDataDTO]): Collection of data transfer objects to insert
            file_name (str): Identifier for the data file batch

        Returns:
            Tuple[int, int]: 
                - Number of successfully inserted rows
                - Time taken for operation in milliseconds

        Note:
            Will return (0, time) if file_name already exists in the table
        """
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
    
    def clear_test_data(self):
        """
        Clears all data from the forecast_data table.

        Executes a TRUNCATE TABLE query which removes all records but preserves table structure.
        """
        self.client.query("TRUNCATE TABLE forecast_data")
