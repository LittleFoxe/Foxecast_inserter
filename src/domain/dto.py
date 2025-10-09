from __future__ import annotations

from datetime import datetime
from typing import List
from pydantic import BaseModel


class ForecastDataDTO(BaseModel):
    """DTO reflecting the `forecast_data` table schema for batch insertion."""
    # There is no point in creating just single domain object, because the service only inserts data into the DB

    id: str
    forecast_date: datetime
    forecast_hour: int
    data_source: str
    parameter: str
    parameter_unit: str
    surface_type: str
    surface_value: float
    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float
    lon_step: float
    lat_step: float
    grid_size_lat: int
    grid_size_lon: int
    values: List[float]
    file_name: str
