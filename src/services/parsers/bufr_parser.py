from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List
import numpy as np
from eccodes import (
    codes_bufr_new_from_file,
    codes_release,
    codes_set,
    codes_get,
    codes_get_array,
    codes_get_string  # Add this import if you need to get string values
)

from src.domain.dto import ForecastDataDTO
from src.infrastructure.source_resolver import resolve_data_source


class BufrParser:
    """Parser strategy for BUFR files using eccodes."""

    def parse(self, local_path: str, file_name: str) -> List[ForecastDataDTO]:
        dtos: List[ForecastDataDTO] = []

        with open(local_path, "rb") as f:
            while True:
                bufr_id = codes_bufr_new_from_file(f)
                if bufr_id is None:
                    break
                
                try:
                    # Unpack the BUFR message data
                    codes_set(bufr_id, "unpack", 1)
                    
                    # Extract time information using individual keys
                    # Using get methods with defaults if keys are missing
                    try:
                        year = codes_get(bufr_id, "typicalYear")
                    except:
                        year = datetime.now(timezone.utc).year
                    
                    try:
                        month = codes_get(bufr_id, "typicalMonth")
                    except:
                        month = datetime.now(timezone.utc).month
                    
                    try:
                        day = codes_get(bufr_id, "typicalDay")
                    except:
                        day = datetime.now(timezone.utc).day
                    
                    try:
                        hour = codes_get(bufr_id, "typicalHour")
                    except:
                        hour = 0
                    
                    forecast_date = datetime(year, month, day, hour)
                    forecast_hour = 0

                    # Extract coordinate data
                    try:
                        lats = np.array(codes_get_array(bufr_id, "latitude"), dtype=float)
                    except:
                        lats = np.array([], dtype=float)
                    
                    try:
                        lons = np.array(codes_get_array(bufr_id, "longitude"), dtype=float)
                    except:
                        lons = np.array([], dtype=float)

                    # Find any numeric measurement field
                    value = None
                    parameter = "unknown"
                    
                    # Try to get various possible parameters
                    for key in ("airTemperature", "temperature", "windSpeed", "totalPrecipitation"):
                        try:
                            value_data = codes_get_array(bufr_id, key)
                            if value_data is not None and len(value_data) > 0:
                                value = np.array(value_data, dtype=float)
                                parameter = key
                                break
                        except:
                            continue
                    
                    # Skip if no valid data found
                    if value is None or lats.size == 0 or lons.size == 0:
                        continue

                    # Attempt to infer a grid by sorting unique lat/lon
                    unique_lats = np.unique(lats)
                    unique_lons = np.unique(lons)
                    grid_size_lat = unique_lats.size
                    grid_size_lon = unique_lons.size

                    # Check if values fit grid_size_lat * grid_size_lon
                    if value.size != grid_size_lat * grid_size_lon:
                        raise ValueError("BUFR message is not a regular grid and cannot be stored in forecast_data")

                    # Compute bounds and steps
                    min_lat = float(unique_lats.min())
                    max_lat = float(unique_lats.max())
                    min_lon = float(unique_lons.min())
                    max_lon = float(unique_lons.max())
                    lat_step = float(abs(np.diff(unique_lats).mean())) if grid_size_lat > 1 else 0.0
                    lon_step = float(abs(np.diff(unique_lons).mean())) if grid_size_lon > 1 else 0.0

                    values = value.reshape((grid_size_lat, grid_size_lon)).astype(np.float32).ravel(order="C").tolist()

                    # Get data source and other metadata
                    try:
                        data_category = codes_get_string(bufr_id, "dataCategory")
                    except:
                        data_category = "unknown"
                    
                    data_source = resolve_data_source(file_name, fallback=data_category)

                    # Create DTO
                    dto = ForecastDataDTO(
                        id=str(uuid.uuid4()),
                        forecast_date=forecast_date,
                        forecast_hour=forecast_hour,
                        data_source=data_source,
                        parameter=parameter,
                        parameter_unit="",  # You may need to extract this from BUFR metadata
                        surface_type="surface",
                        surface_value=0.0,
                        min_lon=min_lon,
                        max_lon=max_lon,
                        min_lat=min_lat,
                        max_lat=max_lat,
                        lon_step=lon_step,
                        lat_step=lat_step,
                        grid_size_lat=grid_size_lat,
                        grid_size_lon=grid_size_lon,
                        values=values,
                        file_name=file_name,
                    )
                    dtos.append(dto)

                finally:
                    codes_release(bufr_id)

        return dtos
