from __future__ import annotations

import uuid
from datetime import datetime
from typing import List

import numpy as np
from eccodes import (
    codes_bufr_new_from_file,
    codes_bufr_decode_messages,
    codes_release,
)

from src.domain.dto import ForecastDataDTO
from src.infrastructure.source_resolver import resolve_data_source


class BufrParser:
    """Parser strategy for BUFR files using eccodes.

    Note: BUFR is observation-oriented and often lacks gridded fields and
    complete spatial grid metadata required for our DB schema. We attempt to
    infer grid metadata if messages form a regular grid; otherwise, we raise.
    """

    def parse(self, local_path: str, file_name: str) -> List[ForecastDataDTO]:
        dtos: List[ForecastDataDTO] = []

        with open(local_path, "rb") as f:
            while True:
                h = codes_bufr_new_from_file(f)
                if h is None:
                    break
                try:
                    messages = codes_bufr_decode_messages(h)
                finally:
                    codes_release(h)

                for msg in messages:
                    # Expect arrays of lat, lon, value (e.g., temperature)
                    lats = np.array(msg.get("latitude", []), dtype=float)
                    lons = np.array(msg.get("longitude", []), dtype=float)

                    # Find any numeric measurement field
                    value = None
                    for key in ("airTemperature", "temperature", "windSpeed", "totalPrecipitation"):
                        if key in msg:
                            value = np.array(msg.get(key), dtype=float)
                            parameter = key
                            break
                    if value is None or lats.size == 0 or lons.size == 0:
                        # Not suitable for our schema; skip
                        continue

                    # Attempt to infer a grid by sorting unique lat/lon
                    unique_lats = np.unique(lats)
                    unique_lons = np.unique(lons)
                    grid_size_lat = unique_lats.size
                    grid_size_lon = unique_lons.size

                    # Check if values fit grid_size_lat * grid_size_lon
                    if value.size != grid_size_lat * grid_size_lon:
                        # Cannot reshape to a regular grid; our DB requires gridded fields
                        raise ValueError("BUFR message is not a regular grid and cannot be stored in forecast_data")

                    # Compute bounds and steps
                    min_lat = float(unique_lats.min())
                    max_lat = float(unique_lats.max())
                    min_lon = float(unique_lons.min())
                    max_lon = float(unique_lons.max())
                    lat_step = float(abs(np.diff(unique_lats).mean())) if grid_size_lat > 1 else 0.0
                    lon_step = float(abs(np.diff(unique_lons).mean())) if grid_size_lon > 1 else 0.0

                    values = value.reshape((grid_size_lat, grid_size_lon)).astype(np.float32).ravel(order="C").tolist()

                    # Time
                    year = int(msg.get("year", [datetime.utcnow().year])[0])
                    month = int(msg.get("month", [datetime.utcnow().month])[0])
                    day = int(msg.get("day", [datetime.utcnow().day])[0])
                    hour = int(msg.get("hour", [0])[0])
                    forecast_date = datetime(year, month, day, hour)
                    forecast_hour = 0

                    # Units and level if present
                    parameter_unit = ""
                    surface_type = "surface"
                    surface_value = 0.0

                    data_source = resolve_data_source(file_name, fallback=str(msg.get("dataCategory", "unknown")))

                    dto = ForecastDataDTO(
                        id=str(uuid.uuid4()),
                        forecast_date=forecast_date,
                        forecast_hour=forecast_hour,
                        data_source=data_source,
                        parameter=parameter,
                        parameter_unit=parameter_unit,
                        surface_type=surface_type,
                        surface_value=float(surface_value),
                        min_lon=float(min_lon),
                        max_lon=float(max_lon),
                        min_lat=float(min_lat),
                        max_lat=float(max_lat),
                        lon_step=float(lon_step),
                        lat_step=float(lat_step),
                        grid_size_lat=int(grid_size_lat),
                        grid_size_lon=int(grid_size_lon),
                        values=values,
                        file_name=file_name,
                    )
                    dtos.append(dto)

        return dtos


