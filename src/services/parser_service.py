import time
import uuid
from datetime import datetime
from typing import List, Tuple

import numpy as np
from earthkit.data import from_source

from src.domain.dto import ForecastDataDTO
from src.infrastructure.source_resolver import resolve_data_source


class ParserService:
    """Parses hydrometeorological data files (e.g., GRIB/BUFR) into DTOs.

    Implementation note: earthkit will be integrated to read GRIB/BUFR.
    For now, this file contains a placeholder parse implementation to be
    replaced with actual earthkit-based logic.
    """

    def parse_file(self, local_path: str, file_name: str) -> Tuple[List[ForecastDataDTO], int]:
        start = time.perf_counter()

        ds = from_source("file", local_path)
        dtos: List[ForecastDataDTO] = []

        for field in ds:
            # Values and grid
            data = field.to_numpy()
            if data is None:
                continue

            # Ensure 2D grid; flatten row-major (lat-major then lon)
            arr = np.array(data, dtype=np.float32)
            if arr.ndim == 1:
                grid_size_lat = int(arr.shape[0])
                grid_size_lon = 1
            elif arr.ndim == 2:
                grid_size_lat = int(arr.shape[0])
                grid_size_lon = int(arr.shape[1])
            else:
                # Collapse higher dims conservatively
                arr = arr.reshape((-1, arr.shape[-1]))
                grid_size_lat = int(arr.shape[0])
                grid_size_lon = int(arr.shape[1])

            values = arr.astype(np.float32).ravel(order="C").tolist()

            # Spatial metadata
            def meta(key: str, default=None):
                try:
                    return field.metadata(key)
                except Exception:
                    return default

            min_lon = float(meta("longitudeOfFirstGridPointInDegrees", 0.0))
            max_lon = float(meta("longitudeOfLastGridPointInDegrees", 0.0))
            min_lat = float(meta("latitudeOfLastGridPointInDegrees", 0.0))
            max_lat = float(meta("latitudeOfFirstGridPointInDegrees", 0.0))

            lon_step = float(meta("iDirectionIncrementInDegrees", meta("DxInDegrees", 0.0)) or 0.0)
            lat_step = float(meta("jDirectionIncrementInDegrees", meta("DyInDegrees", 0.0)) or 0.0)

            # Time metadata
            data_date = meta("dataDate")  # e.g., 20250101
            data_time = meta("dataTime")  # e.g., 0..2359
            if data_date is None:
                # try date as string
                data_date = meta("date")
            if data_time is None:
                data_time = meta("time")

            try:
                yyyy = int(str(data_date)[0:4])
                mm = int(str(data_date)[4:6])
                dd = int(str(data_date)[6:8])
                hh = int(int(data_time) // 100) if int(data_time) >= 100 else int(data_time)
                forecast_date = datetime(yyyy, mm, dd, hh)
            except Exception:
                forecast_date = datetime.utcnow()

            step = meta("step")
            if step is None:
                step = meta("forecastTime", 0)
            if step is None:
                # try stepRange like "0-3"
                step_range = meta("stepRange")
                if step_range and isinstance(step_range, str) and "-" in step_range:
                    try:
                        step = int(step_range.split("-")[-1])
                    except Exception:
                        step = 0
                else:
                    try:
                        step = int(step_range)
                    except Exception:
                        step = 0
            forecast_hour = int(step) if step is not None else 0

            # Parameter & units
            parameter = str(meta("shortName", meta("param", "unknown")))
            parameter_unit = str(meta("units", meta("unit", "")))

            # Level / surface
            surface_type = str(meta("typeOfLevel", meta("levelType", "surface")))
            try:
                surface_value = float(meta("level", 0.0))
            except Exception:
                surface_value = 0.0

            # Data source: metadata or name-based resolver
            source_meta = meta("centre", meta("center", meta("institution", "unknown")))
            data_source = str(source_meta) if source_meta else "unknown"
            # Allow override from filename heuristics
            data_source = resolve_data_source(file_name, fallback=data_source)

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
                grid_size_lat=grid_size_lat,
                grid_size_lon=grid_size_lon,
                values=values,
                file_name=file_name,
            )
            dtos.append(dto)

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return dtos, elapsed_ms


