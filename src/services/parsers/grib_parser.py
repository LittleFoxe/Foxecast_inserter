from __future__ import annotations

import uuid
from datetime import datetime
from typing import List
import numpy as np
import xarray as xr

from src.domain.dto import ForecastDataDTO
from src.infrastructure.source_resolver import resolve_data_source


class GribParser:
    """Parser strategy for GRIB files using xarray+cfgrib."""

    def parse(self, local_path: str, file_name: str) -> List[ForecastDataDTO]:
        # Open as multi-message dataset; allow multiple indices
        ds = xr.open_dataset(local_path, engine="cfgrib")

        dtos: List[ForecastDataDTO] = []

        # Iterate over variables with 2D grid (y, x) or (latitude, longitude)
        for var_name, da in ds.data_vars.items():
            if da.ndim < 2:
                continue

            arr = da.values
            if arr is None:
                continue

            # Flatten row-major
            values = np.array(arr, dtype=np.float32).reshape(-1).tolist()
            grid_size_lat = int(arr.shape[-2])
            grid_size_lon = int(arr.shape[-1])

            # Coordinates
            lat_coord = None
            lon_coord = None
            if "latitude" in ds.coords:
                lat_coord = ds["latitude"].values
            if "longitude" in ds.coords:
                lon_coord = ds["longitude"].values

            if lat_coord is not None and lon_coord is not None:
                # Assume 1D lat, 1D lon broadcast
                if lat_coord.ndim == 1 and lon_coord.ndim == 1:
                    min_lat = float(np.min(lat_coord))
                    max_lat = float(np.max(lat_coord))
                    min_lon = float(np.min(lon_coord))
                    max_lon = float(np.max(lon_coord))
                    lat_step = float(abs(np.diff(lat_coord).mean())) if lat_coord.size > 1 else 0.0
                    lon_step = float(abs(np.diff(lon_coord).mean())) if lon_coord.size > 1 else 0.0
                else:
                    min_lat = float(np.min(lat_coord))
                    max_lat = float(np.max(lat_coord))
                    min_lon = float(np.min(lon_coord))
                    max_lon = float(np.max(lon_coord))
                    # Approximate step from first row/col
                    lat_step = float(abs(np.diff(lat_coord[:, 0]).mean())) if lat_coord.shape[0] > 1 else 0.0
                    lon_step = float(abs(np.diff(lon_coord[0, :]).mean())) if lon_coord.shape[1] > 1 else 0.0
            else:
                # Fallback to GRIB attributes (cfgrib exposes as GRIB_*)
                a = da.attrs
                min_lon = float(a.get("GRIB_longitudeOfFirstGridPointInDegrees", 0.0))
                max_lon = float(a.get("GRIB_longitudeOfLastGridPointInDegrees", 0.0))
                max_lat = float(a.get("GRIB_latitudeOfFirstGridPointInDegrees", 0.0))
                min_lat = float(a.get("GRIB_latitudeOfLastGridPointInDegrees", 0.0))
                lon_step = float(a.get("GRIB_iDirectionIncrementInDegrees", a.get("GRIB_DxInDegrees", 0.0)) or 0.0)
                lat_step = float(a.get("GRIB_jDirectionIncrementInDegrees", a.get("GRIB_DyInDegrees", 0.0)) or 0.0)

            # Time
            a = da.attrs
            data_date = a.get("GRIB_dataDate") or ds.attrs.get("GRIB_dataDate")
            data_time = a.get("GRIB_dataTime") or ds.attrs.get("GRIB_dataTime")
            try:
                yyyy = int(str(data_date)[0:4])
                mm = int(str(data_date)[4:6])
                dd = int(str(data_date)[6:8])
                t = int(data_time) if data_time is not None else 0
                hh = int(t // 100) if t >= 100 else t
                forecast_date = datetime(yyyy, mm, dd, hh)
            except Exception:
                forecast_date = datetime.utcnow()

            # Forecast step
            step = a.get("GRIB_step") or a.get("GRIB_forecastTime") or a.get("GRIB_stepRange")
            forecast_hour = 0
            if isinstance(step, str) and "-" in step:
                try:
                    forecast_hour = int(step.split("-")[-1])
                except Exception:
                    forecast_hour = 0
            else:
                try:
                    forecast_hour = int(step) if step is not None else 0
                except Exception:
                    forecast_hour = 0

            # Param & units
            parameter = str(a.get("GRIB_shortName", var_name))
            parameter_unit = str(a.get("GRIB_units", a.get("units", "")))

            # Level
            surface_type = str(a.get("GRIB_typeOfLevel", "surface"))
            try:
                surface_value = float(a.get("GRIB_level", 0.0))
            except Exception:
                surface_value = 0.0

            # Source
            data_source = str(a.get("GRIB_centre", ds.attrs.get("GRIB_centre", "unknown")))
            data_source = resolve_data_source(file_name, fallback=data_source)

            dto = ForecastDataDTO(
                id=str(uuid.uuid4()),
                forecast_date=forecast_date,
                forecast_hour=forecast_hour,
                data_source=data_source,
                parameter=parameter,
                parameter_unit=parameter_unit,
                surface_type=surface_type,
                surface_value=surface_value,
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

        return dtos
