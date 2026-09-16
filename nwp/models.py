from dataclasses import dataclass, field
from datetime import timedelta
import xarray as xr
import numpy as np

from shared.constants import REGION_BOUNDS


@dataclass
class NWPModel:
    """Base configuration for an NWP model product."""

    name: str
    model: str
    variable: str | None
    default_forecast_hours: int | list[int] | None
    variable_mapping: dict
    title: str
    schema: str = "gridded-forecast.json"
    search_str: str | None = None
    product: str | None = None
    lookback: timedelta = timedelta(hours=1)
    unit_conversions: dict = field(default_factory=dict)  # {raw_var_name: callable}

    def get_forecast_hours(self, override: int | list[int] | None = None) -> range | list[int] | None:
        hours = override if override is not None else self.default_forecast_hours
        if isinstance(hours, int):
            return range(1, hours + 1)
        return hours

    def reaper_kwargs(self, init_time_str: str, forecast_hours: int | list[int] | None = None) -> dict:
        """Build kwargs dict for NWPReaper."""
        kw = {
            "init_time": init_time_str,
            "forecast_hours": self.get_forecast_hours(forecast_hours),
            "model": self.model,
            "variable": self.variable,
            "transformations": {
                "spatial_subset": {
                    "lat_bounds": (REGION_BOUNDS[1], REGION_BOUNDS[3]),
                    "lon_bounds": (REGION_BOUNDS[0], REGION_BOUNDS[2]),
                }
            },
        }
        if self.search_str is not None:
            kw["search_str"] = self.search_str
        if self.product is not None:
            kw["product"] = self.product
        return kw

    def transform(self, ds: xr.Dataset) -> xr.Dataset:
        raise NotImplementedError


@dataclass
class ForecastModel(NWPModel):
    """Forecast products with init_time + step + valid_time (HRRR, NBM, RRFS, etc.)."""

    def transform(self, ds: xr.Dataset) -> xr.Dataset:
        for var, mapping in self.variable_mapping.items():
            if var in ds.variables:
                ds[var].attrs = mapping["attrs"]
                ds[var].attrs.update({"grid_mapping": "crs"})

        for var_name, convert in self.unit_conversions.items():
            if var_name in ds.data_vars:
                ds[var_name] = convert(ds[var_name])

        rename_mapping = {k: v["var_name"] for k, v in self.variable_mapping.items() if k in ds.variables}
        if rename_mapping:
            ds = ds.rename(rename_mapping)

        ds = ds.rename({"time": "init_time", "gribfile_projection": "crs"})
        crs_attrs = ds["crs"].attrs
        ds = ds.assign_coords(crs=xr.DataArray(np.int32(0), attrs=crs_attrs))

        ds = ds.expand_dims({"init_time": [ds.init_time.values]})
        if "valid_time" in ds and "init_time" not in ds["valid_time"].dims:
            ds["valid_time"] = ds["valid_time"].expand_dims(init_time=ds.init_time)

        ds["init_time"].attrs = {
            "standard_name": "forecast_reference_time",
            "long_name": "initial time of forecast",
        }
        ds = ds.drop_vars(["surface"], errors="ignore")

        ds.attrs = {"Conventions": "CF-1.9", "title": self.title}
        return ds


@dataclass
class AnalysisModel(NWPModel):
    """Analysis products with valid_time only (RTMA, etc.)."""

    def transform(self, ds: xr.Dataset) -> xr.Dataset:
        for var, mapping in self.variable_mapping.items():
            if var in ds.variables:
                ds[var].attrs = mapping["attrs"]
                ds[var].attrs.update({"grid_mapping": "crs"})

        ds = ds.expand_dims({"valid_time": [ds.valid_time.values]})
        ds = ds.drop_vars(["heightAboveGround", "step", "time"], errors="ignore")

        for var_name, convert in self.unit_conversions.items():
            if var_name in ds.data_vars:
                ds[var_name] = convert(ds[var_name])

        rename_mapping = {k: v["var_name"] for k, v in self.variable_mapping.items() if k in ds.variables}
        if rename_mapping:
            ds = ds.rename(rename_mapping)

        ds = ds.rename({"gribfile_projection": "crs"})
        crs_attrs = ds["crs"].attrs
        ds = ds.assign_coords(crs=xr.DataArray(np.int32(0), attrs=crs_attrs))

        ds["valid_time"].attrs.update({
            "standard_name": "time",
            "long_name": "valid time",
        })

        ds.attrs = {"Conventions": "CF-1.9", "title": self.title}
        return ds

HRRR = ForecastModel(
    name="hrrr",
    model="hrrr",
    variable="hourly_precip",
    default_forecast_hours=18,
    variable_mapping={
        "tp": {
            "var_name": "qpf_1hr",
            "attrs": {
                "standard_name": "precipitation_amount",
                "units": "kg m-2",
                "long_name": "1-hour Total Precipitation Forecast",
            },
        },
    },
    title="High Resolution Rapid Refresh (HRRR) Forecast",
)

# search_str is tightly coupled to default_forecast_hours=[6, 12, 18].
# If forecast_hours is overridden, search_str likely needs to be updated as well.
NBM = ForecastModel(
    name="nbm",
    model="nbm",
    variable=None,
    default_forecast_hours=[6, 12, 18],
    variable_mapping={
        "tp": {
            "var_name": "pop_6hr",
            "attrs": {
                "standard_name": "probability_of_precipitation_amount_above_threshold",
                "units": "%",
                "long_name": "6-hour Probability of Precipitation > 0.254mm",
            },
        },
    },
    title="National Blend of Models (NBM)",
    search_str=r":APCP:surface:(?:0-6|6-12|12-18) hour acc fcst:prob >0.254:",
    product="co",
)

RTMA = AnalysisModel(
    name="rtma",
    model="rtma",
    variable="temp_2m",
    default_forecast_hours=None,
    variable_mapping={
        "t2m": {
            "var_name": "temp_2m",
            "attrs": {
                "standard_name": "air_temperature",
                "units": "degC",
                "long_name": "2-meter Air Temperature",
            },
        },
    },
    title="Real Time Mesoscale Analysis (RTMA)",
    unit_conversions={"t2m": lambda x: x - 273.15},
)

RRFS = ForecastModel(
    name="rrfs",
    model="rrfs",
    variable="hourly_precip",
    default_forecast_hours=18,
    variable_mapping={
        "tp": {
            "var_name": "qpf_1hr",
            "attrs": {
                "standard_name": "precipitation_amount",
                "units": "kg m-2",
                "long_name": "1-hour Total Precipitation Forecast",
            },
        },
    },
    title="Rapid Refresh Forecast System (RRFS) Forecast",
)

# Each entry is a model. variable_mapping holds all vars for that model.
# To add a new variable to an existing model, add it to that model's variable_mapping.
PRODUCTS: dict[str, NWPModel] = {
    "hrrr": HRRR,
    "nbm": NBM,
    "rtma": RTMA,
    "rrfs": RRFS,
}
