from cosecha.reaping.nwp import NWPReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import logging
import xarray as xr
import numpy as np

from shared.constants import REGION_BOUNDS
from shared.utils import save_netcdf_to_s3, parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=1)
DEFAULT_VARIABLE = "temp_2m"

RTMA_VARIABLE_MAPPING = {
    't2m': {'var_name': "temp_2m", 'attrs': {'standard_name': 'air_temperature', 'units': 'degC', 'long_name': '2-meter Air Temperature'}},
}

def fetch_rtma_data(variable: str, init_time: datetime) -> tuple[NWPReaper, xr.Dataset]:
    """Fetch raw RTMA data using Cosecha."""

    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    logging.info(f"Fetching RTMA data for variable {variable}, init time {init_time_str} UTC...")

    reaper = NWPReaper(
        init_time=init_time_str,
        forecast_hours=None, 
        model='rtma',
        variable=variable,
        transformations={
            "spatial_subset": {
                'lat_bounds': (REGION_BOUNDS[1], REGION_BOUNDS[3]),
                'lon_bounds': (REGION_BOUNDS[0], REGION_BOUNDS[2])
            }
        }
    )

    return reaper, reaper.reap()


def transform(ds: xr.Dataset) -> xr.Dataset:
    """Apply transformations to the dataset, such as renaming variables and dropping unnecessary ones."""

    for var, attrs in RTMA_VARIABLE_MAPPING.items():
        if var in ds.variables:
            ds[var].attrs = attrs['attrs']
            ds[var].attrs.update({"grid_mapping": "crs"})

    ds = ds.expand_dims({'valid_time': [ds.valid_time.values]})
    ds = ds.drop_vars(['heightAboveGround', 'step', 'time'], errors='ignore')
    
    if 't2m' in ds.data_vars:
        ds['t2m'] = ds['t2m'] - 273.15


    rename_mapping = {k: v['var_name'] for k, v in RTMA_VARIABLE_MAPPING.items() if k in ds.variables}
    if rename_mapping:
        ds = ds.rename(rename_mapping)

    ds = ds.rename({"gribfile_projection": "crs"})
    crs_attrs = ds["crs"].attrs
    ds = ds.assign_coords(crs=xr.DataArray(np.int32(0), attrs=crs_attrs))

    ds["valid_time"].attrs.update({
        "standard_name": "time",
        "long_name": "valid time",
    })

    ds.attrs = {
        "Conventions": "CF-1.9",
        "title": "Real Time Mesoscale Analysis (RTMA)",
        }
    return ds


def main(init_time: datetime, variable: str, output_path: str) -> None:
    """Orchestrates the data extraction and saving for RTMA."""

    if not output_path.lower().endswith('.nc'):
        raise ValueError("Output path must end with '.nc'")
        
    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    logging.info(f"Fetching RTMA data for variable {variable}, init time {init_time_str} UTC...")
    
    reaper, data = fetch_rtma_data(variable, init_time)

    reaper.data = transform(data)

    save_netcdf_to_s3(reaper, output_path)


def handler(event = None, context = None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""
    if event is None:
        event = {}

    variable = event.get("variable", DEFAULT_VARIABLE)

    dt_now = datetime.now(tz=timezone.utc)
    if event.get("init_time"):
        init_time = parse_tz_aware_time(event["init_time"])
    else:
        init_time = dt_now.replace(minute=0, second=0, microsecond=0) - DEFAULT_LOOKBACK

    output_path = event.get("output_path", generate_default_path("rtma", dt_now, "nc"))

    main(
        init_time=init_time,
        variable=variable,
        output_path=output_path
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "RTMA data retrieval complete.",
            "output_path": output_path,
            "model": "rtma",
            "variable": variable,
            "init_time": init_time.isoformat()
        }
    }
