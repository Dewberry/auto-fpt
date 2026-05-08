from cosecha.reaping.nwp import NWPReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import logging
import xarray as xr
from shared.constants import REGION_BOUNDS
from shared.utils import save_netcdf_to_s3, parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=1)
DEFAULT_FORECAST_HOURS = 18
DEFAULT_MODEL = "hrrr"
DEFAULT_VARIABLE = "hourly_precip"
NWP_VARIABLE_MAPPING = {
    'time': 'init_time',
    'tp': 'qpf_1hr'
}

def transform(ds: xr.Dataset) -> xr.Dataset:
    """Apply transformations to the dataset, such as renaming variables and dropping unnecessary ones."""
    rename_mapping = {k: v for k, v in NWP_VARIABLE_MAPPING.items() if k in ds.variables}
    if rename_mapping:
        ds = ds.rename(rename_mapping)

    # Make sure init_time is a dimension to make concatenation easier downstream
    ds = ds.expand_dims({'init_time': [ds.init_time.values]})
    # Ensure valid_time includes the init_time dimension
    if 'valid_time' in ds and 'init_time' not in ds['valid_time'].dims:
        ds['valid_time'] = ds['valid_time'].expand_dims(init_time=ds.init_time)

    ds = ds.drop_vars(['surface', 'gribfile_projection'], errors='ignore')
    return ds

def fetch_nwp_data(model: str, variable: str, init_time: datetime, forecast_hours: int) -> tuple[NWPReaper, xr.Dataset]:
    """Fetch raw NWP data from the API."""
    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    logging.info(f"Fetching NWP data for model {model}, variable {variable}, init time {init_time_str} UTC, forecast hours 1-{forecast_hours}...")
    
    reaper = NWPReaper(
        init_time=init_time_str,
        forecast_hours=range(1, forecast_hours + 1), 
        model=model,
        variable=variable,
        transformations={
            "spatial_subset": {
                'lat_bounds': (REGION_BOUNDS[1], REGION_BOUNDS[3]),
                'lon_bounds': (REGION_BOUNDS[0], REGION_BOUNDS[2])
            }
        }
    )

    return reaper, reaper.reap()

def main(init_time: datetime, model: str, variable: str, forecast_hours: int, output_path: str) -> None:
    """Orchestrates the data extraction and saving for NWP."""

    if not output_path.lower().endswith('.nc'):
        raise ValueError("Output path must end with '.nc'")
        
    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    
    logging.info(f"Fetching NWP data for model {model}, variable {variable}, init time {init_time_str} UTC...")
    
    reaper, data = fetch_nwp_data(model, variable, init_time, forecast_hours)
    if len(data.step) != forecast_hours:
        logging.warning(f"Expected {forecast_hours} forecast hours but got {len(data.step)}.")

    reaper.data = transform(data)

    save_netcdf_to_s3(reaper, output_path)

def handler(event = None, context = None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""
    if event is None:
        event = {}

    model = event.get("model", DEFAULT_MODEL)
    variable = event.get("variable", DEFAULT_VARIABLE)
    forecast_hours = event.get("forecast_hours", DEFAULT_FORECAST_HOURS)

    dt_now = datetime.now(tz=timezone.utc)
    if event.get("init_time"):
        init_time = parse_tz_aware_time(event["init_time"])
    else:
        init_time = dt_now.replace(minute=0, second=0, microsecond=0) - DEFAULT_LOOKBACK

    output_path = event.get("output_path", generate_default_path(model, dt_now, "nc"))

    main(
        init_time=init_time,
        model=model,
        variable=variable,
        forecast_hours=forecast_hours,
        output_path=output_path
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "NWP data retrieval complete.",
            "output_path": output_path,
            "model": model,
            "variable": variable,
            "init_time": init_time.isoformat()
        }
    }
