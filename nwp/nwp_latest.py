from cosecha.reaping.nwp import NWPReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import logging

from shared.constants import REGION_BOUNDS, DEFAULT_OUTPUT_PREFIX, DEFAULT_OUTPUT_TIMESTAMP_FMT
from shared.utils import save_netcdf_to_s3, parse_tz_aware_time

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=1)
DEFAULT_FORECAST_HOURS = 18
DEFAULT_MODEL = "hrrr"
DEFAULT_VARIABLE = "hourly_precip"

def main(init_time: datetime, model: str, variable: str, forecast_hours: int, output_path: str) -> None:
    """Orchestrates the data extraction and saving for NWP."""

    if not output_path.lower().endswith('.nc'):
        raise ValueError("Output path must end with '.nc'")
        
    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    
    logging.info(f"Fetching NWP data for model {model}, variable {variable}, init time {init_time_str} UTC...")
    
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

    data = reaper.reap()
    if len(data.step) != forecast_hours:
        logging.warning(f"Expected {forecast_hours} forecast hours but got {len(data.step)}.")

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

    output_path = event.get("output_path", f"{DEFAULT_OUTPUT_PREFIX}{model}/{dt_now.strftime(DEFAULT_OUTPUT_TIMESTAMP_FMT)}.nc")

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
