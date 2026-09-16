from cosecha.reaping.nwp import NWPReaper
from cosecha import configure_logger
import xarray as xr
from datetime import datetime, timezone
import logging

from shared.utils import save_netcdf_to_s3, parse_tz_aware_time, generate_default_path
from models import PRODUCTS, NWPModel

configure_logger(level="INFO")

DEFAULT_PRODUCT = "hrrr"


def fetch_nwp_data(config: NWPModel, init_time: datetime, forecast_hours: int | list[int] | None = None) -> tuple[NWPReaper, "xr.Dataset"]:
    """Fetch raw NWP data using Cosecha."""

    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    logging.info(f"Fetching {config.name} data for init time {init_time_str} UTC...")

    reaper = NWPReaper(**config.reaper_kwargs(init_time_str, forecast_hours))
    return reaper, reaper.reap()


def main(config: NWPModel, init_time: datetime, output_path: str, forecast_hours: int | list[int] | None = None) -> None:
    """Orchestrates the data extraction and saving for NWP."""

    if not output_path.lower().endswith('.nc'):
        raise ValueError("Output path must end with '.nc'")

    init_time_str = init_time.strftime("%Y-%m-%d %H:%M")
    logging.info(f"Fetching {config.name} data for init time {init_time_str} UTC...")

    reaper, data = fetch_nwp_data(config, init_time, forecast_hours)
    expected_hours = config.get_forecast_hours(forecast_hours)
    if expected_hours is not None and len(data.step) != len(expected_hours):
        logging.warning(f"Expected {len(expected_hours)} forecast steps but got {len(data.step)}.")

    reaper.data = config.transform(data)
    save_netcdf_to_s3(reaper, output_path)


def handler(event = None, context = None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""
    if event is None:
        event = {}

    product_name = event.get("product", DEFAULT_PRODUCT)
    config = PRODUCTS[product_name]
    forecast_hours = event.get("forecast_hours", None)

    dt_now = datetime.now(tz=timezone.utc)
    if event.get("init_time"):
        init_time = parse_tz_aware_time(event["init_time"])
    else:
        init_time = dt_now.replace(minute=0, second=0, microsecond=0) - config.lookback

    output_path = event.get("output_path", generate_default_path(config.name, dt_now, "nc"))

    main(
        config=config,
        init_time=init_time,
        output_path=output_path,
        forecast_hours=forecast_hours,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": f"{config.name} data retrieval complete.",
            "output_path": output_path,
            "model": config.name,
            "init_time": init_time.isoformat()
        }
    }
