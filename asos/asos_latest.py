"""Reaper for fetching latest ASOS precipitation data by report type, transforming it, and saving to Parquet."""

from typing import Tuple

from cosecha.reaping.asos import ASOSReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging
import time

from shared.utils import parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=2)
DEFAULT_STATE = "TX"
DEFAULT_VARIABLE = "p01i"
REPORT_TYPES = {1: "hfmetar", 3: "routine", 4: "special"}
API_SLEEP_SECONDS = 10

ASOS_COLUMN_RENAMES = {
    "station": "site_id",
    "valid": "timestamp",
    "p01i": "value",
}

REGION_BOUNDS = [-99, 31.5, -95, 35] # [min_lon, min_lat, max_lon, max_lat]


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to the DataFrame, such as renaming columns and adding provider."""

    # Clip stations to region bounds [min_lon, min_lat, max_lon, max_lat]
    min_lon, min_lat, max_lon, max_lat = REGION_BOUNDS
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df = df[
        (df["lon"] >= min_lon) & (df["lon"] <= max_lon) &
        (df["lat"] >= min_lat) & (df["lat"] <= max_lat)
    ]

    df = df.rename(columns=ASOS_COLUMN_RENAMES)
    df = df.drop(columns=["lon", "lat"], errors="ignore")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["provider"] = "NWS"

    return df


def fetch_asos_data(state: str, variable: str, start_date: datetime, end_date: datetime) -> Tuple[ASOSReaper, pd.DataFrame]:
    """Fetch ASOS data across all report types from the IEM API."""
    logging.info(f"Fetching ASOS {variable} data for state={state}...")

    dfs = []
    reaper = None
    # IEM API does not return report type values, so we iterate and tag each batch
    for report_type, report_name in REPORT_TYPES.items():
        reaper = ASOSReaper(
            start_date=start_date,
            end_date=end_date,
            state=state,
            variable=variable,
            report_type=report_type,
        )

        data = reaper.reap()
        data["variable"] = f"precip_{report_name}"
        dfs.append(data)
        time.sleep(API_SLEEP_SECONDS)

    df = pd.concat(dfs, ignore_index=True)
    return reaper, df


def main(state: str, variable: str, start_date: datetime, end_date: datetime, output_path: str) -> None:
    """Orchestrates the data extraction and saving."""
    logging.info(f"Fetching ASOS data for state={state} from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}...")

    reaper, data = fetch_asos_data(state, variable, start_date, end_date)

    if data.empty:
        raise ValueError("No ASOS data available for the given parameters.")

    logging.info(f"Transforming {len(data)} records...")
    reaper.data = transform(data)
    logging.info(f"Saving records to {output_path}...")
    reaper.sow_to_parquet(file_path=output_path)

    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    if event is None:
        event = {}

    state = event.get("state", DEFAULT_STATE)
    variable = event.get("variable", DEFAULT_VARIABLE)

    if event.get("end_time"):
        end_date = parse_tz_aware_time(event["end_time"])
    else:
        end_date = datetime.now(tz=timezone.utc)

    if event.get("start_time"):
        start_date = parse_tz_aware_time(event["start_time"])
    else:
        start_date = end_date - DEFAULT_LOOKBACK

    if start_date >= end_date:
        raise ValueError(f"start_time ({start_date}) must be before end_time ({end_date}).")

    output_path = event.get("output_path", generate_default_path("asos", end_date, "parquet"))

    main(
        state=state,
        variable=variable,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "ASOS data retrieval complete.",
            "output_path": output_path,
            "state": state,
            "variable": variable,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        },
    }