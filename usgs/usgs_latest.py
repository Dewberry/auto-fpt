"""Reaper for fetching latest USGS NWIS data for specified gage IDs and parameters, transforming it, and saving to Parquet."""

from typing import Tuple

from cosecha.reaping.nwis import USGSNWISReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

from shared.utils import parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=2)
USGS_PARAMETERS = ["00060", "00065", "00045", "62614", "62615", "62616", "62617", "62618"] # Default parameters: Discharge, Gage Height, Precipitation, and all lake level parameters
DEFAULT_PAYLOAD = "s3://flood-warning/staging/payloads/usgs-v0.2.parquet"

USGS_VARIABLE_MAPPING = {
    '00060': 'discharge',
    '00065': 'gage_height',
    '00045': 'precipitation',
    "62614": "lake_elev_ngvd29_ft", 
    "62615": "lake_elev_navd88_ft", 
    "62616": "lake_elev_ngvd29_m", 
    "62617": "lake_elev_navd88_m", 
    "62618": "lake_stage_m"
}

USGS_COLUMN_RENAMES = {
    'monitoring_location_id': 'site_id',
    'parameter_code': 'variable',
    'time': 'timestamp',
    'value': 'value',
    'approval_status': 'qualifier',
}

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to the DataFrame, such as renaming columns and mapping variable names."""

    df = df.rename(columns=USGS_COLUMN_RENAMES)
    df['variable'] = df['variable'].map(USGS_VARIABLE_MAPPING)
    df = df.dropna(subset=['value'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df[['provider', 'site_id']] = df['site_id'].str.split('-', n=1, expand=True)
    
    return df


def read_payload_parquet(file_path: str) -> list[str]:
    """Reads the payload Parquet file and extracts a list of gage IDs."""
    try:
        if not file_path.lower().endswith(('.parquet', '.pq')):
            raise ValueError(f"Payload file must be a Parquet file (.parquet or .pq suffix): {file_path}")

        df = pd.read_parquet(file_path)
        if 'gage_id' not in df.columns:
            raise ValueError(f"Parquet payload {file_path} must contain a 'gage_id' column.")
        
        # Ensure gage_ids are strings
        df['gage_id'] = df['gage_id'].astype(str)
        return df['gage_id'].tolist()
    except Exception:
        logging.exception("Failed to read payload")
        raise

def fetch_usgs_data(gage_ids: list[str], usgs_params: list[str], start_date: datetime, end_date: datetime) -> Tuple[USGSNWISReaper, pd.DataFrame]:
    """Fetch raw USGS data from the API."""
    logging.info(f"Fetching USGS data for {len(gage_ids)} sites...")
    
    usgs_reaper = USGSNWISReaper(
        site_ids=gage_ids, 
        start_date=start_date.isoformat(), 
        end_date=end_date.isoformat(), 
        parameter_code=usgs_params,
        transformations={
            'filter_columns': ['monitoring_location_id', 'parameter_code', 'time', 'value', 'approval_status']
        }
    )
    
    return usgs_reaper, usgs_reaper.reap()

def main(gage_ids: list[str], usgs_params: list[str], start_date: datetime, end_date: datetime, output_path: str) -> None:
    """Orchestrates the data extraction and saving."""
    logging.info(f"Fetching USGS data for {len(gage_ids)} sites from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}...")
    
    usgs_reaper, usgs_data = fetch_usgs_data(gage_ids, usgs_params, start_date, end_date)

    if usgs_data.empty:
        raise ValueError("No USGS data available for the given parameters.")

    # Save output
    logging.info(f"Transforming {len(usgs_data)} records...")
    usgs_reaper.data = transform(usgs_data)
    logging.info(f"Saving records to {output_path}...")
    usgs_reaper.sow_to_parquet(file_path=output_path)
    
    logging.info("Operation complete")


def handler(event = None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    if event is None:
        event = {}

    payload = event.get("payload")
    if not payload:
        payload = DEFAULT_PAYLOAD

    gage_ids = read_payload_parquet(payload)

    usgs_params = event.get("usgs_params", USGS_PARAMETERS)

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

    output_path = event.get("output_path", generate_default_path("usgs", end_date, "parquet"))

    main(
        gage_ids=gage_ids,
        usgs_params=usgs_params,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "USGS data retrieval complete.",
            "output_path": output_path,
            "number_of_sites": len(gage_ids),
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat()
        }
    }