"""Reaper for fetching latest USACE reservoir data for specified site IDs and parameters, transforming it, and saving to Parquet."""

from typing import Tuple

from cosecha.reaping.usace import ReservoirReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

from shared.utils import parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_LOOKBACK = timedelta(hours=3)
USACE_PARAMETERS = ["outflow", "inflow"]
DEFAULT_PAYLOAD = "s3://flood-warning/staging/payloads/usace-res-v0.1.parquet"

USACE_COLUMN_RENAMES = {
    'datetime': 'timestamp',
}


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to the DataFrame, such as renaming columns and adding provider."""

    df = df.rename(columns=USACE_COLUMN_RENAMES)
    df = df.drop(columns=['unit'], errors='ignore')
    df = df.dropna(subset=['value'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['provider'] = 'usace'

    return df


def read_payload_parquet(file_path: str) -> list[str]:
    """Reads the payload Parquet file and extracts a list of site IDs."""
    try:
        if not file_path.lower().endswith(('.parquet', '.pq')):
            raise ValueError(f"Payload file must be a Parquet file (.parquet or .pq suffix): {file_path}")

        df = pd.read_parquet(file_path)
        if 'site_id' not in df.columns:
            raise ValueError(f"Parquet payload {file_path} must contain a 'site_id' column.")

        # Ensure site_ids are strings
        df['site_id'] = df['site_id'].astype(str)
        return df['site_id'].tolist()
    except Exception:
        logging.exception("Failed to read payload")
        raise


def fetch_usace_data(site_ids: list[str], params: list[str], start_date: datetime, end_date: datetime) -> Tuple[ReservoirReaper, pd.DataFrame]:
    """Fetch raw USACE reservoir data from the API."""
    logging.info(f"Fetching USACE data for {len(site_ids)} sites...")

    reaper = ReservoirReaper(
        site_ids=site_ids,
        params=params,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    return reaper, reaper.reap()


def main(site_ids: list[str], params: list[str], start_date: datetime, end_date: datetime, output_path: str) -> None:
    """Orchestrates the data extraction and saving."""
    logging.info(f"Fetching USACE data for {len(site_ids)} sites from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}...")

    reaper, data = fetch_usace_data(site_ids, params, start_date, end_date)

    if data.empty:
        raise ValueError("No USACE data available for the given parameters.")

    # Save output
    logging.info(f"Transforming {len(data)} records...")
    reaper.data = transform(data)
    logging.info(f"Saving records to {output_path}...")
    reaper.sow_to_parquet(file_path=output_path)

    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    if event is None:
        event = {}

    payload = event.get("payload")
    if not payload:
        payload = DEFAULT_PAYLOAD

    site_ids = read_payload_parquet(payload)

    params = event.get("params", USACE_PARAMETERS)

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

    output_path = event.get("output_path", generate_default_path("usace", end_date, "parquet"))

    main(
        site_ids=site_ids,
        params=params,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "USACE reservoir data retrieval complete.",
            "output_path": output_path,
            "number_of_sites": len(site_ids),
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat()
        }
    }