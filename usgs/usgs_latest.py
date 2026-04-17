from cosecha.reaping.nwis import USGSNWISReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

configure_logger(level="INFO")

DEFAULT_OUTPUT_PREFIX = "s3://flood-warning/staging/temporary/usgs/"
DEFAULT_LOOKBACK = timedelta(hours=2)
DEFAULT_OUTPUT_TIMESTAMP_FMT = '%Y%m%d_%H%M'
USGS_PARAMETERS = ["00060", "00065", "00045", "62614", "62615", "62616", "62617", "62618"] # Default parameters: Discharge, Gage Height, Precipitation, and all lake level parameters
DEFAULT_PAYLOAD = "s3://flood-warning/staging/payloads/usgs-v0.1.parquet"

def read_payload_parquet(file_path: str) -> list[str]:
    """Reads the payload Parquet file and extracts a list of gage IDs."""
    df = pd.read_parquet(file_path)
    if 'gage_id' not in df.columns:
        raise ValueError(f"Parquet payload {file_path} must contain a 'gage_id' column.")
    
    # Ensure gage_ids are strings
    df['gage_id'] = df['gage_id'].astype(str)
    return df['gage_id'].tolist()


def main(gage_ids: list[str], usgs_params: list[str], start_date: datetime, end_date: datetime, output_path: str) -> None:
    """Orchestrates the data extraction and saving."""
    logging.info(f"Fetching USGS data for {len(gage_ids)} sites from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}...")
    
    usgs_reaper = USGSNWISReaper(
        site_ids=gage_ids, 
        start_date=start_date.isoformat(), 
        end_date=end_date.isoformat(), 
        parameter_code = usgs_params,
        transformations={
            'filter_columns': ['monitoring_location_id', 'parameter_code', 'time', 'value', 'approval_status']
        }
    )
    try:
        usgs_data = usgs_reaper.reap()
    except Exception as e:
        raise RuntimeError(
            f"NWIS fetch failed for {len(gage_ids)} sites, {start_date.isoformat()}-{end_date.isoformat()}"
        ) from e

    if usgs_data.empty:
        logging.warning("No data retrieved from USGS for the requested sites/timeframe.")
        raise ValueError("No USGS data available for the given parameters.")

    # Save output
    logging.info(f"Saving {len(usgs_data)} records to {output_path}...")
    usgs_reaper.data = usgs_data
    usgs_reaper.sow_to_parquet(file_path=output_path)
    
    logging.info("Operation complete")


def handler(event = {}, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    payload = event.get("payload")
    if not payload:
        payload = DEFAULT_PAYLOAD

    try:
        gage_ids = read_payload_parquet(payload)
    except Exception as e:
        logging.error(f"Failed to read payload: {e}.")
        raise

    usgs_params = event.get("usgs_params", USGS_PARAMETERS)

    if event.get("end_time"):
        end_date = datetime.fromisoformat(event["end_time"])
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(tz=timezone.utc)

    if event.get("start_time"):
        start_date = datetime.fromisoformat(event["start_time"])
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
    else:
        start_date = end_date - DEFAULT_LOOKBACK

    if start_date >= end_date:
        raise ValueError(f"start_time ({start_date}) must be before end_time ({end_date}).")

    output_path = event.get("output_path", f"{DEFAULT_OUTPUT_PREFIX}{end_date.strftime(DEFAULT_OUTPUT_TIMESTAMP_FMT)}.parquet")

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
            "sites": len(gage_ids),
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat()
        }
    }