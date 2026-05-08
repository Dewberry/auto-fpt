import json
import os
import boto3
import tempfile
import logging
from urllib.parse import urlparse
from datetime import datetime, timezone

from shared.constants import DEFAULT_OUTPUT_PREFIX, DEFAULT_OUTPUT_TIMESTAMP_FMT


def generate_default_path(folder: str, time_ref: datetime, extension: str) -> str:
    """Generates a default output path for the given source folder and reference time."""
    return f"{DEFAULT_OUTPUT_PREFIX}{folder}/{time_ref.strftime(DEFAULT_OUTPUT_TIMESTAMP_FMT)}.{extension}"


def save_netcdf_to_s3(reaper, output_path: str) -> None:
    """Saves a NetCDF file to a temporary local file before uploading to S3 to bypass s3fs limitations."""
    if output_path.startswith("s3://"):
        parsed = urlparse(output_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        with tempfile.NamedTemporaryFile(dir="/tmp", suffix=".nc", delete=True) as tmp:
            logging.info(f"Saving temporary output to {tmp.name}...")
            reaper.sow_to_netcdf(file_path=tmp.name)
            logging.info(f"Uploading to {output_path}...")
            boto3.client('s3').upload_file(tmp.name, bucket, key)
        return

    logging.info(f"Saving output to {output_path}...")
    reaper.sow_to_netcdf(file_path=output_path)


def parse_tz_aware_time(time_str: str) -> datetime:
    """Parses an ISO datetime string and ensures it is timezone-aware, returning UTC."""
    event_time = datetime.fromisoformat(time_str)
    if event_time.tzinfo is None:
        raise ValueError(f"{event_time} must be tz-aware (e.g. '2026-04-20T05:00:00-05:00' or '2026-01-01T00:00Z' for UTC).")
    return event_time.astimezone(timezone.utc)


def load_schema(schema_path: str) -> dict:
    """Load JSON schema for validation."""
    with open(schema_path) as f:
        return json.load(f)