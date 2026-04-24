import os
import boto3
import tempfile
import logging
from urllib.parse import urlparse
from datetime import datetime, timezone

def save_netcdf_to_s3(reaper, output_path: str) -> None:
    """Saves a NetCDF file to a temporary local file before uploading to S3 to bypass s3fs limitations."""
    if output_path.startswith("s3://"):
        parsed = urlparse(output_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp_path = tmp.name
            
        logging.info(f"Saving temporary output to {tmp_path}...")
        reaper.sow_to_netcdf(file_path=tmp_path)
        
        logging.info(f"Uploading to {output_path}...")
        s3 = boto3.client('s3')
        s3.upload_file(tmp_path, bucket, key)
        os.remove(tmp_path)
    else:
        logging.info(f"Saving output to {output_path}...")
        reaper.sow_to_netcdf(file_path=output_path)

def parse_tz_aware_time(time_str: str) -> datetime:
    """Parses an ISO datetime string and ensures it is timezone-aware, returning UTC."""
    event_time = datetime.fromisoformat(time_str)
    if event_time.tzinfo is None:
        raise ValueError(f"{event_time} must be tz-aware (e.g. '2026-04-20T05:00:00-05:00' or '2026-01-01T00:00Z' for UTC).")
    return event_time.astimezone(timezone.utc)
