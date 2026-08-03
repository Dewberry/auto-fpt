import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

import boto3
import pandas as pd
import s3fs
import xarray as xr

from etl.shared._logging import logger


def load_config_parquet(
    sources: Union[str, list[str]], config_path: Union[str, Path]
) -> dict:
    """
    Load config entries for one or more sources from a parquet config file.

    Args:
        sources: A source name or list of source names (e.g. "hrrr" or ["hrrr", "mrms"]).
            Pass "all" to return all configured sources.
        config_path: Path to the parquet config file.

    Returns:
        A dict keyed by source name, each value being the config dict for that source.

    Raises:
        KeyError: If a source is not found in the config file.
    """
    if isinstance(sources, str):
        sources = [sources]

    df = pd.read_parquet(config_path)

    all_sources = {}
    for row in df.to_dict(orient="records"):
        source_name = row.get("source")
        if pd.isna(source_name):
            continue

        # Drop keys with missing values so NaN/None do not persist in configs.
        all_sources[source_name] = {
            k: v for k, v in row.items() if k != "source" and pd.notna(v)
        }

    # Allow selecting every configured source with a single input.
    if any(
        isinstance(source, str) and source.strip().lower() == "all"
        for source in sources
    ):
        return all_sources

    result = {}
    for source in sources:
        if source not in all_sources:
            raise KeyError(f"Source '{source}' not found in {config_path}")
        result[source] = all_sources[source]

    return result


def load_schema(schema_path: str) -> dict:
    """Load JSON schema for validation."""
    with open(schema_path) as f:
        return json.load(f)


def _table_to_records(table) -> list[dict]:
    return [
        {col: table.column(col)[i].as_py() for col in table.schema.names}
        for i in range(len(table))
    ]


def validate_table(
    table, schema_path: Union[str, Path], sample_size: int = 100
) -> None:
    """Validate a sample of rows in a PyArrow table against a JSON schema file."""
    import random

    import jsonschema

    schema = load_schema(str(schema_path))
    validator = jsonschema.Draft7Validator(schema)

    n = len(table)
    indices = random.sample(range(n), min(sample_size, n))
    col_names = table.schema.names

    errors = []
    for idx in indices:
        record = {col: table.column(col)[idx].as_py() for col in col_names}
        errs = list(validator.iter_errors(record))
        if errs:
            errors.append(f"Row {idx}: {errs[0].message}")

    if errors:
        raise ValueError(
            f"Schema validation failed for {len(errors)} row(s):\n" + "\n".join(errors)
        )
    logger.info(f"Schema validation passed (sampled {len(indices)}/{n} rows)")


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket and key."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def delete_s3_files(fs: s3fs.S3FileSystem, file_paths: list[str]) -> None:
    """
    Deletes a list of files from S3.

    Args:
        fs (s3fs.S3FileSystem): The filesystem instance to use.
        file_paths (list[str]): List of S3 URIs to delete.
    """
    if not file_paths:
        logger.info("No files provided for deletion.")
        return

    _ = fs  # Retained for backward compatibility with existing call sites.
    s3_client = boto3.client("s3")

    deleted_count = 0
    for file_path in file_paths:
        try:
            logger.info(f"Deleting file: {file_path}")
            bucket, key = _parse_s3_uri(file_path)
            response = s3_client.delete_object(Bucket=bucket, Key=key)
            status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code is None or not (200 <= status_code < 300):
                raise RuntimeError(f"DeleteObject returned status code {status_code}")
            deleted_count += 1
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")

    logger.info(f"Successfully deleted {deleted_count}/{len(file_paths)} files.")


def find_files(fs: s3fs.S3FileSystem, s3_path: str, file_pattern: str = None, find_latest: bool = False) -> list[str]:
    """
    Utility method to find files in the given S3 path using s3fs.

    Args:
        fs (s3fs.S3FileSystem): The filesystem instance to use.
        s3_path (str): The S3 path to search for files.
        file_pattern (str): Optional filename to match recursively (e.g. "forecast.parquet").
            When provided, uses glob to find matching files in all subdirectories.
        find_latest (bool): If True, return only the file
            with the most recent path-derived timestamp, based on the extract_run_time function.
    """
    s3_path_cleaned = s3_path.replace("s3://", "")
    if file_pattern:
        glob_pattern = f"{s3_path_cleaned}/**/{file_pattern}"
        files = fs.glob(glob_pattern)
    else:
        files = fs.ls(s3_path_cleaned)
    if not files:
        raise FileNotFoundError(f"No files found at {s3_path}")

    s3_paths = [f"s3://{f}" for f in files]

    if find_latest:
        s3_paths = [max(s3_paths, key=extract_run_time)]

    return s3_paths


def read_s3_files(fs: s3fs.S3FileSystem, prefix: Union[str, list]) -> list[str]:
    """Read files from S3 based on a prefix or list of prefixes."""

    logger.info(f"Reading files from S3 with prefix(es): {prefix}")
    if isinstance(prefix, list):
        s3_paths = []
        for p in prefix:
            s3_paths.extend(find_files(fs, p))
    elif isinstance(prefix, str):
        s3_paths = find_files(fs, prefix)
    else:
        raise ValueError(
            f"Invalid prefix type: {type(prefix)}. Must be str or list of prefix."
        )

    logger.info(f"Found {len(s3_paths)} files at prefix(es) {prefix}")
    return s3_paths


def upload_table_to_s3(table, s3_path: str) -> None:
    """Write a PyArrow table to a temp Parquet file and upload to S3."""
    import pyarrow.parquet as pq

    logger.info(f"Uploading parquet to {s3_path}...")
    bucket, key = _parse_s3_uri(s3_path)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        pq.write_table(table, tmp.name, compression="zstd", compression_level=3)
        boto3.client("s3").upload_file(tmp.name, bucket, key)
    logger.info(f"Uploaded to {s3_path}")


def upload_ds_to_s3(ds: xr.Dataset, s3_path: str, compression_lvl: int = 2) -> None:
    """Write dataset to a temp NetCDF file and upload to S3."""

    logger.info(f"Uploading dataset to {s3_path}...")

    path = s3_path.replace("s3://", "")
    s3_bucket, s3_key = path.split("/", 1)
    with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
        encoding = {
            var: {"zlib": True, "complevel": compression_lvl} for var in ds.data_vars
        }
        ds.to_netcdf(tmp.name, engine="h5netcdf", encoding=encoding)
        boto3.client("s3").upload_file(tmp.name, s3_bucket, s3_key)

    logger.info(f"Uploaded to {s3_path}")


def extract_run_time(s3_path: str) -> "pd.Timestamp":
    """
    Extract a model run time from an S3 path with date-partitioned structure.

    Expects paths like: s3://bucket/prefix/{year}/{month}/{day}/{hour}/filename

    Returns:
        UTC-aware pd.Timestamp parsed from the path components.
    """
    parts = s3_path.rstrip('/').split('/')
    year, month, day, hour = parts[-5], parts[-4], parts[-3], parts[-2]
    return pd.Timestamp(f"{year}-{month}-{day}T{hour}:00:00", tz="UTC")


def parse_tz_aware_time(time_str: str) -> datetime:
    """Parses an ISO datetime string and ensures it is timezone-aware, returning UTC."""
    event_time = datetime.fromisoformat(time_str)
    if event_time.tzinfo is None:
        raise ValueError(
            f"{event_time} must be tz-aware (e.g. '2026-04-20T05:00:00-05:00' or '2026-01-01T00:00Z' for UTC)."
        )
    return event_time.astimezone(timezone.utc)
