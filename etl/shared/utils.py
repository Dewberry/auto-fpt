import s3fs
import boto3
from pathlib import Path
from typing import Union
import json
from urllib.parse import urlparse
import pandas as pd

from etl.shared._logging import logger

def load_config_parquet(sources: Union[str, list[str]], config_path: Union[str, Path]) -> dict:
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
    if any(isinstance(source, str) and source.strip().lower() == "all" for source in sources):
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


def find_files(fs: s3fs.S3FileSystem, s3_path: str) -> list[str]:
    """
    Utility method to find files in the given S3 path using s3fs.

    Args:
        fs (s3fs.S3FileSystem): The filesystem instance to use.
        s3_path (str): The S3 path to search for files.
    """
    s3_path_cleaned = s3_path.replace('s3://', '')
    files = fs.ls(s3_path_cleaned)
    if not files:
        raise FileNotFoundError(f"No files found at {s3_path}")
    return [f's3://{f}' for f in files]


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
        raise ValueError(f"Invalid prefix type: {type(prefix)}. Must be str or list of prefix.")
    
    logger.info(f"Found {len(s3_paths)} files at prefix(es) {prefix}")
    return s3_paths