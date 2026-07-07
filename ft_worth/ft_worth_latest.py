"""Reaper for fetching latest Fort Worth / NCTCOG gage data, transforming it, and saving to Parquet."""

import os
from ft_worth_reaper import FtWorthReaper
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging
from dotenv import load_dotenv

from shared.utils import parse_tz_aware_time, generate_default_path

load_dotenv()
logging.basicConfig(level=logging.INFO)

STAC_PAYLOAD = 's3://flood-warning/stac/ft-worth_gages/ft_worth_gages.parquet'

DEFAULT_LOOKBACK = timedelta(hours=2)
SYSTEM_KEY = os.environ.get("SYSTEM_KEY")
DEFAULT_VARIABLES = ["precip_incremental", "precip_cumulative", "stage"]

INCHES_TO_MM = 25.4
FEET_TO_M = 0.3048

# Map reaper variable names to STAC asset keys
VARIABLE_TO_ASSET_KEY = {
    "precip_incremental": "sensor_class_10",
    "precip_cumulative": "sensor_class_11",
    "stage": "sensor_class_20",
}

FT_WORTH_COLUMN_RENAMES = {
    'data_time': 'timestamp',
    'data_value': 'value',
    'data_quality': 'qualifier',
}


def build_sensor_lookup(stac_df: pd.DataFrame) -> pd.DataFrame:
    """Flatten STAC assets into a lookup table: site_id, sensor_id, sensor_class, iceberg_variable_name."""
    records = []
    for _, row in stac_df.iterrows():
        site_id = str(row["id"])
        assets = row.get("assets") or {}
        for asset_key in VARIABLE_TO_ASSET_KEY.values():
            asset = assets.get(asset_key)
            if asset is None:
                continue
            for s in asset.get("sensors", []):
                records.append({
                    "site_id": site_id,
                    "sensor_id": s["sensor_id"],
                    "sensor_class": int(asset["sensor_class"]),
                    "iceberg_variable_name": s["iceberg_variable_name"],
                })
    return pd.DataFrame(records)



def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations and map variable names via STAC sensor lookup."""

    stac_df = pd.read_parquet(STAC_PAYLOAD)
    sensor_lookup = build_sensor_lookup(stac_df)
    
    df = df.copy()
    df["sensor_id"] = df["sensor_id"].astype(str)
    df["sensor_class"] = df["sensor_class"].astype(int)

    # Merge to get iceberg_variable_name
    df = df.merge(
        sensor_lookup[["site_id", "sensor_id", "sensor_class", "iceberg_variable_name"]],
        on=["site_id", "sensor_id", "sensor_class"],
        how="left",
    )
    # Use iceberg name where available, fall back to original variable
    df["variable"] = df["iceberg_variable_name"].fillna(df["variable"])
    df = df.drop(columns=["iceberg_variable_name"])


    df = df.rename(columns=FT_WORTH_COLUMN_RENAMES)
    df = df.drop(columns=['sensor_id', 'sensor_class', 'raw_value'])
    df = df.dropna(subset=['value'])

    # Convert units to metric: in -> mm, ft -> m
    in_mask = df['units'] == 'in'
    ft_mask = df['units'] == 'ft'
    df.loc[in_mask, 'value'] = df.loc[in_mask, 'value'] * INCHES_TO_MM
    df.loc[ft_mask, 'value'] = df.loc[ft_mask, 'value'] * FEET_TO_M
    df = df.drop(columns=['units'])

    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['provider'] = 'onerain'

    return df


def fetch_ft_worth_data(
    system_key: str,
    variables: list[str],
    start_date: datetime,
    end_date: datetime,
    site_ids: list[str] | None = None,
) -> tuple[FtWorthReaper, pd.DataFrame]:
    """Fetch raw Fort Worth / NCTCOG data from the API."""
    logging.info(f"Fetching Fort Worth data for variables {variables}...")

    reaper = FtWorthReaper(system_key=system_key)
    data = reaper.reap(
        site_ids=site_ids,
        variables=variables,
        start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
        end_date=end_date.strftime('%Y-%m-%d %H:%M:%S'),
    )

    return reaper, data


def main(
    system_key: str,
    variables: list[str],
    start_date: datetime,
    end_date: datetime,
    output_path: str,
    site_ids: list[str] | None = None,
) -> None:
    """Orchestrates the data extraction and saving."""
    logging.info(f"Fetching Fort Worth data from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}...")

    reaper, data = fetch_ft_worth_data(system_key, variables, start_date, end_date, site_ids)

    if data.empty:
        raise ValueError("No Fort Worth data available for the given parameters.")

    logging.info(f"Transforming {len(data)} records...")
    transformed = transform(data)
    logging.info(f"Saving {len(transformed)} records to {output_path}...")
    transformed.to_parquet(output_path, index=False, compression='snappy')

    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    if event is None:
        event = {}

    system_key = event.get("system_key") or SYSTEM_KEY
    if not system_key:
        raise ValueError("system_key must be provided via event payload or SYSTEM_KEY environment variable in .env file.")
    
    variables = event.get("variables", DEFAULT_VARIABLES)
    site_ids = event.get("site_ids")

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

    output_path = event.get("output_path", generate_default_path("ft-worth", end_date, "parquet"))

    main(
        system_key=system_key,
        variables=variables,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
        site_ids=site_ids,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "Fort Worth data retrieval complete.",
            "output_path": output_path,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        }
    }