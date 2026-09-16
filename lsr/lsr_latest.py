"""Reaper for fetching latest NWS Local Storm Reports (LSR), transforming them, and saving to Parquet."""

import logging
from datetime import datetime, timedelta, timezone

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from cosecha.exceptions import DataNotFoundError
from cosecha.reaping.lsr import LSRReaper

from shared.utils import generate_default_path, parse_tz_aware_time

logging.basicConfig(level=logging.INFO, force=True)

DEFAULT_WFOS = ["FWD"]
DEFAULT_EVENT_TYPES = ["FLOOD", "FLASH FLOOD"]
DEFAULT_LOOKBACK = timedelta(hours=72)

DROP_COLUMNS = ["wfo", "county", "state", "city"]
NULLABLE_COLUMNS = ["magnitude", "unit", "source", "remark", "product_id"]


def transform(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Apply transformations: rename the time column, build point geometry, and normalise nulls."""
    df = df.rename(columns={"valid": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    df = df.drop(columns=["longitude", "latitude", *DROP_COLUMNS], errors="ignore")

    # Convert NaN to None for nullable fields so downstream
    # serialisation (parquet, JSON schema validation) sees proper nulls.
    for col in NULLABLE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(object).where(df[col].notna(), None)

    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")


def main(
    output_path: str,
    wfos: list[str],
    event_types: list[str],
    start_date: datetime,
    end_date: datetime,
) -> int:
    """Fetch local storm reports, transform, and save. Returns the number of records written."""
    logging.info(
        f"Fetching LSR data for wfos={wfos}, event_types={event_types} "
        f"from {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}..."
    )
    reaper = LSRReaper(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        wfos=wfos,
        event_types=event_types,
    )

    try:
        df = reaper.reap()
    except DataNotFoundError:
        logging.info("No LSR reports matched the query; nothing to write.")
        return 0

    if df.empty:
        logging.info("No LSR reports matched the query; nothing to write.")
        return 0

    logging.info(f"Transforming {len(df)} reports...")
    gdf = transform(df)

    logging.info(f"Saving {len(gdf)} features to {output_path}...")
    gdf.to_parquet(output_path, index=False)
    logging.info("Operation complete")
    return len(gdf)


def handler(event=None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""
    if event is None:
        event = {}

    wfos = event.get("wfos", DEFAULT_WFOS)
    event_types = event.get("event_types", DEFAULT_EVENT_TYPES)

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

    output_path = event.get("output_path", generate_default_path("lsr", end_date, "parquet"))

    record_count = main(
        output_path=output_path,
        wfos=wfos,
        event_types=event_types,
        start_date=start_date,
        end_date=end_date,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": (
                "LSR data retrieval complete."
                if record_count
                else "No LSR reports found for the given parameters."
            ),
            "record_count": record_count,
            "output_path": output_path if record_count else None,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        },
    }