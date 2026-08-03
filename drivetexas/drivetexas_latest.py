"""Reaper for fetching current DriveTexas road conditions, transforming, and saving to Parquet."""

import os
import logging
from datetime import datetime, timezone

import geopandas as gpd
from dotenv import load_dotenv
from drivetexas_reaper import DriveTexasReaper

from shared.utils import generate_default_path

load_dotenv()
logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("DRIVETEXAS_API_KEY")
DEFAULT_BOUNDARY = "s3://flood-warning/staging/payloads/metropolitan_planning_area.pq"
DEFAULT_CONDITIONS = ["Flooding", "Closure"]


def transform(gdf: gpd.GeoDataFrame, boundary_path: str) -> gpd.GeoDataFrame:
    """Clip conditions to boundary area."""
    if boundary_path.endswith(".pq") or boundary_path.endswith(".parquet"):
        boundary = gpd.read_parquet(boundary_path)
    else:
        boundary = gpd.read_file(boundary_path)
    gdf = gpd.clip(gdf, boundary)
    return gdf


def main(
    api_key: str,
    boundary_path: str,
    output_path: str,
    conditions: list[str] | None = None,
) -> None:
    """Fetch current conditions, transform, and save."""
    logging.info("Fetching DriveTexas conditions...")
    reaper = DriveTexasReaper(api_key=api_key)
    gdf = reaper.reap(conditions=conditions)

    if gdf.empty:
        raise ValueError("No DriveTexas data available for the given conditions.")

    logging.info(f"Transforming {len(gdf)} features...")
    gdf = transform(gdf, boundary_path)

    if gdf.empty:
        logging.warning("No features remain after clipping to boundary.")
        return

    logging.info(f"Saving {len(gdf)} features to {output_path}...")
    gdf.to_parquet(output_path, index=False)
    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler."""
    if event is None:
        event = {}

    api_key = event.get("api_key") or API_KEY
    if not api_key:
        raise ValueError("api_key must be provided via event payload or DRIVETEXAS_API_KEY environment variable.")

    conditions = event.get("conditions", DEFAULT_CONDITIONS)
    boundary_path = event.get("boundary_path", DEFAULT_BOUNDARY)
    now = datetime.now(tz=timezone.utc)
    output_path = event.get("output_path", generate_default_path("drivetexas", now, "parquet"))

    main(
        api_key=api_key,
        boundary_path=boundary_path,
        output_path=output_path,
        conditions=conditions,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "DriveTexas data retrieval complete.",
            "output_path": output_path,
            "timestamp": now.isoformat(),
        },
    }