"""Reaper for fetching current Waze traffic alerts, filtering, and saving to Parquet."""

import os
import logging
from datetime import datetime, timezone

import geopandas as gpd
from dotenv import load_dotenv
from waze_reaper import WazeReaper

from shared.utils import generate_default_path

load_dotenv()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.handlers:
    for handler in logger.handlers:
        handler.setLevel(logging.INFO)

PARTNER_ID = os.environ.get("WAZE_PARTNER_ID")
API_TOKEN = os.environ.get("WAZE_API_TOKEN")
DEFAULT_SEARCH_TERM = "flood"
DEFAULT_COLS_TO_REMOVE = ["country"]


def filter_alerts(gdf: gpd.GeoDataFrame, search_term: str) -> gpd.GeoDataFrame:
    """Filter alerts where type, subtype, or reportDescription contains the search term."""
    mask = (
        gdf["type"].str.contains(search_term, case=False, na=False)
        | gdf["subtype"].str.contains(search_term, case=False, na=False)
        | gdf["reportDescription"].str.contains(search_term, case=False, na=False)
    )
    return gdf[mask].reset_index(drop=True)


def transform(gdf: gpd.GeoDataFrame, search_term: str, now: datetime) -> gpd.GeoDataFrame:
    """Apply transformations: filter alerts, drop columns, and add time_retrieved."""
    logging.info(f"Filtering {len(gdf)} alerts for '{search_term}'...")
    gdf = filter_alerts(gdf, search_term)

    if gdf.empty:
        logging.warning(f"No alerts found for search term '{search_term}'.")
        return gdf

    gdf = gdf.drop(columns=DEFAULT_COLS_TO_REMOVE, errors="ignore")
    gdf["time_retrieved"] = now.isoformat(timespec="seconds")

    return gdf


def main(
    partner_id: str,
    api_token: str,
    output_path: str,
    now: datetime,
    search_term: str = DEFAULT_SEARCH_TERM,
) -> None:
    """Fetch current alerts, transform, and save."""
    logging.info("Fetching Waze alerts...")
    reaper = WazeReaper(partner_id=partner_id, api_token=api_token)
    gdf = reaper.reap()

    if gdf.empty:
        raise ValueError("No Waze alert data available.")

    gdf = transform(gdf, search_term, now)

    if gdf.empty:
        return

    logging.info(f"Saving {len(gdf)} features to {output_path}...")
    gdf.to_parquet(output_path, index=False)
    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler."""
    if event is None:
        event = {}

    search_term = event.get("search_term", DEFAULT_SEARCH_TERM)

    now = datetime.now(tz=timezone.utc)
    
    output_path = event.get("output_path", generate_default_path("waze", now, "parquet"))

    main(
        partner_id=PARTNER_ID,
        api_token=API_TOKEN,
        output_path=output_path,
        now=now,
        search_term=search_term,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "Waze data retrieval complete.",
            "output_path": output_path,
            "timestamp": now.isoformat(),
        },
    }