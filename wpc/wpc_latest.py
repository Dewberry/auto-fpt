"""Reaper for fetching latest WPC Excessive Rainfall Outlook data, transforming it, and saving to GeoParquet."""

import logging
from datetime import date, datetime, timezone

import geopandas as gpd
import pandas as pd

from wpc_reaper import WPCReaper
from shared.utils import parse_tz_aware_time, generate_default_path

logging.basicConfig(level=logging.INFO, force=True)

DEFAULT_ISSUANCE = "latest"
DEFAULT_DAYS = "day1"

# Columns to drop from raw shapefile output
DROP_COLUMNS = ["dn", "Snippet", "issuance"]

# Rename shapefile columns to clean lowercase names
COLUMN_RENAMES = {
    "PRODUCT": "product",
    "VALID_TIME": "valid_time",
    "OUTLOOK": "outlook",
    "ISSUE_TIME": "issue_time",
    "START_TIME": "start_time",
    "END_TIME": "end_time",
}


def validate_issue_date(gdf: gpd.GeoDataFrame, expected_date: date) -> None:
    """Verify that the ISSUE_TIME in the data matches the expected date.

    Raises
    ------
    ValueError
        If any record's issue date does not match the expected date.
    """
    issue_dates = pd.to_datetime(gdf["ISSUE_TIME"]).dt.date.unique()
    mismatched = [d for d in issue_dates if d != expected_date]
    if mismatched:
        raise ValueError(
            f"Issue date mismatch: expected {expected_date}, "
            f"but data contains issue dates {mismatched}"
        )


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clean and transform raw ERO GeoDataFrame."""
    gdf = gdf.copy()

    # Drop unwanted columns
    gdf = gdf.drop(columns=[c for c in DROP_COLUMNS if c in gdf.columns])

    # Rename columns to lowercase
    gdf = gdf.rename(columns=COLUMN_RENAMES)

    # Clean outlook: strip parenthetical text, e.g. "Marginal (At Least 5%)" → "Marginal"
    gdf["outlook"] = gdf["outlook"].str.replace(r"\s*\(.*\)$", "", regex=True)

    # Convert timestamps to ISO 8601
    gdf["issue_time"] = gpd.pd.to_datetime(gdf["issue_time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    gdf["start_time"] = gpd.pd.to_datetime(gdf["start_time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    gdf["end_time"] = gpd.pd.to_datetime(gdf["end_time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Reorder columns with geometry last
    cols = [c for c in gdf.columns if c != "geometry"] + ["geometry"]
    return gdf[cols]


def main(
    ref_date: datetime,
    issuance: str,
    days: str | list[str],
    output_path: str,
) -> None:
    """Orchestrates data extraction, transformation, and saving."""
    logging.info(f"Fetching WPC ERO data for {ref_date.date()} ({issuance} issuance)...")

    reaper = WPCReaper(days=days, date=ref_date.date(), issuance=issuance)
    gdf = reaper.reap()

    if gdf.empty:
        raise ValueError(f"No WPC ERO data available for {ref_date.date()} ({issuance}).")

    validate_issue_date(gdf, ref_date.date())

    logging.info(f"Transforming {len(gdf)} features...")
    transformed = transform(gdf)
    logging.info(f"Saving {len(transformed)} features to {output_path}...")
    transformed.to_parquet(output_path, index=False, compression="snappy")

    logging.info("Operation complete")


def handler(event=None, context=None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""

    if event is None:
        event = {}

    issuance = event.get("issuance", DEFAULT_ISSUANCE)
    days = event.get("days", DEFAULT_DAYS)

    if event.get("ref_time"):
        ref_date = parse_tz_aware_time(event["ref_time"])
    else:
        ref_date = datetime.now(tz=timezone.utc)

    output_path = event.get("output_path", generate_default_path("wpc-ero", ref_date, "parquet"))

    main(
        ref_date=ref_date,
        issuance=issuance,
        days=days,
        output_path=output_path,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "WPC ERO data retrieval complete.",
            "output_path": output_path,
            "ref_date": ref_date.date().isoformat(),
            "issuance": issuance,
        },
    }
