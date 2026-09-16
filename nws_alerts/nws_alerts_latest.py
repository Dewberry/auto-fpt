"""Reaper for fetching active NWS flood alerts, clipping to NCTCOG MPA, and saving to Parquet."""

import logging
from datetime import datetime, timezone

import geopandas as gpd
from nws_alerts_reaper import NWSAlertReaper

from shared.utils import generate_default_path

logging.basicConfig(level=logging.INFO, force=True)

USER_AGENT = "auto-fpt (sjanke@dewberry.com)"
DEFAULT_AREA = "TX" 
DEFAULT_BOUNDARY = "s3://flood-warning/staging/payloads/metropolitan_planning_area.pq"


def transform(gdf: gpd.GeoDataFrame, boundary_path: str | None = None) -> gpd.GeoDataFrame:
    """Apply transformations and optionally clip alerts to boundary."""
    if boundary_path:
        if boundary_path.endswith(".pq") or boundary_path.endswith(".parquet"):
            boundary = gpd.read_parquet(boundary_path)
        else:
            boundary = gpd.read_file(boundary_path)
        gdf = gpd.clip(gdf, boundary)

    # Convert NaN to None for nullable fields so downstream
    # serialisation (parquet, JSON schema validation) sees proper nulls.
    nullable = ["onset", "ends", "headline"]
    for col in nullable:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(object).where(gdf[col].notna(), None)

    return gdf


def main(
    output_path: str,
    area: str | list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    boundary_path: str | None = None,
) -> None:
    """Fetch alerts, optionally clip to boundary, and save."""
    logging.info("Fetching NWS flood alerts...")
    reaper = NWSAlertReaper(
        user_agent=USER_AGENT,
        area=area,
        start=start,
        end=end,
    )
    gdf = reaper.reap()

    if gdf.empty:
        raise ValueError("No NWS flood alerts available.")

    logging.info(f"Transforming {len(gdf)} features...")
    gdf = transform(gdf, boundary_path=boundary_path)

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

    area = event.get("area", DEFAULT_AREA)
    start = event.get("start")
    end = event.get("end")
    boundary_path = event.get("boundary_path", DEFAULT_BOUNDARY)
    now = datetime.now(tz=timezone.utc)
    output_path = event.get("output_path", generate_default_path("nws-alerts", now, "parquet"))

    main(
        output_path=output_path,
        area=area,
        start=start,
        end=end,
        boundary_path=boundary_path,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "NWS flood alert retrieval complete.",
            "output_path": output_path,
            "timestamp": now.isoformat(),
        },
    }