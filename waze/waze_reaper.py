"""Waze alerts reaper.

Harvests current traffic alert data from the Waze Partner Feed API.
The API provides a live snapshot of alerts (accidents, road closures, hazards, etc.).
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import tiny_retriever

__all__ = ["WazeReaper"]

BASE_URL = "https://www.waze.com/partnerhub-api/partners"


class WazeReaper:
    """Reaper for Waze traffic alerts.

    Parameters
    ----------
    partner_id : str
        Waze partner ID.
    api_token : str
        Waze feed token/UUID.

    Examples
    --------
    >>> reaper = WazeReaper(partner_id="12345", api_token="abc-def")
    >>> gdf = reaper.reap()
    >>> gdf = reaper.reap(alert_types=["ROAD_CLOSED", "ACCIDENT"])
    """

    def __init__(self, partner_id: str, api_token: str) -> None:
        self.partner_id = partner_id
        self.api_token = api_token

    def _fetch(self) -> dict:
        """Fetch the current alerts JSON from the API."""
        url = f"{BASE_URL}/{self.partner_id}/waze-feeds/{self.api_token}?format=1"
        return tiny_retriever.fetch(url, "json")

    def reap(
        self,
        alert_types: str | list[str] | None = None,
    ) -> gpd.GeoDataFrame:
        """Retrieve current traffic alerts as a GeoDataFrame.

        Parameters
        ----------
        alert_types : str, list[str], or None, optional
            Filter to specific alert types (e.g., "ROAD_CLOSED", "ACCIDENT",
            "HAZARD"). If None, returns all alerts.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame with alert features and point geometry.
        """
        data = self._fetch()
        alerts = data.get("alerts", [])

        geometry = [Point(a["location"]["x"], a["location"]["y"]) for a in alerts]
        gdf = gpd.GeoDataFrame(alerts, geometry=geometry, crs="EPSG:4326")

        gdf = gdf.drop(columns=["location"], errors="ignore")

        gdf["timestamp"] = pd.to_datetime(gdf["pubMillis"], unit="ms", utc=True)
        gdf = gdf.drop(columns=["pubMillis"], errors="ignore")
        
        cols = [c for c in gdf.columns if c != "geometry"] + ["geometry"]
        gdf = gdf[cols]

        if alert_types is not None:
            if isinstance(alert_types, str):
                alert_types = [alert_types]
            gdf = gdf[gdf["type"].isin(alert_types)].reset_index(drop=True)

        return gdf