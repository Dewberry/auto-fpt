"""DriveTexas road conditions reaper.

Harvests current road condition data from the TxDOT DriveTexas API.
The API only provides a live snapshot (refreshed every 5 minutes).
"""

from __future__ import annotations

import geopandas as gpd
import tiny_retriever

__all__ = ["DriveTexasReaper"]

BASE_URL = "https://api.drivetexas.org/api/conditions.geojson"


class DriveTexasReaper:
    """Reaper for TxDOT DriveTexas road conditions.

    Parameters
    ----------
    api_key : str
        API key for the DriveTexas API.

    Examples
    --------
    >>> reaper = DriveTexasReaper(api_key="your-key-here")
    >>> gdf = reaper.reap()
    >>> gdf = reaper.reap(conditions=["Flooding", "Closure"])
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def _fetch(self) -> dict:
        """Fetch the current conditions GeoJSON from the API."""
        url = f"{BASE_URL}?key={self.api_key}"
        return tiny_retriever.fetch(url, "json")

    def reap(
        self,
        conditions: str | list[str] | None = None,
    ) -> gpd.GeoDataFrame:
        """Retrieve current road conditions as a GeoDataFrame.

        Parameters
        ----------
        conditions : str, list[str], or None, optional
            Filter to specific condition types (e.g., "Flooding", "Ice",
            "Construction"). If None, returns all conditions.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame with road condition features and a geometry column.
        """
        data = self._fetch()
        gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
        cols = [c for c in gdf.columns if c != "geometry"] + ["geometry"]
        gdf = gdf[cols]

        if conditions is not None:
            if isinstance(conditions, str):
                conditions = [conditions]
            gdf = gdf[gdf["condition"].isin(conditions)].reset_index(drop=True)

        return gdf
