"""NWS flood alert reaper.

Harvests active flood-related watches, warnings, and advisories from the
National Weather Service (NWS) API.  The `/alerts/active` endpoint returns
a live GeoJSON FeatureCollection of current alerts with polygon geometry.

https://www.weather.gov/documentation/services-web-api
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

import geopandas as gpd
import pandas as pd
import tiny_retriever
from shapely.geometry import shape
from shapely.ops import unary_union

__all__ = ["NWSAlertReaper"]

logger = logging.getLogger(__name__)

ACTIVE_URL = "https://api.weather.gov/alerts/active"
ALERTS_URL = "https://api.weather.gov/alerts"

DEFAULT_EVENTS = [
    "Flood Warning",
    "Flash Flood Warning",
    "Flood Watch",
    "Flash Flood Watch",
    "Flood Advisory",
]

# Columns to keep from the alert properties, in display order.
KEEP_COLUMNS = [
    "area_desc",
    "event",
    "headline",
    "description",
    "severity",
    "certainty",
    "urgency",
    "sender_name",
    "sent",
    "effective",
    "onset",
    "expires",
    "ends",
    "status",
    "message_type",
]


class NWSAlertReaper:
    """Reaper for NWS flood-related alerts.

    Parameters
    ----------
    user_agent : str
        Application identifier sent as the User-Agent header.
        The NWS API requires this for all requests.
        Example: "auto-fpt (contact@example.com)".
    area : str, list[str], or None, optional
        State / territory code(s) to filter alerts server-side
        (e.g. "TX" or ["TX", "OK"]).  If None, alerts for
        the entire US are returned.
    event_types : str, list[str], or None, optional
        NWS event names to query (e.g. "Flood Warning").
        Defaults to DEFAULT_EVENTS.
    start : str or None, optional
        If provided, fetch alerts issued on or after this time
        (uses the /alerts endpoint instead of /alerts/active).
        ISO 8601 format (e.g. "2026-08-20T00:00:00Z").
    end : str or None, optional
        If provided alongside start, fetch alerts issued before
        this time.  ISO 8601 format (e.g. "2026-08-25T00:00:00Z").

    Examples
    --------
    >>> reaper = NWSAlertReaper(user_agent="myapp", area="TX")
    >>> gdf = reaper.reap()
    >>> reaper = NWSAlertReaper(user_agent="myapp", event_types=["Flood Warning"])
    >>> gdf = reaper.reap()
    """

    def __init__(
        self,
        user_agent: str,
        area: str | list[str] | None = None,
        event_types: str | list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.area = [area] if isinstance(area, str) else area

        if event_types is None:
            self.event_types = DEFAULT_EVENTS
        elif isinstance(event_types, str):
            self.event_types = [event_types]
        else:
            self.event_types = list(event_types)

        if end is not None and start is None:
            raise ValueError("'end' requires 'start' to be set.")

        try:
            self.start = pd.to_datetime(start) if start is not None else None
            self.end = pd.to_datetime(end) if end is not None else None
        except Exception as e:
            raise ValueError(f"Could not parse date: {e}") from e

    def _build_url(self) -> str:
        """Construct the request URL with query parameters.

        Uses /alerts/active when no time range is given, otherwise
        /alerts with start / end parameters.
        """
        params: dict[str, str] = {
            "status": "actual",
            "event": ",".join(self.event_types),
        }
        if self.area:
            params["area"] = ",".join(self.area)

        if self.start is not None:
            params["start"] = self.start.isoformat()
            if self.end is not None:
                params["end"] = self.end.isoformat()
            base = ALERTS_URL
        else:
            base = ACTIVE_URL

        return f"{base}?{urlencode(params)}"

    def _fetch(self, url: str) -> dict:
        """Fetch the GeoJSON response from the NWS API."""
        return tiny_retriever.fetch(
            url,
            "json",
            request_kwargs={"headers": {"User-Agent": self.user_agent}},
        )

    def _resolve_zone_geometries(self, features: list[dict]) -> list[dict]:
        """Fill in null geometries by fetching and unioning affected zone polygons.

        Zone-based alerts (watches, advisories, river warnings) are issued
        without polygon geometry.  Their affected zones are listed in
        properties.affectedZones as API URLs.  This method fetches those
        zone geometries in parallel and unions them into a single shape
        per alert.
        """
        null_indices = [
            i for i, f in enumerate(features) if f.get("geometry") is None
        ]
        if not null_indices:
            return features

        # Collect all unique zone URLs we need to fetch.
        zone_urls: list[str] = []
        for i in null_indices:
            urls = features[i].get("properties", {}).get("affectedZones", [])
            zone_urls.extend(urls)

        unique_urls = list(dict.fromkeys(zone_urls))  # dedupe, preserve order
        if not unique_urls:
            return features

        logger.info(f"Resolving {len(unique_urls)} zone geometries for {len(null_indices)} zone-based alerts...")
        headers = {"User-Agent": self.user_agent}
        kwargs_list = [{"headers": headers}] * len(unique_urls)

        try:
            zone_results = tiny_retriever.fetch(unique_urls, "json", request_kwargs=kwargs_list)
        except Exception:
            logger.warning("Failed to fetch zone geometries; zone-based alerts will have null geometry.", exc_info=True)
            return features

        # Build a lookup from zone URL to shapely geometry.
        zone_geoms: dict = {}
        for url, result in zip(unique_urls, zone_results):
            geom_data = result.get("geometry") if isinstance(result, dict) else None
            if geom_data:
                zone_geoms[url] = shape(geom_data)

        # Assign unioned geometry back to each null-geometry feature.
        for i in null_indices:
            urls = features[i].get("properties", {}).get("affectedZones", [])
            geoms = [zone_geoms[u] for u in urls if u in zone_geoms]
            if geoms:
                features[i]["geometry"] = unary_union(geoms).__geo_interface__

        return features

    def reap(self) -> gpd.GeoDataFrame:
        """Retrieve active flood alerts as a GeoDataFrame.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame with alert properties and polygon geometry.
        """
        url = self._build_url()
        data = self._fetch(url)

        features = data.get("features", [])
        if not features:
            return gpd.GeoDataFrame(columns=KEEP_COLUMNS + ["geometry"])

        # Resolve zone-based alerts that have null geometry.
        features = self._resolve_zone_geometries(features)

        gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

        # Rename camelCase property names to snake_case.
        renames = {
            "areaDesc": "area_desc",
            "senderName": "sender_name",
            "messageType": "message_type",
        }
        missing = [v for k, v in renames.items() if k not in gdf.columns and v not in gdf.columns]
        if missing:
            logger.warning(f"Expected columns not found in API response: {missing}")
        gdf = gdf.rename(columns=renames)

        # Keep only the specified columns.
        available = [c for c in KEEP_COLUMNS if c in gdf.columns]
        gdf = gdf[available + ["geometry"]]

        return gdf
