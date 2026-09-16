"""WPC Excessive Rainfall Outlook (ERO) reaper.

Harvests ERO shapefiles (Day 1-3) from the WPC FTP server.
Files are timestamped by issuance date and hour, e.g. `94e_2026082409.zip`.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date, datetime, timezone

import geopandas as gpd
import tiny_retriever

__all__ = ["WPCReaper"]

logger = logging.getLogger(__name__)

BASE_URL = "https://ftp-wpc.ncep.noaa.gov/shapefiles/qpf/excessive/"

# prefix → (morning hour, afternoon hour)
PRODUCTS: dict[str, dict] = {
    "day1": {"prefix": "94e", "hours": {"morning": 9, "afternoon": 16}, "latest": "EXCESSIVERAIN_Day1_latest.zip"},
    "day2": {"prefix": "98e", "hours": {"morning": 9, "afternoon": 21}, "latest": "EXCESSIVERAIN_Day2_latest.zip"},
    "day3": {"prefix": "99e", "hours": {"morning": 9, "afternoon": 21}, "latest": "EXCESSIVERAIN_Day3_latest.zip"},
}

_VALID_ISSUANCES = ("morning", "afternoon", "latest")
_ISSUANCE_PRIORITY = ["afternoon", "morning"]


class WPCReaper:
    """Reaper for WPC Excessive Rainfall Outlook shapefiles.

    Parameters
    ----------
    days : str, list[str], or None, optional
        Day product(s) to fetch ("day1", "day2", "day3").
        If None, fetches all three.
    date : datetime.date, str (YYYY-MM-DD), or None, optional
        Reference date for the outlook. Defaults to today (UTC).
    issuance : "morning", "afternoon", "latest", or None, optional
        Which issuance to retrieve. ``"latest"`` fetches the
        ``EXCESSIVERAIN_DayN_latest.zip`` files (ignores ``date``).
        If None, tries afternoon first and falls back to morning.

    Examples
    --------
    >>> reaper = WPCReaper(days="day1", date="2026-08-23", issuance="morning")
    >>> gdf = reaper.reap()
    >>> reaper = WPCReaper(days=["day1", "day2"])
    >>> gdf = reaper.reap()
    """

    def __init__(
        self,
        days: str | list[str] | None = None,
        date: date | str | None = None,
        issuance: str | None = None,
    ) -> None:
        if days is None:
            self.days = list(PRODUCTS.keys())
        elif isinstance(days, str):
            self.days = [days]
        else:
            self.days = days

        for day in self.days:
            if day not in PRODUCTS:
                raise ValueError(
                    f"Unknown day '{day}'. Available: {list(PRODUCTS.keys())}"
                )

        if date is None:
            self.ref_date = datetime.now(tz=timezone.utc).date()
        elif isinstance(date, str):
            self.ref_date = datetime.strptime(date, "%Y-%m-%d").date()
        else:
            self.ref_date = date

        if issuance is not None:
            if issuance not in _VALID_ISSUANCES:
                raise ValueError(
                    f"Unknown issuance '{issuance}'. Available: {list(_VALID_ISSUANCES)}"
                )
            self.issuance_order = [issuance]
        else:
            self.issuance_order = list(_ISSUANCE_PRIORITY)

        logger.debug(
            "Initialized %s: days=%s, date=%s, issuance=%s",
            self.__class__.__name__, self.days, self.ref_date, self.issuance_order,
        )

    @staticmethod
    def _build_url(prefix: str, ref_date: date, hour: int) -> str:
        """Construct the FTP URL for a given product, date, and issuance hour."""
        stamp = f"{ref_date:%Y%m%d}{hour:02d}"
        return f"{BASE_URL}{prefix}_{stamp}.zip"

    @staticmethod
    def _read_shapefile_bytes(data: bytes) -> gpd.GeoDataFrame:
        """Read a zipped shapefile from raw bytes into a GeoDataFrame."""
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            tmp.write(data)
            tmp.flush()
            return gpd.read_file(tmp.name)

    def _fetch(self, url: str) -> gpd.GeoDataFrame:
        """Download a zipped shapefile and return it as a GeoDataFrame."""
        logger.info("Fetching %s", url)
        data = tiny_retriever.fetch(url, "binary")
        return self._read_shapefile_bytes(data)

    def reap(self) -> gpd.GeoDataFrame:
        """Retrieve ERO shapefiles as a single GeoDataFrame.

        Returns
        -------
        gpd.GeoDataFrame
            Combined GeoDataFrame with `day` and `issuance` columns.
        """
        frames: list[gpd.GeoDataFrame] = []

        for day in self.days:
            product = PRODUCTS[day]
            prefix = product["prefix"]
            hours = product["hours"]

            gdf = None
            used_issuance = None

            for iss in self.issuance_order:
                if iss == "latest":
                    url = f"{BASE_URL}{product['latest']}"
                else:
                    hour = hours[iss]
                    url = self._build_url(prefix, self.ref_date, hour)
                try:
                    gdf = self._fetch(url)
                    used_issuance = iss
                    break
                except Exception:
                    logger.warning("Could not fetch %s (%s issuance), trying next", day, iss)
                    continue

            if gdf is None or gdf.empty:
                logger.warning("No data available for %s on %s", day, self.ref_date)
                continue

            gdf["day"] = day
            gdf["issuance"] = used_issuance
            frames.append(gdf)

        if not frames:
            return gpd.GeoDataFrame()

        result = gpd.pd.concat(frames, ignore_index=True)
        cols = [c for c in result.columns if c != "geometry"] + ["geometry"]
        return result[cols]
