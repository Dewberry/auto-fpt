"""Fort Worth / NCTCOG OneRain DataAPI reaper.

This module provides an implementation for harvesting hydrological observations
from the OneRain DataAPI for the Fort Worth / NCTCOG gage network, including
precipitation, stage/water level, temperature, and other variables.
"""

from __future__ import annotations
from typing import Any

import pandas as pd
import tiny_retriever

__all__ = ["FtWorthReaper"]

BASE_URL = "http://cs-029-exchange.onerain.com:8080/OneRain/DataAPI"

# Mapping of sensor_class codes to human-readable variable names
SENSOR_CLASS_MAP = {
    10: "precip_incremental",
    11: "precip_cumulative",
    20: "stage",
    30: "temperature",
    40: "wind_speed",
    41: "wind_gust",
    44: "wind_direction",
    50: "humidity",
    53: "pressure",
    74: "lightning_count",
    75: "lightning",
    312: "gust_speed",
    440: "wind_chill",
}

VARIABLE_MAP = {v: k for k, v in SENSOR_CLASS_MAP.items()}


class FtWorthReaper:
    """Reaper for Fort Worth / NCTCOG OneRain DataAPI data.

    Parameters
    ----------
    system_key : str, optional
        API system key. Defaults to the NCTCOG public key.

    Examples
    --------
    >>> reaper = FtWorthReaper()
    >>> sites = reaper.get_sites()
    >>> df = reaper.reap(
    ...     site_ids=["43714"],
    ...     variables=["precip_cumulative", "stage"],
    ...     start_date="2026-06-07 00:00:00",
    ...     end_date="2026-06-08 00:00:00",
    ... )
    """

    def __init__(
        self,
        system_key: str | None = None,
    ) -> None:
        self.system_key = system_key

    def _build_url(self, params: dict[str, Any]) -> str:
        params = {**params, "system_key": self.system_key, "format": "json"}
        return f"{BASE_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

    def _fetch_json(self, params: dict[str, Any]) -> dict[str, Any]:
        """Fetch a single JSON response from the API."""
        url = self._build_url(params)
        result = tiny_retriever.fetch(url, "json")
        return result

    def get_sites(self) -> pd.DataFrame:
        """Fetch all site metadata.

        Returns
        -------
        pd.DataFrame
            DataFrame with site_id, name, latitude, longitude, elevation.
        """
        data = self._fetch_json({"method": "sites"})
        records = data["response"]
        df = pd.DataFrame(records)
        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)
        df["elevation"] = df["elevation"].astype(float)
        return df

    def get_sensors(self) -> pd.DataFrame:
        """Fetch the latest sensor data for all sites (metadata + last reading).

        Returns
        -------
        pd.DataFrame
            DataFrame with site_id, sensor_id, sensor_class, variable_name,
            data_time, data_value, units.
        """
        data = self._fetch_json({"method": "getSensorData"})
        records = data["response"]
        df = pd.DataFrame(records)
        df["variable"] = df["sensor_class"].map(SENSOR_CLASS_MAP).fillna("unknown")
        df["data_value"] = df["data_value"].astype(float)
        df["data_time"] = pd.to_datetime(df["data_time"])
        return df

    def _resolve_target_classes(
        self, variables: list[str] | None
    ) -> set[int] | None:
        """Map variable names to sensor_class codes."""
        if variables is None:
            return None
        target_classes = set()
        for var in variables:
            if var not in VARIABLE_MAP:
                raise ValueError(
                    f"Unknown variable '{var}'. Available: {list(VARIABLE_MAP.keys())}"
                )
            target_classes.add(VARIABLE_MAP[var])
        return target_classes

    def _resolve_site_ids(
        self, site_ids: list[str] | None, target_classes: set[int] | None
    ) -> list[str]:
        """If no site_ids given, discover all sites with the target variables."""
        if site_ids is not None:
            return site_ids
        sensors = self.get_sensors()
        if target_classes is not None:
            sensors = sensors[sensors["sensor_class"].isin(target_classes)]
        return sensors["site_id"].unique().tolist()

    def _build_reap_urls(
        self,
        site_ids: list[str],
        start_date: str,
        end_date: str,
        sensor_id: str | None,
    ) -> list[str]:
        """Build API URLs for each site."""
        urls = []
        for site_id in site_ids:
            params = {
                "method": "getSensorData",
                "site_id": site_id,
                "data_start": start_date,
                "data_end": end_date,
            }
            if sensor_id is not None:
                params["sensor_id"] = sensor_id
            urls.append(self._build_url(params))
        return urls

    def _parse_responses(
        self, results: list, target_classes: set[int] | None
    ) -> pd.DataFrame:
        """Parse raw API responses into a DataFrame."""
        all_records = []
        for resp in results:
            if resp is None:
                continue
            records = resp.get("response", [])
            if not isinstance(records, list):
                continue
            for rec in records:
                sc = rec["sensor_class"]
                if target_classes is not None and sc not in target_classes:
                    continue
                all_records.append({
                    "site_id": rec["site_id"],
                    "sensor_id": rec["sensor_id"],
                    "sensor_class": sc,
                    "variable": SENSOR_CLASS_MAP.get(sc, "unknown"),
                    "data_time": rec["data_time"],
                    "data_value": float(rec["data_value"]),
                    "raw_value": float(rec["raw_value"]),
                    "data_quality": rec.get("data_quality"),
                    "units": rec["units"],
                })

        if not all_records:
            return pd.DataFrame(
                columns=["site_id", "sensor_id", "sensor_class", "variable",
                         "data_time", "data_value", "raw_value", "data_quality", "units"]
            )

        df = pd.DataFrame(all_records)
        df["data_time"] = pd.to_datetime(df["data_time"])
        df = df.sort_values(["site_id", "variable", "data_time"]).reset_index(drop=True)
        return df

    def reap(
        self,
        start_date: str,
        end_date: str,
        site_ids: str | list[str] | None = None,
        variables: str | list[str] | None = None,
        sensor_id: str | None = None,
    ) -> pd.DataFrame:
        """Retrieve time-series data for given site(s) and variable(s).

        Parameters
        ----------
        start_date : str
            Start datetime (e.g., "2026-06-07 00:00:00").
        end_date : str
            End datetime (e.g., "2026-06-08 00:00:00").
        site_ids : str, list[str], or None, optional
            Site ID(s) to query. If None, fetches all sites that have
            the requested variables.
        variables : str or list[str], optional
            Variable name(s) to filter (e.g., "precip_incremental", "stage").
            See VARIABLE_MAP for available names. If None, returns all data.
        sensor_id : str, optional
            Specific sensor_id to query. If provided, fetches that sensor directly.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: site_id, sensor_id, sensor_class, variable,
            data_time, data_value, raw_value, units.
        """
        if isinstance(site_ids, str):
            site_ids = [site_ids]
        if isinstance(variables, str):
            variables = [variables]

        target_classes = self._resolve_target_classes(variables)
        site_ids = self._resolve_site_ids(site_ids, target_classes)
        urls = self._build_reap_urls(site_ids, start_date, end_date, sensor_id)
        results = tiny_retriever.fetch(urls, "json") if len(urls) > 1 else [tiny_retriever.fetch(urls[0], "json")]
        return self._parse_responses(results, target_classes)