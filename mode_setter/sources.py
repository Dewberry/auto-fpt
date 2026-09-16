"""Source checker classes for the flood system status checker."""

import logging
from datetime import datetime, timedelta, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
import shapely

from utils.icechunk_utils import IcechunkManager
from utils.iceberg_utils import IcebergManager

logging.basicConfig(level=logging.INFO, force=True)
# Suppress verbose logging from underlying libraries
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.WARNING)
logging.getLogger("pyiceberg").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DEFAULT_BOUNDARY = "s3://flood-warning/staging/payloads/metropolitan_planning_area.pq"


def _is_valid(latest_value, staleness_hours: int, now: datetime) -> tuple[datetime, bool]:
    """Parse a numpy datetime to tz-aware UTC and check it is recent enough.

    Returns a tuple of (parsed_datetime, is_valid) where is_valid is True when
    the value is within `staleness_hours` of `now`.
    """
    latest = pd.Timestamp(latest_value).to_pydatetime().replace(tzinfo=timezone.utc)
    return latest, (now - latest) <= timedelta(hours=staleness_hours)


def _clip_region(boundary_path: str, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Build a boolean mask of grid cells within the boundary polygon.

    Accepts either 1D coordinate arrays (a meshgrid is built) or 2D coordinate
    arrays (used directly). The returned mask matches the 2D grid shape.
    """
    boundary = gpd.read_parquet(boundary_path)
    region_geom = boundary.union_all()

    if lons.ndim == 1 and lats.ndim == 1:
        lons, lats = np.meshgrid(lons, lats)

    points = shapely.points(lons.ravel(), lats.ravel())
    return shapely.contains(region_geom, points).reshape(lats.shape)


class NBMChecker:
    """Check NBM 6-hour POP grids against a threshold within the MPA boundary."""

    def __init__(
        self,
        bucket="flood-warning",
        prefix="dev/icechunk/nbm",
        region="us-east-1",
        boundary_path=DEFAULT_BOUNDARY,
        pop_threshold=30.0,
        staleness_hours=8,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.boundary_path = boundary_path
        self.pop_threshold = pop_threshold
        self.staleness_hours = staleness_hours

    def check(self) -> dict:
        now = datetime.now(tz=timezone.utc)

        manager = IcechunkManager(
            bucket=self.bucket, prefix=self.prefix, region=self.region
        )
        ds = manager.read_icechunk()

        # Get the latest init time and check staleness
        latest_init, valid = _is_valid(
            ds.init_time.values[-1], self.staleness_hours, now
        )
        logger.info(f"NBM latest init_time: {latest_init.isoformat()}, valid: {valid}")

        # Select the latest initialization
        latest_ds = ds.sel(init_time=ds.init_time.values[-1])

        # Build a spatial mask for grid cells within the MPA boundary
        mask = _clip_region(
            self.boundary_path,
            latest_ds.longitude.values,
            latest_ds.latitude.values,
        )

        if not mask.any():
            logger.warning("No NBM grid cells fall within the MPA boundary")
            return {
                "source": "nbm",
                "threshold_met": False,
                "details": {
                    "latest_init_time": latest_init.isoformat(),
                    "max_pop_6hr": 0.0,
                    "pop_threshold": self.pop_threshold,
                    "valid": valid,
                    "message": "No grid cells within MPA boundary",
                },
            }

        # Check max POP across all forecast steps within MPA
        pop_values = latest_ds["pop_6hr"].values  # shape: (step, y, x)
        pop_in_mpa = pop_values[:, mask]

        # All values missing within the MPA -> cannot determine status
        if np.all(np.isnan(pop_in_mpa)):
            logger.warning("All NBM grid values within the MPA are missing")
            return {
                "source": "nbm",
                "threshold_met": False,
                "details": {
                    "latest_init_time": latest_init.isoformat(),
                    "max_pop_6hr": None,
                    "pop_threshold": self.pop_threshold,
                    "valid": False,
                    "message": "All grid values missing within MPA boundary",
                },
            }

        max_pop = float(np.nanmax(pop_in_mpa))
        threshold_met = max_pop > self.pop_threshold

        logger.info(
            f"NBM max POP in MPA: {max_pop:.1f}%, "
            f"threshold ({self.pop_threshold}%): {'MET' if threshold_met else 'not met'}"
        )

        return {
            "source": "nbm",
            "threshold_met": threshold_met,
            "details": {
                "latest_init_time": latest_init.isoformat(),
                "max_pop_6hr": round(max_pop, 1),
                "pop_threshold": self.pop_threshold,
                "valid": valid,
            },
        }


class WPCEROChecker:
    """Check WPC ERO outlooks against risk thresholds within the MPA boundary."""

    GRAY_OUTLOOKS = ["Marginal", "Slight", "Moderate", "High"]

    def __init__(
        self,
        warehouse_path="s3://flood-warning/dev/warehouse",
        region="us-east-1",
        namespace="public_products",
        table_name="wpc_ero",
        boundary_path=DEFAULT_BOUNDARY,
        lookback_hours=36,
        lookahead_hours=18,
    ):
        self.warehouse_path = warehouse_path
        self.region = region
        self.namespace = namespace
        self.table_name = table_name
        self.boundary_path = boundary_path
        self.lookback_hours = lookback_hours
        self.lookahead_hours = lookahead_hours

    def check(self) -> dict:
        now = datetime.now(tz=timezone.utc)

        latest_issue, active = self._fetch_active_outlooks(now)

        if latest_issue is None:
            logger.warning("No WPC ERO records found in lookback window")
            return {
                "source": "wpc_ero",
                "threshold_met": False,
                "details": {
                    "latest_issue_time": None,
                    "max_outlook": None,
                    "valid": False,
                    "message": "No data found in lookback window",
                },
            }

        if active.empty:
            logger.info("No WPC ERO records active within lookahead window")
            return {
                "source": "wpc_ero",
                "threshold_met": False,
                "details": {
                    "latest_issue_time": latest_issue.isoformat(),
                    "max_outlook": None,
                    "valid": False,
                    "window_end": (now + timedelta(hours=self.lookahead_hours)).isoformat(),
                },
            }

        threshold_met, max_outlook = self._evaluate_outlooks(active)

        logger.info(
            f"WPC ERO max outlook in MPA: {max_outlook}, "
            f"threshold: {'MET' if threshold_met else 'not met'}"
        )

        return {
            "source": "wpc_ero",
            "threshold_met": threshold_met,
            "details": {
                "latest_issue_time": latest_issue.isoformat(),
                "max_outlook": max_outlook,
                "valid": True,
                "window_end": (now + timedelta(hours=self.lookahead_hours)).isoformat(),
            },
        }

    def _fetch_active_outlooks(self, now: datetime):
        """Read recent issuances and return (latest_issue_time, active_records).

        Returns (None, empty df) when no records exist in the lookback window.
        `active_records` are the latest issuance's polygons whose valid period
        overlaps [now, now + lookahead_hours].
        """
        manager = IcebergManager(
            warehouse_path=self.warehouse_path,
            region=self.region,
            namespace=self.namespace,
            table_name=self.table_name,
        )

        start_time = (now - timedelta(hours=self.lookback_hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        table = manager.read_table(
            time_col="issue_time",
            start_time=start_time,
            end_time=end_time,
        )

        df = table.to_pandas()
        if df.empty:
            return None, df

        df["issue_time"] = pd.to_datetime(df["issue_time"], utc=True)
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True)

        latest_issue = df["issue_time"].max()
        latest_records = df[df["issue_time"] == latest_issue]

        # Keep only records whose valid period overlaps [now, now + lookahead]
        window_end = now + timedelta(hours=self.lookahead_hours)
        active = latest_records[
            (latest_records["end_time"] >= now)
            & (latest_records["start_time"] <= window_end)
        ].copy()

        return latest_issue, active

    def _evaluate_outlooks(self, active: pd.DataFrame) -> tuple[bool, str | None]:
        """Clip active outlooks to the MPA and return (threshold_met, max_outlook)."""
        gdf = gpd.GeoDataFrame(
            active,
            geometry=gpd.GeoSeries.from_wkb(active["geometry"]),
            crs="EPSG:4326",
        )

        boundary = gpd.read_parquet(self.boundary_path)
        clipped = gpd.clip(gdf, boundary)

        if clipped.empty:
            return False, None

        outlooks = clipped["outlook"].dropna().unique().tolist()
        threshold_met = any(level in self.GRAY_OUTLOOKS for level in outlooks)
        max_outlook = None
        for level in ("High", "Moderate", "Slight", "Marginal"):
            if level in outlooks:
                max_outlook = level
                break

        return threshold_met, max_outlook


class MRMSChecker:
    """Check MRMS QPE accumulation within the MPA boundary."""

    def __init__(
        self,
        bucket="flood-warning",
        prefix="dev/icechunk/mrms",
        region="us-east-1",
        group="15min",
        variable="qpe_15min",
        boundary_path=DEFAULT_BOUNDARY,
        accumulation_threshold_mm=12.7,
        window_hours=1,
        interval_minutes=15,
        staleness_hours=2,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.group = group
        self.variable = variable
        self.boundary_path = boundary_path
        self.accumulation_threshold_mm = accumulation_threshold_mm
        self.window_hours = window_hours
        self.interval_minutes = interval_minutes
        self.expected_steps = round(window_hours * 60 / interval_minutes)
        self.staleness_hours = staleness_hours

    def check(self) -> dict:
        now = datetime.now(tz=timezone.utc)

        manager = IcechunkManager(
            bucket=self.bucket, prefix=self.prefix, region=self.region
        )
        ds = manager.read_icechunk(group=self.group)

        # Check the latest observation is recent enough to trust
        latest_valid, fresh = _is_valid(
            ds.valid_time.values[-1], self.staleness_hours, now
        )

        # Select the trailing window by actual time range and verify coverage,
        # so an ingestion gap cant silently accumulate observations spanning
        # more than the intended window.
        valid_times = pd.DatetimeIndex(ds.valid_time.values)
        window_end = pd.Timestamp(latest_valid).tz_localize(None)
        window_start = window_end - pd.Timedelta(hours=self.window_hours)
        in_window = (valid_times > window_start) & (valid_times <= window_end)
        steps_found = int(in_window.sum())
        complete = steps_found == self.expected_steps

        logger.info(
            f"MRMS latest valid_time: {latest_valid.isoformat()}, fresh: {fresh}, "
            f"steps: {steps_found}/{self.expected_steps}"
        )

        # Stale or incomplete coverage means we cannot trust the accumulation
        if not fresh or not complete:
            reason = "stale data" if not fresh else "incomplete window coverage"
            logger.warning(f"MRMS data invalid: {reason}")
            return {
                "source": "mrms",
                "threshold_met": False,
                "details": {
                    "latest_valid_time": latest_valid.isoformat(),
                    "max_accumulation_mm": None,
                    "accumulation_threshold_mm": self.accumulation_threshold_mm,
                    "valid": False,
                    "steps_found": steps_found,
                    "expected_steps": self.expected_steps,
                    "message": reason,
                },
            }

        # Accumulate precipitation over the verified window. min_count=1 keeps
        # all-missing cells as NaN (rather than summing to 0) so we can detect
        # a fully missing grid below.
        window = ds[self.variable].isel(valid_time=in_window)
        accumulation = window.sum(dim="valid_time", min_count=1)

        # Build a spatial mask for grid cells within the MPA boundary
        mask = _clip_region(
            self.boundary_path, ds.longitude.values, ds.latitude.values
        )

        if not mask.any():
            logger.warning("No MRMS grid cells fall within the MPA boundary")
            return {
                "source": "mrms",
                "threshold_met": False,
                "details": {
                    "latest_valid_time": latest_valid.isoformat(),
                    "max_accumulation_mm": None,
                    "accumulation_threshold_mm": self.accumulation_threshold_mm,
                    "valid": False,
                    "message": "No grid cells within MPA boundary",
                },
            }

        acc_in_mpa = accumulation.values[mask]

        # All values missing within the MPA -> cannot determine status
        if np.all(np.isnan(acc_in_mpa)):
            logger.warning("All MRMS grid values within the MPA are missing")
            return {
                "source": "mrms",
                "threshold_met": False,
                "details": {
                    "latest_valid_time": latest_valid.isoformat(),
                    "max_accumulation_mm": None,
                    "accumulation_threshold_mm": self.accumulation_threshold_mm,
                    "valid": False,
                    "message": "All grid values missing within MPA boundary",
                },
            }

        max_accumulation = float(np.nanmax(acc_in_mpa))
        threshold_met = max_accumulation > self.accumulation_threshold_mm

        logger.info(
            f"MRMS max {self.window_hours}-hr QPE in MPA: {max_accumulation:.1f}mm, "
            f"threshold ({self.accumulation_threshold_mm}mm): "
            f"{'MET' if threshold_met else 'not met'}"
        )

        return {
            "source": "mrms",
            "threshold_met": threshold_met,
            "details": {
                "latest_valid_time": latest_valid.isoformat(),
                "max_accumulation_mm": round(max_accumulation, 1),
                "accumulation_threshold_mm": self.accumulation_threshold_mm,
                "valid": True,
            },
        }


class USGSChecker:
    """Check USGS gage discharge values against a historical percentile from flow stats."""

    FLOW_STATS_PATH = "s3://flood-warning/stac/gages/{}/{}-flow-stats.pq"

    def __init__(
        self,
        warehouse_path="s3://flood-warning/dev/warehouse",
        region="us-east-1",
        namespace="observations",
        table_name="usgs",
        lookback_hours=2,
        percentile=90,
        min_gages=20,
    ):
        self.warehouse_path = warehouse_path
        self.region = region
        self.namespace = namespace
        self.table_name = table_name
        self.lookback_hours = lookback_hours
        self.percentile = percentile
        self.percentile_col = f"p{percentile:02d}_va"
        self.min_gages = min_gages

    def check(self) -> dict:
        now = datetime.now(tz=timezone.utc)
        fs = s3fs.S3FileSystem()

        latest = self._fetch_latest_readings(now)
        if latest.empty:
            logger.warning("No USGS discharge records found in lookback window")
            return {
                "source": "usgs",
                "threshold_met": False,
                "details": {
                    "valid": False,
                    "message": "No discharge data found in lookback window",
                    "gages_exceeded": [],
                },
            }

        gages_exceeded = []
        gages_checked = 0

        for _, row in latest.iterrows():
            result = self._evaluate_gage(row, fs)
            if result is None:
                continue
            gages_checked += 1
            if result["exceeded"]:
                gages_exceeded.append(result["details"])

        # Too few gages with usable stats means we cannot trust a clear result
        if gages_checked < self.min_gages:
            logger.warning(
                f"USGS: only {gages_checked} gages checked "
                f"(min {self.min_gages}); marking invalid"
            )
            return {
                "source": "usgs",
                "threshold_met": False,
                "details": {
                    "valid": False,
                    "percentile": self.percentile,
                    "gages_checked": gages_checked,
                    "min_gages": self.min_gages,
                    "gages_exceeded": [],
                    "message": "Insufficient gage coverage",
                },
            }

        threshold_met = len(gages_exceeded) > 0

        logger.info(
            f"USGS: {gages_checked} gages checked, "
            f"{len(gages_exceeded)} exceeded avg p{self.percentile:02d}, "
            f"threshold: {'MET' if threshold_met else 'not met'}"
        )

        return {
            "source": "usgs",
            "threshold_met": threshold_met,
            "details": {
                "valid": True,
                "percentile": self.percentile,
                "gages_checked": gages_checked,
                "gages_exceeded_count": len(gages_exceeded),
                "gages_exceeded": gages_exceeded,
            },
        }

    def _fetch_latest_readings(self, now: datetime) -> pd.DataFrame:
        """Read recent discharge records and return the latest reading per gage."""
        manager = IcebergManager(
            warehouse_path=self.warehouse_path,
            region=self.region,
            namespace=self.namespace,
            table_name=self.table_name,
        )

        start_time = (now - timedelta(hours=self.lookback_hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        table = manager.read_table(
            time_col="timestamp",
            start_time=start_time,
            end_time=end_time,
            variable_col="variable",
            variables=["discharge"],
        )

        df = table.to_pandas()
        if df.empty:
            return df

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").groupby("site_id").last().reset_index()

    def _evaluate_gage(self, row: pd.Series, fs: s3fs.S3FileSystem) -> dict | None:
        """Compare a gage's latest value against its average percentile threshold.

        Returns None when the gage has no flow stats (or an error occurs),
        otherwise a dict with an `exceeded` flag and comparison `details`.
        """
        site_id = row["site_id"]
        stats_path = self.FLOW_STATS_PATH.format(site_id, site_id)

        try:
            if not fs.exists(stats_path):
                logger.debug(f"No flow stats for gage {site_id}, skipping")
                return None
        except Exception:
            return None

        try:
            stats = pd.read_parquet(stats_path, filesystem=fs)
            avg_percentile = stats[self.percentile_col].mean()
        except Exception as e:
            logger.debug(f"Error reading flow stats for {site_id}: {e}")
            return None

        exceeded = pd.notna(row["value"]) and row["value"] > avg_percentile
        return {
            "exceeded": bool(exceeded),
            "details": {
                "site_id": site_id,
                "value": round(float(row["value"]), 2),
                "avg_threshold": round(float(avg_percentile), 2),
            },
        }

