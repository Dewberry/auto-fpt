"""Convert Parquet files to HEC-DSS format."""

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from hecdss import HecDss, RegularTimeSeries
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def read_parquet_schema(parquet_path: str) -> dict:
    """
    Extract column names from parquet file.

    Expected columns:
    - datetime: timestamp with timezone
    - value: numeric values
    - A, B, C, D, E, F: DSS path parts
    - model_name: (optional) model identifier not used for DSS path

    Returns:
        Dictionary with column metadata
    """
    table = pq.read_table(parquet_path)
    columns = table.column_names

    schema = {
        "datetime_col": "datetime" if "datetime" in columns else None,
        "value_col": "value" if "value" in columns else None,
        "dss_parts": {part: part for part in ["A", "B", "C", "D", "E", "F"] if part in columns},
        "model_name_col": "model_name" if "model_name" in columns else None,
    }

    if not schema["datetime_col"] or not schema["value_col"]:
        raise ValueError(f"Parquet must have 'datetime' and 'value' columns. Found: {columns}")

    return schema


def parquet_to_dss(
    parquet_path: str,
    output_dss_path: str,
    path_f_part: str = None,
) -> dict:
    """
    Convert parquet file to HEC-DSS format.

    Args:
        parquet_path: Path to input parquet file
        output_dss_path: Path to output DSS file
        path_f_part: Optional F part for DSS path (overrides parquet F column)

    Returns:
        Manifest with conversion details
    """
    import numpy as np

    logger.info(f"Starting parquet to DSS conversion: {parquet_path} -> {output_dss_path}")

    # Read parquet schema
    schema = read_parquet_schema(parquet_path)
    df = pq.read_table(parquet_path).to_pandas()

    logger.info(f"Loaded parquet with {len(df)} records")

    # Group by DSS path parts
    for part in ["A", "B", "C", "D", "E", "F"]:
        if schema["dss_parts"].get(part):
            df[part] = df[part].fillna("")

    # Group by DSS path combination (A-E required, F optional)
    groupby_cols = [part for part in ["A", "B", "C", "D", "E", "F"] if schema["dss_parts"].get(part)]

    if not groupby_cols or "A" not in groupby_cols:
        logger.error("No DSS path parts (A-E) found in parquet schema")
        return {"error": "No DSS path parts found", "converted": 0}

    # Create output directory
    Path(output_dss_path).parent.mkdir(parents=True, exist_ok=True)

    # Initialize DSS file
    try:
        dss = HecDss(output_dss_path)
        converted_count = 0

        try:
            # Group data by unique path combinations
            for group_vals, group_data in df.groupby(groupby_cols, sort=False):
                if not isinstance(group_vals, tuple):
                    group_vals = (group_vals,)

                # Build DSS path parts mapping
                path_parts = {col: str(group_vals[i]) for i, col in enumerate(groupby_cols)}

                # Get F part - priority: path_f_part > parquet F column
                if path_f_part:
                    f_part = path_f_part
                else:
                    f_part = path_parts.get("F", "")

                # Build full DSS path: /A/B/C/D/E/F/
                dss_path = f"/{path_parts.get('A', '')}/{path_parts.get('B', '')}/{path_parts.get('C', '')}/{path_parts.get('D', '')}/{path_parts.get('E', '')}/{f_part}/"
                dss_path = dss_path.strip("/")
                dss_path = f"/{dss_path}/"

                logger.debug(f"Writing DSS path: {dss_path}")

                # Sort by datetime
                group_data = group_data.sort_values(schema["datetime_col"])

                # Convert to times and values
                times = group_data[schema["datetime_col"]].tolist()
                values = group_data[schema["value_col"]].tolist()

                # Write to DSS
                try:
                    # Create RegularTimeSeries object using attribute assignment
                    ts = RegularTimeSeries()
                    ts.id = dss_path
                    ts.times = times
                    ts.values = np.array(values, dtype=float)
                    ts.units = 'CFS'
                    ts.data_type = 'INST-VAL'
                    ts.interval = '15Min'
                    ts.start_date = times[0].strftime("%d%b%Y %H:%M")
                    ts.time_granularity_seconds = 900  # 15 minutes in seconds

                    dss.put(ts)
                    converted_count += 1
                    logger.info(f"Wrote {len(values)} values to {dss_path}")
                except Exception as e:
                    logger.error(f"Failed to write DSS path {dss_path}: {e}")

            logger.info(f"Parquet to DSS conversion complete: {converted_count} timeseries written")
            return {
                "input": parquet_path,
                "output": output_dss_path,
                "converted": converted_count,
                "total_records": len(df),
            }
        finally:
            dss.close()

    except Exception as e:
        logger.error(f"DSS file operation failed: {e}", exc_info=True)
        return {"error": str(e), "converted": 0}
