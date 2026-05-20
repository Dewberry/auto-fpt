#!/usr/bin/env python3
"""Convert ETL config YAML to parquet with one row per source."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

INPUT_PATH = Path("/home/ubuntu/repos/auto-fpt/etl/configs/hms_forcing_config.yaml")
OUTPUT_PATH = Path("/home/ubuntu/repos/auto-fpt/etl/configs/hms-forcing-config.parquet")


def main() -> None:
    with INPUT_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    sources = cfg.get("sources", {})
    raw_rows = [{"source": name, **values} for name, values in sources.items()]

    # Preserve columns that only appear in some sources by normalizing every row
    # to the union of all observed keys.
    all_cols = sorted({k for row in raw_rows for k in row})

    # Push sparse columns (those with missing values) to the end.
    cols_with_nans = [
        col for col in all_cols if any(row.get(col) is None for row in raw_rows)
    ]
    cols_without_nans = [col for col in all_cols if col not in cols_with_nans]
    ordered_cols = [
        "source",
        *[col for col in cols_without_nans if col != "source"],
        *[col for col in cols_with_nans if col != "source"],
    ]
    rows = [{col: row.get(col) for col in ordered_cols} for row in raw_rows]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), OUTPUT_PATH)
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
