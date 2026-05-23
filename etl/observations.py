from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from etl.shared._logging import configure_logger, logger
from etl.shared.utils import parse_tz_aware_time, upload_table_to_s3

configure_logger(level="INFO")
import pyarrow as pa
import pyarrow.compute as pc

# Import libraries from etl/requirements.txt
from etl.point.iceberg_utils import IcebergManager

DEFAULT_CONFIG_PATH = Path(__file__).parent / "configs" / "observations_config.yaml"


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


CONFIG = load_config()
pq_to_hms: dict[str, str] = CONFIG["variable_mapping"]


def apply_hms_mapping(table, variable_col: str = "variable") -> pa.Table:
    """Filter rows to only those whose variable is in pq_to_hms, then rename
    the variable values to their HMS equivalents. Encodes repetitive string
    columns as dictionaries to reduce memory and file size."""

    known_vars = list(pq_to_hms.keys())
    mask = pc.is_in(table.column(variable_col), value_set=pa.array(known_vars))
    filtered = table.filter(mask)

    original = filtered.column(variable_col).to_pylist()
    renamed = pa.array([pq_to_hms[v] for v in original], type=pa.string())
    col_idx = filtered.schema.get_field_index(variable_col)
    filtered = filtered.set_column(col_idx, variable_col, renamed)

    # Dictionary-encode low-cardinality string columns to reduce memory and parquet size
    dict_cols = [
        c
        for c in ("variable", "qualifier", "provider", "site_id")
        if c in filtered.schema.names
    ]
    for col in dict_cols:
        idx = filtered.schema.get_field_index(col)
        filtered = filtered.set_column(
            idx, col, filtered.column(col).dictionary_encode()
        )

    return filtered


def process_observations(
    config: dict = CONFIG,
    hms_mapping: bool = True,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    output_prefix: str | None = None,
) -> dict:
    dt_now = end_date or datetime.now(UTC)
    dt_start = start_date or (dt_now - timedelta(hours=config["lookback_hours"]))

    start_str = dt_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = dt_now.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"Processing observations from {start_str} to {end_str}")

    iceberg = IcebergManager(
        warehouse_path=config["warehouse_path"],
        region=config["region"],
        namespace=config["namespace"],
        table_name=config["table_name"],
    )

    variable_filter = list(pq_to_hms.keys()) if hms_mapping else None
    table = iceberg.read_table(
        time_col=config["time_col"],
        start_time=start_str,
        end_time=end_str,
        variable_col="variable" if variable_filter else None,
        variables=variable_filter,
        selected_fields=(
            tuple(config["selected_fields"]) if config.get("selected_fields") else None
        ),
    )

    if hms_mapping:
        table = apply_hms_mapping(table)

    prefix = output_prefix or config["output_prefix"]
    output_path = f"{prefix}/{dt_now.year}/{dt_now.month:02d}/{dt_now.day:02d}/{dt_now.hour:02d}/{config['output_filename']}"
    upload_table_to_s3(table, output_path)
    logger.info(f"Successfully processed observations. Output path: {output_path}")

    return {
        "output_path": output_path,
        "row_count": len(table),
    }


def handler(event=None, context=None):
    if event is None:
        event = {}

    source = event.get("source", "all")
    hms_mapping = event.get("hms_mapping", True)
    output_prefix = event.get("output_prefix")
    end_date_str = event.get("end_date")
    start_date_str = event.get("start_date")

    end_date = parse_tz_aware_time(end_date_str) if end_date_str else None
    start_date = parse_tz_aware_time(start_date_str) if start_date_str else None

    result = process_observations(
        config=CONFIG,
        hms_mapping=hms_mapping,
        start_date=start_date,
        end_date=end_date,
        output_prefix=output_prefix,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": "Task completed successfully",
            "source": source,
            "result": result,
        },
    }
