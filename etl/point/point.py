
import jsonschema
import s3fs
import pandas as pd
import pyarrow as pa
from pyarrow import ArrowInvalid
from typing import Union

from etl.point.iceberg_utils import IcebergManager
from etl.shared._logging import logger
from etl.shared.exceptions import EmptyDataError
from etl.shared.utils import delete_s3_files, extract_run_time, find_files, load_schema


def validate_df_schema(df: pd.DataFrame, schema_path: str, sample_size: int = 100):
    """Validate a sample of DataFrame rows against a JSON schema."""
    schema = load_schema(schema_path)

    # Convert datetime columns to ISO strings for JSON schema validation
    df_for_validation = df.copy()
    for col in df_for_validation.select_dtypes(include=["datetimetz", "datetime64"]).columns:
        df_for_validation[col] = df_for_validation[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Convert NaN values to None so JSON schema recognizes them as null
    records = [
        {k: (None if pd.isna(v) else v) for k, v in record.items()}
        for record in df_for_validation.to_dict('records')
    ]
    indices = range(len(records)) if len(records) <= sample_size else pd.RangeIndex(len(records)).to_series().sample(sample_size).index
    for idx in indices:
        jsonschema.validate(records[idx], schema)

        
def load_data(
    fs,
    s3_paths: list[str],
    schema_path: str,
    run_time_col: str = None,
) -> pd.DataFrame:
    """
    Loads data from S3. Default implementation assumes Parquet files.
    Iterates through all files found at the given S3 path, reads them into DataFrames, validates them, 
    and concatenates them into a single DataFrame.

    Args:
        fs: S3 filesystem instance.
        s3_paths: List of S3 URIs to read.
        schema_path: Path to JSON schema for validation, or None to skip.
        run_time_col: Optional column name for a timestamp derived from the file path.
            When provided, extracts the run time from the date-partitioned path structure.
    """

    dfs = []
    files_to_delete = []
    for file_path in s3_paths:
        logger.info(f"Reading file: {file_path}")
        try:
            df = pd.read_parquet(file_path, engine='pyarrow', filesystem=fs)
            if df.empty:
                raise EmptyDataError(f"DataFrame is empty for file: {file_path}")
            if schema_path:
                validate_df_schema(df, schema_path)
            if run_time_col:
                df[run_time_col] = extract_run_time(file_path)
            dfs.append(df)
            files_to_delete.append(file_path)
        except EmptyDataError as e:
            logger.warning(f"Warning: {e}")
            files_to_delete.append(file_path)
        except ArrowInvalid as e:
            logger.error(f"Error reading {file_path}: {e}")
            files_to_delete.append(file_path)
        except jsonschema.ValidationError as e:
            logger.error(f"Schema validation error in {file_path}: {e.message}")
            # If schema validation fails, we want to save the file for further investigation
        except Exception as e:
            logger.error(f"Unexpected error reading {file_path}: {e}")
            # If error is unexpected, we want to save the file for further investigation
    if not dfs:
        raise RuntimeError(f"Could not read any valid data from {s3_paths}")
    
    df_sorted = pd.concat(dfs, ignore_index=True)
    df_unique = df_sorted.drop_duplicates()

    return df_unique, files_to_delete

def process_point_data_source(
    fs: s3fs.S3FileSystem,
    input_prefix: str,
    schema_path: Union[str, None], 
    warehouse_path: str,
    namespace: str,
    table_name: str,
    time_col: str,
    region: str = "us-east-1",
    file_pattern: str = None,
    run_time_col: str = None,
    delete_files: bool = True,
    find_latest: bool = False
):
    """Process a point data source by reading files from S3, validating and concatenating them, and writing to an Iceberg table.
    
    Args:
        fs (s3fs.S3FileSystem): An instance of the S3 filesystem to read from and write to S3.
        input_prefix (str): The S3 prefix where input files are located.
        schema_path (Union[str, None]): The local path to the JSON schema for validating input data. If None, schema validation is skipped.
        warehouse_path (str): The S3 path to the Iceberg warehouse where the table is located.
        namespace (str): The Iceberg namespace for the table.
        table_name (str): The name of the Iceberg table to write to.
        time_col (str): The name of the time column in the data, used for partitioning and de-duplication.
        region (str): The AWS region for the S3 bucket and Iceberg catalog. Defaults to "us-east-1".
        file_pattern (str): Optional filename to match recursively (e.g. "forecast.parquet").
            When provided, uses glob to find matching files in all subdirectories.
        run_time_col (str): Optional column name for a timestamp derived from the file path.
        delete_files (bool): Whether to delete processed files from S3 after successful processing. Defaults to True.
        find_latest (bool): If True, only the latest file matching the file_pattern will be processed. Defaults to False.
    """

    file_paths = find_files(fs, input_prefix, file_pattern=file_pattern, find_latest=find_latest)
    if not file_paths:
        raise FileNotFoundError(f"No files found at {input_prefix}")
    logger.info(f"Found {len(file_paths)} files to process at prefix: {input_prefix}")

    iceberg = IcebergManager(warehouse_path=warehouse_path, region=region, namespace=namespace, table_name=table_name)

    data, files_to_delete = load_data(
        fs, file_paths, schema_path=schema_path,
        run_time_col=run_time_col
    )
    table = pa.Table.from_pandas(data, preserve_index=False)
    logger.info(f"Writing data to Iceberg table: {namespace}.{table_name}")
    iceberg.write_to_table(
        table=table, 
        time_col=time_col,
    )
    if delete_files and len(files_to_delete) > 0:
        logger.info("Cleaning up processed files from S3...")
        delete_s3_files(fs, files_to_delete)