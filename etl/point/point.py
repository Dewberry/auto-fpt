
import jsonschema
import s3fs
import pandas as pd
import pyarrow as pa
from pyarrow import ArrowInvalid

from etl.point.iceberg_utils import IcebergManager
from etl.shared._logging import logger
from etl.shared.exceptions import EmptyDataError
from etl.shared.utils import delete_s3_files, read_s3_files, load_schema


def validate_df_schema(df: pd.DataFrame, schema_path: str):
    """Validate a DataFrame against a JSON schema."""
    schema = load_schema(schema_path)
    
    # Validate each record in the df
    for idx, record in enumerate(df.to_dict('records')):
        jsonschema.validate(record, schema)

        
def load_data(fs, s3_paths: list[str], schema_path: str) -> pd.DataFrame:
    """
    Loads data from S3. Default implementation assumes Parquet files.
    Iterates through all files found at the given S3 path, reads them into DataFrames, validates them, 
    and concatenates them into a single DataFrame.
    """

    dfs = []
    files_to_delete = []
    for file_path in s3_paths:
        try:
            df = pd.read_parquet(file_path, engine='pyarrow', filesystem=fs)
            if df.empty:
                raise EmptyDataError(f"DataFrame is empty for file: {file_path}")
            validate_df_schema(df, schema_path)
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
    schema_path: str,
    warehouse_path: str,
    namespace: str,
    table_name: str,
    time_col: str,
    region: str = "us-east-1",
):
    """Process a point data source by reading files from S3, validating and concatenating them, and writing to an Iceberg table.
    
    Args:
        fs (s3fs.S3FileSystem): An instance of the S3 filesystem to read from and write to S3.
        source_name (str): The name of the data source being processed, used for logging.
        input_prefix (str): The S3 prefix where input files are located.
        schema_path (str): The local path to the JSON schema for validating input data.
        warehouse_path (str): The S3 path to the Iceberg warehouse where the table is located.
        namespace (str): The Iceberg namespace for the table.
        table_name (str): The name of the Iceberg table to write to.
        time_col (str): The name of the time column in the data, used for partitioning and de-duplication.
        region (str): The AWS region for the S3 bucket and Iceberg catalog. Defaults to "us-east-1".    
        """

    file_paths = read_s3_files(fs, input_prefix)
    iceberg = IcebergManager(warehouse_path=warehouse_path, region=region, namespace=namespace, table_name=table_name)
    if not file_paths:
        raise FileNotFoundError(f"No files found at {input_prefix}")
        

    data, files_to_delete= load_data(fs, file_paths, schema_path=schema_path)
    table = pa.Table.from_pandas(data, preserve_index=False)
    iceberg.write_to_table(
        table=table, 
        time_col=time_col,
    )

    if len(files_to_delete) > 0:
        logger.info("Cleaning up processed files from S3...")
        delete_s3_files(fs, files_to_delete)
