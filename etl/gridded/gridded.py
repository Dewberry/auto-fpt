from typing import Union
from jsonschema import ValidationError, validate
import xarray as xr
import s3fs

from etl.gridded.icechunk_utils import IcechunkManager
from etl.shared.exceptions import EmptyDataError
from etl.shared.utils import delete_s3_files, read_s3_files, load_schema
from etl.shared._logging import logger, configure_logger

configure_logger(level="INFO")


def validate_dataset_schema(ds: xr.Dataset, schema_path: str):
    """Validate an xarray Dataset against a JSON schema."""
    schema = load_schema(schema_path)
    validate(instance=ds.to_dict(data=False), schema=schema)


def load_data(s3_paths: list[str], schema_path: str, concat_dim: str = "valid_time") -> xr.Dataset:
    """Loads gridded data from S3. Default implementation assumes NetCDF/Zarr files."""
    
    datasets = []
    files_to_delete = []
    
    for file_path in s3_paths:
        try:
            ds = xr.open_dataset(file_path, engine='h5netcdf')
            if len(ds.data_vars) == 0:
                raise EmptyDataError(f"Dataset has no data variables for file: {file_path}")
            validate_dataset_schema(ds, schema_path=schema_path)
            datasets.append(ds)
            files_to_delete.append(file_path)
        except EmptyDataError as e:
            logger.error(f"Error reading {file_path}: {e}")
            files_to_delete.append(file_path)
        except ValueError as e: # Corrupt files raise ValueError
            logger.error(f"Error reading {file_path}: {e}")
            files_to_delete.append(file_path)
        except ValidationError as e:
            logger.error(f"Validation error for {file_path}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error reading {file_path}: {e}")

    if not datasets:
        raise RuntimeError(f"Could not read any valid data from {s3_paths}")
    
    ds = xr.combine_nested(
        datasets, 
        concat_dim=concat_dim
    )
    return ds, files_to_delete


def process_gridded_source(
    fs: s3fs.S3FileSystem,
    source: str,
    input_prefix: str,
    schema_path: str,
    bucket: str,
    icechunk_prefix: str,
    concat_dim: str,
    region: str,
    group: str = None,
):

    s3_paths = read_s3_files(fs, input_prefix)

    if not s3_paths:
        raise FileNotFoundError(f"No files found at {input_prefix}")
    
    manager = IcechunkManager(bucket=bucket, prefix=icechunk_prefix, region=region)

    logger.info(f"Loading data for {source} from {s3_paths}...")
    data, files_to_delete = load_data(
        s3_paths=s3_paths,
        schema_path=schema_path,
        concat_dim = concat_dim
    )
    manager.write_to_icechunk(
        ds=data,
        time_dim=concat_dim,
        commit_message=f"Add {source} data",
        group=group
    )
    if len(files_to_delete) > 0:
        logger.info("Cleaning up processed files from S3...")
        delete_s3_files(fs, files_to_delete)

    logger.info(f"Successfully processed and wrote data for {source}")