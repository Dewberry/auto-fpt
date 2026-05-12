from jsonschema import ValidationError, validate
import s3fs
import xarray as xr

from etl.gridded.icechunk_utils import IcechunkManager
from etl.shared.exceptions import EmptyDataError
from etl.shared.utils import delete_s3_files, read_s3_files, load_schema
from etl.shared._logging import logger


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
            # If schema validation fails, we want to save the file for further investigation
        except Exception as e: 
            logger.error(f"Unexpected error reading {file_path}: {e}")
            # If error is unexpected, we want to save the file for further investigation

    if not datasets:
        raise RuntimeError(f"Could not read any valid data from {s3_paths}")
    
    ds = xr.combine_nested(
        datasets, 
        concat_dim=concat_dim
    )
    return ds, files_to_delete


def process_gridded_source(
    fs: s3fs.S3FileSystem,
    input_prefix: str,
    schema_path: str,
    bucket: str,
    icechunk_prefix: str,
    concat_dim: str,
    region: str,
    group: str = None,
):
    """
    Process a gridded data source by loading data from S3, validating it, and writing to Icechunk.
    
    Args:
        fs: s3fs filesystem instance.
        input_prefix: S3 prefix to read input files from.
        schema_path: Local path to JSON schema for validation.
        bucket: S3 bucket name for Icechunk storage.
        icechunk_prefix: S3 prefix for Icechunk storage.
        concat_dim: Dimension name to concatenate on.
        region: AWS region for S3 bucket.
        group: Optional Zarr group path for Icechunk. If None, writes to root.
    """
    s3_paths = read_s3_files(fs, input_prefix)

    if not s3_paths:
        raise FileNotFoundError(f"No files found at {input_prefix}")
    
    manager = IcechunkManager(bucket=bucket, prefix=icechunk_prefix, region=region)

    data, files_to_delete = load_data(
        s3_paths=s3_paths,
        schema_path=schema_path,
        concat_dim = concat_dim
    )
    manager.write_to_icechunk(
        ds=data,
        time_dim=concat_dim,
        group=group
    )
    if len(files_to_delete) > 0:
        logger.info("Cleaning up processed files from S3...")
        delete_s3_files(fs, files_to_delete)
