from jsonschema import ValidationError, validate
import numpy as np
import s3fs
import xarray as xr
from datetime import datetime, UTC

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
    # Remove duplicates along the concat_dim
    _, unique_indices = np.unique(ds[concat_dim].values, return_index=True)
    ds = ds.isel({concat_dim: unique_indices})

    return ds, files_to_delete

def squeeze_gridded_forecast(ds: xr.Dataset) -> xr.Dataset:
    """Squeeze out init_time dimension from forecast dataset for easier ingestion into HMS."""

    ds = (
        ds.squeeze("init_time", drop=True)
        .assign_coords(step=ds.valid_time.squeeze("init_time"))
        .swap_dims({"step": "valid_time"})
        .drop_vars(["step", "init_time"])
        .rename({"valid_time": "time"})
    )
    keep_attrs = {"long_name", "units", "standard_name", "grid_mapping", "coordinates"}
    for var in ds.data_vars:
        ds[var].attrs = {k: v for k, v in ds[var].attrs.items() if k in keep_attrs}
        ds[var].encoding["coordinates"] = "crs latitude longitude time"

    return ds

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

def process_gridded_forcing_ds(ds: xr.Dataset, variable: str | list[str], append_param: str, lookback_hours: int | None, end_date: datetime | None = None) -> xr.Dataset:
    """Process the gridded dataset by subsetting variables, ensuring uniqueness, and filtering by time."""

    if variable:
        ds = ds[variable] if isinstance(variable, list) else ds[[variable]]

    # Make sure duplicates are removed
    _, unique_idx = np.unique(ds[append_param].values, return_index=True)
    ds = ds.isel({append_param: unique_idx})

    if end_date is None:
        latest_time = np.datetime64(datetime.now(UTC).replace(tzinfo=None), 'ns')
    else:
        latest_time = np.datetime64(end_date.replace(tzinfo=None), 'ns')

    if lookback_hours is not None:
        earliest_time = latest_time - np.timedelta64(int(lookback_hours), 'h')
        latest_ds = ds.where(
            (ds[append_param] >= earliest_time) & (ds[append_param] <= latest_time),
            drop=True
        )
    else:
        closest_time = ds[append_param].sel({append_param: latest_time}, method="nearest").values
        latest_ds = ds.where(ds[append_param] == closest_time, drop=True)

    if "init_time" in latest_ds.coords:
        latest_ds = squeeze_gridded_forecast(latest_ds)

    latest_ds.attrs.update({
        "Conventions": "CF-1.9"})
    
    if append_param in latest_ds.coords and append_param != "time":
        latest_ds[append_param].attrs.update({"long_name": "time"})
        latest_ds = latest_ds.rename({append_param: "time"})

    return latest_ds

