import icechunk as ic
import xarray as xr
import numpy as np
from zarr.errors import GroupNotFoundError

from etl.shared._logging import logger

class IcechunkManager:
    def __init__(self, bucket: str, prefix: str, region: str = "us-east-1"):
        """
        Initialize the IcechunkManager with S3 storage configuration.
        
        Args:
            bucket: S3 bucket name.
            prefix: S3 prefix for Icechunk storage.
            region: AWS region for the S3 bucket. Defaults to "us-east-1".
        """
        
        self.bucket = bucket
        self.prefix = prefix
        self.region = region

        self.storage = self.get_storage()
        self.repo = self.get_or_create_repo()

    def get_storage(self):
        """Create and return an S3 storage configuration for Icechunk."""
        return ic.s3_storage(
            bucket=self.bucket,
            prefix=self.prefix,
            region=self.region
        )

    def get_or_create_repo(self):
        """Open an existing Icechunk repository or create a new one."""
        try:
            repo = ic.Repository.open(self.storage)
        except Exception:
            repo = ic.Repository.create(self.storage)
        return repo

    def write_to_icechunk(self, ds: xr.Dataset, time_dim: str, commit_message: str = None, group: str = None):
        """
        Write or append a dataset to Icechunk, avoiding duplicate time values.
        
        This method checks for existing data and only writes new time values.
        If the group doesn't exist, it creates it with the full dataset.
        
        Args:
            ds: xarray Dataset to write to Icechunk.
            time_dim: Name of the time dimension (e.g., 'init_time', 'valid_time').
            commit_message: Message describing the commit transaction.
            group: Optional Zarr group path. If None, writes to root.
        """
        # Determine what needs to be written using a read-only session
        try:
            session = self.repo.readonly_session("main")
            existing_ds = xr.open_zarr(session.store, group=group, zarr_format=3, consolidated=False)
            existing_times = existing_ds[time_dim].values
            
            mask = ~np.isin(ds[time_dim].values, existing_times)
            ds_to_write = ds.isel({time_dim: mask})
            
            write_kwargs = {"mode": "a", "append_dim": time_dim}
        except GroupNotFoundError:
            # If the store or group doesn't exist yet, write the whole dataset
            ds_to_write = ds
            write_kwargs = {"mode": "w-"}
            
            # Force absolute time encoding from the epoch so appending works correctly
            write_kwargs["encoding"] = {
                time_dim: {"units": "seconds since 1970-01-01", "dtype": "int64"}
            }

        # Check if anything to write
        if ds_to_write[time_dim].size == 0:
            logger.info(f"No new {time_dim} values to write (Group: {group}).")
            return
        
        if commit_message is None:
            commit_message = f"Append data with times: {ds_to_write[time_dim].values}"

        # Only open a transaction if there is data to write
        with self.repo.transaction("main", message=commit_message) as store:
            ds_to_write.to_zarr(
                store=store,
                group=group,
                zarr_format=3,
                consolidated=False,
                **write_kwargs
            )
            logger.info(f"Committed {ds_to_write[time_dim].size} new times to Icechunk: {commit_message} (Group: {group})")

    def read_icechunk(self, group: str = None, branch: str = "main") -> xr.Dataset:
        """
        Read the Icechunk dataset for a specific group and branch.
        
        Args:
            group: Optional Zarr group path. If None, reads from root.
            branch: Branch name to read from (default: 'main').
        """
        try:
            session = self.repo.readonly_session(branch)
            ds = xr.open_zarr(session.store, group=group, zarr_format=3, consolidated=False)
            logger.info(f"Successfully read Icechunk dataset (Group: {group}, Branch: {branch})")
            return ds
        except Exception as e:
            logger.error(f"Error reading from Icechunk (Group: {group}): {e}")
            raise