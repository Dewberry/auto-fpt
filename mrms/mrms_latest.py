from cosecha.reaping.mrms import MRMSReaper
from cosecha import configure_logger
from datetime import datetime, timedelta, timezone
import logging
import xarray as xr

from shared.constants import REGION_BOUNDS
from shared.utils import save_netcdf_to_s3, parse_tz_aware_time, generate_default_path

configure_logger(level="INFO")

DEFAULT_VARIABLE = "MultiSensor_QPE_01H_Pass2_00.00" 

MRMS_VARIABLE_MAPPING = {
    'MultiSensor_QPE_01H_Pass2_00.00': {'var_name': "qpe_1hr", 'qc_flag': 'pass2', 'units': 'mm'},
    'RadarOnly_QPE_15M_00.00': {'var_name': "qpe_15min", 'qc_flag': 'radar_only', 'units': 'mm'},
}

def transform(ds: xr.Dataset) -> xr.Dataset:
    """Apply transformations to each dataset in the dictionary, such as renaming variables and dropping unnecessary ones."""

    for var, attrs in MRMS_VARIABLE_MAPPING.items():
        if var in ds.variables:
            ds[var].attrs['qc_flag'] = attrs['qc_flag']
            ds[var].attrs['units'] = attrs['units']

    rename_mapping = {k: v['var_name'] for k, v in MRMS_VARIABLE_MAPPING.items() if k in ds.variables}
    if rename_mapping:
        ds = ds.rename(rename_mapping)

            
    ds = ds.drop_vars(['step', 'heightAboveSea', 'valid_time'], errors='ignore')
    return ds

def fetch_mrms_data(variable: str, input_time: str | tuple[str, str]) -> tuple[MRMSReaper, xr.Dataset]:
    """Fetch raw MRMS data from the API."""
    logging.info(f"Fetching MRMS data for variable {variable} for time {input_time} UTC...")
    
    reaper = MRMSReaper(
        dates=input_time,
        variable=variable,
        transformations={
            "spatial_subset": {
                'lat_bounds': (REGION_BOUNDS[1], REGION_BOUNDS[3]),
                'lon_bounds': (REGION_BOUNDS[0], REGION_BOUNDS[2])
            }
        }
    )

    return reaper, reaper.reap()

def main(start_time: datetime | None, end_time: datetime | None, variable: str, output_path: str) -> None:
    """Orchestrates the data extraction and saving for MRMS."""

    if not output_path.lower().endswith(('.nc',)):
        raise ValueError("Output path must end with '.nc'")
    
    if start_time is None and end_time is None:
        input_time = "latest"
    elif start_time is not None and end_time is not None:
        input_time = (start_time.strftime("%Y-%m-%d %H:%M"), end_time.strftime("%Y-%m-%d %H:%M"))
    else:
        raise ValueError("Both start_time and end_time must be provided if one is given.")
    
    logging.info(f"Fetching MRMS data for variable {variable}, time {input_time} UTC...")
    
    reaper, data = fetch_mrms_data(variable, input_time)
    reaper.data = transform(data)

    save_netcdf_to_s3(reaper, output_path)
    
    logging.info("Operation complete")

def handler(event = None, context = None):
    """AWS Lambda handler. Extracts parameters from the event dict and runs the pipeline."""
    if event is None:
        event = {}

    variable = event.get("variable", DEFAULT_VARIABLE)
    
    start_time_raw = event.get("start_time")
    end_time_raw = event.get("end_time")

    if (start_time_raw is None) != (end_time_raw is None):
        raise ValueError("Both start_time and end_time must be provided if one is given.")

    if start_time_raw and end_time_raw:
        start_time = parse_tz_aware_time(start_time_raw)
        end_time = parse_tz_aware_time(end_time_raw)
    else:
        start_time = None
        end_time = None

    output_path = event.get("output_path")
    
    if output_path is None:
        clean_var = variable.rsplit('_', 1)[0].lower() # Remove height from var name for cleaner output path (e.g. "MultiSensor_QPE_01H_Pass2_00.00" -> "multisensor_qpe_01h_pass2"
        dt_now = datetime.now(tz=timezone.utc)
        output_path = generate_default_path(f"mrms/{clean_var}", dt_now, "nc")

    main(
        start_time=start_time,
        end_time=end_time,
        variable=variable,
        output_path=output_path
    )
    
    time_str = "latest" if start_time is None else f"{start_time.isoformat()} to {end_time.isoformat()}"

    return {
        "statusCode": 200,
        "body": {
            "message": "Success",
            "output_path": output_path,
            "variable": variable,
            "time": time_str
        }
    }

