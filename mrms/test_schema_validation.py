"""Test schema validation for MRMS data transformation using test dataset."""

import json
import numpy as np
import xarray as xr
import jsonschema
import pytest

from mrms_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/gridded-analysis.json'


def get_test_data() -> xr.Dataset:
    """Create minimal test xarray dataset based on real MRMS structure."""
    # Create minimal grid
    lat = np.linspace(30.51, 32.51, 3)
    lon = np.linspace(-100.0, -98.0, 3)
    time = np.array(['2026-05-06T19:00:00'], dtype='datetime64[ns]')
    
    qpe_data = np.random.uniform(0, 50, (1, 3, 3)).astype('float32')
    
    # Create xarray dataset matching MRMS structure
    ds = xr.Dataset(
        data_vars={
            'MultiSensor_QPE_01H_Pass2_00.00': (['time', 'latitude', 'longitude'], qpe_data),
        },
        coords={
            'time': time,
            'latitude': lat,
            'longitude': lon,
            'step': np.timedelta64(0, 'ns'),
            'heightAboveSea': 0.0,
            'valid_time': np.datetime64('2026-05-06T19:00:00'),
        }
    )
    
    return ds


def test_mrms_schema_validation_with_test_data():
    """Test that transformed test data validates against schema."""
    
    # Load test data
    test_data = get_test_data()
    
    # Transform the data
    transformed = transform(test_data)
    
    # Convert dataset to dict representation (without data values)
    ds_dict = transformed.to_dict(data=False)
    
    # Load and validate against schema
    schema = load_schema(SCHEMA_PATH)
    
    try:
        jsonschema.validate(ds_dict, schema)
    except jsonschema.ValidationError as e:
        pytest.fail(f"Schema validation failed: {e.message}\nDataset: {ds_dict}")
    
    print(f"✓ Successfully validated MRMS data against schema")
    print(f"  Dimensions: {dict(transformed.dims)}")
    print(f"  Data variables: {list(transformed.data_vars.keys())}")
    assert len(transformed.data_vars) > 0, "Test data should have data variables after transformation"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
