"""Test schema validation for NWP data transformation using test dataset."""

import numpy as np
import xarray as xr
import jsonschema
import pytest

from nwp_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/gridded-forecast.json'


def get_test_data() -> xr.Dataset:
    """Create minimal test xarray dataset based on real NWP structure."""
    # Create minimal grid (2x2 instead of 170x193)
    y = np.arange(2)
    x = np.arange(2)
    step = np.array([1, 2, 3, 4, 5, 6], dtype='timedelta64[h]')
    
    # Create dummy coordinates
    latitude = np.array([[30.48, 30.48], [34.99, 34.99]], dtype='float64')
    longitude = np.array([[-99.99, -93.82], [-99.99, -93.82]], dtype='float64')
    
    # Create time and valid_time
    init_time = np.datetime64('2025-05-06T12:00:00')
    valid_time = init_time + step
    
    # Create precipitation data
    tp_data = np.random.uniform(0, 10, (6, 2, 2)).astype('float32')
    
    # Create xarray dataset matching NWP structure
    ds = xr.Dataset(
        data_vars={
            'tp': (['step', 'y', 'x'], tp_data),
        },
        coords={
            'step': step,
            'valid_time': ('step', valid_time),
            'latitude': (['y', 'x'], latitude),
            'longitude': (['y', 'x'], longitude),
            'time': init_time,
            'surface': 0.0,
            'gribfile_projection': None,
        }
    )
    
    # Add some attributes like real GRIB data
    ds.attrs.update({
        'GRIB_edition': 2,
        'product': 'sfc',
        'model': 'hrrr',
    })
    
    return ds


def test_nwp_schema_validation_with_test_data():
    """Test that transformed test data validates against gridded-forecast schema."""
    
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
    
    print(f"✓ Successfully validated NWP data against schema")
    print(f"  Dimensions: {dict(transformed.dims)}")
    print(f"  Data variables: {list(transformed.data_vars.keys())}")
    assert len(transformed.data_vars) > 0, "Test data should have data variables after transformation"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
