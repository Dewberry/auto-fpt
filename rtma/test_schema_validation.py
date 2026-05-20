"""Test schema validation for NWP data transformation using test dataset."""

import numpy as np
import xarray as xr
import jsonschema
import pytest

from rtma_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/gridded-forecast.json'


def get_test_data() -> xr.Dataset:
    """Create minimal test xarray dataset based on real RTMA structure."""
    # Create minimal grid (2x2 instead of 202x227)
    latitude = np.array([[30.44, 30.44], [35.06, 35.06]], dtype='float64')
    longitude = np.array([[-99.99, -93.98], [-99.99, -93.98]], dtype='float64')

    valid_time = np.datetime64('2026-05-13T18:00:00')
    t2m_data = np.random.uniform(290, 310, (2, 2)).astype('float32')

    ds = xr.Dataset(
        data_vars={
            't2m': (['y', 'x'], t2m_data),
        },
        coords={
            'latitude': (['y', 'x'], latitude),
            'longitude': (['y', 'x'], longitude),
            'time': np.datetime64('2026-05-13T18:00:00'),
            'step': np.timedelta64(0, 'ns'),
            'heightAboveGround': 2.0,
            'valid_time': valid_time,
            'gribfile_projection': None,
        }
    )

    ds.attrs.update({
        'GRIB_edition': 2,
        'GRIB_centre': 'kwbc',
        'GRIB_centreDescription': 'US National Weather Service - NCEP',
        'GRIB_subCentre': 4,
        'Conventions': 'CF-1.7',
        'institution': 'US National Weather Service - NCEP',
        'model': 'rtma',
        'product': 'anl',
        'description': 'CONUS Real-Time Mesoscale Analysis (RTMA)',
        'GRIB_parameterUnits': 'K',
        'GRIB_units': 'K',
    })

    return ds


def test_rtma_schema_validation_with_test_data():
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
