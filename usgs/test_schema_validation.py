"""Test schema validation for USGS data transformation using test dataset."""

import json
import jsonschema
import pandas as pd
import pytest

from usgs_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/point.json'


def get_test_data() -> pd.DataFrame:
    """Create minimal test dataset based on real USGS data."""
    data = {
        'monitoring_location_id': [
            'USGS-08049300',
            'USGS-08049300',
            'USGS-08060500',
            'USGS-08061540',
            'USGS-332210096454601',
        ],
        'parameter_code': [
            '00060',
            '00065',
            '62614',
            '00060',
            '00045',
        ],
        'time': [
            '2026-05-06 18:45:00+00:00',
            '2026-05-06 18:45:00+00:00',
            '2026-05-06 19:00:00+00:00',
            '2026-05-06 19:15:00+00:00',
            '2026-05-06 19:30:00+00:00',
        ],
        'value': [
            289.0,
            43.42,
            492.25,
            40.30,
            0.0,
        ],
        'approval_status': [
            'Provisional',
            'Provisional',
            'Provisional',
            'Provisional',
            'Provisional',
        ],
    }
    
    return pd.DataFrame(data)


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_usgs_validation():
    """Test that transformed test data validates against schema."""
    
    # Load test data
    test_data = get_test_data()
    
    # Transform the data
    transformed = transform(test_data)
    
    # Load schema
    schema = load_schema(SCHEMA_PATH)
    
    # Validate each record
    for idx, record in enumerate(transformed.to_dict('records')):
        try:
            validate_record(record, schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Record {idx} failed schema validation: {e.message}\nRecord: {record}")
    
    print(f"✓ Successfully validated {len(transformed)} records against schema")

if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
