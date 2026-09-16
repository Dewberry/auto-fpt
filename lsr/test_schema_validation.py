"""Test schema validation for LSR data transformation using test dataset."""

import jsonschema
import pandas as pd
import pytest

from lsr_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/lsr.json'


def get_test_data() -> pd.DataFrame:
    """Create minimal test dataset based on real LSR (IEM GeoJSON) output."""
    data = {
        'valid': [
            '2026-08-25T10:00:00Z',
            '2026-08-25T11:00:00Z',
            '2026-08-25T12:00:00Z',
        ],
        'event_type': ['FLOOD', 'FLASH FLOOD', 'FLOOD'],
        'magnitude': [2.5, None, 1.0],
        'unit': ['INCH', None, 'INCH'],
        'wfo': ['FWD', 'FWD', 'FWD'],
        'county': ['TARRANT', 'DALLAS', 'DENTON'],
        'state': ['TX', 'TX', 'TX'],
        'city': ['FORT WORTH', 'DALLAS', None],
        'source': ['TRAINED SPOTTER', 'PUBLIC', 'MESONET'],
        'remark': ['Water over roadway', None, 'Minor street flooding'],
        'product_id': ['202608251000-KFWD-NWUS54', '202608251100-KFWD-NWUS54', '202608251200-KFWD-NWUS54'],
        'longitude': [-97.3308, -96.7970, -97.1331],
        'latitude': [32.7555, 32.7767, 33.2148],
    }

    return pd.DataFrame(data)


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_lsr_validation():
    """Test that transformed test data validates against schema."""

    # Load test data
    test_data = get_test_data()

    # Transform the data
    transformed = transform(test_data)

    # Load schema
    schema = load_schema(SCHEMA_PATH)

    # Convert geometry to WKB for validation (matching parquet output)
    transformed["geometry"] = transformed["geometry"].apply(lambda g: g.wkb.hex())

    # Validate each record
    for idx, record in enumerate(transformed.to_dict('records')):
        try:
            validate_record(record, schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Record {idx} failed schema validation: {e.message}\nRecord: {record}")

    print(f"Successfully validated {len(transformed)} records against schema")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
