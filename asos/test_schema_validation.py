"""Test schema validation for ASOS precipitation data transformation using test dataset."""

import json
import jsonschema
import pandas as pd
import pytest

from asos_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/point.json'


def get_test_data() -> pd.DataFrame:
    """Create minimal test dataset based on real ASOS data."""
    data = {
        'station': [
            'KDFW',
            'KDFW',
            'KDAL',
            'KFTW',
            'KGKY',
        ],
        'valid': [
            '2026-05-06 18:00:00+00:00',
            '2026-05-06 18:15:00+00:00',
            '2026-05-06 18:00:00+00:00',
            '2026-05-06 18:30:00+00:00',
            '2026-05-06 19:00:00+00:00',
        ],
        'p01i': [
            0.01,
            0.02,
            'M',
            0.10,
            0.00,
        ],
        'lon': [
            -97.038,
            -97.038,
            -96.851,
            -97.362,
            -97.093,
        ],
        'lat': [
            32.897,
            32.897,
            32.847,
            32.820,
            32.663,
        ],
    }

    return pd.DataFrame(data)


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_asos_validation():
    """Test that transformed test data validates against schema."""

    # Load test data
    test_data = get_test_data()

    # Variable is added by fetch_asos_data(), not the reaper
    test_data["variable"] = "precip_routine"

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
