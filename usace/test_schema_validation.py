"""Test schema validation for USACE reservoir data transformation using test dataset."""

import json
import jsonschema
import pandas as pd
import pytest

from usace_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/point.json'


def get_test_data() -> pd.DataFrame:
    """Create minimal test dataset based on real USACE reservoir data."""
    data = {
        'site_id': [
            'GRDT2',
            'GRDT2',
            'LWDT2',
            'LWDT2',
        ],
        'variable': [
            'outflow',
            'inflow',
            'outflow',
            'inflow',
        ],
        'datetime': [
            '2026-05-06 18:00:00+00:00',
            '2026-05-06 18:00:00+00:00',
            '2026-05-06 19:00:00+00:00',
            '2026-05-06 19:00:00+00:00',
        ],
        'value': [
            1500.0,
            1200.0,
            850.0,
            900.0,
        ],
        'unit': [
            'cfs',
            'cfs',
            'cfs',
            'cfs',
        ],
    }

    return pd.DataFrame(data)


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_usace_validation():
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
