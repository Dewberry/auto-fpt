"""Test schema validation for Fort Worth / NCTCOG data transformation using test dataset."""

import json
import jsonschema
import pandas as pd
import pytest

from ft_worth_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/point.json'


def get_test_data() -> pd.DataFrame:
    """Create minimal test dataset based on real Fort Worth / NCTCOG data."""
    data = {
        'site_id': [
            '43714',
            '43714',
            '43750',
            '43750',
        ],
        'sensor_id': [
            '100',
            '101',
            '200',
            '201',
        ],
        'sensor_class': [
            10,
            20,
            11,
            20,
        ],
        'variable': [
            'precip_incremental',
            'stage',
            'precip_cumulative',
            'stage',
        ],
        'data_time': [
            '2026-06-30 06:00:00+00:00',
            '2026-06-30 06:00:00+00:00',
            '2026-06-30 07:00:00+00:00',
            '2026-06-30 07:00:00+00:00',
        ],
        'data_value': [
            0.04,
            2.15,
            1.22,
            3.45,
        ],
        'raw_value': [
            0.04,
            2.15,
            1.22,
            3.45,
        ],
        'data_quality': [
            'Good',
            'Good',
            'Good',
            'Good',
        ],
        'units': [
            'in',
            'ft',
            'in',
            'ft',
        ],
    }

    return pd.DataFrame(data)


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_ft_worth_validation():
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
