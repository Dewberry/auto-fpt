"""Test schema validation for WPC Excessive Rainfall Outlook data transformation."""

import json
import jsonschema
import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from wpc_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/wpc-ero.json'


def get_test_data() -> gpd.GeoDataFrame:
    """Create minimal test dataset based on real WPC ERO shapefile output."""
    data = {
        'dn': [1, 2, 1],
        'PRODUCT': [
            'Day 1 Excessive Rainfall Potential Forecast',
            'Day 1 Excessive Rainfall Potential Forecast',
            'Day 2 Excessive Rainfall Potential Forecast',
        ],
        'VALID_TIME': [
            '12Z 08/24/26 - 12Z 08/25/26',
            '12Z 08/24/26 - 12Z 08/25/26',
            '12Z 08/25/26 - 12Z 08/26/26',
        ],
        'OUTLOOK': [
            'Marginal (At Least 5%)',
            'Slight (At Least 15%)',
            'Marginal (At Least 5%)',
        ],
        'ISSUE_TIME': [
            '2026-08-24 09:02:00',
            '2026-08-24 09:02:00',
            '2026-08-24 09:02:00',
        ],
        'START_TIME': [
            '2026-08-24 12:00:00',
            '2026-08-24 12:00:00',
            '2026-08-25 12:00:00',
        ],
        'END_TIME': [
            '2026-08-25 12:00:00',
            '2026-08-25 12:00:00',
            '2026-08-26 12:00:00',
        ],
        'Snippet': [
            '12Z 08/24/26 - 12Z 08/25/26',
            '12Z 08/24/26 - 12Z 08/25/26',
            '12Z 08/25/26 - 12Z 08/26/26',
        ],
        'day': ['day1', 'day1', 'day2'],
        'issuance': ['morning', 'morning', 'morning'],
    }

    polygons = [
        Polygon([(-97, 32), (-96, 32), (-96, 33), (-97, 33), (-97, 32)]),
        Polygon([(-96.5, 32.2), (-96, 32.2), (-96, 32.8), (-96.5, 32.8), (-96.5, 32.2)]),
        Polygon([(-95, 30), (-94, 30), (-94, 31), (-95, 31), (-95, 30)]),
    ]

    return gpd.GeoDataFrame(data, geometry=polygons, crs="EPSG:4326")


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_wpc_ero_validation():
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


def test_outlook_cleaned():
    """Test that parenthetical text is removed from outlook column."""
    test_data = get_test_data()
    transformed = transform(test_data)

    for outlook in transformed["outlook"]:
        assert "(" not in outlook
        assert ")" not in outlook


def test_columns_dropped():
    """Test that unwanted columns are removed."""
    test_data = get_test_data()
    transformed = transform(test_data)

    assert "dn" not in transformed.columns
    assert "Snippet" not in transformed.columns
    assert "issuance" not in transformed.columns
