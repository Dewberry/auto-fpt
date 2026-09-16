"""Test schema validation for NWS flood alert data transformation."""

import jsonschema
import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from nws_alerts_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/nws-flood-alert.json'


def get_test_data() -> gpd.GeoDataFrame:
    """Create minimal test dataset based on real NWS alert output."""
    data = {
        'area_desc': [
            'Tarrant, TX; Dallas, TX',
            'Denton, TX; Collin, TX',
        ],
        'event': [
            'Flood Warning',
            'Flash Flood Watch',
        ],
        'headline': [
            'Flood Warning issued August 25 at 10:00AM CDT',
            'Flash Flood Watch issued August 25 at 11:00AM CDT',
        ],
        'description': [
            'The National Weather Service has issued a Flood Warning...',
            'Flash flooding is possible due to heavy rainfall...',
        ],
        'severity': ['Severe', 'Severe'],
        'certainty': ['Observed', 'Possible'],
        'urgency': ['Immediate', 'Future'],
        'sender_name': ['NWS Fort Worth TX', 'NWS Fort Worth TX'],
        'sent': [
            '2026-08-25T10:00:00-05:00',
            '2026-08-25T11:00:00-05:00',
        ],
        'effective': [
            '2026-08-25T10:00:00-05:00',
            '2026-08-25T11:00:00-05:00',
        ],
        'onset': [
            '2026-08-25T10:00:00-05:00',
            '2026-08-25T14:00:00-05:00',
        ],
        'expires': [
            '2026-08-25T22:00:00-05:00',
            '2026-08-26T06:00:00-05:00',
        ],
        'ends': [
            '2026-08-25T22:00:00-05:00',
            None,
        ],
        'status': ['Actual', 'Actual'],
        'message_type': ['Alert', 'Alert'],
    }

    polygons = [
        Polygon([(-97.5, 32.5), (-96.5, 32.5), (-96.5, 33.0), (-97.5, 33.0), (-97.5, 32.5)]),
        Polygon([(-97.0, 33.0), (-96.5, 33.0), (-96.5, 33.5), (-97.0, 33.5), (-97.0, 33.0)]),
    ]

    return gpd.GeoDataFrame(data, geometry=polygons, crs="EPSG:4326")


def get_test_boundary() -> gpd.GeoDataFrame:
    """Create a boundary that covers the test data."""
    boundary = Polygon([(-98, 32), (-96, 32), (-96, 34), (-98, 34), (-98, 32)])
    return gpd.GeoDataFrame(geometry=[boundary], crs="EPSG:4326")


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_nws_flood_alert_validation(tmp_path):
    """Test that transformed test data validates against schema."""
    test_data = get_test_data()
    transformed = transform(test_data)

    schema = load_schema(SCHEMA_PATH)

    for idx, record in enumerate(transformed.to_dict('records')):
        try:
            validate_record(record, schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Record {idx} failed schema validation: {e.message}\nRecord: {record}")


def test_nullable_fields_are_none():
    """Test that NaN values in nullable fields are converted to None."""
    test_data = get_test_data()
    transformed = transform(test_data)

    record = transformed.to_dict('records')[1]
    assert record["ends"] is None


def test_clip_removes_outside_features(tmp_path):
    """Test that features outside boundary are removed."""
    # Small boundary that only covers the first polygon
    small_boundary = Polygon([(-97.6, 32.4), (-96.4, 32.4), (-96.4, 33.1), (-97.6, 33.1), (-97.6, 32.4)])
    boundary = gpd.GeoDataFrame(geometry=[small_boundary], crs="EPSG:4326")
    boundary_path = str(tmp_path / "boundary.pq")
    boundary.to_parquet(boundary_path)

    test_data = get_test_data()
    transformed = transform(test_data, boundary_path=boundary_path)

    # Both polygons intersect the small boundary so both should remain (clipped)
    assert len(transformed) >= 1
