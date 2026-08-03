"""Test schema validation for Waze data transformation using test dataset."""

import json
import jsonschema
import geopandas as gpd
import pytest
from datetime import datetime, timezone
from shapely.geometry import Point

from waze_latest import transform
from shared.utils import load_schema

SCHEMA_PATH = './schemas/waze.json'


def get_test_data() -> gpd.GeoDataFrame:
    """Create minimal test dataset based on real Waze alert data."""
    data = {
        'uuid': [
            'abc123-def456',
            'ghi789-jkl012',
            'mno345-pqr678',
        ],
        'type': [
            'WEATHERHAZARD',
            'ROAD_CLOSED',
            'WEATHERHAZARD',
        ],
        'subtype': [
            'HAZARD_WEATHER_FLOOD',
            'ROAD_CLOSED_EVENT',
            'HAZARD_WEATHER_FLOOD',
        ],
        'fromNodeId': [101, 202, 303],
        'toNodeId': [201, 302, 403],
        'reportByMunicipalityUser': ['false', 'true', 'false'],
        'reportRating': [5, 3, 4],
        'confidence': [3, 0, 2],
        'reliability': [8, 5, 7],
        'street': ['Main St', 'Oak Ave', 'Elm St'],
        'roadType': [2, 1, 3],
        'reportDescription': [
            'Road flooding near intersection',
            'Road closed for construction',
            'Flood water rising',
        ],
        'nThumbsUp': [2, 1, 0],
        'magvar': [90, 180, 270],
        'city': ['Fort Worth, TX', 'Arlington, TX', 'Dallas, TX'],
        'country': ['US', 'US', 'US'],
        'timestamp': [
            '2026-08-03T12:00:00Z',
            '2026-08-03T12:05:00Z',
            '2026-08-03T12:10:00Z',
        ],
        'geometry': [
            Point(-97.3308, 32.7555),
            Point(-97.3200, 32.7600),
            Point(-96.7970, 32.7767),
        ],
    }

    return gpd.GeoDataFrame(data, crs="EPSG:4326")


def validate_record(record: dict, schema: dict) -> None:
    """Validate a single record against the schema."""
    jsonschema.validate(record, schema)


def test_waze_validation():
    """Test that transformed test data validates against schema."""

    # Load test data
    test_data = get_test_data()

    # Transform the data
    now = datetime(2026, 8, 3, 12, 15, 0, tzinfo=timezone.utc)
    transformed = transform(test_data, search_term="flood", now=now)

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
