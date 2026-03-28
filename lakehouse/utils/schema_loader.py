import pyarrow as pa
import json
import os


def json_schema_to_pa_schema(json_schema):
    """Convert JSON schema to PyArrow schema.

    Supports comprehensive type mapping between JSON Schema and PyArrow types,
    including geospatial data types.

    Args:
        json_schema: Either a dict with the JSON schema or a file path string to load
    """
    # If json_schema is a string, treat it as a file path and load it
    if isinstance(json_schema, str):
        if not os.path.exists(json_schema):
            raise FileNotFoundError(f"Schema file not found: {json_schema}")
        with open(json_schema, 'r') as f:
            json_schema = json.load(f)
    # Base type mapping
    base_type_mapping = {
        "string": pa.string(),
        "number": pa.float64(),
        "integer": pa.int64(),
        "boolean": pa.bool_(),
        "null": pa.null(),
    }

    # Format-specific mappings for string type
    string_format_mapping = {
        "date-time": pa.timestamp('us'),
        "datetime": pa.timestamp('us'),
        "date": pa.date32(),
        "time": pa.time64('us'),
        "email": pa.string(),
        "uuid": pa.string(),
        "uri": pa.string(),
        "url": pa.string(),
        "ipv4": pa.string(),
        "ipv6": pa.string(),
        "hostname": pa.string(),
        "json-pointer": pa.string(),
        "relative-json-pointer": pa.string(),
        # Geospatial formats (WKT/WKB text/binary representations)
        "wkt": pa.string(),  # Well-Known Text
        "wkb": pa.binary(),  # Well-Known Binary
        "geojson": pa.string(),  # GeoJSON string representation
        "point": pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())]),
        "linestring": pa.string(),  # WKT representation
        "polygon": pa.string(),  # WKT representation
        "multipoint": pa.string(),  # WKT representation
        "multilinestring": pa.string(),  # WKT representation
        "multipolygon": pa.string(),  # WKT representation
        "geometry": pa.string(),  # Generic geometry as WKT
        "geometrycollection": pa.string(),  # WKT representation
    }

    # Number variant mappings (for "longDescription" or custom formats)
    number_format_mapping = {
        "float": pa.float32(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "double": pa.float64(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
    }

    # Geospatial object type mappings
    geospatial_type_mapping = {
        "point": pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())]),
        "linestring": pa.list_(pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())])),
        "polygon": pa.list_(pa.list_(pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())]))),
        "multipoint": pa.list_(pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())])),
        "multilinestring": pa.list_(pa.list_(pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())]))),
        "multipolygon": pa.list_(pa.list_(pa.list_(pa.struct([pa.field("longitude", pa.float64()), pa.field("latitude", pa.float64())])))),
        "geometry": pa.string(),  # Generic geometry as WKT
        "geometrycollection": pa.string(),
    }

    fields = []
    properties = json_schema.get("properties", {})
    required = json_schema.get("required", [])

    for name, prop in properties.items():
        prop_type = prop.get("type")
        prop_format = prop.get("format")

        # Determine field type
        if prop_type == "string":
            if prop_format and prop_format in string_format_mapping:
                field_type = string_format_mapping[prop_format]
            else:
                field_type = pa.string()
        elif prop_type == "number":
            if prop_format and prop_format in number_format_mapping:
                field_type = number_format_mapping[prop_format]
            else:
                field_type = pa.float64()
        elif prop_type == "integer":
            if prop_format and prop_format in number_format_mapping:
                field_type = number_format_mapping[prop_format]
            else:
                field_type = pa.int64()
        elif prop_type == "array":
            # Handle array items recursively
            items = prop.get("items", {})
            item_type_name = items.get("type", "string")
            item_format = items.get("format")

            if item_type_name == "string" and item_format in string_format_mapping:
                item_type = string_format_mapping[item_format]
            elif item_type_name == "number":
                item_type = number_format_mapping.get(item_format, pa.float64())
            else:
                item_type = base_type_mapping.get(item_type_name, pa.string())

            field_type = pa.list_(item_type)
        elif prop_type == "object":
            # Check if it's a geospatial object type
            if prop_format and prop_format in geospatial_type_mapping:
                field_type = geospatial_type_mapping[prop_format]
            elif prop_type == "geospatial" and prop_format in geospatial_type_mapping:
                field_type = geospatial_type_mapping[prop_format]
            else:
                # For nested objects, default to string representation
                field_type = pa.string()
        elif prop_type == "geospatial":
            # Handle geospatial type at root level
            if prop_format and prop_format in geospatial_type_mapping:
                field_type = geospatial_type_mapping[prop_format]
            else:
                field_type = pa.string()  # Default to WKT string
        else:
            field_type = base_type_mapping.get(prop_type, pa.string())

        # Fields are nullable unless specified otherwise
        nullable = name not in required
        fields.append(pa.field(name, field_type, nullable=nullable))

    return pa.schema(fields)
