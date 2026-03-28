from lakehouse.utils.iceberg import check_existing_tables, create_table
from lakehouse.utils.schema_loader import json_schema_to_pa_schema


def main(table_name: str, schema_path: str):
    """Add new table to Iceberg catalog."""
    print(f"Adding New Table: {table_name}")

    # Check existing tables
    table_status = check_existing_tables()

    if table_status["missing"]:
        print(f"\n{len(table_status['missing'])} tables need to be created")
        print("\nCreating missing tables...")

        # Create table if missing
        if table_name in table_status["missing"]:
            pa_schema = json_schema_to_pa_schema(schema_path)
            create_table(table_name, pa_schema)
    else:
        print(f"\nAll tables are initialized!\n")


if __name__ == "__main__":
    table_name = "usgs"
    schema_path = "<path>/auto-fpt/lakehouse/schemas/observations-usgs.json"
    main(table_name, schema_path)