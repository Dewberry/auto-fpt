import yaml
import os
from pyiceberg.catalog import load_catalog
import pyarrow as pa

def load_config(config_path: str = None):
    """Load configuration from config.yaml.

    Args:
        config_path: Path to config.yaml. If None, looks for it in the setup directory.
    """
    if config_path is None:
        # Default to config.yaml in the setup directory
        config_path = os.path.join(
            os.path.dirname(__file__),
            "../config.yaml"
        )

    config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_catalog(config_path: str = None):
    """Load Glue Catalog with configuration from config.yaml."""
    config = load_config(config_path)
    catalog_config = config.get('catalog', {})

    return load_catalog(
        "local",
        **catalog_config
    )

def get_namespace(config_path: str = None):
    """Get the default namespace from config."""
    config = load_config(config_path)
    return config.get('warehouse', {}).get('namespace', 'default')


def get_tables(config_path: str = None):
    """Get list of tables from config."""
    config = load_config(config_path)
    return config.get('warehouse', {}).get('tables', [])


def check_existing_tables(config_path: str = None):
    """Check which tables exist in the catalog."""
    try:
        catalog = get_catalog(config_path)
        namespace = get_namespace(config_path)
        tables = get_tables(config_path)

        print(f"\nChecking tables in namespace: {namespace}")
        print(f"   Configured tables: {', '.join(tables)}\n")

        existing = []
        missing = []

        for table_name in tables:
            full_name = f"{namespace}.{table_name}"
            try:
                catalog.load_table(full_name)
                existing.append(table_name)
                print(f"{table_name} - exists")
            except Exception:
                missing.append(table_name)
                print(f"{table_name} - NOT FOUND")

        print(f"\nSummary: {len(existing)} existing, {len(missing)} missing")
        return {"existing": existing, "missing": missing}

    except Exception as e:
        print(f"Error connecting to catalog: {e}")
        raise


def create_table(table_name: str, schema: pa.Schema, config_path: str = None):
    """Create an Iceberg table in the catalog.

    Args:
        table_name: Name of the table to create
        schema: PyArrow schema for the table
        config_path: Path to config.yaml
    """
    try:
        catalog = get_catalog(config_path)
        namespace = get_namespace(config_path)
        full_table_name = f"{namespace}.{table_name}"

        # Get warehouse location from config
        config = load_config(config_path)
        warehouse = config.get('catalog', {}).get('warehouse', '')
        table_location = f"{warehouse}/{namespace}/{table_name}"

        # Check if table already exists
        try:
            catalog.load_table(full_table_name)
            print(f"Table {full_table_name} already exists")
            return False
        except:
            pass  # Table doesn't exist, proceed with creation

        # Create table
        _ = catalog.create_table(
            full_table_name,
            schema=schema,
            location=table_location
        )

        print(f"Created table {full_table_name} at {table_location}")
        return True

    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        raise
