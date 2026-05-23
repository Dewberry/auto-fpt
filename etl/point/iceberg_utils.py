import os

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual

from etl.shared._logging import logger


class IcebergManager:
    def __init__(
        self, warehouse_path: str, region: str, namespace: str, table_name: str
    ):
        self.warehouse_path = warehouse_path
        self.region = region
        self.namespace = namespace
        self.table_name = table_name

        self.catalog = self.get_catalog()
        self.full_table_name = f"{self.namespace}.{self.table_name}"

    def get_catalog(self):
        """Load Glue Catalog with configuration from config.yaml."""
        os.environ["AWS_DEFAULT_REGION"] = self.region
        os.environ["AWS_REGION"] = self.region

        catalog_config = {"s3.region": self.region, "warehouse": self.warehouse_path}

        if self.warehouse_path.startswith("s3://"):
            catalog_config["type"] = "glue"
            catalog_config["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"

        return load_catalog("local", **catalog_config)

    def delete_table(self) -> bool:
        """Drop the table from the catalog."""
        try:
            self.catalog.drop_table(self.full_table_name)
            logger.info(f"Dropped table {self.full_table_name} from catalog.")
            return True
        except Exception as e:
            logger.error(f"Error dropping table {self.table_name}: {e}")
            return False

    def create_table(self, schema: pa.Schema) -> bool:
        """Create a new table in the catalog using the given schema."""
        try:
            try:
                self.catalog.load_table(self.full_table_name)
                logger.info(f"Table {self.full_table_name} already exists")
                return False
            except NoSuchTableError:
                pass

            table_location = f"{self.warehouse_path}/{self.namespace}/{self.table_name}"

            _ = self.catalog.create_table(
                self.full_table_name, schema=schema, location=table_location
            )
            logger.info(f"Created table {self.full_table_name} at {table_location}")
            return True
        except Exception as e:
            logger.error(f"Error creating table {self.table_name}: {e}")
            raise

    def write_to_table(
        self, table: pa.Table, time_col: str = None, unique_cols: list[str] = None
    ) -> bool:
        """Append a PyArrow table to the Iceberg table, de-duplicating on unique_cols or all columns when unique_cols is None."""
        try:
            try:
                iceberg_table = self.catalog.load_table(self.full_table_name)
                logger.info(
                    f"Table {self.full_table_name} already exists. Appending data..."
                )
            except Exception:
                logger.info(
                    f"Table {self.full_table_name} does not exist. Creating it..."
                )
                try:
                    self.catalog.create_namespace(self.namespace)
                except Exception:
                    pass
                self.create_table(table.schema)
                iceberg_table = self.catalog.load_table(self.full_table_name)

            if time_col:
                dedupe_cols = unique_cols if unique_cols else table.column_names

                # Use a time window to only scan relevant historical data
                min_time = pc.min(table.column(time_col)).as_py()
                max_time = pc.max(table.column(time_col)).as_py()

                time_filter = And(
                    GreaterThanOrEqual(time_col, min_time),
                    LessThanOrEqual(time_col, max_time),
                )
                existing_data = iceberg_table.scan(row_filter=time_filter).to_arrow()

                if len(existing_data) > 0:
                    df_new = table.to_pandas()
                    df_existing = existing_data.to_pandas()

                    # If no unique columns are provided, remove duplicate incoming rows using all columns.
                    if not unique_cols:
                        df_new = df_new.drop_duplicates(subset=dedupe_cols)

                    # Left anti-join to keep only rows from df_new that don't match existing data on dedupe_cols.
                    merged = df_new.merge(
                        df_existing[dedupe_cols].drop_duplicates(),
                        on=dedupe_cols,
                        how="left",
                        indicator=True,
                    )
                    df_to_append = merged[merged["_merge"] == "left_only"].drop(
                        columns=["_merge"]
                    )

                    if len(df_to_append) == 0:
                        logger.info(
                            f"All {len(table)} rows already exist. Skipping append."
                        )
                        return True

                    logger.info(
                        f"Identified {len(df_to_append)} new rows out of {len(table)} incoming rows. Appending those..."
                    )
                    table = pa.Table.from_pandas(df_to_append, schema=table.schema)

            iceberg_table.append(table)
            logger.info(
                f"Successfully appended {len(table)} rows to {self.full_table_name}"
            )
            return True
        except Exception as e:
            logger.error(f"Error writing to table {self.table_name}: {e}")
            raise

    def get_table(self):
        """Load and return the Iceberg table instance."""
        return self.catalog.load_table(self.full_table_name)

    def read_table(
        self,
        row_filter=None,
        time_col: str = None,
        start_time=None,
        end_time=None,
        variable_col: str = None,
        variables: list = None,
        selected_fields: tuple = None,
    ) -> pa.Table:
        """Read data from the Iceberg table into a PyArrow table, optionally applying a filter."""
        try:
            if time_col and start_time is not None and end_time is not None:
                row_filter = And(
                    GreaterThanOrEqual(time_col, start_time),
                    LessThanOrEqual(time_col, end_time),
                )
            if variable_col and variables:
                var_filter = In(variable_col, variables)
                row_filter = And(row_filter, var_filter) if row_filter else var_filter
            iceberg_table = self.get_table()
            scan_kwargs = {}
            if row_filter:
                scan_kwargs["row_filter"] = row_filter
            if selected_fields:
                scan_kwargs["selected_fields"] = selected_fields
            scan = iceberg_table.scan(**scan_kwargs)
            table_data = scan.to_arrow()
            logger.info(
                f"Successfully read {len(table_data)} rows from {self.table_name}"
            )
            return table_data
        except Exception as e:
            logger.error(f"Error reading from table {self.table_name}: {e}")
            raise
