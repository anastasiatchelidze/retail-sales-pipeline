from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from retail_pipeline.config import PipelineConfig
from retail_pipeline.database import DatabaseClient
from retail_pipeline.kaggle import KaggleDatasetClient
from retail_pipeline.processor import RetailDataProcessor


class RetailSalesPipeline:
    DATASET_OWNER = "fahadrehman07"
    DATASET_NAME = "retail-transaction-dataset"

    def __init__(self, config=None):
        self.config = (
            config or PipelineConfig.from_environment()
        )

        self.database = DatabaseClient(
            self.config
        )

        self.kaggle = KaggleDatasetClient(
            self.config.kaggle_api_token,
            self.config.data_dir,
        )

        self.processor = RetailDataProcessor()

    def initialize_database(self):
        self.database.execute_script(
            "01_schema.sql"
        )

    def download_data(self):
        self.kaggle.download(
            self.DATASET_OWNER,
            self.DATASET_NAME,
        )

    def load_raw_data(self):
        archive_path = self._get_archive()

        dataframe = self.processor.read_archive(
            archive_path
        )

        dataframe = self.processor.normalize_columns(
            dataframe
        )

        dataframe = self.processor.clean(
            dataframe
        )

        rows = [
            tuple(
                None
                if pd.isna(value)
                else value.item()
                if hasattr(value, "item")
                else value
                for value in row
            )
            for row in dataframe.itertuples(
                index=False,
                name=None,
            )
        ]

        insert_sql = """
            INSERT INTO staging.sales_raw (
                customer_id,
                product_id,
                quantity,
                price,
                transaction_date,
                payment_method,
                store_location,
                product_category,
                discount_applied,
                total_amount
            )
            VALUES %s
        """

        connection = self.database.connect()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "TRUNCATE TABLE staging.sales_raw;"
                )

                execute_values(
                    cursor,
                    insert_sql,
                    rows,
                    page_size=5000,
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        print(
            "Loaded "
            f"{len(rows):,} rows into staging.sales_raw."
        )

    def transform_data(self):
        self.database.execute_script(
            "02_transformations.sql"
        )

    def validate(self):
        connection = self.database.connect()

        try:
            with connection.cursor() as cursor:
                self._validate(cursor)

        finally:
            connection.close()

    def _validate(self, cursor):
        tables = [
            "staging.sales_raw",
            "warehouse.dim_store",
            "warehouse.dim_product",
            "warehouse.fact_sales",
            "warehouse.dim_lfl_store",
            "analytics.daily_store_product_sales",
        ]

        counts = {}

        for table in tables:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table};"
            )
            counts[table] = cursor.fetchone()[0]

        print("Pipeline row counts:")

        for table, count in counts.items():
            print(
                f"  {table}: {count:,}"
            )

        self._validate_row_counts(counts)
        self._validate_abc(cursor)
        self._validate_referential_integrity(cursor)
        self._validate_store_mapping(cursor)
        self._validate_fact_values(cursor)
        self._validate_daily_aggregation(cursor)

        print(
            "Validation completed successfully."
        )

    def _validate_row_counts(self, counts):
        required_tables = [
            "staging.sales_raw",
            "warehouse.dim_store",
            "warehouse.dim_product",
            "warehouse.fact_sales",
            "warehouse.dim_lfl_store",
            "analytics.daily_store_product_sales",
        ]

        for table in required_tables:
            if counts[table] == 0:
                raise ValueError(
                    f"{table} is empty."
                )

        if (
            counts["staging.sales_raw"]
            != counts["warehouse.fact_sales"]
        ):
            raise ValueError(
                "Staging and fact row counts do not match."
            )

    def _validate_abc(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.dim_product
            WHERE abc_quantity IS NULL
               OR abc_amount IS NULL
               OR abc_quantity NOT IN ('A', 'B', 'C')
               OR abc_amount NOT IN ('A', 'B', 'C');
            """
        )

        invalid_abc = cursor.fetchone()[0]

        if invalid_abc:
            raise ValueError(
                "Product dimension contains "
                f"{invalid_abc:,} invalid ABC classifications."
            )

    def _validate_referential_integrity(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.fact_sales fact
            LEFT JOIN warehouse.dim_store store
                ON store.store_id = fact.store_id
            WHERE store.store_id IS NULL;
            """
        )

        if cursor.fetchone()[0]:
            raise ValueError(
                "Fact table contains unknown stores."
            )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.fact_sales fact
            LEFT JOIN warehouse.dim_product product
                ON product.product_id = fact.product_id
            WHERE product.product_id IS NULL;
            """
        )

        if cursor.fetchone()[0]:
            raise ValueError(
                "Fact table contains unknown products."
            )

    def _validate_store_mapping(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM staging.sales_raw raw
            LEFT JOIN warehouse.dim_store store
                ON store.source_location =
                   TRIM(raw.store_location)
            WHERE store.store_id IS NULL;
            """
        )

        unmapped_rows = cursor.fetchone()[0]

        if unmapped_rows:
            raise ValueError(
                "Some staging records could not be "
                f"mapped to a store: {unmapped_rows:,} rows."
            )

    def _validate_fact_values(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.fact_sales
            WHERE quantity <= 0
               OR price < 0
               OR total_amount < 0
               OR (
                   discount_applied IS NOT NULL
                   AND (
                       discount_applied < 0
                       OR discount_applied > 100
                   )
               );
            """
        )

        invalid_rows = cursor.fetchone()[0]

        if invalid_rows:
            raise ValueError(
                "Fact table contains "
                f"{invalid_rows:,} invalid value rows."
            )

    def _validate_daily_aggregation(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM analytics.daily_store_product_sales
            WHERE total_quantity <= 0
               OR total_revenue < 0
               OR total_transaction_count <= 0
               OR unique_customers < 0
               OR (
                   weighted_discount_pct IS NOT NULL
                   AND (
                       weighted_discount_pct < 0
                       OR weighted_discount_pct > 100
                   )
               );
            """
        )

        invalid_rows = cursor.fetchone()[0]

        if invalid_rows:
            raise ValueError(
                "Daily sales aggregation contains "
                f"{invalid_rows:,} invalid rows."
            )

    def _get_archive(self):
        archives = sorted(
            Path(
                self.config.data_dir
            ).glob("*.zip")
        )

        if not archives:
            raise FileNotFoundError(
                "No dataset archive found in "
                f"{self.config.data_dir}."
            )

        if len(archives) > 1:
            raise RuntimeError(
                "Expected one dataset archive, found: "
                + ", ".join(
                    str(path)
                    for path in archives
                )
            )

        return archives[0]
        