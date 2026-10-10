"""
Retail dataset processing.

This class handles source-specific operations such as reading the
Kaggle archive, normalizing column names, converting data types,
and removing invalid records.
"""

from zipfile import ZipFile

import pandas as pd


class RetailDataProcessor:
    """Read and clean the raw retail transaction dataset."""

    COLUMN_MAPPING = {
        "CustomerID": "customer_id",
        "ProductID": "product_id",
        "Quantity": "quantity",
        "Price": "price",
        "TransactionDate": "transaction_date",
        "PaymentMethod": "payment_method",
        "StoreLocation": "store_location",
        "ProductCategory": "product_category",
        "DiscountApplied(%)": "discount_applied",
        "TotalAmount": "total_amount",
    }

    REQUIRED_COLUMNS = {
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "transaction_date",
        "store_location",
        "total_amount",
    }

    def read_archive(self, archive_path):
        """
        Read the CSV contained in the downloaded dataset archive.

        The dataset is expected to contain exactly one CSV file.
        """

        with ZipFile(archive_path) as archive:
            csv_files = [
                name
                for name in archive.namelist()
                if name.lower().endswith(".csv")
            ]

            if len(csv_files) != 1:
                raise ValueError(
                    "Expected exactly one CSV file in the "
                    f"dataset archive. Found {len(csv_files)}."
                )

            csv_name = csv_files[0]

            print(
                f"Reading {csv_name} from dataset archive."
            )

            with archive.open(csv_name) as csv_file:
                return pd.read_csv(
                    csv_file,
                    dtype="string",
                    low_memory=False,
                )

    def normalize_columns(self, dataframe):
        """Rename source columns to pipeline column names."""

        missing = (
            set(self.COLUMN_MAPPING)
            - set(dataframe.columns)
        )

        if missing:
            raise ValueError(
                "Missing expected source columns: "
                f"{sorted(missing)}"
            )

        dataframe = dataframe.rename(
            columns=self.COLUMN_MAPPING
        )

        return dataframe[
            list(self.COLUMN_MAPPING.values())
        ]

    def clean(self, dataframe):
        """
        Convert fields to expected types and remove invalid rows.

        Discount values may be null. When present, they must be
        between 0 and 100.
        """

        dataframe = dataframe.copy()

        string_columns = [
            "customer_id",
            "product_id",
            "payment_method",
            "store_location",
            "product_category",
        ]

        for column in string_columns:
            dataframe[column] = (
                dataframe[column]
                .str.strip()
                .replace("", pd.NA)
            )

        numeric_columns = [
            "quantity",
            "price",
            "discount_applied",
            "total_amount",
        ]

        for column in numeric_columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce",
            )

        dataframe["transaction_date"] = pd.to_datetime(
            dataframe["transaction_date"],
            errors="coerce",
        )

        before = len(dataframe)

        dataframe = dataframe.dropna(
            subset=self.REQUIRED_COLUMNS
        )

        missing_removed = (
            before - len(dataframe)
        )

        invalid = (
            (dataframe["quantity"] <= 0)
            | (dataframe["price"] < 0)
            | (dataframe["total_amount"] < 0)
            | (
                dataframe["discount_applied"].notna()
                & (
                    (dataframe["discount_applied"] < 0)
                    | (
                        dataframe["discount_applied"] > 100
                    )
                )
            )
        )

        invalid_removed = int(
            invalid.sum()
        )

        dataframe = dataframe.loc[
            ~invalid
        ].copy()

        print(
            "Cleaning summary: "
            f"removed {missing_removed:,} records with "
            "missing required fields and "
            f"{invalid_removed:,} records with invalid values."
        )

        if dataframe.empty:
            raise ValueError(
                "No valid records remain after cleaning."
            )

        print(
            f"Valid rows after cleaning: "
            f"{len(dataframe):,}"
        )

        return dataframe
        