"""
Command-line entry point for running the retail sales pipeline
without Airflow.
"""

from retail_pipeline.pipeline import RetailSalesPipeline


def main():
    """Run the complete pipeline from ingestion through validation."""

    pipeline = RetailSalesPipeline()

    pipeline.initialize_database()
    pipeline.download_data()
    pipeline.load_raw_data()
    pipeline.transform_data()
    pipeline.validate()


if __name__ == "__main__":
    main()
    