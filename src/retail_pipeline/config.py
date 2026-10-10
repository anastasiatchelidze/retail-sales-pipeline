"""
Application configuration loaded from environment variables.

Keeping configuration in one place makes the pipeline easier to run
locally, in Docker, and in another deployment environment.
"""

import os
from pathlib import Path


class PipelineConfig:
    """Store runtime configuration required by the pipeline."""

    def __init__(
        self,
        postgres_host,
        postgres_port,
        postgres_database,
        postgres_user,
        postgres_password,
        kaggle_api_token,
        data_dir,
        sql_dir,
    ):
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_database = postgres_database
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.kaggle_api_token = kaggle_api_token
        self.data_dir = data_dir
        self.sql_dir = sql_dir

    @classmethod
    def from_environment(cls):
        """Build pipeline configuration from environment variables."""

        kaggle_token = os.getenv("KAGGLE_API_TOKEN")

        if not kaggle_token:
            raise ValueError("KAGGLE_API_TOKEN is not set.")

        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(
                os.getenv("POSTGRES_PORT", "5432")
            ),
            postgres_database=os.getenv(
                "POSTGRES_DB",
                "retail_db",
            ),
            postgres_user=os.getenv(
                "POSTGRES_USER",
                "retail_user",
            ),
            postgres_password=os.getenv(
                "POSTGRES_PASSWORD",
                "retail_password",
            ),
            kaggle_api_token=kaggle_token,
            data_dir=Path(
                os.getenv(
                    "DATA_DIR",
                    "/opt/airflow/data",
                )
            ),
            sql_dir=Path(
                os.getenv(
                    "SQL_DIR",
                    "/opt/airflow/sql",
                )
            ),
        )
        