from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from retail_pipeline.pipeline import RetailSalesPipeline


def initialize_database():
    RetailSalesPipeline().initialize_database()


def download_data():
    RetailSalesPipeline().download_data()


def load_raw_data():
    RetailSalesPipeline().load_raw_data()


def transform_data():
    RetailSalesPipeline().transform_data()


def validate_pipeline():
    RetailSalesPipeline().validate()


with DAG(
    dag_id="retail_sales_pipeline",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Tbilisi",
    ),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "retail",
        "data-engineering",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    wait_for_postgres = SqlSensor(
        task_id="wait_for_postgres",
        conn_id="retail_postgres",
        sql="SELECT 1;",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    init_database = PythonOperator(
        task_id="init_database",
        python_callable=initialize_database,
        execution_timeout=timedelta(minutes=5),
    )

    download_data_task = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        execution_timeout=timedelta(minutes=10),
    )

    load_raw_data_task = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
        execution_timeout=timedelta(minutes=10),
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=10),
    )

    validate_pipeline_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
        execution_timeout=timedelta(minutes=5),
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
        start
        >> wait_for_postgres
        >> init_database
        >> download_data_task
        >> load_raw_data_task
        >> transform_data_task
        >> validate_pipeline_task
        >> end
    )
    