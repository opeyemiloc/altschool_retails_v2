"""
Airflow DAG for orchestrating extraction from PostgreSQL (OLTP) to BigQuery (OLAP Bronze Layer).
Implements the Pre-Flight Check / Readiness Gate pattern for autonomous validation and testing.
"""

from datetime import datetime, timedelta
from airflow import DAG
try:
    # Airflow 3.0+ (For local IDE linting)
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Airflow 2.x (For Docker execution)
    from airflow.operators.python import PythonOperator # type: ignore
from retail_platform.warehouse.bigquery_loader import BigQueryLoader

# Loading order and table mapping definitions
TABLE_MAPPING = {
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
}

LOADING_ORDER = [
    "product_category_name_translation.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]


def run_test_connection() -> None:
    """Pre-flight check task 1: Verifies GCP ADC credentials and BigQuery API connectivity."""
    loader = BigQueryLoader()
    if not loader.test_connection():
        raise ConnectionError("Pre-flight check failed: BigQuery authentication/API handshake error.")


def run_ensure_dataset() -> None:
    """Pre-flight check task 2: Verifies or provisions the target BigQuery dataset."""
    loader = BigQueryLoader()
    if not loader.create_dataset_if_not_exists():
        raise RuntimeError("Pre-flight check failed: Could not verify or create target BigQuery dataset.")


def run_load_table(csv_file: str, table_name: str) -> None:
    """Task callable: Extracts a specific table from PostgreSQL and loads into BigQuery."""
    loader = BigQueryLoader()
    loader.load_table_to_bq(csv_filename=csv_file, table_name=table_name)


def run_pipeline_complete() -> None:
    """Final summary task logging successful ingestion across all tables."""
    print("[OK] BigQuery Bronze layer warehouse ingestion completed successfully!")


default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_bigquery_warehouse",
    default_args=default_args,
    description="Extracts OLTP data from PostgreSQL and loads into BigQuery (Bronze layer)",
    schedule=None,  # Set to None for on-demand / click-triggered execution in Airflow UI
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["warehouse", "bigquery", "bronze", "medallion", "pre-flight-check"],
) as dag:

    # 1. Pre-flight Check: Verify Google Cloud ADC Authentication & BigQuery API handshake
    verify_connection_task = PythonOperator(
        task_id="verify_gcp_and_bq_connection",
        python_callable=run_test_connection,
    )

    # 2. Pre-flight Check: Verify or provision target BigQuery dataset
    ensure_dataset_task = PythonOperator(
        task_id="ensure_bq_dataset_exists",
        python_callable=run_ensure_dataset,
    )

    # 3. Final convergence task
    pipeline_complete_task = PythonOperator(
        task_id="pipeline_complete",
        python_callable=run_pipeline_complete,
    )

    # Chain pre-flight checks: connection must succeed before dataset verification
    verify_connection_task >> ensure_dataset_task

    # 4. Fan-out into parallel table extraction and loading tasks
    for csv_filename in LOADING_ORDER:
        if csv_filename not in TABLE_MAPPING:
            continue
        tbl_name = TABLE_MAPPING[csv_filename]

        load_table_task = PythonOperator(
            task_id=f"extract_load_{tbl_name.lower()}",
            python_callable=run_load_table,
            op_kwargs={"csv_file": csv_filename, "table_name": tbl_name},
        )

        # Wire fan-out: each table load depends on dataset check, and converges to complete
        ensure_dataset_task >> load_table_task >> pipeline_complete_task
