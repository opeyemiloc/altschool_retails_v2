"""
DAG 2: OLTP Operational Database Seeding (`02_load_csv_to_postgres`)

Orchestrates the ingestion of raw CSV tables from `/opt/airflow/data/raw/` into
the operational PostgreSQL database (`ECOMMERCE` schema) via chunked streaming.
Implements the Pre-Flight Readiness Gate pattern to check database connectivity
and CSV file existence before initiating database transactions.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ------------------------------------------------------------------------------
# 1. Multi-Environment Import Resiliency (Airflow 2.x LTS vs Airflow 3.0+ SDK)
# ------------------------------------------------------------------------------
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator # type: ignore

from airflow.models.dag import DAG
from sqlalchemy import create_engine, text
from retail_platform.config import get_settings
from retail_platform.warehouse.postgres_loader import PostgresLoader, TABLE_MAPPING, LOADING_ORDER

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# 2. Python Callables for DAG Tasks
# ------------------------------------------------------------------------------
def run_verify_postgres_connection(**kwargs):
    """Pre-Flight Check: Verifies SQLAlchemy TCP connection to PostgreSQL and ECOMMERCE schema."""
    settings = get_settings()
    db_url = settings.get_postgres_url() # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Verifying PostgreSQL operational database connection...")
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS ECOMMERCE;"))
        logger.info("[OK] Successfully authenticated with PostgreSQL operational database.")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection verification failed: {e}")
        raise RuntimeError(f"Pre-Flight Check Failed: Cannot connect to PostgreSQL: {e}")

def run_ensure_raw_csv_files_exist(**kwargs):
    """Pre-Flight Gate: Confirms all 9 expected CSV tables exist and are non-empty in raw storage."""
    settings = get_settings()
    raw_dir = settings.RAW_DATA_DIR
    logger.info(f"Verifying raw CSV files in {raw_dir}...")
    
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory missing: {raw_dir}")
        
    missing_files = []
    for csv_file in LOADING_ORDER:
        file_path = raw_dir / csv_file
        if not file_path.exists() or file_path.stat().st_size == 0:
            missing_files.append(csv_file)
            
    if missing_files:
        raise FileNotFoundError(
            f"Pre-Flight Gate Failed: {len(missing_files)} CSV files missing or empty in {raw_dir}: {missing_files}"
        )
        
    logger.info(f"[OK] All {len(LOADING_ORDER)} required CSV tables verified in raw storage.")
    return True

def run_load_csv_to_postgres(csv_file: str, table_name: str, **kwargs):
    """Worker Callable: Streams a single CSV file into PostgreSQL using chunked inserts."""
    logger.info(f"Starting chunked ingestion: {csv_file} -> ECOMMERCE.{table_name.upper()}...")
    loader = PostgresLoader()
    
    rows_inserted = loader.load_table(
        csv_filename=csv_file,
        table_name=table_name,
        chunksize=10000,
        truncate_first=True,
    )
    logger.info(f"[OK] Completed table {table_name.upper()}: {rows_inserted:,} rows inserted.")
    return rows_inserted

# ------------------------------------------------------------------------------
# 3. DAG Definition & Task Graph
# ------------------------------------------------------------------------------
default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="02_load_csv_to_postgres",
    default_args=default_args,
    description="Streams raw CSV tables into PostgreSQL operational database via chunked loading.",
    schedule=None,  # On-demand execution per development standards
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["02_staging", "postgresql", "oltp_layer"],
) as dag:

    verify_db_task = PythonOperator(
        task_id="verify_postgres_connection",
        python_callable=run_verify_postgres_connection,
        doc_md="Verifies TCP connectivity to ingestion-postgres and checks ECOMMERCE schema.",
    )

    verify_files_task = PythonOperator(
        task_id="ensure_raw_csv_files_exist",
        python_callable=run_ensure_raw_csv_files_exist,
        doc_md="Pre-Flight Gate ensuring all 9 raw CSV files are present and non-empty.",
    )

    # Pre-flight readiness gate sequence
    verify_db_task >> verify_files_task

    # Fan out parallel worker tasks for each table in reference hierarchy order
    for csv_file in LOADING_ORDER:
        table_name = TABLE_MAPPING[csv_file]
        task_id = f"load_{table_name}_to_postgres"
        
        load_task = PythonOperator(
            task_id=task_id,
            python_callable=run_load_csv_to_postgres,
            op_kwargs={"csv_file": csv_file, "table_name": table_name},
            doc_md=f"Streams {csv_file} into ECOMMERCE.{table_name.upper()} in 10,000-row chunks.",
        )
        
        # Link pre-flight gate to each worker task
        verify_files_task >> load_task
