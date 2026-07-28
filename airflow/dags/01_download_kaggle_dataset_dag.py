"""
DAG 1: Kaggle Raw Dataset Acquisition (`01_download_kaggle_dataset`)

Orchestrates the extraction of the raw Brazilian E-Commerce dataset from Kaggle
into the local/container volume data directory (`/opt/airflow/data/raw/`).
Implements the Pre-Flight Readiness Gate pattern to verify credentials before network calls.
"""
import os
import logging
from datetime import datetime, timedelta

# ------------------------------------------------------------------------------
# 1. Multi-Environment Import Resiliency (Airflow 2.x LTS vs Airflow 3.0+ SDK)
# ------------------------------------------------------------------------------
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]

from airflow.models.dag import DAG
from retail_platform.config import get_settings
from retail_platform.ingestion.downloader import DatasetDownloader

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# 2. Python Callables for DAG Tasks
# ------------------------------------------------------------------------------
def run_verify_kaggle_credentials(**kwargs):
    """Pre-Flight Check: Ensures Kaggle API credentials exist in environment variables."""
    settings = get_settings()
    username = settings.KAGGLE_USERNAME or os.getenv("KAGGLE_USERNAME")
    key = settings.KAGGLE_KEY or os.getenv("KAGGLE_KEY")
    
    if not username or not key:
        logger.error("Missing KAGGLE_USERNAME or KAGGLE_KEY in environment configuration.")
        raise ValueError("Pre-Flight Check Failed: Missing Kaggle API Credentials.")
        
    logger.info(f"[OK] Verified Kaggle credentials for user: {username}")
    return True

def run_download_and_sync_dataset(**kwargs):
    """
    Executes dataset download from Kaggle with force_download=True to guarantee
    overwriting any existing files or non-empty directory artifacts (.gitkeep).
    """
    logger.info("Initiating dataset download with force_download=True...")
    downloader = DatasetDownloader()
    
    # Enforce force_download=True per architectural specification
    result_files = downloader.run(force_download=True)
    
    if not result_files:
        raise RuntimeError("Dataset download completed, but no CSV files were mapped.")
        
    logger.info(f"[OK] Successfully ingested {len(result_files)} CSV tables into raw storage.")
    return len(result_files)

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
    dag_id="01_download_kaggle_dataset",
    default_args=default_args,
    description="Acquires raw retail dataset CSVs from Kaggle into local volume storage.",
    schedule=None,  # On-demand execution per development standards
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["01_ingestion", "kaggle", "raw_layer"],
) as dag:

    verify_credentials_task = PythonOperator(
        task_id="verify_kaggle_credentials",
        python_callable=run_verify_kaggle_credentials,
        doc_md="Verifies KAGGLE_USERNAME and KAGGLE_KEY before network execution.",
    )

    download_dataset_task = PythonOperator(
        task_id="download_and_sync_kaggle_dataset",
        python_callable=run_download_and_sync_dataset,
        doc_md="Downloads Olist CSVs with force_download=True to override existing files.",
    )

    # Architectural Graph Dependency
    verify_credentials_task >> download_dataset_task
