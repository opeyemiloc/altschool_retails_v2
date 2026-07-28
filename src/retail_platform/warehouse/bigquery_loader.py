"""
BigQuery loader module for extracting data from PostgreSQL and staging into Google BigQuery.
Implements the Bronze landing layer of the Medallion architecture using google-cloud-bigquery.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from retail_platform.config.settings import get_settings, Settings
from retail_platform.warehouse.postgres_loader import TABLE_MAPPING, LOADING_ORDER

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """
    Manages idempotent extraction from PostgreSQL operational database (OLTP)
    and ingestion into Google BigQuery analytical data warehouse (OLAP Bronze layer).
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initializes the BigQuery loader with connection pooling and GCP client.
        
        Args:
            settings: Settings configuration instance. Defaults to singleton if None.
        """
        self.settings = settings or get_settings()
        self.schema = self.settings.POSTGRES_DB
        self.dataset_id = self.settings.BIGQUERY_DATASET
        self.project_id = self.settings.GCP_PROJECT_ID
        
        self.pg_engine: Engine = self._create_pg_engine()
        self.bq_client: bigquery.Client = self._create_bq_client()

    def _create_pg_engine(self) -> Engine:
        """Creates and returns an SQLAlchemy connection pool for PostgreSQL."""
        conn_dict = self.settings.get_postgres_connection_dict()
        url = (
            f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}"
            f"@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['dbname']}"
        )
        logger.debug(f"Connecting to PostgreSQL at {conn_dict['host']}:{conn_dict['port']}...")
        return create_engine(url, pool_pre_ping=True)

    def _create_bq_client(self) -> bigquery.Client:
        """Creates and returns a Google BigQuery API client using Application Default Credentials."""
        try:
            project = self.project_id if self.project_id else None
            return bigquery.Client(project=project)
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize BigQuery client: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Tests Google Cloud authentication and BigQuery API connectivity via handshake query.
        Used by Airflow pre-flight check tasks (Readiness Gate pattern).
        
        Returns:
            bool: True if connection handshake succeeds, False otherwise.
        """
        print(f"[CONNECTING] Initiating GCP BigQuery API handshake (Project: '{self.bq_client.project}', Dataset: '{self.dataset_id}')...")
        try:
            query_job = self.bq_client.query("SELECT 1 AS status;")
            result = list(query_job.result())
            if result and result[0].status == 1:
                print("[CONNECTED] [OK] BigQuery authentication and API handshake successful!")
                logger.info("[OK] Google BigQuery API connection verified.")
                return True
        except Exception as e:
            print(f"[ERROR] BigQuery connection handshake failed: {e}")
            logger.error(f"[ERROR] BigQuery connection failed: {e}")
        return False

    def create_dataset_if_not_exists(self) -> bool:
        """
        Verifies or provisions the target BigQuery analytical dataset in Google Cloud.
        Used by Airflow pre-flight check tasks before table fan-out.
        
        Returns:
            bool: True if dataset exists or was created successfully.
        """
        full_dataset_id = f"{self.bq_client.project}.{self.dataset_id}"
        print(f"[PRE-FLIGHT] Verifying target BigQuery dataset: {full_dataset_id}...")
        try:
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = "US"  # Standard multi-region location for analytical data
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            print(f"[OK] Verified BigQuery dataset ready: {dataset.full_dataset_id}")
            logger.info(f"[OK] BigQuery dataset verified: {dataset.full_dataset_id}")
            return True
        except GoogleAPIError as e:
            print(f"[ERROR] Failed to verify or create BigQuery dataset: {e}")
            logger.error(f"[ERROR] BigQuery dataset error: {e}")
            return False

    def load_table_to_bq(self, csv_filename: str, table_name: str) -> int:
        """
        Extracts a single table from PostgreSQL and loads it into BigQuery
        using WRITE_TRUNCATE for idempotency (Learn.md Module 8).
        
        Args:
            csv_filename: Name of the corresponding raw CSV (used for mapping).
            table_name: Target table name.
            
        Returns:
            int: Number of rows loaded into BigQuery.
        """
        logger.info(f"Extracting {self.schema}.{table_name.upper()} from PostgreSQL...")
        
        # 1. Extract from PostgreSQL
        query = f"SELECT * FROM {self.schema}.{table_name.upper()};"
        with self.pg_engine.connect() as conn:
            df = pd.read_sql_query(query, con=conn)
            
        total_rows = len(df)
        if total_rows == 0:
            logger.warning(f"Table {self.schema}.{table_name.upper()} is empty in PostgreSQL. Proceeding with empty load to maintain BigQuery idempotency.")


        # 2. Transform/Clean timestamps and object types for BigQuery schema compatibility
        for col in df.columns:
            if "date" in col.lower() or "timestamp" in col.lower() or col.lower().endswith("_at"):
                if df[col].dtype == "object":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            elif df[col].dtype == "object":
                # Convert UUIDs and arbitrary Python objects in object columns to strings for PyArrow/BigQuery
                df[col] = df[col].map(lambda x: str(x) if pd.notnull(x) else None)
                    
        # 3. Load into BigQuery (Bronze layer landing with WRITE_TRUNCATE)
        table_id = f"{self.bq_client.project}.{self.dataset_id}.{table_name.lower()}"
        logger.info(f"Loading {total_rows:,} rows into BigQuery table: {table_id}...")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        try:
            job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for asynchronous BigQuery ingestion job to complete
            
            print(f"  [OK] Table {table_name.upper()} finished: {total_rows:,} rows ingested into BigQuery.")
            logger.info(f"[OK] Successfully loaded {total_rows:,} rows into {table_id}.")
            return total_rows
        except Exception as e:
            print(f"  [ERROR] Failed loading {table_name.upper()} into BigQuery: {e}")
            logger.error(f"[ERROR] BigQuery ingestion job failed for {table_id}: {e}")
            raise

    def load_all_to_bq(self) -> Dict[str, int]:
        """
        Orchestrates full extraction from PostgreSQL to BigQuery across all tables.
        
        Returns:
            Dict[str, int]: Map of table name to rows inserted in BigQuery.
        """
        logger.info(f"Starting full warehouse ingestion to BigQuery (Dataset: {self.dataset_id})...")
        
        print("\n[PRE-FLIGHT] Verifying BigQuery connectivity and dataset before pipeline fan-out...")
        if not self.test_connection():
            raise ConnectionError("BigQuery authentication/connection check failed.")
        if not self.create_dataset_if_not_exists():
            raise RuntimeError("BigQuery target dataset verification failed.")
            
        results = {}
        total_tables = len(LOADING_ORDER)
        
        for idx, csv_file in enumerate(LOADING_ORDER, start=1):
            if csv_file not in TABLE_MAPPING:
                continue
            table_name = TABLE_MAPPING[csv_file]
            print(f"\n[PROGRESS {idx}/{total_tables}] >>> Extracting & loading: {table_name.upper()} -> BigQuery '{self.dataset_id}.{table_name.lower()}'")
            rows = self.load_table_to_bq(csv_filename=csv_file, table_name=table_name)
            results[table_name] = rows
            
        logger.info("[OK] Full BigQuery Bronze layer ingestion complete.")
        return results


def load_postgres_to_bigquery() -> Dict[str, int]:
    """Helper function to execute full ingestion from PostgreSQL to BigQuery."""
    loader = BigQueryLoader()
    return loader.load_all_to_bq()
