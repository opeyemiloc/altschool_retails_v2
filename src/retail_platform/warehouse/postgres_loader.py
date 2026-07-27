"""
PostgresLoader module for ingesting raw Kaggle CSV tables into the operational PostgreSQL database.
Implements chunked streaming ingestion to prevent Out-Of-Memory (OOM) errors.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from retail_platform.config import get_settings

logger = logging.getLogger(__name__)

# Map of raw CSV filenames to target table names in the ECOMMERCE schema
TABLE_MAPPING: Dict[str, str] = {
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

# Mapping of raw Kaggle column typos/abbreviations to target DDL schema column names
COLUMN_RENAMES: Dict[str, str] = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
    "geolocation_lat": "geolocation_latitude",
    "geolocation_lng": "geolocation_longitude",
}

# Loading order prioritizing reference/parent tables before transactional tables
LOADING_ORDER: List[str] = [
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


class PostgresLoader:
    """Manages idempotent chunked data loading from data/raw into PostgreSQL."""

    def __init__(self, settings=None, schema: str = "ECOMMERCE"):
        self.settings = settings or get_settings()
        self.schema = schema.upper()
        self.engine: Engine = self._create_engine()

    def _create_engine(self) -> Engine:
        """Creates and returns an SQLAlchemy Engine for PostgreSQL."""
        conn_dict = self.settings.get_postgres_connection_dict()
        url = (
            f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}"
            f"@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['dbname']}"
        )
        logger.debug(f"Connecting to PostgreSQL at {conn_dict['host']}:{conn_dict['port']}...")
        return create_engine(url, pool_pre_ping=True)

    def test_connection(self) -> bool:
        """
        Tests operational database connectivity.
        
        Returns:
            bool: True if connection succeeds, False otherwise.
        """
        conn_dict = self.settings.get_postgres_connection_dict()
        print(f"[CONNECTING] Initiating socket connection to PostgreSQL at {conn_dict['host']}:{conn_dict['port']} (Database: '{conn_dict['dbname']}')...")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1;")).scalar()
                if result == 1:
                    print("[CONNECTED] [OK] Operational database connection handshake successful!")
                    logger.info("[OK] PostgreSQL operational database connection verified.")
                    return True
        except Exception as e:
            print(f"[ERROR] Connection handshake failed: {e}")
            logger.error(f"[ERROR] PostgreSQL connection failed: {e}")
        return False

    def truncate_table(self, table_name: str) -> None:
        """
        Cleanly truncates a table with CASCADE to ensure idempotent loading.
        
        Args:
            table_name: Target table name in PostgreSQL.
        """
        full_table_name = f"{self.schema}.{table_name.upper()}"
        logger.info(f"Truncating table {full_table_name} (CASCADE)...")
        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_table_name} CASCADE;"))

    def _clean_chunk(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Cleans data types and nulls in a DataFrame chunk before SQL insertion.
        """
        # Convert timestamp strings to datetime objects
        for col in df.columns:
            if "date" in col.lower() or "timestamp" in col.lower() or col.lower().endswith("_at"):
                if df[col].dtype == "object":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
        # Rename mismatched columns (Kaggle typos vs init.sql schema)
        df = df.rename(columns=COLUMN_RENAMES)
        
        # In pandas, replace NaN/inf with None so psycopg2 inserts SQL NULL cleanly
        df = df.replace({np.nan: None})
        return df

    def load_table(
        self,
        csv_filename: str,
        table_name: str,
        chunksize: int = 10000,
        truncate_first: bool = True,
    ) -> int:
        """
        Loads a single CSV table into PostgreSQL using chunked streaming.
        
        Args:
            csv_filename: Name of the CSV file in RAW_DATA_DIR.
            table_name: Target database table name.
            chunksize: Number of rows per streaming chunk.
            truncate_first: Whether to truncate the table before loading.
            
        Returns:
            int: Total rows inserted into the table.
        """
        file_path = self.settings.RAW_DATA_DIR / csv_filename
        if not file_path.exists():
            logger.error(f"Raw CSV file not found: {file_path}")
            raise FileNotFoundError(f"Missing raw file: {file_path}")

        if truncate_first:
            self.truncate_table(table_name)

        logger.info(f"Loading {csv_filename} -> {self.schema}.{table_name.upper()} in chunks of {chunksize}...")
        
        total_rows = 0
        chunk_count = 0
        
        # Open an explicit atomic transaction block (Learn.md Scenario 7)
        # Guarantees all-or-nothing rollback if interrupted mid-stream!
        with self.engine.begin() as tx_conn:
            # Read CSV in chunks to keep memory usage bounded (Learn.md Scenario 4)
            for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
                chunk = self._clean_chunk(chunk, table_name)
                rows_in_chunk = len(chunk)
                
                # Write chunk to PostgreSQL within the single atomic transaction
                chunk.to_sql(
                    name=table_name.lower(),
                    con=tx_conn,
                    schema=self.schema.lower(),
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                total_rows += rows_in_chunk
                chunk_count += 1
                print(f"  ... Chunk {chunk_count}: streamed {rows_in_chunk:,} rows (Total so far: {total_rows:,})")
                logger.debug(f"  Chunk {chunk_count}: loaded {rows_in_chunk} rows (Total: {total_rows})")

        print(f"  [OK] Table {self.schema}.{table_name.upper()} finished: {total_rows:,} total rows inserted.")
        logger.info(f"[OK] Successfully loaded {total_rows:,} rows into {self.schema}.{table_name.upper()}.")
        return total_rows

    def load_all(self, chunksize: int = 10000, truncate_first: bool = True) -> Dict[str, int]:
        """
        Orchestrates loading across all expected tables in dependency order.
        
        Args:
            chunksize: Number of rows per chunk.
            truncate_first: Whether to truncate tables before ingestion.
            
        Returns:
            Dict[str, int]: Map of table name to rows inserted.
        """
        logger.info(f"Starting full PostgreSQL warehouse loading (Schema: {self.schema})...")
        print("\n[PRE-LOAD CHECK] Verifying operational database connectivity before starting warehouse loading...")
        if not self.test_connection():
            raise ConnectionError("PostgreSQL connection check failed before warehouse loading.")
        results = {}

        # If truncating first, truncate in reverse order (child tables first) or rely on CASCADE
        if truncate_first:
            logger.info("Performing pre-ingestion cleanup (truncating all tables)...")
            for csv_file in reversed(LOADING_ORDER):
                if csv_file in TABLE_MAPPING:
                    self.truncate_table(TABLE_MAPPING[csv_file])

        # Execute chunked loads in dependency order
        total_tables = len(LOADING_ORDER)
        for idx, csv_file in enumerate(LOADING_ORDER, start=1):
            if csv_file not in TABLE_MAPPING:
                continue
            table_name = TABLE_MAPPING[csv_file]
            print(f"\n[PROGRESS {idx}/{total_tables}] >>> Loading table: {csv_file} -> {self.schema}.{table_name.upper()}")
            # Since we already truncated above if requested, pass truncate_first=False here
            rows = self.load_table(
                csv_filename=csv_file,
                table_name=table_name,
                chunksize=chunksize,
                truncate_first=False,
            )
            results[table_name] = rows

        logger.info("[OK] Full PostgreSQL warehouse ingestion complete.")
        return results

    def print_table_row_counts(self) -> None:
        """
        Queries and prints a formatted status check of row counts across all tables.
        """
        print("\n" + "=" * 65)
        print(f"POSTGRESQL WAREHOUSE STATUS (Schema: {self.schema})")
        print("=" * 65)

        total_all_rows = 0
        with self.engine.connect() as conn:
            for csv_file in LOADING_ORDER:
                if csv_file not in TABLE_MAPPING:
                    continue
                table_name = TABLE_MAPPING[csv_file]
                full_table_name = f"{self.schema}.{table_name.upper()}"
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name};")).scalar()
                    print(f"[TABLE] {full_table_name:<45} : {count:>9,} rows")
                    total_all_rows += count
                except Exception as e:
                    print(f"[ERROR] {full_table_name:<45} : {str(e)[:25]}")

        print("-" * 65)
        print(f"Total rows stored across all operational tables: {total_all_rows:,}")
        print("=" * 65 + "\n")


def load_raw_data_to_postgres(chunksize: int = 10000, truncate_first: bool = True) -> Dict[str, int]:
    """Helper function to run the PostgresLoader pipeline."""
    loader = PostgresLoader()
    return loader.load_all(chunksize=chunksize, truncate_first=truncate_first)
