# 🏗️ Warehouse Layer (`src/retail_platform/warehouse`)

The **Warehouse Layer** is the core database engine and pipeline driver of the platform. It bridges our operational transactional database (PostgreSQL OLTP) and our cloud analytical data warehouse (Google BigQuery OLAP), forming the foundation of our **Medallion Architecture Bronze Layer**.

---

## 🏛️ Architecture & Role in Medallion Pipeline

In our Medallion Architecture, this module implements the **Bronze Layer Ingestion Boundary**:
1. **OLTP Ingestion (`postgres_loader.py`)**: Streams raw CSV files from staging (`data/raw/`) into an operational PostgreSQL database. This simulates a live production transactional database where e-commerce orders, payments, and customers are recorded.
2. **OLAP Extraction & Loading (`bigquery_loader.py`)**: Extracts tables from PostgreSQL and loads them into Google BigQuery. This establishes our **Bronze Layer**, providing an immutable, raw historical record in the cloud ready for downstream Silver/Gold dbt transformations.

---

## 🛠️ Key Modules & Architecture Design

### 1. `PostgresLoader` ([postgres_loader.py](file:///C:/Users/User/Desktop/altschool_retails_v2/src/retail_platform/warehouse/postgres_loader.py))
* **Chunked Streaming**: Ingests massive CSV tables in configurable memory chunks (default: `10,000` rows) using `pandas.read_csv(..., chunksize=...)`. This eliminates Out-Of-Memory (OOM) crashes when processing large e-commerce event logs.
* **Schema Normalization**: Automatically strips trailing whitespace from column headers and fixes source data typos (e.g., converting Kaggle's `product_name_lenght` to `product_name_length`).
* **ACID Transaction Atomicity**: Wraps multi-chunk ingestion loops inside explicit SQLAlchemy transaction blocks (`with engine.begin() as tx_conn:`). If a streaming failure occurs on chunk 50 out of 100, the database issues an automatic `ROLLBACK`, guaranteeing zero partial data corruption.

### 2. `BigQueryLoader` ([bigquery_loader.py](file:///C:/Users/User/Desktop/altschool_retails_v2/src/retail_platform/warehouse/bigquery_loader.py))
* **GCP Handshake & Authentication**: Uses Google Application Default Credentials (ADC) to authenticate and handshake with BigQuery APIs before pipeline execution.
* **Automated Dataset Provisioning**: Checks for the existence of the target analytical dataset (`retail_analytics_warehouse`) and provisions it automatically if missing (`create_dataset_if_not_exists`).
* **Idempotent Loading**: Configures BigQuery load jobs with `write_disposition = WRITE_TRUNCATE`. Re-running the extraction pipeline cleanly replaces staging tables without creating duplicate records.

---

## 🔐 Environment Configuration

Ensure the following variables are configured in `.env` for database and cloud connectivity:

```env
# PostgreSQL Operational Database
POSTGRES_USER=retail_admin
POSTGRES_PASSWORD=mock_password
POSTGRES_DB=ECOMMERCE
POSTGRES_HOST=localhost # (Use 'ingestion-postgres' inside Docker Compose)
POSTGRES_PORT=5434

# Google Cloud Platform & BigQuery
GCP_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=retail_analytics_warehouse
GOOGLE_APPLICATION_CREDENTIALS="C:/path/to/your/gcp_service_account_key.json"
```

---

## ✍️ Engineering Lessons & Case Studies (Medium / Tech Blog Hooks)

### 1. Building Resilient Data Loaders: Defense-in-Depth with ACID Rollbacks
* **The Challenge**: When loading 100,000+ rows in batch chunks, network blips or bad records mid-stream can leave database tables in a corrupted, half-loaded state.
* **The Solution**: Implementing **Defense-in-Depth** by pairing pre-load table truncation (`TRUNCATE TABLE ... CASCADE;`) with ACID transaction blocks (`engine.begin()`). If any chunk fails, an automatic rollback restores the database to its pristine pre-ingestion state.

### 2. Scaling Idempotency: When to Move Beyond `WRITE_TRUNCATE` in BigQuery
* **The Challenge**: While `WRITE_TRUNCATE` is fast and reliable for staging datasets (<100 MB), overwriting terabytes of data daily incurs massive cloud compute/egress billing and destroys columnar time-travel history.
* **The Enterprise Scaling Path**: For multi-million row event tables, enterprise pipelines transition to **Incremental Ingestion via SQL `MERGE`** (using high-water mark timestamps) or **Time-Partitioned Overwrites** (targeting specific daily partition decorators like `table$20260727`).

### 3. Resolving SQLAlchemy Canonical Imports Across Library Versions
* **The Challenge**: Importing typehints like `Engine` directly from the top-level namespace (`from sqlalchemy import Engine`) causes fatal `ImportError` exceptions in older or containerized SQLAlchemy environments (such as Airflow's core container).
* **The Best Practice**: Always import core engine classes from their canonical submodule: `from sqlalchemy.engine import Engine`. This guarantees universal compatibility across local virtual environments and Linux Docker containers.
