# Retail Analytics Platform

> A production-style analytics engineering platform demonstrating modern data engineering practices using Python, Airflow, PostgreSQL, BigQuery, dbt, Docker, and Power BI.

---

## Overview

This project simulates an end-to-end retail analytics platform rather than a simple ETL pipeline. The goal is to demonstrate how raw operational data moves through ingestion, validation, warehousing, transformation, and reporting layers in an enterprise analytics environment.

---

## High-Level Architecture

```text
Kaggle API
    │
    ▼
Airflow DAG (Docker)
    │
    ▼
Python Package (src/retail_platform)
    │
    ▼
Schema & Data Validation
    │
    ▼
PostgreSQL (Operational Landing DB)
    │
    ▼
BigQuery (Data Warehouse)
    │
    ▼
dbt Core (Analytical Transformations)
    │
    ▼
Power BI (Reporting & Dashboards)
```

---

## Technology Stack

| Layer                     | Technology                   | Description                                                   |
| ------------------------- | ---------------------------- | ------------------------------------------------------------- |
| **Orchestration**   | Apache Airflow               | Workflows, DAG scheduling, and task dependencies              |
| **Infrastructure**  | Docker Compose               | Containerized Airflow, PostgreSQL, and Redis                  |
| **Business Logic**  | Python (`retail_platform`) | Package handling ingestion, validation, and warehouse loading |
| **Operational DB**  | PostgreSQL                   | Landing/staging store for raw validated operational data      |
| **Data Warehouse**  | Google BigQuery              | Scalable cloud data warehouse                                 |
| **Transformations** | dbt Core (`dbt-bigquery`)  | Staging, intermediate, and dimensional modeling (marts)       |
| **Analytics & BI**  | Power BI                     | Executive & operational dashboards                            |

---

## Repository Structure

```text
altschool_retails_v2/
├── airflow/            # Airflow DAGs and configuration
├── src/                # Modular Python package (retail_platform)
│   └── retail_platform/
│       ├── config/     # Configuration management
│       ├── ingestion/  # Kaggle API & data extraction
│       ├── validation/ # Data validation rules
│       ├── warehouse/  # Postgres & BigQuery loaders
│       └── transform/  # Python transformations
├── dbt/                # dbt models (staging, intermediate, marts)
├── docker/             # Dockerfiles and container scripts
├── tests/              # Unit & integration test suite
├── data/               # Local data directory (raw/processed)
├── docker-compose.yml  # Infrastructure orchestration
├── pyproject.toml      # Python project build & dependency specs
└── README.md           # Project documentation
```

---

## Quick Start

### 1. Clone & Set Up Python Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

# Install editable package and dependencies
pip install -e .
```

### 2. Environment Configuration

Copy the example environment file and update your credentials:

```bash
cp .env.example .env
```

### 3. Start Infrastructure

Launch PostgreSQL, Airflow, and supporting services using Docker Compose:

```bash
docker compose up -d
```

---

## 📚 Modular Architecture & Subsystem Guides

To keep documentation focused, maintainable, and structured for engineering publications, detailed technical specifications, architecture diagrams, and troubleshooting guides are maintained in specialized submodule READMEs:

| Subsystem / Layer | Path | Description |
| :--- | :--- | :--- |
| **Orchestration & Docker** | [`airflow/README.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/airflow/README.md) | DAG scheduling, Pre-Flight check pattern, container volume syncing, and Windows/Linux auth mounts. |
| **Raw Data Ingestion** | [`src/retail_platform/ingestion/README.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/src/retail_platform/ingestion/README.md) | Kaggle API automation, ZIP decompression, schema validation, and staging workflows. |
| **Medallion Bronze Layer** | [`src/retail_platform/warehouse/README.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/src/retail_platform/warehouse/README.md) | OLTP Postgres chunked streaming, ACID rollbacks, BigQuery ADC handshakes, and scaling idempotency. |
| **Comprehensive Guide & Interview Scenarios** | [`Learn.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/Learn.md) | Senior data engineering theory, architecture defenses, and 12 real-world interview Q&A scenarios. |

---

## ✍️ Engineering Case Studies & Publications

*This section captures real-world engineering challenges, architectural trade-offs, and debug breakthroughs encountered while building this platform—structured for technical blog posts and Medium articles:*

* 📝 **Article 1**: *The Docker vs. Local Trap: Building Resilient Airflow DAGs Across Windows and Linux Containers* (See [`airflow/README.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/airflow/README.md))
* 📝 **Article 2**: *Building Resilient Data Warehouse Loaders: Why Your Pipeline Needs Defense-in-Depth and ACID Rollbacks* (See [`src/retail_platform/warehouse/README.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/src/retail_platform/warehouse/README.md))
* 📝 **Article 3**: *Scaling Idempotency in Google BigQuery: Moving Beyond Table Truncation to SQL MERGE and Partition Overwrites* (See [`Learn.md`](file:///C:/Users/User/Desktop/altschool_retails_v2/Learn.md))

---

## License

This project is licensed under the [MIT License](LICENSE).
