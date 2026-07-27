# ⏱️ Orchestration & Containerization Layer (`airflow/`)

The **Airflow Layer** manages workflow scheduling, task dependency resolution, and automated failure recovery for the analytical platform. It orchestrates our data ingestion and warehouse extraction pipelines inside an isolated, multi-container Linux Docker environment.

---

## 🏛️ Architecture & Role in Medallion Pipeline

In our platform, Airflow acts as the **Central Orchestration Engine**:
1. **Pre-Flight Readiness Gate**: Executes leading tasks to verify Google Cloud authentication, BigQuery API handshakes, and dataset existence before allowing computationally heavy data extraction to begin.
2. **Parallelized Bronze Ingestion**: Fans out extraction tasks across 9 concurrent worker processes to simultaneously stream PostgreSQL OLTP tables into BigQuery OLAP tables.

---

## 🛠️ DAG Architecture & Design Patterns

### 1. The Pre-Flight Check / Readiness Gate Pattern ([postgres_to_bigquery_dag.py](file:///C:/Users/User/Desktop/altschool_retails_v2/airflow/dags/postgres_to_bigquery_dag.py))
Instead of relying on out-of-band test scripts that developers might forget to run, automated testing is baked directly into the DAG graph:
```text
[Task 1: verify_gcp_and_bq_connection]
                 │
                 ▼
[Task 2: ensure_bq_dataset_exists]
                 │
                 ├──────────────────────────────┬──────────────────────────────┐
                 ▼                              ▼                              ▼
[Task 3: extract_load_products]  [Task 4: extract_load_orders]  [Task 5...9: other tables]
```
If GCP credentials expire or dataset permissions fail, Task 1 fails immediately, blocking Tasks 3–11 and preventing partial database extraction or wasted compute resources.

### 2. On-Demand Execution (`schedule=None`)
To prevent accidental cloud billing from recurring daily background runs during active development, DAGs are configured with `schedule=None`. Pipelines execute on-demand via explicit manual triggers in the Airflow Web UI or API.

---

## 🐳 Docker Compose & Container Architecture

Our orchestration stack runs inside Docker Compose (`docker-compose.yml`), mapping local Windows source code directly into isolated Linux container volumes:
* **Live Volume Syncing**: Both `./airflow/dags:/opt/airflow/dags` and `./src:/opt/airflow/src` are mounted as real-time volumes. Any Python code modification on your local Windows IDE is reflected instantly inside running Linux worker containers without restarting Docker.
* **Database Persistence**: PostgreSQL data files are stored in named Docker volumes (`postgres-db-volume`). Running `docker compose down` destroys only temporary compute containers while leaving database tables and Airflow history 100% intact.

---

## ✍️ Engineering Lessons & Case Studies (Medium / Tech Blog Hooks)

### 1. The Airflow 2 vs. Airflow 3 Provider Trap: Multi-Environment Import Resilience
* **The Challenge**: Apache Airflow 3.0+ (AEP-44 Task SDK separation) moved core operators like `PythonOperator` into standard provider packages (`airflow.providers.standard.operators.python`). However, stable LTS production Docker containers run on Airflow 2.9 (`apache/airflow:2.9.3`), where operators live in core (`airflow.operators.python`). A single static import path will crash in one of the two environments.
* **The Solution**: Implementing a defensive `try/except` fallback import block at the head of every DAG:
  ```python
  try:
      # Airflow 3.0+ (For local IDE linting and SDKs)
      from airflow.providers.standard.operators.python import PythonOperator
  except ImportError:
      # Airflow 2.x (For Linux Docker container execution)
      from airflow.operators.python import PythonOperator
  ```
  This achieves 100% cross-environment compatibility without code refactoring.

### 2. The Windows Host vs. Linux Container Secret Trap: Secure GCP Authentication
* **The Challenge**: Setting an environment variable like `GOOGLE_APPLICATION_CREDENTIALS="C:/Users/User/.gcp/key.json"` in Docker on Windows causes fatal `DefaultCredentialsError` exceptions. Why? Because a Linux container has an isolated POSIX filesystem that does not recognize Windows `C:/` drive letters or Windows directory paths.
* **The Enterprise Solution (Read-Only Volume Mounting)**:
  1. Leave your secret JSON key safely in your Windows home folder (`C:/Users/User/.gcp/`) so it never gets copied or accidentally committed to Git.
  2. Mount that Windows folder read-only into Docker under `volumes:` in `docker-compose.yml`:
     ```yaml
     - C:/Users/User/.gcp:/opt/airflow/gcp_keys:ro
     ```
  3. Point your container environment variable in `.env` to the **Linux container path**:
     ```env
     GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp_keys/altschool-retails-v2-5b1defe15394.json
     ```
  This bridges Windows security and Linux container isolation flawlessly.
