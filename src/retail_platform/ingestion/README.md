# 📥 Ingestion Layer (`src/retail_platform/ingestion`)

The **Ingestion Layer** is the entry point of the Brazilian E-Commerce Analytics Platform. It is responsible for automated retrieval, validation, and staging of raw e-commerce datasets from external API sources into our local operational ecosystem.

---

## 🏛️ Architecture & Role in Medallion Pipeline

In our platform's data engineering workflow, the Ingestion Layer serves as the **Pre-Bronze / Staging Boundary**:
1. **Automated Fetching**: Connects to the Kaggle API to extract the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. **Decompression & Staging**: Safely downloads and unzips raw CSV archives into local staging storage (`data/raw/`).
3. **Validation & Integrity**: Performs pre-flight file checks to verify that all 9 critical tables (Orders, Customers, Products, Sellers, Payments, etc.) are present and structurally sound before downstream OLTP ingestion begins.

---

## 🛠️ Key Modules & Scripts

* **`kaggle_ingestion.py` / Ingestion Scripts**: Contains logic to programmatically authenticate with Kaggle, download dataset ZIP archives, and unpack CSV files into the designated raw data directory.
* **Error Handling**: Implements retry logic and path validation to prevent pipeline execution if external API rate limits or network drops occur.

---

## 🔐 Environment Configuration

To execute ingestion scripts, the following environment variables must be configured in your `.env` file:

```env
# Kaggle API Authentication
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_token_hex
KAGGLE_DATASET=olistbr/brazilian-ecommerce
```

---

## ✍️ Engineering Lessons & Case Studies

*💡 This section captures technical challenges and solutions designed for engineering blog posts or Medium articles.*

### 1. Automating Data Retrieval Without Manual Download Steps
* **The Challenge**: Relying on developers or automated cron jobs to manually click and download ZIP datasets creates fragile, non-reproducible data pipelines.
* **The Solution**: By embedding programmatic Kaggle API authentication via environment variables inside our Python ingestion package, the platform guarantees zero-touch, automated staging that can be orchestrated seamlessly in CI/CD or Airflow containers.
