# Retail Lakehouse Architecture (Databricks, Delta Lake, dbt Core, Airflow 3)

A production-style retail data platform built with PostgreSQL (OLTP), Databricks Unity Catalog (Delta Lake), dbt Core, and Apache Airflow.

The pipeline ingests transactional data via Change Data Capture (CDC), transforms it across Bronze, Silver, and Gold Medallion layers, maintains SCD Type 2 dimension history, and runs on an Airflow 3.x schedule inside Docker with automated data quality gates.

---

## Architecture Overview

<p align="center">
  <img src="docs/Data-Architecture.png" alt="Retail Lakehouse High Level Architecture" width="100%"/>
</p>

### Pipeline Flow

1. **OLTP (PostgreSQL)**: Transactional data (`customers`, `stores`, `products`, `employees`, `orders`, `order_items`) generated and modified on Ghost PostgreSQL.
2. **Bronze Layer (Databricks Delta Lake)**: Incremental watermarked PySpark ingestion capturing created and updated records over JDBC pushdown.
3. **Silver Layer (dbt Core)**:
   * **`silver_t`**: Technical standardization, schema casting, and deduplication with automated schema tests.
   * **`silver_b`**: Dynamic metadata-driven One Big Table (`obt_b`) built via Jinja macros.
4. **Gold Layer (Databricks SQL)**:
   * **`ephemeral`**: In-memory CTE models extracting clean dimension subsets with zero storage footprint.
   * **`snapshots`**: Slowly Changing Dimensions (SCD Type 2) tracking historical attribute changes (`dbt_valid_from`, `dbt_valid_to`).
   * **`fact_orders`**: Analytical sales fact table modeled at the order line-item grain on composite keys (`order_id + order_item_id`).
5. **Orchestration (Apache Airflow in Docker)**: 9-task pipeline triggering Databricks jobs via the Python SDK with typed lifecycle polling and circuit-breaking dbt test gates.

---

## Repository Layout

```text
.
├── README.md
├── .env.example
├── .gitignore
├── docs/
│   └── Data-Architecture.png
├── walmart_dataset/
│   ├── ddl/
│   │   └── walmart_schema.sql
│   ├── data/
│   └── load_data.py
└── airflow_dbt_project/
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── requirements.txt
    ├── dags/
    │   └── orchestrate.py
    └── walmart_project/
        ├── dbt_project.yml
        ├── profiles.yml
        ├── models/
        │   ├── silver_t/
        │   ├── silver_b/
        │   └── gold/
        ├── snapshots/
        ├── macros/
        └── tests/
```

---

## Data Models & Transformations

| Layer | Model | Type | Description |
| :--- | :--- | :--- | :--- |
| **Silver Technical** | `customers_t` | Table | Standardizes customer data, normalizes phone and email formats. |
| | `stores_t` | Table | Cleans store metadata and geographic attributes. |
| | `products_t` | Table | Deduplicates product catalog items and normalizes unit pricing. |
| | `employees_t` | Table | Casts staff attributes and job role hierarchies. |
| | `orders_t` | Incremental | Ingests header-level orders using watermark timestamp filters. |
| | `order_items_t` | Incremental | Ingests line-item sales records incrementally. |
| **Silver Business** | `obt_b` | Table | Generates an aggregated One Big Table across all 6 raw entities using dynamic Jinja loops. |
| **Gold Ephemeral** | `eph_*` | Ephemeral | 5 CTE models extracting clean dimension views without materialization costs. |
| **Gold Dimensions** | `dim_*` | Snapshot | SCD Type 2 dimension tables tracking updates to customers, stores, and products. |
| **Gold Facts** | `fact_orders` | Incremental | Core sales fact table with line-item grain and surrogate dimension keys. |

---

## Airflow Orchestration Workflow

The Airflow DAG (`orchestrate.py`) implements a sequential execution path with strict failure propagation:

```text
[ Ingest CDC (Databricks SDK) ]
              │
              ▼
[ Clean Target / Logs ]
              │
              ▼
[ Source Freshness Check ]
              │
              ▼
[ Run Silver Technical Models ]
              │
              ▼
[ Test Silver Technical Models ] ──► (Stops pipeline if schema tests fail)
              │
              ▼
[ Run Silver Business OBT ]
              │
              ▼
[ Test Silver Business OBT ]     ──► (Stops pipeline if OBT integrity fails)
              │
              ▼
[ Compile Gold Ephemeral CTEs ]
              │
              ▼
[ Snapshot Gold SCD2 Dimensions ]
              │
              ▼
[ Run Gold Fact Orders ]
```

---

## Getting Started

### Prerequisites
* Docker & Docker Compose
* Python 3.10+ (or `uv`)
* Databricks workspace with an active SQL Warehouse
* PostgreSQL database (e.g. Ghost)

### 1. Environment Setup
Clone the repository and install dependencies:
```bash
git clone https://github.com/ayoubazacri/Walmart_Airflow_DBT_Project.git
cd Walmart_Airflow_DBT_Project
uv sync
source .venv/bin/activate
```

### 2. Database Initialization
Run the DDL script against your PostgreSQL instance to create the `raw` schema:
```bash
# Execute DDL
psql "$DATABASE_URL" -f walmart_dataset/ddl/walmart_schema.sql

# Seed transactional records
python walmart_dataset/load_data.py
```

### 3. Configure dbt Profile
Update `airflow_dbt_project/walmart_project/profiles.yml` with your Databricks connection details:
```yaml
walmart_project:
  outputs:
    dev:
      catalog: walmart
      host: your_databricks_host
      http_path: your_databricks_http_path
      schema: dbt_schema
      threads: 4
      token: your_databricks_token
      type: databricks
  target: dev
```

Test the connection:
```bash
cd airflow_dbt_project/walmart_project
dbt debug
```

### 4. Start Airflow via Docker
```bash
cd airflow_dbt_project
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker compose up -d
```

Access the Airflow UI at `http://localhost:8080` (default login: `airflow` / `airflow`) and enable the `orchestrate` DAG.

---

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Author
**Ayoub AZACRI** | Data Engineer
* [LinkedIn](https://www.linkedin.com/in/ayoub-azacri/)
* [GitHub](https://github.com/Ayoub-Azacri)
