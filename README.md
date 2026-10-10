# Retail Sales Data Pipeline

A containerized data engineering pipeline that downloads retail transaction data from Kaggle, loads it into PostgreSQL, transforms it into a relational warehouse model, and produces analytical datasets for daily sales, like-for-like (LFL) store analysis, and ABC product classification.

The pipeline is orchestrated with Apache Airflow and is designed to be reproducible, idempotent, and easy to run locally with Docker Compose.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Main Technologies](#2-main-technologies)
3. [Repository Structure](#3-repository-structure)
4. [Data Model](#4-data-model)
5. [Business Logic](#5-business-logic)
6. [Like-for-Like Store Analysis](#6-like-for-like-store-analysis)
7. [ABC Product Classification](#7-abc-product-classification)
8. [Airflow DAG](#8-airflow-dag)
9. [Idempotency](#9-idempotency)
10. [Error Handling](#10-error-handling)
11. [Running the Project](#11-running-the-project)
12. [Start the Environment](#12-start-the-environment)
13. [Verify PostgreSQL](#13-verify-postgresql)
14. [Verify Airflow](#14-verify-airflow)
15. [Run the Pipeline Through Airflow](#15-run-the-pipeline-through-airflow)
16. [Test Tasks Individually](#16-test-tasks-individually)
17. [Database Validation](#17-database-validation)
18. [Data Quality Checks](#18-data-quality-checks)
19. [Source Dataset Characteristics](#19-source-dataset-characteristics)
20. [Design Decisions](#20-design-decisions)
21. [Production Considerations](#21-production-considerations)
22. [Limitations of the Source Dataset](#22-limitations-of-the-source-dataset)
23. [Expected End-to-End Result](#23-expected-end-to-end-result)
24. [Quick Start](#24-quick-start)
25. [Summary](#25-summary)

---

## 1. Project Overview

The pipeline processes the Kaggle **Retail Transaction Dataset** and implements the following flow:

```text
Kaggle Dataset
      │
      ▼
Download
      │
      ▼
Raw Staging
      │
      ▼
Cleaning & Type Validation
      │
      ▼
Warehouse Dimensions + Fact
      │
      ├──────────────► LFL Store Analysis
      │
      ├──────────────► Daily Store/Product Aggregation
      │
      └──────────────► ABC Product Classification
      │
      ▼
Validation
```

The complete workflow is orchestrated by Apache Airflow and runs daily at midnight.

---

## 2. Main Technologies

- Python 3.12
- PostgreSQL 16
- Apache Airflow 2.11.2
- pandas
- psycopg2
- Docker / Docker Compose
- Kaggle API

**Python** is responsible for:

- configuration;
- dataset download;
- source-data processing;
- loading data into PostgreSQL;
- pipeline orchestration at the application level;
- validation.

**SQL** is responsible for:

- relational modeling;
- warehouse transformations;
- aggregations;
- LFL analysis;
- ABC classification.

**Airflow** is responsible for orchestration and scheduling.

---

## 3. Repository Structure

```text
retail-sales-pipeline/
│
├── dags/
│   └── retail_sales_pipeline.py
│
├── src/
│   ├── pipeline.py
│   └── retail_pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── database.py
│       ├── kaggle.py
│       ├── pipeline.py
│       └── processor.py
│
├── sql/
│   ├── 01_schema.sql
│   └── 02_transformations.sql
│
├── data/
│   └── .gitkeep
│
├── logs/
│   └── .gitkeep
│
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Python modules

#### `config.py`

Centralizes runtime configuration loaded from environment variables.

#### `database.py`

Provides PostgreSQL connectivity and SQL script execution.

#### `kaggle.py`

Encapsulates communication with the Kaggle API and validates the downloaded archive.

#### `processor.py`

Handles source-specific data processing:

- reading the CSV from the downloaded ZIP;
- validating expected columns;
- normalizing column names;
- converting data types;
- trimming string values;
- removing invalid records.

#### `pipeline.py`

Contains the main `RetailSalesPipeline` application service.

It coordinates:

- database initialization;
- dataset download;
- raw-data loading;
- transformations;
- validation.

The class is instantiated inside Airflow tasks rather than while the DAG is being parsed.

#### `src/pipeline.py`

Provides a command-line entry point for running the pipeline without Airflow.

---

## 4. Data Model

The database is separated into three schemas.

```text
staging
    │
    ▼
warehouse
    ├── dim_store
    ├── dim_product
    ├── fact_sales
    └── dim_lfl_store
    │
    ▼
analytics
    └── daily_store_product_sales
```

### 4.1 Staging

#### `staging.sales_raw`

Contains the cleaned source dataset before warehouse transformations.

**Grain:** one row per source transaction.

Columns include:

- customer ID
- product ID
- quantity
- price
- transaction date
- payment method
- store location
- product category
- discount
- total amount

The staging layer intentionally remains close to the source structure.

### 4.2 Warehouse

#### `warehouse.dim_store`

Contains one record per source store location.

| Column            | Description                      |
|-------------------|----------------------------------|
| `store_id`        | Generated warehouse identifier   |
| `store_name`      | Generated store name             |
| `source_location` | Original source store identifier |

Store IDs are generated deterministically from sorted source locations. For example:

```text
STORE_000001
STORE_000002
...
STORE_100000
```

The generated IDs use dynamic padding so that values remain unique when the number of stores exceeds 999.

#### `warehouse.dim_product`

Contains one record per product.

| Column         | Description                           |
|----------------|---------------------------------------|
| `product_id`   | Source product identifier             |
| `product_name` | Generated product name                |
| `abc_quantity` | ABC classification by quantity        |
| `abc_amount`   | ABC classification by revenue         |

Product names are generated because the source dataset does not provide descriptive product names.

##### Product category modeling decision

`product_category` is **not** stored in `dim_product`.

Source-data profiling showed that the same `product_id` occurs under multiple categories. For example, products A–D each occur with multiple categories.

Therefore:

```text
product_id → product_category
```

is not a one-to-one relationship in the source data.

Putting `product_category` into the product dimension would introduce an incorrect functional dependency and potentially duplicate products. The category is consequently stored at transaction level in `fact_sales`.

This is an intentional modeling decision based on the actual source data.

#### `warehouse.fact_sales`

Contains the transaction-level business facts.

**Grain:** one row per source transaction.

Important attributes include:

- customer
- product
- store
- transaction timestamp
- quantity
- price
- category
- discount
- revenue
- payment method

The table uses a generated `sales_id` as its surrogate primary key because the source dataset does not provide a transaction identifier.

#### `warehouse.dim_lfl_store`

Stores the LFL status for each current-year store/month combination.

**Grain:** one row per store, year, and month.

Columns:

- `year`
- `month`
- `store_id`
- `lfl_status`

### 4.3 Analytics

#### `analytics.daily_store_product_sales`

Daily aggregation at `sales_date` + `store_id` + `product_id`.

Metrics:

- total quantity;
- total revenue;
- transaction count;
- unique customers;
- revenue-weighted discount percentage.

The table has a composite primary key on:

```text
(sales_date, store_id, product_id)
```

---

## 5. Business Logic

### 5.1 Data Cleaning

Before loading the warehouse, the source data is:

- checked for expected columns;
- normalized to pipeline column names;
- trimmed for string fields;
- converted to numeric/date types;
- checked for required fields;
- checked for valid business values.

Invalid records are removed before loading. The following values are considered invalid:

- `quantity <= 0`;
- `price < 0`;
- `total amount < 0`;
- `discount` outside 0–100%.

If no valid records remain, the pipeline fails rather than loading an empty dataset.

### 5.2 Daily Store/Product Aggregation

The analytical table is aggregated by transaction date, store, and product.

| Metric             | Calculation                  |
|--------------------|------------------------------|
| Revenue            | `SUM(total_amount)`          |
| Quantity           | `SUM(quantity)`              |
| Transaction count  | `COUNT(*)`                   |
| Unique customers   | `COUNT(DISTINCT customer_id)`|

**Weighted discount**

Discount is weighted by realized transaction revenue:

```text
SUM(discount × revenue)
-----------------------
     SUM(revenue)
```

The metric is left `NULL` when total revenue is zero.

---

## 6. Like-for-Like Store Analysis

The latest year present in the dataset is treated as the current year.

A store/month is considered LFL only if:

1. the store traded during the same month in the previous year;
2. the store traded on every expected day of the current-year month.

The expected number of days is calculated dynamically:

- **Normal completed months:** number of calendar days in the month.
- **Latest month:** the latest transaction day.

This means a partial latest month is not incorrectly compared against the full calendar month.

For example, the source data ends on `2024-04-28`. Therefore April 2024 has an expected coverage of **28 days** rather than 30.

Leap years are handled through PostgreSQL date arithmetic, so February 2024 correctly contains 29 days.

### Important LFL dataset observation

The supplied dataset contains approximately 100,000 distinct store locations for 100,000 transactions. A store therefore does not appear across both 2023 and 2024.

As a result:

```text
2023 stores ∩ 2024 stores = 0
```

and no store can satisfy the previous-year LFL requirement. Therefore the current dataset produces:

```text
LFL stores = 0
```

This is expected behavior. The LFL implementation itself is not disabled or hard-coded to return false. It applies the required business rules to the available source data.

---

## 7. ABC Product Classification

ABC classification is calculated separately for:

- quantity realization;
- revenue realization.

The calculation uses the daily analytical table rather than directly using the raw transaction table.

1. Total realization is calculated per product.
2. Products are ordered by descending realization.
3. Cumulative realization share is calculated using a window function.

Classification rules:

| Class | Rule                       |
|-------|----------------------------|
| A     | cumulative share `<= 50%`  |
| B     | cumulative share `<= 70%`  |
| C     | cumulative share `> 70%`   |

The calculation is performed independently for quantity and revenue. The resulting classifications are written back to `warehouse.dim_product` as `abc_quantity` and `abc_amount`.

Product ID is used as a deterministic tie-breaker when realization values are equal.

---

## 8. Airflow DAG

```text
start
  │
  ▼
wait_for_postgres
  │
  ▼
init_database
  │
  ▼
download_data
  │
  ▼
load_raw_data
  │
  ▼
transform_data
  │
  ▼
validate_pipeline
  │
  ▼
end
```

### Schedule

```text
0 0 * * *
```

The pipeline runs daily at midnight.

**Timezone:** `Asia/Tbilisi`

### Additional settings

| Setting           | Value     |
|-------------------|-----------|
| `catchup`         | `false`   |
| `max_active_runs` | `1`       |
| `retries`         | `2`       |
| `retry_delay`     | 5 minutes |

### PostgreSQL Sensor

Before the pipeline starts processing data, an Airflow `SqlSensor` executes:

```sql
SELECT 1;
```

against the configured `retail_postgres` connection.

The sensor uses `mode = reschedule` so it does not unnecessarily occupy a worker slot while waiting for PostgreSQL.

---

## 9. Idempotency

The pipeline is designed to produce the same warehouse state when rerun against the same source data.

- The staging table is truncated before loading.
- Derived warehouse and analytics tables are rebuilt by the transformation SQL.

This makes the pipeline suitable for the technical-test dataset and avoids duplicate data when the DAG is rerun.

The approach is intentionally simple rather than implementing incremental processing or CDC. For a production-scale system, the transformation layer would likely be replaced with incremental loading, partitioning, merge/upsert logic, and schema/data migrations.

---

## 10. Error Handling

The pipeline intentionally fails fast when important assumptions are violated. Examples include:

- missing Kaggle API token;
- missing SQL script;
- empty SQL script;
- failed Kaggle download;
- invalid downloaded archive;
- missing source columns;
- no valid source records;
- empty staging table;
- empty warehouse tables;
- staging/fact row-count mismatch;
- missing ABC classifications;
- invalid fact values;
- broken store/product relationships.

Database operations are executed transactionally and rolled back if an error occurs.

---

## 11. Running the Project

### Prerequisites

Install:

- Docker
- Docker Compose

A local Python installation is not required to run the containerized pipeline.

### 11.1 Configure environment variables

Create `.env` based on `.env.example`.

The configuration includes PostgreSQL and Kaggle credentials. The Kaggle API token must be provided through:

```text
KAGGLE_API_TOKEN
```

> **Warning:** Do not commit `.env` or API credentials to Git.

---

## 12. Start the Environment

From the project root:

```bash
docker compose build
```

Then:

```bash
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

PostgreSQL should be running before the Airflow pipeline executes.

---

## 13. Verify PostgreSQL

On PowerShell, use the actual configured database credentials:

```powershell
docker compose exec postgres psql -U retail_user -d retail_db -c "SELECT 1;"
```

Expected result:

```text
 ?column?
----------
        1
(1 row)
```

> **Note:** Environment variables defined in `.env` for Docker Compose are not automatically PowerShell environment variables. Commands such as the following should **not** be used unless those variables were separately exported into the PowerShell session:
>
> ```powershell
> docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
> ```

---

## 14. Verify Airflow

List DAGs:

```bash
docker compose exec airflow-scheduler airflow dags list
```

List tasks:

```bash
docker compose exec airflow-scheduler airflow tasks list retail_sales_pipeline
```

The expected tasks are:

```text
start
wait_for_postgres
init_database
download_data
load_raw_data
transform_data
validate_pipeline
end
```

---

## 15. Run the Pipeline Through Airflow

Trigger the DAG:

```bash
docker compose exec airflow-scheduler airflow dags trigger retail_sales_pipeline
```

Run history can be inspected with:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d retail_sales_pipeline
```

The Airflow UI is available at: <http://localhost:8080>

---

## 16. Test Tasks Individually

For debugging, Airflow tasks can be tested independently. Example:

```bash
docker compose exec airflow-scheduler \
    airflow tasks test retail_sales_pipeline wait_for_postgres 2026-10-08
```

The recommended testing order is:

1. `start`
2. `wait_for_postgres`
3. `init_database`
4. `download_data`
5. `load_raw_data`
6. `transform_data`
7. `validate_pipeline`
8. `end`

This makes it easier to isolate infrastructure, ingestion, transformation, and validation failures.

---

## 17. Database Validation

After a successful run, the main row counts can be checked with:

```sql
SELECT 'staging.sales_raw' AS table_name, COUNT(*) AS row_count
FROM staging.sales_raw

UNION ALL

SELECT 'warehouse.dim_store', COUNT(*)
FROM warehouse.dim_store

UNION ALL

SELECT 'warehouse.dim_product', COUNT(*)
FROM warehouse.dim_product

UNION ALL

SELECT 'warehouse.fact_sales', COUNT(*)
FROM warehouse.fact_sales

UNION ALL

SELECT 'warehouse.dim_lfl_store', COUNT(*)
FROM warehouse.dim_lfl_store

UNION ALL

SELECT 'analytics.daily_store_product_sales', COUNT(*)
FROM analytics.daily_store_product_sales;
```

For the supplied dataset, the expected results are approximately:

| Table                                 | Rows    |
|---------------------------------------|--------:|
| `staging.sales_raw`                   | 100,000 |
| `warehouse.dim_store`                 | 100,000 |
| `warehouse.dim_product`               | 4       |
| `warehouse.fact_sales`                | 100,000 |
| `warehouse.dim_lfl_store`             | 32,733  |
| `analytics.daily_store_product_sales` | 100,000 |

The exact counts are data-dependent except where determined by the supplied dataset.

---

## 18. Data Quality Checks

The pipeline validation includes checks for:

- non-empty staging;
- non-empty dimensions;
- non-empty fact table;
- non-empty LFL output;
- non-empty daily aggregation;
- staging/fact row-count consistency;
- missing ABC classifications;
- unknown stores;
- unknown products;
- invalid quantity;
- invalid price;
- invalid revenue;
- invalid discount.

Additional manual checks can be performed directly in PostgreSQL.

### Duplicate daily grain

```sql
SELECT
    sales_date,
    store_id,
    product_id,
    COUNT(*)
FROM analytics.daily_store_product_sales
GROUP BY
    sales_date,
    store_id,
    product_id
HAVING COUNT(*) > 1;
```

Expected: `0 rows`

### Invalid discounts

```sql
SELECT COUNT(*)
FROM analytics.daily_store_product_sales
WHERE weighted_discount_pct < 0
   OR weighted_discount_pct > 100;
```

Expected: `0`

### Unknown products

```sql
SELECT COUNT(*)
FROM warehouse.fact_sales f
LEFT JOIN warehouse.dim_product p
    ON p.product_id = f.product_id
WHERE p.product_id IS NULL;
```

Expected: `0`

---

## 19. Source Dataset Characteristics

The supplied dataset contains:

- 100,000 transactions;
- 4 product IDs;
- approximately 100,000 distinct store locations;
- transaction dates spanning 2023 and 2024;
- the latest transaction date of 2024-04-28.

The dataset has an important characteristic: store locations are effectively unique to individual transactions. This affects the resulting warehouse statistics, particularly LFL analysis.

These results are therefore interpreted in the context of the supplied source data rather than treated as generic properties of the pipeline.

---

## 20. Design Decisions

### Why PostgreSQL?

PostgreSQL provides:

- relational constraints;
- foreign keys;
- analytical SQL;
- window functions;
- date functions;
- transactional loading.

It is sufficient for the assignment while remaining representative of a production relational warehouse environment.

### Why separate staging, warehouse, and analytics schemas?

The separation provides clear responsibilities:

| Schema      | Responsibility                  |
|-------------|---------------------------------|
| `staging`   | Source-oriented data            |
| `warehouse` | Relational business model       |
| `analytics` | Derived analytical datasets     |

This prevents analytical transformations from being mixed with source ingestion logic.

### Why Python + SQL?

Python is used where application-level behavior is appropriate:

- API communication;
- file processing;
- data cleaning;
- database loading;
- orchestration.

SQL is used for relational transformations and analytical calculations. This avoids forcing complex warehouse logic into Python unnecessarily.

### Why OOP?

The pipeline contains several components with distinct responsibilities:

- `PipelineConfig`
- `DatabaseClient`
- `KaggleDatasetClient`
- `RetailDataProcessor`
- `RetailSalesPipeline`

The classes are intentionally lightweight. The goal is separation of responsibilities rather than creating an abstraction layer for every operation.

### Why rebuild instead of incremental loading?

The supplied dataset is relatively small and the technical-test requirements do not require incremental processing.

Rebuilding the derived tables provides:

- deterministic results;
- simple reruns;
- straightforward testing;
- low implementation complexity.

For a production system, incremental processing would be preferable for a dataset of substantially larger scale.

---

## 21. Production Considerations

The current implementation is intentionally scoped to the technical assignment. A production implementation could additionally introduce:

### Incremental processing

Instead of rebuilding all derived tables:

- process only new/changed transactions;
- use watermarking or ingestion timestamps;
- use `MERGE`/upsert patterns where appropriate.

### Database migrations

`01_schema.sql` is a bootstrap schema rather than a migration framework. Production deployments should use versioned migrations such as:

- Alembic
- Flyway
- Liquibase

### Separate Airflow metadata and application databases

For the technical test, Airflow and the retail warehouse can share PostgreSQL infrastructure. In production, Airflow metadata and application/warehouse data should normally be separated.

### Object storage

The downloaded source archive could be stored in:

- Amazon S3;
- Google Cloud Storage;
- Azure Blob Storage;
- another object-storage platform.

### Large-scale ingestion

For larger datasets, loading through a Python list would not scale efficiently. Possible alternatives include:

- PostgreSQL `COPY`;
- chunked processing;
- object-storage-based ingestion;
- distributed processing where justified.

### Data quality framework

A production pipeline could use tools such as:

- Great Expectations;
- Soda;
- dbt tests;
- custom SQL quality checks.

### Observability

Production deployment should include:

- structured logging;
- metrics;
- alerting;
- task-level monitoring;
- data freshness monitoring;
- failure notifications.

### Secrets

Kaggle and database credentials should be stored in a secret-management system rather than committed configuration files. Examples include:

- AWS Secrets Manager;
- HashiCorp Vault;
- Kubernetes Secrets;
- managed Airflow secret backends.

### Container orchestration

Docker Compose is used here for local reproducibility. A production deployment could run Airflow and supporting services on Kubernetes or a managed orchestration platform.

---

## 22. Limitations of the Source Dataset

The dataset itself has several limitations that affect analytical interpretation:

- Store locations are effectively unique per transaction.
- Only four product IDs are available.
- Product categories are not uniquely determined by product ID.
- There is no explicit transaction identifier.
- The latest month is incomplete.
- There is no store overlap between the two years.

These limitations are not corrected artificially by the pipeline because doing so would change the source data's meaning. Instead, the warehouse model and analytical logic are designed to preserve the source semantics.

---

## 23. Expected End-to-End Result

A successful run should produce:

```text
                    ┌─────────────────────┐
                    │   Kaggle Dataset    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  staging.sales_raw  │
                    └──────────┬──────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
          dim_store     dim_product      fact_sales
                │              │              │
                └──────────────┼──────────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
          LFL analysis             Daily aggregation
                 │                           │
                 ▼                           ▼
        dim_lfl_store          daily_store_product_sales
                                             │
                                             ▼
                                    ABC classification
                                             │
                                             ▼
                                       dim_product
```

The Airflow DAG validates the complete process before marking the pipeline successful.

---

## 24. Quick Start

For a clean local run:

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

Check services:

```bash
docker compose ps
```

Check PostgreSQL:

```bash
docker compose exec postgres \
    psql -U retail_user -d retail_db -c "SELECT 1;"
```

Check Airflow:

```bash
docker compose exec airflow-scheduler \
    airflow dags list
```

Trigger the pipeline:

```bash
docker compose exec airflow-scheduler \
    airflow dags trigger retail_sales_pipeline
```

Open Airflow: <http://localhost:8080>

After completion, validate the warehouse using the SQL queries described in [Database Validation](#17-database-validation) and [Data Quality Checks](#18-data-quality-checks).

---

## 25. Summary

This project demonstrates an end-to-end data engineering workflow covering:

- API-based data ingestion;
- source validation and cleaning;
- Python application design;
- relational warehouse modeling;
- dimensional modeling;
- transaction-level fact design;
- analytical SQL;
- window functions;
- LFL business logic;
- ABC classification;
- data-quality validation;
- Airflow orchestration;
- Dockerized execution;
- environment-based configuration;
- reproducible local deployment.

The implementation intentionally favors clear responsibilities and maintainability over unnecessary abstraction, while leaving clear paths for incremental processing, production orchestration, observability, and scalable ingestion.
