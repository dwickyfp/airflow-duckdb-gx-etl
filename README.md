# 🚀 Bulletproof ETL Pipeline: Airflow + DuckDB + Great Expectations

A robust, modern ETL pipeline demonstrating data orchestration with **Apache Airflow**, high-performance transformation with **DuckDB**, and strict data validation using **Great Expectations**.

## 📋 Overview

This project demonstrates a "Bulletproof" approach to data engineering. Instead of just moving data, this pipeline ensures quality and performance by integrating:

-   **Apache Airflow**: Orchestration and scheduling.
-   **MinIO (S3)**: Data Lake storage.
-   **Great Expectations**: Data quality validation (stopping bad data at the gate).
-   **DuckDB**: In-memory SQL processing for fast transformations on S3 files.
-   **PostgreSQL**: Final data warehouse.

## 🏗 Architecture

1.  **Extract**: Generate synthetic sales data (using `Faker`) and upload to MinIO (S3).
2.  **Validate**: Run strict data quality checks using Great Expectations. If validation fails, the pipeline stops.
3.  **Transform & Load**: Use DuckDB to query the CSV directly from S3, perform aggregations, and load the clean data into PostgreSQL.

## 🛠 Tech Stack

-   **Orchestration**: Apache Airflow (CeleryExecutor)
-   **Compute/Transformation**: DuckDB, Pandas
-   **Data Quality**: Great Expectations
-   **Storage**: MinIO (S3 Compatible), PostgreSQL
-   **Infrastructure**: Docker, Docker Compose

## 🚀 Getting Started

### Prerequisites

-   Docker
-   Docker Compose

### Installation & Running

1.  **Clone the repository**:
    ```bash
    git clone <your-repo-url>
    cd airflow-etl
    ```

2.  **Build and Start the Stack**:
    This project uses a custom Docker image to include Java (for Spark support if needed) and specific Python dependencies.
    ```bash
    ./build.sh
    ```
    *Alternatively, you can run `docker compose up --build -d`.*

3.  **Access the Services**:

    | Service | URL | Credentials |
    | :--- | :--- | :--- |
    | **Airflow UI** | http://localhost:8080 | `airflow` / `airflow` |
    | **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
    | **Postgres** | `localhost:5443` | `postgres` / `postgres` |

## 🏃‍♂️ Running the Pipeline

1.  Go to the Airflow UI at `http://localhost:8080`.
2.  Login with `airflow` / `airflow`.
3.  Find the DAG named `sales_etl_pipeline`.
4.  Toggle the DAG to **Unpause** it.
5.  Trigger the DAG manually to see it in action.

## 📂 Project Structure

```text
.
├── dags/                   # Airflow DAGs
│   └── sales_etl_pipeline.py
├── config/                 # Airflow configuration
├── plugins/                # Custom Airflow plugins
├── docker-compose.yaml     # Main infrastructure definition
├── Dockerfile              # Custom Airflow image build
├── build.sh                # Helper script to build and start
└── requirements.txt        # Python dependencies
```

## 🔍 Key Features Explained

### Data Validation (Great Expectations)
We don't just assume data is good. We check it.
-   `order_id` must be unique.
-   `price` must be positive.
-   `status` must be one of 'COMPLETED', 'PENDING', 'CANCELLED'.

### DuckDB Transformation
Instead of loading data into Pandas memory or a temporary table, we use DuckDB to query the S3 file directly:
```sql
SELECT 
    order_id, 
    (quantity * price) as total_amount 
FROM read_csv_auto('s3://bucket/file.csv')
```
This is significantly faster and more memory-efficient for ETL tasks.

## 📝 License

This project is licensed under the Apache 2.0 License.
