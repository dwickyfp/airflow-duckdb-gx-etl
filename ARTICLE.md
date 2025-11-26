# 🚀 Building a Bulletproof ETL Pipeline with Airflow, DuckDB, and Great Expectations

In the world of Data Engineering, building pipelines that just "move data" isn't enough anymore. We need pipelines that are **resilient**, **performant**, and most importantly, **trustworthy**.

Today, I'm going to walk you through how I built a robust Sales ETL pipeline using a modern, open-source stack: **Apache Airflow**, **DuckDB**, **Great Expectations**, and **PostgreSQL**.

## 🛠 The Modern Data Stack

Why did I choose these tools? Let's break it down:

*   **Apache Airflow**: The industry standard for orchestration. It handles the "when" and "how" of our tasks, managing retries, dependencies, and alerting.
*   **DuckDB**: The "SQLite for Analytics". It allows us to run lightning-fast SQL queries directly on files (like CSVs in S3) without needing a heavy warehouse for intermediate processing.
*   **Great Expectations (GX)**: A standard for data quality. It ensures that the data entering our system meets our strict requirements *before* it breaks our downstream dashboards.
*   **PostgreSQL**: Our reliable serving layer where the clean, transformed data lives.
*   **MinIO**: A high-performance, S3-compatible object storage server, perfect for local development and testing.

## 🌊 The Pipeline Architecture

Our pipeline, `sales_etl_pipeline`, follows a classic ETL (Extract, Transform, Load) pattern with a heavy emphasis on validation.

1.  **Extract**: Generate raw sales data and upload it to our Data Lake (MinIO).
2.  **Validate**: Run strict data quality checks using Great Expectations.
3.  **Transform & Load**: Use DuckDB to transform the data and load it into Postgres.

Let's dive into the code!

### 1. Generating & Uploading Data (The "Extract")

First, we need data. In a real-world scenario, this might come from an API or a transactional DB. For this demo, we're using `Faker` to generate realistic sales records and uploading them to an S3 bucket (MinIO).

```python
@task(retries=3)
def generate_and_upload_data() -> dict[str, Any]:
    # ... (Faker generation logic) ...
    
    # Upload to S3/MinIO
    df.to_csv(S3_PATH, index=False, storage_options=storage_options)
    
    return {"record_count": len(df), "file_path": S3_PATH}
```

### 2. The Gatekeeper: Data Validation with Great Expectations

This is where many pipelines fail. They blindly ingest data. We don't do that here. We use **Great Expectations** to define what "good" data looks like.

If the data doesn't pass these checks, the pipeline **stops immediately**. No bad data in production!

```python
@task(retries=2)
def validate_data() -> dict[str, Any]:
    # ... load data ...
    
    # Define our expectations
    expectations = [
        # IDs must be unique
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
        # Price must be positive
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01),
        # Status must be valid
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status", 
            value_set=['COMPLETED', 'PENDING', 'CANCELLED']
        ),
        # Critical fields cannot be null
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_name"),
    ]
    
    # Run validation
    for expectation in expectations:
        result = batch.validate(expectation)
        if not result.success:
            raise AirflowException(f"Validation failed: {expectation}")
```

### 3. High-Performance Transformation with DuckDB

Instead of loading everything into Pandas memory or pushing raw data to Postgres to transform it there, we use **DuckDB**.

DuckDB can query the CSV file directly from S3, perform aggregations, filter rows, and calculate new fields (like `total_amount`) on the fly. It's incredibly fast and efficient.

```python
@task(retries=2)
def load_to_postgres() -> dict[str, Any]:
    con = duckdb.connect(database=':memory:')
    
    # Configure S3 access for DuckDB
    con.execute(f"SET s3_endpoint='minio:9000';")
    # ... credentials ...

    # SQL Transformation on the CSV file directly!
    query = f"""
        SELECT 
            order_id,
            customer_name,
            product_name,
            quantity,
            price,
            (quantity * price) as total_amount, -- Calculated field
            CAST(order_date AS DATE) as order_date,
            status,
            CURRENT_TIMESTAMP as loaded_at
        FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_NAME}')
        WHERE status = 'COMPLETED' -- Filter only completed orders
    """
    
    result_df = con.execute(query).df()
    
    # Load to Postgres
    pg_hook.insert_rows(table=TABLE_NAME, rows=rows, ...)
```

## 💡 Why This Matters

By combining these tools, we achieve:

1.  **Data Quality at Source**: We catch issues *before* they enter the warehouse.
2.  **Efficiency**: DuckDB handles the heavy lifting of transformation without needing a Spark cluster for medium-sized datasets.
3.  **Observability**: Airflow provides the logs, retries, and history we need to sleep soundly at night.

## 🏁 Conclusion

Building an ETL pipeline is easy. Building one that you can trust is harder. By integrating **Great Expectations** for validation and **DuckDB** for efficient processing into your **Airflow** DAGs, you create a system that is robust, scalable, and maintainable.

---
*Check out the full code in the repository!*
