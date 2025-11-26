"""
Sales ETL Pipeline DAG
This DAG orchestrates an ETL pipeline that:
1. Generates and uploads sales data to MinIO (S3-compatible storage)
2. Validates data quality using Great Expectations
3. Transforms and loads data to PostgreSQL using DuckDB

Best Practices Applied:
- Uses TaskFlow API with type hints
- Proper error handling and logging
- Configuration management via Airflow Variables/Connections
- Idempotent operations
- Resource cleanup
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import duckdb
import great_expectations as gx
import pandas as pd
import resend
from faker import Faker

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Initialize logger
logger = logging.getLogger(__name__)

# --- Configuration with fallbacks ---
# Use Airflow Variables for better configuration management
MINIO_ENDPOINT = Variable.get("minio_endpoint", default="http://minio:9000")
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default="minioadmin")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default="minioadmin")
BUCKET_NAME = Variable.get("sales_bucket_name", default="airflow-data")
FILE_NAME = "sales_data.csv"
S3_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"

# Postgres Table
TABLE_NAME = Variable.get("sales_table_name", default="sales_data")

# Resend Configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

def send_email_notification(subject: str, html_content: str) -> None:
    """Sends an email notification using Resend.
    
    Args:
        subject: Email subject line
        html_content: HTML body content
    """
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not found. Skipping email notification.")
        return

    try:
        response = resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": "feri.dfp@gmail.com",
            "subject": subject,
            "html": html_content
        })
        logger.info(f"Email sent successfully: {response}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)


def on_failure_callback(context: dict[str, Any]) -> None:
    """Callback function for task failure.
    
    Args:
        context: Airflow task context dictionary
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context.get('logical_date', context.get('execution_date'))
    log_url = task_instance.log_url
    
    subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3>Task Failed</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Log URL:</b> <a href="{log_url}">{log_url}</a></p>
    """
    send_email_notification(subject, html_content)


def on_success_callback(context: dict[str, Any]) -> None:
    """Callback function for DAG success.
    
    Args:
        context: Airflow DAG context dictionary
    """
    dag_id = context['dag'].dag_id
    execution_date = context.get('logical_date', context.get('execution_date'))
    
    subject = f"✅ Airflow DAG Succeeded: {dag_id}"
    html_content = f"""
    <h3>DAG Succeeded</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    """
    send_email_notification(subject, html_content)

# Default Arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': on_failure_callback,
}


def get_s3_storage_options() -> dict[str, dict[str, str]]:
    """Get S3/MinIO storage options for pandas operations.
    
    Returns:
        Dictionary with client_kwargs for S3 connection
    """
    return {
        "client_kwargs": {
            "endpoint_url": MINIO_ENDPOINT,
            "aws_access_key_id": MINIO_ACCESS_KEY,
            "aws_secret_access_key": MINIO_SECRET_KEY
        }
    }

@task(retries=3)
def generate_and_upload_data() -> dict[str, Any]:
    """Generates dummy sales data and uploads to MinIO.
    
    Returns:
        Dictionary containing upload metadata (record count, file path)
        
    Raises:
        AirflowException: If data generation or upload fails
    """
    try:
        fake = Faker()
        num_records = Variable.get("sales_num_records", default=1000, deserialize_json=False)
        num_records = int(num_records)
        
        logger.info(f"Generating {num_records} sales records...")
        data = []

        for _ in range(num_records):
            data.append({
                "order_id": fake.uuid4(),
                "customer_name": fake.name(),
                "product_name": fake.word(),
                "quantity": fake.random_int(min=1, max=10),
                "price": round(fake.random_number(digits=2) + fake.random.random(), 2),
                "order_date": fake.date_between(start_date='-30d', end_date='today').isoformat(),
                "status": fake.random_element(elements=('COMPLETED', 'PENDING', 'CANCELLED'))
            })

        df = pd.DataFrame(data)
        logger.info(f"Generated DataFrame with shape: {df.shape}")
        
        logger.info(f"Uploading to {S3_PATH}...")
        storage_options = get_s3_storage_options()
        
        # Use s3fs via pandas
        df.to_csv(S3_PATH, index=False, storage_options=storage_options)
        logger.info(f"Successfully uploaded {len(df)} records to {S3_PATH}")
        
        return {
            "record_count": len(df),
            "file_path": S3_PATH,
            "columns": list(df.columns)
        }
    except Exception as e:
        logger.error(f"Failed to generate and upload data: {e}", exc_info=True)
        raise AirflowException(f"Data generation failed: {e}")

@task(retries=2)
def validate_data() -> dict[str, Any]:
    """Validates data from MinIO using Great Expectations.
    
    Returns:
        Dictionary containing validation results and statistics
        
    Raises:
        AirflowException: If data validation fails
    """
    try:
        logger.info(f"Reading data from {S3_PATH} for validation...")
        storage_options = get_s3_storage_options()
        df = pd.read_csv(S3_PATH, storage_options=storage_options)
        logger.info(f"Loaded {len(df)} records for validation")
        
        logger.info("Initializing Great Expectations context...")
        context = gx.get_context()
        
        # Create a batch using the pandas DataFrame directly
        batch = context.data_sources.add_pandas("pandas_datasource").add_dataframe_asset(
            name="sales_dataframe"
        ).add_batch_definition_whole_dataframe("batch_def").get_batch(batch_parameters={"dataframe": df})
        
        logger.info("Defining data quality expectations...")
        # Create expectations on the batch
        expectations = [
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="status",
                value_set=['COMPLETED', 'PENDING', 'CANCELLED']
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="product_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
        ]
        
        logger.info("Running data validation...")
        validation_results = []
        all_passed = True
        
        for expectation in expectations:
            result = batch.validate(expectation)
            validation_results.append(result)
            if not result.success:
                all_passed = False
                logger.error(f"Validation failed for: {expectation}")
        
        if not all_passed:
            logger.error("Data validation FAILED!")
            logger.error(f"Failed validations: {[r for r in validation_results if not r.success]}")
            raise AirflowException(
                f"Data validation failed! {sum(1 for r in validation_results if not r.success)} "
                f"out of {len(validation_results)} checks failed."
            )
        
        logger.info("✅ Data validation PASSED successfully")
            
        return {
            "success": all_passed,
            "record_count": len(df),
            "total_checks": len(validation_results),
            "passed_checks": sum(1 for r in validation_results if r.success),
            "validation_timestamp": datetime.now().isoformat()
        }
    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"Validation process failed: {e}", exc_info=True)
        raise AirflowException(f"Validation failed: {e}")

@task(retries=2)
def load_to_postgres() -> dict[str, Any]:
    """Loads data from MinIO to Postgres using DuckDB for transformation.
    
    Returns:
        Dictionary containing load statistics
        
    Raises:
        AirflowException: If data loading fails
    """
    con = None
    try:
        logger.info("Initializing DuckDB connection...")
        con = duckdb.connect(database=':memory:')
        
        # Configure DuckDB for MinIO/S3
        logger.info("Configuring DuckDB for S3/MinIO access...")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute(f"SET s3_endpoint='minio:9000';")
        con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")
        
        logger.info(f"Querying and transforming data from {S3_PATH}...")
        # Transformation: Calculate total amount (quantity * price) and filter COMPLETED orders
        query = f"""
            SELECT 
                order_id,
                customer_name,
                product_name,
                quantity,
                price,
                (quantity * price) as total_amount,
                CAST(order_date AS DATE) as order_date,
                status,
                CURRENT_TIMESTAMP as loaded_at
            FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_NAME}')
            WHERE status = 'COMPLETED'
        """
        
        result_df = con.execute(query).df()
        logger.info(f"Transformed {len(result_df)} rows (shape: {result_df.shape})")
        
        if result_df.empty:
            logger.warning("No COMPLETED orders found in the dataset")
            return {
                "rows_loaded": 0,
                "table_name": TABLE_NAME,
                "status": "no_data"
            }
        
        logger.info(f"Loading data into Postgres table '{TABLE_NAME}'...")
        pg_hook = PostgresHook(postgres_conn_id='db_conn')

        drop_table_sql = f"DROP TABLE IF EXISTS {TABLE_NAME};"
        create_table_sql = f"""
        CREATE TABLE {TABLE_NAME} (
            order_id VARCHAR(255) PRIMARY KEY,
            customer_name VARCHAR(255),
            product_name VARCHAR(255),
            quantity INTEGER,
            price DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            order_date DATE,
            status VARCHAR(50),
            loaded_at TIMESTAMP
        );
        """

        # Recreate table to keep data in sync with latest run
        pg_hook.run(drop_table_sql)
        pg_hook.run(create_table_sql)
        logger.info(f"Table '{TABLE_NAME}' recreated successfully")

        # Prepare rows for bulk insert via pg_hook
        target_fields = [
            "order_id",
            "customer_name",
            "product_name",
            "quantity",
            "price",
            "total_amount",
            "order_date",
            "status",
            "loaded_at"
        ]
        rows = [tuple(row[field] for field in target_fields) for row in result_df.to_dict(orient='records')]

        pg_hook.insert_rows(
            table=TABLE_NAME,
            rows=rows,
            target_fields=target_fields,
            commit_every=1000
        )
        logger.info(f"✅ Successfully loaded {len(result_df)} rows into '{TABLE_NAME}'")
        
        return {
            "rows_loaded": len(result_df),
            "table_name": TABLE_NAME,
            "columns": list(result_df.columns),
            "load_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to load data to Postgres: {e}", exc_info=True)
        raise AirflowException(f"Data loading failed: {e}")
    finally:
        if con:
            try:
                con.close()
                logger.debug("DuckDB connection closed")
            except Exception:
                pass

@dag(
    dag_id='sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: Generate sales data -> Validate with GX -> Load to Postgres via DuckDB',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    tags=['etl', 'duckdb', 'postgres', 'great_expectations', 'sales'],
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    on_success_callback=on_success_callback,
    params={
        "num_records": 1000,
        "filter_status": "COMPLETED"
    }
)
def sales_etl_pipeline():
    """Main ETL pipeline orchestration."""
    
    # Generate and upload sales data to MinIO
    upload_result = generate_and_upload_data()
    
    # Validate data quality with Great Expectations
    validation_result = validate_data()
    
    # Transform and load to Postgres using DuckDB
    load_result = load_to_postgres()

    # Define task dependencies
    upload_result >> validation_result >> load_result


# Instantiate the DAG
dag_instance = sales_etl_pipeline()
