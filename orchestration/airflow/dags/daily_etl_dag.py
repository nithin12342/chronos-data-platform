"""
Airflow DAG for Daily ETL Pipeline
Orchestrates data ingestion, transformation, and quality checks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.microsoft.azure.operators.adls import ADLSCopyOperator
from airflow.providers.databricks.operators.databricks_run import DatabricksSubmitRunOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'chronos_daily_etl',
    default_args=default_args,
    description='Daily ETL pipeline for Chronos Data Platform',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'daily', 'chronos'],
)

# Task definitions
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Bronze to Silver ingestion
ingest_customers = DatabricksSubmitRunOperator(
    task_id='ingest_customers',
    dag=dag,
    json={
        'spark_python_task': {
            'python_file': 'dbfs:/scripts/ingest_customers.py',
            'parameters': ['{{ ds }}']
        },
        'new_cluster': {
            'num_workers': 4,
            'spark_version': '13.3.x-scala12',
        }
    }
)

ingest_orders = DatabricksSubmitRunOperator(
    task_id='ingest_orders',
    dag=dag,
    json={
        'spark_python_task': {
            'python_file': 'dbfs:/scripts/ingest_orders.py',
            'parameters': ['{{ ds }}']
        },
        'new_cluster': {
            'num_workers': 4,
            'spark_version': '13.3.x-scala12',
        }
    }
)

# Quality checks - Bronze layer
quality_bronze_customers = GreatExpectationsOperator(
    task_id='quality_bronze_customers',
    dag=dag,
    expectation_suite_name='customers_bronze_suite',
    batch_request={
        'datasource_name': 'bronze_datasource',
        'data_connector_name': 'default_incremental_data_connector',
        'data_asset_name': 'customers',
        'limit': 10000
    },
    fail_task_on_validation_failure=False,
)

quality_bronze_orders = GreatExpectationsOperator(
    task_id='quality_bronze_orders',
    dag=dag,
    expectation_suite_name='orders_bronze_suite',
    batch_request={
        'datasource_name': 'bronze_datasource',
        'data_connector_name': 'default_incremental_data_connector',
        'data_asset_name': 'orders',
        'limit': 10000
    },
    fail_task_on_validation_failure=False,
)

# dbt transformations - Silver layer
transform_dim_customers = DatabricksSubmitRunOperator(
    task_id='transform_dim_customers',
    dag=dag,
    json={
        'spark_python_task': {
            'python_file': 'dbfs:/scripts/dbt_run.py',
            'parameters': ['--models', 'dim_customers', '--vars', f'run_date: {{ ds }}']
        }
    }
)

transform_fct_orders = DatabricksSubmitRunOperator(
    task_id='transform_fct_orders',
    dag=dag,
    json={
        'spark_python_task': {
            'python_file': 'dbfs:/scripts/dbt_run.py',
            'parameters': ['--models', 'fct_orders', '--vars', f'run_date: {{ ds }}']
        }
    }
)

# Quality checks - Silver layer
quality_silver = GreatExpectationsOperator(
    task_id='quality_silver',
    dag=dag,
    expectation_suite_name='orders_suite',
    batch_request={
        'datasource_name': 'silver_datasource',
        'data_connector_name': 'default_incremental_data_connector',
        'data_asset_name': 'orders',
    },
    fail_task_on_validation_failure=False,
)

# Aggregations - Gold layer
aggregate_daily_metrics = DatabricksSubmitRunOperator(
    task_id='aggregate_daily_metrics',
    dag=dag,
    json={
        'spark_python_task': {
            'python_file': 'dbfs:/scripts/aggregate_metrics.py',
            'parameters': ['{{ ds }}']
        }
    }
)

# Notification on completion
notify_completion = PythonOperator(
    task_id='notify_completion',
    dag=dag,
    python_callable=lambda: print("ETL pipeline completed successfully!"),
)

# Task dependencies
start_task >> [ingest_customers, ingest_orders]
[ingest_customers, ingest_orders] >> [quality_bronze_customers, quality_bronze_orders]
[quality_bronze_customers, quality_bronze_orders] >> [transform_dim_customers, transform_fct_orders]
[transform_dim_customers, transform_fct_orders] >> quality_silver
quality_silver >> aggregate_daily_metrics
aggregate_daily_metrics >> notify_completion
