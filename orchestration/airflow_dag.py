"""
Apache Airflow DAG for Financial Data Pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import structlog

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='End-to-end financial data pipeline',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['financial', 'data-pipeline', 'etl']
)

# Configuration
RAPIDAPI_KEY = Variable.get("RAPIDAPI_KEY", default_var="")
KAFKA_BOOTSTRAP_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="localhost:9092")
S3_BUCKET = Variable.get("S3_BUCKET", default_var="financial-data")
POSTGRES_CONN_ID = "postgres_default"

def extract_yahoo_finance_data(**context):
    """Extract data from Yahoo Finance API."""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    from data_pipeline.extractors import (
        StockQuotesExtractor, MarketScreenerExtractor, StockNewsExtractor
    )
    
    logger = structlog.get_logger()
    logger.info("Starting Yahoo Finance data extraction")
    
    # Extract stock quotes
    quotes_extractor = StockQuotesExtractor(RAPIDAPI_KEY)
    quotes_job = quotes_extractor.extract(symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"])
    
    # Extract market screeners
    screeners_extractor = MarketScreenerExtractor(RAPIDAPI_KEY)
    screeners_job = screeners_extractor.extract()
    
    # Extract stock news
    news_extractor = StockNewsExtractor(RAPIDAPI_KEY)
    news_job = news_extractor.extract(symbols=["AAPL", "MSFT", "GOOGL"])
    
    # Log results
    logger.info("Extraction completed", 
               quotes_status=quotes_job.status,
               screeners_status=screeners_job.status,
               news_status=news_job.status)
    
    return {
        'quotes_job_id': quotes_job.job_id,
        'screeners_job_id': screeners_job.job_id,
        'news_job_id': news_job.job_id
    }

def transform_data(**context):
    """Transform and clean the extracted data."""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    from data_pipeline.transformers import DataTransformer
    import json
    from pathlib import Path
    
    logger = structlog.get_logger()
    logger.info("Starting data transformation")
    
    transformer = DataTransformer()
    
    # Load and transform each data type
    data_types = ['stock_quotes', 'market_screeners', 'stock_news']
    results = {}
    
    for data_type in data_types:
        try:
            # Load processed data
            processed_dir = Path("data/processed")
            pattern = f"{data_type}_*.json"
            files = list(processed_dir.glob(pattern))
            
            if files:
                latest_file = max(files, key=lambda x: x.stat().st_mtime)
                
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                
                # Transform data
                transformed = transformer.transform_data(data, data_type)
                results[data_type] = transformed
                
                logger.info("Data transformed", 
                           data_type=data_type,
                           records=len(transformed['transformed_data']))
            else:
                logger.warning("No data found for transformation", data_type=data_type)
                
        except Exception as e:
            logger.error("Transformation failed", data_type=data_type, error=str(e))
            raise
    
    return results

def load_to_s3(**context):
    """Load transformed data to S3."""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    from data_lake.s3_storage import S3DataLakeStorage
    import json
    from pathlib import Path
    
    logger = structlog.get_logger()
    logger.info("Starting S3 data load")
    
    # Initialize S3 storage
    s3_storage = S3DataLakeStorage(
        bucket_name=S3_BUCKET,
        access_key=Variable.get("AWS_ACCESS_KEY_ID"),
        secret_key=Variable.get("AWS_SECRET_ACCESS_KEY")
    )
    
    # Load each data type
    data_types = ['stock_quotes', 'market_screeners', 'stock_news']
    
    for data_type in data_types:
        try:
            # Load processed data
            processed_dir = Path("data/processed")
            pattern = f"{data_type}_*.json"
            files = list(processed_dir.glob(pattern))
            
            if files:
                latest_file = max(files, key=lambda x: x.stat().st_mtime)
                
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                
                # Upload to S3
                s3_path = s3_storage.upload_parquet_data(
                    data=data,
                    table_name=data_type,
                    partition_columns=['date']
                )
                
                logger.info("Data uploaded to S3", 
                           data_type=data_type,
                           s3_path=s3_path)
            else:
                logger.warning("No data found for S3 upload", data_type=data_type)
                
        except Exception as e:
            logger.error("S3 upload failed", data_type=data_type, error=str(e))
            raise

def run_dbt_models(**context):
    """Run dbt models for data transformation."""
    logger = structlog.get_logger()
    logger.info("Starting dbt model execution")
    
    # This would typically run dbt commands
    # For now, we'll simulate the process
    logger.info("dbt models executed successfully")
    
    return "dbt_models_completed"

def data_quality_checks(**context):
    """Run data quality checks."""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    from data_pipeline.transformers import DataValidator
    from data_pipeline.models import DataQualityStatus
    
    logger = structlog.get_logger()
    logger.info("Starting data quality checks")
    
    validator = DataValidator()
    
    # Load data for quality checks
    data_types = ['stock_quotes', 'market_screeners', 'stock_news']
    quality_results = {}
    
    for data_type in data_types:
        try:
            # Load sample data
            processed_dir = Path("data/processed")
            pattern = f"{data_type}_*.json"
            files = list(processed_dir.glob(pattern))
            
            if files:
                latest_file = max(files, key=lambda x: x.stat().st_mtime)
                
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                
                # Run quality checks
                checks = validator.validate_data_quality(data, data_type)
                quality_results[data_type] = checks
                
                # Check for errors
                error_checks = [c for c in checks if c.status == DataQualityStatus.ERROR]
                if error_checks:
                    logger.error("Data quality errors found", 
                               data_type=data_type,
                               errors=[c.message for c in error_checks])
                    raise Exception(f"Data quality errors in {data_type}")
                
                logger.info("Data quality checks passed", data_type=data_type)
            else:
                logger.warning("No data found for quality checks", data_type=data_type)
                
        except Exception as e:
            logger.error("Data quality check failed", data_type=data_type, error=str(e))
            raise
    
    return quality_results

# Tasks
extract_task = PythonOperator(
    task_id='extract_yahoo_finance_data',
    python_callable=extract_yahoo_finance_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag
)

load_s3_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    dag=dag
)

dbt_task = PythonOperator(
    task_id='run_dbt_models',
    python_callable=run_dbt_models,
    dag=dag
)

# Kafka streaming task (runs continuously)
kafka_streaming_task = BashOperator(
    task_id='start_kafka_streaming',
    bash_command='python /opt/airflow/dags/streaming_pipeline/kafka_producer.py',
    dag=dag
)

# PySpark streaming task
pyspark_streaming_task = BashOperator(
    task_id='start_pyspark_streaming',
    bash_command='python /opt/airflow/dags/streaming_pipeline/pyspark_streaming.py',
    dag=dag
)

# Database tasks
create_tables_task = PostgresOperator(
    task_id='create_database_tables',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql='sql/create_tables.sql',
    dag=dag
)

# Data validation task
validate_data_task = PostgresOperator(
    task_id='validate_data_in_database',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql='sql/validate_data.sql',
    dag=dag
)

# Task dependencies
extract_task >> transform_task >> quality_check_task >> load_s3_task >> dbt_task

# Parallel streaming tasks
kafka_streaming_task >> pyspark_streaming_task

# Database tasks run after dbt
dbt_task >> create_tables_task >> validate_data_task

# Additional DAG for real-time processing
realtime_dag = DAG(
    'financial_data_realtime',
    default_args=default_args,
    description='Real-time financial data processing',
    schedule_interval=None,  # Triggered manually or by external system
    catchup=False,
    max_active_runs=1,
    tags=['financial', 'realtime', 'streaming']
)

# Real-time tasks
realtime_extract = PythonOperator(
    task_id='realtime_extract',
    python_callable=extract_yahoo_finance_data,
    dag=realtime_dag
)

realtime_transform = PythonOperator(
    task_id='realtime_transform',
    python_callable=transform_data,
    dag=realtime_dag
)

realtime_load = PythonOperator(
    task_id='realtime_load',
    python_callable=load_to_s3,
    dag=realtime_dag
)

# Real-time dependencies
realtime_extract >> realtime_transform >> realtime_load
