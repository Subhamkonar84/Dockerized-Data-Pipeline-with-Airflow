import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, "/opt/airflow/scripts")

from fetch_stock_data import StockETL

# Stock symbol (default: IBM if not provided via ENV)
SYMBOL = os.getenv("STOCK_SYMBOL", "IBM")
FREQ = os.getenv("SCHEDULE_INTERVAL")

# Configure logger
logger = logging.getLogger("airflow.task")

def extract_task(**context):
    """
    Extract raw stock data from Yahoo Finance using StockETL.
    Push raw JSON data to XCom for downstream tasks.
    """
    try:
        etl = StockETL(SYMBOL)
        logger.info(f"Starting extraction for symbol: {SYMBOL}")
        raw_data = etl.extract()

        if not raw_data:
            logger.warning("No data extracted. Skipping further tasks.")
            context['ti'].xcom_push(key="raw_data", value=None)
            return

        context['ti'].xcom_push(key="raw_data", value=json.dumps(raw_data))
        logger.info("Extraction completed successfully.")

    except Exception as e:
        logger.error(f"Error in extract_task: {str(e)}", exc_info=True)
        # Push None to XCom so downstream can handle gracefully
        context['ti'].xcom_push(key="raw_data", value=None)
        raise

def transform_task(**context):
    """
    Transform raw stock data into structured record(s).
    Pulls raw JSON data from XCom.
    """
    try:
        raw_json = context['ti'].xcom_pull(task_ids="extract", key="raw_data")

        if not raw_json:
            logger.warning("No raw data found in XCom. Skipping transform task.")
            context['ti'].xcom_push(key="record", value=None)
            return

        raw_data = json.loads(raw_json)
        etl = StockETL(SYMBOL)
        logger.info("Starting transformation.")

        record = etl.transform(raw_data)
        
        if not record:
            logger.warning("Transformation returned empty result.")
            context['ti'].xcom_push(key="record", value=None)
            return

        context['ti'].xcom_push(key="record", value=record)
        logger.info("Transformation completed successfully.")

    except Exception as e:
        logger.error(f"Error in transform_task: {str(e)}", exc_info=True)
        context['ti'].xcom_push(key="record", value=None)
        raise

def load_task(**context):
    """
    Load transformed stock data into PostgreSQL.
    Pulls record from XCom.
    """
    try:
        record = context['ti'].xcom_pull(task_ids="transform", key="record")

        if not record:
            logger.warning("No transformed record found. Skipping load.")
            return

        etl = StockETL(SYMBOL)
        pg_hook = PostgresHook(postgres_conn_id="stocks_postgres")
        logger.info("Starting load into PostgreSQL.")

        etl.load(record, pg_hook)

        logger.info(f"Successfully loaded record for {SYMBOL} into Postgres.")

    except Exception as e:
        logger.error(f"Error in load_task: {str(e)}", exc_info=True)
        raise


# -------------------------
# Define the Airflow DAG
# -------------------------
with DAG(
    dag_id="stock_data_etl",
    default_args={"owner": "airflow"},
    description="Robust ETL pipeline for stock prices from Yahoo Finance to PostgreSQL",
    schedule_interval=FREQ,  #(adjust as needed)
    start_date=days_ago(1),
    catchup=False,
    tags=["stocks", "yahoo finance", "ETL"],
    max_active_runs=1,
) as dag:

    extract_op = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load_op = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    # Task dependencies
    extract_op >> transform_op >> load_op