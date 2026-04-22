# ====================================================================
# Weekly Corporate Actions DAG
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 22, 2026
# ====================================================================
# FILE: airflow/dags/weekly_corporate_actions.py
# Purpose: Fetch weekly splits and dividends for S&P 500
# ====================================================================
"""
Weekly Corporate Actions DAG

Fetches last week's stock splits and dividends for S&P 500 tickers.

Schedule: Every Sunday at 2:00 AM
Runtime: ~10-15 minutes

Flow:
1. Fetch last week's splits/dividends from Polygon API
2. Send to Kafka (market.corporate_actions.v1)
3. Run Bronze Writer (Kafka to Parquet)
4. Run Silver Transformer (quality checks + validation)
5. Run Gold Loader (PostgreSQL)
6. Record pipeline audit

Features:
- Weekly batch processing
- Deduplication handling
- Rate limiting compliance
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config import get_logger, TICKER_UNIVERSE

logger = get_logger(__name__)

# ====================================================================
# DAG CONFIGURATION
# ====================================================================

DEFAULT_ARGS = {
    'owner': 'marketmind',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def fetch_corporate_actions(**context):
    """
    Fetch last week's corporate actions for ticker universe.
    
    Uses bootstrap_corporate_actions.py with last week's date range.
    """
    import subprocess
    
    execution_date = context['execution_date']
    
    # Get last week's date range (Monday to Sunday)
    last_monday = execution_date - timedelta(days=execution_date.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime('%Y-%m-%d')
    end_date = last_sunday.strftime('%Y-%m-%d')
    
    logger.info(f"Fetching corporate actions for {start_date} to {end_date}")
    
    # Build ticker list
    tickers = ','.join(TICKER_UNIVERSE)
    
    # Call bootstrap script with last week's range
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'bootstrap_corporate_actions.py'),
        '--tickers', tickers,
        '--start-date', start_date,
        '--end-date', end_date,
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Bootstrap output: {result.stdout}")
        
        # Store date range in XCom
        context['task_instance'].xcom_push(key='start_date', value=start_date)
        context['task_instance'].xcom_push(key='end_date', value=end_date)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Bootstrap failed: {e.stderr}")
        raise


def run_bronze_writer(**context):
    """Run Bronze Writer to consume Kafka messages."""
    import subprocess
    
    cmd = [
        'python3.11',
        str(project_root / 'code' / 'bronze' / 'writers' / 'bronze_writer.py')
    ]

    # Add project root to PYTHONPATH
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)
    
    logger.info("Running Bronze Writer")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        logger.info(f"Bronze Writer output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Bronze Writer failed: {e.stderr}")
        raise


def run_silver_transformer(**context):
    """Run Silver Transformer for corporate actions."""
    import subprocess
    
    logger.info("Running Silver Transformer for corporate actions")
    
    # Call orchestration script with silver layer only
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'orchestrate_pipeline.py'),
        '--layer', 'silver'
    ]

    # Add project root to PYTHONPATH
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        logger.info(f"Silver Transformer output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Silver Transformer failed: {e.stderr}")
        raise


def run_gold_loader(**context):
    """Run Gold Loader for corporate actions."""
    import subprocess
    
    logger.info("Running Gold Loader for corporate actions")
    
    # Call orchestration script with gold layer only
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'orchestrate_pipeline.py'),
        '--layer', 'gold'
    ]

    # Add project root to PYTHONPATH
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        logger.info(f"Gold Loader output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Gold Loader failed: {e.stderr}")
        raise


def record_pipeline_audit(**context):
    """Record pipeline execution to audit table."""
    from datetime import datetime
    import uuid
    import psycopg2
    from config import DATABASE_CONFIG
    
    start_date = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='start_date'
    )
    end_date = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='end_date'
    )
    
    # Calculate metrics
    start_time = context['dag_run'].start_date
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Insert audit record
    conn = psycopg2.connect(
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port'],
        dbname=DATABASE_CONFIG['database'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
    )
    
    try:
        cur = conn.cursor()
        
        # Count records loaded
        cur.execute("""
            SELECT COUNT(*) 
            FROM gold.corporate_actions 
            WHERE (execution_date BETWEEN %s AND %s)
               OR (ex_dividend_date BETWEEN %s AND %s)
        """, (start_date, end_date, start_date, end_date))
        
        records_written = cur.fetchone()[0]
        
        # Insert audit record
        cur.execute("""
            INSERT INTO gold.pipeline_audit (
                audit_id,
                connector,
                execution_mode,
                status,
                start_datetime,
                end_datetime,
                duration_seconds,
                records_retrieved,
                records_written,
                is_success
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            'polygon_corporate_actions',
            'airflow_weekly',
            'SUCCESS',
            start_time,
            end_time,
            duration,
            len(TICKER_UNIVERSE),
            records_written,
            True
        ))
        
        conn.commit()
        logger.info(f"Pipeline audit recorded: {records_written} corporate actions loaded")
        
    finally:
        conn.close()


# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    'weekly_corporate_actions',
    default_args=DEFAULT_ARGS,
    description='Fetch weekly corporate actions (splits/dividends)',
    schedule_interval='0 2 * * 0',  # 2 AM every Sunday
    start_date=days_ago(1),
    catchup=False,
    tags=['corporate_actions', 'incremental', 'weekly'],
)

# ====================================================================
# TASK DEFINITIONS
# ====================================================================

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_corporate_actions,
    dag=dag,
)

bronze_writer = PythonOperator(
    task_id='bronze_writer',
    python_callable=run_bronze_writer,
    dag=dag,
)

silver_transformer = PythonOperator(
    task_id='silver_transformer',
    python_callable=run_silver_transformer,
    dag=dag,
)

gold_loader = PythonOperator(
    task_id='gold_loader',
    python_callable=run_gold_loader,
    dag=dag,
)

audit = PythonOperator(
    task_id='record_audit',
    python_callable=record_pipeline_audit,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ====================================================================
# TASK DEPENDENCIES
# ====================================================================

start >> fetch_data >> bronze_writer >> silver_transformer >> gold_loader >> audit >> end
