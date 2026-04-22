# ====================================================================
# Monthly Macro Indicators DAG
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 22, 2026
# ====================================================================
# FILE: airflow/dags/monthly_macro_indicators.py
# Purpose: Fetch monthly US macroeconomic indicators
# ====================================================================
"""
Monthly Macro Indicators DAG

Fetches US macroeconomic indicators from AkShare.

Schedule: 1st of each month at 3:00 AM
Runtime: ~5 minutes

Data sources:
- CPI (Consumer Price Index)
- Unemployment Rate
- ADP Employment
- Core CPI
- Federal Funds Rate

Flow:
1. Fetch indicators from AkShare
2. Send to Kafka (macro.indicators.v1)
3. Run Bronze Writer (Kafka to Parquet)
4. Run Silver Transformer (quality checks + calculations)
5. Run Gold Loader (PostgreSQL)
6. Record pipeline audit

Features:
- Monthly batch processing
- All indicators fetched in parallel
- Automatic retry on failure
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

from config import get_logger

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
    'execution_timeout': timedelta(minutes=15),
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def fetch_macro_indicators(**context):
    """
    Fetch macro indicators from AkShare.
    
    Uses bootstrap_macro_indicators.py.
    """
    import subprocess
    
    execution_date = context['execution_date']
    
    logger.info(f"Fetching macro indicators for {execution_date.strftime('%Y-%m')}")
    
    # Call bootstrap script
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'bootstrap_macro_indicators.py'),
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Bootstrap output: {result.stdout}")
        
        # Store execution month in XCom
        context['task_instance'].xcom_push(
            key='target_month', 
            value=execution_date.strftime('%Y-%m')
        )
        
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
    """Run Silver Transformer for macro indicators."""
    import subprocess
    
    logger.info("Running Silver Transformer for macro indicators")
    
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
    """Run Gold Loader for macro indicators."""
    import subprocess
    
    logger.info("Running Gold Loader for macro indicators")
    
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
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=envs)
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
    
    target_month = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='target_month'
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
            FROM gold.macro_indicators
        """)
        
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
                records_written,
                is_success
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            'akshare_macro_indicators',
            'airflow_monthly',
            'SUCCESS',
            start_time,
            end_time,
            duration,
            records_written,
            True
        ))
        
        conn.commit()
        logger.info(f"Pipeline audit recorded: {records_written} macro indicators loaded")
        
    finally:
        conn.close()


# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    'monthly_macro_indicators',
    default_args=DEFAULT_ARGS,
    description='Fetch monthly US macroeconomic indicators',
    schedule_interval='0 3 1 * *',  # 3 AM on 1st of each month
    start_date=days_ago(1),
    catchup=False,
    tags=['macro_indicators', 'incremental', 'monthly'],
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
    python_callable=fetch_macro_indicators,
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
