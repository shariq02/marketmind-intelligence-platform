# ====================================================================
# Quarterly SEC Filings DAG
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 22, 2026
# ====================================================================
# FILE: airflow/dags/quarterly_sec_filings.py
# Purpose: Fetch quarterly SEC filings for S&P 500
# ====================================================================
"""
Quarterly SEC Filings DAG

Fetches recent SEC filings (10-K, 10-Q, 8-K) for S&P 500 tickers.

Schedule: Manual trigger (run during earnings seasons)
Runtime: ~20-30 minutes

Filing types:
- 10-K: Annual reports
- 10-Q: Quarterly reports
- 8-K: Current reports

Flow:
1. Fetch recent filings from EdgarTools
2. Send to Kafka (filings.metadata.v1)
3. Run Bronze Writer (Kafka to Parquet)
4. Run Silver Transformer (categorization + enrichment)
5. Run Gold Loader (PostgreSQL)
6. Record pipeline audit

Features:
- Quarterly batch processing
- Deduplication by accession number
- Filing categorization
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
    'execution_timeout': timedelta(minutes=45),
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def fetch_sec_filings(**context):
    """
    Fetch recent SEC filings for ticker universe.
    
    Uses bootstrap_sec_filings.py.
    """
    import subprocess
    
    execution_date = context['execution_date']
    
    logger.info(f"Fetching SEC filings for Q{((execution_date.month-1)//3)+1} {execution_date.year}")
    
    # Build ticker list
    tickers = ','.join(TICKER_UNIVERSE)
    
    # Call bootstrap script
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'bootstrap_sec_filings.py'),
        '--tickers', tickers,
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Bootstrap output: {result.stdout}")
        
        # Store execution quarter in XCom
        quarter = ((execution_date.month-1)//3) + 1
        context['task_instance'].xcom_push(
            key='target_quarter', 
            value=f"{execution_date.year}-Q{quarter}"
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
    """Run Silver Transformer for SEC filings."""
    import subprocess
    
    logger.info("Running Silver Transformer for SEC filings")
    
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
    """Run Gold Loader for SEC filings."""
    import subprocess
    
    logger.info("Running Gold Loader for SEC filings")
    
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
    from datetime import datetime, timezone
    import uuid
    import psycopg2
    from config import DATABASE_CONFIG
    
    target_quarter = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='target_quarter'
    )
    
    # Calculate metrics
    start_time = context['dag_run'].start_date
    end_time = datetime.now(timezone.utc)
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
            FROM gold.sec_filings
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
                records_retrieved,
                records_written,
                is_success
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            'edgartools_sec_filings',
            'BATCH',
            'SUCCESS',
            start_time,
            end_time,
            duration,
            len(TICKER_UNIVERSE),
            records_written,
            True
        ))
        
        conn.commit()
        logger.info(f"Pipeline audit recorded: {records_written} SEC filings loaded")
        
    finally:
        conn.close()


# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    'quarterly_sec_filings',
    default_args=DEFAULT_ARGS,
    description='Fetch quarterly SEC filings (10-K, 10-Q, 8-K)',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['sec_filings', 'incremental', 'quarterly', 'manual'],
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
    python_callable=fetch_sec_filings,
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
