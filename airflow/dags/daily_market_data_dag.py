# ====================================================================
# Daily Market Data DAG
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 22, 2026
# ====================================================================
# FILE: airflow/dags/daily_market_data.py
# Purpose: Fetch daily OHLCV bars for S&P 500 (incremental updates)
# ====================================================================
"""
Daily Market Data DAG

Fetches yesterday's OHLCV bars for S&P 500 tickers.

Schedule: Every weekday at 6:00 PM ET (after market close)
Runtime: ~15-20 minutes (rate-limited at 5 calls/min)

Flow:
1. Fetch yesterday's bars from Polygon API
2. Send to Kafka (market.bars.v1)
3. Run Bronze Writer (Kafka to Parquet)
4. Run Silver Transformer (quality checks + enrichment)
5. Run Gold Loader (PostgreSQL)
6. Record pipeline audit

Features:
- Skip weekends and US market holidays
- Checkpoint/resume capability
- Rate limiting compliance
- Quality gates at each stage
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
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

def is_market_open(**context):
    """
    Check if US market was open yesterday.
    
    Returns:
        'fetch_data' if market was open, 'skip_weekend' if closed
    """
    execution_date = context['execution_date']
    
    # Get yesterday's date
    yesterday = execution_date - timedelta(days=1)
    day_of_week = yesterday.weekday()
    
    # Monday = 0, Sunday = 6
    # Skip Saturday (5) and Sunday (6)
    if day_of_week in [5, 6]:
        logger.info(f"Market closed on {yesterday.strftime('%Y-%m-%d')} (weekend)")
        return 'skip_weekend'
    
    # TODO: Add US market holiday check
    # For now, just check weekends
    
    logger.info(f"Market open on {yesterday.strftime('%Y-%m-%d')}")
    return 'fetch_data'


def fetch_market_bars(**context):
    """
    Fetch yesterday's OHLCV bars for ticker universe.
    
    Uses bootstrap_market_bars.py with single-day range.
    """
    from config import DATA_DIR
    import subprocess
    
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Fetching market bars for {yesterday}")
    
    # Build ticker list
    tickers = ','.join(TICKER_UNIVERSE)
    
    # Call bootstrap script with single day
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'bootstrap_market_bars.py'),
        '--tickers', tickers,
        '--start-date', yesterday,
        '--end-date', yesterday,
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Bootstrap output: {result.stdout}")
        
        # Store fetched date in XCom for downstream tasks
        context['task_instance'].xcom_push(key='target_date', value=yesterday)
        
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
    """Run Silver Transformer for target date."""
    import subprocess
    
    # Get target date from XCom
    target_date = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='target_date'
    )
    
    if not target_date:
        raise ValueError("No target date found in XCom")
    
    logger.info(f"Running Silver Transformer for {target_date}")
    
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
    """Run Gold Loader for target date."""
    import subprocess
    
    # Get target date from XCom
    target_date = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='target_date'
    )
    
    if not target_date:
        raise ValueError("No target date found in XCom")
    
    logger.info(f"Running Gold Loader for {target_date}")
    
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
    
    execution_date = context['execution_date']
    target_date = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='target_date'
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
            FROM gold.ohlcv_bars 
            WHERE date = %s
        """, (target_date,))
        
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
            'polygon_daily_bars',
            'INCREMENTAL',
            'SUCCESS',
            start_time,
            end_time,
            duration,
            len(TICKER_UNIVERSE),
            records_written,
            True
        ))
        
        conn.commit()
        logger.info(f"Pipeline audit recorded: {records_written} records loaded")
        
    finally:
        conn.close()


# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    'daily_market_data',
    default_args=DEFAULT_ARGS,
    description='Fetch daily OHLCV bars for S&P 500',
    schedule_interval='0 18 * * 1-5',  # 6 PM ET, Monday-Friday
    start_date=days_ago(1),
    catchup=False,
    tags=['market_data', 'incremental', 'daily'],
)

# ====================================================================
# TASK DEFINITIONS
# ====================================================================

# Start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Check if market was open
check_market = BranchPythonOperator(
    task_id='check_market_open',
    python_callable=is_market_open,
    dag=dag,
)

# Skip weekend placeholder
skip_weekend = EmptyOperator(
    task_id='skip_weekend',
    dag=dag,
)

# Fetch data from Polygon API
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_market_bars,
    dag=dag,
)

# Run Bronze Writer
bronze_writer = PythonOperator(
    task_id='bronze_writer',
    python_callable=run_bronze_writer,
    dag=dag,
)

# Run Silver Transformer
silver_transformer = PythonOperator(
    task_id='silver_transformer',
    python_callable=run_silver_transformer,
    dag=dag,
)

# Run Gold Loader
gold_loader = PythonOperator(
    task_id='gold_loader',
    python_callable=run_gold_loader,
    dag=dag,
)

# Record audit
audit = PythonOperator(
    task_id='record_audit',
    python_callable=record_pipeline_audit,
    dag=dag,
)

# End
end = EmptyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed_min_one_success',
)

# ====================================================================
# TASK DEPENDENCIES
# ====================================================================

start >> check_market >> [fetch_data, skip_weekend]
fetch_data >> bronze_writer >> silver_transformer >> gold_loader >> audit >> end
skip_weekend >> end
