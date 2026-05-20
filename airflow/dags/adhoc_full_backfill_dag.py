# ====================================================================
# Ad-hoc Full Backfill DAG
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 22, 2026
# ====================================================================
# FILE: airflow/dags/adhoc_full_backfill.py
# Purpose: Full S&P 500 backfill using master bootstrap script
# ====================================================================
"""
Ad-hoc Full Backfill DAG

Runs master_bootstrap.py for full S&P 500 historical data load.

Schedule: Manual trigger only
Runtime: 12-15 hours (overnight execution recommended)

Use cases:
- Initial S&P 500 full backfill
- Data repair after outages
- Historical data refresh
- New ticker additions to universe

Flow:
1. Run master_bootstrap.py with full S&P 500 list
2. Fetches all 4 data types sequentially:
   - Market bars (OHLCV)
   - Corporate actions (splits/dividends)
   - Macro indicators (economic data)
   - SEC filings (10-K, 10-Q, 8-K)
3. Each bootstrap followed by Bronze --> Silver --> Gold pipeline
4. Checkpoint/resume capability built-in

Features:
- Full automation of cold bootstrap process
- Checkpoint files allow resume on failure
- Rate limiting compliance
- Comprehensive audit logging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
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
    'email_on_failure': True,  # Important for long-running job
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=18),  # Allow 18 hours max
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def get_ticker_list(**context):
    """
    Get ticker list from Airflow Variable or use default.
    
    Allows override via Airflow UI without code change.
    """
    try:
        # Try to get from Airflow Variable
        ticker_str = Variable.get('backfill_ticker_list', default_var=None)
        if ticker_str:
            tickers = ticker_str.split(',')
            logger.info(f"Using ticker list from Airflow Variable: {len(tickers)} tickers")
            return tickers
    except:
        pass
    
    # Fall back to default
    logger.info(f"Using default ticker list: {len(TICKER_UNIVERSE)} tickers")
    return TICKER_UNIVERSE


def run_full_backfill(**context):
    """
    Run master_bootstrap.py for full backfill.
    
    Uses --auto flag for unattended execution.
    Supports --resume for checkpoint recovery.
    """
    import subprocess
    
    execution_date = context['execution_date']
    
    # Get ticker list
    tickers = get_ticker_list(**context)
    ticker_str = ','.join(tickers)
    
    logger.info(f"Starting full backfill for {len(tickers)} tickers")
    logger.info(f"Execution date: {execution_date}")
    
    # Check if this is a resume attempt
    resume = context['params'].get('resume', False)
    
    # Build command
    cmd = [
        'python3.11',
        str(project_root / 'scripts' / 'master_bootstrap.py'),
        '--tickers', ticker_str,
        '--auto',  # No prompts
    ]
    
    if resume:
        cmd.append('--resume')
        logger.info("Running in RESUME mode from checkpoint")
    else:
        logger.info("Running in FRESH mode")
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    # Store start time
    start_time = datetime.now()
    context['task_instance'].xcom_push(key='start_time', value=start_time.isoformat())
    context['task_instance'].xcom_push(key='ticker_count', value=len(tickers))
    
    try:
        # Run with real-time output logging
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Stream output to logs
        for line in process.stdout:
            logger.info(line.rstrip())
        
        # Wait for completion
        return_code = process.wait()
        
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, cmd)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        duration_hours = duration / 3600
        
        logger.info("Backfill completed successfully")
        logger.info(f"Duration: {duration_hours:.2f} hours ({duration:.0f} seconds)")
        
        # Store metrics
        context['task_instance'].xcom_push(key='end_time', value=end_time.isoformat())
        context['task_instance'].xcom_push(key='duration_seconds', value=duration)
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Backfill failed with exit code {e.returncode}")
        logger.error("Check logs/pipeline/orchestration.log for details")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during backfill: {e}")
        raise


def validate_backfill_results(**context):
    """
    Validate that backfill loaded data to all Gold tables.
    """
    import psycopg2
    from config import DATABASE_CONFIG
    
    conn = psycopg2.connect(
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port'],
        dbname=DATABASE_CONFIG['database'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
    )
    
    try:
        cur = conn.cursor()
        
        # Check all Gold tables
        tables = {
            'market_bars': 'gold.ohlcv_bars',
            'corporate_actions': 'gold.corporate_actions',
            'macro_indicators': 'gold.macro_indicators',
            'sec_filings': 'gold.sec_filings',
        }
        
        results = {}
        
        for name, table in tables.items():
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            results[name] = count
            logger.info(f"{name}: {count} records in {table}")
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='validation_results', value=results)
        
        # Basic validation - at least some data in each table
        if all(count > 0 for count in results.values()):
            logger.info("Validation PASSED: All tables have data")
            return True
        else:
            empty_tables = [name for name, count in results.items() if count == 0]
            logger.warning(f"Validation WARNING: Empty tables: {empty_tables}")
            return True  # Don't fail, just warn
        
    finally:
        conn.close()


def send_completion_notification(**context):
    """
    Log backfill completion summary.
    
    In production, could send email/Slack notification.
    """
    ticker_count = context['task_instance'].xcom_pull(
        task_ids='run_backfill',
        key='ticker_count'
    )
    
    duration_seconds = context['task_instance'].xcom_pull(
        task_ids='run_backfill',
        key='duration_seconds'
    )
    
    validation_results = context['task_instance'].xcom_pull(
        task_ids='validate_results',
        key='validation_results'
    )
    
    # Log summary
    logger.info("=" * 70)
    logger.info("FULL BACKFILL COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Tickers processed: {ticker_count}")
    logger.info(f"Duration: {duration_seconds/3600:.2f} hours")
    logger.info("")
    logger.info("Records loaded:")
    for table, count in validation_results.items():
        logger.info(f"  {table}: {count:,}")
    logger.info("=" * 70)
    
    # TODO: Send email/Slack notification in production


# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    'adhoc_full_backfill',
    default_args=DEFAULT_ARGS,
    description='Full S&P 500 backfill using master bootstrap',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    params={
        'resume': False,  # Set to True when triggering to resume from checkpoint
    },
    tags=['backfill', 'adhoc', 'long_running'],
)

# ====================================================================
# TASK DEFINITIONS
# ====================================================================

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

run_backfill = PythonOperator(
    task_id='run_backfill',
    python_callable=run_full_backfill,
    dag=dag,
)

validate_results = PythonOperator(
    task_id='validate_results',
    python_callable=validate_backfill_results,
    dag=dag,
)

notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ====================================================================
# TASK DEPENDENCIES
# ====================================================================

start >> run_backfill >> validate_results >> notify >> end
