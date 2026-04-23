# ====================================================================
# Configuration Settings for MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026 (Updated with enhanced logging)
# ====================================================================
# FILE: config.py (Project Root)
# Purpose: Centralize all configuration settings
# ====================================================================
"""
Configuration settings for MarketMind Intelligence Platform V1.
ALL SENSITIVE DATA IN .env FILE - NEVER COMMIT .env TO GITHUB

This file manages:
- API credentials (Polygon, EdgarTools)
- Kafka connection settings
- PostgreSQL connection
- File paths and directory structure
- Rate limiting configuration
- Enhanced layer-specific logging

Usage:
    from config import get_database_url, POLYGON_API_KEY, KAFKA_CONFIG
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# Load environment variables first
load_dotenv()

# ====================================================================
# PROJECT ROOT
# ====================================================================
PROJECT_ROOT = Path(__file__).parent

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================

# Code directories
CODE_DIR = PROJECT_ROOT / "code"
BRONZE_DIR = CODE_DIR / "bronze"
SILVER_DIR = CODE_DIR / "silver"
GOLD_DIR = CODE_DIR / "gold"

# Bronze layer
BRONZE_CONNECTORS_DIR = BRONZE_DIR / "connectors"
BRONZE_PRODUCERS_DIR = BRONZE_DIR / "producers"
BRONZE_WRITERS_DIR = BRONZE_DIR / "writers"
BRONZE_SCHEMAS_DIR = BRONZE_DIR / "schemas"
BRONZE_SCHEMAS_AVRO_DIR = BRONZE_SCHEMAS_DIR / "avro"

# Silver layer
SILVER_TRANSFORMATIONS_DIR = SILVER_DIR / "transformations"
SILVER_QUALITY_DIR = SILVER_DIR / "quality"

# Gold layer
GOLD_LOADERS_DIR = GOLD_DIR / "loaders"
GOLD_AGGREGATIONS_DIR = GOLD_DIR / "aggregations"
GOLD_SNAPSHOTS_DIR = GOLD_DIR / "snapshots"
GOLD_SCHEMAS_DIR = GOLD_DIR / "schemas"
GOLD_MIGRATIONS_DIR = GOLD_DIR / "migrations"

# Data directories (gitignored)
DATA_DIR = PROJECT_ROOT / "data"
DATA_BRONZE_DIR = DATA_DIR / "bronze"
DATA_SILVER_DIR = DATA_DIR / "silver"
DATA_GOLD_SNAPSHOTS_DIR = DATA_DIR / "gold_snapshots"

# Airflow
AIRFLOW_DIR = PROJECT_ROOT / "airflow"
AIRFLOW_DAGS_DIR = AIRFLOW_DIR / "dags"
AIRFLOW_UTILS_DIR = AIRFLOW_DIR / "utils"
AIRFLOW_CONFIG_DIR = AIRFLOW_DIR / "config"

# Logs - Enhanced with layer-specific directories
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_BRONZE_DIR = LOGS_DIR / "bronze"
LOGS_SILVER_DIR = LOGS_DIR / "silver"
LOGS_GOLD_DIR = LOGS_DIR / "gold"
LOGS_AIRFLOW_DIR = LOGS_DIR / "airflow"
LOGS_KAFKA_DIR = LOGS_DIR / "kafka"
LOGS_PIPELINE_DIR = LOGS_DIR / "pipeline"

# Tests
TESTS_DIR = PROJECT_ROOT / "tests"
TESTS_UNIT_DIR = TESTS_DIR / "unit"
TESTS_INTEGRATION_DIR = TESTS_DIR / "integration"
TESTS_SCHEMAS_DIR = TESTS_DIR / "schemas"

# Notebooks
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

# Docs
DOCS_DIR = PROJECT_ROOT / "docs"

# Scripts
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# Checkpoints
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"

# Auto-create all directories on import
for directory in [
    BRONZE_CONNECTORS_DIR,
    BRONZE_PRODUCERS_DIR,
    BRONZE_WRITERS_DIR,
    BRONZE_SCHEMAS_DIR,
    BRONZE_SCHEMAS_AVRO_DIR,
    SILVER_TRANSFORMATIONS_DIR,
    SILVER_QUALITY_DIR,
    GOLD_LOADERS_DIR,
    GOLD_AGGREGATIONS_DIR,
    GOLD_SNAPSHOTS_DIR,
    GOLD_SCHEMAS_DIR,
    GOLD_MIGRATIONS_DIR,
    DATA_BRONZE_DIR,
    DATA_SILVER_DIR,
    DATA_GOLD_SNAPSHOTS_DIR,
    AIRFLOW_DAGS_DIR,
    AIRFLOW_UTILS_DIR,
    AIRFLOW_CONFIG_DIR,
    LOGS_BRONZE_DIR,
    LOGS_SILVER_DIR,
    LOGS_GOLD_DIR,
    LOGS_AIRFLOW_DIR,
    LOGS_KAFKA_DIR,
    LOGS_PIPELINE_DIR,
    TESTS_UNIT_DIR,
    TESTS_INTEGRATION_DIR,
    TESTS_SCHEMAS_DIR,
    NOTEBOOKS_DIR,
    DOCS_DIR,
    SCRIPTS_DIR,
    CHECKPOINTS_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# API CREDENTIALS - ALL FROM .env
# ====================================================================

# Polygon.io API
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
POLYGON_BASE_URL = os.getenv('POLYGON_BASE_URL', 'https://api.polygon.io')

# EdgarTools - requires identity for SEC requests
EDGAR_USER_IDENTITY = os.getenv('EDGAR_USER_IDENTITY')  # Format: "Name email@domain.com"

# Validation
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY must be set in .env file")

if not EDGAR_USER_IDENTITY:
    raise ValueError("EDGAR_USER_IDENTITY must be set in .env file (format: 'Name email@domain.com')")

# ====================================================================
# RATE LIMITING CONFIGURATION
# ====================================================================

# Polygon.io free tier: 5 calls/minute
POLYGON_RATE_LIMIT = {
    'calls_per_minute': int(os.getenv('POLYGON_RATE_LIMIT_CALLS', 5)),
    'calls_per_day': int(os.getenv('POLYGON_RATE_LIMIT_DAY', 7200)),
    'retry_attempts': int(os.getenv('POLYGON_RETRY_ATTEMPTS', 3)),
    'retry_delay_seconds': int(os.getenv('POLYGON_RETRY_DELAY', 60)),
}

# AkShare - no official rate limit, but be respectful
AKSHARE_RATE_LIMIT = {
    'calls_per_minute': int(os.getenv('AKSHARE_RATE_LIMIT_CALLS', 10)),
    'retry_attempts': int(os.getenv('AKSHARE_RETRY_ATTEMPTS', 3)),
    'retry_delay_seconds': int(os.getenv('AKSHARE_RETRY_DELAY', 30)),
}

# EdgarTools - SEC allows reasonable scraping, limit to avoid blocking
EDGAR_RATE_LIMIT = {
    'calls_per_minute': int(os.getenv('EDGAR_RATE_LIMIT_CALLS', 10)),
    'retry_attempts': int(os.getenv('EDGAR_RETRY_ATTEMPTS', 3)),
    'retry_delay_seconds': int(os.getenv('EDGAR_RETRY_DELAY', 30)),
}

# ====================================================================
# KAFKA CONFIGURATION - ALL FROM .env
# ====================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client_id': os.getenv('KAFKA_CLIENT_ID', 'marketmind-v1'),
    'compression_type': os.getenv('KAFKA_COMPRESSION_TYPE', 'gzip'),
    'max_in_flight_requests': int(os.getenv('KAFKA_MAX_IN_FLIGHT', 5)),
    'acks': os.getenv('KAFKA_ACKS', 'all'),
    'retries': int(os.getenv('KAFKA_RETRIES', 3)),
    'batch_size': int(os.getenv('KAFKA_BATCH_SIZE', 16384)),
    'linger_ms': int(os.getenv('KAFKA_LINGER_MS', 10)),
    'queue.buffering.max.messages': int(os.getenv('KAFKA_QUEUE_BUFFER_MAX_MESSAGES', 100000)),
}

# Kafka Topics
KAFKA_TOPICS = {
    'market_bars': 'market.bars.v1',
    'corporate_actions': 'market.corporate_actions.v1',
    'macro_indicators': 'macro.indicators.v1',
    'filings_metadata': 'filings.metadata.v1',
    'pipeline_audit': 'pipeline.audit.v1',
    'quality_alerts': 'quality.alerts.v1',
}

# Consumer Groups
KAFKA_CONSUMER_GROUPS = {
    'bronze_writer': 'bronze-writer-group',
    'silver_processor': 'silver-processor-group',
}

# ====================================================================
# DATABASE CONFIGURATION - ALL FROM .env
# ====================================================================

DATABASE_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'marketmind_v1'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD'),  # NO DEFAULT - MUST BE IN .env
}


def get_database_url() -> str:
    """Get PostgreSQL connection URL from environment variables."""
    if not DATABASE_CONFIG['password']:
        raise ValueError("POSTGRES_PASSWORD must be set in .env file")
    return (
        f"postgresql+psycopg2://{DATABASE_CONFIG['user']}:"
        f"{DATABASE_CONFIG['password']}@"
        f"{DATABASE_CONFIG['host']}:"
        f"{DATABASE_CONFIG['port']}/"
        f"{DATABASE_CONFIG['database']}"
    )


def get_ticker_universe():
    """
    Read ticker list from external CSV file.
    
    Returns:
        list: List of ticker symbols
        
    Fallback:
        Returns ['AAPL', 'MSFT', 'GOOGL'] if file doesn't exist
    """
    ticker_file = DATA_DIR / 'ticker_universe.csv'
    
    if ticker_file.exists():
        df = pd.read_csv(ticker_file)
        return df['ticker'].tolist()
    
    # Fallback if file missing
    return ['AAPL', 'MSFT', 'GOOGL']


# PostgreSQL Schemas
POSTGRES_SCHEMAS = {
    'bronze': 'bronze',
    'staging': 'staging',
    'gold': 'gold',
    'quality': 'quality',
    'snapshots': 'snapshots',
    'public': 'public',
}

# ====================================================================
# AIRFLOW CONFIGURATION
# ====================================================================

AIRFLOW_CONFIG = {
    'home': os.getenv('AIRFLOW_HOME', str(Path.home() / 'airflow')),
    'load_examples': os.getenv('AIRFLOW__CORE__LOAD_EXAMPLES', 'False'),
    'dags_dir': str(AIRFLOW_DAGS_DIR),
    'executor': os.getenv('AIRFLOW__CORE__EXECUTOR', 'LocalExecutor'),
    'sql_alchemy_conn': get_database_url(),  # Use PostgreSQL for Airflow metadata
}

# ====================================================================
# DATA SCOPE CONFIGURATION
# ====================================================================

# Cold bootstrap date range (Q1 2026)
DATA_SCOPE = {
    'start_date': os.getenv('DATA_START_DATE', '2026-01-01'),
    'end_date': os.getenv('DATA_END_DATE', '2026-03-31'),
    'ticker_universe_file': PROJECT_ROOT / 'data' / 'ticker_universe.csv',
}

# Ticker Universe - Read from external CSV file
TICKER_UNIVERSE = get_ticker_universe()

# Expected data volumes (for monitoring)
EXPECTED_VOLUMES = {
    'bronze_total_gb': 15,
    'silver_total_gb': 10,
    'gold_total_gb': 2,
    'ticker_count': 500,
    'days_count': 90,  # Q1 2026
}

# ====================================================================
# BRONZE LAYER CONFIGURATION
# ====================================================================

BRONZE_CONFIG = {
    # Kafka consumer batch settings
    'consumer_poll_timeout_ms': int(os.getenv('BRONZE_POLL_TIMEOUT', 1000)),
    'consumer_max_poll_records': int(os.getenv('BRONZE_MAX_POLL_RECORDS', 500)),
    'consumer_auto_offset_reset': os.getenv('BRONZE_AUTO_OFFSET_RESET', 'earliest'),
    
    # Parquet write settings
    'parquet_batch_size': int(os.getenv('BRONZE_PARQUET_BATCH_SIZE', 1000)),
    'parquet_write_interval_seconds': int(os.getenv('BRONZE_WRITE_INTERVAL', 60)),
    'parquet_compression': os.getenv('BRONZE_PARQUET_COMPRESSION', 'snappy'),
    
    # Checkpointing
    'checkpoint_table': 'checkpoints',
    'checkpoint_interval_seconds': int(os.getenv('BRONZE_CHECKPOINT_INTERVAL', 300)),
}

# ====================================================================
# SILVER LAYER CONFIGURATION
# ====================================================================

SILVER_CONFIG = {
    # Quality check thresholds
    'completeness_threshold': float(os.getenv('SILVER_COMPLETENESS_THRESHOLD', 0.95)),
    'freshness_threshold_hours': int(os.getenv('SILVER_FRESHNESS_THRESHOLD_HOURS', 24)),
    'uniqueness_threshold': float(os.getenv('SILVER_UNIQUENESS_THRESHOLD', 1.0)),
    
    # Transformation batch size
    'transformation_batch_size': int(os.getenv('SILVER_BATCH_SIZE', 10000)),
}

# ====================================================================
# GOLD LAYER CONFIGURATION
# ====================================================================

GOLD_CONFIG = {
    # Snapshot settings
    'snapshot_check_interval_hours': int(os.getenv('GOLD_SNAPSHOT_CHECK_INTERVAL', 24)),
    'snapshot_retention_days': int(os.getenv('GOLD_SNAPSHOT_RETENTION_DAYS', 90)),
    
    # Aggregation settings
    'aggregation_batch_size': int(os.getenv('GOLD_AGGREGATION_BATCH_SIZE', 50000)),
}

# ====================================================================
# ENHANCED LOGGING CONFIGURATION
# ====================================================================

LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    
    # Master logs
    'app_log': LOGS_DIR / 'app.log',
    'error_log': LOGS_DIR / 'error.log',
    
    # Bronze layer logs
    'bronze_connectors_log': LOGS_BRONZE_DIR / 'connectors.log',
    'bronze_producers_log': LOGS_BRONZE_DIR / 'producers.log',
    'bronze_writers_log': LOGS_BRONZE_DIR / 'writers.log',
    
    # Silver layer logs
    'silver_transformers_log': LOGS_SILVER_DIR / 'transformers.log',
    'silver_quality_log': LOGS_SILVER_DIR / 'quality.log',
    
    # Gold layer logs
    'gold_loaders_log': LOGS_GOLD_DIR / 'loaders.log',
    
    # Infrastructure logs
    'airflow_log': LOGS_AIRFLOW_DIR / 'dags.log',
    'kafka_log': LOGS_KAFKA_DIR / 'kafka.log',
    'pipeline_log': LOGS_PIPELINE_DIR / 'orchestration.log',
}


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger with layer-specific log files.
    
    Determines log file based on module name:
    - bronze.connectors.* --> logs/bronze/connectors.log
    - bronze.producers.* --> logs/bronze/producers.log
    - bronze.writers.* --> logs/bronze/writers.log
    - silver.transformations.* --> logs/silver/transformers.log
    - silver.quality.* --> logs/silver/quality.log
    - gold.loaders.* --> logs/gold/loaders.log
    - airflow.* --> logs/airflow/dags.log
    - kafka.* --> logs/kafka/kafka.log
    - orchestration/bootstrap --> logs/pipeline/orchestration.log
    
    All loggers also write to:
    - Console (stdout)
    - logs/app.log (master log)
    - logs/error.log (errors only)
    
    Usage:
        from config import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    level = getattr(logging, LOGGING_CONFIG['level'].upper(), logging.INFO)
    fmt = logging.Formatter(LOGGING_CONFIG['format'])

    # ================================================================
    # CONSOLE HANDLER - Always show on screen
    # ================================================================
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # ================================================================
    # MASTER APP LOG - Everything goes here (with rotation)
    # ================================================================
    app_handler = RotatingFileHandler(
        LOGGING_CONFIG['app_log'],
        maxBytes=10*1024*1024,  # 10 MB max size
        backupCount=5,  # Keep 5 backup files (app.log.1, app.log.2, etc.)
        encoding='utf-8'
    )
    app_handler.setLevel(level)
    app_handler.setFormatter(fmt)

    # ================================================================
    # ERROR LOG - Errors only
    # ================================================================
    error_handler = logging.FileHandler(LOGGING_CONFIG['error_log'], encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)

    # ================================================================
    # LAYER-SPECIFIC LOG - Based on module name
    # ================================================================
    layer_log_file = None
    
    # Bronze layer
    if 'bronze.connectors' in name or 'connectors.polygon' in name or 'connectors.akshare' in name or 'connectors.edgartools' in name:
        layer_log_file = LOGGING_CONFIG['bronze_connectors_log']
    elif 'bronze.producers' in name or 'kafka_producer' in name:
        layer_log_file = LOGGING_CONFIG['bronze_producers_log']
    elif 'bronze.writers' in name or 'bronze_writer' in name:
        layer_log_file = LOGGING_CONFIG['bronze_writers_log']
    
    # Silver layer
    elif 'silver.transformations' in name or 'transformer' in name:
        layer_log_file = LOGGING_CONFIG['silver_transformers_log']
    elif 'silver.quality' in name or 'quality_check' in name:
        layer_log_file = LOGGING_CONFIG['silver_quality_log']
    
    # Gold layer
    elif 'gold.loaders' in name or ('loader' in name and 'gold' in name):
        layer_log_file = LOGGING_CONFIG['gold_loaders_log']
    
    # Airflow - Match both DAG files and Airflow runtime context
    elif 'airflow' in name or 'dags' in name or '_dag' in name or 'unusual_prefix' in name:
        layer_log_file = LOGGING_CONFIG['airflow_log']
    
    # Kafka - Match producers, consumers, and Kafka modules
    elif 'kafka' in name or 'confluent' in name or 'producer' in name or 'consumer' in name:
        layer_log_file = LOGGING_CONFIG['kafka_log']
    
    # Orchestration
    elif 'orchestrat' in name or 'bootstrap' in name or 'master' in name or 'scripts' in name:
        layer_log_file = LOGGING_CONFIG['pipeline_log']

    # Add layer-specific handler if determined
    if layer_log_file:
        layer_handler = logging.FileHandler(layer_log_file, encoding='utf-8')
        layer_handler.setLevel(level)
        layer_handler.setFormatter(fmt)
        logger.addHandler(layer_handler)

    # Add common handlers
    logger.setLevel(level)
    logger.addHandler(ch)
    logger.addHandler(app_handler)
    logger.addHandler(error_handler)
    logger.propagate = False

    return logger


# ====================================================================
# VALIDATION
# ====================================================================

def validate_config() -> bool:
    """Validate critical configuration settings."""
    errors = []

    # API Keys
    if not POLYGON_API_KEY:
        errors.append("POLYGON_API_KEY not set in .env")
    
    if not EDGAR_USER_IDENTITY:
        errors.append("EDGAR_USER_IDENTITY not set in .env")

    # Database
    if not DATABASE_CONFIG['password']:
        errors.append("POSTGRES_PASSWORD not set in .env")

    # Kafka
    if not KAFKA_CONFIG['bootstrap_servers']:
        errors.append("KAFKA_BOOTSTRAP_SERVERS not set")

    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True


# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def get_bronze_data_path(topic_name: str, year: int, month: int, day: int) -> Path:
    """
    Get Bronze layer Parquet file path for a specific date partition.
    
    Args:
        topic_name: Name of Kafka topic (without version, e.g., 'market_bars')
        year: Year (e.g., 2026)
        month: Month (1-12)
        day: Day (1-31)
    
    Returns:
        Path to the partitioned directory
    
    Example:
        path = get_bronze_data_path('market_bars', 2026, 1, 15)
        # Returns: data/bronze/market_bars/year=2026/month=01/day=15/
    """
    return (
        DATA_BRONZE_DIR / topic_name / 
        f"year={year}" / 
        f"month={month:02d}" / 
        f"day={day:02d}"
    )


def get_avro_schema_path(schema_name: str) -> Path:
    """
    Get path to Avro schema file.
    
    Args:
        schema_name: Name of schema without .avsc extension
    
    Returns:
        Path to schema file
    
    Example:
        path = get_avro_schema_path('market_bars_v1')
        # Returns: code/bronze/schemas/avro/market_bars_v1.avsc
    """
    return BRONZE_SCHEMAS_AVRO_DIR / f"{schema_name}.avsc"


# ====================================================================
# MAIN - RUN DIRECTLY TO VERIFY CONFIG
# ====================================================================

if __name__ == '__main__':
    print("=" * 80)
    print("MARKETMIND V1 CONFIGURATION")
    print("=" * 80)
    print(f"Project root:        {PROJECT_ROOT}")
    print(f"Database:            {DATABASE_CONFIG['database']}")
    print(f"Database host:       {DATABASE_CONFIG['host']}")
    print(f"Kafka bootstrap:     {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"Polygon API key:     {POLYGON_API_KEY[:10]}... (masked)")
    print(f"Edgar identity:      {EDGAR_USER_IDENTITY}")
    print(f"Airflow home:        {AIRFLOW_CONFIG['home']}")
    print(f"Data directory:      {DATA_DIR}")
    print(f"Logs directory:      {LOGS_DIR}")
    print(f"Data scope:          {DATA_SCOPE['start_date']} to {DATA_SCOPE['end_date']}")
    print(f"Expected tickers:    {EXPECTED_VOLUMES['ticker_count']}")
    print(f"Expected days:       {EXPECTED_VOLUMES['days_count']}")
    
    print("\n" + "=" * 80)
    print("LOGGING STRUCTURE")
    print("=" * 80)
    print("Master logs:")
    print(f"  - {LOGGING_CONFIG['app_log']}")
    print(f"  - {LOGGING_CONFIG['error_log']}")
    print("\nBronze layer:")
    print(f"  - {LOGGING_CONFIG['bronze_connectors_log']}")
    print(f"  - {LOGGING_CONFIG['bronze_producers_log']}")
    print(f"  - {LOGGING_CONFIG['bronze_writers_log']}")
    print("\nSilver layer:")
    print(f"  - {LOGGING_CONFIG['silver_transformers_log']}")
    print(f"  - {LOGGING_CONFIG['silver_quality_log']}")
    print("\nGold layer:")
    print(f"  - {LOGGING_CONFIG['gold_loaders_log']}")
    print("\nInfrastructure:")
    print(f"  - {LOGGING_CONFIG['airflow_log']}")
    print(f"  - {LOGGING_CONFIG['kafka_log']}")
    print(f"  - {LOGGING_CONFIG['pipeline_log']}")
    
    print("\n" + "=" * 80)
    print("CONFIGURATION VALIDATION")
    print("=" * 80)
    
    if validate_config():
        print("Status: PASSED")
    else:
        print("Status: FAILED - Fix errors above")
