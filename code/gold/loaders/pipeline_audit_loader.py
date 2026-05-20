# ====================================================================
# Pipeline Audit Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/pipeline_audit_loader.py
# Purpose: Load Silver pipeline audit to Gold PostgreSQL table
# ====================================================================
"""
Pipeline Audit Gold Loader

Loads Silver pipeline audit into PostgreSQL gold.pipeline_audit table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.pipeline_audit_loader import PipelineAuditLoader
    
    loader = PipelineAuditLoader()
    loader.load_all()
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config import (
    DATABASE_CONFIG,
    DATA_DIR,
    get_logger,
)

logger = get_logger(__name__)


class PipelineAuditLoader:
    """
    Load Silver pipeline audit to Gold PostgreSQL table.
    
    Handles pipeline execution metrics.
    """
    
    def __init__(self):
        """Initialize loader."""
        self.silver_base_path = DATA_DIR / 'silver' / 'pipeline_audit'
        self.table_name = 'gold.pipeline_audit'
        
        logger.info("PipelineAuditLoader initialized")
    
    def get_connection(self):
        """Create and return a PostgreSQL connection."""
        return psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            dbname=DATABASE_CONFIG['database'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
        )
    
    def read_silver_data(self) -> pd.DataFrame:
        """
        Read all Silver pipeline audit Parquet files.
        
        Returns:
            DataFrame with Silver data
        """
        if not self.silver_base_path.exists():
            logger.warning(f"Silver directory not found: {self.silver_base_path}")
            return pd.DataFrame()
        
        parquet_files = list(self.silver_base_path.rglob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files in {self.silver_base_path}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} audit records from Silver")
        
        return combined
    
    def prepare_for_gold(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply final transformations for Gold layer.
        
        Args:
            df: Silver DataFrame
        
        Returns:
            Gold-ready DataFrame
        """
        # Select columns for Gold table (match gold_tables.sql schema)
        gold_columns = [
            'audit_id',
            'connector',
            'execution_mode',
            'status',
            'start_datetime',
            'end_datetime',
            'duration_seconds',
            'duration_minutes',
            'records_retrieved',
            'records_written',
            'bytes_written',
            'megabytes_written',
            'api_calls_made',
            'rate_limited',
            'write_success_rate',
            'records_per_second',
            'records_per_api_call',
            'is_success',
            'is_failure',
            'is_slow',
            'error_message'
        ]
        
        # Select only existing columns
        existing_columns = [col for col in gold_columns if col in df.columns]
        df_gold = df[existing_columns].copy()
        
        logger.info(f"Prepared {len(df_gold)} audit records for Gold")
        
        return df_gold
    
    def load_to_postgres(self, conn, df: pd.DataFrame):
        """
        Load DataFrame to PostgreSQL using bulk insert.
        
        Args:
            conn: Database connection
            df: DataFrame to load
        """
        if df.empty:
            logger.warning("No data to load")
            return
        
        columns = list(df.columns)
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        sql = f"""
            INSERT INTO gold.pipeline_audit ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (audit_id) DO NOTHING
        """
        
        cur = conn.cursor()
        try:
            execute_values(cur, sql, rows, page_size=1000)
            conn.commit()
            logger.info(f"Loaded {len(df)} records to {self.table_name}")
        except Exception as exc:
            conn.rollback()
            logger.error(f"Failed to load data: {exc}")
            raise
        finally:
            cur.close()
    
    def load_all(self, mode: str = 'replace'):
        """
        Load all Silver pipeline audit to Gold.
        
        Args:
            mode: Loading mode ('append', 'replace')
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading pipeline audit with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_data()
        
        if df.empty:
            logger.warning("No pipeline audit data to load")
            return False
        
        # Prepare for Gold
        df_gold = self.prepare_for_gold(df)
        
        if df_gold.empty:
            logger.warning("No valid data after preparation")
            return False
        
        # Connect and load
        conn = self.get_connection()
        
        try:
            if mode == 'replace':
                cur = conn.cursor()
                cur.execute("TRUNCATE TABLE gold.pipeline_audit")
                conn.commit()
                cur.close()
                logger.info("Truncated gold.pipeline_audit table")
            
            self.load_to_postgres(conn, df_gold)
            
            logger.info(f"Successfully loaded {len(df_gold)} audit records")
            return True
            
        finally:
            conn.close()
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM gold.pipeline_audit")
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()


# Example usage
if __name__ == '__main__':
    loader = PipelineAuditLoader()
    
    print("Loading pipeline audit to Gold...")
    success = loader.load_all(mode='replace')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.pipeline_audit: {count}")
        
        # Query sample
        conn = loader.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT connector, execution_mode, status, 
                       records_written, duration_minutes
                FROM gold.pipeline_audit
                ORDER BY start_datetime DESC
                LIMIT 5
            """)
            
            print("\nSample records:")
            for row in cur.fetchall():
                print(f"  {row[0]} ({row[1]}): {row[2]} - {row[3]} records in {row[4]:.2f}min")
            
            cur.close()
        finally:
            conn.close()
    else:
        print("No data to load (expected if no audit records in test data)")
