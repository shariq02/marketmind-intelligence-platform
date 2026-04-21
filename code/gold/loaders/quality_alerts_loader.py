# ====================================================================
# Quality Alerts Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/quality_alerts_loader.py
# Purpose: Load Silver quality alerts to Gold PostgreSQL table
# ====================================================================
"""
Quality Alerts Gold Loader

Loads Silver quality alerts into PostgreSQL gold.quality_alerts table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.quality_alerts_loader import QualityAlertsLoader
    
    loader = QualityAlertsLoader()
    loader.load_all()
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

from config import (
    get_database_url,
    DATA_DIR,
    get_logger,
)

logger = get_logger(__name__)


class QualityAlertsLoader:
    """
    Load Silver quality alerts to Gold PostgreSQL table.
    
    Handles data quality monitoring.
    """
    
    def __init__(self):
        """Initialize loader with database connection."""
        self.engine = create_engine(get_database_url())
        self.silver_base_path = DATA_DIR / 'silver' / 'quality_alerts'
        self.table_name = 'gold.quality_alerts'
        
        logger.info("QualityAlertsLoader initialized")
    
    def read_silver_data(self) -> pd.DataFrame:
        """
        Read all Silver quality alerts Parquet files.
        
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
        logger.info(f"Read {len(combined)} quality alerts from Silver")
        
        return combined
    
    def prepare_for_gold(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply final transformations for Gold layer.
        
        Args:
            df: Silver DataFrame
        
        Returns:
            Gold-ready DataFrame
        """
        # Select columns for Gold table
        gold_columns = [
            'alert_id',
            'layer',
            'table_name',
            'check_type',
            'severity',
            'check_result',
            'check_datetime',
            'resolved',
            'resolution_datetime',
            'time_to_resolution_hours',
            'failure_description',
            'row_count_checked',
            'failure_count',
            'failure_rate',
            'threshold_value',
            'actual_value',
            'pipeline_blocked',
            'severity_score',
            'impact_score',
            'is_critical',
            'is_unresolved'
        ]
        
        # Select only existing columns
        existing_columns = [col for col in gold_columns if col in df.columns]
        df_gold = df[existing_columns].copy()
        
        logger.info(f"Prepared {len(df_gold)} quality alerts for Gold")
        
        return df_gold
    
    def load_to_postgres(self, df: pd.DataFrame, if_exists: str = 'append'):
        """
        Load DataFrame to PostgreSQL.
        
        Args:
            df: DataFrame to load
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
        """
        if df.empty:
            logger.warning("No data to load")
            return
        
        # Load to PostgreSQL
        df.to_sql(
            name='quality_alerts',
            schema='gold',
            con=self.engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Loaded {len(df)} records to {self.table_name}")
    
    def load_all(self, mode: str = 'replace'):
        """
        Load all Silver quality alerts to Gold.
        
        Args:
            mode: Loading mode ('append', 'replace')
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading quality alerts with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_data()
        
        if df.empty:
            logger.warning("No quality alerts data to load")
            return False
        
        # Prepare for Gold
        df_gold = self.prepare_for_gold(df)
        
        if df_gold.empty:
            logger.warning("No valid data after preparation")
            return False
        
        # Load to PostgreSQL
        self.load_to_postgres(df_gold, if_exists=mode)
        
        logger.info(f"Successfully loaded {len(df_gold)} quality alerts")
        
        return True
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM gold.quality_alerts"))
            count = result.scalar()
        
        return count
    
    def close(self):
        """Close database connection."""
        self.engine.dispose()


# Example usage
if __name__ == '__main__':
    loader = QualityAlertsLoader()
    
    print("Loading quality alerts to Gold...")
    success = loader.load_all(mode='replace')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.quality_alerts: {count}")
        
        # Query sample
        with loader.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT layer, table_name, check_type, severity, 
                       resolved, failure_rate
                FROM gold.quality_alerts
                ORDER BY check_datetime DESC
                LIMIT 5
            """))
            
            print("\nSample records:")
            for row in result:
                status = "RESOLVED" if row.resolved else "UNRESOLVED"
                print(f"  {row.layer} {row.table_name} - {row.check_type} ({row.severity}): {status} - {row.failure_rate:.1f}% failure")
    else:
        print("No data to load (expected if no quality alerts in test data)")
    
    loader.close()
