# ====================================================================
# Macro Indicators Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/macro_indicators_loader.py
# Purpose: Load Silver macro indicators to Gold PostgreSQL table
# ====================================================================
"""
Macro Indicators Gold Loader

Loads Silver macro indicators into PostgreSQL gold.macro_indicators table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.macro_indicators_loader import MacroIndicatorsLoader
    
    loader = MacroIndicatorsLoader()
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


class MacroIndicatorsLoader:
    """
    Load Silver macro indicators to Gold PostgreSQL table.
    
    Handles all economic indicators.
    """
    
    def __init__(self):
        """Initialize loader with database connection."""
        self.engine = create_engine(get_database_url())
        self.silver_base_path = DATA_DIR / 'silver' / 'macro_indicators'
        self.table_name = 'gold.macro_indicators'
        
        logger.info("MacroIndicatorsLoader initialized")
    
    def read_silver_data(self) -> pd.DataFrame:
        """
        Read all Silver macro indicators Parquet files.
        
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
        logger.info(f"Read {len(combined)} macro indicators from Silver")
        
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
            'indicator_name',
            'date',
            'value',
            'unit',
            'frequency',
            'forecast_value',
            'previous_value',
            'year',
            'month',
            'quarter',
            'value_change',
            'value_pct_change',
            'indicator_category',
            'source_url'
        ]
        
        # Select only existing columns
        existing_columns = [col for col in gold_columns if col in df.columns]
        df_gold = df[existing_columns].copy()
        
        logger.info(f"Prepared {len(df_gold)} macro indicators for Gold")
        
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
            name='macro_indicators',
            schema='gold',
            con=self.engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Loaded {len(df)} records to {self.table_name}")
    
    def delete_by_date_range(self, start_date: str, end_date: str):
        """
        Delete existing records for a date range.
        
        Args:
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD
        """
        with self.engine.connect() as conn:
            delete_sql = text("""
                DELETE FROM gold.macro_indicators
                WHERE date >= :start_date AND date <= :end_date
            """)
            
            result = conn.execute(delete_sql, {
                'start_date': start_date,
                'end_date': end_date
            })
            conn.commit()
            
            deleted_count = result.rowcount
            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} existing records for {start_date} to {end_date}")
    
    def load_all(self, mode: str = 'replace'):
        """
        Load all Silver macro indicators to Gold.
        
        Args:
            mode: Loading mode ('append', 'replace')
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading macro indicators with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_data()
        
        if df.empty:
            logger.warning("No macro indicators data to load")
            return False
        
        # Prepare for Gold
        df_gold = self.prepare_for_gold(df)
        
        if df_gold.empty:
            logger.warning("No valid data after preparation")
            return False
        
        # Load to PostgreSQL
        self.load_to_postgres(df_gold, if_exists=mode)
        
        logger.info(f"Successfully loaded {len(df_gold)} macro indicators")
        
        return True
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM gold.macro_indicators"))
            count = result.scalar()
        
        return count
    
    def close(self):
        """Close database connection."""
        self.engine.dispose()


# Example usage
if __name__ == '__main__':
    loader = MacroIndicatorsLoader()
    
    print("Loading macro indicators to Gold...")
    success = loader.load_all(mode='replace')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.macro_indicators: {count}")
        
        # Query sample
        with loader.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT indicator_name, date, value, unit, indicator_category
                FROM gold.macro_indicators
                ORDER BY date DESC
                LIMIT 5
            """))
            
            print("\nSample records:")
            for row in result:
                print(f"  {row.indicator_name} on {row.date}: {row.value} {row.unit} ({row.indicator_category})")
    else:
        print("No data to load (expected if no macro indicators in test data)")
    
    loader.close()
