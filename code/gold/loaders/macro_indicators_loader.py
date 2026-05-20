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
import psycopg2
from psycopg2.extras import execute_values

from config import (
    DATABASE_CONFIG,
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
        """Initialize loader."""
        self.silver_base_path = DATA_DIR / 'silver' / 'macro_indicators'
        self.table_name = 'gold.macro_indicators'
        
        logger.info("MacroIndicatorsLoader initialized")
    
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
        import numpy as np
        
        # Select columns for Gold table (match gold_tables.sql schema)
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
        
        # Replace infinity and NaN values with None (NULL in PostgreSQL)
        numeric_columns = df_gold.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df_gold[col] = df_gold[col].replace([np.inf, -np.inf], None)
            df_gold[col] = df_gold[col].where(df_gold[col].notna(), None)
        
        logger.info(f"Prepared {len(df_gold)} macro indicators for Gold")
        
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
            INSERT INTO gold.macro_indicators ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (indicator_name, date) DO NOTHING
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
        
        # Connect and load
        conn = self.get_connection()
        
        try:
            if mode == 'replace':
                cur = conn.cursor()
                cur.execute("TRUNCATE TABLE gold.macro_indicators")
                conn.commit()
                cur.close()
                logger.info("Truncated gold.macro_indicators table")
            
            self.load_to_postgres(conn, df_gold)
            
            logger.info(f"Successfully loaded {len(df_gold)} macro indicators")
            return True
            
        finally:
            conn.close()
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM gold.macro_indicators")
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()


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
        conn = loader.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT indicator_name, date, value, unit, indicator_category
                FROM gold.macro_indicators
                ORDER BY date DESC
                LIMIT 5
            """)
            
            print("\nSample records:")
            for row in cur.fetchall():
                print(f"  {row[0]} on {row[1]}: {row[2]} {row[3]} ({row[4]})")
            
            cur.close()
        finally:
            conn.close()
    else:
        print("No data to load (expected if no macro indicators in test data)")
