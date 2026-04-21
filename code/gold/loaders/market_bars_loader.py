# ====================================================================
# Market Bars Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/market_bars_loader.py
# Purpose: Load Silver market bars to Gold PostgreSQL table
# ====================================================================
"""
Market Bars Gold Loader

Loads Silver market bars into PostgreSQL gold.ohlcv_bars table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.market_bars_loader import MarketBarsLoader
    
    loader = MarketBarsLoader()
    loader.load_partition('2026-01-02')
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime

from config import (
    DATABASE_CONFIG,
    DATA_DIR,
    get_logger,
)

logger = get_logger(__name__)


class MarketBarsLoader:
    """
    Load Silver market bars to Gold PostgreSQL table.
    
    Handles incremental loading and deduplication.
    """
    
    def __init__(self):
        """Initialize loader with database connection."""
        self.silver_base_path = DATA_DIR / 'silver' / 'market_bars'
        self.table_name = 'gold.ohlcv_bars'
        
        logger.info("MarketBarsLoader initialized")
    
    def get_connection(self):
        """Create and return a PostgreSQL connection."""
        return psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            dbname=DATABASE_CONFIG['database'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
        )
    
    def read_silver_partition(self, date: str) -> pd.DataFrame:
        """
        Read Silver Parquet files for a specific date.
        
        Args:
            date: Date string YYYY-MM-DD
        
        Returns:
            DataFrame with Silver data
        """
        dt = datetime.strptime(date, '%Y-%m-%d')
        partition_dir = (
            self.silver_base_path / 
            f"year={dt.year}" / 
            f"month={dt.month:02d}" / 
            f"day={dt.day:02d}"
        )
        
        if not partition_dir.exists():
            logger.warning(f"Silver partition not found: {partition_dir}")
            return pd.DataFrame()
        
        parquet_files = list(partition_dir.glob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files in {partition_dir}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} records from Silver partition {date}")
        
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
            'ticker',
            'timestamp',
            'granularity',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'vwap',
            'trade_count',
            'adjusted',
            'date',
            'year',
            'month',
            'day',
            'is_trading_day',
            'is_valid_ohlc'
        ]
        
        # Filter to only valid OHLC records
        df_valid = df[df['is_valid_ohlc'] == True].copy()
        
        if len(df_valid) < len(df):
            invalid_count = len(df) - len(df_valid)
            logger.warning(f"Filtered out {invalid_count} invalid OHLC records")
        
        # Select columns
        df_gold = df_valid[gold_columns].copy()
        
        logger.info(f"Prepared {len(df_gold)} records for Gold")
        
        return df_gold
    
    def delete_partition(self, conn, date: str):
        """
        Delete existing records for a date partition.
        
        Args:
            conn: Database connection
            date: Date string YYYY-MM-DD
        """
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM gold.ohlcv_bars WHERE date = %s",
                (date,)
            )
            deleted_count = cur.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} existing records for {date}")
        except Exception as exc:
            conn.rollback()
            logger.error(f"Failed to delete partition {date}: {exc}")
            raise
        finally:
            cur.close()
    
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
            INSERT INTO gold.ohlcv_bars ({', '.join(columns)})
            VALUES %s
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
    
    def load_partition(self, date: str, mode: str = 'upsert'):
        """
        Load a single date partition to Gold.
        
        Args:
            date: Date string YYYY-MM-DD
            mode: Loading mode ('append', 'upsert', 'replace')
                  - append: Just insert new records
                  - upsert: Delete partition then insert (default)
                  - replace: Truncate entire table then insert
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading partition {date} with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_partition(date)
        
        if df.empty:
            logger.warning(f"No data to load for {date}")
            return False
        
        # Prepare for Gold
        df_gold = self.prepare_for_gold(df)
        
        if df_gold.empty:
            logger.warning(f"No valid data after preparation for {date}")
            return False
        
        # Connect and load
        conn = self.get_connection()
        
        try:
            if mode == 'replace':
                # Truncate entire table
                cur = conn.cursor()
                cur.execute("TRUNCATE TABLE gold.ohlcv_bars")
                conn.commit()
                cur.close()
                logger.info("Truncated gold.ohlcv_bars table")
            elif mode == 'upsert':
                # Delete partition then insert
                self.delete_partition(conn, date)
            
            # Load data
            self.load_to_postgres(conn, df_gold)
            
            logger.info(f"Successfully loaded {len(df_gold)} records for {date}")
            return True
            
        finally:
            conn.close()
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM gold.ohlcv_bars")
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()


# Example usage
if __name__ == '__main__':
    loader = MarketBarsLoader()
    
    print("Loading 2026-01-02 partition to Gold...")
    success = loader.load_partition('2026-01-02', mode='upsert')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.ohlcv_bars: {count}")
        
        # Query sample
        conn = loader.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, date, open, high, low, close, volume
                FROM gold.ohlcv_bars
                ORDER BY date DESC, ticker
                LIMIT 5
            """)
            
            print("\nSample records:")
            for row in cur.fetchall():
                print(f"  {row[0]} {row[1]}: O={row[2]} H={row[3]} L={row[4]} C={row[5]} V={row[6]}")
            
            cur.close()
        finally:
            conn.close()
    else:
        print("Load failed")
