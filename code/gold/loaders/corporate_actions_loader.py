# ====================================================================
# Corporate Actions Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/corporate_actions_loader.py
# Purpose: Load Silver corporate actions to Gold PostgreSQL table
# ====================================================================
"""
Corporate Actions Gold Loader

Loads Silver corporate actions into PostgreSQL gold.corporate_actions table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.corporate_actions_loader import CorporateActionsLoader
    
    loader = CorporateActionsLoader()
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


class CorporateActionsLoader:
    """
    Load Silver corporate actions to Gold PostgreSQL table.
    
    Handles both splits and dividends.
    """
    
    def __init__(self):
        """Initialize loader."""
        self.silver_base_path = DATA_DIR / 'silver' / 'corporate_actions'
        self.table_name = 'gold.corporate_actions'
        
        logger.info("CorporateActionsLoader initialized")
    
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
        Read all Silver corporate actions Parquet files.
        
        Returns:
            DataFrame with Silver data
        """
        if not self.silver_base_path.exists():
            logger.warning(f"Silver directory not found: {self.silver_base_path}")
            return pd.DataFrame()
        
        parquet_files = list(self.silver_base_path.glob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files in {self.silver_base_path}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} corporate actions from Silver")
        
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
            'ticker',
            'action_type',
            'execution_date',
            'split_ratio',
            'ex_dividend_date',
            'payment_date',
            'record_date',
            'cash_amount',
            'declaration_date',
            'frequency',
            'year',
            'month',
            'is_forward_split',
            'is_reverse_split'
        ]
        
        # Filter to only valid records
        if 'is_valid_split' in df.columns:
            df_splits = df[
                (df['action_type'] == 'SPLIT') & 
                (df['is_valid_split'])
            ]
        else:
            df_splits = df[df['action_type'] == 'SPLIT']
        
        if 'is_valid_dividend' in df.columns:
            df_dividends = df[
                (df['action_type'] == 'DIVIDEND') & 
                (df['is_valid_dividend'])
            ]
        else:
            df_dividends = df[df['action_type'] == 'DIVIDEND']
        
        # Combine
        df_valid = pd.concat([df_splits, df_dividends], ignore_index=True)
        
        if len(df_valid) < len(df):
            invalid_count = len(df) - len(df_valid)
            logger.warning(f"Filtered out {invalid_count} invalid corporate actions")
        
        # Select only columns that exist
        existing_columns = [col for col in gold_columns if col in df_valid.columns]
        df_gold = df_valid[existing_columns].copy()
        
        logger.info(f"Prepared {len(df_gold)} corporate actions for Gold")
        
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
            INSERT INTO gold.corporate_actions ({', '.join(columns)})
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
    
    def load_all(self, mode: str = 'replace'):
        """
        Load all Silver corporate actions to Gold.
        
        Args:
            mode: Loading mode ('append', 'replace')
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading corporate actions with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_data()
        
        if df.empty:
            logger.warning("No corporate actions data to load")
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
                cur.execute("TRUNCATE TABLE gold.corporate_actions")
                conn.commit()
                cur.close()
                logger.info("Truncated gold.corporate_actions table")
            
            self.load_to_postgres(conn, df_gold)
            
            logger.info(f"Successfully loaded {len(df_gold)} corporate actions")
            return True
            
        finally:
            conn.close()
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM gold.corporate_actions")
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()


# Example usage
if __name__ == '__main__':
    loader = CorporateActionsLoader()
    
    print("Loading corporate actions to Gold...")
    success = loader.load_all(mode='replace')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.corporate_actions: {count}")
        
        # Query sample
        conn = loader.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, action_type, execution_date, ex_dividend_date, 
                       split_ratio, cash_amount
                FROM gold.corporate_actions
                ORDER BY COALESCE(execution_date, ex_dividend_date) DESC
                LIMIT 5
            """)
            
            print("\nSample records:")
            for row in cur.fetchall():
                if row[1] == 'SPLIT':
                    print(f"  {row[0]} SPLIT on {row[2]}: {row[4]}:1")
                else:
                    print(f"  {row[0]} DIVIDEND on {row[3]}: ${row[5]}")
            
            cur.close()
        finally:
            conn.close()
    else:
        print("No data to load (expected if no corporate actions in test data)")
