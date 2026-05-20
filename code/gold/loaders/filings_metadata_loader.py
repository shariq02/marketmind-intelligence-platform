# ====================================================================
# SEC Filings Metadata Gold Loader
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/gold/loaders/filings_metadata_loader.py
# Purpose: Load Silver SEC filings metadata to Gold PostgreSQL table
# ====================================================================
"""
SEC Filings Metadata Gold Loader

Loads Silver SEC filings metadata into PostgreSQL gold.sec_filings table:
- Read Silver Parquet files
- Apply final transformations
- Load into PostgreSQL
- Handle incremental updates

Usage:
    from code.gold.loaders.filings_metadata_loader import FilingsMetadataLoader
    
    loader = FilingsMetadataLoader()
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


class FilingsMetadataLoader:
    """
    Load Silver SEC filings metadata to Gold PostgreSQL table.
    
    Handles all SEC filing types.
    """
    
    def __init__(self):
        """Initialize loader."""
        self.silver_base_path = DATA_DIR / 'silver' / 'filings_metadata'
        self.table_name = 'gold.sec_filings'
        
        logger.info("FilingsMetadataLoader initialized")
    
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
        Read all Silver filings metadata Parquet files.
        
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
        logger.info(f"Read {len(combined)} filings from Silver")
        
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
            'accession_number',
            'ticker',
            'cik',
            'company_name',
            'form_type',
            'filing_date',
            'report_date',
            'filing_year',
            'filing_quarter',
            'filing_category',
            'is_periodic_report',
            'is_amended',
            'is_xbrl',
            'is_inline_xbrl',
            'has_structured_data',
            'filing_lag_days',
            'document_url'
        ]
        
        # Select only existing columns
        existing_columns = [col for col in gold_columns if col in df.columns]
        df_gold = df[existing_columns].copy()

        # Replace NULL tickers with 'UNKNOWN' to satisfy PostgreSQL constraint
        df_gold['ticker'] = df_gold['ticker'].fillna('UNKNOWN')
        
        logger.info(f"Prepared {len(df_gold)} filings for Gold")
        
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
            INSERT INTO gold.sec_filings ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (accession_number) DO NOTHING
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
        Load all Silver filings metadata to Gold.
        
        Args:
            mode: Loading mode ('append', 'replace')
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Loading filings metadata with mode={mode}")
        
        # Read Silver data
        df = self.read_silver_data()
        
        if df.empty:
            logger.warning("No filings metadata to load")
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
                cur.execute("TRUNCATE TABLE gold.sec_filings")
                conn.commit()
                cur.close()
                logger.info("Truncated gold.sec_filings table")
            
            self.load_to_postgres(conn, df_gold)
            
            logger.info(f"Successfully loaded {len(df_gold)} filings")
            return True
            
        finally:
            conn.close()
    
    def get_record_count(self) -> int:
        """Get total record count in Gold table."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM gold.sec_filings")
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()


# Example usage
if __name__ == '__main__':
    loader = FilingsMetadataLoader()
    
    print("Loading SEC filings metadata to Gold...")
    success = loader.load_all(mode='replace')
    
    if success:
        print("Load successful!")
        
        # Get record count
        count = loader.get_record_count()
        print(f"\nTotal records in gold.sec_filings: {count}")
        
        # Query sample
        conn = loader.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, form_type, filing_date, filing_category
                FROM gold.sec_filings
                ORDER BY filing_date DESC
                LIMIT 5
            """)
            
            print("\nSample records:")
            for row in cur.fetchall():
                print(f"  {row[0]} {row[1]} on {row[2]} ({row[3]})")
            
            cur.close()
        finally:
            conn.close()
    else:
        print("No data to load (expected if no filings in test data)")
