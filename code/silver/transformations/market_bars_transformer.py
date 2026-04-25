# ====================================================================
# Market Bars Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/silver/transformations/market_bars_transformer.py
# Purpose: Transform Bronze market bars to Silver
# ====================================================================
"""
Market Bars Transformer

Transforms Bronze OHLCV bars to Silver:
- Run quality checks
- Remove duplicates
- Add derived date columns
- Validate OHLC relationships
- Write to Silver Parquet

Usage:
    from code.silver.transformations.market_bars_transformer import MarketBarsTransformer
    
    transformer = MarketBarsTransformer()
    transformer.transform_partition('2026-01-02')
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from config import (
    get_bronze_data_path,
    DATA_DIR,
    get_logger,
)

import sys
sys.path.append(str(Path(__file__).parent.parent / 'quality'))

from quality_checks import QualityChecker

logger = get_logger(__name__)


class MarketBarsTransformer:
    """
    Transform Bronze market bars to Silver.
    
    Applies quality checks and transformations.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'market_bars'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("MarketBarsTransformer initialized")
    
    def read_bronze_partition(self, date: str) -> pd.DataFrame:
        """
        Read Bronze Parquet files for a specific date.
        
        Args:
            date: Date string YYYY-MM-DD
        
        Returns:
            DataFrame with Bronze data
        """
        dt = datetime.strptime(date, '%Y-%m-%d')
        partition_dir = get_bronze_data_path('market_bars', dt.year, dt.month, dt.day)
        
        if not partition_dir.exists():
            logger.warning(f"Bronze partition not found: {partition_dir}")
            return pd.DataFrame()
        
        # Read all Parquet files in partition
        parquet_files = list(partition_dir.glob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files in {partition_dir}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} records from Bronze partition {date}")
        
        return combined
    
    def apply_quality_checks(self, df: pd.DataFrame) -> tuple:
        """
        Apply quality checks to DataFrame.
        
        Args:
            df: DataFrame to check
        
        Returns:
            Tuple of (cleaned_df, quality_results)
        """
        results = self.quality_checker.run_all_checks(df, 'market_bars')
        
        # Log results
        for result in results:
            status = "PASS" if result.passed else "FAIL"
            logger.info(f"Quality check {result.check_type.value}: {status}")
            if not result.passed:
                logger.warning(f"  {result.description}")
        
        return df, results
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records based on primary key.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        # Remove duplicates - keep first occurrence
        df_dedup = df.drop_duplicates(
            subset=['ticker', 'timestamp', 'granularity'],
            keep='first'
        )
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate records")
        
        return df_dedup
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived date columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with derived columns
        """
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Extract date components
        df['date'] = df['datetime'].dt.date
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        df['day'] = df['datetime'].dt.day
        df['hour'] = df['datetime'].dt.hour
        df['minute'] = df['datetime'].dt.minute
        
        # Day of week (0=Monday, 6=Sunday)
        df['day_of_week'] = df['datetime'].dt.dayofweek
        
        # Trading day flag (Monday-Friday)
        df['is_trading_day'] = df['day_of_week'].isin([0, 1, 2, 3, 4])
        
        logger.info("Added derived date columns")
        
        return df
    
    def validate_ohlc(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate OHLC relationships and flag invalid records.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with is_valid_ohlc flag
        """
        # Check High >= Low
        df['is_valid_ohlc'] = (
            (df['high'] >= df['low']) &
            (df['open'] >= df['low']) &
            (df['open'] <= df['high']) &
            (df['close'] >= df['low']) &
            (df['close'] <= df['high'])
        )
        
        invalid_count = (~df['is_valid_ohlc']).sum()
        
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} records with invalid OHLC relationships")
        
        return df
    
    def transform_partition(self, date: str) -> bool:
        """
        Transform Bronze partition to Silver.
        
        Args:
            date: Date string YYYY-MM-DD
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Transforming partition: {date}")
        
        # Read Bronze data
        df = self.read_bronze_partition(date)
        
        if df.empty:
            logger.warning(f"No data to transform for {date}")
            return False
        
        # Apply quality checks
        df, quality_results = self.apply_quality_checks(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Validate OHLC
        df = self.validate_ohlc(df)
        
        # Write to Silver
        dt = datetime.strptime(date, '%Y-%m-%d')
        silver_partition_dir = (
            self.silver_base_path / 
            f"year={dt.year}" / 
            f"month={dt.month:02d}" / 
            f"day={dt.day:02d}"
        )
        silver_partition_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = silver_partition_dir / f"part_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        
        logger.info(
            f"Wrote {len(df)} records to Silver: "
            f"{output_file.relative_to(DATA_DIR)}"
        )
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = MarketBarsTransformer()
    
    # Transform the test partition we created
    print("Transforming 2026-01-02 partition...")
    success = transformer.transform_partition('2026-01-02')
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_file = list(
            (transformer.silver_base_path / 'year=2026' / 'month=01' / 'day=02').glob('*.parquet')
        )[0]
        
        df = pd.read_parquet(silver_file)
        print(f"\nSilver data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst record:")
        print(df.iloc[0])
    else:
        print("Transformation failed")
