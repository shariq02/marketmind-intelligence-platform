# ====================================================================
# Corporate Actions Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/silver/transformations/corporate_actions_transformer.py
# Purpose: Transform Bronze corporate actions to Silver
# ====================================================================
"""
Corporate Actions Transformer

Transforms Bronze corporate actions (splits/dividends) to Silver:
- Run quality checks
- Remove duplicates
- Validate action-specific fields
- Add derived columns
- Write to Silver Parquet

Usage:
    from code.silver.transformations.corporate_actions_transformer import CorporateActionsTransformer
    
    transformer = CorporateActionsTransformer()
    transformer.transform_all()
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


class CorporateActionsTransformer:
    """
    Transform Bronze corporate actions to Silver.
    
    Handles both stock splits and dividends.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'corporate_actions'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("CorporateActionsTransformer initialized")
    
    def read_bronze_data(self) -> pd.DataFrame:
        """
        Read all Bronze corporate actions Parquet files.
        
        Returns:
            DataFrame with Bronze data
        """
        bronze_base = DATA_DIR / 'bronze' / 'corporate_actions'
        
        if not bronze_base.exists():
            logger.warning(f"Bronze directory not found: {bronze_base}")
            return pd.DataFrame()
        
        # Find all Parquet files recursively
        parquet_files = list(bronze_base.rglob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files found in {bronze_base}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} corporate actions from Bronze")
        
        return combined
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate corporate actions.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        # For splits: unique by ticker + execution_date
        # For dividends: unique by ticker + ex_dividend_date
        df['_dedup_key'] = df.apply(
            lambda row: (
                f"{row['ticker']}_{row['execution_date']}" 
                if row['action_type'] == 'SPLIT' 
                else f"{row['ticker']}_{row['ex_dividend_date']}"
            ),
            axis=1
        )
        
        df_dedup = df.drop_duplicates(subset=['_dedup_key'], keep='first')
        df_dedup = df_dedup.drop('_dedup_key', axis=1)
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate corporate actions")
        
        return df_dedup
    
    def validate_splits(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate split records have required fields.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with is_valid_split flag
        """
        splits = df[df['action_type'] == 'SPLIT'].copy()
        
        splits['is_valid_split'] = (
            splits['execution_date'].notna() &
            splits['split_ratio'].notna() &
            (splits['split_ratio'] > 0)
        )
        
        invalid_count = (~splits['is_valid_split']).sum()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid split records")
        
        # Merge back
        df.loc[df['action_type'] == 'SPLIT', 'is_valid_split'] = splits['is_valid_split']
        
        return df
    
    def validate_dividends(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate dividend records have required fields.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with is_valid_dividend flag
        """
        dividends = df[df['action_type'] == 'DIVIDEND'].copy()
        
        dividends['is_valid_dividend'] = (
            dividends['ex_dividend_date'].notna() &
            dividends['cash_amount'].notna() &
            (dividends['cash_amount'] > 0)
        )
        
        invalid_count = (~dividends['is_valid_dividend']).sum()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid dividend records")
        
        # Merge back
        df.loc[df['action_type'] == 'DIVIDEND', 'is_valid_dividend'] = dividends['is_valid_dividend']
        
        return df
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with derived columns
        """
        # For splits: extract year/month from execution_date
        splits_mask = df['action_type'] == 'SPLIT'
        if splits_mask.any():
            df.loc[splits_mask, 'event_date'] = pd.to_datetime(df.loc[splits_mask, 'execution_date'])
        
        # For dividends: extract year/month from ex_dividend_date
        dividends_mask = df['action_type'] == 'DIVIDEND'
        if dividends_mask.any():
            df.loc[dividends_mask, 'event_date'] = pd.to_datetime(df.loc[dividends_mask, 'ex_dividend_date'])
        
        # Add year/month/day
        df['year'] = df['event_date'].dt.year
        df['month'] = df['event_date'].dt.month
        df['day'] = df['event_date'].dt.day
        
        # Split type classification
        if splits_mask.any():
            df.loc[splits_mask, 'is_forward_split'] = df.loc[splits_mask, 'split_ratio'] > 1
            df.loc[splits_mask, 'is_reverse_split'] = df.loc[splits_mask, 'split_ratio'] < 1
        
        # Dividend frequency label
        if dividends_mask.any():
            df.loc[dividends_mask, 'frequency_label'] = df.loc[dividends_mask, 'frequency'].map({
                1: 'Annual',
                2: 'Semi-Annual',
                4: 'Quarterly',
                12: 'Monthly'
            })
        
        logger.info("Added derived columns")
        
        return df
    
    def transform_all(self) -> bool:
        """
        Transform all Bronze corporate actions to Silver.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Transforming corporate actions")
        
        # Read Bronze data
        df = self.read_bronze_data()
        
        if df.empty:
            logger.warning("No corporate actions data to transform")
            return False
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Validate splits
        df = self.validate_splits(df)
        
        # Validate dividends
        df = self.validate_dividends(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Write to Silver (not partitioned - corporate actions are infrequent)
        output_file = self.silver_base_path / f"corporate_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        
        logger.info(
            f"Wrote {len(df)} corporate actions to Silver: "
            f"{output_file.relative_to(DATA_DIR)}"
        )
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = CorporateActionsTransformer()
    
    print("Transforming corporate actions...")
    success = transformer.transform_all()
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_files = list(transformer.silver_base_path.glob('*.parquet'))
        if silver_files:
            df = pd.read_parquet(silver_files[-1])
            print(f"\nSilver data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nAction types: {df['action_type'].value_counts().to_dict()}")
    else:
        print("No data to transform (expected - no corporate actions in test data)")
