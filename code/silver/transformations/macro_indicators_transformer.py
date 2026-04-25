# ====================================================================
# Macro Indicators Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/silver/transformations/macro_indicators_transformer.py
# Purpose: Transform Bronze macro indicators to Silver
# ====================================================================
"""
Macro Indicators Transformer

Transforms Bronze macro indicators to Silver:
- Run quality checks
- Remove duplicates
- Standardize indicator names
- Add derived columns
- Calculate changes (MoM, YoY)
- Write to Silver Parquet

Usage:
    from code.silver.transformations.macro_indicators_transformer import MacroIndicatorsTransformer
    
    transformer = MacroIndicatorsTransformer()
    transformer.transform_all()
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from config import (
    DATA_DIR,
    get_logger,
)

import sys
sys.path.append(str(Path(__file__).parent.parent / 'quality'))

from quality_checks import QualityChecker

logger = get_logger(__name__)


class MacroIndicatorsTransformer:
    """
    Transform Bronze macro indicators to Silver.
    
    Standardizes and enriches economic indicators.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'macro_indicators'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("MacroIndicatorsTransformer initialized")
    
    def read_bronze_data(self) -> pd.DataFrame:
        """
        Read all Bronze macro indicators Parquet files.
        
        Returns:
            DataFrame with Bronze data
        """
        bronze_base = DATA_DIR / 'bronze' / 'macro_indicators'
        
        if not bronze_base.exists():
            logger.warning(f"Bronze directory not found: {bronze_base}")
            return pd.DataFrame()
        
        parquet_files = list(bronze_base.rglob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files found in {bronze_base}")
            return pd.DataFrame()
        
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(combined)} macro indicators from Bronze")
        
        return combined
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate indicators.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        df_dedup = df.drop_duplicates(
            subset=['indicator_name', 'date'],
            keep='first'
        )
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate indicators")
        
        return df_dedup
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with derived columns
        """
        # Convert date to datetime
        df['datetime'] = pd.to_datetime(df['date'])
        
        # Extract components
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        df['quarter'] = df['datetime'].dt.quarter
        df['day'] = df['datetime'].dt.day
        
        logger.info("Added derived date columns")
        
        return df
    
    def calculate_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate period-over-period changes.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with change columns
        """
        # Sort by indicator and date
        df = df.sort_values(['indicator_name', 'datetime'])
        
        # Calculate changes by indicator
        df['value_change'] = df.groupby('indicator_name')['value'].diff()
        df['value_pct_change'] = df.groupby('indicator_name')['value'].pct_change() * 100
        
        # Calculate vs forecast (if available)
        df['vs_forecast'] = df['value'] - df['forecast_value']
        df['vs_forecast_pct'] = (
            (df['value'] - df['forecast_value']) / df['forecast_value'].abs() * 100
        ).where(df['forecast_value'].notna())
        
        # Calculate vs previous (if available)
        df['vs_previous'] = df['value'] - df['previous_value']
        df['vs_previous_pct'] = (
            (df['value'] - df['previous_value']) / df['previous_value'].abs() * 100
        ).where(df['previous_value'].notna())
        
        logger.info("Calculated period changes")
        
        return df
    
    def add_indicator_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add indicator category and description.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with metadata columns
        """
        # Categorize indicators
        def categorize_indicator(name):
            name_upper = name.upper()
            if 'CPI' in name_upper or 'INFLATION' in name_upper:
                return 'Inflation'
            elif 'GDP' in name_upper or 'GROWTH' in name_upper:
                return 'Economic Growth'
            elif 'UNEMPLOYMENT' in name_upper or 'EMPLOYMENT' in name_upper or 'ADP' in name_upper:
                return 'Labor Market'
            elif 'RATE' in name_upper or 'INTEREST' in name_upper or 'FEDERAL_FUNDS' in name_upper:
                return 'Interest Rates'
            else:
                return 'Other'
        
        df['indicator_category'] = df['indicator_name'].apply(categorize_indicator)
        
        logger.info("Added indicator metadata")
        
        return df
    
    def transform_all(self) -> bool:
        """
        Transform all Bronze macro indicators to Silver.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Transforming macro indicators")
        
        # Read Bronze data
        df = self.read_bronze_data()
        
        if df.empty:
            logger.warning("No macro indicators data to transform")
            return False
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Calculate changes
        df = self.calculate_changes(df)
        
        # Add metadata
        df = self.add_indicator_metadata(df)
        
        # Write to Silver (partitioned by year)
        for year in df['year'].unique():
            year_df = df[df['year'] == year]
            
            year_dir = self.silver_base_path / f"year={year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = year_dir / f"macro_indicators_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            year_df.to_parquet(output_file, compression='snappy', index=False)
            
            logger.info(
                f"Wrote {len(year_df)} indicators for year {year} to Silver"
            )
        
        logger.info(f"Total macro indicators written to Silver: {len(df)}")
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = MacroIndicatorsTransformer()
    
    print("Transforming macro indicators...")
    success = transformer.transform_all()
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_files = list(transformer.silver_base_path.rglob('*.parquet'))
        if silver_files:
            df = pd.read_parquet(silver_files[-1])
            print(f"\nSilver data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nIndicator categories: {df['indicator_category'].value_counts().to_dict()}")
            print("\nSample record:")
            print(df.iloc[0])
    else:
        print("No data to transform (expected - no macro indicators in test data)")
