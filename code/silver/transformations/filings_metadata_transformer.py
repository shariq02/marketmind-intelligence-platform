# ====================================================================
# SEC Filings Metadata Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/silver/transformations/filings_metadata_transformer.py
# Purpose: Transform Bronze SEC filings metadata to Silver
# ====================================================================
"""
Filings Metadata Transformer

Transforms Bronze SEC filings metadata to Silver:
- Run quality checks
- Remove duplicates
- Add derived columns
- Categorize filing types
- Write to Silver Parquet

Usage:
    from code.silver.transformations.filings_metadata_transformer import FilingsMetadataTransformer
    
    transformer = FilingsMetadataTransformer()
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


class FilingsMetadataTransformer:
    """
    Transform Bronze SEC filings metadata to Silver.
    
    Standardizes and enriches SEC filing records.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'filings_metadata'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("FilingsMetadataTransformer initialized")
    
    def read_bronze_data(self) -> pd.DataFrame:
        """
        Read all Bronze filings metadata Parquet files.
        
        Returns:
            DataFrame with Bronze data
        """
        bronze_base = DATA_DIR / 'bronze' / 'filings_metadata'
        
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
        logger.info(f"Read {len(combined)} filings from Bronze")
        
        return combined
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate filings.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        df_dedup = df.drop_duplicates(
            subset=['accession_number'],
            keep='first'
        )
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate filings")
        
        return df_dedup
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with derived columns
        """
        # Convert dates to datetime
        df['filing_datetime'] = pd.to_datetime(df['filing_date'])
        
        if 'report_date' in df.columns:
            df['report_datetime'] = pd.to_datetime(df['report_date'])
            
            # Calculate filing lag (days between report date and filing date)
            df['filing_lag_days'] = (df['filing_datetime'] - df['report_datetime']).dt.days
        
        # Extract year/month/quarter from filing date
        df['filing_year'] = df['filing_datetime'].dt.year
        df['filing_month'] = df['filing_datetime'].dt.month
        df['filing_quarter'] = df['filing_datetime'].dt.quarter
        df['filing_day_of_week'] = df['filing_datetime'].dt.dayofweek
        
        # Extract year/quarter from report date (if available)
        if 'report_datetime' in df.columns:
            df['report_year'] = df['report_datetime'].dt.year
            df['report_quarter'] = df['report_datetime'].dt.quarter
        
        logger.info("Added derived date columns")
        
        return df
    
    def categorize_filings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categorize filing types.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with filing category
        """
        def get_filing_category(form_type):
            form_upper = form_type.upper()
            
            if form_upper in ['10-K', '10-K/A']:
                return 'Annual Report'
            elif form_upper in ['10-Q', '10-Q/A']:
                return 'Quarterly Report'
            elif form_upper in ['8-K', '8-K/A']:
                return 'Current Report'
            elif form_upper.startswith('S-'):
                return 'Registration Statement'
            elif form_upper.startswith('DEF 14'):
                return 'Proxy Statement'
            elif form_upper in ['3', '4', '5']:
                return 'Insider Trading'
            elif form_upper.startswith('SC 13'):
                return 'Beneficial Ownership'
            else:
                return 'Other'
        
        df['filing_category'] = df['form_type'].apply(get_filing_category)
        
        # Flag key periodic reports
        df['is_periodic_report'] = df['filing_category'].isin([
            'Annual Report', 'Quarterly Report'
        ])
        
        # Flag amended filings
        df['is_amended'] = df['form_type'].str.contains('/A', case=False, na=False)
        
        logger.info("Categorized filings")
        
        return df
    
    def add_xbrl_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add XBRL-related flags.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with XBRL flags
        """
        # Flag filings with structured data
        df['has_structured_data'] = df['is_xbrl'] | df['is_inline_xbrl']
        
        logger.info("Added XBRL flags")
        
        return df
    
    def transform_all(self) -> bool:
        """
        Transform all Bronze filings metadata to Silver.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Transforming filings metadata")
        
        # Read Bronze data
        df = self.read_bronze_data()
        
        if df.empty:
            logger.warning("No filings metadata to transform")
            return False
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Categorize filings
        df = self.categorize_filings(df)
        
        # Add XBRL flags
        df = self.add_xbrl_flags(df)
        
        # Write to Silver (partitioned by filing year)
        for year in df['filing_year'].unique():
            year_df = df[df['filing_year'] == year]
            
            year_dir = self.silver_base_path / f"year={year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = year_dir / f"filings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            year_df.to_parquet(output_file, compression='snappy', index=False)
            
            logger.info(
                f"Wrote {len(year_df)} filings for year {year} to Silver"
            )
        
        logger.info(f"Total filings written to Silver: {len(df)}")
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = FilingsMetadataTransformer()
    
    print("Transforming filings metadata...")
    success = transformer.transform_all()
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_files = list(transformer.silver_base_path.rglob('*.parquet'))
        if silver_files:
            df = pd.read_parquet(silver_files[-1])
            print(f"\nSilver data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFiling categories: {df['filing_category'].value_counts().to_dict()}")
            print(f"\nXBRL filings: {df['has_structured_data'].sum()}")
    else:
        print("No data to transform (expected - no filings in test data)")
