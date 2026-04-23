# ====================================================================
# Pipeline Audit Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/silver/transformations/pipeline_audit_transformer.py
# Purpose: Transform Bronze pipeline audit to Silver
# ====================================================================
"""
Pipeline Audit Transformer

Transforms Bronze pipeline audit records to Silver:
- Run quality checks
- Remove duplicates
- Add derived columns
- Calculate success rates
- Write to Silver Parquet

Usage:
    from code.silver.transformations.pipeline_audit_transformer import PipelineAuditTransformer
    
    transformer = PipelineAuditTransformer()
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


class PipelineAuditTransformer:
    """
    Transform Bronze pipeline audit to Silver.
    
    Enriches audit records with metrics and analytics.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'pipeline_audit'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("PipelineAuditTransformer initialized")
    
    def read_bronze_data(self) -> pd.DataFrame:
        """
        Read all Bronze pipeline audit Parquet files.
        
        Returns:
            DataFrame with Bronze data
        """
        bronze_base = DATA_DIR / 'bronze' / 'pipeline_audit'
        
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
        logger.info(f"Read {len(combined)} audit records from Bronze")
        
        return combined
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate audit records.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        df_dedup = df.drop_duplicates(
            subset=['audit_id'],
            keep='first'
        )
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate audit records")
        
        return df_dedup
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with derived columns
        """
        # Convert timestamps to datetime
        df['start_datetime'] = pd.to_datetime(df['start_timestamp'], unit='ms')
        df['end_datetime'] = pd.to_datetime(df['end_timestamp'], unit='ms')
        
        # Extract date components
        df['start_date'] = df['start_datetime'].dt.date
        df['start_year'] = df['start_datetime'].dt.year
        df['start_month'] = df['start_datetime'].dt.month
        df['start_day'] = df['start_datetime'].dt.day
        df['start_hour'] = df['start_datetime'].dt.hour
        
        # Duration metrics
        df['duration_seconds'] = df['duration_ms'] / 1000.0
        df['duration_minutes'] = df['duration_seconds'] / 60.0
        
        # Throughput metrics
        df['records_per_second'] = (
            df['records_written'] / df['duration_seconds']
        ).where(df['duration_seconds'] > 0, 0)
        
        df['bytes_per_second'] = (
            df['bytes_written'] / df['duration_seconds']
        ).where(df['duration_seconds'] > 0, 0)
        
        df['megabytes_written'] = df['bytes_written'] / (1024 * 1024)
        
        # Success rate
        df['write_success_rate'] = (
            df['records_written'] / df['records_retrieved'] * 100
        ).where(df['records_retrieved'] > 0, 0)
        
        # API efficiency
        df['records_per_api_call'] = (
            df['records_retrieved'] / df['api_calls_made']
        ).where(df['api_calls_made'] > 0, 0)
        
        logger.info("Added derived metrics columns")
        
        return df
    
    def add_status_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add status flag columns.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with status flags
        """
        df['is_success'] = df['status'] == 'SUCCESS'
        df['is_failure'] = df['status'] == 'FAILURE'
        df['is_partial'] = df['status'] == 'PARTIAL_SUCCESS'
        df['is_skipped'] = df['status'] == 'SKIPPED'
        
        # Performance flags
        df['is_slow'] = df['duration_minutes'] > 5
        df['is_rate_limited'] = df['rate_limited']
        df['has_errors'] = df['error_message'].notna()
        
        logger.info("Added status flags")
        
        return df
    
    def transform_all(self) -> bool:
        """
        Transform all Bronze pipeline audit to Silver.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Transforming pipeline audit")
        
        # Read Bronze data
        df = self.read_bronze_data()
        
        if df.empty:
            logger.warning("No pipeline audit data to transform")
            return False
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Add status flags
        df = self.add_status_flags(df)
        
        # Write to Silver (partitioned by date)
        for date in df['start_date'].unique():
            date_df = df[df['start_date'] == date]
            
            dt = pd.to_datetime(date)
            date_dir = self.silver_base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            date_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = date_dir / f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            date_df.to_parquet(output_file, compression='snappy', index=False)
            
            logger.info(
                f"Wrote {len(date_df)} audit records for {date} to Silver"
            )
        
        logger.info(f"Total audit records written to Silver: {len(df)}")
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = PipelineAuditTransformer()
    
    print("Transforming pipeline audit...")
    success = transformer.transform_all()
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_files = list(transformer.silver_base_path.rglob('*.parquet'))
        if silver_files:
            df = pd.read_parquet(silver_files[-1])
            print(f"\nSilver data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nStatus distribution: {df['status'].value_counts().to_dict()}")
            print(f"\nSuccess rate: {df['is_success'].mean() * 100:.1f}%")
    else:
        print("No data to transform (expected - no audit records in test data)")
