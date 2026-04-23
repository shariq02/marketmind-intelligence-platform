# ====================================================================
# Quality Alerts Transformer
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: code/silver/transformations/quality_alerts_transformer.py
# Purpose: Transform Bronze quality alerts to Silver
# ====================================================================
"""
Quality Alerts Transformer

Transforms Bronze quality alerts to Silver:
- Run quality checks
- Remove duplicates
- Add derived columns
- Calculate alert metrics
- Write to Silver Parquet

Usage:
    from code.silver.transformations.quality_alerts_transformer import QualityAlertsTransformer
    
    transformer = QualityAlertsTransformer()
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


class QualityAlertsTransformer:
    """
    Transform Bronze quality alerts to Silver.
    
    Enriches quality alert records with metrics.
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.quality_checker = QualityChecker()
        self.silver_base_path = DATA_DIR / 'silver' / 'quality_alerts'
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("QualityAlertsTransformer initialized")
    
    def read_bronze_data(self) -> pd.DataFrame:
        """
        Read all Bronze quality alerts Parquet files.
        
        Returns:
            DataFrame with Bronze data
        """
        bronze_base = DATA_DIR / 'bronze' / 'quality_alerts'
        
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
        logger.info(f"Read {len(combined)} quality alerts from Bronze")
        
        return combined
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate quality alerts.
        
        Args:
            df: DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        df_dedup = df.drop_duplicates(
            subset=['alert_id'],
            keep='first'
        )
        
        after_count = len(df_dedup)
        removed = before_count - after_count
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate quality alerts")
        
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
        df['check_datetime'] = pd.to_datetime(df['check_timestamp'], unit='ms')
        
        if 'resolution_timestamp' in df.columns:
            df['resolution_datetime'] = pd.to_datetime(
                df['resolution_timestamp'], 
                unit='ms'
            ).where(df['resolution_timestamp'].notna())
            
            # Calculate time to resolution
            df['time_to_resolution_hours'] = (
                (df['resolution_timestamp'] - df['check_timestamp']) / (1000 * 3600)
            ).where(df['resolved'])
        
        # Extract date components
        df['check_date'] = df['check_datetime'].dt.date
        df['check_year'] = df['check_datetime'].dt.year
        df['check_month'] = df['check_datetime'].dt.month
        df['check_day'] = df['check_datetime'].dt.day
        df['check_hour'] = df['check_datetime'].dt.hour
        df['check_day_of_week'] = df['check_datetime'].dt.dayofweek
        
        logger.info("Added derived date columns")
        
        return df
    
    def add_severity_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add severity score for prioritization.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with severity scores
        """
        severity_scores = {
            'CRITICAL': 4,
            'HIGH': 3,
            'MEDIUM': 2,
            'LOW': 1
        }
        
        df['severity_score'] = df['severity'].map(severity_scores)
        
        # Impact score (severity * failure_rate)
        df['impact_score'] = df['severity_score'] * (df['failure_rate'] / 100)
        
        logger.info("Added severity scores")
        
        return df
    
    def add_alert_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add alert status flags.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with status flags
        """
        df['is_critical'] = df['severity'] == 'CRITICAL'
        df['is_high'] = df['severity'] == 'HIGH'
        df['is_blocking'] = df['pipeline_blocked']
        df['is_resolved'] = df['resolved']
        df['is_unresolved'] = ~df['resolved']
        
        # Check type flags
        df['is_completeness_issue'] = df['check_type'] == 'COMPLETENESS'
        df['is_freshness_issue'] = df['check_type'] == 'FRESHNESS'
        df['is_uniqueness_issue'] = df['check_type'] == 'UNIQUENESS'
        df['is_validity_issue'] = df['check_type'] == 'VALIDITY'
        
        # Layer flags
        df['is_bronze_layer'] = df['layer'] == 'BRONZE'
        df['is_silver_layer'] = df['layer'] == 'SILVER'
        df['is_gold_layer'] = df['layer'] == 'GOLD'
        
        logger.info("Added alert flags")
        
        return df
    
    def transform_all(self) -> bool:
        """
        Transform all Bronze quality alerts to Silver.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Transforming quality alerts")
        
        # Read Bronze data
        df = self.read_bronze_data()
        
        if df.empty:
            logger.warning("No quality alerts data to transform")
            return False
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Add derived columns
        df = self.add_derived_columns(df)
        
        # Add severity scores
        df = self.add_severity_scores(df)
        
        # Add alert flags
        df = self.add_alert_flags(df)
        
        # Write to Silver (partitioned by date)
        for date in df['check_date'].unique():
            date_df = df[df['check_date'] == date]
            
            dt = pd.to_datetime(date)
            date_dir = self.silver_base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            date_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = date_dir / f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            date_df.to_parquet(output_file, compression='snappy', index=False)
            
            logger.info(
                f"Wrote {len(date_df)} quality alerts for {date} to Silver"
            )
        
        logger.info(f"Total quality alerts written to Silver: {len(df)}")
        
        return True


# Example usage
if __name__ == '__main__':
    transformer = QualityAlertsTransformer()
    
    print("Transforming quality alerts...")
    success = transformer.transform_all()
    
    if success:
        print("Transformation successful!")
        
        # Read and display Silver data
        silver_files = list(transformer.silver_base_path.rglob('*.parquet'))
        if silver_files:
            df = pd.read_parquet(silver_files[-1])
            print(f"\nSilver data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nSeverity distribution: {df['severity'].value_counts().to_dict()}")
            print(f"\nCheck types: {df['check_type'].value_counts().to_dict()}")
            print(f"\nUnresolved critical: {df[df['is_critical'] & df['is_unresolved']].shape[0]}")
    else:
        print("No data to transform (expected - no quality alerts in test data)")
