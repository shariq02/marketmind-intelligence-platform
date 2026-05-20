# ====================================================================
# Quality Check Framework
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/silver/quality/quality_checks.py
# Purpose: Data quality validation framework for Silver layer
# ====================================================================
"""
Quality Check Framework

Performs data quality checks on Bronze data before Silver transformation:
- Completeness: Required fields not null
- Freshness: Data recency validation
- Uniqueness: No duplicate records
- Validity: Data types and ranges
- Consistency: Cross-field validations
- Schema compliance: Column presence

Usage:
    from code.silver.quality.quality_checks import QualityChecker
    
    checker = QualityChecker()
    
    # Run all checks
    results = checker.run_all_checks(df, 'market_bars')
    
    # Check specific type
    result = checker.check_completeness(df, required_columns)
"""

import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from config import (
    SILVER_CONFIG,
    get_logger,
)

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent / 'bronze' / 'schemas'))

from quality_alert import QualityAlert, Layer, CheckType, Severity, CheckResult

logger = get_logger(__name__)


class QualityCheckResult:
    """Result of a quality check."""
    
    def __init__(
        self,
        check_type: CheckType,
        passed: bool,
        severity: Severity,
        description: str,
        details: Optional[Dict] = None,
        row_count_checked: int = 0,
        failure_count: int = 0,
        threshold_value: Optional[float] = None,
        actual_value: Optional[float] = None
    ):
        self.check_type = check_type
        self.passed = passed
        self.severity = severity
        self.description = description
        self.details = details or {}
        self.row_count_checked = row_count_checked
        self.failure_count = failure_count
        self.threshold_value = threshold_value
        self.actual_value = actual_value
    
    def to_quality_alert(self, table_name: str) -> Optional[QualityAlert]:
        """Convert failed check to QualityAlert."""
        if self.passed:
            return None
        
        return QualityAlert.create_new(
            layer=Layer.SILVER,
            table_name=table_name,
            check_type=self.check_type,
            severity=self.severity,
            check_result=CheckResult.FAIL,
            failure_description=self.description,
            row_count_checked=self.row_count_checked,
            failure_count=self.failure_count,
            threshold_value=self.threshold_value,
            actual_value=self.actual_value,
            failure_details=str(self.details)
        )


class QualityChecker:
    """
    Data quality checker for Silver layer.
    
    Validates Bronze data before transformation.
    """
    
    def __init__(self):
        """Initialize quality checker."""
        self.completeness_threshold = SILVER_CONFIG['completeness_threshold']
        self.freshness_threshold_hours = SILVER_CONFIG['freshness_threshold_hours']
        self.uniqueness_threshold = SILVER_CONFIG['uniqueness_threshold']
        
        logger.info("QualityChecker initialized")
    
    def check_completeness(
        self,
        df: pd.DataFrame,
        required_columns: List[str]
    ) -> QualityCheckResult:
        """
        Check that required columns have no null values.
        
        Args:
            df: DataFrame to check
            required_columns: List of columns that must not be null
        
        Returns:
            QualityCheckResult
        """
        total_rows = len(df)
        
        # Check each required column
        null_counts = {}
        total_nulls = 0
        
        for col in required_columns:
            if col not in df.columns:
                return QualityCheckResult(
                    check_type=CheckType.COMPLETENESS,
                    passed=False,
                    severity=Severity.CRITICAL,
                    description=f"Required column '{col}' not found in DataFrame",
                    row_count_checked=total_rows,
                    failure_count=total_rows
                )
            
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_counts[col] = null_count
                total_nulls += null_count
        
        # Calculate completeness rate
        completeness_rate = 1.0 - (total_nulls / (total_rows * len(required_columns)))
        
        passed = completeness_rate >= self.completeness_threshold
        
        return QualityCheckResult(
            check_type=CheckType.COMPLETENESS,
            passed=passed,
            severity=Severity.HIGH if not passed else Severity.LOW,
            description=f"Completeness check: {completeness_rate*100:.2f}% complete",
            details={'null_counts': null_counts},
            row_count_checked=total_rows * len(required_columns),
            failure_count=total_nulls,
            threshold_value=self.completeness_threshold * 100,
            actual_value=completeness_rate * 100
        )
    
    def check_freshness(
        self,
        df: pd.DataFrame,
        timestamp_column: str
    ) -> QualityCheckResult:
        """
        Check that data is recent (within threshold hours).
        
        Args:
            df: DataFrame to check
            timestamp_column: Column containing timestamps
        
        Returns:
            QualityCheckResult
        """
        if timestamp_column not in df.columns:
            return QualityCheckResult(
                check_type=CheckType.FRESHNESS,
                passed=False,
                severity=Severity.HIGH,
                description=f"Timestamp column '{timestamp_column}' not found",
                row_count_checked=len(df),
                failure_count=len(df)
            )
        
        # Convert to datetime
        if df[timestamp_column].dtype == 'int64':
            # Unix timestamp in milliseconds
            df['_temp_dt'] = pd.to_datetime(df[timestamp_column], unit='ms')
        else:
            df['_temp_dt'] = pd.to_datetime(df[timestamp_column])
        
        # Check freshness
        now = datetime.now()
        threshold = now - timedelta(hours=self.freshness_threshold_hours)
        
        stale_count = (df['_temp_dt'] < threshold).sum()
        total_rows = len(df)
        
        # Clean up temp column
        df.drop('_temp_dt', axis=1, inplace=True)
        
        passed = stale_count == 0
        
        return QualityCheckResult(
            check_type=CheckType.FRESHNESS,
            passed=passed,
            severity=Severity.MEDIUM if not passed else Severity.LOW,
            description=f"Freshness check: {stale_count} stale records (>{self.freshness_threshold_hours}h old)",
            row_count_checked=total_rows,
            failure_count=stale_count,
            threshold_value=float(self.freshness_threshold_hours),
            actual_value=float(stale_count)
        )
    
    def check_uniqueness(
        self,
        df: pd.DataFrame,
        key_columns: List[str]
    ) -> QualityCheckResult:
        """
        Check that key columns have unique combinations.
        
        Args:
            df: DataFrame to check
            key_columns: Columns that should be unique together
        
        Returns:
            QualityCheckResult
        """
        total_rows = len(df)
        
        # Check key columns exist
        missing_cols = [col for col in key_columns if col not in df.columns]
        if missing_cols:
            return QualityCheckResult(
                check_type=CheckType.UNIQUENESS,
                passed=False,
                severity=Severity.CRITICAL,
                description=f"Key columns not found: {missing_cols}",
                row_count_checked=total_rows,
                failure_count=total_rows
            )
        
        # Count duplicates
        duplicate_count = df.duplicated(subset=key_columns, keep=False).sum()
        uniqueness_rate = 1.0 - (duplicate_count / total_rows) if total_rows > 0 else 1.0
        
        passed = uniqueness_rate >= self.uniqueness_threshold
        
        return QualityCheckResult(
            check_type=CheckType.UNIQUENESS,
            passed=passed,
            severity=Severity.HIGH if not passed else Severity.LOW,
            description=f"Uniqueness check: {uniqueness_rate*100:.2f}% unique",
            details={'duplicate_count': duplicate_count},
            row_count_checked=total_rows,
            failure_count=duplicate_count,
            threshold_value=self.uniqueness_threshold * 100,
            actual_value=uniqueness_rate * 100
        )
    
    def check_validity(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> QualityCheckResult:
        """
        Check that column values are within valid range.
        
        Args:
            df: DataFrame to check
            column: Column to validate
            min_value: Minimum allowed value
            max_value: Maximum allowed value
        
        Returns:
            QualityCheckResult
        """
        if column not in df.columns:
            return QualityCheckResult(
                check_type=CheckType.VALIDITY,
                passed=False,
                severity=Severity.HIGH,
                description=f"Column '{column}' not found",
                row_count_checked=len(df),
                failure_count=len(df)
            )
        
        total_rows = len(df)
        invalid_count = 0
        
        if min_value is not None:
            invalid_count += (df[column] < min_value).sum()
        
        if max_value is not None:
            invalid_count += (df[column] > max_value).sum()
        
        passed = invalid_count == 0
        
        return QualityCheckResult(
            check_type=CheckType.VALIDITY,
            passed=passed,
            severity=Severity.HIGH if not passed else Severity.LOW,
            description=f"Validity check for {column}: {invalid_count} invalid values",
            row_count_checked=total_rows,
            failure_count=invalid_count
        )
    
    def run_all_checks(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> List[QualityCheckResult]:
        """
        Run all applicable quality checks for a table.
        
        Args:
            df: DataFrame to check
            table_name: Name of table (for determining checks)
        
        Returns:
            List of QualityCheckResults
        """
        results = []
        
        if table_name == 'market_bars':
            # Completeness
            results.append(self.check_completeness(
                df,
                ['ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            ))
            
            # Uniqueness
            results.append(self.check_uniqueness(
                df,
                ['ticker', 'timestamp', 'granularity']
            ))
            
            # Validity - volume must be >= 0
            results.append(self.check_validity(df, 'volume', min_value=0))
            
            # Validity - prices must be > 0
            results.append(self.check_validity(df, 'open', min_value=0))
            results.append(self.check_validity(df, 'high', min_value=0))
            results.append(self.check_validity(df, 'low', min_value=0))
            results.append(self.check_validity(df, 'close', min_value=0))
        
        return results


# Example usage
if __name__ == '__main__':
    # Test with sample data
    df = pd.DataFrame({
        'ticker': ['AAPL', 'AAPL', 'MSFT'],
        'timestamp': [1767330000000, 1767330000000, 1767330000000],
        'granularity': ['daily', 'daily', 'daily'],
        'open': [272.255, 272.255, 420.0],
        'high': [277.84, 277.84, 425.0],
        'low': [269.0, 269.0, 418.0],
        'close': [271.01, 271.01, 422.5],
        'volume': [37838054.0, 37838054.0, 25000000.0]
    })
    
    checker = QualityChecker()
    results = checker.run_all_checks(df, 'market_bars')
    
    print(f"Ran {len(results)} quality checks:")
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"  {result.check_type.value}: {status} - {result.description}")
