# MarketMind Intelligence Platform V1
# Unit Tests for Quality Checks
# Date: April 24, 2026

import pytest
import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.silver.quality.quality_checks import QualityChecker, QualityCheckResult
from code.bronze.schemas.quality_alert import CheckType, Severity


@pytest.mark.unit
@pytest.mark.quality
class TestQualityCheckResult:
    """Test suite for QualityCheckResult"""
    
    def test_initialization(self):
        """Test QualityCheckResult initialization"""
        result = QualityCheckResult(
            check_type=CheckType.COMPLETENESS,
            passed=True,
            severity=Severity.LOW,
            description="Test check",
            row_count_checked=100,
            failure_count=0
        )
        
        assert result.check_type == CheckType.COMPLETENESS
        assert result.passed
        assert result.severity == Severity.LOW
        assert result.description == "Test check"
        assert result.row_count_checked == 100
        assert result.failure_count == 0
    
    def test_to_quality_alert_returns_none_for_passed(self):
        """Test that passed checks return None"""
        result = QualityCheckResult(
            check_type=CheckType.COMPLETENESS,
            passed=True,
            severity=Severity.LOW,
            description="Test check"
        )
        
        alert = result.to_quality_alert('test_table')
        assert alert is None
    
    def test_to_quality_alert_returns_alert_for_failed(self):
        """Test that failed checks return QualityAlert"""
        result = QualityCheckResult(
            check_type=CheckType.COMPLETENESS,
            passed=False,
            severity=Severity.HIGH,
            description="Test check failed",
            row_count_checked=100,
            failure_count=10
        )
        
        alert = result.to_quality_alert('test_table')
        assert alert is not None
        assert alert.table_name == 'test_table'
        assert alert.check_type == CheckType.COMPLETENESS


@pytest.mark.unit
@pytest.mark.quality
class TestQualityChecker:
    """Test suite for QualityChecker"""
    
    def test_initialization(self):
        """Test QualityChecker initialization"""
        checker = QualityChecker()
        assert checker is not None
        assert hasattr(checker, 'completeness_threshold')
        assert hasattr(checker, 'freshness_threshold_hours')
        assert hasattr(checker, 'uniqueness_threshold')
    
    def test_check_completeness_with_complete_data(self):
        """Test completeness check with no nulls"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.0, 2.0, 3.0]
        })
        
        checker = QualityChecker()
        result = checker.check_completeness(df, ['col1', 'col2', 'col3'])
        
        assert result.check_type == CheckType.COMPLETENESS
        assert result.passed
        assert result.actual_value == 100.0
    
    def test_check_completeness_with_nulls(self):
        """Test completeness check with nulls"""
        df = pd.DataFrame({
            'col1': [1, None, 3],
            'col2': ['a', 'b', None],
            'col3': [1.0, 2.0, 3.0]
        })
        
        checker = QualityChecker()
        result = checker.check_completeness(df, ['col1', 'col2', 'col3'])
        
        assert result.check_type == CheckType.COMPLETENESS
        assert result.failure_count == 2
        assert result.actual_value < 100.0
    
    def test_check_completeness_missing_column(self):
        """Test completeness check with missing column"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
        })
        
        checker = QualityChecker()
        result = checker.check_completeness(df, ['col1', 'col2'])
        
        assert not result.passed
        assert result.severity == Severity.CRITICAL
        assert 'not found' in result.description
    
    def test_check_freshness_with_recent_data(self):
        """Test freshness check with recent data"""
        now_ms = int(datetime.now().timestamp() * 1000)
        df = pd.DataFrame({
            'timestamp': [now_ms - 1000, now_ms - 2000, now_ms - 3000]
        })
        
        checker = QualityChecker()
        result = checker.check_freshness(df, 'timestamp')
        
        assert result.check_type == CheckType.FRESHNESS
        assert result.passed
    
    def test_check_freshness_with_stale_data(self):
        """Test freshness check with old data"""
        old_time = datetime.now() - timedelta(hours=100)
        old_ms = int(old_time.timestamp() * 1000)
        df = pd.DataFrame({
            'timestamp': [old_ms, old_ms, old_ms]
        })
        
        checker = QualityChecker()
        result = checker.check_freshness(df, 'timestamp')
        
        assert result.check_type == CheckType.FRESHNESS
        assert not result.passed
        assert result.failure_count == 3
    
    def test_check_freshness_missing_column(self):
        """Test freshness check with missing timestamp column"""
        df = pd.DataFrame({
            'col1': [1, 2, 3]
        })
        
        checker = QualityChecker()
        result = checker.check_freshness(df, 'timestamp')
        
        assert not result.passed
        assert result.severity == Severity.HIGH
        assert 'not found' in result.description
    
    def test_check_freshness_with_datetime_column(self):
        """Test freshness check with datetime column"""
        now = datetime.now()
        df = pd.DataFrame({
            'timestamp': [now, now - timedelta(hours=1), now - timedelta(hours=2)]
        })
        
        checker = QualityChecker()
        result = checker.check_freshness(df, 'timestamp')
        
        assert result.check_type == CheckType.FRESHNESS
        assert result.passed
    
    def test_check_uniqueness_with_unique_data(self):
        """Test uniqueness check with no duplicates"""
        df = pd.DataFrame({
            'key1': [1, 2, 3],
            'key2': ['a', 'b', 'c']
        })
        
        checker = QualityChecker()
        result = checker.check_uniqueness(df, ['key1', 'key2'])
        
        assert result.check_type == CheckType.UNIQUENESS
        assert result.passed
        assert result.failure_count == 0
    
    def test_check_uniqueness_with_duplicates(self):
        """Test uniqueness check with duplicates"""
        df = pd.DataFrame({
            'key1': [1, 1, 2],
            'key2': ['a', 'a', 'b']
        })
        
        checker = QualityChecker()
        result = checker.check_uniqueness(df, ['key1', 'key2'])
        
        assert result.check_type == CheckType.UNIQUENESS
        assert result.failure_count == 2
        assert result.actual_value < 100.0
    
    def test_check_uniqueness_missing_columns(self):
        """Test uniqueness check with missing key columns"""
        df = pd.DataFrame({
            'col1': [1, 2, 3]
        })
        
        checker = QualityChecker()
        result = checker.check_uniqueness(df, ['key1', 'key2'])
        
        assert not result.passed
        assert result.severity == Severity.CRITICAL
        assert 'not found' in result.description
    
    def test_check_uniqueness_empty_dataframe(self):
        """Test uniqueness check with empty DataFrame"""
        df = pd.DataFrame({'key1': [], 'key2': []})
        
        checker = QualityChecker()
        result = checker.check_uniqueness(df, ['key1', 'key2'])
        
        assert result.passed
        assert result.actual_value == 100.0
    
    def test_check_validity_with_valid_range(self):
        """Test validity check with values in range"""
        df = pd.DataFrame({
            'value': [10, 20, 30]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', min_value=0, max_value=100)
        
        assert result.check_type == CheckType.VALIDITY
        assert result.passed
        assert result.failure_count == 0
    
    def test_check_validity_with_values_below_min(self):
        """Test validity check with values below minimum"""
        df = pd.DataFrame({
            'value': [-10, 20, 30]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', min_value=0)
        
        assert not result.passed
        assert result.failure_count == 1
    
    def test_check_validity_with_values_above_max(self):
        """Test validity check with values above maximum"""
        df = pd.DataFrame({
            'value': [10, 20, 150]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', max_value=100)
        
        assert not result.passed
        assert result.failure_count == 1
    
    def test_check_validity_missing_column(self):
        """Test validity check with missing column"""
        df = pd.DataFrame({
            'col1': [1, 2, 3]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', min_value=0)
        
        assert not result.passed
        assert result.severity == Severity.HIGH
        assert 'not found' in result.description
    
    def test_check_validity_with_only_min(self):
        """Test validity check with only minimum value"""
        df = pd.DataFrame({
            'value': [10, 20, 30]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', min_value=5)
        
        assert result.passed
    
    def test_check_validity_with_only_max(self):
        """Test validity check with only maximum value"""
        df = pd.DataFrame({
            'value': [10, 20, 30]
        })
        
        checker = QualityChecker()
        result = checker.check_validity(df, 'value', max_value=50)
        
        assert result.passed
    
    def test_run_all_checks_for_market_bars(self, valid_ohlcv_data):
        """Test run_all_checks for market_bars table"""
        checker = QualityChecker()
        results = checker.run_all_checks(valid_ohlcv_data, 'market_bars')
        
        assert len(results) > 0
        assert all(isinstance(r, QualityCheckResult) for r in results)
        
        # Should include completeness, uniqueness, and validity checks
        check_types = [r.check_type for r in results]
        assert CheckType.COMPLETENESS in check_types
        assert CheckType.UNIQUENESS in check_types
        assert CheckType.VALIDITY in check_types
    
    def test_run_all_checks_with_invalid_market_bars(self, invalid_ohlcv_data):
        """Test run_all_checks with invalid market bars data"""
        checker = QualityChecker()
        results = checker.run_all_checks(invalid_ohlcv_data, 'market_bars')
        
        # Should have some failures
        failures = [r for r in results if not r.passed]
        assert len(failures) > 0
