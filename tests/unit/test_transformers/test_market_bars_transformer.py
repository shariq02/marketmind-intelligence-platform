# MarketMind Intelligence Platform V1
# Unit Tests for Market Bars Transformer
# Date: April 23, 2026

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.silver.transformations.market_bars_transformer import MarketBarsTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestMarketBarsTransformer:
    """Test suite for MarketBarsTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = MarketBarsTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_with_no_duplicates(self, sample_market_bars_df):
        """Test remove_duplicates with no duplicate data"""
        transformer = MarketBarsTransformer()
        result = transformer.remove_duplicates(sample_market_bars_df)
        
        # No duplicates should be removed
        assert len(result) == len(sample_market_bars_df)
    
    def test_remove_duplicates_with_duplicates(self, sample_market_bars_df):
        """Test remove_duplicates with duplicate data"""
        # Create data with duplicates
        df_with_dupes = pd.concat([sample_market_bars_df, sample_market_bars_df.head(2)])
        
        transformer = MarketBarsTransformer()
        result = transformer.remove_duplicates(df_with_dupes)
        
        # Duplicates should be removed
        assert len(result) == len(sample_market_bars_df)
        
        # No duplicates should remain
        assert not result.duplicated(subset=['ticker', 'timestamp', 'granularity']).any()
    
    def test_remove_duplicates_keeps_first(self, sample_market_bars_df):
        """Test that remove_duplicates keeps first occurrence"""
        # Create duplicate with different volume
        df = sample_market_bars_df.copy()
        duplicate = df.iloc[0:1].copy()
        duplicate['volume'] = 99999999
        df_with_dupe = pd.concat([df, duplicate])
        
        transformer = MarketBarsTransformer()
        result = transformer.remove_duplicates(df_with_dupe)
        
        # Should keep first (original volume)
        first_row = result[
            (result['ticker'] == df.iloc[0]['ticker']) & 
            (result['timestamp'] == df.iloc[0]['timestamp'])
        ].iloc[0]
        
        assert first_row['volume'] == df.iloc[0]['volume']
    
    def test_add_derived_columns_adds_date(self, sample_market_bars_df):
        """Test that add_derived_columns adds date column"""
        transformer = MarketBarsTransformer()
        result = transformer.add_derived_columns(sample_market_bars_df)
        
        # Check date column exists
        assert 'date' in result.columns
        assert result['date'].notna().all()
    
    def test_add_derived_columns_adds_time_components(self, sample_market_bars_df):
        """Test that add_derived_columns adds year, month, day"""
        transformer = MarketBarsTransformer()
        result = transformer.add_derived_columns(sample_market_bars_df)
        
        # Check time component columns exist
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert 'day' in result.columns
        assert 'hour' in result.columns
        assert 'minute' in result.columns
        
        # Check values are valid
        assert result['year'].between(1900, 2100).all()
        assert result['month'].between(1, 12).all()
        assert result['day'].between(1, 31).all()
    
    def test_add_derived_columns_adds_day_of_week(self, sample_market_bars_df):
        """Test that add_derived_columns adds day_of_week"""
        transformer = MarketBarsTransformer()
        result = transformer.add_derived_columns(sample_market_bars_df)
        
        # Check day_of_week exists and is valid (0-6)
        assert 'day_of_week' in result.columns
        assert result['day_of_week'].between(0, 6).all()
    
    def test_add_derived_columns_adds_trading_day_flag(self, sample_market_bars_df):
        """Test that add_derived_columns adds is_trading_day flag"""
        transformer = MarketBarsTransformer()
        result = transformer.add_derived_columns(sample_market_bars_df)
        
        # Check is_trading_day exists
        assert 'is_trading_day' in result.columns
        
        # Check it's boolean
        assert result['is_trading_day'].dtype == bool
        
        # Check Monday-Friday are True, Saturday-Sunday are False
        weekdays = result[result['day_of_week'].isin([0, 1, 2, 3, 4])]
        weekends = result[result['day_of_week'].isin([5, 6])]
        
        if len(weekdays) > 0:
            assert weekdays['is_trading_day'].all()
        if len(weekends) > 0:
            assert not weekends['is_trading_day'].any()
    
    def test_validate_ohlc_with_valid_data(self, valid_ohlcv_data):
        """Test validate_ohlc with valid OHLC relationships"""
        transformer = MarketBarsTransformer()
        result = transformer.validate_ohlc(valid_ohlcv_data)
        
        # Check is_valid_ohlc column added
        assert 'is_valid_ohlc' in result.columns
        
        # All should be valid
        assert result['is_valid_ohlc'].all()
    
    def test_validate_ohlc_with_invalid_data(self, invalid_ohlcv_data):
        """Test validate_ohlc with invalid OHLC relationships"""
        transformer = MarketBarsTransformer()
        result = transformer.validate_ohlc(invalid_ohlcv_data)
        
        # Check is_valid_ohlc column added
        assert 'is_valid_ohlc' in result.columns
        
        # Should flag invalid records
        assert not result['is_valid_ohlc'].all()
        invalid_count = (~result['is_valid_ohlc']).sum()
        assert invalid_count > 0
    
    def test_validate_ohlc_high_less_than_low(self):
        """Test validate_ohlc detects high < low"""
        df = pd.DataFrame([{
            'ticker': 'TEST',
            'timestamp': 1704153600000,
            'open': 100,
            'high': 95,  # Invalid: high < low
            'low': 105,
            'close': 102,
            'volume': 1000000
        }])
        
        transformer = MarketBarsTransformer()
        result = transformer.validate_ohlc(df)
        
        assert not result.iloc[0]['is_valid_ohlc']
    
    def test_validate_ohlc_open_outside_range(self):
        """Test validate_ohlc detects open outside high-low range"""
        df = pd.DataFrame([{
            'ticker': 'TEST',
            'timestamp': 1704153600000,
            'open': 110,  # Invalid: open > high
            'high': 105,
            'low': 95,
            'close': 100,
            'volume': 1000000
        }])
        
        transformer = MarketBarsTransformer()
        result = transformer.validate_ohlc(df)
        
        assert not result.iloc[0]['is_valid_ohlc']
    
    def test_validate_ohlc_close_outside_range(self):
        """Test validate_ohlc detects close outside high-low range"""
        df = pd.DataFrame([{
            'ticker': 'TEST',
            'timestamp': 1704153600000,
            'open': 100,
            'high': 105,
            'low': 95,
            'close': 90,  # Invalid: close < low
            'volume': 1000000
        }])
        
        transformer = MarketBarsTransformer()
        result = transformer.validate_ohlc(df)
        
        assert not result.iloc[0]['is_valid_ohlc']
    
    def test_apply_quality_checks_returns_tuple(self, sample_market_bars_df):
        """Test apply_quality_checks returns (df, results)"""
        transformer = MarketBarsTransformer()
        result = transformer.apply_quality_checks(sample_market_bars_df)
        
        # Should return tuple
        assert isinstance(result, tuple)
        assert len(result) == 2
        
        # First element should be DataFrame
        assert isinstance(result[0], pd.DataFrame)
        
        # Second element should be list of quality results
        assert isinstance(result[1], list)
    
    def test_apply_quality_checks_preserves_data(self, sample_market_bars_df):
        """Test apply_quality_checks preserves DataFrame"""
        transformer = MarketBarsTransformer()
        df_result, quality_results = transformer.apply_quality_checks(sample_market_bars_df)
        
        # Data should be preserved
        assert len(df_result) == len(sample_market_bars_df)
    
    @patch('code.silver.transformations.market_bars_transformer.get_bronze_data_path')
    @patch('code.silver.transformations.market_bars_transformer.pd.read_parquet')
    def test_read_bronze_partition_success(self, mock_read_parquet, mock_get_path, sample_market_bars_df):
        """Test read_bronze_partition with existing data"""
        # Mock path exists
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.glob.return_value = [Path('test.parquet')]
        mock_get_path.return_value = mock_path
        
        # Mock parquet read
        mock_read_parquet.return_value = sample_market_bars_df
        
        transformer = MarketBarsTransformer()
        result = transformer.read_bronze_partition('2026-01-02')
        
        # Should return DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
    
    @patch('code.silver.transformations.market_bars_transformer.get_bronze_data_path')
    def test_read_bronze_partition_no_files(self, mock_get_path):
        """Test read_bronze_partition with no files"""
        # Mock path exists but no parquet files
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.glob.return_value = []
        mock_get_path.return_value = mock_path
        
        transformer = MarketBarsTransformer()
        result = transformer.read_bronze_partition('2026-01-02')
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch('code.silver.transformations.market_bars_transformer.get_bronze_data_path')
    def test_read_bronze_partition_path_not_exists(self, mock_get_path):
        """Test read_bronze_partition with non-existent path"""
        # Mock path doesn't exist
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_get_path.return_value = mock_path
        
        transformer = MarketBarsTransformer()
        result = transformer.read_bronze_partition('2026-01-02')
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_methods_handle_empty_dataframe(self):
        """Test that all methods handle empty DataFrame gracefully"""
        transformer = MarketBarsTransformer()
        empty_df = pd.DataFrame()
        
        # remove_duplicates
        result = transformer.remove_duplicates(empty_df)
        assert result.empty
        
        # validate_ohlc should add is_valid_ohlc column even to empty df
        result = transformer.validate_ohlc(pd.DataFrame(columns=['open', 'high', 'low', 'close']))
        assert 'is_valid_ohlc' in result.columns
