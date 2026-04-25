# MarketMind Intelligence Platform V1
# Unit Tests for Market Bars Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.gold.loaders.market_bars_loader import MarketBarsLoader


@pytest.mark.unit
@pytest.mark.loader
class TestMarketBarsLoader:
    """Test suite for MarketBarsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = MarketBarsLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.ohlcv_bars'
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = MarketBarsLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = MarketBarsLoader()
        loader.get_connection()
        
        # Verify connect was called with config parameters
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_partition_path_not_exists(self, tmp_path):
        """Test reading non-existent partition"""
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            loader = MarketBarsLoader()
            result = loader.read_silver_partition('2026-01-02')
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_partition_no_files(self, tmp_path):
        """Test reading partition with no files"""
        # Create directory structure but no files
        silver_dir = tmp_path / 'silver' / 'market_bars' / 'year=2026' / 'month=01' / 'day=02'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            loader = MarketBarsLoader()
            result = loader.read_silver_partition('2026-01-02')
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_partition_success(self, tmp_path):
        """Test reading partition with data"""
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'open': 100, 'high': 105, 'low': 99, 'close': 103}
        ])
        
        # Create directory and file
        silver_dir = tmp_path / 'silver' / 'market_bars' / 'year=2026' / 'month=01' / 'day=02'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            loader = MarketBarsLoader()
            result = loader.read_silver_partition('2026-01-02')
            
            assert len(result) > 0
            assert 'ticker' in result.columns
    
    def test_prepare_for_gold_filters_invalid_ohlc(self):
        """Test that invalid OHLC records are filtered"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': True},
            {'ticker': 'MSFT', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 95, 'low': 105, 'close': 102, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': False},
        ])
        
        loader = MarketBarsLoader()
        result = loader.prepare_for_gold(df)
        
        # Only valid record should remain
        assert len(result) == 1
        assert result.iloc[0]['ticker'] == 'AAPL'
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test that only Gold columns are selected"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': True,
             'extra_column': 'should_be_removed'},
        ])
        
        loader = MarketBarsLoader()
        result = loader.prepare_for_gold(df)
        
        # Extra column should not be in result
        assert 'extra_column' not in result.columns
        assert 'ticker' in result.columns
        assert 'open' in result.columns
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_delete_partition_success(self, mock_connect):
        """Test deleting partition data"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = MarketBarsLoader()
        loader.delete_partition(mock_conn, '2026-01-02')
        
        # Verify DELETE was executed
        mock_cursor.execute.assert_called_once()
        assert 'DELETE' in mock_cursor.execute.call_args[0][0]
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_delete_partition_handles_error(self, mock_connect):
        """Test delete partition error handling"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception('Database error')
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = MarketBarsLoader()
        
        with pytest.raises(Exception):
            loader.delete_partition(mock_conn, '2026-01-02')
        
        # Verify rollback was called
        mock_conn.rollback.assert_called_once()
    
    @patch('code.gold.loaders.market_bars_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'open': 100, 'high': 105, 'low': 99, 'close': 103}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = MarketBarsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        # Verify execute_values was called
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.market_bars_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = MarketBarsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        # execute_values should not be called
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.market_bars_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = MarketBarsLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        # Verify rollback
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (12345,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = MarketBarsLoader()
        count = loader.get_record_count()
        
        assert count == 12345
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_partition_with_no_data(self, tmp_path):
        """Test load_partition when no data exists"""
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            loader = MarketBarsLoader()
            result = loader.load_partition('2026-01-02')
            
            assert not result
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_load_partition_upsert_mode(self, mock_connect, tmp_path):
        """Test load_partition in upsert mode"""
        # Create test data
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': True}
        ])
        
        # Create directory and file
        silver_dir = tmp_path / 'silver' / 'market_bars' / 'year=2026' / 'month=01' / 'day=02'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.market_bars_loader.execute_values'):
                loader = MarketBarsLoader()
                result = loader.load_partition('2026-01-02', mode='upsert')
                
                assert result
                # Verify DELETE was called (upsert deletes first)
                assert any('DELETE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_load_partition_replace_mode(self, mock_connect, tmp_path):
        """Test load_partition in replace mode"""
        # Create test data
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': True}
        ])
        
        silver_dir = tmp_path / 'silver' / 'market_bars' / 'year=2026' / 'month=01' / 'day=02'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.market_bars_loader.execute_values'):
                loader = MarketBarsLoader()
                result = loader.load_partition('2026-01-02', mode='replace')
                
                assert result
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
