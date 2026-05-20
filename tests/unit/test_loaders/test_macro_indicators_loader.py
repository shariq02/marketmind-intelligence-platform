# MarketMind Intelligence Platform V1
# Unit Tests for Macro Indicators Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.gold.loaders.macro_indicators_loader import MacroIndicatorsLoader


@pytest.mark.unit
@pytest.mark.loader
class TestMacroIndicatorsLoader:
    """Test suite for MacroIndicatorsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = MacroIndicatorsLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.macro_indicators'
    
    @patch('code.gold.loaders.macro_indicators_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = MacroIndicatorsLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.macro_indicators_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = MacroIndicatorsLoader()
        loader.get_connection()
        
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            loader = MacroIndicatorsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_no_files(self, tmp_path):
        """Test reading directory with no files"""
        silver_dir = tmp_path / 'silver' / 'macro_indicators'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            loader = MacroIndicatorsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_success(self, tmp_path):
        """Test reading Silver data successfully"""
        sample_df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3}
        ])
        
        silver_dir = tmp_path / 'silver' / 'macro_indicators'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            loader = MacroIndicatorsLoader()
            result = loader.read_silver_data()
            
            assert len(result) > 0
            assert 'indicator_name' in result.columns
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test column selection for Gold"""
        df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3,
             'unit': 'percent', 'frequency': 'monthly', 'forecast_value': 0.2,
             'previous_value': 0.1, 'year': 2024, 'month': 1, 'quarter': 1,
             'value_change': 0.2, 'value_pct_change': 200.0, 'indicator_category': 'Inflation',
             'source_url': 'http://test.com', 'extra_column': 'should_be_removed'}
        ])
        
        loader = MacroIndicatorsLoader()
        result = loader.prepare_for_gold(df)
        
        assert 'extra_column' not in result.columns
        assert 'indicator_name' in result.columns
        assert 'value' in result.columns
    
    def test_prepare_for_gold_handles_infinity(self):
        """Test that infinity values are replaced with None"""
        df = pd.DataFrame([
            {'indicator_name': 'TEST', 'date': '2024-01-01', 'value': np.inf,
             'value_change': -np.inf, 'value_pct_change': 100.0}
        ])
        
        loader = MacroIndicatorsLoader()
        result = loader.prepare_for_gold(df)
        
        assert pd.isna(result.iloc[0]['value'])
        assert pd.isna(result.iloc[0]['value_change'])
    
    def test_prepare_for_gold_handles_nan(self):
        """Test that NaN values are replaced with None"""
        df = pd.DataFrame([
            {'indicator_name': 'TEST', 'date': '2024-01-01', 'value': np.nan,
             'value_change': 0.5, 'value_pct_change': np.nan}
        ])
        
        loader = MacroIndicatorsLoader()
        result = loader.prepare_for_gold(df)
        
        assert pd.isna(result.iloc[0]['value'])
        assert pd.isna(result.iloc[0]['value_pct_change'])
    
    @patch('code.gold.loaders.macro_indicators_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = MacroIndicatorsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.macro_indicators_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = MacroIndicatorsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.macro_indicators_loader.execute_values')
    def test_load_to_postgres_uses_on_conflict(self, mock_execute_values):
        """Test that ON CONFLICT clause is used"""
        df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = MacroIndicatorsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        # Check SQL contains ON CONFLICT
        assert mock_execute_values.called
        sql_arg = mock_execute_values.call_args[0][1]
        assert 'ON CONFLICT' in sql_arg
    
    @patch('code.gold.loaders.macro_indicators_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'indicator_name': 'TEST', 'date': '2024-01-01'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = MacroIndicatorsLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.macro_indicators_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5000,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = MacroIndicatorsLoader()
        count = loader.get_record_count()
        
        assert count == 5000
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_all_with_no_data(self, tmp_path):
        """Test load_all when no data exists"""
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            loader = MacroIndicatorsLoader()
            result = loader.load_all()
            
            assert not result
    
    @patch('code.gold.loaders.macro_indicators_loader.psycopg2.connect')
    def test_load_all_append_mode(self, mock_connect, tmp_path):
        """Test load_all in append mode"""
        sample_df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3,
             'unit': 'percent', 'frequency': 'monthly'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'macro_indicators'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.macro_indicators_loader.execute_values'):
                loader = MacroIndicatorsLoader()
                result = loader.load_all(mode='append')
                
                assert result
                # Verify TRUNCATE was NOT called
                assert not any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.macro_indicators_loader.psycopg2.connect')
    def test_load_all_replace_mode(self, mock_connect, tmp_path):
        """Test load_all in replace mode"""
        sample_df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3,
             'unit': 'percent', 'frequency': 'monthly'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'macro_indicators'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.macro_indicators_loader.execute_values'):
                loader = MacroIndicatorsLoader()
                result = loader.load_all(mode='replace')
                
                assert result
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
