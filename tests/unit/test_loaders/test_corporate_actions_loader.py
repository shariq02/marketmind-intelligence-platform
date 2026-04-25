# MarketMind Intelligence Platform V1
# Unit Tests for Corporate Actions Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
from code.gold.loaders.corporate_actions_loader import CorporateActionsLoader


@pytest.mark.unit
@pytest.mark.loader
class TestCorporateActionsLoader:
    """Test suite for CorporateActionsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = CorporateActionsLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.corporate_actions'
    
    @patch('code.gold.loaders.corporate_actions_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = CorporateActionsLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.corporate_actions_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = CorporateActionsLoader()
        loader.get_connection()
        
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            loader = CorporateActionsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_no_files(self, tmp_path):
        """Test reading directory with no files"""
        silver_dir = tmp_path / 'silver' / 'corporate_actions'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            loader = CorporateActionsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_success(self, tmp_path):
        """Test reading Silver data successfully"""
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'corporate_actions'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            loader = CorporateActionsLoader()
            result = loader.read_silver_data()
            
            assert len(result) > 0
            assert 'ticker' in result.columns
    
    def test_prepare_for_gold_filters_invalid_splits(self):
        """Test that invalid splits are filtered"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'year': 2024, 'month': 1, 'is_forward_split': True,
             'is_reverse_split': False, 'is_valid_split': True},
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 0.0, 'year': 2024, 'month': 1, 'is_forward_split': False,
             'is_reverse_split': False, 'is_valid_split': False}
        ])
        
        loader = CorporateActionsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert result.iloc[0]['ticker'] == 'AAPL'
    
    def test_prepare_for_gold_filters_invalid_dividends(self):
        """Test that invalid dividends are filtered"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15',
             'cash_amount': 0.25, 'year': 2024, 'month': 1, 'is_valid_dividend': True},
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15',
             'cash_amount': 0.0, 'year': 2024, 'month': 1, 'is_valid_dividend': False}
        ])
        
        loader = CorporateActionsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert result.iloc[0]['ticker'] == 'AAPL'
    
    def test_prepare_for_gold_handles_missing_validation_columns(self):
        """Test prepare when validation columns are missing"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'year': 2024, 'month': 1}
        ])
        
        loader = CorporateActionsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test column selection for Gold"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'ex_dividend_date': None, 'payment_date': None,
             'record_date': None, 'cash_amount': None, 'declaration_date': None,
             'frequency': None, 'year': 2024, 'month': 1, 'is_forward_split': True,
             'is_reverse_split': False, 'is_valid_split': True, 'extra_column': 'remove'}
        ])
        
        loader = CorporateActionsLoader()
        result = loader.prepare_for_gold(df)
        
        assert 'extra_column' not in result.columns
        assert 'ticker' in result.columns
        assert 'action_type' in result.columns
    
    @patch('code.gold.loaders.corporate_actions_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = CorporateActionsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.corporate_actions_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = CorporateActionsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.corporate_actions_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = CorporateActionsLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.corporate_actions_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (250,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = CorporateActionsLoader()
        count = loader.get_record_count()
        
        assert count == 250
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_all_with_no_data(self, tmp_path):
        """Test load_all when no data exists"""
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            loader = CorporateActionsLoader()
            result = loader.load_all()
            
            assert result == False
    
    @patch('code.gold.loaders.corporate_actions_loader.psycopg2.connect')
    def test_load_all_append_mode(self, mock_connect, tmp_path):
        """Test load_all in append mode"""
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'year': 2024, 'month': 1, 'is_valid_split': True,
             'is_forward_split': True, 'is_reverse_split': False}
        ])
        
        silver_dir = tmp_path / 'silver' / 'corporate_actions'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.corporate_actions_loader.execute_values'):
                loader = CorporateActionsLoader()
                result = loader.load_all(mode='append')
                
                assert result == True
                # Verify TRUNCATE was NOT called
                assert not any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.corporate_actions_loader.psycopg2.connect')
    def test_load_all_replace_mode(self, mock_connect, tmp_path):
        """Test load_all in replace mode"""
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'year': 2024, 'month': 1, 'is_valid_split': True,
             'is_forward_split': True, 'is_reverse_split': False}
        ])
        
        silver_dir = tmp_path / 'silver' / 'corporate_actions'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.corporate_actions_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.corporate_actions_loader.execute_values'):
                loader = CorporateActionsLoader()
                result = loader.load_all(mode='replace')
                
                assert result == True
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
