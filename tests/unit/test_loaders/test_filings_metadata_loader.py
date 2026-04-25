# MarketMind Intelligence Platform V1
# Unit Tests for Filings Metadata Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
from code.gold.loaders.filings_metadata_loader import FilingsMetadataLoader


@pytest.mark.unit
@pytest.mark.loader
class TestFilingsMetadataLoader:
    """Test suite for FilingsMetadataLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = FilingsMetadataLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.sec_filings'
    
    @patch('code.gold.loaders.filings_metadata_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = FilingsMetadataLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.filings_metadata_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = FilingsMetadataLoader()
        loader.get_connection()
        
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            loader = FilingsMetadataLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_no_files(self, tmp_path):
        """Test reading directory with no files"""
        silver_dir = tmp_path / 'silver' / 'filings_metadata'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            loader = FilingsMetadataLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_success(self, tmp_path):
        """Test reading Silver data successfully"""
        sample_df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL', 'form_type': '10-K'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'filings_metadata'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            loader = FilingsMetadataLoader()
            result = loader.read_silver_data()
            
            assert len(result) > 0
            assert 'accession_number' in result.columns
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test column selection for Gold"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL', 'cik': '0000320193',
             'company_name': 'Apple Inc.', 'form_type': '10-K', 'filing_date': '2024-01-15',
             'report_date': '2023-12-31', 'filing_year': 2024, 'filing_quarter': 1,
             'filing_category': 'Annual Report', 'is_periodic_report': True, 'is_amended': False,
             'is_xbrl': True, 'is_inline_xbrl': False, 'has_structured_data': True,
             'filing_lag_days': 15, 'document_url': 'http://test.com',
             'extra_column': 'should_be_removed'}
        ])
        
        loader = FilingsMetadataLoader()
        result = loader.prepare_for_gold(df)
        
        assert 'extra_column' not in result.columns
        assert 'accession_number' in result.columns
        assert 'ticker' in result.columns
    
    def test_prepare_for_gold_fills_null_ticker(self):
        """Test that NULL tickers are filled with UNKNOWN"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': None, 'cik': '0000320193',
             'form_type': '10-K', 'filing_date': '2024-01-15'}
        ])
        
        loader = FilingsMetadataLoader()
        result = loader.prepare_for_gold(df)
        
        assert result.iloc[0]['ticker'] == 'UNKNOWN'
    
    @patch('code.gold.loaders.filings_metadata_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL', 'form_type': '10-K'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = FilingsMetadataLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.filings_metadata_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = FilingsMetadataLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.filings_metadata_loader.execute_values')
    def test_load_to_postgres_uses_on_conflict(self, mock_execute_values):
        """Test that ON CONFLICT clause is used"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = FilingsMetadataLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        sql_arg = mock_execute_values.call_args[0][1]
        assert 'ON CONFLICT' in sql_arg
        assert 'accession_number' in sql_arg
    
    @patch('code.gold.loaders.filings_metadata_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = FilingsMetadataLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.filings_metadata_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1500,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = FilingsMetadataLoader()
        count = loader.get_record_count()
        
        assert count == 1500
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_all_with_no_data(self, tmp_path):
        """Test load_all when no data exists"""
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            loader = FilingsMetadataLoader()
            result = loader.load_all()
            
            assert result == False
    
    @patch('code.gold.loaders.filings_metadata_loader.psycopg2.connect')
    def test_load_all_append_mode(self, mock_connect, tmp_path):
        """Test load_all in append mode"""
        sample_df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL', 
             'cik': '0000320193', 'form_type': '10-K'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'filings_metadata'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.filings_metadata_loader.execute_values'):
                loader = FilingsMetadataLoader()
                result = loader.load_all(mode='append')
                
                assert result == True
                # Verify TRUNCATE was NOT called
                assert not any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.filings_metadata_loader.psycopg2.connect')
    def test_load_all_replace_mode(self, mock_connect, tmp_path):
        """Test load_all in replace mode"""
        sample_df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL',
             'cik': '0000320193', 'form_type': '10-K'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'filings_metadata'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.filings_metadata_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.filings_metadata_loader.execute_values'):
                loader = FilingsMetadataLoader()
                result = loader.load_all(mode='replace')
                
                assert result == True
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
