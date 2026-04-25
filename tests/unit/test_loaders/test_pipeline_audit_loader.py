# MarketMind Intelligence Platform V1
# Unit Tests for Pipeline Audit Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
from code.gold.loaders.pipeline_audit_loader import PipelineAuditLoader


@pytest.mark.unit
@pytest.mark.loader
class TestPipelineAuditLoader:
    """Test suite for PipelineAuditLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = PipelineAuditLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.pipeline_audit'
    
    @patch('code.gold.loaders.pipeline_audit_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = PipelineAuditLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.pipeline_audit_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = PipelineAuditLoader()
        loader.get_connection()
        
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            loader = PipelineAuditLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_no_files(self, tmp_path):
        """Test reading directory with no files"""
        silver_dir = tmp_path / 'silver' / 'pipeline_audit'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            loader = PipelineAuditLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_success(self, tmp_path):
        """Test reading Silver data successfully"""
        sample_df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'status': 'SUCCESS'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'pipeline_audit'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            loader = PipelineAuditLoader()
            result = loader.read_silver_data()
            
            assert len(result) > 0
            assert 'audit_id' in result.columns
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test column selection for Gold"""
        df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'execution_mode': 'full',
             'status': 'SUCCESS', 'start_datetime': pd.Timestamp('2024-01-01'),
             'end_datetime': pd.Timestamp('2024-01-01 00:10:00'), 'duration_seconds': 600,
             'duration_minutes': 10, 'records_retrieved': 1000, 'records_written': 1000,
             'bytes_written': 1048576, 'megabytes_written': 1.0, 'api_calls_made': 10,
             'rate_limited': False, 'write_success_rate': 100.0, 'records_per_second': 1.67,
             'records_per_api_call': 100.0, 'is_success': True, 'is_failure': False,
             'is_slow': False, 'error_message': None, 'extra_column': 'should_be_removed'}
        ])
        
        loader = PipelineAuditLoader()
        result = loader.prepare_for_gold(df)
        
        assert 'extra_column' not in result.columns
        assert 'audit_id' in result.columns
        assert 'connector' in result.columns
    
    @patch('code.gold.loaders.pipeline_audit_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'status': 'SUCCESS'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = PipelineAuditLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.pipeline_audit_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = PipelineAuditLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.pipeline_audit_loader.execute_values')
    def test_load_to_postgres_uses_on_conflict(self, mock_execute_values):
        """Test that ON CONFLICT clause is used"""
        df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = PipelineAuditLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        sql_arg = mock_execute_values.call_args[0][1]
        assert 'ON CONFLICT' in sql_arg
        assert 'audit_id' in sql_arg
    
    @patch('code.gold.loaders.pipeline_audit_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'audit_id': '001'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = PipelineAuditLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.pipeline_audit_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (500,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = PipelineAuditLoader()
        count = loader.get_record_count()
        
        assert count == 500
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_all_with_no_data(self, tmp_path):
        """Test load_all when no data exists"""
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            loader = PipelineAuditLoader()
            result = loader.load_all()
            
            assert result == False
    
    @patch('code.gold.loaders.pipeline_audit_loader.psycopg2.connect')
    def test_load_all_append_mode(self, mock_connect, tmp_path):
        """Test load_all in append mode"""
        sample_df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'execution_mode': 'full',
             'status': 'SUCCESS', 'records_written': 1000}
        ])
        
        silver_dir = tmp_path / 'silver' / 'pipeline_audit'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.pipeline_audit_loader.execute_values'):
                loader = PipelineAuditLoader()
                result = loader.load_all(mode='append')
                
                assert result == True
                # Verify TRUNCATE was NOT called
                assert not any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.pipeline_audit_loader.psycopg2.connect')
    def test_load_all_replace_mode(self, mock_connect, tmp_path):
        """Test load_all in replace mode"""
        sample_df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'execution_mode': 'full',
             'status': 'SUCCESS', 'records_written': 1000}
        ])
        
        silver_dir = tmp_path / 'silver' / 'pipeline_audit'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.pipeline_audit_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.pipeline_audit_loader.execute_values'):
                loader = PipelineAuditLoader()
                result = loader.load_all(mode='replace')
                
                assert result == True
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
