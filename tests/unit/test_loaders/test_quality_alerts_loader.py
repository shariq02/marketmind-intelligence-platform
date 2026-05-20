# MarketMind Intelligence Platform V1
# Unit Tests for Quality Alerts Gold Loader
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.gold.loaders.quality_alerts_loader import QualityAlertsLoader


@pytest.mark.unit
@pytest.mark.loader
class TestQualityAlertsLoader:
    """Test suite for QualityAlertsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = QualityAlertsLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.quality_alerts'
    
    @patch('code.gold.loaders.quality_alerts_loader.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = QualityAlertsLoader()
        conn = loader.get_connection()
        
        assert mock_connect.called
        assert conn == mock_conn
    
    @patch('code.gold.loaders.quality_alerts_loader.psycopg2.connect')
    def test_get_connection_with_config(self, mock_connect):
        """Test connection uses DATABASE_CONFIG"""
        loader = QualityAlertsLoader()
        loader.get_connection()
        
        assert mock_connect.called
        call_kwargs = mock_connect.call_args[1]
        assert 'host' in call_kwargs
        assert 'port' in call_kwargs
        assert 'dbname' in call_kwargs
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            loader = QualityAlertsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_no_files(self, tmp_path):
        """Test reading directory with no files"""
        silver_dir = tmp_path / 'silver' / 'quality_alerts'
        silver_dir.mkdir(parents=True)
        
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            loader = QualityAlertsLoader()
            result = loader.read_silver_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_silver_data_success(self, tmp_path):
        """Test reading Silver data successfully"""
        sample_df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'check_type': 'COMPLETENESS'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'quality_alerts'
        silver_dir.mkdir(parents=True)
        
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            loader = QualityAlertsLoader()
            result = loader.read_silver_data()
            
            assert len(result) > 0
            assert 'alert_id' in result.columns
    
    def test_prepare_for_gold_selects_correct_columns(self):
        """Test column selection for Gold"""
        df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'table_name': 'market_bars',
             'check_type': 'COMPLETENESS', 'severity': 'HIGH', 'check_result': 'FAIL',
             'check_datetime': pd.Timestamp('2024-01-01'), 'resolved': False,
             'resolution_datetime': None, 'time_to_resolution_hours': None,
             'failure_description': 'Test failure', 'row_count_checked': 1000,
             'failure_count': 100, 'failure_rate': 10.0, 'threshold_value': 95.0,
             'actual_value': 90.0, 'pipeline_blocked': False, 'severity_score': 3,
             'impact_score': 0.3, 'is_critical': False, 'is_unresolved': True,
             'extra_column': 'should_be_removed'}
        ])
        
        loader = QualityAlertsLoader()
        result = loader.prepare_for_gold(df)
        
        assert 'extra_column' not in result.columns
        assert 'alert_id' in result.columns
        assert 'layer' in result.columns
    
    @patch('code.gold.loaders.quality_alerts_loader.execute_values')
    def test_load_to_postgres_success(self, mock_execute_values):
        """Test successful load to PostgreSQL"""
        df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'check_type': 'COMPLETENESS'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = QualityAlertsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.quality_alerts_loader.execute_values')
    def test_load_to_postgres_with_empty_dataframe(self, mock_execute_values):
        """Test load with empty DataFrame"""
        df = pd.DataFrame()
        mock_conn = MagicMock()
        
        loader = QualityAlertsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert not mock_execute_values.called
    
    @patch('code.gold.loaders.quality_alerts_loader.execute_values')
    def test_load_to_postgres_uses_on_conflict(self, mock_execute_values):
        """Test that ON CONFLICT clause is used"""
        df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        loader = QualityAlertsLoader()
        loader.load_to_postgres(mock_conn, df)
        
        assert mock_execute_values.called
        sql_arg = mock_execute_values.call_args[0][1]
        assert 'ON CONFLICT' in sql_arg
        assert 'alert_id' in sql_arg
    
    @patch('code.gold.loaders.quality_alerts_loader.execute_values')
    def test_load_to_postgres_handles_error(self, mock_execute_values):
        """Test load error handling"""
        df = pd.DataFrame([
            {'alert_id': '001'}
        ])
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = Exception('Insert failed')
        
        loader = QualityAlertsLoader()
        
        with pytest.raises(Exception):
            loader.load_to_postgres(mock_conn, df)
        
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('code.gold.loaders.quality_alerts_loader.psycopg2.connect')
    def test_get_record_count_success(self, mock_connect):
        """Test getting record count"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (75,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = QualityAlertsLoader()
        count = loader.get_record_count()
        
        assert count == 75
        mock_cursor.execute.assert_called_once()
        assert 'COUNT' in mock_cursor.execute.call_args[0][0]
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_load_all_with_no_data(self, tmp_path):
        """Test load_all when no data exists"""
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            loader = QualityAlertsLoader()
            result = loader.load_all()
            
            assert not result
    
    @patch('code.gold.loaders.quality_alerts_loader.psycopg2.connect')
    def test_load_all_append_mode(self, mock_connect, tmp_path):
        """Test load_all in append mode"""
        sample_df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'table_name': 'market_bars',
             'check_type': 'COMPLETENESS', 'severity': 'HIGH', 'check_result': 'FAIL'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'quality_alerts'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.quality_alerts_loader.execute_values'):
                loader = QualityAlertsLoader()
                result = loader.load_all(mode='append')
                
                assert result
                # Verify TRUNCATE was NOT called
                assert not any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
    
    @patch('code.gold.loaders.quality_alerts_loader.psycopg2.connect')
    def test_load_all_replace_mode(self, mock_connect, tmp_path):
        """Test load_all in replace mode"""
        sample_df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'table_name': 'market_bars',
             'check_type': 'COMPLETENESS', 'severity': 'HIGH', 'check_result': 'FAIL'}
        ])
        
        silver_dir = tmp_path / 'silver' / 'quality_alerts'
        silver_dir.mkdir(parents=True)
        test_file = silver_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('code.gold.loaders.quality_alerts_loader.DATA_DIR', tmp_path):
            with patch('code.gold.loaders.quality_alerts_loader.execute_values'):
                loader = QualityAlertsLoader()
                result = loader.load_all(mode='replace')
                
                assert result
                # Verify TRUNCATE was called
                assert any('TRUNCATE' in str(call) for call in mock_cursor.execute.call_args_list)
