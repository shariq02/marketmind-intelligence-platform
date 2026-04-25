# MarketMind Intelligence Platform V1
# Unit Tests for Pipeline Audit Transformer
# Date: April 24, 2026

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from code.silver.transformations.pipeline_audit_transformer import PipelineAuditTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestPipelineAuditTransformer:
    """Test suite for PipelineAuditTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = PipelineAuditTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_with_no_duplicates(self):
        """Test remove_duplicates with no duplicate data"""
        df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS'},
            {'audit_id': '002', 'status': 'FAILURE'},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
    
    def test_remove_duplicates_with_duplicates(self):
        """Test remove_duplicates with duplicate data"""
        df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS'},
            {'audit_id': '001', 'status': 'SUCCESS'},
            {'audit_id': '002', 'status': 'FAILURE'},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
        assert not result.duplicated(subset=['audit_id']).any()
    
    def test_add_derived_columns_adds_datetime(self):
        """Test that add_derived_columns adds datetime columns"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000, 
             'duration_ms': 3600000, 'records_written': 1000, 'records_retrieved': 1000, 
             'bytes_written': 1048576, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'start_datetime' in result.columns
        assert 'end_datetime' in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result['start_datetime'])
        assert pd.api.types.is_datetime64_any_dtype(result['end_datetime'])
    
    def test_add_derived_columns_adds_date_components(self):
        """Test that date components are added"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000,
             'duration_ms': 3600000, 'records_written': 1000, 'records_retrieved': 1000,
             'bytes_written': 1048576, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'start_date' in result.columns
        assert 'start_year' in result.columns
        assert 'start_month' in result.columns
        assert 'start_day' in result.columns
        assert 'start_hour' in result.columns
    
    def test_add_derived_columns_calculates_duration_metrics(self):
        """Test that duration metrics are calculated"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000,
             'duration_ms': 60000, 'records_written': 1000, 'records_retrieved': 1000,
             'bytes_written': 1048576, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'duration_seconds' in result.columns
        assert 'duration_minutes' in result.columns
        assert result.iloc[0]['duration_seconds'] == 60.0
        assert result.iloc[0]['duration_minutes'] == 1.0
    
    def test_add_derived_columns_calculates_throughput(self):
        """Test that throughput metrics are calculated"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000,
             'duration_ms': 10000, 'records_written': 100, 'records_retrieved': 100,
             'bytes_written': 1000, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'records_per_second' in result.columns
        assert 'bytes_per_second' in result.columns
        assert 'megabytes_written' in result.columns
        assert result.iloc[0]['records_per_second'] == 10.0  # 100 records / 10 seconds
        assert result.iloc[0]['bytes_per_second'] == 100.0  # 1000 bytes / 10 seconds
    
    def test_add_derived_columns_calculates_success_rate(self):
        """Test that success rate is calculated"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000,
             'duration_ms': 10000, 'records_written': 80, 'records_retrieved': 100,
             'bytes_written': 1000, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'write_success_rate' in result.columns
        assert result.iloc[0]['write_success_rate'] == 80.0  # 80/100 * 100
    
    def test_add_derived_columns_calculates_api_efficiency(self):
        """Test that API efficiency is calculated"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704157200000,
             'duration_ms': 10000, 'records_written': 100, 'records_retrieved': 100,
             'bytes_written': 1000, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'records_per_api_call' in result.columns
        assert result.iloc[0]['records_per_api_call'] == 10.0  # 100 / 10
    
    def test_add_derived_columns_handles_zero_duration(self):
        """Test handling of zero duration"""
        df = pd.DataFrame([
            {'audit_id': '001', 'start_timestamp': 1704153600000, 'end_timestamp': 1704153600000,
             'duration_ms': 0, 'records_written': 100, 'records_retrieved': 100,
             'bytes_written': 1000, 'api_calls_made': 10},
        ])
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_derived_columns(df)
        
        assert result.iloc[0]['records_per_second'] == 0
        assert result.iloc[0]['bytes_per_second'] == 0
    
    def test_add_status_flags_adds_status_flags(self):
        """Test that status flags are added"""
        df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS', 'duration_ms': 60000, 'rate_limited': False, 'error_message': None},
            {'audit_id': '002', 'status': 'FAILURE', 'duration_ms': 120000, 'rate_limited': True, 'error_message': 'Error'},
            {'audit_id': '003', 'status': 'PARTIAL_SUCCESS', 'duration_ms': 360000, 'rate_limited': False, 'error_message': None},
            {'audit_id': '004', 'status': 'SKIPPED', 'duration_ms': 0, 'rate_limited': False, 'error_message': None},
        ])
        
        # Add duration_minutes column
        df['duration_minutes'] = df['duration_ms'] / 60000.0
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_status_flags(df)
        
        assert 'is_success' in result.columns
        assert 'is_failure' in result.columns
        assert 'is_partial' in result.columns
        assert 'is_skipped' in result.columns
        
        assert result.iloc[0]['is_success'] == True
        assert result.iloc[1]['is_failure'] == True
        assert result.iloc[2]['is_partial'] == True
        assert result.iloc[3]['is_skipped'] == True
    
    def test_add_status_flags_adds_performance_flags(self):
        """Test that performance flags are added"""
        df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS', 'duration_ms': 360000, 'rate_limited': True, 'error_message': 'Error'},
        ])
        
        df['duration_minutes'] = df['duration_ms'] / 60000.0
        
        transformer = PipelineAuditTransformer()
        result = transformer.add_status_flags(df)
        
        assert 'is_slow' in result.columns
        assert 'is_rate_limited' in result.columns
        assert 'has_errors' in result.columns
        
        assert result.iloc[0]['is_slow'] == True
        assert result.iloc[0]['is_rate_limited'] == True
        assert result.iloc[0]['has_errors'] == True
    
    def test_read_bronze_data_path_not_exists(self, tmp_path):
        """Test read_bronze_data with non-existent path"""
        with patch('code.silver.transformations.pipeline_audit_transformer.DATA_DIR', tmp_path):
            transformer = PipelineAuditTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_no_files(self, tmp_path):
        """Test read_bronze_data with no files"""
        bronze_dir = tmp_path / 'bronze' / 'pipeline_audit'
        bronze_dir.mkdir(parents=True)
        
        with patch('code.silver.transformations.pipeline_audit_transformer.DATA_DIR', tmp_path):
            transformer = PipelineAuditTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_success(self, tmp_path):
        """Test read_bronze_data with existing files"""
        sample_df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS'},
        ])
        
        bronze_dir = tmp_path / 'bronze' / 'pipeline_audit'
        bronze_dir.mkdir(parents=True)
        
        test_file = bronze_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.silver.transformations.pipeline_audit_transformer.DATA_DIR', tmp_path):
            transformer = PipelineAuditTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
    
    def test_full_transformation_pipeline(self):
        """Test complete transformation pipeline"""
        df = pd.DataFrame([
            {'audit_id': '001', 'status': 'SUCCESS', 'start_timestamp': 1704153600000, 
             'end_timestamp': 1704157200000, 'duration_ms': 60000, 'records_written': 1000, 
             'records_retrieved': 1000, 'bytes_written': 1048576, 'api_calls_made': 10,
             'rate_limited': False, 'error_message': None},
        ])
        
        transformer = PipelineAuditTransformer()
        
        df = transformer.remove_duplicates(df)
        df = transformer.add_derived_columns(df)
        df = transformer.add_status_flags(df)
        
        expected_columns = [
            'audit_id', 'status', 'start_datetime', 'end_datetime', 'start_date',
            'start_year', 'start_month', 'duration_seconds', 'duration_minutes',
            'records_per_second', 'write_success_rate', 'is_success', 'is_slow'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        assert len(df) > 0
