# MarketMind Intelligence Platform V1
# Unit Tests for Quality Alerts Transformer
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import patch
from code.silver.transformations.quality_alerts_transformer import QualityAlertsTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestQualityAlertsTransformer:
    """Test suite for QualityAlertsTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = QualityAlertsTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_with_no_duplicates(self):
        """Test remove_duplicates with no duplicate data"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'HIGH'},
            {'alert_id': '002', 'severity': 'MEDIUM'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
    
    def test_remove_duplicates_with_duplicates(self):
        """Test remove_duplicates with duplicate data"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'HIGH'},
            {'alert_id': '001', 'severity': 'HIGH'},
            {'alert_id': '002', 'severity': 'MEDIUM'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
        assert not result.duplicated(subset=['alert_id']).any()
    
    def test_add_derived_columns_adds_check_datetime(self):
        """Test that check_datetime is added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'check_timestamp': 1704153600000, 'resolved': False},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'check_datetime' in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result['check_datetime'])
    
    def test_add_derived_columns_adds_date_components(self):
        """Test that date components are added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'check_timestamp': 1704153600000, 'resolved': False},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'check_date' in result.columns
        assert 'check_year' in result.columns
        assert 'check_month' in result.columns
        assert 'check_day' in result.columns
        assert 'check_hour' in result.columns
        assert 'check_day_of_week' in result.columns
    
    def test_add_derived_columns_calculates_resolution_time(self):
        """Test that time to resolution is calculated"""
        df = pd.DataFrame([
            {'alert_id': '001', 'check_timestamp': 1704153600000, 
             'resolution_timestamp': 1704157200000, 'resolved': True},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'resolution_datetime' in result.columns
        assert 'time_to_resolution_hours' in result.columns
        assert result.iloc[0]['time_to_resolution_hours'] == 1.0
    
    def test_add_derived_columns_handles_unresolved_alerts(self):
        """Test handling of unresolved alerts"""
        df = pd.DataFrame([
            {'alert_id': '001', 'check_timestamp': 1704153600000, 
             'resolution_timestamp': None, 'resolved': False},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert pd.isna(result.iloc[0]['time_to_resolution_hours'])
    
    def test_add_severity_scores_maps_scores(self):
        """Test that severity scores are mapped correctly"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'CRITICAL', 'failure_rate': 80.0},
            {'alert_id': '002', 'severity': 'HIGH', 'failure_rate': 60.0},
            {'alert_id': '003', 'severity': 'MEDIUM', 'failure_rate': 40.0},
            {'alert_id': '004', 'severity': 'LOW', 'failure_rate': 20.0},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_severity_scores(df)
        
        assert 'severity_score' in result.columns
        assert result.iloc[0]['severity_score'] == 4
        assert result.iloc[1]['severity_score'] == 3
        assert result.iloc[2]['severity_score'] == 2
        assert result.iloc[3]['severity_score'] == 1
    
    def test_add_severity_scores_calculates_impact(self):
        """Test that impact score is calculated"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'CRITICAL', 'failure_rate': 50.0},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_severity_scores(df)
        
        assert 'impact_score' in result.columns
        assert result.iloc[0]['impact_score'] == 2.0  # 4 * (50/100)
    
    def test_add_alert_flags_adds_severity_flags(self):
        """Test that severity flags are added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'CRITICAL', 'pipeline_blocked': True, 'resolved': False,
             'check_type': 'COMPLETENESS', 'layer': 'BRONZE'},
            {'alert_id': '002', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': True,
             'check_type': 'FRESHNESS', 'layer': 'SILVER'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_alert_flags(df)
        
        assert 'is_critical' in result.columns
        assert 'is_high' in result.columns
        assert result.iloc[0]['is_critical']
        assert result.iloc[1]['is_high']
    
    def test_add_alert_flags_adds_status_flags(self):
        """Test that status flags are added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'CRITICAL', 'pipeline_blocked': True, 'resolved': False,
             'check_type': 'COMPLETENESS', 'layer': 'BRONZE'},
            {'alert_id': '002', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': True,
             'check_type': 'FRESHNESS', 'layer': 'SILVER'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_alert_flags(df)
        
        assert 'is_blocking' in result.columns
        assert 'is_resolved' in result.columns
        assert 'is_unresolved' in result.columns
        
        assert result.iloc[0]['is_blocking']
        assert result.iloc[0]['is_unresolved']
        assert result.iloc[1]['is_resolved']
    
    def test_add_alert_flags_adds_check_type_flags(self):
        """Test that check type flags are added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'COMPLETENESS', 'layer': 'BRONZE'},
            {'alert_id': '002', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'FRESHNESS', 'layer': 'SILVER'},
            {'alert_id': '003', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'UNIQUENESS', 'layer': 'GOLD'},
            {'alert_id': '004', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'VALIDITY', 'layer': 'BRONZE'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_alert_flags(df)
        
        assert 'is_completeness_issue' in result.columns
        assert 'is_freshness_issue' in result.columns
        assert 'is_uniqueness_issue' in result.columns
        assert 'is_validity_issue' in result.columns
        
        assert result.iloc[0]['is_completeness_issue']
        assert result.iloc[1]['is_freshness_issue']
        assert result.iloc[2]['is_uniqueness_issue']
        assert result.iloc[3]['is_validity_issue']
    
    def test_add_alert_flags_adds_layer_flags(self):
        """Test that layer flags are added"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'COMPLETENESS', 'layer': 'BRONZE'},
            {'alert_id': '002', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'FRESHNESS', 'layer': 'SILVER'},
            {'alert_id': '003', 'severity': 'HIGH', 'pipeline_blocked': False, 'resolved': False,
             'check_type': 'UNIQUENESS', 'layer': 'GOLD'},
        ])
        
        transformer = QualityAlertsTransformer()
        result = transformer.add_alert_flags(df)
        
        assert 'is_bronze_layer' in result.columns
        assert 'is_silver_layer' in result.columns
        assert 'is_gold_layer' in result.columns
        
        assert result.iloc[0]['is_bronze_layer']
        assert result.iloc[1]['is_silver_layer']
        assert result.iloc[2]['is_gold_layer']
    
    def test_read_bronze_data_path_not_exists(self, tmp_path):
        """Test read_bronze_data with non-existent path"""
        with patch('code.silver.transformations.quality_alerts_transformer.DATA_DIR', tmp_path):
            transformer = QualityAlertsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_no_files(self, tmp_path):
        """Test read_bronze_data with no files"""
        bronze_dir = tmp_path / 'bronze' / 'quality_alerts'
        bronze_dir.mkdir(parents=True)
        
        with patch('code.silver.transformations.quality_alerts_transformer.DATA_DIR', tmp_path):
            transformer = QualityAlertsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_success(self, tmp_path):
        """Test read_bronze_data with existing files"""
        sample_df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'HIGH'},
        ])
        
        bronze_dir = tmp_path / 'bronze' / 'quality_alerts'
        bronze_dir.mkdir(parents=True)
        
        test_file = bronze_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.silver.transformations.quality_alerts_transformer.DATA_DIR', tmp_path):
            transformer = QualityAlertsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
    
    def test_full_transformation_pipeline(self):
        """Test complete transformation pipeline"""
        df = pd.DataFrame([
            {'alert_id': '001', 'severity': 'CRITICAL', 'check_timestamp': 1704153600000,
             'resolution_timestamp': 1704157200000, 'resolved': True, 'pipeline_blocked': True,
             'check_type': 'COMPLETENESS', 'layer': 'BRONZE', 'failure_rate': 80.0},
        ])
        
        transformer = QualityAlertsTransformer()
        
        df = transformer.remove_duplicates(df)
        df = transformer.add_derived_columns(df)
        df = transformer.add_severity_scores(df)
        df = transformer.add_alert_flags(df)
        
        expected_columns = [
            'alert_id', 'severity', 'check_datetime', 'check_date', 'check_year',
            'severity_score', 'impact_score', 'is_critical', 'is_blocking',
            'is_completeness_issue', 'is_bronze_layer'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        assert len(df) > 0
        assert df.iloc[0]['severity_score'] == 4
        assert df.iloc[0]['is_critical']
