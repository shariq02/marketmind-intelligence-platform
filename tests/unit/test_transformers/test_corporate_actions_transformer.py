# MarketMind Intelligence Platform V1
# Unit Tests for Corporate Actions Transformer
# Date: April 23, 2026

import pytest
import pandas as pd
from unittest.mock import patch
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.silver.transformations.corporate_actions_transformer import CorporateActionsTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestCorporateActionsTransformer:
    """Test suite for CorporateActionsTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = CorporateActionsTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_splits(self):
        """Test remove_duplicates for split actions"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0, 'ex_dividend_date': None},
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0, 'ex_dividend_date': None},  # Duplicate
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-02-01', 'split_ratio': 3.0, 'ex_dividend_date': None},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
        assert not result.duplicated(subset=['ticker', 'execution_date']).any()
    
    def test_remove_duplicates_dividends(self):
        """Test remove_duplicates for dividend actions"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'cash_amount': 0.24, 'execution_date': None},
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'cash_amount': 0.24, 'execution_date': None},  # Duplicate
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-01', 'cash_amount': 0.30, 'execution_date': None},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.remove_duplicates(df)
        
        assert len(result) == 2
    
    def test_validate_splits_with_valid_data(self):
        """Test validate_splits with valid split records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0},
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-02-01', 'split_ratio': 3.0},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_splits(df)
        
        assert 'is_valid_split' in result.columns
        assert result['is_valid_split'].all()
    
    def test_validate_splits_with_invalid_data(self):
        """Test validate_splits with invalid split records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': None, 'split_ratio': 2.0},  # Missing date
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-02-01', 'split_ratio': None},  # Missing ratio
            {'ticker': 'GOOGL', 'action_type': 'SPLIT', 'execution_date': '2024-03-01', 'split_ratio': 0},  # Invalid ratio
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_splits(df)
        
        assert 'is_valid_split' in result.columns
        assert not result['is_valid_split'].any()
    
    def test_validate_splits_ignores_dividends(self):
        """Test that validate_splits ignores dividend records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0},
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'execution_date': None, 'split_ratio': None},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_splits(df)
        
        # Only SPLIT row should have is_valid_split
        assert result[result['action_type'] == 'SPLIT']['is_valid_split'].notna().all()
    
    def test_validate_dividends_with_valid_data(self):
        """Test validate_dividends with valid dividend records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'cash_amount': 0.24},
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-01', 'cash_amount': 0.30},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_dividends(df)
        
        assert 'is_valid_dividend' in result.columns
        assert result['is_valid_dividend'].all()
    
    def test_validate_dividends_with_invalid_data(self):
        """Test validate_dividends with invalid dividend records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': None, 'cash_amount': 0.24},  # Missing date
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-01', 'cash_amount': None},  # Missing amount
            {'ticker': 'GOOGL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-03-01', 'cash_amount': 0},  # Invalid amount
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_dividends(df)
        
        assert 'is_valid_dividend' in result.columns
        assert not result['is_valid_dividend'].any()
    
    def test_validate_dividends_ignores_splits(self):
        """Test that validate_dividends ignores split records"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'cash_amount': 0.24},
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'ex_dividend_date': None, 'cash_amount': None},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.validate_dividends(df)
        
        # Only DIVIDEND row should have is_valid_dividend
        assert result[result['action_type'] == 'DIVIDEND']['is_valid_dividend'].notna().all()
    
    def test_add_derived_columns_adds_event_date_for_splits(self):
        """Test that add_derived_columns adds event_date for splits"""
        df = pd.DataFrame([
        {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 
         'split_ratio': 2.0, 'ex_dividend_date': None, 'cash_amount': None, 'frequency': None},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'event_date' in result.columns
        assert result.iloc[0]['event_date'] == pd.Timestamp('2024-01-15')
    
    def test_add_derived_columns_adds_event_date_for_dividends(self):
        """Test that add_derived_columns adds event_date for dividends"""
        df = pd.DataFrame([
        {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 
         'execution_date': None, 'split_ratio': None, 'cash_amount': 0.24, 'frequency': 4},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'event_date' in result.columns
        assert result.iloc[0]['event_date'] == pd.Timestamp('2024-01-15')
    
    def test_add_derived_columns_adds_year_month_day(self):
        """Test that year, month, day columns are added"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'ex_dividend_date': None, 'split_ratio': 2.0, 'cash_amount': None, 'frequency': None},
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-20', 'execution_date': None, 'split_ratio': None, 'cash_amount': 0.30, 'frequency': 4},
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert 'day' in result.columns
        
        assert result['year'].iloc[0] == 2024
        assert result['month'].iloc[0] == 1
        assert result['day'].iloc[0] == 15
        
        assert result['year'].iloc[1] == 2024
        assert result['month'].iloc[1] == 2
        assert result['day'].iloc[1] == 20
    
    def test_add_derived_columns_classifies_split_type(self):
        """Test that splits are classified as forward or reverse"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0, 'ex_dividend_date': None},  # Forward
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-02-01', 'split_ratio': 0.5, 'ex_dividend_date': None},  # Reverse
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'is_forward_split' in result.columns
        assert 'is_reverse_split' in result.columns
        
        assert result.iloc[0]['is_forward_split']
        assert not result.iloc[0]['is_reverse_split']
        
        assert not result.iloc[1]['is_forward_split']
        assert result.iloc[1]['is_reverse_split']
    
    def test_add_derived_columns_adds_frequency_label(self):
        """Test that dividend frequency labels are added"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'frequency': 4, 'execution_date': None},  # Quarterly
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-01', 'frequency': 12, 'execution_date': None},  # Monthly
            {'ticker': 'GOOGL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-03-01', 'frequency': 1, 'execution_date': None},  # Annual
            {'ticker': 'TSLA', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-04-01', 'frequency': 2, 'execution_date': None},  # Semi-Annual
        ])
        
        transformer = CorporateActionsTransformer()
        result = transformer.add_derived_columns(df)
        
        assert 'frequency_label' in result.columns
        assert result.iloc[0]['frequency_label'] == 'Quarterly'
        assert result.iloc[1]['frequency_label'] == 'Monthly'
        assert result.iloc[2]['frequency_label'] == 'Annual'
        assert result.iloc[3]['frequency_label'] == 'Semi-Annual'
    
    def test_read_bronze_data_path_not_exists(self, tmp_path):
        """Test read_bronze_data with non-existent path"""
        with patch('code.silver.transformations.corporate_actions_transformer.DATA_DIR', tmp_path):
            transformer = CorporateActionsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_no_files(self, tmp_path):
        """Test read_bronze_data with no files"""
        bronze_dir = tmp_path / 'bronze' / 'corporate_actions'
        bronze_dir.mkdir(parents=True)
        
        with patch('code.silver.transformations.corporate_actions_transformer.DATA_DIR', tmp_path):
            transformer = CorporateActionsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_success(self, tmp_path):
        """Test read_bronze_data with existing files"""
        sample_df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0},
        ])
        
        bronze_dir = tmp_path / 'bronze' / 'corporate_actions'
        bronze_dir.mkdir(parents=True)
        
        test_file = bronze_dir / 'test.parquet'
        sample_df.to_parquet(test_file)
        
        with patch('code.silver.transformations.corporate_actions_transformer.DATA_DIR', tmp_path):
            transformer = CorporateActionsTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
    
    def test_methods_handle_empty_dataframe(self):
        """Test that all methods handle empty DataFrame gracefully"""
        transformer = CorporateActionsTransformer()
        pd.DataFrame()
        
        result = transformer.remove_duplicates(pd.DataFrame({'ticker': [], 'action_type': [], 'execution_date': [], 'ex_dividend_date': []}))
        assert len(result) == 0
    
    def test_full_transformation_pipeline_splits(self):
        """Test complete pipeline with split data"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15', 'split_ratio': 2.0, 'ex_dividend_date': None, 'cash_amount': None, 'frequency': None},
            {'ticker': 'MSFT', 'action_type': 'SPLIT', 'execution_date': '2024-02-01', 'split_ratio': 0.5, 'ex_dividend_date': None, 'cash_amount': None, 'frequency': None},
        ])
        
        transformer = CorporateActionsTransformer()
        
        df = transformer.remove_duplicates(df)
        df = transformer.validate_splits(df)
        df = transformer.validate_dividends(df)
        df = transformer.add_derived_columns(df)
        
        expected_columns = ['ticker', 'action_type', 'execution_date', 'split_ratio', 'is_valid_split', 'event_date', 'year', 'month', 'day', 'is_forward_split', 'is_reverse_split']
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        assert len(df) == 2
        assert df['is_valid_split'].all()
    
    def test_full_transformation_pipeline_dividends(self):
        """Test complete pipeline with dividend data"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-01-15', 'cash_amount': 0.24, 'frequency': 4, 'execution_date': None, 'split_ratio': None},
            {'ticker': 'MSFT', 'action_type': 'DIVIDEND', 'ex_dividend_date': '2024-02-01', 'cash_amount': 0.30, 'frequency': 12, 'execution_date': None, 'split_ratio': None},
        ])
        
        transformer = CorporateActionsTransformer()
        
        df = transformer.remove_duplicates(df)
        df = transformer.validate_splits(df)
        df = transformer.validate_dividends(df)
        df = transformer.add_derived_columns(df)
        
        expected_columns = ['ticker', 'action_type', 'ex_dividend_date', 'cash_amount', 'is_valid_dividend', 'event_date', 'year', 'month', 'day', 'frequency_label']
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        assert len(df) == 2
        assert df['is_valid_dividend'].all()
