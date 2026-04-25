# MarketMind Intelligence Platform V1
# Unit Tests for Macro Indicators Transformer
# Date: April 23, 2026

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.silver.transformations.macro_indicators_transformer import MacroIndicatorsTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestMacroIndicatorsTransformer:
    """Test suite for MacroIndicatorsTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = MacroIndicatorsTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_with_no_duplicates(self, sample_macro_indicators_df):
        """Test remove_duplicates with no duplicate data"""
        transformer = MacroIndicatorsTransformer()
        result = transformer.remove_duplicates(sample_macro_indicators_df)
        
        # No duplicates should be removed
        assert len(result) == len(sample_macro_indicators_df)
    
    def test_remove_duplicates_with_duplicates(self, sample_macro_indicators_df):
        """Test remove_duplicates with duplicate data"""
        # Create data with duplicates
        df_with_dupes = pd.concat([sample_macro_indicators_df, sample_macro_indicators_df.head(2)])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.remove_duplicates(df_with_dupes)
        
        # Duplicates should be removed
        assert len(result) == len(sample_macro_indicators_df)
        
        # No duplicates should remain
        assert not result.duplicated(subset=['indicator_name', 'date']).any()
    
    def test_remove_duplicates_keeps_first(self, sample_macro_indicators_df):
        """Test that remove_duplicates keeps first occurrence"""
        # Create duplicate with different value
        df = sample_macro_indicators_df.copy()
        duplicate = df.iloc[0:1].copy()
        duplicate['value'] = 99.99
        df_with_dupe = pd.concat([df, duplicate])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.remove_duplicates(df_with_dupe)
        
        # Should keep first (original value)
        first_row = result[
            (result['indicator_name'] == df.iloc[0]['indicator_name']) & 
            (result['date'] == df.iloc[0]['date'])
        ].iloc[0]
        
        assert first_row['value'] == df.iloc[0]['value']
    
    def test_add_derived_columns_adds_datetime(self, sample_macro_indicators_df):
        """Test that add_derived_columns adds datetime column"""
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_derived_columns(sample_macro_indicators_df)
        
        # Check datetime column exists
        assert 'datetime' in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result['datetime'])
    
    def test_add_derived_columns_adds_year_month_quarter(self, sample_macro_indicators_df):
        """Test that add_derived_columns adds year, month, quarter"""
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_derived_columns(sample_macro_indicators_df)
        
        # Check time component columns exist
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert 'quarter' in result.columns
        assert 'day' in result.columns
        
        # Check values are valid
        assert result['year'].between(1900, 2100).all()
        assert result['month'].between(1, 12).all()
        assert result['quarter'].between(1, 4).all()
        assert result['day'].between(1, 31).all()
    
    def test_calculate_changes_adds_value_change(self, sample_macro_indicators_df):
        """Test that calculate_changes adds value_change column"""
        transformer = MacroIndicatorsTransformer()
        
        # Add datetime first (required for calculate_changes)
        df = transformer.add_derived_columns(sample_macro_indicators_df)
        result = transformer.calculate_changes(df)
        
        # Check value_change column exists
        assert 'value_change' in result.columns
    
    def test_calculate_changes_adds_value_pct_change(self, sample_macro_indicators_df):
        """Test that calculate_changes adds value_pct_change column"""
        transformer = MacroIndicatorsTransformer()
        
        # Add datetime first
        df = transformer.add_derived_columns(sample_macro_indicators_df)
        result = transformer.calculate_changes(df)
        
        # Check value_pct_change column exists
        assert 'value_pct_change' in result.columns
    
    def test_calculate_changes_adds_forecast_comparison(self, sample_macro_indicators_df):
        """Test that calculate_changes adds vs_forecast columns"""
        transformer = MacroIndicatorsTransformer()
        
        # Add datetime first
        df = transformer.add_derived_columns(sample_macro_indicators_df)
        result = transformer.calculate_changes(df)
        
        # Check forecast comparison columns exist
        assert 'vs_forecast' in result.columns
        assert 'vs_forecast_pct' in result.columns
    
    def test_calculate_changes_adds_previous_comparison(self, sample_macro_indicators_df):
        """Test that calculate_changes adds vs_previous columns"""
        transformer = MacroIndicatorsTransformer()
        
        # Add datetime first
        df = transformer.add_derived_columns(sample_macro_indicators_df)
        result = transformer.calculate_changes(df)
        
        # Check previous comparison columns exist
        assert 'vs_previous' in result.columns
        assert 'vs_previous_pct' in result.columns
    
    def test_calculate_changes_computes_correctly(self):
        """Test that calculate_changes computes values correctly"""
        # Create simple test data
        df = pd.DataFrame([
            {'indicator_name': 'TEST', 'date': '2024-01-01', 'value': 100.0, 'forecast_value': 98.0, 'previous_value': 95.0},
            {'indicator_name': 'TEST', 'date': '2024-02-01', 'value': 110.0, 'forecast_value': 105.0, 'previous_value': 100.0},
        ])
        
        transformer = MacroIndicatorsTransformer()
        df = transformer.add_derived_columns(df)
        result = transformer.calculate_changes(df)
        
        # Check second row calculations
        row2 = result.iloc[1]
        
        # value_change should be 110 - 100 = 10
        assert row2['value_change'] == 10.0
        
        # vs_forecast should be 110 - 105 = 5
        assert row2['vs_forecast'] == 5.0
        
        # vs_previous should be 110 - 100 = 10
        assert row2['vs_previous'] == 10.0
    
    def test_add_indicator_metadata_adds_category(self, sample_macro_indicators_df):
        """Test that add_indicator_metadata adds indicator_category"""
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(sample_macro_indicators_df)
        
        # Check indicator_category column exists
        assert 'indicator_category' in result.columns
        assert result['indicator_category'].notna().all()
    
    def test_add_indicator_metadata_categorizes_cpi(self):
        """Test that CPI indicators are categorized as Inflation"""
        df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3},
            {'indicator_name': 'US_CORE_CPI_MOM', 'date': '2024-01-01', 'value': 0.2},
        ])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(df)
        
        # All should be categorized as Inflation
        assert (result['indicator_category'] == 'Inflation').all()
    
    def test_add_indicator_metadata_categorizes_unemployment(self):
        """Test that unemployment indicators are categorized as Labor Market"""
        df = pd.DataFrame([
            {'indicator_name': 'US_UNEMPLOYMENT_RATE', 'date': '2024-01-01', 'value': 3.7},
            {'indicator_name': 'US_ADP_EMPLOYMENT', 'date': '2024-01-01', 'value': 150.0},
        ])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(df)
        
        # All should be categorized as Labor Market
        assert (result['indicator_category'] == 'Labor Market').all()
    
    def test_add_indicator_metadata_categorizes_rates(self):
        """Test that interest rate indicators are categorized as Interest Rates"""
        df = pd.DataFrame([
            {'indicator_name': 'US_FEDERAL_FUNDS_RATE', 'date': '2024-01-01', 'value': 5.5},
            {'indicator_name': 'US_INTEREST_RATE', 'date': '2024-01-01', 'value': 5.0},
        ])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(df)
        
        # All should be categorized as Interest Rates
        assert (result['indicator_category'] == 'Interest Rates').all()
    
    def test_add_indicator_metadata_categorizes_gdp(self):
        """Test that GDP indicators are categorized as Economic Growth"""
        df = pd.DataFrame([
            {'indicator_name': 'US_GDP_GROWTH', 'date': '2024-01-01', 'value': 2.5},
        ])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(df)
        
        assert result.iloc[0]['indicator_category'] == 'Economic Growth'
    
    def test_add_indicator_metadata_categorizes_other(self):
        """Test that unknown indicators are categorized as Other"""
        df = pd.DataFrame([
            {'indicator_name': 'UNKNOWN_METRIC', 'date': '2024-01-01', 'value': 100.0},
        ])
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.add_indicator_metadata(df)
        
        assert result.iloc[0]['indicator_category'] == 'Other'
    
    @patch('code.silver.transformations.macro_indicators_transformer.DATA_DIR')
    def test_read_bronze_data_success(self, mock_read_parquet, sample_macro_indicators_df, tmp_path):
        """Test read_bronze_data with existing files"""
        # Create temporary bronze directory structure
        bronze_dir = tmp_path / 'bronze' / 'macro_indicators'
        bronze_dir.mkdir(parents=True)
    
        # Create a test parquet file
        test_file = bronze_dir / 'test.parquet'
        sample_macro_indicators_df.to_parquet(test_file)
    
        # Mock DATA_DIR to point to tmp_path
        with patch('code.silver.transformations.macro_indicators_transformer.DATA_DIR', tmp_path):
            transformer = MacroIndicatorsTransformer()
            result = transformer.read_bronze_data()
    
        # Should return DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
    
    @patch('code.silver.transformations.macro_indicators_transformer.DATA_DIR')
    def test_read_bronze_data_no_files(self, mock_data_dir):
        """Test read_bronze_data with no files"""
        # Mock path exists but no parquet files
        mock_bronze_path = MagicMock()
        mock_bronze_path.exists.return_value = True
        mock_bronze_path.rglob.return_value = []
        mock_data_dir.__truediv__.return_value = mock_bronze_path
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.read_bronze_data()
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch('code.silver.transformations.macro_indicators_transformer.DATA_DIR')
    def test_read_bronze_data_path_not_exists(self, mock_data_dir):
        """Test read_bronze_data with non-existent path"""
        # Mock path doesn't exist
        mock_bronze_path = MagicMock()
        mock_bronze_path.exists.return_value = False
        mock_data_dir.__truediv__.return_value = mock_bronze_path
        
        transformer = MacroIndicatorsTransformer()
        result = transformer.read_bronze_data()
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_methods_handle_empty_dataframe(self):
        """Test that all methods handle empty DataFrame gracefully"""
        transformer = MacroIndicatorsTransformer()
        empty_df = pd.DataFrame()
        
        # remove_duplicates
        result = transformer.remove_duplicates(empty_df)
        assert result.empty
    
    def test_full_transformation_pipeline(self, sample_macro_indicators_df):
        """Test the complete transformation pipeline"""
        transformer = MacroIndicatorsTransformer()
        
        # Apply all transformations in sequence
        df = sample_macro_indicators_df.copy()
        df = transformer.remove_duplicates(df)
        df = transformer.add_derived_columns(df)
        df = transformer.calculate_changes(df)
        df = transformer.add_indicator_metadata(df)
        
        # Verify all expected columns exist
        expected_columns = [
            'indicator_name', 'date', 'value',
            'datetime', 'year', 'month', 'quarter', 'day',
            'value_change', 'value_pct_change',
            'vs_forecast', 'vs_forecast_pct',
            'vs_previous', 'vs_previous_pct',
            'indicator_category'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        # Verify data integrity
        assert len(df) > 0
        assert df['indicator_category'].notna().all()
