# MarketMind Intelligence Platform V1
# Unit Tests for Filings Metadata Transformer
# Date: April 23, 2026

import pytest
import pandas as pd
from unittest.mock import patch
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.silver.transformations.filings_metadata_transformer import FilingsMetadataTransformer


@pytest.mark.unit
@pytest.mark.transformer
class TestFilingsMetadataTransformer:
    """Test suite for FilingsMetadataTransformer"""
    
    def test_initialization(self):
        """Test transformer initialization"""
        transformer = FilingsMetadataTransformer()
        assert transformer is not None
        assert hasattr(transformer, 'quality_checker')
        assert hasattr(transformer, 'silver_base_path')
        assert transformer.silver_base_path.exists()
    
    def test_remove_duplicates_with_no_duplicates(self, sample_sec_filings_df):
        """Test remove_duplicates with no duplicate data"""
        transformer = FilingsMetadataTransformer()
        result = transformer.remove_duplicates(sample_sec_filings_df)
        
        assert len(result) == len(sample_sec_filings_df)
    
    def test_remove_duplicates_with_duplicates(self, sample_sec_filings_df):
        """Test remove_duplicates with duplicate data"""
        df_with_dupes = pd.concat([sample_sec_filings_df, sample_sec_filings_df.head(2)])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.remove_duplicates(df_with_dupes)
        
        assert len(result) == len(sample_sec_filings_df)
        assert not result.duplicated(subset=['accession_number']).any()
    
    def test_remove_duplicates_keeps_first(self, sample_sec_filings_df):
        """Test that remove_duplicates keeps first occurrence"""
        df = sample_sec_filings_df.copy()
        duplicate = df.iloc[0:1].copy()
        duplicate['form_type'] = 'DUPLICATE'
        df_with_dupe = pd.concat([df, duplicate])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.remove_duplicates(df_with_dupe)
        
        first_row = result[result['accession_number'] == df.iloc[0]['accession_number']].iloc[0]
        assert first_row['form_type'] == df.iloc[0]['form_type']
    
    def test_add_derived_columns_adds_filing_datetime(self, sample_sec_filings_df):
        """Test that add_derived_columns adds filing_datetime"""
        transformer = FilingsMetadataTransformer()
        result = transformer.add_derived_columns(sample_sec_filings_df)
        
        assert 'filing_datetime' in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result['filing_datetime'])
    
    def test_add_derived_columns_adds_report_datetime(self, sample_sec_filings_df):
        """Test that add_derived_columns adds report_datetime if report_date exists"""
        transformer = FilingsMetadataTransformer()
        result = transformer.add_derived_columns(sample_sec_filings_df)
        
        if 'report_date' in sample_sec_filings_df.columns:
            assert 'report_datetime' in result.columns
            assert pd.api.types.is_datetime64_any_dtype(result['report_datetime'])
    
    def test_add_derived_columns_calculates_filing_lag(self, sample_sec_filings_df):
        """Test that filing_lag_days is calculated"""
        transformer = FilingsMetadataTransformer()
        result = transformer.add_derived_columns(sample_sec_filings_df)
        
        if 'report_date' in sample_sec_filings_df.columns:
            assert 'filing_lag_days' in result.columns
            assert result['filing_lag_days'].notna().any()
    
    def test_add_derived_columns_adds_year_month_quarter(self, sample_sec_filings_df):
        """Test that year, month, quarter columns are added"""
        transformer = FilingsMetadataTransformer()
        result = transformer.add_derived_columns(sample_sec_filings_df)
        
        assert 'filing_year' in result.columns
        assert 'filing_month' in result.columns
        assert 'filing_quarter' in result.columns
        assert 'filing_day_of_week' in result.columns
        
        assert result['filing_year'].between(1900, 2100).all()
        assert result['filing_month'].between(1, 12).all()
        assert result['filing_quarter'].between(1, 4).all()
        assert result['filing_day_of_week'].between(0, 6).all()
    
    def test_categorize_filings_adds_category(self, sample_sec_filings_df):
        """Test that filing_category is added"""
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(sample_sec_filings_df)
        
        assert 'filing_category' in result.columns
        assert result['filing_category'].notna().all()
    
    def test_categorize_filings_10k(self):
        """Test that 10-K is categorized as Annual Report"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': '10-K', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': '10-K/A', 'filing_date': '2024-01-16'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert (result['filing_category'] == 'Annual Report').all()
    
    def test_categorize_filings_10q(self):
        """Test that 10-Q is categorized as Quarterly Report"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': '10-Q', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': '10-Q/A', 'filing_date': '2024-01-16'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert (result['filing_category'] == 'Quarterly Report').all()
    
    def test_categorize_filings_8k(self):
        """Test that 8-K is categorized as Current Report"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': '8-K', 'filing_date': '2024-01-15'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert result.iloc[0]['filing_category'] == 'Current Report'
    
    def test_categorize_filings_registration(self):
        """Test that S- forms are categorized as Registration Statement"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': 'S-1', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': 'S-3', 'filing_date': '2024-01-16'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert (result['filing_category'] == 'Registration Statement').all()
    
    def test_categorize_filings_proxy(self):
        """Test that DEF 14 forms are categorized as Proxy Statement"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': 'DEF 14A', 'filing_date': '2024-01-15'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert result.iloc[0]['filing_category'] == 'Proxy Statement'
    
    def test_categorize_filings_insider(self):
        """Test that forms 3,4,5 are categorized as Insider Trading"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': '3', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': '4', 'filing_date': '2024-01-16'},
            {'accession_number': '0003', 'ticker': 'GOOGL', 'form_type': '5', 'filing_date': '2024-01-17'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert (result['filing_category'] == 'Insider Trading').all()
    
    def test_categorize_filings_beneficial_ownership(self):
        """Test that SC 13 forms are categorized as Beneficial Ownership"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': 'SC 13D', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': 'SC 13G', 'filing_date': '2024-01-16'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert (result['filing_category'] == 'Beneficial Ownership').all()
    
    def test_categorize_filings_other(self):
        """Test that unknown forms are categorized as Other"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': 'UNKNOWN', 'filing_date': '2024-01-15'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert result.iloc[0]['filing_category'] == 'Other'
    
    def test_categorize_filings_adds_periodic_flag(self, sample_sec_filings_df):
        """Test that is_periodic_report flag is added"""
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(sample_sec_filings_df)
        
        assert 'is_periodic_report' in result.columns
        assert result['is_periodic_report'].dtype == bool
    
    def test_categorize_filings_adds_amended_flag(self, sample_sec_filings_df):
        """Test that is_amended flag is added"""
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(sample_sec_filings_df)
        
        assert 'is_amended' in result.columns
        assert result['is_amended'].dtype == bool
    
    def test_categorize_filings_amended_detection(self):
        """Test that amended filings are detected"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'form_type': '10-K/A', 'filing_date': '2024-01-15'},
            {'accession_number': '0002', 'ticker': 'MSFT', 'form_type': '10-K', 'filing_date': '2024-01-16'},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.categorize_filings(df)
        
        assert result.iloc[0]['is_amended']
        assert not result.iloc[1]['is_amended']
    
    def test_add_xbrl_flags_adds_structured_data_flag(self):
        """Test that has_structured_data flag is added"""
        df = pd.DataFrame([
            {'accession_number': '0001', 'ticker': 'AAPL', 'is_xbrl': True, 'is_inline_xbrl': False},
            {'accession_number': '0002', 'ticker': 'MSFT', 'is_xbrl': False, 'is_inline_xbrl': True},
            {'accession_number': '0003', 'ticker': 'GOOGL', 'is_xbrl': False, 'is_inline_xbrl': False},
        ])
        
        transformer = FilingsMetadataTransformer()
        result = transformer.add_xbrl_flags(df)
        
        assert 'has_structured_data' in result.columns
        assert result.iloc[0]['has_structured_data']
        assert result.iloc[1]['has_structured_data']
        assert not result.iloc[2]['has_structured_data']
    
    def test_read_bronze_data_path_not_exists(self, tmp_path):
        """Test read_bronze_data with non-existent path"""
        with patch('code.silver.transformations.filings_metadata_transformer.DATA_DIR', tmp_path):
            transformer = FilingsMetadataTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_no_files(self, tmp_path):
        """Test read_bronze_data with no files"""
        bronze_dir = tmp_path / 'bronze' / 'filings_metadata'
        bronze_dir.mkdir(parents=True)
        
        with patch('code.silver.transformations.filings_metadata_transformer.DATA_DIR', tmp_path):
            transformer = FilingsMetadataTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_read_bronze_data_success(self, sample_sec_filings_df, tmp_path):
        """Test read_bronze_data with existing files"""
        bronze_dir = tmp_path / 'bronze' / 'filings_metadata'
        bronze_dir.mkdir(parents=True)
        
        test_file = bronze_dir / 'test.parquet'
        sample_sec_filings_df.to_parquet(test_file)
        
        with patch('code.silver.transformations.filings_metadata_transformer.DATA_DIR', tmp_path):
            transformer = FilingsMetadataTransformer()
            result = transformer.read_bronze_data()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
    
    def test_methods_handle_empty_dataframe(self):
        """Test that all methods handle empty DataFrame gracefully"""
        transformer = FilingsMetadataTransformer()
        empty_df = pd.DataFrame()
        
        result = transformer.remove_duplicates(empty_df)
        assert result.empty
    
    def test_full_transformation_pipeline(self, sample_sec_filings_df):
        """Test the complete transformation pipeline"""
        # Add XBRL columns to sample data
        sample_sec_filings_df['is_xbrl'] = True
        sample_sec_filings_df['is_inline_xbrl'] = False
        
        transformer = FilingsMetadataTransformer()
        
        df = sample_sec_filings_df.copy()
        df = transformer.remove_duplicates(df)
        df = transformer.add_derived_columns(df)
        df = transformer.categorize_filings(df)
        df = transformer.add_xbrl_flags(df)
        
        expected_columns = [
            'accession_number', 'ticker', 'form_type', 'filing_date',
            'filing_datetime', 'filing_year', 'filing_month', 'filing_quarter',
            'filing_category', 'is_periodic_report', 'is_amended',
            'has_structured_data'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"
        
        assert len(df) > 0
        assert df['filing_category'].notna().all()
