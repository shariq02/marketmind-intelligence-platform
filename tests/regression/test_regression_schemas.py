# MarketMind Intelligence Platform V1
# Regression Tests - Schema Stability & Backward Compatibility
# Date: April 24, 2026

import pytest
from pydantic_core import ValidationError

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT)

from code.bronze.schemas.market_bar import MarketBar
from code.bronze.schemas.corporate_action import CorporateAction, ActionType
from code.bronze.schemas.macro_indicator import MacroIndicator, Frequency
from code.bronze.schemas.filing_metadata import FilingMetadata
from code.bronze.schemas.pipeline_audit import PipelineAudit, ConnectorName, IngestionStatus, ExecutionMode


@pytest.mark.regression
class TestSchemaBackwardCompatibility:
    """Test that schema changes don't break existing data"""
    
    def test_market_bar_schema_v1_compatibility(self):
        """Test MarketBar can deserialize historical V1 data"""
        # Historical V1 record (simulated)
        historical_data = {
            'ticker': 'AAPL',
            'timestamp': 1704153600000,
            'granularity': 'daily',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 103.0,
            'volume': 1000000.0,
            'vwap': 102.0,
            'trade_count': 5000,
            'adjusted': True,
            'source': 'polygon',
            'ingestion_timestamp': 1704153600000
        }
        
        # Should deserialize without error
        bar = MarketBar(**historical_data)
        
        assert bar.ticker == 'AAPL'
        assert bar.close == 103.0
        assert bar.granularity == 'daily'
    
    def test_corporate_action_schema_v1_compatibility(self):
        """Test CorporateAction can deserialize historical V1 data"""
        historical_data = {
            'ticker': 'AAPL',
            'action_type': 'SPLIT',
            'execution_date': '2020-08-31',
            'split_ratio': 4.0,
            'ex_dividend_date': None,
            'record_date': None,
            'declaration_date': None,
            'pay_date': None,
            'cash_amount': None,
            'dividend_type': None,
            'frequency': None,
            'source': 'polygon',
            'ingestion_timestamp': 1704153600000
        }
        
        action = CorporateAction(**historical_data)
        
        assert action.ticker == 'AAPL'
        assert action.action_type == ActionType.SPLIT
        assert action.split_ratio == 4.0
    
    def test_macro_indicator_schema_v1_compatibility(self):
        """Test MacroIndicator can deserialize historical V1 data"""
        historical_data = {
            'indicator_name': 'US_CPI_MOM',
            'date': '2026-01-15',
            'value': 0.3,
            'forecast_value': 0.2,
            'previous_value': 0.4,
            'unit': 'percent',
            'frequency': 'MONTHLY',
            'source': 'akshare',
            'source_url': 'https://www.bls.gov/cpi/',
            'ingestion_timestamp': 1704153600000
        }
        
        indicator = MacroIndicator(**historical_data)
        
        assert indicator.indicator_name == 'US_CPI_MOM'
        assert indicator.value == 0.3
        assert indicator.frequency == Frequency.MONTHLY


@pytest.mark.regression
class TestFieldPresenceStability:
    """Test that required fields remain required across versions"""
    
    def test_market_bar_required_fields(self):
        """Test MarketBar required fields haven't changed"""
        required_fields = {
            'ticker', 'timestamp', 'granularity', 'open', 'high', 
            'low', 'close', 'volume', 'ingestion_timestamp'
        }
        
        # Try creating without each required field
        base_data = {
            'ticker': 'AAPL',
            'timestamp': 1704153600000,
            'granularity': 'daily',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 103.0,
            'volume': 1000000.0,
            'vwap': 102.0,
            'trade_count': 5000,
            'adjusted': True,
            'source': 'polygon',
            'ingestion_timestamp': 1704153600000
        }
        
        for field in required_fields:
                
            test_data = base_data.copy()
            del test_data[field]
            
            with pytest.raises(ValidationError):  # Should raise validation error
                MarketBar(**test_data)
    
    def test_corporate_action_required_fields(self):
        """Test CorporateAction required fields haven't changed"""
        # Split requires: ticker, action_type, execution_date, split_ratio
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker='AAPL',
                action_type=ActionType.SPLIT,
                execution_date='2020-08-31',
                # Missing split_ratio
                source='polygon',
                ingestion_timestamp=1704153600000
            )
    
    def test_filing_metadata_required_fields(self):
        """Test FilingMetadata required fields haven't changed"""
        required_fields = {
            'accession_number', 'cik', 'company_name', 
            'form_type', 'filing_date', 'filing_url', 'ingestion_timestamp'
        }
        
        base_data = {
            'accession_number': '0000320193-25-000079',
            'cik': '0000320193',
            'company_name': 'Apple Inc.',
            'form_type': '10-K',
            'filing_date': '2025-10-31',
            'filing_url': 'https://www.sec.gov/test.html',
            'source': 'edgartools',
            'ingestion_timestamp': 1704153600000
        }
        
        for field in required_fields:
            test_data = base_data.copy()
            del test_data[field]
            
            with pytest.raises(ValidationError):
                FilingMetadata(**test_data)


@pytest.mark.regression
class TestEnumStability:
    """Test that enum values remain stable"""
    
    def test_action_type_enum_values(self):
        """Test ActionType enum values haven't changed"""
        expected_values = {'SPLIT', 'DIVIDEND'}
        actual_values = {e.value for e in ActionType}
        
        assert expected_values.issubset(actual_values), \
            f"ActionType enum values changed. Missing: {expected_values - actual_values}"
    
    def test_frequency_enum_values(self):
        """Test Frequency enum values haven't changed"""
        expected_values = {'DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'ANNUALLY'}
        actual_values = {e.value for e in Frequency}
        
        assert expected_values.issubset(actual_values), \
            f"Frequency enum values changed. Missing: {expected_values - actual_values}"
    
    def test_connector_name_enum_values(self):
        """Test ConnectorName enum values haven't changed"""
        expected_values = {
            'POLYGON_BARS',
            'POLYGON_CORPORATE_ACTIONS', 
            'AKSHARE_MACRO',
            'EDGARTOOLS_FILINGS'
        }
        actual_values = {e.value for e in ConnectorName}
        
        assert expected_values.issubset(actual_values), \
            f"ConnectorName enum values changed. Missing: {expected_values - actual_values}"


@pytest.mark.regression
class TestDataTypeStability:
    """Test that field data types remain consistent"""
    
    def test_market_bar_numeric_fields(self):
        """Test MarketBar numeric fields accept correct types"""
        bar = MarketBar(
            ticker='AAPL',
            timestamp=1704153600000,
            granularity='daily',
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source='polygon',
            ingestion_timestamp=1704153600000
        )
        
        assert isinstance(bar.open, float)
        assert isinstance(bar.high, float)
        assert isinstance(bar.low, float)
        assert isinstance(bar.close, float)
        assert isinstance(bar.volume, float)
    
    def test_market_bar_integer_fields(self):
        """Test MarketBar integer fields accept correct types"""
        bar = MarketBar(
            ticker='AAPL',
            timestamp=1704153600000,
            granularity='daily',
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source='polygon',
            ingestion_timestamp=1704153600000
        )
        
        assert isinstance(bar.timestamp, int)
        assert isinstance(bar.trade_count, int)
        assert isinstance(bar.ingestion_timestamp, int)


@pytest.mark.regression
class TestValidationRulesStability:
    """Test that validation rules remain consistent"""
    
    def test_ticker_uppercase_validation(self):
        """Test ticker uppercase conversion remains stable"""
        bar = MarketBar(
            ticker='aapl',  # lowercase input
            timestamp=1704153600000,
            granularity='daily',
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source='polygon',
            ingestion_timestamp=1704153600000
        )
        
        assert bar.ticker == 'AAPL'  # Should be uppercase
    
    def test_price_relationships_validation(self):
        """Test OHLC price relationship validation remains stable"""
        # High must be >= Low
        with pytest.raises(ValidationError):
            MarketBar(
                ticker='AAPL',
                timestamp=1704153600000,
                granularity='daily',
                open=100.0,
                high=99.0,  # Invalid: high < low
                low=105.0,
                close=103.0,
                volume=1000000.0,
                vwap=102.0,
                trade_count=5000,
                adjusted=True,
                source='polygon',
                ingestion_timestamp=1704153600000
            )
    
    def test_cik_numeric_validation(self):
        """Test CIK numeric validation remains stable"""
        # CIK must be numeric
        with pytest.raises(ValidationError):
            FilingMetadata(
                accession_number='0000320193-25-000079',
                cik='INVALID',  # Invalid: not numeric
                company_name='Apple Inc.',
                form_type='10-K',
                filing_date='2025-10-31',
                filing_url='https://www.sec.gov/test.html',
                source='edgartools',
                ingestion_timestamp=1704153600000
            )


@pytest.mark.regression
class TestMethodSignatureStability:
    """Test that method signatures remain backward compatible"""
    
    def test_to_dict_method_exists(self):
        """Test to_dict() method exists on all schemas"""
        bar = MarketBar(
            ticker='AAPL',
            timestamp=1704153600000,
            granularity='daily',
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source='polygon',
            ingestion_timestamp=1704153600000
        )
        
        result = bar.to_dict()
        
        assert isinstance(result, dict)
        assert 'ticker' in result
        assert result['ticker'] == 'AAPL'
    
    def test_create_new_method_signature(self):
        """Test PipelineAudit.create_new() signature unchanged"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type='ticker',
            entity_id='AAPL',
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        assert audit.entity_id == 'AAPL'
        assert audit.status == IngestionStatus.SUCCESS
    
    def test_complete_success_method_signature(self):
        """Test PipelineAudit.complete_success() signature unchanged"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type='ticker',
            entity_id='AAPL',
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        audit.complete_success(
            records_retrieved=100,
            records_written=100,
            bytes_written=10000,
            api_calls_made=1
        )
        
        assert audit.records_written == 100
        assert audit.status == IngestionStatus.SUCCESS
