# MarketMind Intelligence Platform V1
# Unit Tests for CorporateAction Schema
# Date: April 24, 2026

import pytest
from datetime import datetime
from pydantic import ValidationError
from code.bronze.schemas.corporate_action import CorporateAction, ActionType


@pytest.mark.unit
@pytest.mark.schema
class TestCorporateActionSchema:
    """Test suite for CorporateAction Pydantic model"""
    
    def test_valid_split(self):
        """Test creating valid stock split"""
        split = CorporateAction(
            ticker="AAPL",
            action_type=ActionType.SPLIT,
            execution_date="2020-08-31",
            split_ratio=4.0,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert split.ticker == "AAPL"
        assert split.action_type == ActionType.SPLIT
        assert split.split_ratio == 4.0
    
    def test_valid_dividend(self):
        """Test creating valid dividend"""
        dividend = CorporateAction(
            ticker="MSFT",
            action_type=ActionType.DIVIDEND,
            ex_dividend_date="2026-02-10",
            cash_amount=0.75,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert dividend.ticker == "MSFT"
        assert dividend.action_type == ActionType.DIVIDEND
        assert dividend.cash_amount == 0.75
    
    def test_ticker_uppercase_conversion(self):
        """Test ticker is converted to uppercase"""
        split = CorporateAction(
            ticker="aapl",
            action_type=ActionType.SPLIT,
            execution_date="2020-08-31",
            split_ratio=4.0,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert split.ticker == "AAPL"
    
    def test_split_missing_execution_date_fails(self):
        """Test split without execution_date fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                split_ratio=4.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "requires execution_date" in str(exc_info.value)
    
    def test_split_missing_split_ratio_fails(self):
        """Test split without split_ratio fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                execution_date="2020-08-31",
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "requires split_ratio" in str(exc_info.value)
    
    def test_split_with_dividend_fields_fails(self):
        """Test split with dividend-specific fields fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                execution_date="2020-08-31",
                split_ratio=4.0,
                ex_dividend_date="2020-08-31",
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "should not have dividend-specific fields" in str(exc_info.value)
    
    def test_dividend_missing_ex_dividend_date_fails(self):
        """Test dividend without ex_dividend_date fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                cash_amount=0.75,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "requires ex_dividend_date" in str(exc_info.value)
    
    def test_dividend_missing_cash_amount_fails(self):
        """Test dividend without cash_amount fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                ex_dividend_date="2026-02-10",
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "requires cash_amount" in str(exc_info.value)
    
    def test_dividend_with_split_fields_fails(self):
        """Test dividend with split-specific fields fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                ex_dividend_date="2026-02-10",
                cash_amount=0.75,
                split_ratio=2.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "should not have split-specific fields" in str(exc_info.value)
    
    def test_invalid_date_format_fails(self):
        """Test invalid date format fails"""
        with pytest.raises(ValidationError) as exc_info:
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                execution_date="2020/08/31",
                split_ratio=4.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "YYYY-MM-DD format" in str(exc_info.value)
    
    def test_negative_split_ratio_fails(self):
        """Test negative split ratio fails"""
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                execution_date="2020-08-31",
                split_ratio=-4.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_zero_split_ratio_fails(self):
        """Test zero split ratio fails"""
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker="AAPL",
                action_type=ActionType.SPLIT,
                execution_date="2020-08-31",
                split_ratio=0.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_negative_cash_amount_fails(self):
        """Test negative cash amount fails"""
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                ex_dividend_date="2026-02-10",
                cash_amount=-0.75,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_zero_cash_amount_fails(self):
        """Test zero cash amount fails"""
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                ex_dividend_date="2026-02-10",
                cash_amount=0.0,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_dividend_with_optional_dates(self):
        """Test dividend with optional date fields"""
        dividend = CorporateAction(
            ticker="MSFT",
            action_type=ActionType.DIVIDEND,
            ex_dividend_date="2026-02-10",
            record_date="2026-02-12",
            declaration_date="2026-01-15",
            pay_date="2026-02-20",
            cash_amount=0.75,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert dividend.record_date == "2026-02-12"
        assert dividend.declaration_date == "2026-01-15"
        assert dividend.pay_date == "2026-02-20"
    
    def test_dividend_with_frequency(self):
        """Test dividend with frequency"""
        dividend = CorporateAction(
            ticker="MSFT",
            action_type=ActionType.DIVIDEND,
            ex_dividend_date="2026-02-10",
            cash_amount=0.75,
            frequency=4,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert dividend.frequency == 4
    
    def test_invalid_frequency_fails(self):
        """Test invalid frequency fails"""
        with pytest.raises(ValidationError):
            CorporateAction(
                ticker="MSFT",
                action_type=ActionType.DIVIDEND,
                ex_dividend_date="2026-02-10",
                cash_amount=0.75,
                frequency=13,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        split = CorporateAction(
            ticker="AAPL",
            action_type=ActionType.SPLIT,
            execution_date="2020-08-31",
            split_ratio=4.0,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        split_dict = split.to_dict()
        
        assert isinstance(split_dict, dict)
        assert split_dict['ticker'] == 'AAPL'
        assert split_dict['action_type'] == 'SPLIT'
    
    def test_from_polygon_split(self):
        """Test creating from Polygon split response"""
        split_data = {
            'execution_date': '2020-08-31',
            'split_ratio': 4.0
        }
        
        split = CorporateAction.from_polygon_split('AAPL', split_data)
        
        assert split.ticker == 'AAPL'
        assert split.action_type == ActionType.SPLIT
        assert split.split_ratio == 4.0
    
    def test_from_polygon_dividend(self):
        """Test creating from Polygon dividend response"""
        dividend_data = {
            'ex_dividend_date': '2026-02-10',
            'record_date': '2026-02-12',
            'pay_date': '2026-02-20',
            'cash_amount': 0.75,
            'dividend_type': 'CD',
            'frequency': 4
        }
        
        dividend = CorporateAction.from_polygon_dividend('MSFT', dividend_data)
        
        assert dividend.ticker == 'MSFT'
        assert dividend.action_type == ActionType.DIVIDEND
        assert dividend.cash_amount == 0.75
        assert dividend.frequency == 4
