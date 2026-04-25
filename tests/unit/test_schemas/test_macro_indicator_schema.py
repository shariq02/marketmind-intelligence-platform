# MarketMind Intelligence Platform V1
# Unit Tests for MacroIndicator Schema
# Date: April 24, 2026

import pytest
from datetime import datetime
from pydantic import ValidationError
from code.bronze.schemas.macro_indicator import MacroIndicator, Frequency


@pytest.mark.unit
@pytest.mark.schema
class TestMacroIndicatorSchema:
    """Test suite for MacroIndicator Pydantic model"""
    
    def test_valid_indicator(self):
        """Test creating valid macro indicator"""
        indicator = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026-01-15",
            value=0.3,
            forecast_value=0.2,
            previous_value=0.4,
            unit="percent",
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.indicator_name == "US_CPI_MOM"
        assert indicator.value == 0.3
    
    def test_indicator_name_uppercase_conversion(self):
        """Test indicator name is converted to uppercase"""
        indicator = MacroIndicator(
            indicator_name="us_cpi_mom",
            date="2026-01-15",
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.indicator_name == "US_CPI_MOM"
    
    def test_indicator_name_replaces_spaces(self):
        """Test spaces in indicator name are replaced with underscores"""
        indicator = MacroIndicator(
            indicator_name="US CPI MOM",
            date="2026-01-15",
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.indicator_name == "US_CPI_MOM"
    
    def test_invalid_date_format_fails(self):
        """Test invalid date format fails"""
        with pytest.raises(ValidationError) as exc_info:
            MacroIndicator(
                indicator_name="US_CPI_MOM",
                date="2026/01/15",
                value=0.3,
                frequency=Frequency.MONTHLY,
                source="akshare",
                ingestion_timestamp=1713564000000
            )
        
        assert "YYYY-MM-DD format" in str(exc_info.value)
    
    def test_negative_value_allowed(self):
        """Test negative values are allowed (for rates)"""
        indicator = MacroIndicator(
            indicator_name="US_INTEREST_RATE",
            date="2026-01-15",
            value=-0.5,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.value == -0.5
    
    def test_optional_forecast_value(self):
        """Test forecast_value is optional"""
        indicator = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026-01-15",
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.forecast_value is None
    
    def test_optional_previous_value(self):
        """Test previous_value is optional"""
        indicator = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026-01-15",
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        assert indicator.previous_value is None
    
    def test_all_frequencies(self):
        """Test all valid frequencies"""
        frequencies = [Frequency.DAILY, Frequency.WEEKLY, Frequency.MONTHLY, 
                      Frequency.QUARTERLY, Frequency.ANNUALLY]
        
        for freq in frequencies:
            indicator = MacroIndicator(
                indicator_name="TEST_INDICATOR",
                date="2026-01-15",
                value=0.3,
                frequency=freq,
                source="akshare",
                ingestion_timestamp=1713564000000
            )
            assert indicator.frequency == freq
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        indicator = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026-01-15",
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
        
        indicator_dict = indicator.to_dict()
        
        assert isinstance(indicator_dict, dict)
        assert indicator_dict['indicator_name'] == 'US_CPI_MOM'
    
    def test_from_akshare_response(self):
        """Test creating from AkShare response"""
        row = {
            '商品': '美国CPI月率',
            '日期': datetime.strptime('2026-01-15', '%Y-%m-%d').date(),
            '今值': 0.3,
            '预测值': 0.2,
            '前值': 0.4
        }
        
        indicator = MacroIndicator.from_akshare_response(
            indicator_name='US_CPI_MOM',
            row=row,
            frequency=Frequency.MONTHLY,
            unit='percent'
        )
        
        assert indicator.indicator_name == 'US_CPI_MOM'
        assert indicator.value == 0.3
        assert indicator.forecast_value == 0.2
    
    def test_missing_required_field_fails(self):
        """Test validation fails when required field missing"""
        with pytest.raises(ValidationError):
            MacroIndicator(
                indicator_name="US_CPI_MOM",
                date="2026-01-15",
                # Missing value
                frequency=Frequency.MONTHLY,
                source="akshare",
                ingestion_timestamp=1713564000000
            )
