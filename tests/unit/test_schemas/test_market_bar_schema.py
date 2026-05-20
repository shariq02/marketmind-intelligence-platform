# MarketMind Intelligence Platform V1
# Unit Tests for MarketBar Schema
# Date: April 24, 2026

import pytest
from pydantic import ValidationError
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.schemas.market_bar import MarketBar


@pytest.mark.unit
@pytest.mark.schema
class TestMarketBarSchema:
    """Test suite for MarketBar Pydantic model"""
    
    def test_valid_market_bar(self):
        """Test creating valid market bar"""
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.ticker == "AAPL"
        assert bar.open == 100.0
        assert bar.high == 105.0
    
    def test_ticker_uppercase_conversion(self):
        """Test ticker is converted to uppercase"""
        bar = MarketBar(
            ticker="aapl",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.ticker == "AAPL"
    
    def test_valid_granularities(self):
        """Test all valid granularities"""
        valid_granularities = ['1min', '5min', '15min', '30min', '1hour', '4hour', 'daily', 'weekly', 'monthly']
        
        for granularity in valid_granularities:
            bar = MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity=granularity,
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
            assert bar.granularity == granularity
    
    def test_invalid_granularity(self):
        """Test invalid granularity raises error"""
        with pytest.raises(ValidationError) as exc_info:
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="invalid",
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "Granularity must be one of" in str(exc_info.value)
    
    def test_high_less_than_low_fails(self):
        """Test validation fails when high < low"""
        with pytest.raises(ValidationError) as exc_info:
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=100.0,
                high=95.0,
                low=105.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "must be >=" in str(exc_info.value)
    
    def test_open_outside_range_fails(self):
        """Test validation fails when open outside high/low range"""
        with pytest.raises(ValidationError) as exc_info:
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=110.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "must be between" in str(exc_info.value)
    
    def test_close_outside_range_fails(self):
        """Test validation fails when close outside high/low range"""
        with pytest.raises(ValidationError) as exc_info:
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=100.0,
                high=105.0,
                low=99.0,
                close=110.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
        
        assert "must be between" in str(exc_info.value)
    
    def test_negative_price_fails(self):
        """Test validation fails for negative prices"""
        with pytest.raises(ValidationError):
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=-100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_zero_price_fails(self):
        """Test validation fails for zero prices"""
        with pytest.raises(ValidationError):
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=0.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_negative_volume_fails(self):
        """Test validation fails for negative volume"""
        with pytest.raises(ValidationError):
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=-1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_zero_volume_allowed(self):
        """Test zero volume is allowed"""
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=0.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.volume == 0.0
    
    def test_optional_vwap(self):
        """Test vwap is optional"""
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.vwap is None
    
    def test_optional_trade_count(self):
        """Test trade_count is optional"""
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.trade_count is None
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        bar_dict = bar.to_dict()
        
        assert isinstance(bar_dict, dict)
        assert bar_dict['ticker'] == 'AAPL'
        assert bar_dict['open'] == 100.0
    
    def test_from_polygon_response(self):
        """Test creating from Polygon API response"""
        bar_data = {
            't': 1704153600000,
            'o': 100.0,
            'h': 105.0,
            'l': 99.0,
            'c': 103.0,
            'v': 1000000.0,
            'vw': 102.0,
            'n': 5000
        }
        
        bar = MarketBar.from_polygon_response(
            ticker='AAPL',
            bar_data=bar_data,
            granularity='daily'
        )
        
        assert bar.ticker == 'AAPL'
        assert bar.timestamp == 1704153600000
        assert bar.open == 100.0
        assert bar.vwap == 102.0
        assert bar.trade_count == 5000
    
    def test_missing_required_field(self):
        """Test validation fails when required field is missing"""
        with pytest.raises(ValidationError):
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=100.0,
                high=105.0,
                low=99.0,
                # Missing close
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_invalid_timestamp(self):
        """Test validation fails for invalid timestamp"""
        with pytest.raises(ValidationError):
            MarketBar(
                ticker="AAPL",
                timestamp=0,
                granularity="daily",
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1713564000000
            )
    
    def test_ticker_strip_whitespace(self):
        """Test ticker whitespace is stripped"""
        bar = MarketBar(
            ticker="  AAPL  ",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
        
        assert bar.ticker == "AAPL"
