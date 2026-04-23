# ====================================================================
# Pydantic Model for Market OHLCV Bars
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/market_bar.py
# Purpose: Python validation model for OHLCV bar data from Polygon API
# ====================================================================
"""
Market Bar Pydantic Model

Validates OHLCV (Open, High, Low, Close, Volume) bar data before writing
to Kafka topic market.bars.v1.

Data Source: Polygon.io Stock Market API
Kafka Topic: market.bars.v1
Avro Schema: market_bars_v1.avsc

Field Validation:
- ticker: Non-empty string, uppercase
- timestamp: Positive integer (Unix milliseconds)
- granularity: Must be valid timeframe (1min, 5min, 15min, 1hour, daily)
- OHLC prices: Positive floats, High >= Low, Open/Close within High/Low range
- volume: Non-negative float
- vwap: Optional positive float
- trade_count: Optional non-negative integer

Usage:
    from code.bronze.schemas.market_bar import MarketBar
    
    bar = MarketBar(
        ticker="AAPL",
        timestamp=1767330000000,
        granularity="daily",
        open=272.255,
        high=277.84,
        low=269.0,
        close=271.01,
        volume=37838054.0,
        vwap=271.9197,
        trade_count=642187,
        adjusted=True,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
    
    # Validate
    bar_dict = bar.model_dump()
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime


class MarketBar(BaseModel):
    """
    OHLCV bar data model for stock market candlestick bars.
    
    Represents price and volume data for a specific time period
    (1min, 5min, 15min, 1hour, daily, etc.)
    """
    
    ticker: str = Field(
        ...,
        description="Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)",
        min_length=1,
        max_length=10
    )
    
    timestamp: int = Field(
        ...,
        description="Unix timestamp in milliseconds (UTC). Start of bar period.",
        gt=0
    )
    
    granularity: str = Field(
        ...,
        description="Bar granularity: 1min, 5min, 15min, 1hour, daily"
    )
    
    open: float = Field(
        ...,
        description="Opening price for the period",
        gt=0
    )
    
    high: float = Field(
        ...,
        description="Highest price during the period",
        gt=0
    )
    
    low: float = Field(
        ...,
        description="Lowest price during the period",
        gt=0
    )
    
    close: float = Field(
        ...,
        description="Closing price for the period",
        gt=0
    )
    
    volume: float = Field(
        ...,
        description="Total trading volume (shares traded)",
        ge=0
    )
    
    vwap: Optional[float] = Field(
        None,
        description="Volume-weighted average price",
        gt=0
    )
    
    trade_count: Optional[int] = Field(
        None,
        description="Number of individual trades",
        ge=0
    )
    
    adjusted: bool = Field(
        True,
        description="Whether prices are adjusted for corporate actions"
    )
    
    source: str = Field(
        "polygon",
        description="Data source identifier"
    )
    
    ingestion_timestamp: int = Field(
        ...,
        description="Unix timestamp when record was ingested",
        gt=0
    )
    
    @field_validator('ticker')
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        """Ensure ticker is uppercase."""
        return v.upper().strip()
    
    @field_validator('granularity')
    @classmethod
    def valid_granularity(cls, v: str) -> str:
        """Validate granularity is a known timeframe."""
        valid_values = ['1min', '5min', '15min', '30min', '1hour', '4hour', 'daily', 'weekly', 'monthly']
        if v not in valid_values:
            raise ValueError(f'Granularity must be one of {valid_values}')
        return v
    
    @model_validator(mode='after')
    def validate_ohlc_relationships(self) -> 'MarketBar':
        """Validate OHLC price relationships."""
        # High must be >= Low
        if self.high < self.low:
            raise ValueError(f'High ({self.high}) must be >= Low ({self.low})')
        
        # Open must be within High/Low range
        if not (self.low <= self.open <= self.high):
            raise ValueError(f'Open ({self.open}) must be between Low ({self.low}) and High ({self.high})')
        
        # Close must be within High/Low range
        if not (self.low <= self.close <= self.high):
            raise ValueError(f'Close ({self.close}) must be between Low ({self.low}) and High ({self.high})')
        
        # VWAP (if present) should generally be within High/Low range
        if self.vwap is not None:
            if not (self.low <= self.vwap <= self.high):
                # This is a warning case, not fatal - VWAP can be slightly outside in edge cases
                pass
        
        return self
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def from_polygon_response(cls, ticker: str, bar_data: dict, granularity: str) -> 'MarketBar':
        """
        Create MarketBar from Polygon API response.
        
        Args:
            ticker: Stock ticker symbol
            bar_data: Single bar from Polygon API results array
            granularity: Bar granularity (1min, daily, etc.)
        
        Returns:
            MarketBar instance
        
        Example:
            bar = MarketBar.from_polygon_response(
                ticker='AAPL',
                bar_data={
                    'v': 37838054.0,
                    'vw': 271.9197,
                    'o': 272.255,
                    'c': 271.01,
                    'h': 277.84,
                    'l': 269,
                    't': 1767330000000,
                    'n': 642187
                },
                granularity='daily'
            )
        """
        return cls(
            ticker=ticker,
            timestamp=bar_data['t'],
            granularity=granularity,
            open=bar_data['o'],
            high=bar_data['h'],
            low=bar_data['l'],
            close=bar_data['c'],
            volume=bar_data['v'],
            vwap=bar_data.get('vw'),
            trade_count=bar_data.get('n'),
            adjusted=True,
            source='polygon',
            ingestion_timestamp=int(datetime.now().timestamp() * 1000)
        )


# Example usage and validation
if __name__ == '__main__':
    # Valid bar
    valid_bar = MarketBar(
        ticker="aapl",  # Will be converted to uppercase
        timestamp=1767330000000,
        granularity="daily",
        open=272.255,
        high=277.84,
        low=269.0,
        close=271.01,
        volume=37838054.0,
        vwap=271.9197,
        trade_count=642187,
        adjusted=True,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
    print("Valid bar:", valid_bar.ticker, valid_bar.close)
    
    # Invalid bar (high < low) - will raise ValidationError
    try:
        invalid_bar = MarketBar(
            ticker="AAPL",
            timestamp=1767330000000,
            granularity="daily",
            open=272.255,
            high=269.0,  # Invalid: high < low
            low=277.84,
            close=271.01,
            volume=37838054.0,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
    except Exception as e:
        print(f"Validation error (expected): {e}")
